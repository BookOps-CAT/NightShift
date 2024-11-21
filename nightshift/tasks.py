# -*- coding: utf-8 -*-

"""
This module provides the manager methods to perform particular tasks
"""
from datetime import datetime, timezone
import logging
import os
from typing import Optional

from sqlalchemy.orm.session import Session

from nightshift.comms.worldcat import Worldcat
from nightshift.comms.sierra_search_platform import NypPlatform, BplSolr
from nightshift.comms.storage import get_credentials, Drive
from nightshift.datastore import Resource, WorldcatQuery
from nightshift.datastore_transactions import (
    ResCatById,
    ResCatByName,
    add_event,
    add_output_file,
    add_resource,
    add_source_file,
    retrieve_processed_files,
    retrieve_rotten_apples,
    update_resource,
)
from nightshift.marc.marc_parser import BibReader
from nightshift.marc.marc_writer import BibEnhancer
from nightshift.ns_exceptions import DriveError

logger = logging.getLogger("nightshift")


class Tasks:
    """
    Handles various operations related to ingesting new files, searching Worldcat,
    manipulating results and outputting the results to SFTP.
    """

    def __init__(
        self,
        db_session: Session,
        library: str,
        libraryId: int,
        resource_categories: dict[str, ResCatByName],
    ) -> None:
        """
        Args:
            db_session:                         `sqlalchemy.Session` instance
            library:                            'NYP' or 'BPL'
            libraryId:                          `datastore.Library.nid`
            resource_categories:                dictionary by category name with
                                                associated data
        """
        self.db_session = db_session
        self.library = library
        self.libraryId = libraryId
        self._res_cat = resource_categories
        self._res_cat_idx = self._create_resource_category_idx()
        self.rotten_apples: dict[int, list[str]] = dict()

    def _create_resource_category_idx(self) -> dict[int, ResCatById]:
        """
        Creates a dictionary of resource categories by their id

        Returns:
            resource categories by id
        """
        res_cat_idx = dict()
        for name, data in self._res_cat.items():
            res_cat_idx[data.nid] = ResCatById(
                name,
                data.sierraBibFormatBpl,
                data.sierraBibFormatNyp,
                data.srcTags2Keep,
                data.dstTags2Delete,
                data.queryDays,
            )
        return res_cat_idx

    def _create_rotten_apples_idx(self) -> dict[int, list[str]]:
        """
        Creates a dictionary of forbidden organization codes which records
        should be excluded from retrieved from Worldcat results
        """
        rotten_apples = retrieve_rotten_apples(self.db_session)
        return rotten_apples

    def check_resources_sierra_state(self, resources: list[Resource]) -> None:
        """
        Checks and updates status & suppression of records using
        NYPL Platform & BPL Solr.
        This method updates resources in the database.

        Args:
            resources:                      list of `nightshift.datastore.Resource`
                                            instances to be checked
        """
        if self.library == "NYP":
            sierra_platform = NypPlatform()
        elif self.library == "BPL":
            sierra_platform = BplSolr()
        else:
            logger.error(
                f"Invalid library argument passed: '{self.library}'. "
                "Must be 'NYP' or 'BPL'."
            )
            raise ValueError("Invalid library argument. Must be 'NYP' or 'BPL'")

        logger.info(
            f"Checking {self.library} Sierra status for {len(resources)} resources."
        )

        for resource in resources:
            response = sierra_platform.get_sierra_bib(resource.sierraId)
            resource.suppressed = response.is_suppressed()
            resource.status = response.get_status()

            if resource.status in ("staff_enhanced", "staff_deleted"):
                add_event(self.db_session, resource, status=resource.status)

            # persist changes
            self.db_session.commit()

        sierra_platform.close()

    def enhance_and_output_bibs(
        self, resource_category: str, resources: list[Resource]
    ) -> None:
        """
        Manipulates downloaded WorldCat records, serializes them into MARC21 format
        and saves produced file to SFTP

        Args:
            resource_category:              name of resource category being
                                            processed
            resources:                      list of `nightshift.datastore.Resource`
                                            instances to be processed
        """
        # manipulate records and save to a temporary file
        temp_file, enhanced_resources = self.manipulate_and_serialize_bibs(
            resource_category, resources
        )

        # move temporary file to SFTP
        remote_file = self.transfer_to_drive(resource_category, temp_file)

        # finalize datastore resource status
        self.update_status_to_upgraded(remote_file, enhanced_resources)

    def get_worldcat_brief_bib_matches(self, resources: list[Resource]) -> None:
        """
        Queries Worldcat for given resources and persists responses
        in the database.

        Args:
            resources:                      list of `nightshift.datastore.Resource`
                                            instances
        """
        logger.info(
            f"Searching Worldcat for brief records for {len(resources)} resources."
        )

        if not self.rotten_apples:
            self.rotten_apples = self._create_rotten_apples_idx()

        with Worldcat(self.library) as worldcat:
            results = worldcat.get_brief_bibs(
                resources, rotten_apples=self.rotten_apples
            )
            for resource, response in results:
                if response.is_match:
                    instance = update_resource(
                        self.db_session,
                        resource.sierraId,
                        resource.libraryId,
                        oclcMatchNumber=response.oclc_number,
                    )
                    instance.queries.append(
                        WorldcatQuery(
                            resourceId=resource.nid,
                            match=True,
                            response=response.as_json,
                        )
                    )

                    # add event for stats
                    add_event(self.db_session, resource, status="worldcat_hit")
                else:
                    resource.queries.append(
                        WorldcatQuery(match=False, response=response.as_json)
                    )
                    add_event(self.db_session, resource, status="worldcat_miss")
                self.db_session.commit()

    def get_worldcat_full_bibs(self, resources: list[Resource]) -> None:
        """
        Requests full bibliographic records from MetadataAPI service and
        stores the responses in the db.

        Args:
            resources:                      list of `nightshift.datastore.Resource`
                                            instances
        """
        logger.info(
            f"Downloading full records from WorldCat for {len(resources)} resources."
        )
        with Worldcat(self.library) as worldcat:
            results = worldcat.get_full_bibs(resources)
            for resource, response in results:
                update_resource(
                    self.db_session,
                    resource.sierraId,
                    resource.libraryId,
                    fullBib=response,
                )

                # commit each full bib response in case something
                # breaks during this lengthy process;
                # this should save time if process need to be restarted
                self.db_session.commit()

    def ingest_new_files(self) -> None:
        """
        Imports to the database resources in a newly added files on the SFTP.

        Files on the remote server must have 'NYP' or 'BPL' in their names to be
        considered by this process (Sierra bib themselves do not have any
        system identifying data to rely on).

        Sierra Scheduler will be configured to output data dumps following this
        practice.
        """
        drive_creds = get_credentials()
        with Drive(*drive_creds) as drive:

            # find files that have not been processed
            unproc_files = self.isolate_unprocessed_files(drive)
            logger.info(f"Found following unprocessed files: {unproc_files}.")

            # add records data to the db
            for handle in unproc_files:
                file_record = add_source_file(self.db_session, self.libraryId, handle)
                logger.debug(f"Added SourceFile record for '{handle}': {file_record}")
                marc_target = drive.fetch_file(handle)
                marc_reader = BibReader(
                    marc_target, self.library, self.libraryId, self._res_cat
                )

                n = 0
                for resource in marc_reader:
                    n += 1
                    resource.sourceId = file_record.nid
                    add_resource(self.db_session, resource)

                self.db_session.commit()
                logger.info(f"Ingested {n} records from the file '{handle}'.")

    def isolate_unprocessed_files(self, drive: Drive) -> list[str]:
        """
        Finds unprocessed files on the network drive but comparing db SourceFile
        record handles with handles retrieved from the drive.

        Args:
            drive:                      `nightshift.comms.storage.Drive` client instance

        Returns:
            list of remote unprocessed file handles
        """
        remote_files = drive.list_src_directory()

        # select only files with library code and -pout suffix
        library_remote_files = [
            file for file in remote_files if self.library in file and "-pout" in file
        ]
        logger.debug(
            f"Found following remote files for {self.library}: {library_remote_files}."
        )

        proc_files = retrieve_processed_files(self.db_session, self.libraryId)

        return [f for f in library_remote_files if f not in proc_files]

    def manipulate_and_serialize_bibs(
        self,
        resource_category: str,
        resources: list[Resource],
        out_fh: str = "temp.mrc",
    ) -> tuple[Optional[str], list[Resource]]:
        """
        Merges Sierra brief bibs data with WorldCat full bib,
        and serializes them into MARC21 format

        Args:
            resource_category:              name of resource category ('ebook', etc.)
            resources:                      list of `nightshift.datastore.Resource`
                                            instances
            out_fh:                         path of the file where records are saved

        Returns:
            tuple (output file, list of enhanced resources)
        """
        enhanced_resources = []
        skipped_resources = []

        # make sure to start from scratch
        try:
            os.remove(out_fh)
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.error(
                f"Unable to empty temp file '{out_fh}' before appending MARC "
                f"records. Error {exc}"
            )
            raise

        for resource in resources:
            be = BibEnhancer(resource, self.library, self._res_cat_idx)
            be.manipulate()
            if be.bib is not None:
                be.save2file(out_fh)
                logger.debug(
                    f"{self.library} b{resource.sierraId}a has been output "
                    f"to '{out_fh}'."
                )
                enhanced_resources.append(resource)
            else:
                # update to blank state to allow later date query
                update_resource(
                    self.db_session,
                    resource.sierraId,
                    resource.libraryId,
                    oclcMatchNumber=None,
                    fullBib=None,
                )
                logger.warning(
                    f"{self.library} b{resource.sierraId}a enhancement incomplete. "
                    "Skipping."
                )
                skipped_resources.append(resource)

        logger.info(
            f"Enhanced and serialized {len(enhanced_resources)} and skipped "
            f"{len(skipped_resources)} {self.library} {resource_category} record(s)."
        )
        self.db_session.commit()

        if len(enhanced_resources) > 0:
            return (out_fh, enhanced_resources)
        else:
            return (None, enhanced_resources)

    def transfer_to_drive(
        self, resource_category: str, src_file: Optional[str]
    ) -> Optional[str]:
        """
        Transfers local temporary MARC21 file to network drive.
        Arguments `library` and `resource_category` is used to name the MARC file on the
        network drive.
        After successful transfer the temporary file is deleted.

        Args:
            resource_category:             name of resource category being processed
            src_file:                      temporary local file to be transferred

        Returns:
            SFTP file handle

        Raises:
            DriveError
        """
        today = datetime.now().date()
        remote_file_name_base = f"{today:%y%m%d}-{self.library}-{resource_category}"
        remote_file = None

        creds = get_credentials()
        with Drive(*creds) as drive:
            try:
                if src_file is not None:
                    remote_file = drive.output_file(src_file, remote_file_name_base)
                else:
                    logger.info("No source file to output to SFTP.")
            except DriveError:
                raise

            return remote_file

    def update_status_to_upgraded(
        self,
        out_file_handle: Optional[str],
        resources: list[Resource],
    ) -> None:
        """
        Upgrades given resources status to "bot_enhanced" and records output file id.
        Adds appropriate event record to the database.

        Args:
            out_file_handle:                handle of the output file
            resources:                      list of `nightshift.datastore.Resource`
                                            instances
        """
        if out_file_handle is not None:

            logger.info(
                f"Updating {len(resources)} resources status to 'bot_enhanced'."
            )

            out_file_record = add_output_file(
                self.db_session, self.libraryId, out_file_handle
            )

            for resource in resources:
                update_resource(
                    self.db_session,
                    resource.sierraId,
                    resource.libraryId,
                    status="bot_enhanced",
                    outputId=out_file_record.nid,
                    enhanceTimestamp=datetime.now(timezone.utc),
                )

                add_event(self.db_session, resource, status="bot_enhanced")

            self.db_session.commit()
        else:
            logger.info(
                "Skipping resources enhancement status update. "
                "No SFTP output file this time."
            )
