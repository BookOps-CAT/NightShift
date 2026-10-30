"""
This module provides the manager methods to perform particular tasks
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from nightshift.comms.sierra_search_platform import BplSolr, NypPlatform
from nightshift.comms.storage import Drive, get_credentials
from nightshift.comms.worldcat import Worldcat
from nightshift.datastore import DatabaseProvider, Resource, WorldcatQuery
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

logger = logging.getLogger("nightshift")


class Tasks:
    """
    Handles various operations related to ingesting new files, searching Worldcat,
    manipulating results and outputting the results to SFTP.
    """

    def __init__(
        self,
        db_provider: DatabaseProvider,
        library: str,
        libraryId: int,
        resource_categories: dict[str, ResCatByName],
    ) -> None:
        """
        Args:
            db_provider:                        `DatabaseProvider` instance
            library:                            'NYP' or 'BPL'
            libraryId:                          `datastore.Library.nid`
            resource_categories:                dictionary by category name with
                                                associated data
        """
        self.db_provider = db_provider
        self.library = library
        self.libraryId = libraryId
        self._res_cat = resource_categories
        self._res_cat_idx = self._create_resource_category_idx()
        self.rotten_apples: dict[int, list[str]] = dict()

    def _create_resource_category_idx(self) -> dict[int, ResCatById]:
        """Creates a dictionary of resource categories by their id."""
        return {
            data.nid: ResCatById(
                name,
                data.sierraBibFormatBpl,
                data.sierraBibFormatNyp,
                data.srcTags2Keep,
                data.dstTags2Delete,
                data.queryDays,
            )
            for name, data in self._res_cat.items()
        }

    def _create_rotten_apples_idx(self) -> dict[int, list[str]]:
        """
        Creates a dictionary of forbidden organization codes whose records
        should be excluded from WorldCat results.
        """
        with self.db_provider.session_scope() as session:
            return retrieve_rotten_apples(session)

    def check_resources_sierra_state(self, resources: list[Resource]) -> None:
        """
        Checks and updates status & suppression of records using
        NYPL Platform & BPL Solr.
        """
        sierra_platform: BplSolr | NypPlatform
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

        try:
            for resource in resources:
                response = sierra_platform.get_sierra_bib(resource.sierraId)
                resource.suppressed = response.is_suppressed()
                resource.status = response.get_status()

                with self.db_provider.session_scope() as session:
                    update_resource(
                        session,
                        resource.sierraId,
                        resource.libraryId,
                        suppressed=resource.suppressed,
                        status=resource.status,
                    )
                    if resource.status in ("staff_enhanced", "staff_deleted"):
                        add_event(session, resource, status=resource.status)
        finally:
            sierra_platform.close()

    def enhance_and_output_bibs(
        self, resource_category: str, resources: list[Resource]
    ) -> None:
        """
        Manipulates downloaded WorldCat records, serializes them into MARC21 format,
        transfers file to SFTP, and updates database records.

        Args:
            resource_category:              name of resource category being
                                            processed
            resources:                      list of `nightshift.datastore.Resource`
                                            instances to be processed
        """
        temp_file, enhanced_resources = self.manipulate_and_serialize_bibs(
            resource_category, resources
        )
        remote_file = self.transfer_to_drive(resource_category, temp_file)
        self.update_status_to_upgraded(remote_file, enhanced_resources)

    def get_worldcat_brief_bib_matches(self, resources: list[Resource]) -> None:
        """
        Queries Worldcat for given resources and persists responses in the database.

        Args:
            resources:                      list of `nightshift.datastore.Resource`
                                            instances
        """
        logger.info(
            f"Searching Worldcat for brief records for {len(resources)} resources."
        )

        self.rotten_apples = (
            self._create_rotten_apples_idx()
            if not self.rotten_apples
            else self.rotten_apples
        )

        with Worldcat(self.library) as worldcat:
            results = worldcat.get_brief_bibs(
                resources, rotten_apples=self.rotten_apples
            )
            for resource, response in results:
                with self.db_provider.session_scope() as session:
                    if response.is_match:
                        instance = update_resource(
                            session,
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
                        add_event(session, resource, status="worldcat_hit")
                    else:
                        db_res = update_resource(
                            session, resource.sierraId, resource.libraryId
                        )
                        db_res.queries.append(
                            WorldcatQuery(
                                resourceId=resource.nid,
                                match=False,
                                response=response.as_json,
                            )
                        )
                        add_event(session, resource, status="worldcat_miss")

    def get_worldcat_full_bibs(self, resources: list[Resource]) -> None:
        """
        Requests full bibliographic records from MetadataAPI service and
        stores the responses in the db.
        """
        logger.info(
            f"Downloading full records from WorldCat for {len(resources)} resources."
        )
        with Worldcat(self.library) as worldcat:
            results = worldcat.get_full_bibs(resources)
            for resource, response in results:
                with self.db_provider.session_scope() as session:
                    update_resource(
                        session, resource.sierraId, resource.libraryId, fullBib=response
                    )

    def ingest_new_files(self) -> None:
        """
        Imports resources from newly added SFTP files to the database.
        """
        drive_creds = get_credentials()
        with Drive(*drive_creds) as drive:
            unproc_files = self.isolate_unprocessed_files(drive)
            logger.info(f"Found following unprocessed files: {unproc_files}.")

            for handle in unproc_files:
                marc_target = drive.fetch_file(handle)
                marc_reader = BibReader(
                    marc_target, self.library, self.libraryId, self._res_cat
                )

                with self.db_provider.session_scope() as session:
                    file_record = add_source_file(session, self.libraryId, handle)
                    logger.debug(
                        f"Added SourceFile record for '{handle}': {file_record}"
                    )

                    n = 0
                    for resource in marc_reader:
                        n += 1
                        resource.sourceId = file_record.nid
                        add_resource(session, resource)

                    logger.info(f"Ingested {n} records from the file '{handle}'.")

    def isolate_unprocessed_files(self, drive: Drive) -> list[str]:
        """
        Finds unprocessed files on the network drive by comparing db SourceFile
        record handles with handles retrieved from the drive.
        """
        remote_files = drive.list_src_directory()
        library_remote_files = [
            file for file in remote_files if self.library in file and "-pout" in file
        ]
        logger.debug(
            f"Found following remote files for {self.library}: {library_remote_files}."
        )

        with self.db_provider.session_scope() as session:
            proc_files = retrieve_processed_files(session, self.libraryId)

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
            logger.info(f"Enhancing {self.library} Sierra bib # b{resource.sierraId}a.")
            be = BibEnhancer(resource, self.library, self._res_cat_idx)
            be.manipulate()
            if be.is_acceptable():
                be.save2file(out_fh)
                logger.debug(
                    f"{self.library} b{resource.sierraId}a has been output "
                    f"to '{out_fh}'."
                )
                enhanced_resources.append(resource)
            else:
                with self.db_provider.session_scope() as session:
                    update_resource(
                        session,
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

        if len(enhanced_resources) > 0:
            return (out_fh, enhanced_resources)
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

        creds = get_credentials()
        with Drive(*creds) as drive:
            if src_file is not None:
                return drive.output_file(src_file, remote_file_name_base)
            logger.info("No source file to output to SFTP.")
        return None

    def update_status_to_upgraded(
        self, out_file_handle: Optional[str], resources: list[Resource]
    ) -> None:
        """
        Upgrades given resources status to "bot_enhanced" and records output file id.
        Adds appropriate event record to the database.

        Args:
            out_file_handle:                handle of the output file
            resources:                      list of `nightshift.datastore.Resource`
                                            instances
        """
        if out_file_handle is None:
            logger.info(
                "Skipping resources enhancement status update. "
                "No SFTP output file this time."
            )
            return

        logger.info(f"Updating {len(resources)} resources status to 'bot_enhanced'.")

        with self.db_provider.session_scope() as session:
            out_file_record = add_output_file(session, self.libraryId, out_file_handle)

            for resource in resources:
                update_resource(
                    session,
                    resource.sierraId,
                    resource.libraryId,
                    status="bot_enhanced",
                    outputId=out_file_record.nid,
                    enhanceTimestamp=datetime.now(timezone.utc),
                )
                add_event(session, resource, status="bot_enhanced")
