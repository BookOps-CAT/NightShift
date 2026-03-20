"""
This module provides methods for manipulation and serialization of Worldcat responses
into MARC21.
"""

import logging
import pickle

from pymarc import Field, Indicators, Subfield

from .. import __title__, __version__
from ..datastore import Resource
from ..datastore_transactions import ResCatById
from .marc_parser import worldcat_response_to_bib

logger = logging.getLogger("nightshift")


class BibEnhancer:
    """
    A class used for upgrading MARC records.

    To initiate the class pass `nightshift.datastore.Resource` instance that
    was updated with WorldCat MARC XML record.

    Invoking `manipulate()` method on the instance of this class does the following:
     - encodes matching Sierra bib to be overlaid (matching bib # in 907 - BPL
        or 945 - NYPL)
     - removes unwanted MARC tags specified in `constants.RESOURCE_CATEGORIES`,
     - deletes 6xx from unsupported thesauri,
     - adds local tags preserved from the original Sierra bib specified in
        `constants.RESOURCE_CATEGORIES`
     - creates for each resource type an appropriate call number tag (099 for BPL or
        091 for NYPL)
     - enforces presence of appropriate genre heading in 655 ('Electronic books', etc.)
     - adds a Sierra command tag in the 949 tag that specifies Sierra bib format
        code and optionally Sierra bib code 3 for records suppressed from public view
    - adds Nightshift name and version to bib Sierra initials MARC tag (947 for BPL,
        901 for NYPL)
    - removes OCLC prefix from the 001 tag for NYPL records

    Calling `save2file()` method on the instance of this class serializes pymarc object
    into MARC 21 and saves it to a temporary file.
    """

    DATA: dict[str, dict[str, dict[str, str]]] = {
        "BPL": {
            "tags": {"initials_tag": "947", "sierra_id_tag": "907", "call_tag": "099"},
            "values": {
                "default_loc": "bn=elres",
                "bib_format_attr": "sierraBibFormatBpl",
                "ebook": "eBOOK",
                "eaudio": "eAUDIO",
                "evideo": "eVIDEO",
            },
        },
        "NYP": {
            "tags": {"initials_tag": "901", "sierra_id_tag": "945", "call_tag": "091"},
            "values": {
                "default_loc": "bn=ia",
                "bib_format_attr": "sierraBibFormatNyp",
                "ebook": "eNYPL Book",
                "eaudio": "eNYPL Audio",
                "evideo": "eNYPL Video",
            },
        },
    }

    def __init__(
        self,
        resource: Resource,
        library: str,
        resource_categories: dict[int, ResCatById],
    ) -> None:
        """
        Initiates BibEnhancer by parsing WorldCat MARC XML byte string received
        from MetadataAPI service.

        Args:
            resource:                       `datastore.Resource` instance
            library:                        'NYP' or 'BPL'
                                            as a key and code as value
            resource_categories:            resource categories data with
                                            `datastore.ResourceCategory.nid` as key

        Raises:
            TypeError
        """
        self.resource = resource
        self.library = library
        self.res_cat = resource_categories.get(self.resource.resourceCategoryId)
        self.tags: dict[str, str] = self.DATA[self.library]["tags"]
        self.values: dict[str, str] = self.DATA[self.library]["values"]

        self.bib = worldcat_response_to_bib(resource.fullBib, self.library)

    def is_acceptable(self) -> bool:
        """
        Checks if full Worldcat record meet minimum criteria and
        a valid call number can be constructed.
        """
        if self._meets_minimum_criteria() and self._add_call_number():
            return True
        else:
            return False

    def manipulate(self) -> None:
        """
        Manipulates WorldCat record according to `nightshift.constants` module
        specs.

        Full manipulation happens only if records meets minimum requirements and
        a call number can be constructed.
        """
        # delete unwanted MARC tags
        self._purge_tags()

        # remove 6xx tags with terms from unsupported thesauri
        self.bib.remove_unsupported_subjects()

        # remove e-resources vendor tags
        self._remove_eresource_vendors()

        # if does not meet criteria delete Worldcat bib
        if not self.is_acceptable():
            logger.info(
                f"Worldcat record # {self.resource.oclcMatchNumber} is rejected. "
                "Does not meet minimum requirements."
            )
            return None
        else:
            logger.info(
                f"Worldcat record # {self.resource.oclcMatchNumber} is acceptable. "
                "Meets minimum requirements."
            )

            # genre tags
            self._clean_up_genre_tags()

            # add tags from the local bib
            self._add_local_tags()

            # add Sierra bib # for overlaying
            self._add_sierraId()

            # add Sierra import command tags
            self._add_command_tag()

            # add bot's initials
            self._add_initials_tag()

            # prep OCLC control number
            if self.library == "NYP":
                self._digits_only_in_tag_001()

    def save2file(self, file_path: str = "temp.mrc") -> None:
        """
        Appends bib as MARC21 to a temporary dump file.

        Args:
            file_path:                    path of the file to output records

        Raises:
            OSError
        """
        try:
            with open(file_path, "ab") as out:
                out.write(self.bib.as_marc())
                logger.debug(
                    f"Saving to file {self.library} record b{self.resource.sierraId}a."
                )
        except OSError as exc:
            logger.error(f"Unable to save record to a temp file. Error {exc}.")
            raise

    def _add_call_number(self) -> bool:
        """
        Adds a call number MARC tag specific to resource category and each library.

        !!Creation of call numbers will be moved to a separate module or even package
        when print materials will be incorporated into the process (due to complexity)!!

        Returns:
            bool
        """
        resource_cat = getattr(self.res_cat, "name", None)

        tag = self.tags.get("call_tag")
        value = self.values.get(resource_cat) if resource_cat else None

        if tag and value:
            call_number = Field(
                tag=tag,
                indicators=Indicators(" ", " "),
                subfields=[Subfield("a", value)],
            )
            self.bib.add_field(call_number)
            logger.debug(f"Added {value} to {self.library} b{self.resource.sierraId}a.")
            return True
        else:
            logger.warning(
                f"Unable to create call number for {self.library} "
                f"b{self.resource.sierraId}a."
            )
            return False

    def _add_command_tag(self) -> None:
        """
        Adds Sierra's command MARC tag (949) specific to resource category and
        each library.

        Includes commands for matching Sierra bib # (ov=) Sierra bib format (b2=), and
        optional suppression (b2=) code.
        """
        commands = []

        # Sierra bib format
        sierra_format_code = getattr(self.res_cat, self.values["bib_format_attr"])

        commands.append(f"b2={sierra_format_code}")

        # Sierra suppression code
        if self.resource.suppressed:
            commands.append("b3=n")

        # set default location
        commands.append(self.values["default_loc"])

        command_str = ";".join(commands)

        # add command to bib
        command_tag = Field(
            tag="949",
            indicators=Indicators(" ", " "),
            subfields=[Subfield("a", f"*{command_str};")],
        )
        self.bib.add_field(command_tag)
        logger.debug(
            f"Added 949 command tag: {command_tag.value()} to {self.library} "
            f"b{self.resource.sierraId}a."
        )

    def _clean_up_genre_tags(self) -> None:
        """
        Adds genre tags to e-resources.
        """
        resource_cat = getattr(self.res_cat, "name", None)

        if resource_cat == "ebook":
            for field in self.bib.subjects:
                if "electronic books" in field.value().lower():
                    self.bib.remove_field(field)
                    return
        if resource_cat == "eaudio":
            # 'Audiobooks' term
            # remove electronic audiobooks
            for field in self.bib.subjects:
                if "electronic audiobooks" in field.value().lower():
                    self.bib.remove_field(field)

            # but keep lcgft audiobooks
            found = False
            for field in self.bib.subjects:
                if "audiobooks." in field.value().lower():
                    found = True
                    break
            if not found:
                self.bib.add_field(
                    Field(
                        tag="655",
                        indicators=Indicators(" ", "7"),
                        subfields=[
                            Subfield("a", "Audiobooks."),
                            Subfield("2", "lcgft"),
                        ],
                    )
                )
                logger.debug("Added 'Audiobooks' LCGFT genre to 655 tag.")
            return
        if resource_cat == "evideo":
            found = False
            for field in self.bib.subjects:
                if "internet videos." in field.value().lower():
                    found = True
                    break
            if not found:
                self.bib.add_field(
                    Field(
                        tag="655",
                        indicators=Indicators(" ", "7"),
                        subfields=[
                            Subfield("a", "Internet videos."),
                            Subfield("2", "lcgft"),
                        ],
                    )
                )
                logger.debug("Added 'Internet videos' LCGFT genre to 655 tag.")
            return

    def _add_local_tags(self) -> None:
        """
        Adds local tags to the WorldCat bib.
        """
        if self.resource.srcFieldsToKeep:
            tags2keep = pickle.loads(self.resource.srcFieldsToKeep)
            fields = []
            for tag in tags2keep:
                self.bib.add_ordered_field(tag)
                fields.append(tag.tag)
            logger.debug(
                f"Added following local fields {fields} to {self.library} "
                f"b{self.resource.sierraId}a."
            )
        else:
            logger.debug(
                f"No local tags to keep were found for {self.library} "
                f"b{self.resource.sierraId}a."
            )

    def _add_initials_tag(self) -> None:
        """
        Marks records as produced by the NightShift bot.
        """
        tag = self.tags["initials_tag"]

        self.bib.add_field(
            Field(
                tag=tag,
                indicators=Indicators(" ", " "),
                subfields=[Subfield("a", f"{__title__}/{__version__}")],
            )
        )
        logger.debug(
            f"Added initials tag {tag} to {self.library} b{self.resource.sierraId}a."
        )

    def _add_sierraId(self) -> None:
        """
        Adds 907 (BPL) or 945 (NYP) tag to manipulated MARC record for
        matching/overlaying purposes.
        """
        self.bib.add_field(
            Field(
                tag=self.tags["sierra_id_tag"],
                indicators=Indicators(" ", " "),
                subfields=[Subfield("a", f".b{self.resource.sierraId}a")],
            )
        )

    def _digits_only_in_tag_001(self) -> None:
        """
        Removes OCLC control number prefix from the 001 tag
        """
        controlNo = self.bib["001"].value()
        controlNo_without_prefix = controlNo.strip("ocnm")
        self.bib["001"].data = controlNo_without_prefix

    def _meets_minimum_criteria(self) -> bool:
        """
        Checks if Worldcat record meets minimum criteria
        """
        # check uppercase title (indicates poor quality)
        if self.bib.title and self.bib.title.isupper():
            logger.debug("Worldcat record failed uppercase title test.")
            return False

        # missing statement of responsibility
        if "c" not in self.bib["245"]:
            logger.debug("Worldcat record failed statement of resp. test.")
            return False

        # no physical description
        if "300" not in self.bib:
            logger.debug("Worldcat record failed physical desc. test.")
            return False

        # messed up diacritics indicated by presence of "©" (b"\xc2\xa9") or
        # "℗" (b"\xe2\x84\x97")
        if (
            self.bib.author
            and self.bib.title
            and (
                b"\xc2\xa9" in bytes(self.bib.author, "utf-8")
                or b"\xc2\xa9" in bytes(self.bib.title, "utf-8")
            )
        ):
            logger.debug("Worldcat record failed characters encoding test.")
            return False

        elif (
            self.bib.author
            and self.bib.title
            and (
                b"\xe2\x84\x97" in bytes(self.bib.author, "utf-8")
                or b"\xe2\x84\x97" in bytes(self.bib.title, "utf-8")
            )
        ):
            logger.debug("Worldcat record failed characters encoding test.")
            return False

        # has at least one valid subject tag
        if not self.bib.subjects:
            logger.debug("Worldcat record failed subjects test.")
            return False

        return True

    def _purge_tags(self) -> None:
        """
        Removes MARC tags indicated in `constants.RESOURCE_CATEGORIES`
        from the WorldCat bib.
        """
        if self.res_cat:
            delete_tags = self.res_cat.dstTags2Delete
            for tag in delete_tags:
                if tag in self.bib:
                    self.bib.remove_fields(tag)
            logger.debug(
                f"Removed {delete_tags} from {self.library} b{self.resource.sierraId}a."
            )
        else:
            logger.warning("Encountered unsupported resource category.")

    def _remove_eresource_vendors(self) -> None:
        """
        Removes from e-resource bib any tags indicating distributor
        """
        vendors = ["overdrive", "cloudlibrary", "3m", "recorded books"]

        for tag in self.bib.get_fields("710"):
            for vendor in vendors:
                if vendor in tag.value().lower():
                    self.bib.remove_field(tag)
