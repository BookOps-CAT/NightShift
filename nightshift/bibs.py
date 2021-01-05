# -*- coding: utf-8 -*-

"""
This module provides methods to generate MARC records

MARC manipulation rules:
Use OCLC matching record as a base

    shared:
        - remove 020 from OCLC full bib
        - remove 024 from OCLC full bib (?)
        - remove 037s from OCLC full bib
        - remove local fields 019, 084, 938
        - remove subject heading tags different than LCSH, LCGN, GSAFD, FAST, (BISACSH?)
        - remove 856s tags from full OCLC bib

        - add 019 with ODN control number and sub
        - add 020 based on MarcExpress bib (ME) with $q (electronic bk.)
        - add 024 based on ME bib
        - add 037$a based on ME bib
        - add 710 2 $a Overdrive, Inc.,$edistributor (if does not exists)
        - add 856
            - exerpt: 856 4  $u link $3 Exerpt
            - image: 856 4  $u link $3 Image
            - image: 856 4  $u link $3 Thumbnail

    NYPL only:
        - 001  includes OCLC number without a prefix
        - create 091  $a eBOOK/eAudio/eVideo based on Sierra format value
        - create 949 command line to set Sierra format, target bib
        - add NYPL specific 856s based on ME
            - content: 856 40 $u link $y Access eNYPL
        - add 901 $a NightShift staff

    BPL only:
        - 001  includes OCLC number with a prefix
        - create 099  $a eBook/eAudio/eVideo based on Sierra format value
        - create 949 command line to set Sierra format, target bib
        - add BPL specific 856s based on ME
            - content: 856 40 $u link $z An electronic book accessible online
        - add 947 $a NightShift staff

"""
import datetime
from io import BytesIO
from typing import List

# import xml.etree.ElementTree as ET

# import pymarc
from pymarc import Field, MARCWriter, Record, XmlHandler, parse_xml


from nightshift.datastore import Resource, UrlField
from nightshift.errors import NightShiftError


APPROVED_VOCAB = {
    "nyp": ["fast", "gsafd", "lcgft", "bisacsh"],
    "bpl": ["fast", "gsafd", "lcgft"],
}
# lctgm?


class NightShiftXmlHandler(XmlHandler):
    """
    Subclass of pymarc's XmlHandler that deals with single record only.
    """

    def __init__(self, strict=False, normalize_form=None):

        super().__init__(strict, normalize_form)

    def process_record(self, record):
        self.records = record


def parse_xml_record(xml_file, strict=False, normalize_form=None):
    """Parse XML data."""
    handler = NightShiftXmlHandler(strict, normalize_form)
    parse_xml(xml_file, handler)
    return handler.records


def response2pymarc(response_content: bytes) -> Record:
    """
    Serializes full record xml Worldcat response to pymarc record object

    Args:
        response_content:           full record response content (bytes)
    returns:
        `pymarc.record.Record`
    """
    record = BytesIO(response_content)
    return parse_xml_record(record)


def add_oclc_prefix(control_number: str) -> str:
    """
    Adds appropriate OCLC prefix to OCLC's control number.

    Args:
        control_number:             OCLC control number without a prefix

    Returns:
        control_number with prefix
    """
    if len(control_number) <= 8:
        # left pad with zeros
        control_number = add_zeros_to_oclc_number(control_number)
        control_number = f"ocm{control_number}"
    elif len(control_number) == 9:
        control_number = f"ocn{control_number}"
    elif len(control_number) >= 10:
        control_number = f"on{control_number}"

    return control_number


def add_zeros_to_oclc_number(control_number: str) -> str:
    """
    Pads with zeros 8 digit OCLC numbers

    Args:
        control_number:             OCLC control number without a prefix

    Returns:
        control_number
    """
    if len(control_number) < 8:
        return control_number.zfill(8)
    else:
        return control_number


def remove_unwanted_tags(record: Record, material_type: str) -> List[str]:
    """Removes tags that will be replaced with MarcExpress tags or are
    simply not desired.

    Args:
        record:                 full Worldcat record as pymarc object

    Returns:
        list of tags
    """
    eresource_tags = [
        "001",  # will be re-generated insted bc sys diffs
        "019",
        "020",
        "024",
        "037",
        "084",
        "099",
        "091",
        "263",
        "856",
        "938",  # is there a way to remove all 9xxs?
        "949",
    ]
    print_tags = [
        "001",  # will be re-generated instead bc sys diffs
        "019",
        "084",
        "099",
        "091",
        "263",
        "938",
        "949",
    ]
    if material_type == "eresources":
        record.remove_fields(*eresource_tags)
    elif material_type == "print":
        record.remove_fields(*print_tags)
    else:
        raise NightShiftError(
            "Unable to manipulate MARC record for unknown material type."
        )


def construct_isbn_tags(isbns: str) -> List[Field]:
    """Converts a comma separated string of ISBNs to list of ISBN fields

    Args:
        isbns:                      a comma seprated list of ISBNs

    Returns:
        list of `pymarc.field.Field`
    """
    tags = []
    if isbns is not None:
        isbns = isbns.split(",")
        for isbn in isbns:
            tags.append(
                Field(
                    tag="020",
                    indicators=[" ", " "],
                    subfields=["a", isbn, "q", "(electronic bk.)"],
                )
            )
    return tags


def construct_upc_tags(upcs: str) -> List[Field]:
    """
    Converst a comma separated string of UPCs to list of 024 fields

    Args:
        upcs:                       a comma separated list of UPC numbers

    Returns:
        list of `pymarc.field.Field`
    """
    tags = []
    if upcs is not None:
        upcs = upcs.split(",")
        for upc in upcs:
            tags.append(
                Field(
                    tag="024",
                    indicators=["1", " "],
                    subfields=["a", upc],
                )
            )
    return tags


def construct_overdrive_reserve_id_tag(reserve_id: str) -> Field:
    """
    Converts reserve id into a MARC tag

    Args:
        reserve_id:                 OverDrive reserve id number

    Returns:
        `pymarc.field.Field` object
    """
    if reserve_id is not None:
        return Field(
            tag="037",
            indicators=[" ", " "],
            subfields=[
                "a",
                reserve_id,
                "b",
                "OverDrive, Inc.",
                "n",
                "http://www.overdrive.com",
            ],
        )


def has_overdrive_access_point_tag(record: Record) -> bool:
    """
    Determines if the record has 710 2 $a Overdrive, Inc. tag

    Args:
        record:                     pymarc.field.Field instance

    Returns:
        bool
    """
    found = False
    for tag in record.get_fields("710"):
        if "overdrive" in tag.value().lower():
            found = True
            break
    return found


def determine_url_label(uTypeId: int) -> str:
    """
    Maps UrlField urlTypeId param to public label subfield in subfield 3

    Args:
        uTypeId:                    datastore UrlField uTypeId

    Returns:
        `pymarc.field.Field` object
    """
    if uTypeId == 2:
        return "Excerpt"
    elif uTypeId == 3:
        return "Image"
    elif uTypeId == 4:
        return "Thumbnail"


def construct_callnumber_tag(sierraFormatId: int, librarySystemId: int) -> Field:
    """
    Creates call number (091 for NYPL or 099 for BPL) tag

    Args:
        sierraFormatId:             datastore Resource sierraFormatId
        librarySystemId:            datastore Resource librarySystemId

    Returns:
        `pymarc.field.Field` object
    """
    if sierraFormatId == 1:
        call_number = "ERROR UNKNOWN"

    if librarySystemId == 1:
        tag = "091"

        if sierraFormatId == 2:
            call_number = "eNYPL Book"
        elif sierraFormatId == 3:
            call_number = "eNYPL Audio"
        elif sierraFormatId == 4:
            call_number = "eNYPL Video"
        elif sierraFormatId == 5:
            raise NightShiftError("Processing of print materials not implemented yet.")

    elif librarySystemId == 2:
        tag = "099"
        if sierraFormatId == 2:
            call_number = "eBOOK"
        elif sierraFormatId == 3:
            call_number = "eAUDIO"
        elif sierraFormatId == 4:
            call_number = "eVIDEO"
        elif sierraFormatId == 5:
            raise NightShiftError("Processing of print materials not implemented yet.")

    return Field(
        tag=tag,
        indicators=[" ", " "],
        subfields=["a", call_number],
    )


def construct_generic_url_tags(
    url_data: List[UrlField],
) -> List[Field]:
    """
    Converts datastore url records into 856 tags

    Args:
        url_data:                   datastore UrlField records as a list

    Returns:
        list of `pymarc.field.Field`
    """
    tags = []
    if url_data:
        for url in url_data:
            if url.uTypeId in (2, 3, 4):
                label = determine_url_label(url.uTypeId)
                tags.append(
                    Field(
                        tag="856",
                        indicators=["4", " "],
                        subfields=["u", url.url, "3", label],
                    )
                )

    return tags


def construct_content_url_tag(url: str, librarySystemId: int) -> Field:
    """Creates library specific content url

    Args:
        url:                        content url
        librarySystemId:            datastore LibrarySystem id

    Returns:
        `pymarc.field.Field` object
    """
    # NYPL
    if librarySystemId == 1:
        label = "Access eNYPL"
        subfield_tag = "y"
    elif librarySystemId == 2:
        label = "An electronic book accessible online"
        subfield_tag = "z"

    return Field(
        tag="856",
        indicators=["4", "0"],
        subfields=["u", url, subfield_tag, label],
    )


def construct_initials_tag(librarySystemId: int) -> Field:
    """
    Creates 901 or 947 tag with NightShift marker

    Args:                           datastore.Resrouce.librarySystemId

    Returns:
        `pymarc.field.Field` object
    """

    # NYPL
    if librarySystemId == 1:
        tag = "901"
    elif librarySystemId == 2:
        tag = "947"

    return Field(tag=tag, indicators=[" ", " "], subfields=["a", "NightShift"])


def construct_overdrive_access_point_tag() -> Field:
    """
    Construct 710 access point for OverDrive

    Returns:
        `pymarc.field.Field` object
    """
    return Field(
        tag="710",
        indicators=["2", " "],
        subfields=["a", "OverDrive, Inc."],
    )


def construct_overdrive_control_number_tag(control_number: str) -> Field:
    """
    Constructs 019 MARC tag with provided OverDrive control number

    Args:
        control_number:             OverdDrive MarcExpress control number

    Returns:
        `pymarc.field.Field` object
    """

    return Field(
        tag="019",
        indicators=[" ", " "],
        subfields=["a", control_number],
    )


def construct_oclc_control_number_tag(
    control_number: str, librarySystemId: int
) -> Field:
    """
    Constructs library specific OCLC control number

    Args:
        control_number:             OCLC control number without a prefix
        librarySystemId:            datastore Resource librarySystemId

    Returns:
        `pymarc.field.Field` object
    """
    # NYPL
    if librarySystemId == 1:
        control_number = add_zeros_to_oclc_number(control_number)
    # BPL
    elif librarySystemId == 2:
        control_number = add_oclc_prefix(control_number)

    return Field(tag="001", data=control_number)


def construct_sierra_command_tag(
    sbid: int, sierraFormatId: int, librarySystemId: int
) -> Field:
    """
    Creates Sierra command line tag with sierra format, load table, etc. info
    Args:
        sbid:                       Sierra bib number, datastore Resource sbid
        sierraFormatId:             datastore Resource sierraFormatId
        librarySystemId:            datastore Resource librarySystemId

    Returns:
        `pymarc.field.Field` object
    """
    commands = []
    # NYPL
    if librarySystemId == 1:
        if sierraFormatId == 1:
            pass
        elif sierraFormatId == 2:
            commands.append("b2=z")
        elif sierraFormatId == 3:
            commands.append("b2=n")
        elif sierraFormatId == 4:
            commands.append("b2=3")
        elif sierraFormatId == 5:
            commands.append("b2=a")

        # location code
        if sierraFormatId in (2, 3, 4):
            commands.append(
                "bn=ia"
            )  # is it really needed, the loc should be already set

        # bib code3
        commands.append("b3=a")  # value for Backastage processing?

    # BPL
    elif librarySystemId == 2:
        if sierraFormatId == 1:
            pass
        elif sierraFormatId == 2:
            commands.append("b2=x")
        elif sierraFormatId == 3:
            commands.append("b2=z")
        elif sierraFormatId == 4:
            commands.append("b2=v")
        elif sierraFormatId == 5:
            commands.append("b2=a")

    # overlay match point
    commands.append(f"ov=.b{sbid}a")

    command_str = f"*{';'.join(commands)};"
    return Field(tag="949", indicators=[" ", " "], subfields=["a", command_str])


def determine_material_type(bibCategoryId: int) -> str:
    """
    Maps datastore Resource bibCategoryId to more readable labels - just
    to add some readability to code

    Args:
        bibCategoryId:              datastore bibCategoryId value

    Returns:
        material_type label
    """

    if bibCategoryId == 1:
        material_type = "eresources"
    elif bibCategoryId == 2:
        material_type = "print"
    else:
        material_type = "unknown"
        # what would be appropriate course for this type of scenario?
        # log? raise exception?
    return material_type


def is_approved_vacabulary(value: str, librarySystemId: int) -> bool:
    """
    Checks if value of subfield $2 is part of approved by system thesauri

    Args:
        value:                      value of subfield $2 of the MARC record
        librarySystemId:            datastore Resource librarySystemId value

    Returns:
        bool
    """
    found = False
    # NYPL
    if librarySystemId == 1:
        for src in APPROVED_VOCAB["nyp"]:
            if src in value.lower():
                found = True
                break
    elif librarySystemId == 2:
        for src in APPROVED_VOCAB["bpl"]:
            if src in value.lower():
                found = True
                break
    return found


def filter_subject_headings(record: Record, librarySystemId: int) -> List[Field]:
    """
    Removes subject heading tags that are not supported by our systems

    Args:
        record:                     pymarc.record.Record object

    Returns:
        list of tags
    """
    approved_tags = []
    subjects = record.subjects()
    for tag in subjects:
        # LCSH
        if tag.indicator2 == "0":
            approved_tags.append(tag)

        # Children's LCSH
        elif tag.indicator2 == "1":
            if librarySystemId == 1:
                approved_tags.append(tag)
        # source specified in $2
        elif tag.indicator2 == "7":
            src_vocab = tag["2"]
            if src_vocab:
                if is_approved_vacabulary(src_vocab, librarySystemId):
                    approved_tags.append(tag)

        record.remove_field(tag)
    return approved_tags


def prepare_output_record(resource: Resource) -> Record:
    record = response2pymarc(resource.wqueries[0].record)

    material_type = determine_material_type(resource.bibCategoryId)

    # clean up record from data that will not be passed to the catalog
    remove_unwanted_tags(record, material_type)

    # add new tags
    new_tags = []

    # control number (001)
    new_tags.append(
        construct_oclc_control_number_tag(resource.wcn, resource.librarySystemId)
    )

    # call number (091/099)
    new_tags.append(
        construct_callnumber_tag(resource.sierraFormatId, resource.librarySystemId)
    )

    # subject tags (6xx)
    new_tags.extend(filter_subject_headings(record, resource.librarySystemId))

    # content url (856)
    if material_type == "eresources":
        for url_data in resource.urls:
            if (
                url_data.uTypeId == 1
            ):  # what if in rare occasion there is no content url?
                new_tags.append(
                    construct_content_url_tag(url_data.url, url_data.librarySystemId)
                )

    # initials (901/947)
    new_tags.append(construct_initials_tag(resource.librarySystemId))

    # Sierra command line (949)
    new_tags.append(
        construct_sierra_command_tag(
            resource.sbid, resource.sierraFormatId, resource.librarySystemId
        )
    )

    # OverDrive reserve ID tag
    if material_type == "eresources":
        # 019 tag
        if resource.cno:
            new_tags.append(construct_overdrive_control_number_tag(resource.cno))

        # 020 tag
        new_tags.extend(construct_isbn_tags(resource.sbn))

        # 024 tag
        new_tags.extend(construct_upc_tags(resource.sid))

        # 037 tag
        if resource.did is not None:
            new_tags.append(construct_overdrive_reserve_id_tag(resource.did))

        # 710 OverDrive, Inc. tag
        if not has_overdrive_access_point_tag(record):
            new_tags.append(construct_overdrive_access_point_tag())

        # 856 tags
        new_tags.extend(construct_generic_url_tags(resource.urls))

    elif material_type == "print":
        raise NightShiftError("Processing of print materials not implemented yet.")

    # add tags to the record
    for tag in new_tags:
        if tag is not None:
            record.add_ordered_field(tag)

    return record


def name_marc_file(library_system: int, sierraFormatId: int) -> str:
    """
    Names output MARC files

    Args:
        library_system:         library system: 'nyp' or 'bpl'
        sierraFormatId:         datastore Resrouce.sierraFormatId

    returns:
        file_name
    """
    today = datetime.date.today().strftime("%Y%m%d")
    if sierraFormatId == 1:
        sform = "UNKNOWN"
    elif sierraFormatId == 2:
        sform = "eBook"
    elif sierraFormatId == 3:
        sform = "eAudio"
    elif sierraFormatId == 4:
        sform = "eVideo"
    elif sierraFormatId == 5:
        sform = "print"

    return f"{library_system}-{sform}-{today}.mrc"


def save2marc(outfile: str, record: Record) -> None:
    """
    Appends MARC records to outfile

    Args:
        outfile:                    file path
        record:                     MARC record as pymarc object
    """
    try:
        writer = MARCWriter(open(outfile, "ab"))
        writer.write(record)
    except:
        raise
    finally:
        writer.close()
