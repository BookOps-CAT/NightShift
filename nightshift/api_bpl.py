# -*- coding: utf-8 -*-

"""
This module provides methods for the bot to communicate with BPL Solr platform (Sierra)
"""
from collections import namedtuple
import datetime
import os
from typing import Any, Dict, Iterator, List, Optional, Tuple

from bookops_bpl_solr.session import SolrSession, BookopsSolrError
import requests
from requests import Response

from . import __title__, __version__
from .errors import NightShiftError
from .models import SierraMeta
from .datastore_values import UPGRADE_SRC, URL_TYPE, SIERRA_FORMAT


RESPONSE_FIELDS = ",".join(
    [
        "id",
        "isbn",
        "publishYear",
        "material_type",
        "ss_marc_tag_001",
        "ss_marc_tag_010_a",
        "sm_marc_tag_024_a",
        "econtrolnumber",
        "deleted",
        "title",
        "author_raw",
        "eurl",
        "esampleurl",
        "digital_cover_image",
    ]
)


class SolrResponseReader:
    def __init__(self, response: Response) -> None:
        if response.status_code == requests.codes.ok:
            try:
                self.data = response.json()["response"]["docs"][0]
            except IndexError:
                self.data = None
        else:
            raise NightShiftError(
                f"BPL Solr {response.status_code} error: {response.json()}"
            )

        if self.data is not None:
            self.meta = self._map_data(self.data)
        else:
            self.meta = None

    def _construct_thumbnail_url(self, url: str) -> Optional[str]:
        """Based on a cover image url constructs url for the thumbnail"""
        try:
            idx = url.index("ImageType-100")
            return f"{url[:idx]}ImageType-200{url[idx+13:-7]}200.jpg"
        except ValueError:
            return None

    def _is_deleted(self, data: Dict[str, Any]) -> bool:
        """
        Parses deleted status of the bib

        Args:
            data:           single BPL Solr record

        Returns:
            bool
        """
        return data["deleted"]

    def _is_upgraded(
        self, control_number: str
    ) -> Tuple[Optional[str], Optional[datetime.datetime], bool, Optional[int]]:
        wcn = self._parse_worldcat_number(control_number)
        if wcn:
            upgradeStamp = datetime.datetime.now()
            upgraded = True
            upgradeSourceId = UPGRADE_SRC["staff"]["usid"]
        else:
            upgradeStamp = None
            upgraded = False
            upgradeSourceId = None

        return (wcn, upgradeStamp, upgraded, upgradeSourceId)

    def _map_data(self, data: Dict[str, Any]) -> namedtuple:
        """
        Maps response data into SierraMeta namedtuple

        Args:
            data:               response's first document

        Returns:
            meta
        """
        sbid = self._parse_bib_id(data)
        sbn = self._parse_isbns(data)
        lcn = self._parse_lccn(data)
        did = self._parse_distributor_number(data)
        sid = self._parse_standard_numbers(data)
        deleted = self._is_deleted(data)
        title = self._parse_title(data)
        author = self._parse_author(data)
        sierraFormatId = self._parse_sierra_format_id(data)
        pubDate = self._parse_publication_date(data)
        urls = self._parse_urls(data)

        cno = self._parse_control_number(data)
        wcn, upgradeStamp, upgraded, upgradeSourceId = self._is_upgraded(cno)

        return SierraMeta(
            sbid,
            sbn,
            lcn,
            did,
            sid,
            wcn,
            deleted,
            sierraFormatId,
            title,
            author,
            pubDate,
            upgradeStamp,
            upgraded,
            upgradeSourceId,
            urls,
        )

    def _parse_author(self, data: Dict[str, Any]) -> Optional[str]:
        """Parses author from the response"""
        try:
            return data["author_raw"][:50].lower().strip()
        except KeyError:
            return None

    def _parse_bib_id(self, data: Dict[str, Any]) -> int:
        """Parses Sierra bib number from the response"""
        return int(data["id"])

    def _parse_control_number(self, data: Dict[str, Any]) -> Optional[str]:
        """Parses control number (001 tag) from the response"""
        try:
            return data["ss_marc_tag_001"]
        except KeyError:
            return None

    def _parse_distributor_number(self, data: Dict[str, Any]) -> Optional[str]:
        """Parses distributor number (037$a tag) from the response"""
        try:
            return data["econtrolnumber"]
        except KeyError:
            return None

    def _parse_isbns(self, data: Dict[str, Any]) -> Optional[str]:
        """Parses ISBNs from the response"""
        try:
            return ",".join([i for i in data["isbn"] if "-" not in i])
        except KeyError:
            return None

    def _parse_lccn(self, data: Dict[str, Any]) -> Optional[str]:
        """Parses LCCN (010 tag) from the response"""
        try:
            return data["ss_marc_tag_010_a"].strip()
        except KeyError:
            return None

    def _parse_publication_date(self, data: Dict[str, Any]) -> Optional[str]:
        """Parses publication date from the response"""
        try:
            return str(data["publishYear"])[:10].strip()
        except KeyError:
            return None

    def _parse_title(self, data: Dict[str, Any]) -> str:
        """Parses title from the response"""
        return data["title"][:50].lower().strip()

    def _parse_sierra_format_id(self, data: Dict[str, Any]) -> int:
        """Parses bib's sierra format"""
        try:
            mat_type = data["material_type"].lower()
        except (KeyError, TypeError):
            mat_type = "unknown"

        if mat_type == "book":
            return SIERRA_FORMAT["book"]["sfid"]
        elif mat_type == "ebook":
            return SIERRA_FORMAT["ebook"]["sfid"]
        elif mat_type == "eaudiobook":
            return SIERRA_FORMAT["eaudio"]["sfid"]
        elif mat_type == "evideo":
            return SIERRA_FORMAT["evideo"]["sfid"]
        else:
            return SIERRA_FORMAT["unknown"]["sfid"]

    def _parse_standard_numbers(self, data: Dict[str, Any]) -> Optional[str]:
        """Parses other standard numbers (024$a tag)"""
        try:
            return ",".join(data["sm_marc_tag_024_a"])
        except KeyError:
            return None

    def _parse_urls(self, data: Dict) -> List[Dict[int, str]]:
        """Parses urls from the response"""
        urls = []
        if "eurl" in data:
            urls.append(dict(uTypeId=URL_TYPE["content"]["utid"], url=data["eurl"]))

        if "esampleurl" in data:
            urls.append(
                dict(
                    uTypeId=URL_TYPE["excerpt"]["utid"],
                    url=data["esampleurl"],
                )
            )

        if "digital_cover_image" in data:
            urls.append(
                dict(uTypeId=URL_TYPE["image"]["utid"], url=data["digital_cover_image"])
            )

            # thumbnail must be constructed from cover image
            thumbnail_url = self._construct_thumbnail_url(data["digital_cover_image"])
            if thumbnail_url is not None:
                urls.append(
                    dict(uTypeId=URL_TYPE["thumbnail"]["utid"], url=thumbnail_url)
                )

        return urls

    def _parse_worldcat_number(self, control_number: str) -> Optional[str]:
        """
        Determines if control number is OCLC/Worldcat number and if
        so it returns it as pure digits number

        Args:
            data:           single BPL Solr record

        Returns:
            wcn
        """

        # BPL format includes OCLC prefixes
        if control_number is not None:
            if "ocm" in control_number or "ocn" in control_number:
                return control_number[3:]
            elif "on" == control_number[:2]:
                return control_number[2:]
            else:
                return None
        else:
            return None


def get_bpl_sierra_bib_data(sbids: List[int]) -> Iterator[namedtuple]:
    """
    Queries BPL Solr by Sierra bib number

    Args:
        sbids:                  list of Sierra bib numbers without the "b" prefix

    Yields:
        meta
    """
    with SolrSession(
        authorization=os.environ["solr-client-key"],
        endpoint=os.environ["solr-endpoint"],
        agent=f"{__title__}/{__version__}",
    ) as session:
        for sbid in sbids:
            try:
                response = session.search_bibNo(
                    keyword=sbid,
                    default_response_fields=False,
                    response_fields=RESPONSE_FIELDS,
                )
            except BookopsSolrError as exc:
                raise NightShiftError(f"Solr request error: {exc}")
            try:
                meta = SolrResponseReader(response).meta
            except NightShiftError:
                raise
            else:
                yield meta
