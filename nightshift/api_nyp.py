# -*- coding: utf-8 -*-

"""
This module provides methods for the bot to communicate with NYPL Platform (Sierra)
"""
from collections import namedtuple
import datetime
import os
from typing import Dict, List, Tuple, Type

import requests
from bookops_nypl_platform import PlatformToken, PlatformSession
from bookops_nypl_platform.errors import BookopsPlatformError


from . import __version__, __title__
from .errors import NightShiftError
from .datastore_values import UPGRADE_SRC, URL_TYPE


SierraMeta = namedtuple(
    "SierraMeta",
    [
        "sbid",
        "sbn",
        "lcn",
        "did",
        "sid",
        "wcn",
        "title",
        "author",
        "pubDate",
        "upgradeStamp",
        "upgraded",
        "upgradeSourceId",
        "urls",
    ],
)


class PlatformResponseReader:
    def __init__(self, responses: Type[requests.Response]):

        if responses.status_code == requests.codes.ok:
            self.datas = responses.json()["data"]
        elif responses.status_code == 404:
            self.datas = []
        else:
            raise NightShiftError(
                f"Platform {responses.status_code} error: {responses.json()}"
            )

    def _get_variable_field_content(self, tag, subfield, data: Dict) -> str:
        """
        Generator. Parses variable field subfield $a for specified MARC tag

        Args:
            tag:            MARC tag
            data:           single platform record

        Returns:
            content
        """
        content = []
        for field in data["varFields"]:
            if field["marcTag"] == tag:
                for sub in field["subfields"]:
                    if sub["tag"] == subfield:
                        content.append(sub["content"].strip())
        return content

    def _is_upgraded(self, control_number: str) -> Tuple:
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

    def _map_data(self, data: Dict) -> Type[namedtuple]:
        sbid = self._parse_bib_id(data)
        sbn = self._parse_isbns(data)
        lcn = self._parse_lccn(data)
        did = self._parse_distributor_number(data)
        sid = self._parse_standard_numbers(data)
        title = self._parse_title(data)
        author = self._parse_author(data)
        pubDate = self._parse_publication_date(data)
        urls = self._parse_urls(data)

        # determine if record in Sierra upgraded manually
        cno = self._parse_control_number(data)
        wcn, upgradeStamp, upgraded, upgradeSourceId = self._is_upgraded(cno)

        return SierraMeta(
            sbid,
            sbn,
            lcn,
            did,
            sid,
            wcn,
            title,
            author,
            pubDate,
            upgradeStamp,
            upgraded,
            upgradeSourceId,
            urls,
        )

    def _parse_author(self, data: Dict) -> str:
        """
        Parses normalized author

        Args:
            data:           single platform record

        Returns:
            author
        """
        return data["normAuthor"]

    def _parse_bib_id(self, data: Dict) -> int:
        """
        Parses Sierra bib number

        Args:
            data:           single platform record

        Returns:
            sbid
        """
        return int(data["id"])

    def _parse_control_number(self, data: Dict) -> str:
        """
        Parses Sierra control number (tag 001)

        Args:
            data:           single platform record

        Returns:
            cno
        """
        return data["controlNumber"]

    def _parse_distributor_number(self, data: Dict) -> str:
        """
        Parses distributor number (tag 037$a - repeatable but enforce one
        per record)

        Args:
            data:           single platform record

        Returns:
            did:                first encountered distributor number
        """
        did = self._get_variable_field_content("037", "a", data)
        if did:
            return did[0]
        else:
            return None

    def _parse_isbns(self, data: Dict) -> str:
        """
        Parses ISBN (tag 020)

        Args:
            data:           single platform record

        Returns:
            sbn
        """
        isbns = self._get_variable_field_content("020", "a", data)
        if isbns:
            return ",".join(isbns)
        else:
            return None

    def _parse_lccn(self, data: Dict) -> str:
        """
        Parses LCCN from data (tag 010 - is not repeatable)

        Args:
            data:           single platform record

        Returns:
            lcn
        """
        lccn = self._get_variable_field_content("010", "a", data)
        if lccn:
            return lccn[0]
        else:
            return None

    def _parse_publication_date(self, data: Dict) -> str:
        """
        Parses publication date
        """
        return str(data["publishYear"])

    def _parse_standard_numbers(self, data: Dict) -> str:
        """
        Parses standard number (tag 024$a)

        Args:
            data:           single platform record

        Returns:
            sid
        """
        sid = self._get_variable_field_content("024", "a", data)
        if sid:
            return ",".join(sid)
        else:
            return None

    def _parse_title(self, data: Dict) -> str:
        """
        Parses normalized title (tag 245 without subfield $c)

        Args:
            data:           single platform record

        Returns:
            title
        """
        return data["normTitle"]

    def _determine_url_type_id(self, url: str) -> Dict:
        """
        Determines type of url based on the pattern:

        Args:
            url:            url string

        Returns:
            url type id
        """
        if "link.overdrive.com" in url:
            return URL_TYPE["content"]["utid"]
        elif "samples.overdrive.com" in url:
            return URL_TYPE["excerpt"]["utid"]
        elif "ImageType-100" in url:
            return URL_TYPE["image"]["utid"]
        elif "ImageType-200" in url:
            return URL_TYPE["thumbnail"]["utid"]
        else:
            return None

    def _parse_urls(self, data: Dict) -> List[Dict]:
        """
        Parses URLs found in MARC record

        Args:
            data:           single platform record

        Returns:
            list of dicts
        """
        url_data = []
        urls = self._get_variable_field_content("856", "u", data)

        # determine type of url based on the pattern
        for url in urls:
            uid = self._determine_url_type_id(url)
            if uid is None:
                pass
            else:
                url_data.append(dict(urlTypeId=uid, url=url))

        return url_data

    def _parse_worldcat_number(self, control_number: str) -> str:
        """
        Determines if control number is OCLC/Worldcat number and if
        so it returns it as pure digits number

        Args:
            data:           single platform record

        Returns:
            wcn
        """

        # NYPL format includes only digits, no prefixes
        if control_number.isdigit():
            return control_number
        else:
            return None

    def __iter__(self):
        for data in self.datas:
            record = self._map_data(data)
            yield record


def split_into_batches(sbids: List[int], size: int = 50) -> List[List]:
    """
    Splits list of Sierra bib numbers for querying into batches of
    desired size

    Args:
        sbids:                  list of Sierra bib numbers
        size:                   batch size

    Returns:
        batches
    """
    incomplete = True
    batches = []
    start = 0
    end = size
    while incomplete:
        batch = sbids[start:end]
        if not batch:
            incomplete = False
        elif len(batch) < size:
            batches.append(batch)
            incomplete = False
        else:
            batches.append(batch)
            start += size
            end += size

    return batches


def get_access_token():
    try:
        token = PlatformToken(
            client_id=os.environ["platform-client-id"],
            client_secret=os.environ["platform-client-secret"],
            oauth_server=os.environ["platform-oauth-server"],
            agent=f"{__title__}/{__version__}",
        )
    except BookopsPlatformError as exc:
        raise NightShiftError(f"Platform autorization error: {exc}")
    else:
        return token


def get_sierra_bib_data(sbids: List[int]):
    """
    Queries NYPL Platform by Sierra bib number

    Args:
        sbids:                  list of Sierra bib numbers withough "b" prefix
                                or last digit check
        session:                NYPL Platform session
    """

    # split sierra bib numbers into batches of 50
    batches = split_into_batches(sbids)

    token = get_access_token()

    with PlatformSession(
        authorization=token, agent=f"{__title__}/{__version__}"
    ) as session:
        for batch in batches:
            try:
                # include data if bib deleted
                responses = session.get_bib_list(batch, deleted=None)
            except BookopsPlatformError as exc:
                # log errors
                raise NightShiftError(f"Platform request error: {exc}")

            try:
                reader = PlatformResponseReader(responses)
            except NightShiftError:
                raise
            else:
                for meta in reader:
                    yield meta
