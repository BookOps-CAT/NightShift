# -*- coding: utf-8 -*-
from datetime import datetime

from nightshift.datastore import (
    SourceFile,
    Library,
    OutputFile,
    Resource,
    ResourceCategory,
    Status,
    WorldcatQuery,
)


def test_Library_tbl_repr():
    assert str(Library(nid=1, code="foo")) == "<Library(nid='1', code='foo')>"


def test_OutputFile_tbl_repr():
    stamp = datetime.now()
    assert (
        str(OutputFile(nid=1, libraryId=2, handle="foo.mrc", timestamp=stamp))
        == f"<OutputFile(nid='1', libraryId='2', handle='foo.mrc', timestamp='{stamp}')>"
    )


def test_Resource_tbl_repr():
    stamp = datetime.now()
    bibDate = stamp.date()
    assert (
        str(
            Resource(
                sierraId=1,
                libraryId=2,
                resourceCategoryId=3,
                archived=False,
                bibDate=bibDate,
                author="foo",
                title="spam",
                pubDate="2021",
                congressNumber="0001",
                controlNumber="0002",
                distributorNumber="0003",
                otherNumber="0004",
                outputId=None,
                sourceId=5,
                srcFieldsToKeep=None,
                standardNumber="0005",
                statusId=6,
                oclcMatchNumber=None,
                upgradeTimestamp=stamp,
            )
        )
        == f"<Resource(sierraId='1', libraryId='2', sourceId='5', resourceCategoryId='3', archived='False', bibDate='{bibDate}', author='foo', title='spam', pubDate='2021', controlNumber='0002', congressNumber='0001', standardNumber='0005', distributorNumber='0003', statusId='6', outputId='None', oclcMatchNumber='None', upgradeTimestamp='{stamp}')>"
    )


def test_ResourceCategory_tbl_repr():
    assert (
        str(ResourceCategory(nid=1, code="foo", description="spam"))
        == "<ResourceCategory(nid='1', code='foo', description='spam')>"
    )


def test_SourceFile_tbl_repr():
    stamp = datetime.now()
    assert (
        str(SourceFile(nid=1, libraryId=2, handle="foo.mrc", timestamp=stamp))
        == f"<SourceFile(nid='1', libraryId='2', handle='foo.mrc', timestamp='{stamp}')>"
    )


def test_Status_tbl_repr():
    assert (
        str(
            Status(
                nid=1,
                name="upgraded-staff",
                description="Upgraded inhouse by staff before bot",
            )
        )
        == "<Status(nid='1', name='upgraded-staff', description='Upgraded inhouse by staff before bot')>"
    )


def test_WorldcatQuery_tbl_repr():
    assert (
        str(
            WorldcatQuery(
                nid=1,
                resourceId=2,
                libraryId=1,
                match=False,
                responseCode="404",
                response=None,
            )
        )
        == "<WorldcatQuery(nid='1', resourceId='2', libraryId='1', match='False', responseCode='404')>"
    )
