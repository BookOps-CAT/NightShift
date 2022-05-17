# -*- coding: utf-8 -*-
from collections import namedtuple
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import and_, create_engine, delete, func, inspect, update
from sqlalchemy.orm import Session

# from nightshift.constants import LIBRARIES, RESOURCE_CATEGORIES
from nightshift import constants
from nightshift.datastore import (
    DataAccessLayer,
    Event,
    Library,
    OutputFile,
    Resource,
    ResourceCategory,
    RottenApple,
    RottenAppleResource,
    SourceFile,
    WorldcatQuery,
)

ResCatById = namedtuple(
    "ResCatById",
    [
        "name",
        "sierraBibFormatBpl",
        "sierraBibFormatNyp",
        "srcTags2Keep",
        "dstTags2Delete",
        "queryDays",
    ],
)

ResCatByName = namedtuple(
    "ResCatByName",
    [
        "nid",
        "sierraBibFormatBpl",
        "sierraBibFormatNyp",
        "srcTags2Keep",
        "dstTags2Delete",
        "queryDays",
    ],
)


def init_db() -> None:
    """
    Initiates the database and prepopulates needed tables

    Args:
        session:                `sqlalchemy.Session` instance
    """
    # make sure to start from scratch
    dal = DataAccessLayer()

    dal.engine = create_engine(dal.conn)
    dal.connect()
    session = dal.Session()

    # recreate schema & prepopulate needed tables
    for k, v in constants.LIBRARIES.items():
        session.add(Library(nid=v["nid"], code=k))

    for k, v in constants.RESOURCE_CATEGORIES.items():
        session.add(
            ResourceCategory(
                nid=v["nid"],
                name=k,
                description=v["description"],
                sierraBibFormatBpl=v["sierraBibFormatBpl"],
                sierraBibFormatNyp=v["sierraBibFormatNyp"],
                srcTags2Keep=v["srcTags2Keep"],
                dstTags2Delete=v["dstTags2Delete"],
                queryDays=v["queryDays"],
            ),
        )

    for code, resource_cat_ids in constants.ROTTEN_APPLES.items():
        ids = [
            RottenAppleResource(
                resourceCategoryId=constants.RESOURCE_CATEGORIES[n]["nid"]
            )
            for n in resource_cat_ids
        ]
        session.add(RottenApple(code=code, applicableResourceIds=ids))

    session.commit()

    # verify integrity of the database
    try:
        # check all tables were created
        insp = inspect(dal.engine)
        assert sorted(insp.get_table_names()) == sorted(
            [
                "event",
                "library",
                "output_file",
                "resource",
                "resource_category",
                "rotten_apple",
                "rotten_apple_resource",
                "source_file",
                "worldcat_query",
            ]
        ), "Database is missing requried tables."

        # check both libraries were added
        libraries = session.query(Library).all()
        assert len(libraries) == 2, "Invalid number of initial libraries."
        codes = [row.code for row in libraries]
        assert "NYP" in codes, "'NYP' code missing in 'Library' table."
        assert "BPL" in codes, "'BPL' code missing in 'Library' table."

        # check e-resource names in ResourceCategory table
        categories = session.query(ResourceCategory).all()
        names = [row.name for row in categories]
        assert len(categories) == 11, "Invalid number of 'ResourceCategory' records."
        assert "ebook" in names, "Missing 'ebook' category in 'ResourceCategory' table."
        assert (
            "eaudio" in names
        ), "Missing 'eaudio' category in 'ResourceCategory' table."
        assert (
            "evideo" in names
        ), "Missing 'evideo' category in 'ResourceCategory' table."
    except AssertionError:
        raise
    finally:
        session.close()


def add_event(session: Session, resource: Resource, status: str) -> Optional[Event]:
    """
    Inserts an event row.

    Args:
        session:                `sqlalchemy.Session` instance
        resource:               datastore Resource record
        status:                 one of `datastore.Event.outcome` enum values:
                                    'expired',
                                    'staff_enhanced',
                                    'staff_deleted',
                                    'bot_enhanced',
                                    'worldcat_hit',
                                    'worldcat_miss',

    Returns:
        `nightshift.datastore.Event` instance
    """
    instance = insert_or_ignore(
        session,
        Event,
        libraryId=resource.libraryId,
        sierraId=resource.sierraId,
        bibDate=resource.bibDate,
        resourceCategoryId=resource.resourceCategoryId,
        status=status,
    )
    return instance


def add_output_file(
    session: Session, libraryId: int, file_handle: str
) -> Optional[OutputFile]:
    """
    Adds OutputFile record to db.

    Args:
        session:                `sqlalchemy.Session` instance
        libaryId:               datastore Library record nid
        file_handle:            handle of the out file

    Returns:
        `nightshift.datastore.OutputFile` instance
    """
    instance = insert_or_ignore(
        session, OutputFile, libraryId=libraryId, handle=file_handle
    )
    session.flush()
    return instance


def add_resource(session: Session, resource: Resource) -> Optional[Resource]:
    """
    Adds Resource record to db.

    Args:
        session:                `sqlalchemy.Session` instance
        resource:               `datastore.Resource` object

    Returns:
        `Resource` instance updated with `nid`
    """
    instance = (
        session.query(Resource)
        .filter_by(sierraId=resource.sierraId, libraryId=resource.libraryId)
        .one_or_none()
    )
    if not instance:
        session.add(resource)
        session.flush()
        return resource
    else:
        return None


def add_source_file(
    session: Session, libraryId: int, handle: str
) -> Optional[SourceFile]:
    """
    Adds SourceFile record to db.

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              db library id
        handle:                 marc file handle

    Returns:
        `SourceFile` instance
    """
    instance = insert_or_ignore(session, SourceFile, libraryId=libraryId, handle=handle)
    session.flush()
    return instance


def delete_resources(session: Session, resourceCategoryId: int, age: int) -> int:
    """
    Deletes resources from the database based on category and days since
    bib creation in Sierra.

    Args:
        session:                `sqlalchemy.Session` instance
        resourceCategoryId:     `nightshift.datastore.ResourceCategory.nid` identifier
        age:                    number of days since bib created in Sierra

    Returns:
        number of deleted rows in the database
    """
    rowcount = (
        session.query(Resource)
        .filter(
            Resource.resourceCategoryId == resourceCategoryId,
            Resource.bibDate < datetime.utcnow() - timedelta(days=age),
        )
        .delete()
    )
    return rowcount


def insert_or_ignore(session, model, **kwargs):
    """
    Adds a new record to given table (model) or ignores if the same.

    Args:
        session:                `sqlalchemy.Session` instance
        model:                  one of datastore table classes
        kwargs:                 new record values as dictionary

    Returns:
        instance of inserted or duplicate record
    """
    instance = session.query(model).filter_by(**kwargs).one_or_none()
    if not instance:
        instance = model(**kwargs)
        session.add(instance)
        return instance
    else:
        return None


def library_by_id(session: Session) -> dict[int, str]:
    """
    Creates a dictionary where the key is `datastore.Library.nid` and a value is the
    library code.

    Args:
        session:                `sqlalchemy.Session` instance

    Returns:
        dict of `nid` and library `codes`
    """
    instances = session.query(Library).all()
    return {i.nid: i.code for i in instances}


def parse_query_days(query_days: str) -> list[tuple[int, int]]:
    """
    Parses query days stored as strings in the datastore
    into proper format.

    Args:
        query_days:             `datastore.ResourceCategory.queryDays` value

    Returns:
        list of day periods tuples
    """
    periods = []

    values = query_days.split(",")
    for v in values:
        periods.append(tuple([int(x) for x in v.split("-")]))

    return periods


def resource_category_by_name(session: Session) -> dict[str, ResCatByName]:
    """
    Creates a dictionary of resource categories with names as the key.

    Args:
        session:                `sqlalchemy.Session` instance

    Returns:
        dict of `names` and resource category data as values
    """
    instances = session.query(ResourceCategory).all()
    data = dict()
    for i in instances:
        queryDays = parse_query_days(i.queryDays)
        srcTags2Keep = i.srcTags2Keep.split(",")
        dstTags2Delete = i.dstTags2Delete.split(",")

        data[i.name] = ResCatByName(
            i.nid,
            i.sierraBibFormatBpl,
            i.sierraBibFormatNyp,
            srcTags2Keep,
            dstTags2Delete,
            queryDays,
        )
    return data


def retrieve_expired_resources(
    session: Session, resourceCategoryId: int, expiration_age: int
) -> list[Resource]:
    """
    Retrieves resources for a particular category specified by resourceCategoryId that
    expired accoring to the schedule from the `constants.RESOURCE_CATEGORIES`
    query_days.

    Args:
        session:                `sqlalchemy.Session` instance
        resourceCategoryId:     `nightshift.datastore.ResourceCategory.nid` value
        expiration_age:         age in days since bib in Sierra was created

    Returns:
        list of matching query `Resource` instances
    """
    resources = (
        session.query(Resource)
        .filter(
            Resource.resourceCategoryId == resourceCategoryId,
            Resource.status == "open",
            Resource.bibDate
            < datetime.utcnow().date() - timedelta(days=expiration_age),
        )
        .all()
    )
    return resources


def retrieve_new_resources(session: Session, libraryId: int) -> list[Resource]:
    """
    Retrieves resources that have been added to the db
    but has not been processed yet.
    The results are grouped by the resource category and ordered by
    resource nid.

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `Library.nid`

    Returns:
        list of `Resource` instances
    """
    resources = (
        session.query(Resource)
        .filter_by(libraryId=libraryId, status="open", queries=None)
        .order_by(Resource.resourceCategoryId, Resource.nid)
        .all()
    )
    return resources


def retrieve_open_older_resources(
    session: Session, libraryId: int, resourceCategoryId: int, minAge: int, maxAge: int
) -> list[Resource]:
    """
    Queries resources with open status that has not been queried in WorldCat
    betweeen minAge and maxAge.

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              library id
        resourceCategoryId:     resource category id
        minAge:                 min number of days since bib creation date
        maxAge:                 max numb of days since bib creation date

    Returns:
        list of `Row` instances
    """

    subq = (
        session.query(Resource, func.max(WorldcatQuery.timestamp).label("last_query"))
        .join(WorldcatQuery)
        .filter(
            Resource.libraryId == libraryId,
            Resource.resourceCategoryId == resourceCategoryId,
            Resource.status == "open",
            Resource.oclcMatchNumber == None,
            Resource.bibDate > datetime.utcnow() - timedelta(days=maxAge),
        )
        .group_by(Resource.nid)
        .subquery()
    )

    resources = (
        session.query(Resource)
        .join(subq, and_(Resource.nid == subq.c.nid))
        .filter(
            subq.c.last_query < Resource.bibDate + timedelta(days=minAge),
        )
        .all()
    )
    return resources


def retrieve_open_matched_resources_without_full_bib(
    session: Session, libraryId: int
) -> list[Resource]:
    """
    Retrieves resources that have been matched to records in WorldCat,
    but not upgraded yet.
    The results are grouped by resource category and ordered by resource nid

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `Library.nid`

    Returns:
        list of `Resource` instances
    """
    resources = (
        session.query(Resource)
        .filter(
            Resource.libraryId == libraryId,
            Resource.status == "open",
            Resource.oclcMatchNumber != None,
            Resource.fullBib == None,
        )
        .order_by(Resource.resourceCategoryId, Resource.nid)
        .all()
    )
    return resources


def retrieve_open_matched_resources_with_full_bib_obtained(
    session: Session, libraryId: int, resourceCategoryId: int
) -> list[Resource]:
    """
    Retrieves resources that include MARC XML with full bib

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `nightshift.datastore.Library.nid`
        resourceCategoryId:     `nightshift.datastore.ResourceCategory.nid`

    Returns:
        list of `Resource` instances
    """
    resources = (
        session.query(Resource)
        .filter(
            Resource.libraryId == libraryId,
            Resource.resourceCategoryId == resourceCategoryId,
            Resource.status == "open",
            Resource.fullBib.isnot(None),
        )
        .all()
    )
    return resources


def retrieve_processed_files(session: Session, libraryId: int) -> list[str]:
    """
    Retrieves file handles of all processed files for specific library.

    Args:
        session:                `sqlalchemy.Session` instance
        libraryId:              `nightshift.datastore.Library.nid`

    Returns:
        list of file handles
    """
    instances = session.query(SourceFile.handle).filter_by(libraryId=libraryId).all()
    return [instance[0] for instance in instances]


def retrieve_rotten_apples(session: Session) -> dict[int, list[str]]:
    """
    Retrieves OCLC codes of organizations that result should be excluded from

    Args:
        session:                `sqlalchemy.Session` instance

    Returns:
        dictionary of resource category ids and list of org codes
    """
    results = (
        session.query(RottenAppleResource.resourceCategoryId, RottenApple.code)
        .join(RottenApple, RottenApple.nid == RottenAppleResource.rottenAppleId)
        .all()
    )

    rotten_apples = dict()

    for r in results:
        if r.resourceCategoryId in rotten_apples:
            codes = rotten_apples[r.resourceCategoryId]
            codes.append(r.code)
            rotten_apples[r.resourceCategoryId] = codes
        else:
            rotten_apples[r.resourceCategoryId] = [r.code]

    return rotten_apples


def set_resources_to_expired(
    session: Session, resourceCategoryId: int, age: int
) -> int:
    """
    Updates status from 'open' to 'expired' in resources with given category
    and age in days since bib created in Sierra.

    Args:
        session:                `sqlalchemy.Session` instance
        resourceCategoryId:     `nightshift.datastore.ResourceCategory.nid` identifier
        age:                    number of days since bib created in Sierra

    Returns:
        number of updated rows in the database
    """
    rowcount = (
        session.query(Resource)
        .filter(
            Resource.resourceCategoryId == resourceCategoryId,
            Resource.status == "open",
            Resource.bibDate < datetime.utcnow().date() - timedelta(days=age),
        )
        .update({"status": "expired"})
    )
    return rowcount


def update_resource(session, sierraId, libraryId, **kwargs) -> Optional[Resource]:
    """
    Updates Resource record.

    Args:
        session:                `sqlalchemy.Session` instance
        sierraId:               sierra 8 digit bib # (without prefix
                                or check digit)
        libraryId:              datastore.Library.nid
        kwargs:                 Resource table values to be updated as dictionary
    Returns:
        instance of updated record
    """
    instance = (
        session.query(Resource)
        .filter_by(sierraId=sierraId, libraryId=libraryId)
        .one_or_none()
    )
    if instance:
        for key, value in kwargs.items():
            setattr(instance, key, value)
        return instance
    else:
        return None
