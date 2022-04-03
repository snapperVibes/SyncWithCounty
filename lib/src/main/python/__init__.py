__all__ = "sync"

import enum
import html
import os
import re
from contextlib import contextmanager
from typing import Generator, TypedDict, Optional, Any, Literal

from httpx import Client
from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.future.engine import (
    Connection,
    create_engine as sqlalchemy_create_engine,
    Engine,
)


########################################################################################
# API


def sync(parcel_id: str):
    raise RuntimeError("Did you mean to call sync2?")


def sync2(db_conn: Connection, web_client: Client, parcel_id: str):
    _gaze_info = gaze_owner_info(web_client, parcel_id)
    gaze_owner: Optional[GazeAddress] = _gaze_info["results"]["mailing"]["parsed"]
    gaze_mortgage: Optional[GazeAddress] = _gaze_info["results"]["mortgage"]["parsed"]

    _cog_info: CogDataHolder = cog_get_info(db_conn, parcel_id)
    cog_owner = _cog_info["owner"]
    cog_mortgage = _cog_info["mortgage"]

    update_owner_mailing(db_conn, parcel_id, cog_owner, gaze_owner)
    update_mortgage_mailing(db_conn, parcel_id, cog_mortgage, gaze_mortgage)

    log_info["times_called"] += 1
    print("Times called:\t%s" % log_info["times_called"])


def _compare(gaze: Optional[str], cog: Optional["NullableStr"], cog_variants=None) -> "Difference":
    """Returns true if there are differences"""
    if gaze is None:
        if cog is None:
            return Difference.EQUIVALENT
        raise RuntimeError("COG_EXISTS_BUT_GAZE_DOES_NOT")
        return Difference.COG_EXISTS_BUT_GAZE_DOES_NOT

    if cog is None:
        return Difference.GAZE_EXISTS_BUT_COG_DOES_NOT

    if gaze.upper() == cog.upper():
        return Difference.EQUIVALENT
    if cog_variants and (gaze in cog_variants):
        raise RuntimeError("Behavior has this case has not yet been decided upon.")
        raise Difference.MATCHED_VARIANT
    return Difference.MISMATCHED


# Gaze API
########################################################################################
_api_base = "https://corpsearch-api.pittsburghhousing.org"
_gaze_username = os.getenv("GAZE_USERNAME", "")
_gaze_password = os.getenv("GAZE_PASSWORD", "")

_gaze_token: dict[str, Optional[str]] = {"token": None}


def _get_gaze_token(web_client) -> str:
    if not _gaze_token["token"]:
        _gaze_token["token"] = get_token(web_client)
    return _gaze_token["token"]


def get_token(web_client):
    login_response = web_client.get(_api_base + "/auth/login")
    # Response contains a url
    accounts_url = login_response.content
    accounts_response = web_client.get(accounts_url.decode("utf-8"))
    pattern = re.compile(rb'.*action="(.*?)"')
    matches = re.finditer(pattern, accounts_response.content)
    match = matches.__next__()
    authenticate_url = match.group(1).decode("utf-8")
    authenticate_url = html.unescape(authenticate_url)
    auth_response = web_client.post(
        authenticate_url,
        data={
            "username": _gaze_username,
            "password": _gaze_password,
            "credentialId": "",
        },
        follow_redirects=True,
    )
    auth_html = auth_response.content.decode("utf-8")
    pattern = re.compile(r"eyJ.*")
    matches = re.finditer(pattern, auth_html)
    return matches.__next__().group(0)


def gaze_owner_info(web_client, parcel_id: str, access_token=None) -> dict:
    """Get property information from gaze's api"""
    access_token = _get_gaze_token(web_client)
    endpoint = _api_base + f"/browser/owner-info/{parcel_id}"
    response = web_client.get(
        endpoint,
        params={"parcelId": parcel_id},
        headers={"Authorization": access_token},
    )
    response.raise_for_status()
    return response.json()


# DATABASE STUFF
########################################################################################
# Engine
################################
def _create_engine() -> Engine:
    _db_user = "sylvia"
    _db_password = "changeme"
    _host = "127.0.0.1"
    _port = "5432"
    _db_name = "cogdb"
    _engine_params = f"postgresql+psycopg2://{_db_user}:{_db_password}@{_host}:{_port}/{_db_name}"
    return sqlalchemy_create_engine(_engine_params)


_db = _create_engine()


@contextmanager
def get_db() -> Generator[Connection, None, None]:
    with _db.connect() as connection:
        yield connection


# Queries
################################
_OWNER_MAILING_ROLE_ID = 233
_MORTGAGE_MAILING_ROLE_ID = 234
# assert _MAILING_ROLE_ID == _querey_mailing_role_id()
# assert _MORTGAGE_ROLE_ID == _querey_mortage_role_id()


def values(vals: Optional[Any], /, length: int):
    # Todo: naming. I just want to get this done
    if vals is None:
        return [None for _ in range(length)]
    return [_none_to_null(v) for v in vals]


def _none_to_null(v: Any):
    if v is None:
        return Null
    return v


def cog_get_info(conn: Connection, parcel_id: str) -> "CogDataHolder":
    address_data: CogDataHolder = {
        "owner": _new_cog_address(),
        "mortgage": _new_cog_address(),
    }

    result: Result = conn.execute(
        text("select parcelkey from parcel where parcelidcnty=:parcel_id and deactivatedts is null;"),
        parameters={"parcel_id": parcel_id},
    )
    _parcelkey: int = result.scalar_one()
    if not _parcelkey:
        raise RuntimeError("No parcel key")
    address_data["owner"]["parcel__parcelidcnty"] = parcel_id
    address_data["mortgage"]["parcel__parcelidcnty"] = parcel_id

    # Assigns parcel__parcelkey and mailingaddress__addressid
    result = conn.execute(
        text(
            "select mailingaddress_addressid, linkedobjectrole_lorid from parcelmailingaddress"
            "   where parcel_parcelkey=:parcel_key"
            "   and deactivatedts IS NULL"
            "   and linkedobjectrole_lorid IN (:mailing_role, :mortage_role);"
        ).bindparams(mailing_role=_OWNER_MAILING_ROLE_ID, mortage_role=_MORTGAGE_MAILING_ROLE_ID),
        parameters={"parcel_key": _parcelkey},
    )
    rows = result.all()
    for row in rows:
        # row[1] is linkedobjectrole_lorid
        if row[1] == _OWNER_MAILING_ROLE_ID:
            (
                address_data["owner"]["parcel__parcelkey"],
                address_data["owner"]["mailingaddress__addressid"],
            ) = values(row, length=2)
        if row[1] == _MORTGAGE_MAILING_ROLE_ID:
            (
                address_data["mortgage"]["parcel__parcelkey"],
                address_data["mortgage"]["mailingaddress__addressid"],
            ) = values(row, length=2)

    ## Sanity checks
    # _count = bool(address_data["owner"]) + bool(address_data["mortgage"])
    # assert (not _parcelkey) or (_count > 0)
    # assert len(rows) == _count
    # assert len(rows) < 2

    for key in address_data:
        key: Literal["owner"] | Literal["mortgage"]
        if address_data[key] is None:
            continue
        result = conn.execute(
            text(
                "select bldgno, street_streetid from mailingaddress"
                "   where addressid=:address_id"
                "   and deactivatedts is null;",
            ),
            {"address_id": address_data[key]["mailingaddress__addressid"]},
        )
        x = result.one_or_none()
        (
            address_data[key]["mailingaddress__bldgno"],
            address_data[key]["mailingstreet__streetid"],
        ) = values(x, length=2)

        result = conn.execute(
            text(
                "select name, namevariantsarr, citystatezip_cszipid, pobox from mailingstreet"
                "   where streetid=:street_id"
                "   and deactivatedts is null;"
            ),
            {"street_id": address_data[key]["mailingstreet__streetid"]},
        )
        x = result.one_or_none()
        (
            address_data[key]["mailingstreet__name"],
            address_data[key]["mailingstreet__namevariantsarr"],
            address_data[key]["mailingcitystatezip__id"],
            address_data[key]["mailingstreet__pobox"],
        ) = values(x, length=4)

        result = conn.execute(
            text(
                "select zip_code, state_abbr, city, list_type, default_state, default_city, default_type from mailingcitystatezip"
                "   where id=:id_"
                "   and deactivatedts is null;"
            ),
            {"id_": address_data[key]["mailingcitystatezip__id"]},
        )
        x = result.one_or_none()
        (
            address_data[key]["mailingcitystatezip__zip_code"],
            address_data[key]["mailingcitystatezip__state_abbr"],
            address_data[key]["mailingcitystatezip__city"],
            address_data[key]["mailingcitystatezip__list_type"],
            address_data[key]["mailingcitystatezip__default_state"],
            address_data[key]["mailingcitystatezip__default_city"],
            address_data[key]["mailingcitystatezip__default_type"],
        ) = values(x, length=7)

    return address_data

class NotFoundHierarchy:
    PARCEL_NOT_FOUND = 10
    LINKED_ROLE_NOT_FOUND = 20
    BUILDING_NOT_FOUND = 30
    STREET_NOT_FOUND = 40
    CITY_NOT_FOUND = 50
    STATE_NOT_FOUND = 60


class MismatchedHierarchy:
    PARCEL_MISMATCHED = 10
    LINKED_ROLE_MISMATCHED = 20
    # BUILDING_VARIANT_MISMATCHED = 30
    BUILDING_MISMATCHED = 30
    STREET_MISMATCHED = 40
    CITY_MISMATCHED = 50
    STATE_MISMATCHED = 60


class CogState(enum.Enum):
    MATCHES_GAZE = enum.auto()
    PARCEL_NOT_FOUND = enum.auto()

    # For example, if gaze has owner and mortgage
    #  while the db only has owner
    PARCEL_WITH_LINKED_ROLE_NOT_FOUND = enum.auto()

    # These imply a working linked role
    PARCEL_WITH_LINKED_ROLE_BUILDING_MISMATCHED = enum.auto()
    PARCEL_WITH_LINKED_ROLE_STREET_MISMATCHED = enum.auto()
    PARCEL_WITH_LINKED_ROLE_STREET_VARIENT = enum.auto()
    PARCEL_WITH_LINKED_ROLE_STREET_NOT_FOUND = enum.auto()
    PARCEL_WITH_LINKED_ROLE_MISMATCHED_CITY = enum.auto()
    PARCEL_WITH_LINKED_ROLE_CITY_NOT_FOUND = enum.auto()
    PARCEL_WITH_LINKED_ROLE_MISMATCHED_STATE = enum.auto()
    # If this is the case (and this Python is correct) then we as a nation added a new state
    # or Gaze has an error
    PARCEL_WITH_LINKED_ROLE_STATE_NOT_FOUND = enum.auto()





# INSERTS / UPDATES
################################

# Todo: rename.
def _update_mailing(
    conn: Connection,
    parcel_id: str,
    _cog_argument: Optional["CogAddress"],
    _gaze_argument: Optional["GazeAddress"],
    role_id: int,
) -> None:
    _gaze = _gaze_argument
    if _gaze is None:
        if _cog_argument is None:
            return  # Neither a gaze nor cog address exist. Nothing to update 😊
        # TODO: HANDLE and ensure _compare doesn't rely on cog existing
        _gaze = _new_gaze_address()

    _cog = _cog_argument
    if _cog is None:
        print("Cog address does not exist. Creating address")
        _cog = _new_cog_address()

    # TODO: this should be done earlier. This is also confusing to new devs
    # Merge an empty dictionary with the filled in dictionary
    g: GazeAddress = _new_gaze_address() | _gaze
    c: CogAddress = _new_gaze_address() | _cog

    diff = _compare(parcel_id, c["parcel__parcelidcnty"])
    # Todo: explanation
    if diff is not Difference.COG_EXISTS_BUT_GAZE_DOES_NOT:
        write_parcel_id_if_different(conn, parcel_id, diff=diff)

    diff = _compare(g["number"], c["mailingaddress__bldgno"])
    write_building_if_different(conn, g["number"], diff=diff)
    #
    # _gaze_street = " ".join(
    #     v
    #     for v in [
    #         g["prefix"],
    #         g["street"],
    #         g["type"],
    #         g["suffix"],
    #     ]
    #     if v
    # )
    # diff = _compare(
    #     _gaze_street,
    #     "mailingstreet__name",
    #     cog_variants="mailingstreet__namevariantsarr",
    # )
    # write_street_if_different(conn, street, diff=diff)
    #
    # diff = _compare(g["city"], c["mailingcitystatezip__zip_code"])
    # write_city_if_different(conn, g["city"], diff=diff)
    #
    # diff = _compare(g["state"], c["mailingcitystatezip__state_abbr"])
    # write_state_if_different(conn, g["state"], diff=diff)
    #
    # diff = _compare(g["zip"], c["mailingcitystatezip__zip_code"])
    # write_zip_if_different(conn, g["zip"], diff=diff)
    #
    # raise RuntimeError


def write_parcel_id_if_different(conn, parcel_id, diff):
    # TODO: This currently breaks because it requires the municode
    return _handle_write_generic_if_different(conn, ("parcel", "parcelidcnty", parcel_id), diff)


def write_building_if_different(conn, number, diff):

    breakpoint()
    raise NotImplementedError


def write_street_if_different(conn, street, diff):
    raise NotImplementedError


def write_city_if_different(conn, city, diff):
    raise NotImplementedError


def write_state_if_different(conn, state, diff):
    raise NotImplementedError


def write_zip_if_different(conn, zip_, diff):
    raise NotImplementedError


def _handle_write_generic_if_different(conn, info, diff) -> None:
    if diff is Difference.EQUIVALENT:
        return
    if diff is Difference.GAZE_EXISTS_BUT_COG_DOES_NOT:
        breakpoint()
        _generic_insert(conn, info)
    elif diff is Difference.MISMATCHED:
        _generic_update(conn, info)
    elif diff is Difference.COG_EXISTS_BUT_GAZE_DOES_NOT:
        raise RuntimeError()
    else:
        raise RuntimeError("Unknown difference.")


def _generic_insert(conn, info):
    """TABLE AND COLUMN VALUES MUST NOT BE SPECIFIED BY THE USER"""
    table, column, value = info
    statement = text(f"INSERT INTO {table} ({column}) VALUES (:value);")
    return conn.execute(statement, {"value": value})


def update_owner_mailing(
    conn: Connection,
    parcel_id: str,
    cog: Optional["CogAddress"],
    gaze: Optional["GazeAddress"],
):
    return _update_mailing(conn, parcel_id, cog, gaze, _OWNER_MAILING_ROLE_ID)


def update_mortgage_mailing(conn, parcel_id, cog, gaze):
    return _update_mailing(conn, parcel_id, cog, gaze, _MORTGAGE_MAILING_ROLE_ID)


########################################################################################
# Logging

DASHES = "-" * 89
log_info = {"times_called": 0, "gaze==cog": 0, "gaze!=cog": 0}


########################################################################################
# Types


class _NullType:
    """Represents a "null" value in the database.
    Different from None, which represents when the database contains no row"""

    def __repr__(self):
        return "Null"

    def __bool__(self):
        return False


Null = _NullType()
# TODO: I've currently (and incorrectly) typed each column as Nullable. Fix
NullableBool = bool | _NullType
NullableInt = int | _NullType
NullableStr = str | _NullType
NullableListOfStr = list[str] | _NullType


class CogDataHolder(TypedDict):
    owner: "CogAddress"
    mortgage: "CogAddress"


class GazeAddress(TypedDict):
    number: Optional[NullableStr]
    prefix: Optional[NullableStr]
    street: Optional[NullableStr]
    type: Optional[NullableStr]
    suffix: Optional[NullableStr]
    city: Optional[NullableStr]
    state: Optional[NullableStr]
    zip: Optional[NullableStr]


def _new_gaze_address() -> GazeAddress:
    return {
        "number": None,
        "prefix": None,
        "street": None,
        "type": None,
        "suffix": None,
        "city": None,
        "state": None,
        "zip": None,
    }


class CogAddress(TypedDict):
    # As a naming convention, I've chosen map table/column
    #  names 1:1 to their Python counterparts
    #  using the format {description}__{table_name}__{column_name}
    #  I understand that this convention is ugly and unpythonic.
    #  I also understand SQLAlchemy has ORM stuff built in.
    parcel__parcelkey: NullableInt
    parcel__parcelidcnty: Optional[str]

    mailingaddress__addressid: Optional[NullableInt]
    mailingaddress__bldgno: Optional[NullableStr]

    mailingstreet__streetid: Optional[NullableInt]
    mailingstreet__name: Optional[NullableStr]
    mailingstreet__namevariantsarr: Optional[NullableListOfStr]
    mailingstreet__pobox: Optional[NullableBool]

    mailingcitystatezip__id: Optional[NullableInt]
    mailingcitystatezip__zip_code: Optional[NullableStr]
    mailingcitystatezip__state_abbr: Optional[NullableStr]
    mailingcitystatezip__city: Optional[NullableStr]
    mailingcitystatezip__list_type: Optional[NullableStr]
    mailingcitystatezip__default_state: Optional[NullableStr]
    mailingcitystatezip__default_city: Optional[NullableStr]
    mailingcitystatezip__default_type: Optional[NullableStr]


def _new_cog_address() -> "CogAddress":
    return {
        "parcel__parcelkey": None,
        "parcel__parcelidcnty": None,
        "mailingaddress__addressid": None,
        "mailingaddress__bldgno": None,
        "mailingstreet__streetid": None,
        "mailingstreet__name": None,
        "mailingstreet__namevariantsarr": None,
        "mailingstreet__pobox": None,
        "mailingcitystatezip__id": None,
        "mailingcitystatezip__zip_code": None,
        "mailingcitystatezip__state_abbr": None,
        "mailingcitystatezip__city": None,
        "mailingcitystatezip__list_type": None,
        "mailingcitystatezip__default_state": None,
        "mailingcitystatezip__default_city": None,
        "mailingcitystatezip__default_type": None,
    }
