__all__ = "sync"

import enum
import html
import os
import re
import warnings
from contextlib import contextmanager
from typing import Generator, TypedDict, Optional, Any, Literal

from httpx import Client, HTTPStatusError
from sqlalchemy import text
from sqlalchemy.engine import Result, Row
from sqlalchemy.future.engine import (
    Connection,
    create_engine as sqlalchemy_create_engine,
    Engine,
)

import gottem


class PyParcelError(RuntimeError):
    pass


########################################################################################
# API


def sync(parcel_id: str):
    raise RuntimeError("Did you mean to call sync2?")


def sync2(db_conn: Connection, web_client: Client, parcel_id: str):
    response = gaze_owner_info(web_client, parcel_id)
    if response.is_client_error:
        log_client_error(response)
        return
    if response.is_server_error:
        log_gaze_error(response)
        return

    _gaze_info = response.json()
    gaze_owner: Optional[GazeAddress] = _gaze_info["results"]["mailing"]["parsed"]
    gaze_mortgage: Optional[GazeAddress] = _gaze_info["results"]["mortgage"]["parsed"]

    _cog_info: CogDataHolder = cog_get_info(db_conn, parcel_id)
    cog_owner = _cog_info["owner"]
    cog_mortgage = _cog_info["mortgage"]
    #
    update_owner_mailing(db_conn, parcel_id, cog_owner, gaze_owner)
    update_mortgage_mailing(db_conn, parcel_id, cog_mortgage, gaze_mortgage)

    log_info["times_called"] += 1
    print("Rounds completed:\t%s" % log_info["times_called"])


def _compare(gaze: Optional[str], cog: Optional["NullableStr"], cog_variants=None) -> "SyncState":
    if gaze is None:
        if cog is None:
            print("Both Gaze and Cog are None")
            return SyncState.OK
        return SyncState.MISMATCHED
    else:
        if cog is None:
            return SyncState.MISSING

    if gaze.upper() == cog.upper():
        return SyncState.OK

    if cog_variants is not None:
        for v in cog_variants:
            if gaze.upper() == v.upper():
                return SyncState.VARIANT
        return SyncState.MISMATCHED

    # if gaze is None:
    #     if cog is None:
    #         return Difference.EQUIVALENT
    #     raise RuntimeError("COG_EXISTS_BUT_GAZE_DOES_NOT")
    #     return Difference.COG_EXISTS_BUT_GAZE_DOES_NOT
    #
    # if cog is None:
    #     return Difference.GAZE_EXISTS_BUT_COG_DOES_NOT
    #
    # if gaze.upper() == cog.upper():
    #     return Difference.EQUIVALENT
    # if cog_variants and (gaze in cog_variants):
    #     raise RuntimeError("Behavior has this case has not yet been decided upon.")
    #     raise Difference.MATCHED_VARIANT
    # return Difference.MISMATCHED


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
    try:
        return matches.__next__().group(0)
    except StopIteration:
        raise RuntimeError("Check your Gaze username and password.")


def gaze_owner_info(web_client, parcel_id: str, access_token=None):
    """Get property information from gaze's api"""
    access_token = _get_gaze_token(web_client)
    endpoint = _api_base + f"/browser/owner-info/{parcel_id}"
    return web_client.get(
        endpoint,
        headers={"Authorization": access_token},
    )


# DATABASE STUFF
########################################################################################
# Engine
################################
def _create_engine() -> Engine:
    _db_user = "sylvia"
    _db_password = "temppass"
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
    address_data["owner"]["parcel__parcelkey"] = _parcelkey
    address_data["mortgage"]["parcel__parcelidcnty"] = parcel_id
    address_data["mortgage"]["parcel__parcelkey"] = _parcelkey

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

    # Todo: naming. x should be role
    x: Optional[Literal["owner"] | Literal["mortgage"]]
    for row in rows:
        x = None
        if row[1] == _OWNER_MAILING_ROLE_ID:
            x = "owner"
        elif row[1] == "mortgage":
            x = "mortgage"
        if x:
            (address_data[x]["mailingaddress__addressid"], address_data[x]["linkedobjectrole__lordid"]) = values(
                row, length=2
            )

    ## Sanity checks
    # _count = bool(address_data["owner"]) + bool(address_data["mortgage"])
    # assert (not _parcelkey) or (_count > 0)
    # assert len(rows) == _count
    # assert len(rows) < 2

    key: Literal["owner"] | Literal["mortgage"]
    maybe_result: Optional[Row]
    for key in address_data:
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
        maybe_result = result.one_or_none()
        (
            address_data[key]["mailingaddress__bldgno"],
            address_data[key]["mailingstreet__streetid"],
        ) = values(maybe_result, length=2)

        result = conn.execute(
            text(
                "select name, namevariantsarr, citystatezip_cszipid, pobox from mailingstreet"
                "   where streetid=:street_id"
                "   and deactivatedts is null;"
            ),
            {"street_id": address_data[key]["mailingstreet__streetid"]},
        )
        maybe_result = result.one_or_none()
        (
            address_data[key]["mailingstreet__name"],
            address_data[key]["mailingstreet__namevariantsarr"],
            address_data[key]["mailingcitystatezip__id"],
            address_data[key]["mailingstreet__pobox"],
        ) = values(maybe_result, length=4)

        result = conn.execute(
            text(
                "select zip_code, state_abbr, city, list_type, default_state, default_city, default_type from mailingcitystatezip"
                "   where id=:id_"
                "   and deactivatedts is null;"
            ),
            {"id_": address_data[key]["mailingcitystatezip__id"]},
        )
        maybe_result = result.one_or_none()
        (
            address_data[key]["mailingcitystatezip__zip_code"],
            address_data[key]["mailingcitystatezip__state_abbr"],
            address_data[key]["mailingcitystatezip__city"],
            address_data[key]["mailingcitystatezip__list_type"],
            address_data[key]["mailingcitystatezip__default_state"],
            address_data[key]["mailingcitystatezip__default_city"],
            address_data[key]["mailingcitystatezip__default_type"],
        ) = values(maybe_result, length=7)

    return address_data


class _MissingHierarchy:
    PARCEL = 10
    LINKED_ROLE = 20
    BUILDING = 30
    STREET = 40
    CITY = 50
    STATE = 60


class _MismatchedHierarchy:
    PARCEL = 10
    LINKED_ROLE = 20
    # BUILDING_VARIANT = 30
    BUILDING = 30
    STREET = 40
    CITY = 50
    STATE = 60


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
    print(parcel_id, role_id, sep="\t")
    _gaze = _gaze_argument
    if _gaze is None:
        if _cog_argument is None:
            return  # Neither a gaze nor cog address exist. Nothing to update 😊
        # TODO: HANDLE and ensure _compare doesn't rely on cog existing
        _gaze = _new_gaze_address()

    _cog = _cog_argument
    if _cog is None:
        print("Cog address does not exist.")
        _cog = _new_cog_address()

    # TODO: this should be done earlier. This is also confusing to new devs
    # Merge an empty dictionary with the filled in dictionary
    gaze: GazeAddress = _new_gaze_address() | _gaze
    cog: CogAddress = _new_cog_address() | _cog
    warnings.warn("TODO: Determine behavior for gaze['sec_unit_number'] and gaze['sec_unit_type']")

    # sanity check
    assert len(gaze) == len(_new_gaze_address()), gaze

    # First, we do a quick check to see if there are differences
    _gaze_street = " ".join(
        v
        for v in [
            gaze["prefix"],
            gaze["street"],
            gaze["type"],
            gaze["suffix"],
        ]
        if v
    )
    diff: dict[str, "SyncState"] = {
        "parcel_id": _compare(parcel_id, cog["parcel__parcelidcnty"]),
        "linked_object_role": _compare(str(role_id), _optional_int_to_optional_str(cog["linkedobjectrole__lordid"])),
        "building_number": _compare(gaze["number"], cog["mailingaddress__bldgno"]),
        "street": _compare(
            _gaze_street,
            cog["mailingstreet__name"],
            cog_variants="mailingstreet__namevariantsarr",
        ),
        "city": _compare(gaze["city"], cog["mailingcitystatezip__zip_code"]),
        "state": _compare(gaze["state"], cog["mailingcitystatezip__state_abbr"]),
        "zip": _compare(gaze["zip"], cog["mailingcitystatezip__zip_code"]),
    }
    all_ok = all(map(lambda key: key is SyncState.OK, diff.values()))
    if all_ok:
        return

    ## TODO: Determine gaze minimum requirements
    # If any of these are missing in the Gaze, we probably can't continue
    # gaze_minimum = [
    #     gaze["city"].upper(),
    #     gaze["state"].upper(),
    #     gaze["zip"].upper(),
    #     _gaze_street.upper(),
    # ]
    # if any(map(lambda x: not x, gaze_minimum)):
    #     breakpoint()
    #     raise PyParcelError


    # Get the ids soley based on Gaze info,
    try:
        _city = gaze["city"].upper()
        _state = gaze["state"].upper()
        _zip = gaze["zip"].upper()
        _street = _gaze_street.upper()
    except AttributeError:
        print(gaze)
        breakpoint()
        raise AttributeError


    if gaze["number"] is None:
        raise PyParcelError(f"Building is None.\nGaze is {gaze}")

    _building_number = gaze["number"].upper()

    _csz_id = gottem.query_for_mailingcitystatezip_id(conn, _city, _state, _zip)
    if _csz_id is None:
        _csz_id = gottem.write_city_state_zip(conn, _city, _state, _zip)

    _street_id = gottem.query_for_mailingstreet_id(conn, _csz_id, _street)
    if _street_id is None:
        _street_id = gottem.write_street(conn, _csz_id, _street)
        print("wrote street %s" % _street)

    _mailing_address_ids = gottem.query_for_mailing_addresses(conn, _street_id, _building_number)
    if _mailing_address_ids is None:
        _mailing_address_ids = [gottem.write_mailing_address(conn, _street_id, _building_number)]

    result: Result
    to_write = _new_cog_address()
    match diff["parcel_id"]:
        case SyncState.OK:
            pass
        case SyncState.VARIANT:
            raise IllogicalState
        case SyncState.MISSING:
            # Write parcel to database
            raise NotImplementedError
        case SyncState.MISMATCHED:
            raise IllogicalState

    # match diff["linked_object_role"]:
    #     case SyncState.OK:
    #         pass
    #     case SyncState.VARIANT:
    #         pass
    #     case SyncState.MISSING:
    #         to_write["linkedobjectrole__lordid"] = role_id
    #     case SyncState.MISMATCHED(_):
    #         raise IllogicalState
    #
    # match diff["building_number"]:
    #     case SyncState.OK:
    #         pass
    #     case SyncState.VARIANT:
    #         raise IllogicalState
    #     case SyncState.MISSING:
    #         to_write["linkedobjectrole__lordid"] = role_id
    #         to_write["mailingaddress__bldgno"] = gaze["number"]
    #     case SyncState.MISMATCHED:
    #         if not manually_overrode:
    #             deactivate_current_building()
    #         raise NotImplementedError
    #
    # match diff["street"]:
    #     case SyncState.OK:
    #         pass
    #     case SyncState.VARIANT:
    #         raise NotImplementedError
    #     case SyncState.MISSING:
    #         to_write["linkedobjectrole__lordid"] = role_id
    #         to_write["mailingaddress__bldgno"] = gaze["number"]
    #         to_write["mailingstreet__name"] = gaze["street"]
    #     case SyncState.MISMATCHED:
    #         street_exists: bool = check_if_street_still_exists()
    #         if not street_exists:
    #             deactivate_street()
    #         raise NotImplementedError
    #
    # match diff["city"]:
    #     case SyncState.OK:
    #         pass
    #     case SyncState.VARIANT:
    #         raise NotImplementedError
    #     case SyncState.MISSING:
    #         to_write["parcel__parcelkey"] = int(parcel_id)
    #         to_write["linkedobjectrole__lordid"] = role_id
    #         to_write["mailingaddress__bldgno"] = gaze["number"]
    #         to_write["mailingstreet__name"] = gaze["street"]
    #         to_write["mailingcitystatezip__city"] = gaze["city"]
    #         to_write["mailingcitystatezip__state_abbr"] = gaze["state"]
    #         to_write["mailingcitystatezip__state_abbr"] = gaze["zip"]
    #     case SyncState.MISMATCHED:
    #         raise NotImplementedError
    #
    # match diff["state"]:
    #     case SyncState.OK:
    #         pass
    #     case SyncState.VARIANT:
    #         raise NotImplementedError
    #     case SyncState.MISSING:
    #         to_write["parcel__parcelkey"] = int(parcel_id)
    #         to_write["linkedobjectrole__lordid"] = role_id
    #         to_write["mailingaddress__bldgno"] = gaze["number"]
    #         to_write["mailingstreet__name"] = gaze["street"]
    #         to_write["mailingcitystatezip__city"] = gaze["city"]
    #         to_write["mailingcitystatezip__state_abbr"] = gaze["state"]
    #         to_write["mailingcitystatezip__state_abbr"] = gaze["zip"]
    #     case SyncState.MISMATCHED:
    #         raise NotImplementedError
    #
    # match diff["zip"]:
    #     case SyncState.OK:
    #         pass
    #     case SyncState.VARIANT:
    #         raise NotImplementedError
    #     case SyncState.MISSING:
    #         to_write["parcel__parcelkey"] = int(parcel_id)
    #         to_write["linkedobjectrole__lordid"] = role_id
    #         to_write["mailingaddress__bldgno"] = gaze["number"]
    #         to_write["mailingstreet__name"] = gaze["street"]
    #         to_write["mailingcitystatezip__city"] = gaze["city"]
    #         to_write["mailingcitystatezip__state_abbr"] = gaze["state"]
    #         to_write["mailingcitystatezip__state_abbr"] = gaze["zip"]
    #     case SyncState.MISMATCHED:
    #         raise NotImplementedError
    #
    # write_to_db(conn, to_write)


class SyncState(enum.Enum):
    OK = 10
    VARIANT = 19
    MISMATCHED = 20
    MISSING = 30


class IllogicalState(RuntimeError):
    pass


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


# # Old code
# def _handle_write_generic_if_different(conn, info, diff) -> None:
#     if diff is Difference.EQUIVALENT:
#         return
#     if diff is Difference.GAZE_EXISTS_BUT_COG_DOES_NOT:
#         breakpoint()
#         _generic_insert(conn, info)
#     elif diff is Difference.MISMATCHED:
#         _generic_update(conn, info)
#     elif diff is Difference.COG_EXISTS_BUT_GAZE_DOES_NOT:
#         raise RuntimeError()
#     else:
#         raise RuntimeError("Unknown difference.")
#
#
# def _generic_insert(conn, info):
#     """TABLE AND COLUMN VALUES MUST NOT BE SPECIFIED BY THE USER"""
#     table, column, value = info
#     statement = text(f"INSERT INTO {table} ({column}) VALUES (:value);")
#     return conn.execute(statement, {"value": value})


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
import logging

logger = logging.getLogger(__name__)

DASHES = "-" * 89
log_info = {"times_called": 0, "gaze==cog": 0, "gaze!=cog": 0}


def log_gaze_error(r):
    try:
        r.raise_for_status()
    except HTTPStatusError as err:
        logger.error(r.json(), exc_info=err)


def log_client_error(r):
    logger.error(r)


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
    number: Optional[str]
    prefix: Optional[str]
    street: Optional[str]
    type: Optional[str]
    suffix: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip: Optional[str]

    addressee: Optional[str]
    sec_unit_type: Optional[str]
    sec_unit_num: Optional[str]

    # Unused
    plus4: Optional[str]


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
        "addressee": None,
        "sec_unit_type": None,
        "sec_unit_num": None,
        "plus4": None
    }


class CogAddress(TypedDict):
    # As a naming convention, I've chosen map table/column
    #  names 1:1 to their Python counterparts
    #  using the format {description}__{table_name}__{column_name}
    #  I understand that this convention is ugly and unpythonic.
    #  I also understand SQLAlchemy has ORM stuff built in.
    parcel__parcelkey: NullableInt
    parcel__parcelidcnty: Optional[str]

    linkedobjectrole__lordid: Optional[int]

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
        "linkedobjectrole__lordid": None,
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


# fmt: off
########################################################################################
# Other

USA_STATE_ABBREVIATIONS = [
    'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL',
    'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA',
    'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE',
    'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI',
    'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV',
    'WY'
]
""" A local copy of the state abbreviations saves us a trip to the database"""


def _optional_int_to_optional_str(i: Optional[int]) -> Optional[str]:
    if i is None:
        return None
    return str(i)
