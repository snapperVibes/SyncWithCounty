import enum
from contextlib import contextmanager
from typing import TypedDict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.future import create_engine, Connection
from sqlalchemy.exc import NoResultFound, MultipleResultsFound

_db_user = "sylvia"
_db_password = "temppass"
_host = "localhost"
_port = "5432"
_db_name = "cogdb"
_engine_params = (
    f"postgresql+psycopg2://{_db_user}:{_db_password}@{_host}:{_port}/{_db_name}"
)
_db = create_engine(_engine_params)


@contextmanager
def get_db():
    with _db.connect() as conn:
        yield conn


class Err(enum.Enum):
    NO_RESULT_FOUND = enum.auto()

    NO_MAILING_ADDRESS_FOUND = enum.auto()

    CITY_STATE_ZIP_NOT_FOUND = enum.auto()

    NO_GAZE_MAILING_ADDRESS = enum.auto()


class CogDbAddress(TypedDict):
    bldgno: str
    street_name: str
    pobox: Optional[bool]
    city: str
    state: str
    zip_: str


def _get_parcel_id_pk(conn, parcel_id):
    r: Result = conn.execute(
        text(
            "SELECT parcelkey from parcel where parcelidcnty=:parcel_id AND deactivatedts IS NULL"
        ),
        parameters={"parcel_id": parcel_id},
    )
    return r.scalar_one()


def _get_mailing_address_pk(conn, parcel_id_primary_key):
    r: Result = conn.execute(
        text(
            "SELECT mailingparcel_mailingid from parcelmailingaddress where mailingparcel_parcelid=:parcel_pk AND deactivatedts IS NULL"
        ),
        parameters={"parcel_pk": parcel_id_primary_key},
    )
    try:
        return r.scalar_one(), None
    except NoResultFound:
        return None, Err.NO_RESULT_FOUND


def write_mailing_address(conn):
    pass


def _get_building_number_and_street_id_pk(
    conn, mailing_address_pk
) -> Tuple[Optional[Tuple[str, int]], Optional[Err]]:
    r: Result = conn.execute(
        text(
            "SELECT bldgno, street_streetid FROM mailingaddress WHERE addressid=:address_pk AND deactivatedts IS NULL"
        ),
        parameters={"address_pk": mailing_address_pk},
    )
    try:
        return r.one(), None
    except NoResultFound:
        return None, Err.NO_RESULT_FOUND


def _get_mailing_street_info(conn, street_id_primary_key) -> Tuple[str, bool, int]:
    r: Result = conn.execute(
        text(
            "SELECT name, pobox, citystatezip_cszipid FROM mailingstreet WHERE streetid=:street_pk"
        ),
        parameters={"street_pk": street_id_primary_key},
    )
    return r.one()


def _get_city_state_zip(conn, csz_pk) -> Tuple[str, str, str]:
    r: Result = conn.execute(
        text("SELECT city, state_abbr, zip_code FROM mailingcitystatezip WHERE id=:pk"),
        parameters={"pk": csz_pk},
    )
    return r.one()


def query_mailingaddress_tables(
    conn: Connection, parcel_id: str
) -> Tuple[Optional[CogDbAddress], dict, Optional[Err]]:
    pk = {}  # Primary Keys
    pk["parcel"] = _get_parcel_id_pk(conn, parcel_id)

    pk["mailing_address"], err = _get_mailing_address_pk(conn, pk["parcel"])
    if err is not None:
        return None, pk, Err.NO_MAILING_ADDRESS_FOUND

    (
        building_number,
        pk["mailing_street"],
    ), err = _get_building_number_and_street_id_pk(conn, pk["mailing_address"])
    if err is not None:
        return None, pk, Err.NO_RESULT_FOUND

    street_name, is_po_box, pk["csz"] = _get_mailing_street_info(
        conn, pk["mailing_street"]
    )
    city, state, zip_ = _get_city_state_zip(conn, pk["csz"])
    return (
        CogDbAddress(
            bldgno=building_number,
            street_name=street_name,
            pobox=is_po_box,
            city=city,
            state=state,
            zip_=zip_,
        ),
        pk,
        None,
    )


def query_for_city_state_zip_pk(conn: Connection, city, state, zip_):
    print(city, state, zip_)
    if zip_:
        r: Result = conn.execute(
            text(
                "SELECT id FROM mailingcitystatezip WHERE (city=:city) AND state_abbr=:state AND zip_code=:zip"
            ),
            parameters={"city": city, "state": state, "zip": zip_},
        )
    else:
        r: Result = conn.execute(
            text(
                "SELECT id FROM mailingcitystatezip WHERE (city=:city) AND state_abbr=:state"
            ),
            parameters={"city": city, "state": state},
        )
    try:
        return r.scalar_one(), None
    except NoResultFound:
        return None, Err.CITY_STATE_ZIP_NOT_FOUND
    except MultipleResultsFoud:
        raise MultipleResultsFound


def query_address_pk_using_city_state_zip(
    conn, city=None, state=None, zip_=None, **kwargs
):

    csz_pk, err = query_for_city_state_zip_pk(conn, city, state, zip_)
    if err is not None:
        return None, err
