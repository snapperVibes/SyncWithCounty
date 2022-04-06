from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Result

USER_ID = 99
SOURCE_ID: int


def query_for_mailingcitystatezip_id(conn, city: str, state: str, zip_code: str) -> Optional[int]:
    result: Result = conn.execute(
        text(
            "select id from mailingcitystatezip where "
            "   city=:city and"
            "   state_abbr=:state and"
            "   zip_code=:zip_code and"
            "   deactivatedts is null;"
        ),
        {"city": city, "state": state, "zip_code": zip_code},
    )
    return result.scalar_one_or_none()


def query_for_mailingstreet_id(conn, mailingcitystatezip_id: int, street_name: str) -> Optional[int]:
    result: Result = conn.execute(
        text(
            "select streetid FROM mailingstreet where "
            "   citystatezip_cszipid=:csz_id and"
            "   name=:street_name and"
            "   deactivatedts is null"
        ),
        {"csz_id": mailingcitystatezip_id, "street_name": street_name},
    )
    return result.scalar_one_or_none()


def query_for_mailing_addresses(conn, mailingstreet_id: int, building_number: str) -> Optional[list[int]]:
    result: Result = conn.execute(
        text(
            "select addressid from mailingaddress "
            "   where street_streetid=:street_id and "
            "   bldgno=:building_number and "
            "   deactivatedts is null;",
        ),
        {"street_id": mailingstreet_id, "building_number": building_number},
    )
    if result is None:
        return None
    return [id_ for id_ in result]


def write_city_state_zip(conn, _city, _state, _zip) -> int:
    result = conn.execute(
        text(
            "insert into mailingcitystatezip"
            "   (id, zip_code, sid, state_abbr, city, list_type_id, list_type, default_state, default_city, default_type, source_sourceid, lastupdatedts, lastupdatedby_userid, deactivatedts, deactivatedby_userid) VALUES"
            "   ()"

        )
    )


def write_street(conn, _csz_id, _street) -> int:
    result = conn.execute(
        text(
            "insert into mailingstreet"
            "   ( name,  citystatezip_cszipid,  pobox, lastupdatedts,  lastupdatedby_userid) values"
            "   (:name, :citystatezip_cszipid, :pobox, now()        , :lastupdatedby_userid)"
            "   RETURNING streetid;"
        ),
        {
            "name": _street,
            "citystatezip_cszipid": _csz_id,
            "pobox": None,
            "lastupdatedby_userid": USER_ID
        }
    )
    return result.scalar_one()


def write_mailing_address(conn, street_id: int, building_number: str) -> int:
    result = conn.execute(
        text(
            "insert into mailingaddress"
            "   ( bldgno,           street_streetid,  source_sourceid, createdts, createdby_userid, lastupdatedts, lastupdatedby_userid) VALUES"
            "   (:building_number, :street_streetid, :source_sourceid, now()    , :user_id        , now()       , :user_id            )"
            "   RETURNING addressid;"
        ),
        {
            "building_number": building_number,
            "street_streetid": street_id,
            "source_sourceid": SOURCE_ID,
            "user_id": USER_ID
        }
    )
    return result.scalar_one()