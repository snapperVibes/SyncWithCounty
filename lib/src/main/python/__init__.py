import gaze
from gaze import GazeAddress
from db import (
    query_mailingaddress_tables,
    CogDbAddress,
    get_db,
    Err,
    query_address_pk_using_city_state_zip,
)
from sqlalchemy.future import Connection


def compare_gaze_and_cogdb(gaze: GazeAddress, cogdb: CogDbAddress):
    def show_error():
        def gaze_to_string(gaze: GazeAddress):
            return f"{gaze['number']} {gaze['street']} {gaze['type']}, {gaze['city']} {gaze['state']}".upper()

        def cog_to_string(cog: CogDbAddress):
            return f"{cog['bldgno']} {cog['street_name']}, {cog['city']} {cog['state']}"

        return f"GAZE:\t{gaze_to_string(gaze)}\nCOG:\t{cog_to_string(cogdb)}".upper()

    assert gaze is not None, "Gaze is None"
    assert cogdb is not None, "Cog is None"

    assert gaze["number"] == cogdb["bldgno"], show_error()
    assert (
        gaze["street"] + " " + gaze["type"] + gaze.get("suffix", "")
        == cogdb["street_name"]
    ), show_error()
    assert gaze["city"] == cogdb["city"], show_error()
    assert gaze["state"] == cogdb["state"], show_error()
    assert gaze["zip_"] == cogdb["zip_"], show_error()
    return True


DASHES = "-" * 89


def handle_no_mailing_address(
    conn: Connection, gaze_address: GazeAddress, cog_address: CogDbAddress, pk: dict
):
    print("NO MAILING ADDRESS")
    print("GAZE ADDRESS: ", gaze_address)
    print("COG ADDRESS: ", cog_address)
    if gaze_address is None:
        if cog_address is None:
            print("WHERE IS EVERYTHING?")
            return False
        raise RuntimeError("Unexpected state")
    pk["mailing_address"], err2 = query_address_pk_using_city_state_zip(
        conn,
        gaze_address.get("city"),
        gaze_address.get("state"),
        gaze_address.get("zip_"),
    )
    if err2 is Err.NO_MAILING_ADDRESS_FOUND:
        print("NO MAILING ADDRESS FOUND, WRITING TO DB")
        address_pk = write_address_to_db(conn, address)
    if err2 is Err.CITY_STATE_ZIP_NOT_FOUND:
        # raise RuntimeError(err2)
        print("ERROR: CITY/STATE/ZIP NOT FOUND IN TABLE")
        return False
    if err2:
        raise RuntimeError("Unexpected error")
    link_parcel_pk_to_address_pk(conn, parcel_pk, address_pk)


def sync(gaze_token: str, parcel_id: str):
    print(DASHES)
    print("PARCEL ID:", parcel_id, sep="\t")

    gaze_response = gaze.property_search(parcel_id, gaze_token)
    gaze_address: gaze.GazeAddress = gaze.get_parsed_mailing_data_from_gaze_response(
        gaze_response
    )

    with get_db() as conn:
        # human_info = query_human_table(conn)
        cog_db_address, pk, err = query_mailingaddress_tables(conn, parcel_id)
        match err:
            case None:
                pass
            case err.NO_MAILING_ADDRESS_FOUND:
                handle_no_mailing_address(conn, gaze_address, cog_db_address, pk)
            case _:
                raise RuntimeError
    print(gaze_address)
    if gaze_address is None:
        print("Gaze says that the property does not have a mailing address.")
        if cog_db_address is None:
            print("OH no")
            return False
        if parcel_is_linked_to_mailing_address(conn, pk["parcel"]):
            deactivate_parcel(conn, parcel_id)

    synchronized = False
    try:
        synchronized: bool = compare_gaze_and_cogdb(gaze_address, cog_db_address)
        print("SUCCESS!")
    except AssertionError as err:
        print("ERROR!")
        print(RuntimeError(f"{err}"))

    if synchronized:
        print(gaze_address)
        return True
    return False

    #
    # human_info = query_human_table()
    # mailingaddress_info = query_mailingaddress_tables(conn, parcel_id)
    #
    # synchronized: bool = compare_database_and_gaze_api_information(
    #     gaze_response,
    #     human_info,
    #     mailingaddress_info
    # )
    #
    # if synchronized:
    #     return
    # return True


def _conditional(condition, consequent, alternative):
    if condition:
        return consequent
    return alternative
