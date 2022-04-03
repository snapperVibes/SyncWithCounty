import httpx
from sqlalchemy.future import Connection
from sqlalchemy import text
from __init__ import sync2, get_db


def get_parcel_ids(conn: Connection):
    cursor_result = conn.execute(text("SELECT parcelidcnty FROM parcel WHERE deactivatedts IS NULL LIMIT 100;"))
    return [i[0] for i in cursor_result]


if __name__ == "__main__":
    with get_db() as db_conn:
        parcel_ids = get_parcel_ids(db_conn)
        with httpx.Client() as web_client:
            for parcel_id in parcel_ids:
                sync2(db_conn, web_client, parcel_id)
    # for parcel_id in parcel_ids:
    #     sync(parcel_id)
    # token = get_token()
    # # test_owner_api(token)
    # with get_db() as conn:
    #     parcel_ids = get_parcel_ids(conn)
    #     counter = 1
    #     skip_to = 0
    #     for i, parcel_id in enumerate(parcel_ids):
    #         if i < skip_to:
    #             print("skipping", parcel_id)
    #             counter += 1
    #
    #         sync(token, parcel_id)
    #         print("COMPLETED: ", counter)
    #         counter += 1
#
