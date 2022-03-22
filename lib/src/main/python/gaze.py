import httpx
import copy
import html
import re
from typing import TypedDict, Optional

_api_base = "https://corpsearch-api.pittsburghhousing.org"
gaze_username = ""
gaze_password = ""


def get_token():
    with httpx.Client() as client:
        login_response = client.get(
            "https://owner-api.pittsburghhousing.org/auth/login"
        )
        # Response contains a url
        accounts_url = login_response.content
        accounts_response = client.get(accounts_url.decode("utf-8"))
        pattern = re.compile(rb'.*action="(.*?)"')
        matches = re.finditer(pattern, accounts_response.content)
        match = matches.__next__()
        authenticate_url = match.group(1).decode("utf-8")
        authenticate_url = html.unescape(authenticate_url)
        auth_response = client.post(
            authenticate_url,
            data={
                "username": gaze_username,
                "password": gaze_password,
                "credentialId": "",
            },
            follow_redirects=True,
        )
        auth_html = auth_response.content.decode("utf-8")
        pattern = re.compile(r"eyJ.*")
        matches = re.finditer(pattern, auth_html)
        return matches.__next__().group(0)


def test_endpoint(api_key):
    endpoint = _api_base + "/auth/required"
    response = httpx.get(endpoint, headers={"Authorization": api_key})
    return response.json()


def property_search(parcel_id: str, access_token) -> dict:
    endpoint = _api_base + f"/browser/owner-info/{parcel_id}"
    response = httpx.get(
        endpoint,
        params={"parcelId": parcel_id},
        headers={"Authorization": access_token},
    )
    response.raise_for_status()
    return response.json()


class GazeAddress(TypedDict):
    number: str
    street: str
    type: str
    suffix: str
    city: str
    state: str
    zip_: str
    plus4: str
    sec_unit_type: str
    sec_unit_num: str

def get_parsed_mailing_data_from_gaze_response(
    gaze_response: dict,
) -> Optional[GazeAddress]:
    parsed: GazeAddress = gaze_response["results"]["mailing"]["parsed"]
    if parsed is None:
        return None
    # This check is to catch edge cases (since the API's return type isn't well documented
    keys = copy.copy(parsed)
    for attr in ["number", "street", "type", "suffix", "city", "state", "zip", "plus4", "sec_unit_type", "sec_unit_num"]:
        try:
            keys.pop(attr)
        except KeyError:
            continue
    if keys.keys():
        raise RuntimeError(f"Unexpected keys {keys.keys()} in parsed address")
    return parsed
