import datetime

import requests

import properties

_access_token_creation_time = None
_last_keycloak_realm = None
_last_access_token = None


def get_access_token_for_realm(realm):
    global _access_token_creation_time, _last_keycloak_realm, _last_access_token

    if _access_token_creation_time:
        access_token_age_seconds = (datetime.datetime.utcnow() - _access_token_creation_time).total_seconds()
    else:
        access_token_age_seconds = properties.MAX_ACCESS_TOKEN_AGE_SECONDS

    if realm == _last_keycloak_realm and access_token_age_seconds < properties.MAX_ACCESS_TOKEN_AGE_SECONDS:
        return _last_access_token
    url = f"{properties.KEYCLOAK_URL}/auth/realms/{properties.CLIENT}/protocol/openid-connect/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    payload = {"client_id": properties.KEYCLOAK_CLIENT_ID, "grant_type": "password", "username": properties.USERNAME,
               "password": properties.PASSWORD,
               "client_secret": properties.KEYCLOAK_CLIENT_SECRET}
    res = requests.post(url, data=payload, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Access token retrieval failure with http response status code={res.status_code} and response={res.text}")
    _access_token_creation_time = datetime.datetime.utcnow()
    _last_access_token = res.json()["access_token"]
    _last_keycloak_realm = realm
    return _last_access_token


def get_access_token():
    return get_access_token_for_realm(properties.CLIENT)
