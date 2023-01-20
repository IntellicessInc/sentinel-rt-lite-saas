import requests

import properties


def create_well_and_return_id(client_name):
    url = f"{properties.BROOK_URL}/clients/{client_name}/assets"
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {"assetName": properties.WELL, "region": properties.REGION}
    res = requests.post(url, json=payload, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Brook well creation failure with http response status code={res.status_code} and response={res.text}")
    return res.json()["id"]


def delete_well(client_name, well_id):
    url = f"{properties.BROOK_URL}/clients/{client_name}/assets/{well_id}"
    res = requests.delete(url)
    if res.status_code >= 300:
        raise Exception(
            f"Brook well creation failure with http response status code={res.status_code} and response={res.text}")


def register_well_if_doesnt_exist(client_name):
    url = f"{properties.BROOK_URL}/clients/{client_name}/assets"
    headers = {
        'Content-Type': 'application/json'
    }
    res = requests.get(url, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Assets from Brook retrieval failure with http response status code={res.status_code} and response={res.text}")
    found_matched_wells = [well for well in res.json() if well["name"] == properties.WELL]
    if len(found_matched_wells) == 1:
        return found_matched_wells[0]["id"]
    return create_well_and_return_id(client_name)


def delete_well_if_exists(client_name):
    url = f"{properties.BROOK_URL}/clients/{client_name}/assets"
    headers = {
        'Content-Type': 'application/json'
    }
    res = requests.get(url, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Assets from Brook retrieval failure with http response status code={res.status_code} and response={res.text}")
    found_matched_wells = [well for well in res.json() if well["name"] == properties.WELL]
    if len(found_matched_wells) == 1:
        delete_well(client_name, found_matched_wells[0]["id"])


def get_bot_id(client_name):
    retry_number = 0
    while retry_number < properties.MAX_GET_BOT_ID_REQUESTS:
        url = f"{properties.BROOK_URL}/clients/{client_name}/bots?assetName={properties.WELL}"
        headers = {
            'Content-Type': 'application/json'
        }
        res = requests.get(url, headers=headers)
        bot_id = -1
        if 200 <= res.status_code < 300:
            matched_bots_ids = [bot["id"] for bot in res.json() if bot["type"] == properties.BROOK_BOT_TYPE]
            if len(matched_bots_ids) == 1:
                bot_id = matched_bots_ids[0]
        if bot_id != -1:
            return bot_id
    raise Exception("Brook bot id retrieval failure")


def update_sentinel_cloud_properties(client_name, properties_dict):
    bot_id = get_bot_id(client_name)
    url = f"{properties.BROOK_URL}/clients/{client_name}/bots/{bot_id}/environment-variables"
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        "brookBotEnvironmentVariables": [
            {"name": property_key, "value": properties_dict[property_key], "secret": False}
            for property_key in properties_dict.keys()
        ]
    }
    res = requests.put(url, json=payload, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Brook bot environment variables update failure with http response status code={res.status_code} and response={res.text}")

