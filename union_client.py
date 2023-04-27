import json

import requests
from requests import Response
from requests.exceptions import ChunkedEncodingError, ReadTimeout

import app_logger
import keycloak_client
import properties
import utils


def delete_well_data_from_union(client_name, region, well):
    url = f"{properties.UNION_URL}/well-logs/{client_name}/{region}/{well}"
    headers = {
        'Content-Type': 'application/json',
        "Authorization": f"Bearer {keycloak_client.get_access_token()}"
    }
    res = requests.delete(url, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"Well Logs deletion failure with http response status code={res.status_code} and response={res.text}")

    url = f"{properties.UNION_URL}/general-data-entries/{client_name}/{region}/{well}"
    headers = {
        'Content-Type': 'application/json',
        "Authorization": f"Bearer {keycloak_client.get_access_token()}"
    }
    res = requests.delete(url, headers=headers)
    if res.status_code >= 300:
        raise Exception(
            f"General data entries deletion failure with http response status code={res.status_code} and response={res.text}")


def send(client, region, well, folder, jwlf):
    url = f"{properties.UNION_URL}/well-logs/{client}/{region}/{well}/{folder}"
    res: Response = requests.post(url, json=jwlf,
                                  headers={"Authorization": f"Bearer {keycloak_client.get_access_token()}"})
    if res.status_code >= 300:
        raise Exception(
            f"Sending data failure with http response status code={res.status_code} and response={res.text}")
    return res.json()["id"]


def get_jwlfs_stream(client, region, well, folder, resume_token, _resume_at_timestamp, full_data=True):
    for event in _get_stream(client, region, well, folder, resume_token, _resume_at_timestamp, full_data,
                             'well-logs'):
        yield event


def get_general_data_stream(client, region, well, folder, resume_token, _resume_at_timestamp, full_data=True):
    for event in _get_stream(client, region, well, folder, resume_token, _resume_at_timestamp, full_data,
                             'general-data-entries'):
        yield event


def _get_stream(client, region, well, folder, resume_token, _resume_at_timestamp, full_data, endpoint_name):
    query_params = {'fullData': ('true' if full_data else 'false')}
    url = f"{properties.UNION_URL}/{endpoint_name}-stream/{client}/{region}/{well}/{folder}"
    while True:
        access_token = keycloak_client.get_access_token()
        headers = {'Authorization': f"Bearer {access_token}", 'Accept': 'application/x-ndjson'}
        if resume_token is not None:
            query_params['resumeToken'] = resume_token
        if _resume_at_timestamp is not None:
            query_params['resumeAtTimestamp'] = _resume_at_timestamp
        try:
            with requests.get(url, stream=True, params=query_params, headers=headers, timeout=90) as response:
                for line in response.iter_lines(decode_unicode=True):
                    if line and line != '':
                        new_event = json.loads(line)
                        resume_token = new_event['id']
                        yield new_event
        except ChunkedEncodingError:
            pass
        except ReadTimeout:
            pass


def get_data_entries_from_union(url, since_date_time):
    query_params = {}
    since_date_time_milliseconds = utils.date_time_to_milliseconds_timestamp(since_date_time)
    if since_date_time:
        query_params['sinceTimestamp'] = since_date_time_milliseconds
    query_params['tillTimestamp'] = since_date_time_milliseconds

    stable_timestamp_response: Response = requests.get(
        url, params=query_params, headers={"Authorization": f"Bearer {keycloak_client.get_access_token()}"})
    if stable_timestamp_response.status_code >= 300:
        raise Exception(
            f"Retrieving data from UNION failure with"
            f" http response status code={stable_timestamp_response.status_code}"
            f" and response={stable_timestamp_response.text}")

    stable_timestamp_response_json = stable_timestamp_response.json()
    stable_data_timestamp = stable_timestamp_response_json['stableDataTimestamp']
    till_timestamp = max(since_date_time_milliseconds, stable_data_timestamp + 1)
    query_params['tillTimestamp'] = till_timestamp
    till_date_time = utils.milliseconds_timestamp_to_date_time(till_timestamp)

    little_data_elements_response: Response = requests.get(
        url, params=query_params, headers={"Authorization": f"Bearer {keycloak_client.get_access_token()}"})
    if little_data_elements_response.status_code >= 300:
        raise Exception(
            f"Retrieving data from UNION failure with"
            f" http response status code={little_data_elements_response.status_code}"
            f" and response={little_data_elements_response.text}")

    ids_response_json = little_data_elements_response.json()

    little_data_elements = ids_response_json['list']
    all_ids = [element['id'] for element in little_data_elements]
    if len(all_ids) == 0:
        return {'entries': [], 'till_date_time': till_date_time}

    ids_packages = [[]]
    id_package_index = 0
    id_index_in_package = 0
    for id in all_ids:
        if id_index_in_package >= properties.MAX_NUMBER_OF_IDS_PER_REQUEST:
            id_package_index += 1
            ids_packages.append([])
            id_index_in_package = 0
        ids_packages[id_package_index].append(id)
        id_index_in_package += 1

    entries = []
    all_ids_num = len(all_ids)
    for ids in ids_packages:
        query_params['id'] = ids
        full_data_elements_response: Response = requests.get(
            url, params=query_params, headers={"Authorization": f"Bearer {keycloak_client.get_access_token()}"})
        if full_data_elements_response.status_code >= 300:
            raise Exception(
                f"Retrieving data from UNION failure with"
                f" http response status code={full_data_elements_response.status_code}"
                f" and response={full_data_elements_response.text}")
        data_json = full_data_elements_response.json()
        entries.extend(data_json['list'])
        if len(ids_packages) > 1:
            app_logger.log(f"Receiving new output data packets {(len(entries) / all_ids_num) * 100.0}%")

    return {'entries': entries, 'till_date_time': till_date_time}
