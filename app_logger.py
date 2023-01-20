import datetime
import json

import file_utils
import properties


def log(str_content):
    log_message = f"{datetime.datetime.now()} {str_content}"
    print(log_message)
    file_utils.append_lines_to_file([log_message], properties.SENTINEL_CLOUD_CLIENT_LOGS)


def map_log_json_to_log_format(lines):

    json_str = "".join(lines)
    json_value = json.loads(json_str)
    log_formatted_value = ""
    if 'dateTime' in json_value.keys():
        log_formatted_value += f"{json_value['dateTime']}"
    if 'level' in json_value.keys():
        log_formatted_value += f" {json_value['level']}"
    if 'message' in json_value.keys():
        log_formatted_value += f" {json_value['message']}"
    if 'stackTrace' in json_value.keys():
        log_formatted_value += f"\n{json_value['stackTrace']}"

    if 'Sentinel exit' in log_formatted_value:
        log('Sentinel RT Lite SaaS exited!')
        properties.sentinel_exit = True

    return [log_formatted_value]