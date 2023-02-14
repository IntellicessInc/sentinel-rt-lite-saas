import datetime
import json
import os
import shutil
import threading
import time
import traceback
from os.path import exists

import app_logger
import brook_client
import csv_jwlf_converter
import file_utils
import properties
import union_client
import utils

_start_datetime = None
_sleep_time = None
_bot_properties = None
_till_date_time_jwlf = None
_till_date_time_general_data = None
_new_output_data_lines_dict = {}
_ordered_curves_dict = {}
_headers_dict = {}
_running_consumer = True
_last_data_fetch_datetime = None
_contextual_input_data_files_hash_dict = {}
_sentinel_rt_lite_started = False
_resume_token_jwlf = None
_resume_token_general_data = None
_new_lines_to_print_in_console_dict = {}


def send_input_data(input_data_type):
    input_data_config = properties.INPUT_DATA_TYPES_DICT[input_data_type]
    file_paths = get_existing_file_paths_for_input_data_type()
    for file_path in file_paths:
        current_hash = file_utils.generate_file_sha256(file_path)
        last_hash = None
        if file_path in _contextual_input_data_files_hash_dict:
            last_hash = _contextual_input_data_files_hash_dict[file_path]
        if current_hash == last_hash:
            continue

        _contextual_input_data_files_hash_dict[file_path] = current_hash
        num_of_lines = file_utils.get_num_of_lines_in_file(file_path)
        with open(file_path, 'r', encoding='utf8') as file:
            metadata_headers_count = input_data_config['metadata_headers_count']
            metadata_header_lines = []
            headers_line = None
            input_data_lines = []
            line_index = 0
            for line in file:
                line = line.replace("\n", "")
                if line_index < metadata_headers_count:
                    metadata_header_lines.append(line)
                elif line_index == metadata_headers_count:
                    headers_line = line
                else:
                    input_data_lines.append(line)
                if len(input_data_lines) >= properties.BATCH_SIZE or line_index + 1 == num_of_lines:
                    metadata = {
                        "endOfFile": line_index + 1 >= num_of_lines,
                        "jwlfHeaderMetadataLines": metadata_header_lines,
                        properties.SENT_FILE_ID_METADATA_KEY: current_hash
                    }
                    input_jwlf = csv_jwlf_converter.csv_line_to_jwlf_log(input_data_type, headers_line,
                                                                         input_data_lines, metadata)
                    utils.run_request_with_retries(
                        lambda: union_client.send(properties.CLIENT, properties.REGION, properties.WELL,
                                                  properties.LOG_INPUT_UNION_FOLDER, input_jwlf))
                    input_data_config = properties.INPUT_DATA_TYPES_DICT[input_data_type]
                    if 'sent_data_log_filename' in input_data_config.keys():
                        sent_data_log_file_path = properties.SENT_DATA_INPUT_FILES_LOCAL_FOLDER + "/" + \
                                                  input_data_config[
                                                      'sent_data_log_filename']
                        save_locally_sent_input_data(sent_data_log_file_path, metadata_header_lines, headers_line,
                                                     input_data_lines,
                                                     input_data_type)
                    input_data_lines = []
                    app_logger.log(
                        f"Sending input data from '{file_path}': {((line_index + 1) / num_of_lines) * 100}%")
                line_index += 1


def general_data_entries_to_new_output_data_lines_dict(new_general_data_entries):
    global _new_output_data_lines_dict

    _new_output_data_lines_dict = {}
    if len(new_general_data_entries) > 0:
        for entry in new_general_data_entries:
            data_name = entry['name']
            new_output_data_line = entry['content']
            if "<class 'dict'>" == str(type(new_output_data_line)):
                new_output_data_line = json.dumps(new_output_data_line, indent=3)
            if data_name not in _new_output_data_lines_dict.keys():
                _new_output_data_lines_dict[data_name] = [[]]
            _new_output_data_lines_dict[data_name][0].append([new_output_data_line])

    return _new_output_data_lines_dict


def jwlf_logs_to_new_output_data_lines_dict(new_jwlf_logs):
    global _new_output_data_lines_dict

    _new_output_data_lines_dict = {}
    if len(new_jwlf_logs) > 0:
        for new_jwlf in new_jwlf_logs:
            jwlf_name = new_jwlf['header']['name']
            new_output_data_lines = []
            if jwlf_name not in _ordered_curves_dict.keys() or \
                    hash_of_curves_ignoring_order(
                        _ordered_curves_dict[jwlf_name]) != hash_of_curves_ignoring_order(
                new_jwlf['curves']):
                _ordered_curves_dict[jwlf_name] = new_jwlf['curves']
                csv_header = csv_jwlf_converter.create_csv_header(jwlf_name, _ordered_curves_dict[jwlf_name])
                if jwlf_name not in _headers_dict:
                    _headers_dict[jwlf_name] = []
                _headers_dict[jwlf_name].append(csv_header)

            headers_index = len(_headers_dict[jwlf_name]) - 1
            new_output_data_lines.extend(
                [csv_jwlf_converter.convert_jwlf_to_csv_line(jwlf_data_points, new_jwlf,
                                                             _ordered_curves_dict[jwlf_name]) for
                 jwlf_data_points in new_jwlf['data']]
            )
            if "metadata" in new_jwlf['header'] and properties.SPLIT_BY_BHA_METADATA_KEY in new_jwlf['header'][
                "metadata"]:
                if jwlf_name not in _new_output_data_lines_dict.keys():
                    _new_output_data_lines_dict[jwlf_name] = {}
                split_by_bha = new_jwlf['header']["metadata"][properties.SPLIT_BY_BHA_METADATA_KEY]
                if split_by_bha not in _new_output_data_lines_dict[jwlf_name]:
                    _new_output_data_lines_dict[jwlf_name][split_by_bha] = []
                while len(_new_output_data_lines_dict[jwlf_name][split_by_bha]) <= headers_index:
                    _new_output_data_lines_dict[jwlf_name][split_by_bha].append([])
                _new_output_data_lines_dict[jwlf_name][split_by_bha][headers_index].append(new_output_data_lines)
            else:
                if jwlf_name not in _new_output_data_lines_dict.keys():
                    _new_output_data_lines_dict[jwlf_name] = []
                while len(_new_output_data_lines_dict[jwlf_name]) <= headers_index:
                    _new_output_data_lines_dict[jwlf_name].append([])
                _new_output_data_lines_dict[jwlf_name][headers_index].append(new_output_data_lines)

    return _new_output_data_lines_dict


def hash_of_curves_ignoring_order(curves):
    return hash(frozenset([json.dumps(curve, sort_keys=True, default=str) for curve in curves]))


def save_headers_dict():
    file_utils.overwrite_file([json.dumps(_headers_dict)], properties.HEADERS_DICT_SAVE_FILE_PATH)
    file_utils.overwrite_file([json.dumps(_ordered_curves_dict)], properties.HEADERS_DICT_SAVE_FILE_PATH)


def save_locally_output_data(lines_dict):
    for data_name, elements in lines_dict.items():
        lines_entries_with_different_headers = elements
        for headers_index in range(0, len(lines_entries_with_different_headers)):
            lines_entries = lines_entries_with_different_headers[headers_index]
            headers = get_headers(data_name, headers_index)
            save_locally_output_data_lines(lines_entries, data_name, headers)
        if data_name in _headers_dict:
            _headers_dict[data_name] = [_headers_dict[data_name][-1]]
    save_headers_dict()


def get_headers(data_name, headers_index):
    headers = []
    if data_name in _headers_dict.keys() and headers_index < len(_headers_dict[data_name]):
        headers = [_headers_dict[data_name][headers_index]]
    return headers


def save_locally_output_data_lines(lines_entries, data_name, headers):
    global _new_lines_to_print_in_console_dict

    output_data_file_path = resolve_output_data_file_path(data_name)
    appended = True
    if data_name in properties.OUTPUT_DATA_NAMES_FILENAMES_DICT.keys():
        output_data_config = properties.OUTPUT_DATA_NAMES_FILENAMES_DICT[data_name]
        if 'value_mapper' in output_data_config.keys():
            lines_entries = [output_data_config['value_mapper'](lines_entry) for lines_entry in lines_entries]
        appended = properties.OUTPUT_DATA_NAMES_FILENAMES_DICT[data_name]['appended']
    if not lines_entries:
        return
    if appended:
        lines = utils.flat(lines_entries)
        if properties.CONSOLE_LOGGING_OUTPUT_DATA_CONTENT:
            for line in lines:
                app_logger.log(f"OUTPUT DATA FOR '{data_name}': {line}")
        else:
            lines_num = len(lines)
            if lines_num == 0:
                app_logger.log(f"Received headers of '{output_data_file_path}'")
            else:
                if output_data_file_path not in _new_lines_to_print_in_console_dict.keys():
                    _new_lines_to_print_in_console_dict[output_data_file_path] = lines_num
                else:
                    _new_lines_to_print_in_console_dict[output_data_file_path] += lines_num
        active_row_limit = data_name in properties.VOL_POSTFIX_OUTPUT_DATA_LIST
        file_utils.append_lines_to_file(lines, output_data_file_path, active_row_limit, headers)
    else:
        lines = lines_entries[-1]
        if headers:
            new_lines = []
            new_lines.extend(headers)
            new_lines.extend(lines)
            lines = new_lines

        if properties.CONSOLE_LOGGING_OUTPUT_DATA_CONTENT:
            for line in lines:
                app_logger.log(f"OUTPUT DATA FOR '{data_name}': {line}")
        file_utils.overwrite_file(lines, output_data_file_path)


def resolve_output_data_file_path(data_name):
    output_data_file_path = f"{properties.OUTPUT_FILES_LOCAL_FOLDER}/{data_name + '_' + properties.NOT_MATCHED_OUTPUT_FILENAME_POSTFIX}"
    if data_name in properties.OUTPUT_DATA_NAMES_FILENAMES_DICT.keys():
        output_data_config = properties.OUTPUT_DATA_NAMES_FILENAMES_DICT[data_name]
        if 'output_data_full_file_path' in output_data_config.keys():
            file_path = output_data_config['output_data_full_file_path']
        else:
            file_path = f"{properties.OUTPUT_FILES_LOCAL_FOLDER}/{output_data_config['filename']}"
        extension = ""
        file_path_without_extension = file_path
        if "." in file_path:
            parts = file_path.rsplit(".", 1)
            file_path_without_extension = parts[0]
            extension = "." + parts[1]
        output_data_file_path = file_path_without_extension + extension
    return output_data_file_path


def save_locally_sent_input_data(sent_data_file_path, metadata_header_lines=[], headers_line=None, input_data_lines=[],
                                 input_data_type='REAL_TIME'):
    lines_to_save = []
    for data_line in input_data_lines:
        lines_to_save.append(data_line)

    headers_lines = []
    headers_lines.extend(metadata_header_lines)
    if headers_line is not None:
        headers_lines.append(headers_line)
    active_row_limit = input_data_type == 'REAL_TIME'
    file_utils.append_lines_to_file(lines_to_save, sent_data_file_path, active_row_limit, headers=headers_lines)


def save_locally_sent_input_header(header):
    file_utils.append_lines_to_file([], properties.BROOK_SENT_RT_INPUT_DATA_FILE_PATH, headers=[header])


def get_existing_file_paths_for_input_data_type():
    return [properties.INPUT_FILES_LOCAL_FOLDER + "/" + properties.SOURCE_DATA_FILENAME]


def get_ordered_active_input_data_types():
    active_input_data_types_dict = {}
    for (input_data_type, input_data_config) in properties.INPUT_DATA_TYPES_DICT.items():
        if properties.RTInputDataTypes.HISTORICAL == properties.RT_INPUT_DATA_TYPE:
            priority = input_data_config['priority']
            if priority not in active_input_data_types_dict.keys():
                active_input_data_types_dict[priority] = []
            active_input_data_types_dict[priority].append(input_data_type)

    sorted_keys = sorted(active_input_data_types_dict.keys(), reverse=True)
    ordered_active_input_data_types = []
    for key in sorted_keys:
        ordered_active_input_data_types.extend(active_input_data_types_dict[key])
    return ordered_active_input_data_types


def load_headers_dict():
    global _headers_dict, _ordered_curves_dict

    if exists(properties.HEADERS_DICT_SAVE_FILE_PATH):
        with open(properties.HEADERS_DICT_SAVE_FILE_PATH, 'r', encoding='utf8') as f:
            _headers_dict = json.load(f)
    if exists(properties.ORDERED_CURVES_DICT_SAVE_FILE_PATH):
        with open(properties.ORDERED_CURVES_DICT_SAVE_FILE_PATH, 'r', encoding='utf8') as f:
            _ordered_curves_dict = json.load(f)


def load_bot_properties():
    global _bot_properties

    if _bot_properties is None:
        ordered_active_input_data_types = get_ordered_active_input_data_types()
        _bot_properties = {
            'brook.data.read.init-data': ','.join(ordered_active_input_data_types),
            'brook.data.read.timeseries': 'YES' if properties.RTInputDataTypes.REAL_TIME == properties.RT_INPUT_DATA_TYPE else 'NO'
        }


def register_sentinel_cloud_instance():
    global _bot_properties

    client_name = properties.CLIENT
    region = properties.REGION
    well = properties.WELL
    if properties.CLEAN_START:
        brook_client.delete_well_if_exists(client_name)
        time.sleep(20)
        union_client.delete_well_data_from_union(client_name, region, well)
    well_id = brook_client.register_well_if_doesnt_exist(client_name)

    app_logger.log(f"Brook well with id={well_id} is registered")

    properties_dict = {}
    for key in _bot_properties.keys():
        properties_dict[key] = _bot_properties[key]

    brook_client.update_sentinel_cloud_properties(client_name, properties_dict)


def create_data_directories():
    if properties.CLEAN_START:
        remove_local_data()
    if properties.INPUT_FILES_LOCAL_FOLDER not in os.listdir():
        os.makedirs(properties.INPUT_FILES_LOCAL_FOLDER)
    if properties.SENT_DATA_INPUT_FILES_LOCAL_FOLDER.split("/")[-1] not in os.listdir(
            properties.INPUT_FILES_LOCAL_FOLDER):
        os.makedirs(properties.SENT_DATA_INPUT_FILES_LOCAL_FOLDER)
    if properties.OUTPUT_FILES_LOCAL_FOLDER not in os.listdir():
        os.makedirs(properties.OUTPUT_FILES_LOCAL_FOLDER)
    if properties.LOGS_FOLDER not in os.listdir():
        os.makedirs(properties.LOGS_FOLDER)


def remove_local_data():
    if properties.INPUT_FILES_LOCAL_FOLDER in os.listdir() \
            and properties.SENT_DATA_INPUT_FILES_LOCAL_FOLDER.split("/")[-1] in os.listdir(
        properties.INPUT_FILES_LOCAL_FOLDER):
        shutil.rmtree(properties.SENT_DATA_INPUT_FILES_LOCAL_FOLDER)
    if properties.OUTPUT_FILES_LOCAL_FOLDER in os.listdir():
        shutil.rmtree(properties.OUTPUT_FILES_LOCAL_FOLDER)
    if properties.LOGS_FOLDER in os.listdir():
        shutil.rmtree(properties.LOGS_FOLDER)


def rename_sent_input_files():
    filenames = [filename for filename in os.listdir(properties.SENT_DATA_INPUT_FILES_LOCAL_FOLDER) if
                 filename.endswith(properties.BROOK_RT_INPUT_DATA_FILENAME_POSTFIX)]
    if len(filenames) == 1:
        os.rename(f"{properties.SENT_DATA_INPUT_FILES_LOCAL_FOLDER}/{filenames[0]}",
                  properties.BROOK_SENT_RT_INPUT_DATA_FILE_PATH)


def load_resume_tokens():
    global _resume_token_jwlf, _resume_token_general_data

    file_path_parts = properties.OUTPUT_FILES_SYNCH_TIMESTAMP_FILE_PATH.split("/")
    data = {}
    if not properties.CLEAN_START and file_path_parts[-1] in os.listdir("/".join(file_path_parts[0:-1])):
        with open(properties.OUTPUT_FILES_SYNCH_TIMESTAMP_FILE_PATH, 'r', encoding='utf8') as file:
            try:
                data_json = "\n".join(file.readlines())
                data = json.loads(data_json)
            except:
                traceback.print_exc()
                print(f"Loading resume tokens failure. Setting default values...")

    if 'resume_token_jwlf' in data:
        _resume_token_jwlf = str(data['resume_token_jwlf'])
    else:
        _resume_token_jwlf = None

    if 'resume_token_general_data' in data:
        _resume_token_general_data = str(data['resume_token_general_data'])
    else:
        _resume_token_general_data = None


def update_resume_tokens():
    data = {}
    if _till_date_time_jwlf:
        data['resume_token_jwlf'] = str(utils.date_time_to_milliseconds_timestamp(_till_date_time_jwlf))
    if _till_date_time_general_data:
        data['resume_token_general_data'] = str(
            utils.date_time_to_milliseconds_timestamp(_till_date_time_general_data))
    with open(properties.OUTPUT_FILES_SYNCH_TIMESTAMP_FILE_PATH, 'w', encoding='utf8') as file:
        json_data = json.dumps(data)
        file.write(json_data)


def get_rt_input_data_offset():
    offset = 0
    num_of_lines_list = [file_utils.get_num_of_lines_in_file(file_path) for file_path in
                         file_utils.get_all_partitions_of_file(properties.BROOK_SENT_RT_INPUT_DATA_FILE_PATH)]
    for num in num_of_lines_list:
        offset += num
    return offset


def print_received_new_lines():
    global _new_lines_to_print_in_console_dict
    if properties.RECEIVED_NEW_LINES_PRINTING_FREQUENCY > 0:
        while True:
            for output_data_file_path in _new_lines_to_print_in_console_dict:
                lines_num = _new_lines_to_print_in_console_dict[output_data_file_path]
                if lines_num > 0:
                    lines_str = 'lines'
                    if lines_num == 1:
                        lines_str = 'line'
                    app_logger.log(f"Received {lines_num} new {lines_str} of '{output_data_file_path}'")
                    _new_lines_to_print_in_console_dict[output_data_file_path] = 0
            time.sleep(1 / properties.RECEIVED_NEW_LINES_PRINTING_FREQUENCY)


def consume_jwlf_data():
    global _sentinel_rt_lite_started, _resume_token_jwlf, _last_data_fetch_datetime

    while True:
        try:
            for new_event in union_client.get_jwlfs_stream(properties.CLIENT, properties.REGION, properties.WELL,
                                                           properties.LOG_OUTPUT_UNION_FOLDER, _resume_token_jwlf):
                log = new_event['data']
                _resume_token_jwlf = new_event['id']
                save_locally_output_data(jwlf_logs_to_new_output_data_lines_dict([log]))
                _last_data_fetch_datetime = datetime.datetime.utcnow()
                _sentinel_rt_lite_started = True
                update_resume_tokens()
        except:
            continue


def consume_general_data():
    global _sentinel_rt_lite_started, _resume_token_general_data, _last_data_fetch_datetime

    while True:
        try:
            for new_event in union_client.get_general_data_stream(properties.CLIENT, properties.REGION, properties.WELL,
                                                                  properties.LOG_OUTPUT_UNION_FOLDER,
                                                                  _resume_token_general_data):
                general_data_entry = new_event['data']
                _resume_token_general_data = new_event['id']
                save_locally_output_data(general_data_entries_to_new_output_data_lines_dict([general_data_entry]))
                _last_data_fetch_datetime = datetime.datetime.utcnow()
                _sentinel_rt_lite_started = True
                update_resume_tokens()
        except:
            continue


def send_historical_input_data():
    ordered_active_input_data_types = get_ordered_active_input_data_types()
    for active_input_data_type in ordered_active_input_data_types:
        send_input_data(active_input_data_type)


def run_real_time_mode():
    global _start_datetime, _sleep_time

    if properties.RunTypes.CONSUMER in properties.ACTIVE_RUN_TYPES:
        threading.Thread(target=consume_jwlf_data, daemon=True).start()
        threading.Thread(target=consume_general_data, daemon=True).start()

    while not _sentinel_rt_lite_started or properties.RunTypes.PRODUCER not in properties.ACTIVE_RUN_TYPES:
        time.sleep(1)

    if properties.RunTypes.PRODUCER in properties.ACTIVE_RUN_TYPES:
        source_data_file_path = f"{properties.INPUT_FILES_LOCAL_FOLDER}/{properties.SOURCE_DATA_FILENAME}"
        with open(source_data_file_path, 'r', encoding='utf8') as source_data_file:
            input_header_line = ''
            input_data_index = 0
            input_data_lines = []

            for input_data_line in source_data_file:
                _start_datetime = datetime.datetime.utcnow()
                input_data_index += 1
                input_data_line = input_data_line.replace('\n', '')
                if input_header_line == '':
                    input_header_line = input_data_line
                    save_locally_sent_input_header(input_data_line)
                    continue
                if rt_input_data_offset >= input_data_index:
                    continue

                input_data_lines.append(input_data_line)
                input_jwlf = csv_jwlf_converter.csv_line_to_jwlf_log(properties.RT_INPUT_DATA_TYPE.name,
                                                                     input_header_line, input_data_lines)
                if input_jwlf:
                    utils.run_request_with_retries(
                        lambda: union_client.send(properties.CLIENT, properties.REGION, properties.WELL,
                                                  properties.LOG_INPUT_UNION_FOLDER, input_jwlf))
                    save_locally_sent_input_data(properties.BROOK_SENT_RT_INPUT_DATA_FILE_PATH,
                                                 input_data_lines=input_data_lines)
                    app_logger.log(
                        f"New line of real time data was sent to Sentinel RT Lite SaaS. Time Stamp: {input_jwlf['header']['date']}")
                    input_data_lines = []

                if properties.sentinel_exit:
                    break

                _sleep_time = max(
                    properties.DATA_SYNCH_RATE_IN_SECONDS - (
                            datetime.datetime.utcnow() - _start_datetime).total_seconds(),
                    0)
                time.sleep(_sleep_time)
        app_logger.log("Input data have been sent")


def run_historical_mode():
    global _start_datetime, _sleep_time, _running_consumer, _sentinel_rt_lite_started, _last_data_fetch_datetime

    if properties.RunTypes.PRODUCER in properties.ACTIVE_RUN_TYPES:
        send_historical_input_data()
    if properties.RunTypes.CONSUMER in properties.ACTIVE_RUN_TYPES:
        loading_data_message_logged = False

        threading.Thread(target=consume_jwlf_data, daemon=True).start()
        threading.Thread(target=consume_general_data, daemon=True).start()

        while _running_consumer:
            _sentinel_rt_lite_started = _last_data_fetch_datetime is not None
            if not loading_data_message_logged and _sentinel_rt_lite_started:
                loading_data_message_logged = True
                app_logger.log("Sentinel RT Lite SaaS is loading the data...")
                app_logger.log("It may take up to a few minutes")
            _start_datetime = datetime.datetime.utcnow()
            if properties.sentinel_exit:
                break
            if _last_data_fetch_datetime is not None:
                _running_consumer = datetime.datetime.utcnow() - datetime.timedelta(
                    milliseconds=properties.LAST_DATA_FETCH_MAX_DELAY_MILLISECONDS) < _last_data_fetch_datetime

            _sleep_time = max(
                properties.DATA_SYNCH_RATE_IN_SECONDS / 2.0 - (
                        datetime.datetime.utcnow() - _start_datetime).total_seconds(),
                0)
            time.sleep(_sleep_time)
        app_logger.log("Finished receiving data from Sentinel RT Lite SaaS")


create_data_directories()
load_bot_properties()
load_headers_dict()
rename_sent_input_files()
rt_input_data_offset = get_rt_input_data_offset()
load_resume_tokens()

app_logger.log(f"{properties.RT_INPUT_DATA_TYPE.name} mode is on")
app_logger.log("Creating new Sentinel RT Lite SaaS instance...")
register_sentinel_cloud_instance()
threading.Thread(target=print_received_new_lines, daemon=True).start()

app_logger.log("Sentinel RT Lite SaaS is starting...")
app_logger.log("Please wait for about 3 minutes")
if properties.RTInputDataTypes.REAL_TIME == properties.RT_INPUT_DATA_TYPE:
    run_real_time_mode()
elif properties.RTInputDataTypes.HISTORICAL == properties.RT_INPUT_DATA_TYPE:
    run_historical_mode()
