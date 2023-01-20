import hashlib
import os
import re

import properties
import utils


_ordinal_files_dict = {}

def are_headers_matched(file_path, headers):
    if is_completely_new_file(file_path):
        return True
    headers = [header_with_new_line.replace('\n', '') for header_with_new_line in
               utils.flat([header.split('\n') for header in headers])]
    lines = []
    with open(file_path, 'r', encoding='utf8') as f:
        for line in f:
            if len(headers) <= len(lines):
                break
            lines.append(line.replace('\n', ''))
    result = True
    for i in range(0, len(lines)):
        result = result and lines[i] == headers[i]
    return result


def append_lines_to_file(lines, file_path, active_row_limit=True, headers=[]):
    global _ordinal_files_dict

    if active_row_limit:
        if file_path in _ordinal_files_dict:
            ordinal_file_data = _ordinal_files_dict[file_path]
            current_ordinal_file_path = ordinal_file_data['current_ordinal_file_path']
            next_ordinal_file_path = ordinal_file_data['next_ordinal_file_path']
            remaining_number_of_lines = ordinal_file_data['remaining_number_of_lines']
        else:
            current_ordinal_file_path, next_ordinal_file_path, remaining_number_of_lines \
                = get_current_order_file_path(file_path, properties.OUTPUT_DATA_ROW_LIMIT)
        if not are_headers_matched(current_ordinal_file_path, headers):
            remaining_number_of_lines = 0

        current_ordinal_file_lines = lines[0:min(len(lines), remaining_number_of_lines)]
        append_lines_to_file(current_ordinal_file_lines, current_ordinal_file_path, False, headers)
        if len(lines) > remaining_number_of_lines:
            next_ordinal_file_lines = lines[remaining_number_of_lines:]
            append_lines_to_file(next_ordinal_file_lines, next_ordinal_file_path, False, headers)
            current_ordinal_file_path, next_ordinal_file_path, remaining_number_of_lines \
                = get_current_order_file_path(file_path, properties.OUTPUT_DATA_ROW_LIMIT)
        elif len(lines) <= remaining_number_of_lines:
            remaining_number_of_lines = remaining_number_of_lines - len(current_ordinal_file_lines)

        _ordinal_files_dict[file_path] = {
            'current_ordinal_file_path': current_ordinal_file_path,
            'next_ordinal_file_path': next_ordinal_file_path,
            'remaining_number_of_lines': remaining_number_of_lines
        }
    else:
        completely_new_file = is_completely_new_file(file_path)
        all_lines = []
        if completely_new_file:
            all_lines.extend(headers)
        all_lines.extend(lines)
        with open(file_path, 'a', encoding='utf8') as f:
            for line in all_lines:
                new_line = '\n'
                if completely_new_file:
                    new_line = ''
                    completely_new_file = False
                f.write(new_line + line)


def get_num_of_lines_in_file(file_path):
    num_of_lines = 0
    try:
        with open(file_path, 'r', encoding='utf8') as source_data_file:
            for _ in source_data_file:
                num_of_lines += 1
    except FileNotFoundError:
        pass
    return num_of_lines


def get_current_order_file_path(file_path, row_limit):
    file_path_parts = file_path.rsplit('/', 1)
    folder = file_path_parts[0]
    filename = file_path_parts[1]
    filename_parts = filename.rsplit('.', 1)
    file_name_without_extension = filename_parts[0].split(properties.FILE_ORDINAL_PREFIX, 1)[0]
    extension = filename_parts[1]
    current_ordinal = 1
    for folder_filename in os.listdir(folder):
        ordinal = folder_filename \
            .replace(file_name_without_extension, '') \
            .replace(properties.FILE_ORDINAL_PREFIX, '') \
            .replace('.' + extension, '')
        try:
            ordinal = int(ordinal)
            if re.match(f"{file_name_without_extension}{properties.FILE_ORDINAL_PREFIX}\d*.{extension}",
                        folder_filename) \
                    and current_ordinal < ordinal:
                current_ordinal = ordinal
        except:
            pass

    current_ordinal_file_path = f"{folder}/{file_name_without_extension}{properties.FILE_ORDINAL_PREFIX}{current_ordinal}.{extension}"
    next_ordinal_file_path = f"{folder}/{file_name_without_extension}{properties.FILE_ORDINAL_PREFIX}{current_ordinal + 1}.{extension}"
    num_of_lines = get_num_of_lines_in_file(current_ordinal_file_path)
    remaining_number_of_lines = max(row_limit - num_of_lines, 0)
    return current_ordinal_file_path, next_ordinal_file_path, remaining_number_of_lines


def get_all_partitions_of_file(file_path):
    file_path_parts = file_path.rsplit('/', 1)
    folder = file_path_parts[0]
    filename = file_path_parts[1]
    filename_parts = filename.rsplit('.', 1)
    file_name_without_extension = filename_parts[0].split(properties.FILE_ORDINAL_PREFIX, 1)[0]
    extension = filename_parts[1]
    current_ordinal = 0
    for folder_filename in os.listdir(folder):
        ordinal = folder_filename \
            .replace(file_name_without_extension, '') \
            .replace(properties.FILE_ORDINAL_PREFIX, '') \
            .replace('.' + extension, '')
        try:
            ordinal = int(ordinal)
            if re.match(f"{file_name_without_extension}{properties.FILE_ORDINAL_PREFIX}\d*.{extension}",
                        folder_filename) \
                    and current_ordinal < ordinal:
                current_ordinal = ordinal
        except:
            pass
    return [f"{folder}/{file_name_without_extension}{properties.FILE_ORDINAL_PREFIX}{i}.{extension}" for i in
            range(1, current_ordinal + 1)]


def overwrite_file(lines, filename):
    with open(filename, 'w', encoding='utf8') as f:
        for line in lines:
            f.write(line + '\n')


def clear_file(filename):
    with open(filename, 'w', encoding='utf8') as f:
        pass


def generate_file_sha256(file_path, block_size=2 ** 20):
    md = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            buffer = f.read(block_size)
            if not buffer:
                break
            md.update(buffer)
    return md.hexdigest()


def get_all_files_including_subdirectories(main_path):
    list_of_file = os.listdir(main_path)
    all_files = []
    for entry in list_of_file:
        full_path = main_path + "/" + entry
        if os.path.isdir(full_path):
            all_files.extend(get_all_files_including_subdirectories(full_path))
        else:
            all_files.append(full_path)

    return all_files


def is_completely_new_file(file_path):
    file_path_parts = file_path.split("/")
    completely_new_file = file_path_parts[-1] not in os.listdir("/".join(file_path_parts[0:-1]))
    return completely_new_file
