import datetime

import pytz

import properties
import utils


def create_jwlf_curves(headers_line, data_example):
    headers = headers_line.split(',')
    curves = []
    i = 0
    while i < len(headers):
        header = headers[i]
        unit = None
        if properties.UNIT_IN_HEADER_REGEX.match(header):
            unit = header \
                .rsplit("(", maxsplit=1)[-1] \
                .split(")", maxsplit=1)[0] \
                .strip()

        data_point = data_example[i]
        data_type = 'string'
        if str(type(data_point)) == "<class 'int'>":
            data_type = 'integer'
        elif str(type(data_point)) == "<class 'float'>":
            data_type = 'float'
        elif str(type(data_point)) == "<class 'bool'>":
            data_type = 'boolean'
        elif str(type(data_point)) == "<class 'str'>":
            try:
                utils.convert_to_datetime(data_point)
                data_type = 'datetime'
            except:
                pass
        curves.append({
            "name": header,
            "valueType": data_type,
            "unit": unit,
            "dimensions": 1
        })
        i += 1

    return curves


def create_data_points(line):
    values = line.split(',')
    data_points = []
    for value in values:
        result = None
        if result is None and value.lower() in ['true', 'false']:
            result = bool(value.lower().capitalize())
        if result is None and value.replace("-", "").isnumeric():
            result = int(value)
        if result is None and value.replace(".", "").replace("-", "").isnumeric():
            result = float(value)
        if result is None:
            try:
                result = utils.convert_to_datetime(value)
                result = result.replace(tzinfo=pytz.utc)
                result = result.isoformat()
            except:
                result = None
        if result is None or 'NaT' == result:
            result = str(value)
        data_points.append(result)

    return data_points


def get_jwlf_curve_unit_header_part(ordered_curve):
    if 'unit' in ordered_curve:
        return ' [' + ordered_curve['unit'] + ']'
    return ''


def map_jwlf_to_sentinel_value_type(jwlf_value_type):
    if jwlf_value_type in properties.JWLF_TO_SENTINEL_DATA_TYPE_MAPPINGS:
        return properties.JWLF_TO_SENTINEL_DATA_TYPE_MAPPINGS[jwlf_value_type]
    return jwlf_value_type


def csv_line_to_jwlf_log(name, headers_line, lines, metadata=None):
    if metadata is None:
        metadata = {properties.SENT_FILE_ID_METADATA_KEY: properties.REAL_TIME_DATA_SENT_FILE_ID}

    data = [create_data_points(line) for line in lines]
    curves = create_jwlf_curves(headers_line, data[0])
    date = datetime.datetime.now(pytz.utc).isoformat()
    if curves[0]['valueType'] == 'datetime' and data[-1][0]:
        date = data[-1][0]
    headers = {
        "name": name,
        "well": properties.WELL,
        "operator": properties.CLIENT,
        "date": date,
        "metadata": metadata
    }

    jwlf = {
        "header": headers,
        "curves": curves,
        "data": data
    }
    return jwlf


def convert_jwlf_to_csv_line(jwlf_data_points, jwlf, ordered_curves):
    ordered_data_points = []
    ordered_curves_index_dict = {}
    actual_curves = jwlf['curves']
    for actual_index in range(len(ordered_curves)):
        ordered_data_points.append('')
        ordered_curve = ordered_curves[actual_index]
        ordered_curves_index_dict[ordered_curve['name']] = actual_index

    for actual_index in range(len(actual_curves)):
        actual_curve = actual_curves[actual_index]
        if actual_curve['name'] not in ordered_curves_index_dict.keys():
            ordered_curves.append(actual_curve)
            raise Exception(
                f"Actual curve '{actual_curve['name']}' not found in ordered curves '{ordered_curves_index_dict.keys()}'")
        ordered_index = ordered_curves_index_dict[actual_curve['name']]
        value = jwlf_data_points[actual_index]
        if value is not None:
            str_value = str(value)
            if type(value) is bool:
                str_value = str_value.lower()
            ordered_data_points[ordered_index] = str_value
    prefix = ''
    if properties.OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED:
        date = ''
        if jwlf['header']['date']:
            date = jwlf['header']['date']
        prefix += date + ','
    return prefix + ",".join(ordered_data_points)


def create_csv_header(jwlf_name, ordered_curves):
    three_lines_header = False
    if 'three-lines-header' in properties.OUTPUT_DATA_NAMES_FILENAMES_DICT[jwlf_name]:
        three_lines_header = properties.OUTPUT_DATA_NAMES_FILENAMES_DICT[jwlf_name]['three-lines-header']
    first_line_prefix = 'date time,' if properties.OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED else ''
    if three_lines_header:
        second_line_prefix = ',' if properties.OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED else ''
        third_line_prefix = 'DateTime,' if properties.OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED else ''
        csv_header = "\n".join([
            first_line_prefix + ",".join(
                [ordered_curve['name'] for ordered_curve in ordered_curves]),
            second_line_prefix + ",".join(
                [ordered_curve['unit'] if 'unit' in ordered_curve else '' for ordered_curve in ordered_curves]),
            third_line_prefix + ",".join(
                [map_jwlf_to_sentinel_value_type(ordered_curve['valueType']) if 'valueType' in ordered_curve else '' for
                 ordered_curve in ordered_curves])
        ])
    else:
        csv_header = first_line_prefix + ",".join(
            [ordered_curve['name'] + get_jwlf_curve_unit_header_part(ordered_curve)
             for ordered_curve in ordered_curves])
    return csv_header
