import datetime
import time

import app_logger

import pandas
import requests

import properties


_used_date_formats = []

def date_time_to_milliseconds_timestamp(date_time):
    return int(float(date_time.timestamp()) * 1000.0)


def milliseconds_timestamp_to_date_time(milliseconds_timestamp):
    return datetime.datetime.fromtimestamp(milliseconds_timestamp / 1000.0)


def convert_to_datetime(value):
    global _used_date_formats

    formats = []
    formats.extend(_used_date_formats)
    formats.extend(properties.DATE_FORMATS_ORDERED_BY_PRIORITY)
    for format in formats:
        try:
            result = pandas.to_datetime(value, format=format)
            if result != 'NaT':
                if format not in _used_date_formats:
                    _used_date_formats.append(format)
                    properties.DATE_FORMATS_ORDERED_BY_PRIORITY.remove(format)
                return result
        except:
            pass
    raise Exception('NaT')


def flat(elements):
    return [y for x in elements for y in x]


def run_request_with_retries(fun, default_response=None, max_retries=24 * 3600):
    sent = False
    response = default_response
    request_index = 0
    while not sent and request_index < max_retries:
        try:
            response = fun()
            sent = True
        except Exception as e:
            sent = False
            app_logger.log(f"Error occured: {e}")
            time.sleep(1)
    return response


def historical_match(filename_from_property, input_filename):
    return properties.RTInputDataTypes.HISTORICAL == properties.RT_INPUT_DATA_TYPE and filename_from_property == input_filename
