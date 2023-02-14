import datetime
import enum
import app_logger
import re
import secrets

import pytz

import utils


class RTInputDataTypes(enum.Enum):
    REAL_TIME = 1
    HISTORICAL = 2


class RunTypes(enum.Enum):
    CONSUMER = 1
    PRODUCER = 2


# CONFIGURABLE PROPERTIES:

# Options are RTInputDataTypes.HISTORICAL or RTInputDataTypes.REAL_TIME
RT_INPUT_DATA_TYPE = RTInputDataTypes.HISTORICAL

# Well name is configurable too.
# There may be only one Sentinel RT Lite SaaS running for the same well at the same time.
WELL = "Default"

# For REAL_TIME mode: the period of sending new input data line
# For HISTORICAL mode: it doesn't change the speed of sending input data because in historical mode all input data is sent at the beginning
# For each of mode: the period of checking and pulling new output data is two times higher than the value below
DATA_SYNCH_RATE_IN_SECONDS = 1.0

# Source file for timeseries input data
SOURCE_DATA_FILENAME = "Sample_1_Second_Data.csv"


# Constants
BROOK_URL = 'https://dev-brook.eastus.cloudapp.azure.com/api/v1'
UNION_URL = 'https://theunion.cloud/api/v1'
KEYCLOAK_URL = 'https://users.intellicess.com'

USERNAME = 'Writer_gmail'
PASSWORD = 'Writer_gmail'
KEYCLOAK_CLIENT_ID = 'dev-brook-client'

KEYCLOAK_CLIENT_SECRET = 'i7kKXAjbF600wLuPyWmiODptPFysdGWf'

CLIENT = 'public-test'
REGION = 'texas'
LOG_INPUT_UNION_FOLDER = 'sentinel-saas-lite_input-folder'
LOG_OUTPUT_UNION_FOLDER = 'sentinel-saas-lite_output-folder'
GENERAL_DATA_INPUT_UNION_FOLDER = 'sentinel-saas-lite_input-folder'
GENERAL_DATA_OUTPUT_UNION_FOLDER = 'sentinel-saas-lite_output-folder'
BROOK_BOT_TYPE = "Sentinel Saas Lite"
MAX_GET_BOT_ID_REQUESTS = 300
BOT_ID_REQUESTS_RETRY_SLEEP_SECONDS = 1.0
BATCH_SIZE = 3000
DATE_TIME = datetime.datetime.now(pytz.utc).strftime('%m_%d_%Y_T%H_%M')
INPUT_FILES_LOCAL_FOLDER = "input_files"
SENT_DATA_INPUT_FILES_LOCAL_FOLDER = f"{INPUT_FILES_LOCAL_FOLDER}/sent-data"
OUTPUT_FILES_LOCAL_FOLDER = "output_files"
BROOK_RT_INPUT_DATA_FILENAME_POSTFIX = "_Second_Data.csv"
MICROSECONDS_PER_SECOND = 1000000
CONFIG_PROPERTIES_PATH = 'cfg/config.properties'
OUTPUT_FILES_SYNCH_TIMESTAMP_FILENAME = 'output_files_synch_timestamp.local'
LOGS_FOLDER = "logs"
OUTPUT_FILES_SYNCH_TIMESTAMP_FILE_PATH = f"{LOGS_FOLDER}/{OUTPUT_FILES_SYNCH_TIMESTAMP_FILENAME}"
UNIT_IN_HEADER_REGEX = re.compile('.*\(.*\)')
# lower date raises exception when timestamp() method is called
MIN_DATE_TIME = datetime.datetime.strptime('3/1/1970', '%d/%m/%Y')
# note that “%f” will parse all the way up to nanoseconds
# https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
DATE_FORMATS_ORDERED_BY_PRIORITY = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%Sa",
    "%m/%d/%Y %H:%M:%S a",
    "%m/%d/%Y %H:%M:%S a ",
    "%m/%d/%Y %H:%M:%Sa ",
    "%m/%d/%Y %H:%M:%Sa%z",
    "%m/%d/%Y %H:%M:%S a%z",
    "%m/%d/%Y %H:%M:%S a %z",
    "%m/%d/%Y %H:%M:%Sa %z",
]
# no data for 5 minutes implies consumer running finish
LAST_DATA_FETCH_MAX_DELAY_MILLISECONDS = 300000
SENT_FILE_ID_METADATA_KEY = 'sentFileId'
REAL_TIME_DATA_SENT_FILE_ID = secrets.token_hex(16)
SENTINEL_CLOUD_CLIENT_LOGS = f"{LOGS_FOLDER}/sentinel-cloud-client.logs"
HEADERS_DICT_SAVE_FILE_PATH = f"{LOGS_FOLDER}/headers_dict_save.local"
ORDERED_CURVES_DICT_SAVE_FILE_PATH = f"{LOGS_FOLDER}/ordered_curves_dict_save.local"
FILE_ORDINAL_PREFIX = '_vol'
VOL_POSTFIX_OUTPUT_DATA_LIST = ['REAL_TIME', 'LOGS']
SPLIT_BY_BHA_METADATA_KEY = 'splitByBhaPart'
JWLF_TO_SENTINEL_DATA_TYPE_MAPPINGS = {
    'boolean': 'Boolean',
    'string': 'String',
    'integer': 'Integer',
    'float': 'Real',
    'datetime': 'DateTime',
}
CLEAN_START = True

ACTIVE_RUN_TYPES = [RunTypes.CONSUMER, RunTypes.PRODUCER]

BROOK_SENT_RT_INPUT_DATA_FILENAME = f"sent-Sample_{DATA_SYNCH_RATE_IN_SECONDS}{BROOK_RT_INPUT_DATA_FILENAME_POSTFIX}"

BROOK_SENT_RT_INPUT_DATA_FILE_PATH = f"{SENT_DATA_INPUT_FILES_LOCAL_FOLDER}/{BROOK_SENT_RT_INPUT_DATA_FILENAME}"

OUTPUT_DATA_ROW_LIMIT = 3600

CONSOLE_LOGGING_OUTPUT_DATA_CONTENT = RT_INPUT_DATA_TYPE == RTInputDataTypes.REAL_TIME

NOT_MATCHED_OUTPUT_FILENAME_POSTFIX = '_Not_Matched_Filename.csv'
OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX = f"{DATE_TIME}.csv"


OUTPUT_DATA_COLUMN_DATE_TIME_ENABLED = False
MAX_ACCESS_TOKEN_AGE_SECONDS = 60
MAX_NUMBER_OF_IDS_PER_REQUEST = 25
RECEIVED_NEW_LINES_PRINTING_FREQUENCY = 2

OUTPUT_DATA_NAMES_FILENAMES_DICT = {
    'REAL_TIME': {
        'filename': f"{WELL}_Sentinel_Time_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
        'appended': True,
        'jwlf': True,
        'three-lines-header': True
    },
    'RIG_STATE_DATA': {
        'filename': f"{WELL}_Sentinel_RigState_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
        'appended': True,
        'jwlf': True,
    },
    'S2S_SLIP_TO_SLIP_TEMPORARY': {
        'filename': f"{WELL}_Sentinel_S2STemp_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
        'appended': True,
        'jwlf': True,
    },
    'S2S_SLIP_TO_SLIP': {
        'filename': f"{WELL}_Sentinel_S2S_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
        'appended': False,
        'jwlf': True,
    },
    'W2W_WEIGHT_TO_WEIGHT': {
        'filename': f"{WELL}_Sentinel_W2W_Outputs_{OUTPUT_JWLF_FILENAMES_DYNAMIC_POSTFIX}",
        'appended': True,
        'jwlf': True,
    },
    'BROOK_DATA_PROCESSING_NOTE': {
        'output_data_full_file_path': f"{LOGS_FOLDER}/{WELL}_Sentinel_Processing_Notes.txt",
        'appended': True,
        'jwlf': False,
    },
    'LOGS': {
        'appended': True,
        'jwlf': False,
        'output_data_full_file_path': f"{LOGS_FOLDER}/{WELL}_logs.log",
        'value_mapper': lambda lines: app_logger.map_log_json_to_log_format(lines)
    },
}

INPUT_DATA_TYPES_DICT = {
    'HISTORICAL': {
        'priority': 0,
        'filename_config': {
            'property': 'inputdatatimebased.filename',
            'property_value_index': 0,
            'filename_match_strategy': lambda filename_from_property, input_filename: utils.historical_match(filename_from_property, input_filename)
        },
        'metadata_headers_count': 0,
        'sent_data_log_filename': f"sent_historical-data.csv"
    }
}


# Global variables
sentinel_exit = False
