import configparser
import logging
import os
import sys
import time
from collections import Counter
from yaml import safe_load
from os import environ
from google.cloud import secretmanager
config = configparser.RawConfigParser(delimiters = ":")


def get_yaml_properties(file_name:str):
    with open(file_name, "r") as config_stream:
        config = safe_load(config_stream)
    if(config['CREDENTIALS_PATH']):
            environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["CREDENTIALS_PATH"]
    client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{config['PROJECT_ID']}/"\
                    f"secrets/{config['AZUREAD_CLIENT_SECRET']}/versions/latest"
    password_name = f"projects/{config['PROJECT_ID']}/"\
                    f"secrets/{config['COLLIBRA_PASSWORD']}/versions/latest"

    secret_response = client.access_secret_version(name=secret_name)
    password_response = client.access_secret_version(name=password_name)

    config["AZUREAD_CLIENT_SECRET"] = secret_response.payload.data.decode('UTF-8')
    config["COLLIBRA_PASSWORD"] = password_response.payload.data.decode('UTF-8')
    # # __prop_dict = config
    
    return config 


def getPropInstance(sdirPath, sfileName):
    fileDir = os.path.dirname(sdirPath)
    filename = os.path.join(fileDir, sfileName)
    config.read(filename)
    return config


def getPropDict(item):
    return dict(config.items(item))


def retun_file_content_asString(file_path):
    """
        This method reads an entire file and converts into a string
    :param fileName:
    :return: String as (filecontent)
    """
    if os.path.isfile(file_path):
        filecontents = open(file_path, 'r').readlines()
        return ' '.join(map(str, filecontents))
    else:
        return ''

def get_heyStackARCforSynonym(acronyms, search)-> list:
    returnList = []
    for acronym in acronyms:
        if acronym[0]==search:
            returnList = acronym
            break
    return returnList



def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


def __setLoggingInfo(__log_prop_dict):
    """
    __log_prop_dict: dict w/ log info
    private method
    """
    logFile = __log_prop_dict["logfilepath"] + sys.argv[0].split("/")[-1][
                                               0:-3] + "_" + os.getlogin() + "_" + time.strftime(
        '%Y%m%d%H%M%S') + ".log"

    """
    LOGGING SETTINGS
    """
    logging.basicConfig(
        filename=logFile
        , level=logging.DEBUG
        , format=__log_prop_dict["log.format"]
        , datefmt=__log_prop_dict["log.date.format"]
    )

    root = logging.getLogger()
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    # ch.setLevel(logging.ERROR)
    formatter = logging.Formatter(__log_prop_dict["log.format"], datefmt=__log_prop_dict["log.date.format"])
    ch.setFormatter(formatter)
    root.addHandler(ch)
