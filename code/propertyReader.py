import configparser
import os
import logging
import time
import sys


import re
from collections import Counter
config = configparser.RawConfigParser()


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



def haslocalqlik_qvd_name(lines):
    gotmatch = False
    lineno = 0
    for lineno, line in enumerate(lines.split('\r\n')):

        #        logging.info(line)
        if ':' in line:

            if line.strip()[-1] == ':':
                logging.info(line)
                gotmatch = True
                if lines[lineno + 1].split(' ')[0].lower() == 'load':
                    gotmatch = True
                    break
    return gotmatch




def get_tab_name(line):
    db_tab_name = ""
    if 'FROM ' in line:
        from_stat = line.upper().split('FROM ')

        if "WHERE" in from_stat[1].upper():
            db_tab_name = from_stat[1].upper().split('WHERE')[0].replace('\\\"', "").strip()
        else:
            db_tab_name = from_stat[1].upper().replace('\\\"', "").strip()
        #--GEO--C-- if there are join conditions 
        # check from cluase has mutipe join then take only the firsttae name 
        db_tab_name = db_tab_name.split(" ")[0]#??? + " MTFS "
    return db_tab_name


def load_rogue_qvdbuilder_inapp(qvds, filename, key, ldbLoader ):
    prev_qvd_name = key
    inc = 0
    _conn_lib_fromMain = ""
    for qvd in qvds:  ## loop thru each LIB
        #
        inc = inc + 1

        qvd_load = qvd

        # replace the 1st \r\r

        qvd_script_lines = qvd.strip().split("\r\n")
        if len(qvd_script_lines) > 1:
            inc = inc + 1
            qlik_qvd_name = prev_qvd_name

            """
            Our pattenr will be LIB CONNECT connectionname; 
            qvdtablename:
            Load 

            Select .....
            from 
            """
            connection_name= ""
            sqvd_load = ""
            semicolondelimetedline = qvd_load.split(";")
            for line in semicolondelimetedline:
                # get QVD NAME
                if "vQVDTableFileName" in line:
                    #--GEO--C-- Added code to remove trailing space ' vQVDTableFileName' 
                    qlik_qvd_name = line.split(' vQVDTableFileName')[-1].replace('=', '').strip().replace("'",'')
                    qlik_qvd_name = qlik_qvd_name.lower().replace('.qvd', '').upper()

                    logging.info("qvd_name = {}".format(qlik_qvd_name))
                 # check for connect first and get connection name
                if "CONNECT" in line.strip():
                    connection_name = line.strip().split(' ')[-1].strip().replace("'", '')
                    logging.info("Conectionname = {}".format(connection_name))
                elif "LOAD" in line.strip() or 'load' in line.strip():
                    sqvd_load = line.split('LOAD')[-1]
                    logging.info(sqvd_load)    
                    
                elif "SELECT " in line.upper().strip().replace('\r\n', "  ")  :  # found select now split by from
                    db_tab_name= get_tab_name(line)
                    from_stat = line.upper().split('FROM ')
                    # if "WHERE" in from_stat[1].upper():
                    #     db_tab_name = from_stat[1].upper().split('WHERE')[0].replace('\\\"', "").strip()
                    # else:
                    #     db_tab_name = from_stat[1].upper().strip()

                    # load the load script ot differen table 
                   

                    logging.info("Select  = {}\n  from tab = {} ".format(from_stat[0][:30], db_tab_name))
                    asd = 100
                    # # not qvd name found
                    if qlik_qvd_name == "":
                    #     qlik_qvd_name = key
                        if haslocalqlik_qvd_name(from_stat[0]):
                            # removeing where carried from
                            frm = from_stat[0].split(";")[-1]
                            if ':' in frm:
                                qlik_qvd_name = frm.split(":")[0].replace("[", "").replace("]", "")
                                prev_qvd_name = qlik_qvd_name
                            else:
                                qlik_qvd_name = "{}_{}".format(prev_qvd_name, inc)

                            inc = 0
                        else:
                            if qlik_qvd_name == prev_qvd_name:
                                qlik_qvd_name = "{}_{}".format(prev_qvd_name, inc)

                    if connection_name == "":
                        connection_name = _conn_lib_fromMain

                    if sqvd_load != "":
                        ldbLoader.load_qvd_load_text(filename.replace(".txt", ""), key.strip(), connection_name, qlik_qvd_name.strip(),db_tab_name.strip(), sqvd_load)

                    selectcolumns = load_qvd_build_info(from_stat[0].replace("\\t", "\t"))
                    # for loop to fire insert
                    # create table qvd_tab_columns(qlik_app_name varchar(100), qlik_tab_name varchar(200), qlik_conn_name varchar(200), db_tab_name varchar(200), db_col_name varchar(200))
                    logging.info(
                        "qlik_conn_name= {}; qlik_qvd_name = {}; db_tab_name = {}; column count = [{}] {}".format(
                            connection_name, qlik_qvd_name, db_tab_name, len(selectcolumns), key))
                    for column in selectcolumns:
                        column_cleaned = column
                        for rep in ['\\\"', ',']:
                            column_cleaned = column_cleaned.replace(rep, "")
                        logging.info(
                            "qlik_app_name= {}; qlik_tab_name = {} qlik_conn_name= {}; qlik_qvd_name = {}; db_tab_name = {}; db_col_name= {} ".format(
                                filename.replace(".txt", "")
                                , key, connection_name, qlik_qvd_name, db_tab_name, column_cleaned[:20])
                        )
                        ldbLoader.load_qvd_tab_columns(filename.replace(".txt", ""), key.strip(), connection_name,
                                                       qlik_qvd_name.strip(), db_tab_name.strip(), column_cleaned)
            if key.strip().lower()=='main':
                _conn_lib_fromMain = connection_name



def load_qvd_build_info(qvd_load):
    qvd_load = qvd_load.replace("SELECT", "").strip()
    qvd_flds = qvd_load.split("\r\n")
    new_qvd_flds = []
    for fld in qvd_flds:
        if not fld.startswith("//"):
            fld_lines = fld.split(",\r\n")
            for fld_line in fld_lines:
                if not fld_line.strip().startswith("//") and not (fld_line.strip() == ""):
                    new_qvd_flds.append(fld_line.strip())
    return new_qvd_flds
    # return '\n'.join([str(elem) for elem in new_qvd_flds])


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
