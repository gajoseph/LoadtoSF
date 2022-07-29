
import logging
import sys
import time
import json 
import re

from datetime import datetime
from postgresDbloader import dbLoader

import propertyReader as pr
import commonfun as comfun
from  qlik_app_tab_load_script import qlikapptabload
from qlik_app_tab_load_residentqvd import qlikappTabResidentloader

from qlikWS import qlikeWS
qlikappNames = ""


def getScriptExdetails(qDocId :str , qDocName :str, tqlikeWS : qlikeWS):
    """
    for a given app's  docid and docname qlik wss connection class
    Method GetScriptEx
    
    """
    loadScript = ""
    appinfo = tqlikeWS.OpenQlikApp(qDocId, qDocName, 2)
    if "result" in appinfo.keys():
        logging.info("App handle for {} = [{}]".format(
            qDocName, appinfo['result']['qReturn']['qHandle']))
       
        app_handle = appinfo['result']['qReturn']['qHandle']
        # get the qliksheets
        loadScript = tqlikeWS.GetScriptEx(app_handle)
        loadScript = loadScript['result']['qScript']
    return loadScript



def getscript(appname, __prop_dict):
    loadScript =""
    
    tqlikeWS = qlikeWS(logging.getLogger(), __prop_dict)
    tqlikeWS.connect1('aaa-373e-4bff-bec0-fd3411ffa6en')

    a = tqlikeWS.getQlikAppList()
    # print(a, "getQlikAppList")

    tqlikeWS.close_conn(1)  # close if already open

    qlik_wss ={}

    for dic in a['result']['qDocList']:
        qlik_wss[dic['qDocName']] = dic['qDocId']
      
    print("============================{}===========================")
    qlikappNames =  [k for k, v in qlik_wss.items()]
    # loop thru the list of appnames provided by earnest
    
    for qlikappname in qlikappNames:
        # check if file contains qlikappname or qlikappanme contains filename
        
        
        if appname.strip() in qlikappname: # or qlikappname.strip() in filename:
            logging.info("app name found from earnestsList =  {}  qlik appname = {}".format(appname, qlikappname) )
            found= True
            logging.getLogger().setLevel(logging.ERROR)
            docid = qlik_wss[qlikappname]
            docname = qlikappname 

            tqlikeWS.connect1(docid)
            loadScript = getScriptExdetails(docid, docname, tqlikeWS )

            tqlikeWS.close_conn(docid)
            logging.getLogger().setLevel(logging.ERROR)
            break    
       
    del tqlikeWS
    return loadScript


   

def parse_qlik_files(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

    config = pr.getPropInstance('config/', 'NLP.properties')
    __prop_dict = dict(config.items('log'))
    logFile = __prop_dict["logfilepath"] + sys.argv[0].split('\\')[-1] + "_" + os.getlogin() + "_" + time.strftime(
        '%Y%m%d%H%M%S') + '.log'

    log_format = '%(asctime)s - [%(levelname)s] - [%(filename)s->%(funcName)s():%(lineno)d] - %(message)s'
    log_date_format = '[%m/%d/%Y %I:%M:%S %p]'

    logging.basicConfig(
        level=logging.INFO
        # , filename = logFile
        , format=log_format
        , datefmt=log_date_format
    )

    # ch = logging.StreamHandler(sys.stdout)
    # logging.getLogger().addHandler(ch)

    tabs = {}
    qvd_dict = {}
    new_tabs = {}

    # path = "D:\\Users\\tgaj2\\Documents\\test"
    path = "D:\\Users\\tgaj2\\Documents\\DDO"
 
    sql = open(os.path.join(path, "\\sap.sql"), "w",encoding="utf8")

    qli_app_name_tab_qvd_name = open(os.path.join(path, "\\qli_app_name_tab_qvd_name11.sql"), "w", encoding="utf8")

    lists_path = "D:\\Users\\tgaj2\\Documents\\mrotest.txt"


    """ creating db loader 
    
    """
    
    path = "D:\\Users\\tgaj2\\Documents\\MRO_ALL"

    lists_path = __prop_dict['qlik.app.list.loadscript']

    ldbLoader = dbLoader(__prop_dict)
    ldbLoader.init_db_conn()

    """ truncate table """
    # ldbLoader.truc_qlik_apps_info()
    # ldbLoader.truc_QVD_tab_columns()

    with open(lists_path, "r", encoding="utf8")  as f:
        filenames = f.readlines()

    for filename in filenames:
    
        # delete already loaded stuff from each table
        
        ldbLoader.del_qvd_tab_columns_by_app_name( filename.replace(".txt", "")) 
        ldbLoader.del_qlik_app_qvd_flds_by_app_name( filename.replace(".txt", ""))
        ldbLoader.del_load_qvd_load_text( filename.replace(".txt", ""))

        filename = filename.strip() + ".txt"
        logging.info("  {}  ".format(filename))

        tabs = {} # qlik load script tabs 
        qvd_dict = {}
        qvd_build_dict = {}
        new_tabs = {}

        lines=[]

        file_exists = os.path.exists(os.path.join(path, filename))
        if file_exists:

            with open(os.path.join(path, filename), "r", encoding="utf8")  as f:
                lines = f.readlines()
        else:
            logging.info("--GEO--C-- file not found {} {}\n".format(filename, path))
            sql.write("--GEO--C-- file not found  {} {}\n".format(filename,path))

            # call proc to call QLIK api to getScriptEx and write to file
            #
            lines = getscript(filename.replace(".txt", ""), __prop_dict)
            f = open(os.path.join(path, filename), "w", encoding='utf8')
            f.write(json.dumps(lines))
            f.close()
            with open(os.path.join(path, filename), "r", encoding="utf8")  as f:
                lines = f.readlines()
            
        logging.info("--GEO--C-- file name {}\n".format(filename))

        index = 0
        for index, l in enumerate(lines):

            logging.info("file:[{}]; line [{}]  length= {}".format(filename , index, len(l)))
            temp = l.split("///$tab") #split by keyword
            logging.info(" tabs count   {}".format(len(temp)))

            for ll in temp:
               # print(ll[0])
                # tab_list = [ 'Catalog Master', 'Catalog: MP2']#'MP2DW_PORECEIPTS', 'Facility', 'Direct Order Analysis', 'Resident Direct Order']
                # if ll.split("\\r\\n")[0].strip() in tab_list:
                tabs[ll.split("\\r\\n")[0]] = '\\n'.join(ll.split("\\r\\n")[1:])
                logging.info(" creating tabs dict tabs [{}]   {}".format(ll.split("\\r\\n")[0],len('\n'.join(ll.split("\\r\\n")[1:]))))

       
        for key, value in tabs.items():
            qvd_name = key
            prev_qvd_name = key
            inc = 0
            # if ("MDM" in key or "MDM" in value) and "(qvd)" in value:
            cleaned_value = comfun.cleanup_fld(value)
            logging.info("Value after cleanup  {}".format(cleaned_value[:30]))
            qvds =[]
            if "(QVD)" in cleaned_value:
                qvds = cleaned_value.split("(QVD)")  # Split by (qvd)
            if "(qvd)" in cleaned_value:
                qvds = cleaned_value.split("(qvd)")  # Split by (qvd)
                logging.info("---------------------------------{}".format(" (qvd) split"))

            logging.info("key: {}  has  qvd   {}".format(key, len(qvds)))
            # split the qvd load script by keyword (qvd)
            vQVDTableFileName = ""
            if len(qvds) > 0:
                for qvd in qvds:
                    inc = inc + 1
                    qvd_name = prev_qvd_name
                    qvd_load = qvd
                    # replace the 1st \r\r

                    logging.info("\nStarting QVD [{}]/[{}]\n".format(inc, len(qvds) ))
                    if vQVDTableFileName=="" and "vQVDTableFileName".lower() in qvd.lower():
                        vQVDTableFileName = qvd.split(' vQVDTableFileName')[-1].replace('=', '').strip().replace("'",'')
                        vQVDTableFileName = vQVDTableFileName.lower().replace('.qvd', '').upper().split(";")[0]

                    qvd_script_lines = qvd.strip().split("\r\n")
                    if len(qvd_script_lines) > 1:
                        from_clauses = qvd_load.split("FROM")
                        
                        # Chek for local qlik name 
                        if pr.haslocalqlik_qvd_name(qvd_load.split("FROM")[0]):
                            #removeing where carried from
                            frm = from_clauses[0].split(";")[-1]
                            if ':' in frm:
                                qvd_name =frm.split(":")[0].replace("[", "").replace("]", "")
                                prev_qvd_name = qvd_name
                            else:
                                qvd_name = "{}_{}".format(prev_qvd_name, inc)

                            inc = 0
                        else:
                            if qvd_name == prev_qvd_name:
                                    qvd_name = "{}_{}".format(prev_qvd_name, inc)


                        if len(from_clauses) > 1:
                            #for loop the loop thru each from clause
                            #for eachfrom in len(from_clauses)-1:
                            for i in range(0, 1):
                                eachfrom = from_clauses
                                logging.info(" tab {} has from {}".format(key, eachfrom[-1][:50]))
                                eachfrom = qvd_load.split("FROM")[-1].split(';')[0]  # get the last part
                                if eachfrom.find(']') > 0:
                                    eachfrom = eachfrom[: eachfrom.find(']')]

                                eachfrom.replace("lib:/", "").replace("[", "").replace("]", "")

                                for rep in ['\\\"']:
                                    eachfrom = eachfrom.replace(rep, "")

                                if ".qvd" in eachfrom.lower() :  # add an if beofre that into
                                    qlik_qvd_name = qvd_load.split("FROM ")[-1].strip()
                                    """
                                        localtab_name:  
                                            Load *, fld fld ;
                                            load *, fld, fld from [lib://];
                                        1st split by from 
                                        2nd split by :
                                        3rd split by ;
                                        
                                    """
                                    
                                    #process_load_qlik_apps(qvd_load_script, qlik_tab_name, qlik_app_local_qvd_name, qlik_qvd_name, qlik_qvd_cols )
                                    #Using regular expression to avoid split by : and ; if inside a quote
                                    
                                    newStr = re.split(r":(?=')", qvd_load.split("FROM ")[0])
                                    newStr = newStr[-1]
                                    newStr = re.split(r';(?=")', newStr)
                                    # qlik_qvd_cols = qvd_load.split("FROM ")[0].strip().split(':')[-1].split(';')[-1]
                                    qlik_qvd_cols = newStr[-1]
                                    # Calling the method to load each load  script within qlik load tab 
                                    tqlikapptabload = qlikapptabload(filename.replace(".txt", "")
                                            , key
                                            , qvd_name
                                            , qlik_qvd_name
                                            , qlik_qvd_cols)
                                    tqlikapptabload.ldbLoader = ldbLoader
                                    # Parses and loads fields defined in load stat load fld1..n resident qvd
                                    tqlikapptabload.load_qlik_app_tab_data() # see method for more info
                                    #load data to qlik_app_qvd_flds
                                    tqlikapptabload.load_qlik_app_tab_fields()
                                    del tqlikapptabload     

                                elif  ".xls" in eachfrom.lower() or ".csv" in eachfrom.lower() :
                                    # xls file as data source. 
                                    semicolondelimetedline = qvd_load.split(";")
                                    if len(eachfrom.strip().split("/")) > 0:
                                        db_tab_name = eachfrom.strip().split("/")[-1]
                                    else:
                                        db_tab_name = qvd_name
                                   
                                    if pr.haslocalqlik_qvd_name(qvd_load.split("FROM")[0]):
                                        logging.info(
                                            "qvd_load_from[0]_: = {}".format(qvd_load.split("FROM")[0].strip().split(':')[-1]))

                                        qlik_qvd_cols = qvd_load.split("FROM")[0].strip().split(':')[-1]
                                        qlik_qvd_name = qvd_load.split("FROM")[0].strip().split(':')[0].split(';')[
                                            -1]  # to handel nWHERE LEFT(APPID,7)<>\'SESSION\'\r\nAND SUBSTRINGCOUNT(\\"USERID\\",\'SVC_QLIKPROD\') = 0\r\n;\r\n[SESSION USER INFO]:\r\nIF(ISNULL(FACILITY)
                                    else:
                                        qlik_qvd_cols = qvd_load.split("FROM")[0].strip().split(':')[-1]
                                        qlik_qvd_name = key.strip()
                                    if not "," in eachfrom:
                                        logging.info(eachfrom)
                                        sqvd_load = qvd_load.split('LOAD')[-1].split(';')[0]
                                        logging.info(sqvd_load)
                                        eachfrom= eachfrom.strip()
                                        ldbLoader.load_qvd_load_text(filename.replace(".txt", ""), key.strip(), eachfrom, qlik_qvd_name.strip(),db_tab_name.strip(), sqvd_load)

        
                                        selectcolumns = pr.load_qvd_build_info(qlik_qvd_cols.replace("\\t", "\t"))
                                        # for loop to fire insert
                                        # create table qvd_tab_columns(qlik_app_name varchar(100), qlik_tab_name varchar(200), qlik_conn_name varchar(200), db_tab_name varchar(200), db_col_name varchar(200))

                                        for column in selectcolumns:
                                            column_cleaned = column
                                            for rep in ['\\\"', ',']:
                                                column_cleaned = column_cleaned.replace(rep, "")
                                            logging.info(
                                                "qlik_app_name= {}; qlik_tab_name = {}; qlik_conn_name= {}; qlik_qvd_name = {}; db_tab_name = {}; db_col_name= {} ".format(
                                                    filename.replace(".txt", "")
                                                    , key, eachfrom, qlik_qvd_name, db_tab_name, column_cleaned)
                                            )
                                            # qvd_build_dict["{}.{}".format(qvd_name,db_tab_name)] = {'db_tab_name' : db_tab_name, 'cols': pr.load_qvd_build_info(qvd_load.replace("\\t", "\t"))}
                                            ldbLoader.load_qvd_tab_columns(filename.replace(".txt", ""), key.strip(),
                                                                           eachfrom.strip(), qlik_qvd_name.strip(),
                                                                           db_tab_name.strip(), column_cleaned)
                                else:
                                    libs = qvd.split("LIB ")  # Split by LIB
                                    logging.info("key: {}  has 4 LIB connect {}".format(key, len(libs)))
                                    if vQVDTableFileName !="":
                                        pr.load_rogue_qvdbuilder_inapp(libs, filename, vQVDTableFileName, ldbLoader)
                                    else:
                                        pr.load_rogue_qvdbuilder_inapp(libs, filename, key, ldbLoader)
                        else:
                            # check if we are pulling from resident qvds
                            #  check if there is load without a from
                            # if "load "
                            tqlikappTabResidentloader = qlikappTabResidentloader(filename.replace(".txt", "")
                                            , key
                                            , ""
                                            , qvd_load
                                            , 1)
                            tqlikappTabResidentloader.ldbLoader = ldbLoader
                            
                            tqlikappTabResidentloader.getResidents_qvds_in_query()
  
                            del tqlikappTabResidentloader
                            logging.info("qvd_script_lines[0] = {}".format(qvd_script_lines[0]))           
            else: # with a qvd there could a rogue qvd builder capturing that
                libs = cleaned_value.split("LIB ")  # Split by LIB
                logging.info("filename = {}key: {}  has 4 LIB connect {}".format(filename, key, len(libs)))
                pr.load_rogue_qvdbuilder_inapp(libs, filename, key, ldbLoader)
                if len(libs) ==0:
                    sql.write("--GEO--C-- File not processed  {} {}\n".format(filename,path))
 
    sql.close()
    qli_app_name_tab_qvd_name.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = datetime.now()
    print("started at {}: ended at {}", start)
    parse_qlik_files('PyCharm')
    end = datetime.now()
    print("started at {}: ended at {}".format( start, end ))
