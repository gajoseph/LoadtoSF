

import logging
import inspect
import os
import sys
import time

from sqlalchemy import true
import propertyReader as pr
import threading
from qliksheets import qliksheets
import typing
from qlikWS import qlikeWS

def getappdetails(qDocId :str , qDocName :str, tqlikeWS : qlikeWS):
    appinfo = tqlikeWS.OpenQlikApp(qDocId, qDocName, 2)
    if "result" in appinfo.keys():
        logging.info("App handle for {} = [{}]".format(
            qDocName, appinfo['result']['qReturn']['qHandle']))
       
        app_handle = appinfo['result']['qReturn']['qHandle']
        # get the qliksheets
        qsheets = tqlikeWS.getobjects(app_handle, "sheet")

        lqliksheets = qliksheets(qsheets
                                , app_handle
                                , tqlikeWS
                                , qDocId
                                , qDocName)

        lqliksheets.getqlikSheets()
        del lqliksheets
        
       
    tqlikeWS.close_conn(qDocId)  # close if already open
    # now loop thru the list


def main():
    config = pr.getPropInstance('config/', 'NLP.properties')
    __prop_dict = dict(config.items('log'))

    logFile = __prop_dict["logfilepath"] + sys.argv[0].split(
        "/")[-1][0:-3] + "_" + os.getlogin() + "_" + time.strftime('%Y%m%d%H%M%S') + ".log"
    # --GEO--C-- LOGGING SETTINGS
    
    logging.basicConfig(
       # filename=logFile,
        level=logging.INFO, format=__prop_dict["log.format"], datefmt=__prop_dict["log.date.format"]
    )

    root = logging.getLogger()
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        __prop_dict["log.format"], datefmt=__prop_dict["log.date.format"])
    ch.setFormatter(formatter)


    #lists_path = "D:\\Users\\georgj3\\Documents\\mro_dashboard.txt"
    lists_path = "D:\\Users\\georgj3\\Documents\\MDM_extract.txt"
    lists_path = "D:\\Users\\georgj3\\Documents\\MDM_supplier.txt"
    #MDM_Catalog
    lists_path = "D:\\Users\\georgj3\\Documents\\MDM_Catalog.txt"
    #lists_path = "D:\\Users\\georgj3\\Documents\\skipped.txt"
    lists_path = __prop_dict['qlik.app.list.controls']
    
    with open(lists_path, "r", encoding="utf8")  as f:
        filenames = f.readlines()

    filenames = [w.replace('\n', '') for w in filenames]

    tqlikeWS = qlikeWS(logging.getLogger(), __prop_dict)
    tqlikeWS.connect1('583eb6e1-373e-4bff-bec0-fd2411eea6eb')

    a = tqlikeWS.getQlikAppList()
    # print(a, "getQlikAppList")

    tqlikeWS.close_conn(1)  # close if already open

    qlik_wss ={}

    for dic in a['result']['qDocList']:
        qlik_wss[dic['qDocName']] = dic['qDocId']
      
    print("============================{}===========================")
    qlikappNames =  [k for k, v in qlik_wss.items()]
    # loop thru the list of appnames provided by Chad
    for filename in filenames:
        found = False
        # loop thru list of apps from qlik server 

        for qlikappname in qlikappNames:
            # check if file contains qlikappname or qlikappanme contains filename
            
           
            if filename.strip() in qlikappname: # or qlikappname.strip() in filename:
                logging.info("app name found from ChadsList =  {}  qlik appname = {}".format(filename, qlikappname) )
                found= True
                logging.getLogger().setLevel(logging.ERROR)
                docid = qlik_wss[qlikappname]
                docname = qlikappname 

                tqlikeWS.connect1(docid)
                getappdetails(docid, docname, tqlikeWS )

                tqlikeWS.close_conn(docid)
                logging.getLogger().setLevel(logging.ERROR)
                break
        # finally print if not found     
        if not found:
            logging.info("app name not found from ChadsList =  {}  ".format(filename) )

    del tqlikeWS


if __name__ == "__main__":
    main()
