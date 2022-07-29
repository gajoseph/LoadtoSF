import logging
from typing import Dict, List, OrderedDict
import propertyReader as pr
import sys
import datetime
import os
import json

import time
from datetime import datetime
import commonfun as cf
# from qlikWS import qlikeWS
from postgresDbloader import dbLoader
import typing
from qliksheet import qliksheet


class qliksheets:
    """
    qlik sheet object collection class
        pull all the child objectsina sheet 
        Methods to load data to Db
    """

    qliksheetscount = 0
    qList = []
    qSheet_comp_qFieldDefs = {
        'qId': "", 'qType': "", 'qFields': {}, 'qParentId': "", 'qParentType': ""
    }
    qSheetControls = {}

    def __init__(self, response, app_handle, tqlikWSS, app_id, app_name):
        """
        Params:
            response : json message from qlik

        """

        logging.info("Constructor called for object {} --> {}".format(
            __class__.__name__, self.__class__.__name__))
        self.app_handle = app_handle
        self.tqlikWSS = tqlikWSS
        self.app_id = app_id
        self.app_name = app_name
        self.ldbLoader = dbLoader(tqlikWSS.get__prop_dict())
        self.ldbLoader.init_db_conn()

        if "result" in response.keys():
            self.qList = response['result']['qList']
            self.qliksheetscount = len(self.qList)

    def __del__(self):
        del self.tqlikWSS
        del self.ldbLoader
        logging.info("Destructor called for object [{}] ".format(
            __class__.__name__))

    def getqlikSheets_new(self):
        """
            for a given app's qlist get the sheets and get the list of objects used in the sheets along with qvd columns
            Load into db table : ins_qlik_app_sheet_controls

        """
        qSheetName = ''
        qSheetId = ""

        for qsheet in self.qList:
            qSheetName = qsheet['qMeta']['title']  # Sheet title
            #if qSheetName == "SLOB Overmax Max Opt":

            # Sheet's id UUID
            qSheetId = qsheet['qInfo']['qId']
            #  intantiate qlikSheetObj
            lqsheet = qliksheet(self.tqlikWSS, qSheetId,
                                qSheetName, self.app_handle)

            # Passing the ldbLoader to sheet instance object
            lqsheet.ldbLoader = self.ldbLoader

            # Get the objects its details for a sheet 
            lqsheet.getsheetObjects()
            
            lqsheet.sheet_comp_dict.items

            # Delete data if exists
            self.ldbLoader.del_qlik_app_sheet_controls(self.app_id, qSheetId)

            # load the data into database
            lqsheet.load_sheet_db(self.app_id, self.app_name)

            del lqsheet

    def getqlikSheets(self):
        """
            for a given app's qlist get the sheets and get the list of objects used in the sheets along with qvd columns
            Load into db table : ins_qlik_app_sheet_controls

        """
        return self.getqlikSheets_new()


def main():
    config = pr.getPropInstance('config/', 'NLP.properties')
    __prop_dict = dict(config.items('log'))
    logFile = __prop_dict["logfilepath"] + sys.argv[0].split('\\')[-1] + "_" + os.getlogin() + "_" + time.strftime(
        '%Y%m%d%H%M%S') + '.log'

    log_format = '%(asctime)s - [%(levelname)s] - [%(filename)s->%(funcName)s():%(lineno)d] - %(message)s'
    log_date_format = '[%m/%d/%Y %I:%M:%S %p]'

    logging.basicConfig(
        level=logging.INFO        # , filename = logFile
        , format=log_format, datefmt=log_date_format
    )

    # ch = logging.StreamHandler(sys.stdout)
    # logging.getLogger().addHandler(ch)
    l = [1, 2, 3]

    lqsheets = qliksheets(l)

    print(lqsheets)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = datetime.now()
    print("started at {}: ended at {}", start)
    main()
    end = datetime.now()
    print("started at {}: ended at {}".format(start, end))
