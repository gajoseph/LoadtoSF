import logging
from typing import Dict, List, OrderedDict

import sys
import datetime
import os 
import json

import time
from datetime import datetime

# from qlikWS import qlikeWS
import propertyReader as pr 
from postgresDbloader import dbLoader

import commonfun as cf

from qlikObj import qlikObj, comp_qFieldDefs

class qliksheet(qlikObj):
    """
     qlik sheet object  class
        pull all the child objectsina sheet 
        Methods to load data to Db
    """
   
    def __init__(self, tqlikWSS , sheet_id :str , sheet_name:str, app_handle : int ):
        """
        Params:
            response : json message from qlik

        """
        qlikObj.__init__(self,sheet_id, sheet_name, "sheet" )
        self.app_handle= app_handle
        logging.info("Constructor called for object {} --> {}".format(__class__.__name__, self.__class__.__name__))
        self.tqlikWSS = tqlikWSS
        
        self.sheet_comp_dict = {}

        self.sheet_start_dt = datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
    
 
    def exec_ins_qlik_app_sheet_controls( self, qlik_app_id, qlik_app_name, sheet_id, sheet_name, parent_control_id
                            , parent_control_type, control_id, control_type , control_title, control_fld_label, control_fld_field  ):
        
        logging.info("app_id{}, app_name{}, sheet_id = {}, sheet_name ={}, parent_control_id = {}, parent_control_type = {}, control_id = {}, control_type = {}, control_fld_label= {}, control_fld_field = {}".format(
                                qlik_app_id, qlik_app_name, sheet_id, sheet_name, parent_control_id
                            , parent_control_type, control_id, control_type, control_title , control_fld_label, control_fld_field
                            ))
        try:
            self.ldbLoader.ins_qlik_app_sheet_controls(qlik_app_id
                    , qlik_app_name
                    , sheet_id
                    , sheet_name
                    , parent_control_id
                    , parent_control_type
                    , control_id, control_type 
                    , cf.convertIfdict_json(control_title)
                    , control_fld_label
                    , control_fld_field )  
        except Exception as e :
            logging.error(e)
            logging.error(" error for the following \n app_id{}, app_name{}, sheet_id = {}, sheet_name ={}, parent_control_id = {}, parent_control_type = {}, control_id = {}, control_type = {}, control_fld_label= {}, control_fld_field = {}".format(
                                qlik_app_id, qlik_app_name, sheet_id, sheet_name, parent_control_id
                            , parent_control_type, control_id, control_type , control_fld_label, control_fld_field
                            ))




    def load_sheet_db(self, app_id, app_name):
        """
        sheet_comp_dict looks lik e
        sheet_comp_dict[qid] = comp_qFieldDefs
        
        """

        for key, val in self.sheet_comp_dict.items():
            lcomp_qFieldDefs = val
            if isinstance( lcomp_qFieldDefs, comp_qFieldDefs):
                lqFields = lcomp_qFieldDefs.qFields
                if isinstance( lqFields, dict):
                    if lqFields:
                        for control_fld_label, control_fld_value in lqFields.items():
                            self.exec_ins_qlik_app_sheet_controls(app_id
                                , app_name
                                , self.id
                                , self.name
                                , lcomp_qFieldDefs.parent_id
                                , lcomp_qFieldDefs.parent_type
                                , lcomp_qFieldDefs.id
                                , lcomp_qFieldDefs.type
                                , lcomp_qFieldDefs.title
                                , control_fld_label
                                , control_fld_value)            
                    else:
                        self.exec_ins_qlik_app_sheet_controls(app_id
                                , app_name, self.id
                                , self.name
                                , lcomp_qFieldDefs.parent_id
                                , lcomp_qFieldDefs.parent_type
                                , lcomp_qFieldDefs.id
                                , lcomp_qFieldDefs.type
                                , lcomp_qFieldDefs.title
                                , ""
                                , "")  

    
   

    def getsheetObjects(self):
        # Getting the qlik handel for a sheet  
        qSheetResponse = self.tqlikWSS.getobject(self.app_handle, self.id )
        self.handle = cf.getqlik_hnadle_frm_response(qSheetResponse)

        # With the handle get the child info for a sheet
        qsheetChildobjsResponse = self.tqlikWSS.GetChildInfos(self.handle, self.name )     
         # logging.info("\n\t{}".format( qsheetchildobjs))
                # Process child objects 
        self.proc_qsheet_childObjs(qsheetChildobjsResponse) 

        logging.info("sheet:{}; start:{}; key= {}".format(self.name, self.sheet_start_dt, len(self.sheet_comp_dict.keys())))


    def proc_qsheet_childObjs(self, qsheetchildobjs : Dict, qparentid = "", qparenttype="" )->dict:
        """
        process child objects belong to a qlik sheet object!
        Params: 
            qsheetchildobjs: dict json meesage retend by getChildinfo apis 
            Sheetname : name of the qlik sheet 
        returns:
            Dictonary object obj['qId']:{'qId' : obj['qId'], 'qType' : obj['qType'] 
                                        , 'qFields' : sheet_comp_qFieldDefs, 'qParentId':"", 'qParentType':"" 
                                }

        """
        qHandleResp4Obj = {}
        qObjId = ""
        qObjType = ""
        qObjHandle = 0
        
        # check if the qlik reponse ahs result
        if cf.has_result_reponse(qsheetchildobjs):
            for obj in qsheetchildobjs['result']['qInfos']:
                # Get object Id and type
                qObjId = obj['qId']
                qObjType = obj['qType'].strip()
                logging.info(obj['qId'] + " " + obj['qType'] )

                qHandleResp4Obj = self.tqlikWSS.getobject(self.app_handle, qObjId)
                qObjHandle = cf.getqlik_hnadle_frm_response(qHandleResp4Obj)

                # initiating the comppnent fieldsDefs
                self.sheet_comp_dict[qObjId] =  comp_qFieldDefs(qObjId, qObjId, qObjType, qparentid, qparenttype ) 
                # self.sheet_comp_dict[qObjId].title = qsheetchildobjs['result']['title']

                # chking for group parents will add more type here 
                if qObjType in ["filterpane"]:
                    # get the child objects info qsheetChildobjsResponse passing handle and ID
                    qChildobjsResponse = self.tqlikWSS.GetChildInfos( qObjHandle
                                                                        , qObjId )  

                    self.proc_qsheet_childObjs(qChildobjsResponse, qObjId, qObjType)                                                   
                   
                else:
                    # Get the properties of a qlik object passing hnadle and id 
                    qgetobjproperties = self.tqlikWSS.GetEffectiveProperties(qObjHandle
                                                    , qObjId ) 
                    # Get the qvd field info 
                    # get the title of control
                    if "title" in qgetobjproperties['result']["qProp"]:
                        self.sheet_comp_dict[qObjId].title = qgetobjproperties['result']["qProp"]['title']
                    else:
                         self.sheet_comp_dict[qObjId].title = "title not found"       

                    self.sheet_comp_dict[qObjId].recur_getQFieldDefs_U(qgetobjproperties)
                
            logging.info("Sheet:{}\n{}\n{}".format(self.name, ("="*20), self.sheet_comp_dict))
  

    def __del__(self):
        for key, val in self.sheet_comp_dict.items():
            lcomp_qFieldDefs = val
            if isinstance( lcomp_qFieldDefs, comp_qFieldDefs):
                del lcomp_qFieldDefs
        #del self.tqlikWSS
        #del self.ldbLoader
        super().__del__()
        logging.info("Destructor called for object [{}] ".format(__class__.__name__))
    

##################################################################################################################


def main():
    from qlikWS import qlikeWS
    import  propertyReader as pr 
    config = pr.getPropInstance('config/', 'NLP.properties')
    __prop_dict = dict(config.items('log'))
    logFile = __prop_dict["logfilepath"] + sys.argv[0].split("/")[-1][0:-3] + "_" + os.getlogin() + "_" + time.strftime('%Y%m%d%H%M%S')   + ".log"
    # --GEO--C-- LOGGING SETTINGS
    logging.basicConfig(
                       # filename=logFile
                         level=logging.DEBUG
                        , format=__prop_dict["log.format"]
                        , datefmt=__prop_dict["log.date.format"]
                        )

    root = logging.getLogger()
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(__prop_dict["log.format"], datefmt=__prop_dict["log.date.format"])
    ch.setFormatter(formatter)
    #root.addHandler(ch)
    logging.info("Starting SSS")
    tqlikeWS = qlikeWS(logging.getLogger(),__prop_dict)
    tqlikeWS.connect1('583ebdsf6e1-3d73e-4dfsdsbff-becdfs0-fd2411eeccdfdsfa6eb')

    tqliksheet = qliksheet(tqlikeWS , "123-111", "name")


    
    del tqlikeWS

if __name__ == "__main__":
    main()


