import logging
from typing import Dict, List, OrderedDict
import propertyReader as pr

import os 


import time
from datetime import datetime
import commonfun as cf
# from qlikWS import qlikeWS
from postgresDbloader import dbLoader
import typing



class  qlikObj:
    """
    Class object to store qlik objects id, name , type and hadled. This is a baseclass
    """
    id :str
    name: str
    handle: int
    type: str

    def __init__(self,  id :str , name:str, type : str ):
        """
        Params:
            response : json message from qlik

        """
        logging.info("Constructor called for object {} --> {}".format(__class__.__name__, self.__class__.__name__))
        self.handle = 0 
        self.id = id 
        self.name = name 
        self.type = type 
       
    
    def __del__(self):
        
        logging.info("Destructor called for object [{}] ".format(__class__.__name__))
        # super().__del__()

class comp_qFieldDefs(qlikObj):
    """
    Inherits qlikobj; class object to store component to store fieldDefs
    """
    qFields = OrderedDict()
    parent_id = ""
    parent_type = "'"
    title = ""

    def __init__(self,  id :str , name:str, type : str, parent_id :str ="" ,  parent_type : str =""):
        """
        Params:
            response : json message from qlik

        """
        logging.info("Constructor called for object {} --> {}".format(__class__.__name__, self.__class__.__name__))
        qlikObj.__init__(self,id, name, type )
        self.qFields = OrderedDict()
        self.parent_id = parent_id 
        self.parent_type = parent_type
       
    
    def __del__(self):
        # self.super().__del__()
        logging.info("Destructor called for object [{}] ".format(__class__.__name__))
       

    def recur_getQFieldDefs_U(self, qgetobjproperties, tabs= "\t"):
        """
            recur loop thru qFieldDefs listed under qDimensions, qMeasures and qListObjectdDef

"result": 
{
      "qProp": {
         "qInfo": {
            "qId": "NFznJeA",
            "qType": "auto-chart"
         },
         "qExtendsId": "",
         "qMetaDef": {},
         "qStateName": "",
         "qHyperCubeDef": {
            "qStateName": "",
            "qDimensions": [
            ],
            "qMeasures": [
              
            ],
         "showTitles": true,
         "title": "By Facility",
         "subtitle": "",
         "footnote": "",
         "showDetails": false,
         "visualization": "auto-chart",
         }
      }
   }


        """
        for key, value in  qgetobjproperties.items():
            if key.strip() == "qDimensions": #loop thru the collections
                self.qFields.update(self.get_qDimensions(value, tabs))
            elif key.strip() == "qMeasures":
                self.qFields.update(self.get_qMeasures(value, tabs) )
            elif key.strip() == "title":
                self.title = value
            elif key.strip() == "qListObjectDef":
                self.qFields.update(self.get_qListObjectDef(value, tabs) )
            elif isinstance(value,dict):
                logging.info("{}Dictionary found: {}  keys count: {}".format(tabs, key, len(value.keys())))
                tabs = tabs + "\t"
                # self.qFields.update()
                self.recur_getQFieldDefs_U(value, tabs)
                     
            elif isinstance(value, list):
                logging.info("{}List found {}  len :{}".format(tabs, key, len(value)))
                tabs = tabs + "\t"
                for dic in value:
                    if isinstance(dic, dict):
                        # self.qFields.update(self.recur_getQFieldDefs_U(dic, tabs))
                        self.recur_getQFieldDefs_U(dic, tabs)
        # self.qFields = qFieldDefs


    def get_qDimensions(self, qDimensions, tabs):
        """
        params:
                qDimensions collection of qDimension which is dict with moytle kys and values

        returns QDimensions[0..n]["qDef"][qFieldLabels_value] = QDimensions[0..n]["qDef"][qFieldDefs]    
        """
        return_dic =OrderedDict()
        for idx,  qDimension in enumerate(qDimensions):
            return_dic.update(self.get_qFieldDefs(qDimension, tabs ))

        return return_dic


    def get_qListObjectDef(self,qListObjectDef, tabs="\t"):
        return self.get_qFieldDefs(qListObjectDef, "\t")


    def get_qFieldDefs(self, qFieldDefs, tabs= "\t"):
        qFieldDefs_value = ""
        qFieldLabels_value = ""
        return_dic =OrderedDict()
       
        qFieldDefs_value = cf.findkey_return_val_astring(qFieldDefs["qDef"], "qFieldDefs")

        qFieldLabels_value = cf.findkey_return_val_astring(qFieldDefs["qDef"], "qFieldLabels")

        if qFieldLabels_value == "":
            if len(qFieldDefs_value.strip()) > 150:
                return_dic[  self.assignTitleif_valueIsnull(self.title) ] = qFieldDefs_value
            else:
                return_dic[qFieldDefs_value] = qFieldDefs_value
        else:
            return_dic[qFieldLabels_value[:145]] = qFieldDefs_value    

        return return_dic 

    
    def get_qMeasures(self, qMeasures, tabs="\t"):
        """
            returns qMeasures[0..n]["qDef"][qFieldLabels_value] = QDimensions[0..n]["qDef"][qFieldDefs]  

        """
        qDef_value = ""
        qLabel_value = ""
        return_dic =OrderedDict()

        for idx, qMeasure in enumerate(qMeasures):
            qDef_value = cf.findkey_return_val_astring(qMeasure["qDef"], "qDef")

            qLabel_value = cf.findkey_return_val_astring(qMeasure["qDef"], "qLabel")

            if qLabel_value.strip() == "":
                return_dic[ "{}_{}".format(self.assignTitleif_valueIsnull(self.title), idx) ] = qDef_value
            else:
                return_dic[qLabel_value[:145]] = qDef_value   
                    
        return return_dic

    def assignTitleif_valueIsnull(self,title):
        if isinstance(title, dict):
            return "titleisdict"
        else:
            return title[:145] # leaving extra room for _9s

        
