import ssl
import json
from xml.dom.minidom import Identified
import websocket

from websocket import create_connection
import logging
import inspect
import os
import sys
import time
import propertyReader as pr
import threading
from qliksheets import qliksheets
import typing


class qlikeWS:
    """
    qlik wss connection class
    """
    __steve = "Empty"
    sWsUrl = ""
    sWsPrivKeyPath = ""
    certs = ({"ca_certs": sWsPrivKeyPath + "root.pem",
              "certfile": sWsPrivKeyPath + "client.pem",
              "keyfile": sWsPrivKeyPath + "client_key.pem",
              "cert_reqs": ssl.CERT_NONE,
              "server_side": False
              })
    sUserDirectory = ""
    sUserId = ""
    __prop_dict = ""
    __objWsS_Conn = ""
    __objLogger = ""
    handle = -1
    qSessionAppId = ""
    sappName = ""

    def setLogging(self):
        print(sys.argv[0].split("/")[-1][0:-3])
        scriptName = inspect.stack()[1][1].split("/")[-1][0:-3]
        print(" Starting : log file is {}".format(scriptName))
        # in the format YYYYMMDDHHMMSS
        dateTimeStamp = time.strftime('%Y%m%d%H%M%S')
        userName = os.getlogin()
        logFile = self.__prop_dict["logfilepath"] + scriptName + \
            "_" + userName + "_" + dateTimeStamp + ".log"

        logging.basicConfig(filename=logFile, level=logging.DEBUG, format=self.__prop_dict["log.format"],
                            datefmt=self.__prop_dict["log.date.format"])

        self.__objLogger = logging.getLogger()
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            self.__prop_dict["log.format"], datefmt=self.__prop_dict["log.date.format"])
        ch.setFormatter(formatter)
        self.__objLogger.addHandler(ch)
        self.__objLogger.info("GEORGR ")

    def __init__(self, pobjLogger=None, pprop_dict=None):

        if (pobjLogger is None):
            self.setLogging()
        else:
            self.__objLogger = pobjLogger

        self.__objLogger.info(
            "Starting a new instance of {}".format(type(self).__name__))
        self.__prop_dict = pprop_dict
        self.setProps()

    def get__prop_dict(self):
        return self.__prop_dict

    def GetActiveDoc(self):
        getActiveDoc = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "GetActiveDoc",
            "handle": -1,
            "params": []
        }

        ret = self.__callqlikjsonrpc(getActiveDoc, 1)
        return ret

    def createSessApp(self, sappName):
        createAppJson = {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "CreateSessionApp",
            "handle": -1,
            "params": []
        }
        # "Sales1stQuater.qvf"
        ret = self.__callqlikjsonrpc(createAppJson, 1)
        self.sappName = sappName
        self.qSessionAppId = ret["result"]["qSessionAppId"]
        self.handle = ret["result"]["qReturn"]["qHandle"]
        return ret

    def OpenQlikApp(self, id: str, name: str, cnt: int) -> dict:
        """
        open an qlik application for id 
        Params:
            Id: id of the qlik app
            name: name of wlik app 

        """
        OpenQDoc = {
            "method": "OpenDoc",
            "handle": -1,
            "params": [
                "{}".format(id)
            ],
            "jsonrpc": "2.0",

        }
        logging.info("Calling get OpenQDoc for qlik app  {}".format(name))
        return self.__callqlikjsonrpc(OpenQDoc, cnt)

    def setQscript(self, handle: int, script: str):
        setscript = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "SetScript",
            "handle": self.handle,
            "params": [
                script
            ]
        }
        setscriptJsonreturn = self.__callqlikjsonrpc(setscript, 1)
        print("ZZZZZ Calling releadod")

        return self.reloadQApp(handle)

    def GetScriptEx(self, id: int):
        print(self.handle)
        OpenQDoc = {
            "jsonrpc": "2.0",
            "id": 6,
            "handle": id,
            "method": "GetScriptEx",
            "params": {}

        }
        return self.__callqlikjsonrpc(OpenQDoc, 1)

    def reloadQApp(self, handle):
        ReloadJsonQ = {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "DoReloadEx",
            "handle": handle,
            "params": []
        }
        print("XXXX Done Reload")

        return self.__callqlikjsonrpc(ReloadJsonQ, 1)

    def reloadQAppStatus(self, handle):
        ReloadStatusJsonQ = {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "GetProgress",
            "handle": -1,
            "params": [4]
        }
        print("XXXX Reload Status ::")
        return self.__callqlikjsonrpc(ReloadStatusJsonQ, 1)

    def __del__(self):
        self.__objLogger.info(
            'Destructor called, for ws url :{}'.format(self.sWsUrl))
        if not self.__objWsS_Conn and not self.__objWsS_Conn == "":
            self.__objWsS_Conn.close()
            del self.__objWsS_Conn
        del self.__prop_dict

    def getobjects(self, handle: int, qType: str) -> dict:
        """
        calls the qlik json rpc call GetObjcts to get the list of objects qType
        Params:
            Handle: Handle of the parent object 
            qType: qlike type sheets etc 
        Returns JSON objects for the type
        """
        request = {
            "jsonrpc": "2.0",
            "handle": handle,
            "id": 3,
            "method": "GetObjects",
            "params": {
                "qOptions": {
                    "qTypes": [
                        qType
                    ]
                }
            }
        }
        logging.info("Calling get objects for qType  {}".format(qType))
        return self.__callqlikjsonrpc(request, 1)

    def getobject(self, handle: int, id: str) -> dict:
        """
        Calls JSON API to get the handle of an object 
        Params:
            Handle
            qid : qlik id of the object 
        returns : JSON string/ dict w/ hnade info    
        """
        request = {
            "handle": handle,
            "method": "GetObject",
            "params": {
                "qId": id
            }
        }
        logging.info("Calling get object for qid  {}".format(id))
        return self.__callqlikjsonrpc(request, 1)

    def GetChildInfos(self, handle: int, object_name: str) -> dict:
        """
        Returns the identifier and the type for each child in an app object
        """
        request = {
            "handle": handle,
            "method": "GetChildInfos",
            "params": {}
        }

        logging.info("Calling GetChildInfos {}".format(object_name))
        return self.__callqlikjsonrpc(request, 1)

    def GetProperties(self, handle: int, object_name: str) -> dict:
        """
        Returns the identifier, the type and the properties of the object
        """
        request = {
            "handle": handle,
            "method": "GetProperties",
            "params": {}
        }
        logging.info("Calling GetProperties {}".format(object_name))
        return self.__callqlikjsonrpc(request, 1)

    def GetEffectiveProperties(self, handle: int, object_name: str) -> dict:
        request = {
            "handle": handle,
            "method": "GetEffectiveProperties",
            "params": {}
        }

        logging.info("Calling GetProperties {}".format(object_name))
        return self.__callqlikjsonrpc(request, 1)


########################################################################################################

    def setProps(self):
        self.sWsUrl = self.__prop_dict['qlik.sense.web.socket.url']
        self.sWsPrivKeyPath = self.__prop_dict['qlik.sense.web.socket.priv.key.path']
        self.certs = ({"ca_certs": self.sWsPrivKeyPath + "root.pem",
                       "certfile": self.sWsPrivKeyPath + "client.pem",
                       "keyfile": self.sWsPrivKeyPath + "client_key.pem",
                       "cert_reqs": ssl.VerifyMode.CERT_OPTIONAL,
                       "server_side": False
                       })

        self.sUserDirectory = self.__prop_dict['qlik.sense.web.api.user.directory']
        self.sUserId = self.__prop_dict['qlik.sense.web.api.user.id']

    def connect1(self, stabName):
        """
        makes a wss connection 
        """
        frame = inspect.stack()[1]
#        module = inspect.getmodule(frame[0])
        websocket.WebSocketApp
        logging.info(self.sWsUrl)
        headers = {}
        headers['content-type'] = "application/json"
        headers['X-Qlik-User'] = 'UserDirectory=%s; UserId=%s' % (
            self.sUserDirectory, self.sUserId)

        try:
            self.__objWsS_Conn = create_connection(self.sWsUrl
                                                   + "/" + stabName
                                                   + "?reloadUri=https://asd.com/dev-hub/engine-api-explorer", sslopt=self.certs, header=headers
                                                   )
        except Exception as e:
            logging.error(e)
            raise e
            # self.__objLogger.critical("Error when connecting " + self.sWsUrl + sys.exc_info()[0])

    def close_conn(self, stabname):
        """
        Closes wss connection
        """
        if self.__objWsS_Conn != None:
            self.__objWsS_Conn.close()

    def getQlikAppList(self) -> typing.Dict:
        GetDocList = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "GetDocList",
            "handle": -1,
            "params": [],
        }
        return self.__callqlikjsonrpc(GetDocList, 2)

    def __callqlikjsonrpc(self, jsonQlikReq, i):
        jreq = json.dumps(jsonQlikReq)
        # print(jreq)
        self.__objWsS_Conn.send(jreq)

        # while True:
        x = 1
        time.sleep(1)
        for x in range(i):
            msg = self.__objWsS_Conn.recv()
            msgJson = json.loads(msg)
            # print(json.dumps(msgJson, indent=4, sort_keys=True))

        return msgJson


def main():
    config = pr.getPropInstance('config/', 'NLP.properties')
    __prop_dict = dict(config.items('log'))
    logFile = __prop_dict["logfilepath"] + sys.argv[0].split(
        "/")[-1][0:-3] + "_" + os.getlogin() + "_" + time.strftime('%Y%m%d%H%M%S') + ".log"
    # --GEO--C-- LOGGING SETTINGS
    logging.basicConfig(
        # filename=logFile
        level=logging.DEBUG, format=__prop_dict["log.format"], datefmt=__prop_dict["log.date.format"]
    )

    root = logging.getLogger()
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        __prop_dict["log.format"], datefmt=__prop_dict["log.date.format"])
    ch.setFormatter(formatter)
    # root.addHandler(ch)
    logging.info("Starting SSS")

    tqlikeWS = qlikeWS(logging.getLogger(), __prop_dict)
    tqlikeWS.connect1('583eb6e1-373e-4bff-bec0-fd2411eea6eb')

    a = tqlikeWS.getQlikAppList()
    # print(a, "getQlikAppList")

    tqlikeWS.close_conn(1)  # close if already open

    for dic in a['result']['qDocList']:
        # print(dic['qDocName'], dic['qDocId'])
       
        if dic['qDocName'].strip() in ["BP MRO Supply Analysis (1)", "Categorization Master"]:
            print(dic['qDocName'], dic['qDocId'])
            tqlikeWS.connect1(dic['qDocId'])

            # opne the app to get the handle
            appinfo = tqlikeWS.OpenQlikApp(dic['qDocId'], dic['qDocName'], 2)
            if "result" in appinfo.keys():
                logging.info("App handle for {} = [{}]".format(
                    dic['qDocName'], appinfo['result']['qReturn']['qHandle']))
                app_handle = appinfo['result']['qReturn']['qHandle']
                # get the qliksheets
                qsheets = tqlikeWS.getobjects(app_handle, "sheet")

                lqliksheets = qliksheets(qsheets
                                        , app_handle
                                        , tqlikeWS
                                        , dic['qDocId']
                                        , dic['qDocName'])

                lqliksheets.getqlikSheets()
                del lqliksheets
                # logging.info(sheets)

            tqlikeWS.close_conn(dic['qDocId'])  # close if already open
    # now loop thru the list

    del tqlikeWS


if __name__ == "__main__":
    main()

