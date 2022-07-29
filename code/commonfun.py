"""
Common function used 

"""
import re
import logging
import typing
import json

"""
--GEO--C--  Clean up script 
            1. removed commented out line usinf /**/ block or line //
               exception if wthin a /**/ there is // line level comment then code won't process these exceptions/ ouliers
               and will take tha code into account-- found one.. But will write this into a log file or table 



"""
def cleanup_fld_old(qvd_load):
    #qvd_load = qvd_load.replace("LOAD", "").strip()

    # Split by /* and */ to remove commenetd blocks
    logging.info("{} = {}".format(qvd_load[:20], "/*" in qvd_load))
    if "/*" in qvd_load and '//' not in qvd_load:
        qvd_load = qvd_load.split("/*")
        logging.info(qvd_load[0][len(qvd_load[0]) - 300:len(qvd_load[0])])
        qvd_load_botton = qvd_load[-1].split('*/')[-1]
        logging.info(qvd_load_botton)
        qvd_load = "\\n" + qvd_load[0] + "\\n" + (qvd_load_botton)
        logging.info(qvd_load[len(qvd_load) - 1000:len(qvd_load)])

    qvd_flds = qvd_load.split("\\n")

    new_qvd_flds = []
    # removing  the commented out line using // or partilly commentedout line
    i=0
    for i, fld in enumerate(qvd_flds):
        if '\\t' in fld:
            fld = fld.replace('\\t', "")

        if not fld.startswith("//"):
            fld = add_qvd_to_load(fld)

            # fld_lines = fld.split(",\n")
            # for fld_line in fld_lines:
            #     if not fld_line.strip().startswith("//") and not (fld_line.strip() == "") :
            if "//" in fld and not 'lib://' in fld:

                new_qvd_flds.append(fld.split("//")[0].strip())
                logging.info(fld.split("//")[0].strip())

            else:
                fld = add_FROM_to_load(fld)
                # check if the line contains ; if then check if there is qvd else add (QVD)
               
                new_qvd_flds.append(fld.strip())
    #--GEO--C-- add qvd  if tehr is no(qvd) keyword
    
    sappend = "(QVD)"
    if "(qvd)" not in " ". join( new_qvd_flds).lower():
        new_qvd_flds.append(sappend)

    #logging.info(new_qvd_flds);
    # return '\r\n'.join([str(elem) for elem in new_qvd_flds])

    return '\r\n'.join([str(elem) for elem in new_qvd_flds])

def cleanup_fld(qvd_load):
    
    new_qvd_flds = []
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "",qvd_load)
    q = q.replace("\\t", "")
    q = add_qvd_to_load(q)
    q = add_FROM_to_load(q)


    # remove whole line -- and # comments
    lines = [line for line in q.split("\\n") if not re.match("^\s*(--|#|//)", line)]
    q = lines

   
    sappend = "(QVD)"
    if "(qvd)" not in " ". join( q).lower():
        q.append(sappend)

    return '\r\n'.join([str(elem) for elem in q])

"""
--GEO--C--   add (QVD) which is used to split. Clean up the esge cases and log that info into excption table
"""

def add_qvd_to_load(fld):
    # edge cases if there are more than 1 occurence of qvd name
    # split the app . tab and line to another table like report for coding standards 
    if len(re.findall("qvd", fld.lower())) >=1:
    # if 'qvd' in fld.lower() and not '.qvd' in fld.lower(): 
        if not '(qvd)' in fld.lower():

            qvdposition = fld.lower().find("qvd")
            if qvdposition > 0 and fld[qvdposition - 1] == "(":
                logging.info(" FOUND QVD  = {} @ {}".format(fld, qvdposition))
                fld = fld[:qvdposition ] + 'QVD) ' + fld[qvdposition + 3:len(fld)]

            if qvdposition >= 0 and qvdposition + 3 <= len(fld) and fld[qvdposition + 3] == ")":
                logging.info(" FOUND QVD  = {}".format(fld))
                fld = fld[:qvdposition] + ' (QVD' + fld[qvdposition + 3:len(fld)+1]

                logging.info(" after ingesting QVD  = {}".format(fld))
        else:
            p = ['(qvd)', '(Qvd)', '(QVd)']
            for i in p:
                fld = fld.replace(i, '(QVD)')
    return fld

def addKeywordTostring(token:str, keyword: str, indx: int = 0):
        keyword_pos = qlikapptabload.getindexx(token, keyword)

        # indx = load_token.lower().index("load ")
        token = token[:keyword_pos ]+ " " + keyword + " " + token[keyword_pos+ len(keyword) :]
        return token



def add_FROM_to_load(fld):
    """
        searhc a replace from w/ FROM  
    """
    fromposition = fld.lower().find("from ")
    if fromposition > 0 and fld[fromposition - 1] == " ":
        logging.info(" FOUND FROM  = {}".format(fld))
        fld = fld[:fromposition - 1] + ' FROM ' + fld[fromposition + 5:len(fld)]

        logging.info(" after ingesting FROM  = {}".format(fld))

    return fld

def getqlik_hnadle_frm_response(qresponse):
    handle = 0 
    if 'result' in qresponse.keys():
        handle  = qresponse['result']['qReturn']['qHandle']
        
    return handle


def has_result_reponse(qresponse):
    return 'result' in qresponse.keys()

def  convertIfdict_json(control_title):
        if isinstance(control_title, dict):
            return json.dumps(control_title)
        else: 
            return  control_title    


def findkey_return_val_astring(dict1, key):
    sdict_key_val = ""
    if key in dict1:
        sdict_key_val = ''.join(dict1[key])
    return sdict_key_val

