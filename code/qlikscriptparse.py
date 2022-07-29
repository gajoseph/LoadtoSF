
import re
from collections import OrderedDict
def tables_in_query(sql_str):

    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("[--|#]$", line)[0] for line in lines])

    # remove whole line -- and # comments
    # lines = [line for line in q.splitlines() if not re.match("*(--|#)$", line)]

    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).

    result = set()
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
                result.add(tok)
            get_next = False
        get_next = tok.lower() in ["from", "join"]

    return result


def cols_in_query(sql_str):

    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#|(?<!:)//)", line)]

    # remove in between  -- and # comments and skipping if like this ://  
    q = " ".join([re.split("--|#|(?<!:)//", line)[0] for line in lines])

    print(q)
    from_token = q #re.split("From", q)[0]
    tokens = OrderedDict()
    tokens = re.split("(,)", from_token)
    for t in tokens:
        print (t)
    

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).

    result = OrderedDict()
    get_next = False
    asd = []
    stackable = 0
    for tok in tokens:
        if get_next:
            if stackable>0:

                stackable = upd_stackable(tok, stackable=stackable)
                asd.append(tok.strip())
            else:
                if len(asd) >0:
                    d= join_stackbaleString(asd) #', '.join(asd)
                    result[d] = d
                asd = []
                if "(" in tok:
                    asd.append(tok)
                    stackable = upd_stackable(tok, stackable=stackable)
                else:
                    result[tok] = tok.strip()


        else:
            get_next = "load " in tok.lower()
            #result.add(tok.lower().replace("select", ""))
            asd = []
            if "(" in tok:
                asd.append(tok)
                stackable = upd_stackable(tok, stackable=stackable)
            else:
                indx = tok.lower().index("load ")

                d = tok[:indx] + tok[indx+len("load") :]
                result[d] = d

    if len(asd) >0:
        d = join_stackbaleString(asd)
        result[d] = d
    return result

def upd_stackable( token :str, stackable:int  ):
    bracket_val = {"(": 1, ")": -1}
    if ")" in token:
        token = token.replace(")", ") ")
        stackable = stackable - token.count(")")
    if  "(" in token:
        token = token.replace("(", "( ")
        stackable = stackable + token.count("(")

    return stackable

def join_stackbaleString(asd):
    return ', '.join(asd).replace("(", "( ").replace(")", " )")

sql = """

LOAD
/*g

a
ant*/
// ghsagdjgsa
//jkl
\"Facility Code\",/* test 111*/
\"Facility Status\",
replace(george, ' ', 'll'), //lib://GEoRGE commentts 
name & "_" & "+" &"__"&last_name


"""


a = tables_in_query(sql)
print(a)

a= cols_in_query(sql)
print("dfdsfsfs")
for i in a:
    print(i,"\n" )

