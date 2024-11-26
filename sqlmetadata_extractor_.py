import re
from sql_metadata import Parser

COMMENTED_FIELDS = ["CHANGED", "REMOVED", "SORTED", "ADDED", "FILTERED", "DUPLICATED", 
    "TRIMMED", "RENAMED", "REORDERED", "MERGED", "EXPANDED", "GROUPED", "APPENDED"]

def extractTables( query ):
    query = query.replace("''", "`")
    parser = Parser(query)
    res=None

    try:
        res = parser.columns
    except:
        if query.count("(") != query.count(")"):
            if "'])," in query or "`])," in query:
                if "`])," in query:
                    arr = query.split("`]),")
                else:
                    arr = query.split("']),")
                query=arr[0]
                parser = Parser(query)
                res = parser.columns
            else:
                print("pattern not found")
                
    # note that columns list do not contain aliases of the columns
    cols=[]
    cols_final=[]
    tables_final = []
    for col in res:
        if col not in COMMENTED_FIELDS:
            cols.append(col)
        else:
            break

    tables = parser.tables
    tables_without_schema = []
    for table in tables:
        if table == "CASE":
            break
        tables_final.append(table)
        arr = table.split(".")
        tables_without_schema.append(arr[len(arr)-1])

    for col in cols:
        if "." in col:
            arr = col.split(".")
            table_name = arr[len(arr)-2]
            if table_name in tables_without_schema:
                cols_final.append(col)
            else:
                table_match = re.findall(f"FROM (.*?) {table_name}", query)
                if table_match:            
                    table_name = table_match[0]
                    cols_final.append(table_name+"."+arr[len(arr)-1])
                    if table_name not in tables:
                        tables.append(table_name)
        else :
            cols_final.append(col)

    for table in tables_without_schema:
        table_name = table.replace("[", "")
        table_name = table_name.replace("]", "")
        for t in tables:
            if t.startswith(table_name+".") or t.startswith(table+"."):
                cols_final.append(t)
                if t in tables_final:
                    tables_final.remove(t)      



    result=[]
    for table in tables_final:        
        table = table.replace("[", "").replace("]", "")
        result.append(table)
    return result
