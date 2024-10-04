import pandas as pd
import sqlparse
import re
from sqlglot import parse_one, exp
from collections import OrderedDict

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}
query = """
SELECT 
	B.SRC_SYS_KEY_TXT, 
	B.FRST_NM+' '+B.LST_NM AS EMP_NM, 
	C.ORZN_ZONE_CDE, 
	C.ORZN_DEPT_CDE, 
	A.JOB_TYP_CDE, 
	CAST(A.EFF_BEG_DT AS DATE) AS EFF_BEG_DT, 
	ADJ_SVC_DT 
FROM [DM_01].[WORKER_STATUS_FCT] A LEFT JOIN [DM_01].[ORGANIZATION_DIM] B ON A.ORZN_DIM_SK=B.ORZN_DIM_SK LEFT JOIN [DM_01].[SALE_HIER_DIM] C ON (B.SRC_SYS_KEY_TXT=C.SALE_HIER_ID AND C.EFF_END_DT='9999-12-31 00:00:00' AND C.CURR_ROW_IND='Y') WHERE ( --A.JOB_TYP_CDE='001004' OR A.JOB_TYP_CDE='001005' OR A.JOB_TYP_CDE='001011' OR A.JOB_TYP_CDE='001007' ) AND  B.CURR_ROW_IND='Y' AND A.EFF_END_DT='9999-12-31 00:00:00' AND EMP_STS_TYP_CDE='A'']),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(SOURCE,{{'EFF_BEG_DT', TYPE DATE}, {'ADJ_SVC_DT', TYPE DATE}}),    #'FILTERED ROWS' = TABLE.SELECTROWS(#'CHANGED TYPE', EACH [JOB_TYP_CDE] <> '001004'),    #'REMOVED COLUMNS' = TABLE.REMOVECOLUMNS(#'FILTERED ROWS',{'ADJ_SVC_DT'}),    #'MERGED QUERIES' = TABLE.NESTEDJOIN(#'REMOVED COLUMNS', {'SRC_SYS_KEY_TXT'}, #'WORKDAY LEADER DIRECTORY', {'EMPLOYEE ID'}, 'WORKDAY LEADER DIRECTORY', JOINKIND.LEFTOUTER),    #'EXPANDED WORKDAY LEADER DIRECTORY' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES', 'WORKDAY LEADER DIRECTORY', {'COST CENTER', 'START DATE IN CURRENT JOB OR HIRE DATE', 'WORKER'}, {'WORKDAY LEADER DIRECTORY.COST CENTER', 'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE', 'WORKDAY LEADER DIRECTORY.WORKER'}),    #'ADDED CUSTOM1' = TABLE.ADDCOLUMN(#'EXPANDED WORKDAY LEADER DIRECTORY', 'FINAL NAME', EACH IF [WORKDAY LEADER DIRECTORY.WORKER] IS NULL THEN [EMP_NM] ELSE [WORKDAY LEADER DIRECTORY.WORKER]),    #'REMOVED COLUMNS2' = TABLE.REMOVECOLUMNS(#'ADDED CUSTOM1',{'EMP_NM', 'WORKDAY LEADER DIRECTORY.WORKER'}),    #'REORDERED COLUMNS' = TABLE.REORDERCOLUMNS(#'REMOVED COLUMNS2',{'SRC_SYS_KEY_TXT', 'FINAL NAME', 'ORZN_ZONE_CDE', 'ORZN_DEPT_CDE', 'JOB_TYP_CDE', 'EFF_BEG_DT', 'WORKDAY LEADER DIRECTORY.COST CENTER', 'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE'}),    #'RENAMED COLUMNS2' = TABLE.RENAMECOLUMNS(#'REORDERED COLUMNS',{{'FINAL NAME', 'EMP_NM'}}),    #'MERGED QUERIES1' = TABLE.NESTEDJOIN(#'RENAMED COLUMNS2', {'SRC_SYS_KEY_TXT'}, #'MARKET MAPPING', {'ZONE_LEADER_TSID'}, 'MARKET MAPPING', JOINKIND.LEFTOUTER),    #'EXPANDED MARKET MAPPING' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES1', 'MARKET MAPPING', {'HIER_ID'}, {'MARKET MAPPING.HIER_ID'}),    #'RENAMED COLUMNS' = TABLE.RENAMECOLUMNS(#'EXPANDED MARKET MAPPING',{{'ORZN_ZONE_CDE', 'WORKER_STS_FCT_MKT'}, {'MARKET MAPPING.HIER_ID', 'ORZN_ZONE_CDE'}, {'EFF_BEG_DT', 'WORKER_STS_FCT_DT'}, {'WORKDAY LEADER DIRECTORY.START DATE IN CURRENT JOB OR HIRE DATE', 'EFF_BEG_DT'}}),    #'MERGED QUERIES2' = TABLE.NESTEDJOIN(#'RENAMED COLUMNS', {'SRC_SYS_KEY_TXT'}, #'0630 LEADER BACKUP', {'EMPLOYEE ID'}, '0630 LEADER BACKUP', JOINKIND.LEFTOUTER),    #'EXPANDED 0630 LEADER BACKUP' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES2', '0630 LEADER BACKUP', {'EMPLOYEE ID', 'START DATE IN CURRENT JOB OR HIRE DATE'}, {'0630 LEADER BACKUP.EMPLOYEE ID', '0630 LEADER BACKUP.START DATE IN CURRENT JOB OR HIRE DATE'}),    #'FILTERED ROWS1' = TABLE.SELECTROWS(#'EXPANDED 0630 LEADER BACKUP', EACH ([0630 LEADER BACKUP.EMPLOYEE ID] <> NULL)),    #'DATE FILTER' = TABLE.SELECTROWS(#'FILTERED ROWS1', EACH [EFF_BEG_DT] >= #DATE(2023, 1, 1)),    #'JOB CODE FILTER' = TABLE.SELECTROWS(#'DATE FILTER', EACH ([JOB_TYP_CDE] = '001007')),    #'APPEND EXCEPTIONS' = TABLE.COMBINE({#'JOB CODE FILTER', #'DIR EXCEPTIONS'}),    CUSTOM1 = TABLE.ADDCOLUMN(#'APPEND EXCEPTIONS', 'PICTURE', EACH 'HTTPS://MYFIELD.THRIVENT.COM/CONTENT/FAIMAGES/' & [SRC_SYS_KEY_TXT] & '.JPG

"""


# Function to check if column is complete
def is_balanced(val):
    count_open_sqr = val.count("[")
    count_clse_sqr = val.count("]")
    count_open_flr = val.count("{")
    count_clse_flr = val.count("{")
    count_open_brc = val.count("(")
    count_clse_brc = val.count(")")  
    count_quote = val.count("'")
    if ( (count_open_sqr - count_clse_sqr) != 0 or (count_open_flr - count_clse_flr) !=0 or 
        (count_open_brc - count_clse_brc) != 0 or (count_quote%2 != 0)):
        return False
    else:
        # Check if Case statements are complete or not
        if val.strip().startswith("CASE ") and (" END " not in val or " AS " not in val):
            return False
        else:
            return True 

# Function to clean and simplify the SQL query
def clean_sql_query(query):
    #query = re.sub(r"['\"]", "", query)  # Remove single and double quotes
    query = re.sub(r"(\n|\t|\r)", " ", query)  # Remove new lines and tabs
    query = re.sub(r"\s+", " ", query)  # Replace multiple spaces with a single space
    return query.strip()


def extract_main_table(query):
    # Clean the query to make it more uniform
    cleaned_query = clean_sql_query(query)

    # Modified pattern to stop before LEFT, INNER, or JOIN keywords, ensuring they are excluded
    main_table_pattern = re.compile(r"FROM\s+([a-zA-Z0-9_\[\].\s]+?)(?=\s+(?:LEFT|INNER|JOIN|RIGHT|FULL|WHERE|GROUP|ORDER|HAVING|$))", re.IGNORECASE)

    # Search for the main table using the defined pattern
    main_table_match = re.search(main_table_pattern, cleaned_query)
    if main_table_match:
        # Extract the main table name and clean brackets if present
        main_table = main_table_match.group(1).replace("[", "").replace("]", "").strip()
        print(f"Main Source Table: {main_table}")
        return main_table

    return None

# Call the function with the query
extract_main_table(query)
# Function to extract tables and columns from the SELECT part of the query
def extract_columns(query):
    parsed = sqlparse.parse(query)[0]

    # Set to hold extracted columns and tables
    columns = set()

    # Patterns to identify source columns and tables
    column_pattern = re.compile(r"SELECT\s+(.*?)\s+FROM", re.IGNORECASE)

    # Clean and split the query into components
    cleaned_query = clean_sql_query(query)

    # Extract columns from SELECT clause
    select_match = re.search(column_pattern, cleaned_query)
    if select_match:

        # Split the columns, handle aliases, and strip extra spaces
        columns_clause = select_match.group(1)
        # Remove DISTINCT keyword
        if columns_clause.startswith("DISTINCT "):
            columns_clause = columns_clause[8:]
        
        columnArr = columns_clause.split(",")
        i=0
        while( i < len(columnArr)):
            val = columnArr[i]
            while((not is_balanced(val)) and (i < len(columnArr)-1)):
                i = i+1
                val = val+","+columnArr[i]
            columns.add(val)
            i = i+1

    return columns


# Function to split a column into output and alias
def split_column_and_alias(column):
    column = column.strip()
    contains_table_ref = "." in column
    columnArr = column.split(" ")
    n = len(columnArr)
    output_column = columnArr[n-1]

    # Check if there is a table reference in column name
    if "." in output_column:
        output_columnArr = output_column.split(".")
        m = len(output_columnArr)
        output_column = output_columnArr[m-1]
    
    # Check if there is a [] format to extract the columns
    input_columns =  []
    res = re.findall(r'\[.*?\]', column)
    if not res:
        if ("AS" in columnArr) and (columnArr[n-2] == "AS"):
            # Remove last 2 elements - AS and output column
            input_columnArr = columnArr[: n-2]
        else:
            # Remove output column
            input_columnArr = columnArr[: n-1]

        for col in input_columnArr:
            # Contains table name
            if("." in col):
                val = col.split(".")[1]
                res = re.split(r"[^_a-zA-Z0-9\s]", val)
                val = res[0]
                if val not in input_columns and not val.isnumeric():
                    input_columns.append(val)   
            else:
                if( not contains_table_ref):
                    res = re.split(r"[^_a-zA-Z0-9\s]", col)
                    for val in res:
                        if (val not in KEYWORDS) and (len(val) > 1 and not val.isnumeric()):
                            if val not in input_columns:
                                input_columns.append(val)                                
    else :
        if column.endswith("]"):
            # Check if it contains both input and output
            total = len(res)
            if total > 1:
                for i in range(total-1):
                    val = str(res[i]).replace("[", "").replace("]", "")
                    if val not in input_columns and not val.isnumeric():
                        input_columns.append(val)
                val = str(res[total-1]).replace("[", "").replace("]", "")
                output_column = val
            else:
                val = str(res[0]).replace("[", "").replace("]", "")
                output_column = val
        else :
            for col in res:
                val = str(col).replace("[", "").replace("]", "")
                if val not in input_columns and not val.isnumeric():
                        input_columns.append(val)

    return input_columns, output_column
        

clean_sql_query(query)
columns = extract_columns(query)
all_columns = {}
for column in columns:
    input_columns, output_column = split_column_and_alias(column)
    all_columns[output_column] = input_columns

print(all_columns)