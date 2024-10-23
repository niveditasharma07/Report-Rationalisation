import pandas as pd
import sqlparse
import re
from sqlglot import parse_one, exp
from collections import OrderedDict

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}
query = """
SELECT  APPT_FCT.[WORKER APPOINTMENT ASSOCIATION],   APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY],  APPT_FCT.[PERSON DIMENSION SURROGATE KEY],  SUM(APPT_FCT.[APPOINTMENT OCCURS]) [APPT_OCCURS],  MBR_DIM.[MEMBERSHIP TYPE CODE],  MBR_DIM.[MEMBERSHIP TYPE DESCRIPTION],  APPT_DIM.[APPOINTMENT DATE],  APPT_DIM.[APPOINTMENT TYPE CODE],  CASE WHEN APPT_DIM.[APPOINTMENT TYPE CODE] IN ('GATHER DATA') THEN 'DISCOVER'             WHEN APPT_DIM.[APPOINTMENT TYPE CODE] IN ('TAKE ACTION') THEN 'DELIVER'             WHEN APPT_DIM.[APPOINTMENT TYPE CODE] NOT IN ('GATHER DATA','TAKE ACTION') THEN APPT_DIM.[APPOINTMENT TYPE CODE]        END AS [APPOINTMENT TYPE CODE],        CASE WHEN APPT_DIM.[APPOINTMENT TYPE CODE] IN ('GATHER DATA') THEN 'DISCOVER'             WHEN APPT_DIM.[APPOINTMENT TYPE CODE] IN ('TAKE ACTION') THEN 'DELIVER'             WHEN APPT_DIM.[APPOINTMENT TYPE CODE] NOT IN ('GATHER DATA','TAKE ACTION') THEN APPT_DIM.[APPOINTMENT TYPE DESCRIPTION]        END AS [APPOINTMENT TYPE DESCRIPTION],   CASE WHEN APPT_DIM.[FIELD USER COUNT] <> 0 AND APPT_DIM.[APPOINTMENT TYPE CODE] IN ('CONNECT','GATHER DATA','TAKE ACTION','STRATEGY CALL/MEETING','REVIEW','SERVICE','DISCOVER','DELIVER')    THEN 1     ELSE 0    END [JFW_IND],  DATE_DIM.[CALENDAR YEAR NAME],  DATE_DIM.[CALENDAR YEAR WEEK NUMBER],  DATE_DIM.[CALENDAR WEEK END DATE],  (LEFT(SALE_HIER_DIM.[ORZN_ZONE_CDE],4)) AS RFO,  SALE_HIER_DIM.ORZN_ZONE_CDE [MARKET],          SALE_HIER_DIM.ORZN_SUB_DEPT_CDE [MVP GROUP],  APPT_ORG_HIST.[SOURCE SYSTEM KEY TEXT] [TSID],  APPT_ORG_HIST.[ADJUSTED SERVICE DATE],  APPT_ORG_HIST.[EMPLOYEE STATUS TYPE DESCRIPTION],   APPT_ORG_HIST.[FINANCIAL PROFESSIONAL TENURE] [HISTORICAL FP TENURE],  APPT_ORG_HIST.[FINANCIAL PROFESSIONAL TENURE GROUP] [HISTORICAL FP TENURE GROUP],  APPT_ORG_HIST.[JOB TYPE CODE] [HISTORICAL JOB TYPE CODE],  APPT_ORG_HIST.[JOB TYPE DESCRIPTION] [HISTORICAL JOB TYPE DESCRIPTION]  FROM ENTERPRISEDATAMART.DM_01.[APPOINTMENT COMBINE FACT] APPT_FCT LEFT JOIN ENTERPRISEDATAMART.DM_01.[MEMBERSHIP TYPE DIMENSION] MBR_DIM  ON (APPT_FCT.[MEMBERSHIP TYPE DIMENSION SURROGATE KEY]=MBR_DIM.[MEMBERSHIP TYPE DIMENSION SURROGATE KEY]) LEFT JOIN ENTERPRISEDATAMART.DM_01.[APPOINTMENT DIMENSION] APPT_DIM  ON (APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY]=APPT_DIM.[APPOINTMENT DIMENSION SURROGATE KEY]) LEFT JOIN ENTERPRISEDATAMART.DM_01.[DATE DIMENSION] DATE_DIM  ON (APPT_DIM.[APPOINTMENT DATE]=DATE_DIM.[CALENDAR DATE DATE]) LEFT JOIN ENTERPRISEDATAMART.DM_01.[ORGANIZATION DIMENSION] APPT_ORG_HIST     ON ((APPT_FCT.[APPOINTMENT ORGANIZATION DIMENSION SURROGATE KEY]=APPT_ORG_HIST.[ORGANIZATION DIMENSION SURROGATE KEY])     AND (APPT_DIM.[APPOINTMENT DATE] BETWEEN APPT_ORG_HIST.[EFFECTIVE BEGIN DATE] AND APPT_ORG_HIST.[EFFECTIVE END DATE])) LEFT JOIN ENTERPRISEDATAMART.DM_01.[SALE_HIER_DIM] SALE_HIER_DIM  ON ((SALE_HIER_DIM.EFF_BEG_DT<=APPT_DIM.[APPOINTMENT DATE])   AND (APPT_DIM.[APPOINTMENT DATE]<= DATEADD(DAY,-1,SALE_HIER_DIM.EFF_END_DT))    AND SALE_HIER_DIM.CURR_ROW_IND = 'Y'   AND APPT_ORG_HIST.[SOURCE SYSTEM KEY TEXT]=SALE_HIER_DIM.[SALE_HIER_ID])   WHERE   APPT_DIM.[CURRENT ROW INDICATOR] = 'Y'  AND APPT_DIM.[APPOINTMENT CREATE DATE OVER 14 DAYS] = 'N'  AND APPT_FCT.[PERSON DIMENSION SURROGATE KEY] <> 0  AND APPT_FCT.[APPOINTMENT STATUS NAME] <> 'DECLINED'  AND SALE_HIER_DIM.ORZN_DEPT_CDE IN ('0115','0190','0283','0291','0361','0384','0435','0525','0383')  AND APPT_ORG_HIST.[JOB TYPE CODE] IN ('001000','001001','001002','001003','001004','001005','001006','001007','001011','001505','002000', '002003', '002008', '002010', '002011', '002012', '002016', '002022', '002024', '002025', '002026', '002027', '002030', '002031', '002032', '003100', '003500')  AND APPT_DIM.[EVENT RESULT DESCRIPTION] NOT IN ('NO SHOW','CANCELLED')  AND DATE_DIM.[CALENDAR YEAR NAME] >= YEAR(GETDATE()-10)-2 AND DATE_DIM.[CALENDAR YEAR NAME] <=YEAR(GETDATE()-10)         AND APPT_ORG_HIST.[SOURCE SYSTEM KEY TEXT] NOT IN ('TS06033', 'TS13320', 'TS21623', 'TS22934', 'TS32012', 'TS34394', 'TS34662', 'TS67185')         AND APPT_DIM.[APPOINTMENT TYPE CODE] <> 'UNK'     GROUP BY  APPT_FCT.[WORKER APPOINTMENT ASSOCIATION],  APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY],   APPT_FCT.[PERSON DIMENSION SURROGATE KEY],  MBR_DIM.[MEMBERSHIP TYPE CODE],  MBR_DIM.[MEMBERSHIP TYPE DESCRIPTION],  APPT_DIM.[APPOINTMENT DATE],  APPT_DIM.[APPOINTMENT TYPE CODE],  APPT_DIM.[APPOINTMENT TYPE DESCRIPTION],  APPT_DIM.[FIELD USER COUNT],  DATE_DIM.[CALENDAR YEAR NAME],  DATE_DIM.[CALENDAR YEAR WEEK NUMBER],  DATE_DIM.[CALENDAR WEEK END DATE],  SALE_HIER_DIM.ORZN_DEPT_CDE,  SALE_HIER_DIM.ORZN_ZONE_CDE,          SALE_HIER_DIM.ORZN_SUB_DEPT_CDE,   APPT_ORG_HIST.[SOURCE SYSTEM KEY TEXT],  APPT_ORG_HIST.[ADJUSTED SERVICE DATE],  APPT_ORG_HIST.[EMPLOYEE STATUS TYPE DESCRIPTION],  APPT_ORG_HIST.[FINANCIAL PROFESSIONAL TENURE],   APPT_ORG_HIST.[FINANCIAL PROFESSIONAL TENURE GROUP],  APPT_ORG_HIST.[DEPARTMENT IDENTIFIER],  APPT_ORG_HIST.[RFO ZONE IDENTIFIER],  APPT_ORG_HIST.[JOB TYPE CODE],  APPT_ORG_HIST.[JOB TYPE DESCRIPTION]  HAVING   SUM(APPT_FCT.[APPOINTMENT OCCURS])>0']),    #'ADDED CUSTOM' = TABLE.ADDCOLUMN(SOURCE, 'TSID_MARKET_APPT', EACH [TSID]&'-'&NUMBER.TOTEXT([APPOINTMENT DIMENSION SURROGATE KEY])),     #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(#'ADDED CUSTOM',{{'APPOINTMENT DATE', TYPE DATE}, {'CALENDAR WEEK END DATE', TYPE DATE}, {'ADJUSTED SERVICE DATE', TYPE DATE}}),    #'REMOVED COLUMNS' = TABLE.REMOVECOLUMNS(#'CHANGED TYPE',{'PERSON DIMENSION SURROGATE KEY', 'APPT_OCCURS', 'MEMBERSHIP TYPE DESCRIPTION', 'APPOINTMENT TYPE DESCRIPTION', 'CALENDAR YEAR NAME', 'CALENDAR YEAR WEEK NUMBER', 'CALENDAR WEEK END DATE', 'RFO', 'HISTORICAL FP TENURE', 'HISTORICAL FP TENURE GROUP', 'HISTORICAL JOB TYPE DESCRIPTION'})IN    #'REMOVED COLUMNS

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

    if val.strip().startswith("CASE"):
        if " END " in val:
            return True

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

def extract_source_tables(query):
    # Update the pattern to capture full table names with or without brackets
    table_pattern = re.compile(r"(FROM|JOIN)\s+((?:\[[a-zA-Z0-9_]+\]\.){0,2}(?:\[[a-zA-Z0-9_]+\]|\w+\.\w+))", re.IGNORECASE)
    
    # Clean the query before parsing
    cleaned_query = clean_sql_query(query)

    # Use regex to find all table references in the FROM and JOIN clauses
    matches = table_pattern.findall(cleaned_query)

    # Set to store unique table names
    table_names = set()

    for match in matches:
        # Extract the actual table name (match[1] is the table name)
        table_name = match[1]

        # Remove square brackets if present and strip any extra spaces
        table_name = table_name.replace("[", "").replace("]", "").strip()
        
        # Add the cleaned table name to the set
        table_names.add(table_name)

    return list(table_names)

source_tables = extract_source_tables(query)
print("Source Tables:", source_tables)
# def extract_main_table(query):
#     # Clean the query to make it more uniform
#     cleaned_query = clean_sql_query(query)

#     # Modified pattern to stop before LEFT, INNER, or JOIN keywords, ensuring they are excluded
#     main_table_pattern = re.compile(r"FROM\s+([a-zA-Z0-9_\[\].\s]+?)(?=\s+(?:LEFT|INNER|JOIN|RIGHT|FULL|WHERE|GROUP|ORDER|HAVING|$))", re.IGNORECASE)

#     # Search for the main table using the defined pattern
#     main_table_match = re.search(main_table_pattern, cleaned_query)
#     if main_table_match:
#         # Extract the main table name and clean brackets if present
#         main_table = main_table_match.group(1).replace("[", "").replace("]", "").strip()
#         print(f"Main Source Table: {main_table}")
#         return main_table

#     return None

# Call the function with the query

def preprocess_query( query ):
    query = query.replace("/*", "")
    query = query.replace("*/", "")
    query = query.replace("--", "")
    if "''" in query:
        arr = query.split("''")
        i=1
        result = ""
        for a in arr:            
            if i%2 == 1:
                result = result + a + "["
            else:
                result = result + a + "]"
            i = i+1
        query = result
    return query

# Function to extract tables and columns from the SELECT part of the query
def extract_columns(query):
    query = preprocess_query(query)

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
            if total == 1 and "AS" in column:
                arr = column.split(" AS ")
                if len(arr) == 2:
                    val = arr[0].replace("[", "").replace("]", "")
                    input_columns.append(val)
                    val = arr[1].replace("[", "").replace("]", "")                
                    output_column = val
            elif total > 1:
                for i in range(total-1):
                    val = str(res[i]).replace("[", "").replace("]", "")
                    if val not in input_columns and not val.isnumeric():
                        input_columns.append(val)
                val = str(res[total-1]).replace("[", "").replace("]", "")
                output_column = val                
            else:               
                val = str(res[0]).replace("[", "").replace("]", "")
                output_column = val                    
                if column.endswith("[MARKET]"):
                    s = column.split(res[0])[0].strip()
                    if s.endswith("AS"):
                        s = s[: len(s)-2]
                    if("." in s):
                        val = s.split(".")[1]
                        res = re.split(r"[^_a-zA-Z0-9\s]", val)
                        val = res[0]
                        if val not in input_columns and not val.isnumeric():
                            input_columns.append(val)   
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
