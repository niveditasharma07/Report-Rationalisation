import pandas as pd
import sqlparse
import re
from sqlglot import parse_one, exp
from collections import OrderedDict

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT DATE"}
query = """
SELECT TASK_FCT.[TASK DIMENSION SURROGATE KEY],        TASK_FCT.[PERSON DIMENSION SURROGATE KEY],        TASK_DIM.[TASK DATE],        SUM(TASK_FCT.[TASK OCCURS]) [TASK_OCCURS],        TASK_FCT.[PERSON OCCURS],         TASK_DIM.[TASK TYPE NAME],        TASK_DIM.[TASK STATUS NAME],        DATE_DIM.[CALENDAR YEAR NAME] AS ''PERFORMANCE YEAR NAME'',        DATE_DIM.[CALENDAR YEAR WEEK NUMBER] AS ''PERFORMANCE YEAR WEEK NUMBER'',        ORG_DIM.[SOURCE SYSTEM KEY TEXT] AS TSID,        --SALE_HIER_DIM.ORZN_DEPT_CDE,        --SALE_HIER_DIM.ORZN_ZONE_CDE,        ORG_DIM.[FIRST NAME] + ' ' + ORG_DIM.[LAST NAME] AS NAME         --ORG_DIM.[JOB TYPE CODE] AS JOB_TYP_CDE,        --ORG_DIM.[ADJUSTED SERVICE DATE] AS ADJ_SVC_DT,        --ORG_DIM.[EMPLOYEE STATUS TYPE CODE] AS EMP_STS_TYP_CDE   FROM ENTERPRISEDATAMART.[DM_01].[TASK FACT] TASK_FCT LEFT JOIN        ENTERPRISEDATAMART.[DM_01].[TASK DIMENSION] TASK_DIM     ON TASK_FCT.[TASK DIMENSION SURROGATE KEY] = TASK_DIM.[TASK DIMENSION SURROGATE KEY] LEFT JOIN        ENTERPRISEDATAMART.DM_01.[DATE DIMENSION] DATE_DIM     ON TASK_DIM.[TASK DATE]=DATE_DIM.[CALENDAR DATE DATE] LEFT JOIN         ENTERPRISEDATAMART.DM_01.[ORGANIZATION DIMENSION] ORG_DIM     ON ((TASK_FCT.[TASK ORGANIZATION DIMENSION SURROGATE KEY] = ORG_DIM.[ORGANIZATION DIMENSION SURROGATE KEY]) AND     (TASK_DIM.[TASK DATE] BETWEEN ORG_DIM.[EFFECTIVE BEGIN DATE] AND ORG_DIM.[EFFECTIVE END DATE])) LEFT JOIN         ENTERPRISEDATAMART.DM_01.[SALE_HIER_DIM] SALE_HIER_DIM    ON ((SALE_HIER_DIM.EFF_BEG_DT <= TASK_DIM.[TASK DATE]) AND        (TASK_DIM.[TASK DATE] <= DATEADD(DAY,-1,SALE_HIER_DIM.EFF_END_DT)) AND          SALE_HIER_DIM.CURR_ROW_IND = 'Y' AND          ORG_DIM.[SOURCE SYSTEM KEY TEXT]=SALE_HIER_DIM.[SALE_HIER_ID])   WHERE TASK_DIM.[TASK TYPE NAME] IN ('APPROACH  CALL','APPROACH CALL')    AND TASK_DIM.[TASK STATUS NAME] IN ('COMPLETED', 'AUTO COMPLETED', 'IN PROGRESS')     AND TASK_FCT.[PERSON DIMENSION SURROGATE KEY] <> 0    AND SALE_HIER_DIM.ORZN_DEPT_CDE IN ('0115','0165','0190','0240','0283','0291','0361','0365','0384','0410','0435','0475','0496','0525','0529','0810')    AND (ORG_DIM.[JOB TYPE CODE] IN ('002000', '002003', '002008', '002010', '002011', '002012', '002016', '002022', '002025', '002026', '002027', '002030', '002032', '003100', '003500')     OR ORG_DIM.[DEPARTMENT IDENTIFIER] IN ('5405','5407','5408','5409','1701','1702','1703','0383'))       AND DATE_DIM.[PERFORMANCE YEAR NAME] IN ('2022','2023')      GROUP BY    TASK_FCT.[TASK DIMENSION SURROGATE KEY],    TASK_FCT.[PERSON DIMENSION SURROGATE KEY],    TASK_DIM.[TASK DATE],    DATE_DIM.[CALENDAR YEAR NAME],    DATE_DIM.[CALENDAR YEAR WEEK NUMBER],    TASK_FCT.[PERSON OCCURS],     TASK_DIM.[TASK TYPE NAME],    TASK_DIM.[TASK STATUS NAME],    DATE_DIM.[CALENDAR YEAR NAME],    DATE_DIM.[CALENDAR YEAR WEEK NUMBER],    DATE_DIM.[CALENDAR WEEK END DATE],    ORG_DIM.[SOURCE SYSTEM KEY TEXT],    --SALE_HIER_DIM.ORZN_DEPT_CDE,    --SALE_HIER_DIM.ORZN_ZONE_CDE,    ORG_DIM.[FIRST NAME] + ' ' + ORG_DIM.[LAST NAME]     --ORG_DIM.[JOB TYPE CODE],    --ORG_DIM.[ADJUSTED SERVICE DATE],    --ORG_DIM.[EMPLOYEE STATUS TYPE CODE]   HAVING     SUM(TASK_FCT.[TASK OCCURS]) > 0   

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
            if total == 1 and " AS " in column:
                arr = column.split(" AS ")
                if len(arr) == 2:
                    val = arr[0].replace("[", "").replace("]", "")
                    if "(" in val:
                        if val.startswith("CASE "):
                            val = val.replace("CASE", "").replace("WHEN", "").replace("THEN", "")
                        res = re.split(r"[^_a-zA-Z0-9\s]", val)                  
                        for val in res:
                            val = val.strip()
                            if (val not in KEYWORDS) and (len(val) > 1 and not val.isnumeric()):
                                if val not in input_columns:
                                    input_columns.append(val)  
                    else:
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