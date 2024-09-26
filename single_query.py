import pandas as pd
import sqlparse
import re
from sqlglot import parse_one, exp
from collections import OrderedDict

KEYWORDS = {"SUM", "MIN", "MAX", "AVG"}
query = """
SELECT DISTINCT T1.ORZN_DEPT_CDE AS RFO_CDE, T1.ORZN_ZONE_CDE AS RFO_ZONE_NM, (SUBSTRING(T1.ORZN_ZONE_CDE,6,5)) AS MARKET_ID, T3.ORZN_DEPT_DSCR, T3.EMP_ID, T3.EMP_STS_TYP_CDE, T3.TRMN_DT, T3.JOB_TYP_CDE, T3.JOB_TYP_DSCR, T3.EMP_NM, CASE WHEN T3.EMP_NM IS NULL THEN 'VACANT' ELSE T3.EMP_NM END AS MARKET_LEADERS FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN ( SELECT DISTINCT T1.SALE_HIER_ID, T1.ORZN_ZONE_CDE, T1.ORZN_DEPT_DSCR, T2.ORZN_DEPT_CDE, T2.EMP_ID, T2.EMP_STS_TYP_CDE, T2.TRMN_DT, T2.JOB_TYP_CDE, T2.JOB_TYP_DSCR, T2.EMP_NM FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL T2 ON T1.SALE_HIER_ID = T2.EMP_ID WHERE T1.CURR_ROW_IND = 'Y' AND T2.EMP_STS_TYP_CDE IN ('A') AND T1.EFF_END_TMSP IS NULL AND T2.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL AND T2.JOB_TYP_CDE IN ('001002', '001003',/* '001004', '001005',*/ '001007') AND T2.EMP_ID NOT IN('TS62904','TS67022') ) T3 ON T1.ORZN_ZONE_CDE = T3.ORZN_ZONE_CDE WHERE T1.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL --AND T1.RFO_ZONE_STS_CDE IN ('A') AND SUBSTRING(T1.ORZN_ZONE_CDE,6,2) <> '00' AND T1.ORZN_DEPT_CDE <> 'UKWN' ORDER BY T1.ORZN_ZONE_CDE; ']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH ([EMP_ID] <> 'TS18670    ' AND [EMP_ID] <> 'TS73067    ' AND [EMP_ID] <> 'TS73124    ' AND [EMP_ID] <> 'TS74827    ' AND [EMP_ID] <> 'TS77319    '))IN    #'FILTERED ROWS
"""

# Function to check if column is complete
def is_balanced(val):
    count_open_sqr = val.count("[")
    count_clse_sqr = val.count("]")
    count_open_flr = val.count("{")
    count_clse_flr = val.count("{")
    count_open_brc = val.count("(")
    count_clse_brc = val.count(")")   
    if ( (count_open_sqr - count_clse_sqr) != 0 or (count_open_flr - count_clse_flr) !=0 or 
        (count_open_brc - count_clse_brc) != 0):
        return False
    else:
        return True 

# Function to clean and simplify the SQL query
def clean_sql_query(query):
    query = re.sub(r"['\"]", "", query)  # Remove single and double quotes
    query = re.sub(r"(\n|\t|\r)", " ", query)  # Remove new lines and tabs
    query = re.sub(r"\s+", " ", query)  # Replace multiple spaces with a single space
    return query.strip()

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
                if val not in input_columns:
                    input_columns.append(val)   
            else:
                if( not contains_table_ref):
                    res = re.split(r"[^_a-zA-Z0-9\s]", col)
                    for val in res:
                        if (val not in KEYWORDS) and (len(val) > 1):
                            input_columns.append(val)                                

    else :
        if column.endswith("]"):
            # Check if it contains both input and output
            total = len(res)
            if total > 1:
                for i in range(total-1):
                    val = str(res[i]).replace("[", "").replace("]", "")
                    if val not in input_columns:
                        input_columns.append(val)
                val = str(res[total-1]).replace("[", "").replace("]", "")
                output_column = val
            else:
                val = str(res[0]).replace("[", "").replace("]", "")
                output_column = val
        else :
            for col in res:
                val = str(col).replace("[", "").replace("]", "")
                if val not in input_columns:
                        input_columns.append(val)

    return input_columns, output_column
        

clean_sql_query(query)
columns = extract_columns(query)
all_columns = {}
for column in columns:
    input_columns, output_column = split_column_and_alias(column)
    all_columns[output_column] = input_columns

print(all_columns)