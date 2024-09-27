import pandas as pd
import sqlparse
import re
from sqlglot import parse_one, exp
from collections import OrderedDict

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}
query = """
SELECT  A.EMP_ID,          A.JOB_TYP_DSCR,         A.EMP_STS_TYP_CDE,         A.CURR_ROW_IND, A.TRMN_DT,         A.ADJ_SVC_DT,        /* TENURE */         (DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT) ) / 365.25 AS TENURE,         /* TENURE_GROUP */         (CASE WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 1 THEN 'NFR1'                WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 1 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 2 THEN 'NFR2'              WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 2 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 3 THEN 'NFR3'               WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 3 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 4 THEN 'NFR4'               WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 4 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 5 THEN 'VET5'               WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 5 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 6 THEN 'VET6'              WHEN ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) > 6 AND                   ((DAYS(CURRENT_DATE)-DAYS(A.ADJ_SVC_DT))/365.25 ) <= 7 THEN 'VET7'                ELSE 'VET8+'          END) AS TENURE_GROUP              FROM HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL A    WHERE A.EMP_STS_TYP_CDE IN ('A','I','L','D','T','R')     AND A.CURR_ROW_IND = 'Y'               AND (A.JOB_FMLY_CDE LIKE 'FLD%' OR          A.JOB_FMLY_CDE ='SALES' OR          A.TF_JOB_CLAS_CDE ='FAC')     AND A.ORZN_DEPT_CDE NOT IN ('2410','3005','7151','7152',                                 '9150','9134','NA', '4100',                                 '6053','6100','8900')     AND (A.JOB_TYP_CDE IN ('002003','002010','002011','002012','002016','002017','002018','002019',                            '002000','002020','002021','002022','002024','002026','002027',                            '002030','002031','002032','003100','003500','003602',                            '003604','003605','003606','003607','003608') OR           A.ORZN_DEPT_CDE IN ('5405','5407','5408','5409','6100','1701','1702','1703'))    AND A.ADJ_SVC_DT IS NOT NULL     ORDER BY A.EMP_ID']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH TRUE),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(#'FILTERED ROWS',{{'ADJ_SVC_DT', TYPE DATE}, {'TENURE', INT64.TYPE}, {'TRMN_DT', TYPE DATE}}),    #'ADDED CUSTOM' = TABLE.ADDCOLUMN(#'CHANGED TYPE', 'TSID FOR MERGE', EACH TEXT.TRIM([EMP_ID]))IN    #'ADDED CUSTOM
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