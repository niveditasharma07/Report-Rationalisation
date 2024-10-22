import pandas as pd
import sqlparse
import re
from sqlglot import parse_one, exp
from collections import OrderedDict

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}
query = """
SELECT --IA_DATES_ITRL.DT_SK AS ''DATE SURROGATE KEY'',       -- TF_FIN_CUST_EVT_FCT_CFDL.CTRC_VRSN_NBR AS ''CONTRACT VERSION NUMBER'',       -- TF_FIN_CUST_EVT_FCT_CFDL.WRTG_AGT_VRSN_NBR AS ''WRITING AGENT VERSION NUMBER'',        TF_FIN_CUST_EVT_FCT_CFDL.FIN_CUST_PNT_STS_CDE AS ''FINANCIAL CUSTOMER POINT STATUS CODE'',       -- TF_FIN_CUST_EVT_FCT_CFDL.CUST_AGE AS ''FINANCIAL CUSTOMER AGE'',       -- TF_FIN_CUST_EVT_FCT_CFDL.AGE_CLAS_CDE AS ''AGE CLASS CODE'',       -- TF_FIN_CUST_EVT_FCT_CFDL.AGE_CLAS_DSCR AS ''AGE CLASS DESCRIPTION'',        TF_FIN_CUST_EVT_FCT_CFDL.FIN_CUST_PNT_VLU AS ''FINANCIAL CUSTOMER POINT VALUE'',        CNF_CUSTOMER_DIM_CFDL.CUST_ID_NBR,        CNF_CUSTOMER_DIM_CFDL.CUST_LGAL_FMT_NM,         --CNF_CUSTOMER_DIM_CFDL.CUST_BRTH_DT,       -- CNF_CUSTOMER_DIM_CFDL.MBR_TYP_CDE AS ''MEMBER TYPE CODE'',       -- CNF_CUSTOMER_DIM_CFDL.MBR_TYP_DSCR AS ''MEMBER TYPE DESCRIPTION'',       -- CNF_CUSTOMER_DIM_CFDL.RSDL_ST_CDE AS ''RESIDENTIAL STATE CODE'',        CNF_CONTRACT_DIM_DTL_CFDL.CTRC_ISS_DT AS ''CONTRACT ISSUE DATE'',        -- CNF_CONTRACT_DIM_DTL_CFDL.L5_PLOB_SGRP_CDE AS ''LEVEL 5 PRODUCT LINE OF BUSINESS SUBGROUP CODE'',       -- CNF_CONTRACT_DIM_DTL_CFDL.L5_PLOB_SGRP_DSCR AS ''LEVEL 5 PRODUCT LINE OF BUSINESS SUBGROUP DESCRIPTION'',       -- CNF_CONTRACT_DIM_DTL_CFDL.L4_PLOB_GRP_CDE AS ''LEVEL 4 PRODUCT LINE OF BUSINESS GROUP CODE'',       -- CNF_CONTRACT_DIM_DTL_CFDL.L4_PLOB_GRP_DSCR AS ''LEVEL 4 PRODUCT LINE OF BUSINESS GROUP DESCRIPTION'',        CNF_CONTRACT_DIM_DTL_CFDL.L3_PROD_LOB_CDE AS ''LEVEL 3 PRODUCT LINE OF BUSINESS CODE'',        CNF_CONTRACT_DIM_DTL_CFDL.L3_PROD_LOB_DSCR AS ''LEVEL 3 PRODUCT LINE OF BUSINESS DESCRIPTION'',      --  CNF_CONTRACT_DIM_DTL_CFDL.L2_PROD_CHAR_CDE AS ''LEVEL 2 PRODUCT CHARACTER CODE'',       -- CNF_CONTRACT_DIM_DTL_CFDL.L2_PROD_CHAR_DSCR AS ''LEVEL 2 PRODUCT CHARACTER DESCRIPTION'',       -- AGT_DIM_CURR_CFDL.PARY_DSPL_2_NM AS ''ASSIGNED FR'',       -- AGT_DIM_CURR_CFDL.FRST_NM AS ''EMPLOYEE FIRST NAME'',       -- AGT_DIM_CURR_CFDL.MDDL_NM AS ''EMPLOYEE MIDDLE NAME'',       -- AGT_DIM_CURR_CFDL.LST_NM AS ''EMPLOYEE LAST NAME'',       AGT_DIM_CURR_CFDL.FRST_NM || ' ' || AGT_DIM_CURR_CFDL.LST_NM AS EMPL_NM,       -- AGT_DIM_CURR_CFDL.ADJ_SVC_DT AS ''ADJUSTED SERVICE DATE'',        AGT_DIM_CURR_CFDL.EMP_ID AS ''TS ID'',       -- AGT_DIM_CURR_CFDL.EMP_STS_TYP_CDE AS ''EMPLOYEE STATUS TYPE CODE'',       -- AGT_DIM_CURR_CFDL.EMP_STS_TYP_DSCR AS ''EMPLOYEE STATUS TYPE DESCRIPTION'',       -- AGT_DIM_CURR_CFDL.JOB_TYP_CDE AS ''JOB TYPE CODE'',       -- AGT_DIM_CURR_CFDL.JOB_TYP_DSCR AS ''JOB TYPE DESCRIPTION'',       -- AGT_DIM_CURR_CFDL.CURR_ROW_IND AS ''CURRENT ROW INDICATOR SALES HIERARCHY'',        IA_DATES_ITRL.CAL_DAY_DT AS ''CALENDAR DATE'',        --IA_DATES_ITRL.CAL_MTH_NM,       -- IA_DATES_ITRL.CAL_MTH_STRT_DT AS ''CALENDAR MONTH STATE DATE'',       -- IA_DATES_ITRL.CAL_MTH_END_DT AS ''CALENDAR MONTH END DATE'',        --IA_DATES_ITRL.CAL_YR_QTR_NBR,        --IA_DATES_ITRL.CAL_QTR_NM,        --IA_DATES_ITRL.CAL_WK_NM,        FIN_CUST_ELIG_DIM_CFDL.FIN_CUST_PNT_RSN_CDE AS ''FINANCIAL CUSTOMER POINT REASON CODE'',        FIN_CUST_ELIG_DIM_CFDL.FIN_CUST_PNT_RSN_DSCR AS ''FINANCIAL CUSTOMER POINT REASON DESCRIPTION'',        FIN_CUST_ELIG_DIM_CFDL.CTRC_RLTN_TYP_CDE AS ''FINANCIAL CUSTOMER POINT RELATIONSHIP CODE'',        FIN_CUST_ELIG_DIM_CFDL.CTRC_RLTN_TYP_DSCR AS ''FINANCIAL CUSTOMER POINT RELATIONSHIP DESCRIPTION'',       -- CASE       --    WHEN (CUST_AGE >= 0) AND (CUST_AGE <= 15) THEN '0-15'       --    WHEN (CUST_AGE >= 16) AND (CUST_AGE <= 25) THEN '16-25'       --    WHEN (CUST_AGE >= 26) AND (CUST_AGE <= 35) THEN '26-35'       --    WHEN (CUST_AGE >= 36) AND (CUST_AGE <= 45) THEN '36-45'       --    WHEN (CUST_AGE >= 46) AND (CUST_AGE <= 55) THEN '46-55'       --    WHEN (CUST_AGE >= 56) AND (CUST_AGE <= 65) THEN '56-65'       --    WHEN (CUST_AGE >= 66) AND (CUST_AGE <= 75) THEN '66-75'       --    WHEN CUST_AGE > 75 THEN '76+'       -- END       --    AS ''FINANCIAL CUSTOMER AGE GROUP'',       -- INTEGER (FLOOR ( (CURRENT DATE - CUST_BRTH_DT) / 10000)) AS ''MEMBER AGE'',       -- DTRB_PERF_DATES.DTRB_PERF_RPT_WK_END_DT AS ''DISTRIBUTION PERFORMANCE REPORTING WEEK END DATE'',       -- DTRB_PERF_DATES.DTRB_PERF_RPT_WK_NBR AS ''DISTRIBUTION PERFORMANCE REPORTING WEEK NUMBER'',        DTRB_PERF_DATES.DTRB_PERF_RPT_YR_NBR AS ''DISTRIBUTION PERFORMANCE REPORTING YEAR NUMBER'',       -- DTRB_PERF_DATES.DTRB_PERF_RPT_YR_WK_NBR AS ''DISTRIBUTION PERFORMANCE REPORTING YEAR WEEK NUMBER'',       -- DTRB_PERF_DATES.DTRB_PERF_RPT_WK_TXT,        DTRB_PERF_DATES.DTRB_PERF_RPT_YR_TXT       --  DTRB_PERF_DATES.DTRB_PERF_RPT_DAY_TXT,      --  DTRB_PERF_DATES.DTRB_PERF_RPT_MTH_TXT,      --  DTRB_PERF_DATES.DTRB_PERF_RPT_QTR_TXT,      --  SALE_HIER_DIM.SALE_HIER_ID AS ''SALES HIERARCHY IDENTIFIER''        -- SALE_HIER_DIM.EFF_BEG_TMSP AS ''EFFECTIVE BEGIN TIMESTAMP'',       -- SALE_HIER_DIM.EFF_END_TMSP AS ''EFFECTIVE END TIMESTAMP'',       -- SALE_HIER_DIM.CURR_ROW_IND AS ''AGENT CURRENT ROW INDICATOR'',       -- SALE_HIER_DIM.PRTR_NM AS ''PARTNER NAME'',       -- SALE_HIER_DIM.MANP_NM AS ''MANAGING PARTNER NAME'',       -- SALE_HIER_DIM.ORZN_ZONE_CDE AS ''MARKET'',       -- SALE_HIER_DIM.ORZN_ZONE_DSCR AS ''ORGANIZATION ZONE DESCRIPTION'',       -- SALE_HIER_DIM.ORZN_DEPT_CDE AS ''RFO'',       -- SALE_HIER_DIM.ORZN_DEPT_DSCR AS ''ORGANIZATION DEPARTMENT DESCRIPTION'',       -- SALE_HIER_DIM.ORZN_DIV_CDE AS ''ORGANIZATION DIVISION CODE'',       -- SALE_HIER_DIM.ORZN_DIV_DSCR AS ''ORGANIZATION DIVISION DESCRIPTION'',       -- SALE_HIER_DIM.SALE_HIER_DIM_SK AS ''SALES HIERARCHY DIMENSION SURROGATE KEY''    FROM ((((((COMMON.IA_DATES_ITRL IA_DATES_ITRL INNER JOIN              COMMON.DTRB_PERF_DATES DTRB_PERF_DATES           ON (IA_DATES_ITRL.DT_SK = DTRB_PERF_DATES.DT_SK)) LEFT OUTER JOIN              SALES.TF_FIN_CUST_EVT_FCT_CFDL TF_FIN_CUST_EVT_FCT_CFDL           ON (IA_DATES_ITRL.DT_SK = TF_FIN_CUST_EVT_FCT_CFDL.FIN_CUST_CRNG_DT_SK)) RIGHT OUTER JOIN              HUMAN_RESOURCES.SALE_HIER_DIM SALE_HIER_DIM           ON (SALE_HIER_DIM.SALE_HIER_DIM_SK = TF_FIN_CUST_EVT_FCT_CFDL.SLHR_DIM_CURR_CRNG_AGT_SK)) LEFT OUTER JOIN              MEMBER.CNF_CUSTOMER_DIM_CFDL CNF_CUSTOMER_DIM_CFDL           ON (CNF_CUSTOMER_DIM_CFDL.CUST_SK = TF_FIN_CUST_EVT_FCT_CFDL.CUST_SK)) LEFT OUTER JOIN              HUMAN_RESOURCES.AGT_DIM_CURR_CFDL AGT_DIM_CURR_CFDL           ON (AGT_DIM_CURR_CFDL.AGT_DIM_SK = TF_FIN_CUST_EVT_FCT_CFDL.CRNG_AGT_SK)) RIGHT OUTER JOIN              SALES.FIN_CUST_ELIG_DIM_CFDL FIN_CUST_ELIG_DIM_CFDL           ON (FIN_CUST_ELIG_DIM_CFDL.FIN_CUST_PNT_ELIG_DIM_SK = TF_FIN_CUST_EVT_FCT_CFDL.FIN_CUST_PNT_ELIG_DIM_SK)) LEFT OUTER JOIN              CONTRACT.CNF_CONTRACT_DIM_DTL_CFDL CNF_CONTRACT_DIM_DTL_CFDL           ON (CNF_CONTRACT_DIM_DTL_CFDL.CTRC_SK = TF_FIN_CUST_EVT_FCT_CFDL.CTRC_SK) AND               (CNF_CONTRACT_DIM_DTL_CFDL.VRSN_NBR = TF_FIN_CUST_EVT_FCT_CFDL.CTRC_VRSN_NBR)                        WHERE (AGT_DIM_CURR_CFDL.CURR_ROW_IND = 'Y')    AND (SALE_HIER_DIM.CURR_ROW_IND = 'Y')    AND (SALE_HIER_DIM.ORZN_DEPT_CDE IN ('0001', '0115','0165','0190','0240','0283','0291','0361','0365','0384','0410','0435','0475','0496','0525','0529','0810')     OR AGT_DIM_CURR_CFDL.ORZN_DEPT_CDE IN ('5405','5407','5408','5409','1701','1702','1703','0383'))   AND DTRB_PERF_DATES.DTRB_PERF_RPT_YR_NBR IN ('2021','2022','2023','2024')    AND SUBSTRING(SALE_HIER_DIM.SALE_HIER_ID,1,2)='TS'    AND TF_FIN_CUST_EVT_FCT_CFDL.FIN_CUST_PNT_VLU <> 0']),    #'CHANGED TYPE' = TABLE.TRANSFORMCOLUMNTYPES(SOURCE,{{'CALENDAR DATE', TYPE DATE}, {'CONTRACT ISSUE DATE', TYPE DATE}}),    #'ADDED CUSTOM' = TABLE.ADDCOLUMN(#'CHANGED TYPE', 'TRIM TSID', EACH TEXT.TRIM([TS ID]))IN    #'ADDED CUSTOM

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

# Function to extract tables and columns from the SELECT part of the query
def extract_columns(query):
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