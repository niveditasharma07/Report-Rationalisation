import pandas as pd
import sqlparse
import re
from sqlglot import parse_one, exp
from collections import OrderedDict

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}
query = """
SELECT 
	A.CUST_SK,        
	A.CUST_ID_NBR,        
	CASE WHEN XX.HOH_NAMEXX <> '' THEN XX.HOH_NAMEXX        ELSE A.CUST_LGAL_LST_NM||', '||A.CUST_LGAL_FRST_NM       END AS HOH_NAME,
	A.CUST_LGAL_LST_NM||', '||A.CUST_LGAL_FRST_NM AS CLIENT_NAME,       
	A.CUST_LGAL_FMT_NM,        
	A.CUST_LGAL_FRST_NM,        
	A.CUST_LGAL_LST_NM,        
	A.CUST_GRLN_TYP_CDE,        
	A.CUST_GRP_ID_NBR,        
	B.STP_STS_CDE,        
	CASE WHEN B.FUT_VLU_INDX_CDE = 'GOLD' THEN 'GOLD'             WHEN B.FUT_VLU_INDX_CDE = 'SILVR' THEN 'SILVER'             WHEN B.FUT_VLU_INDX_CDE = 'BRNZE' THEN 'BRONZE'             WHEN B.FUT_VLU_INDX_CDE IN ('NA','UNK',' ' ) THEN 'UNKNOWN'        END AS FUTUREVALUE,        
	CASE WHEN A.MBR_TYP_CDE = 'BEN' THEN 'BENEFIT'             WHEN A.MBR_TYP_CDE = 'ASSOC' THEN 'ASSOCIATE'             WHEN A.MBR_TYP_CDE = 'JUV' THEN 'JUVENILE'             WHEN A.MBR_TYP_CDE = 'NON' AND A.CLAS_CTRC_RLTN_CDE <> 'NA' THEN 'NON-MEMBER'             WHEN A.MBR_TYP_CDE = 'NON' AND A.CLAS_CTRC_RLTN_CDE = 'NA' THEN 'PROSPECT'        END AS MBRTYPE,        CASE WHEN A.CUST_AGE < 0 THEN 'UKWN'            WHEN A.CUST_AGE < 18 THEN '<18'            WHEN A.CUST_AGE < 25 THEN '18-24'            WHEN A.CUST_AGE < 35 THEN '25-34'            WHEN A.CUST_AGE < 45 THEN '35-44'            WHEN A.CUST_AGE < 55 THEN '45-54'            WHEN A.CUST_AGE < 65 THEN '55-64'            WHEN A.CUST_AGE < 75 THEN '65-74'            WHEN A.CUST_AGE < 85 THEN '75-84'            WHEN A.CUST_AGE >= 85 THEN '85+'         ELSE ' '         END AS CUST_AGE_GRP,        CASE WHEN A.CUST_AGE < 18 THEN '<18'             WHEN A.CUST_AGE >= 18 THEN '18+'         ELSE ' '        END AS CLNT_AGE_TYP,        
	CASE WHEN B.FUT_VLU_INDX_CDE = 'GOLD' AND B.STP_STS_CDE = 'YES' THEN 'A'            WHEN B.FUT_VLU_INDX_CDE = 'SILVR' AND B.STP_STS_CDE = 'YES' THEN 'B'          WHEN B.FUT_VLU_INDX_CDE = 'GOLD' AND B.STP_STS_CDE <> 'YES' THEN 'B'          WHEN B.FUT_VLU_INDX_CDE = 'BRNZE' AND B.STP_STS_CDE = 'YES' THEN 'C'          WHEN B.FUT_VLU_INDX_CDE = 'SILVR' AND B.STP_STS_CDE <> 'YES' THEN 'C'          WHEN B.FUT_VLU_INDX_CDE = 'BRNZE' AND B.STP_STS_CDE <> 'YES' THEN 'D'          ELSE ' '        END AS CLIENT_SERVICE_MODEL,        
	C.EMP_ORZN_ID   
FROM MEMBER.ITGR_IDVL_ALL_CURR_CFDL A LEFT JOIN        MEMBER.CNF_CUST_RESID_MAIL_GRP_DIM_CFDL B     ON A.CUST_GRP_SK=B.CUST_GRP_SK INNER JOIN         CLNT_ASMT_DM.CUST_REPR_ASGN_CURR_CFDL C       /* CLNT_ASMT_DM.CUST_SREP_RLTN_CURR_CFDL C */       /* CLNT_ASMT_DM.CUST_REPR_COMP_RLTN_CURR_CFDL C */     ON A.CUST_SK = C.ACRT_CUST_SK AND        A.MBR_TYP_CDE IN ('BEN','NON','ASSOC','JUV') AND        C.EMP_ORZN_ID LIKE 'TS%' LEFT JOIN      (SELECT X.CUST_GRP_ID_NBR,               X.CUST_LGAL_LST_NM||', '||X.CUST_LGAL_FRST_NM AS HOH_NAMEXX        
FROM MEMBER.ITGR_IDVL_ALL_CURR_CFDL X       WHERE X.CUST_GRLN_TYP_CDE IN ('PRIM','PRIMD')) AS XX    ON A.CUST_GRP_ID_NBR=XX.CUST_GRP_ID_NBR    WHERE (A.MBR_TYP_CDE IN ('BEN','ASSOC','JUV') OR         A.MBR_TYP_CDE = 'NON' AND A.CLAS_CTRC_RLTN_CDE <> 'NA') ']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH TRUE),    #'REPLACED VALUE' = TABLE.REPLACEVALUE(#'FILTERED ROWS','','UNKNOWN',REPLACER.REPLACEVALUE,{'FUTUREVALUE'}),    #'REPLACED VALUE1' = TABLE.REPLACEVALUE(#'REPLACED VALUE',NULL,'UNKNOWN',REPLACER.REPLACEVALUE,{'FUTUREVALUE'}),    #'APPENDED QUERY' = TABLE.COMBINE({#'REPLACED VALUE1', CLIENT_HHLD_NON_MGP}),    #'REMOVED DUPLICATES' = TABLE.DISTINCT(#'APPENDED QUERY', {'CUST_ID_NBR'}),    #'REPLACED VALUE2' = TABLE.REPLACEVALUE(#'REMOVED DUPLICATES','JUVENILE','YOUTH',REPLACER.REPLACETEXT,{'MBRTYPE'}),    #'ADDED CUSTOM' = TABLE.ADDCOLUMN(#'REPLACED VALUE2', 'YEAR-TSID', EACH 'CY'&[EMP_ORZN_ID]),    #'MERGED QUERIES' = TABLE.NESTEDJOIN(#'ADDED CUSTOM', {'CUST_ID_NBR'}, MGP_CLIENTS, {'CUST_ID_NBR'}, 'MGP_CLIENTS', JOINKIND.LEFTOUTER),    #'EXPANDED MGP_CLIENTS' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES', 'MGP_CLIENTS', {'ASOFDT', 'TSID_MGP', 'GOALS DOCUMENTED', 'GOALS DOCUMENTED TOTAL', 'GOAL INDICATOR'}, {'MGP_CLIENTS.ASOFDT', 'MGP_CLIENTS.TSID_MGP', 'MGP_CLIENTS.GOALS DOCUMENTED', 'MGP_CLIENTS.GOALS DOCUMENTED TOTAL', 'MGP_CLIENTS.GOAL INDICATOR'}),    #'REPLACED VALUE3' = TABLE.REPLACEVALUE(#'EXPANDED MGP_CLIENTS',NULL,'UNKNOWN',REPLACER.REPLACEVALUE,{'MGP_CLIENTS.TSID_MGP'}),    #'REPLACED VALUE4' = TABLE.REPLACEVALUE(#'REPLACED VALUE3',NULL,0,REPLACER.REPLACEVALUE,{'MGP_CLIENTS.GOALS DOCUMENTED'}),    #'REPLACED VALUE5' = TABLE.REPLACEVALUE(#'REPLACED VALUE4',NULL,0,REPLACER.REPLACEVALUE,{'MGP_CLIENTS.GOALS DOCUMENTED TOTAL'}),    #'REPLACED VALUE6' = TABLE.REPLACEVALUE(#'REPLACED VALUE5',NULL,'N',REPLACER.REPLACEVALUE,{'MGP_CLIENTS.GOAL INDICATOR'})IN    #'REPLACED VALUE6
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