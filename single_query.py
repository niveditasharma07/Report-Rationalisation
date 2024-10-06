import pandas as pd
import sqlparse
import re
from sqlglot import parse_one, exp
from collections import OrderedDict

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}
query = """
SELECT DISTINCT 
	T1.ORZN_DEPT_CDE AS RFO_CDE, 
	--T1.ORZN_ZONE_CDE AS RFO_ZONE_NM, 
	T1.ORZN_SUB_DEPT_CDE AS MVP_ID, 
	--(SUBSTRING(T1.ORZN_ZONE_CDE,6,5)) AS MARKET_ID, 
	T3.ORZN_DEPT_DSCR, 
	T3.EMP_ID, 
	T3.EMP_STS_TYP_CDE, 
	T3.TRMN_DT, 
	T3.JOB_TYP_CDE, 
	T3.JOB_TYP_DSCR, 
	T3.EMP_NM, 
	CASE WHEN T3.EMP_NM IS NULL THEN 'VACANT' ELSE T3.EMP_NM END AS MARKET_LEADERS 
FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN ( SELECT DISTINCT T1.SALE_HIER_ID, T1.ORZN_ZONE_CDE, T1.ORZN_DEPT_DSCR, T1.ORZN_SUB_DEPT_CDE, T2.ORZN_DEPT_CDE, T2.EMP_ID, T2.EMP_STS_TYP_CDE, T2.TRMN_DT, T2.JOB_TYP_CDE, T2.JOB_TYP_DSCR, T2.EMP_NM FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL T2 ON T1.SALE_HIER_ID = T2.EMP_ID WHERE T1.CURR_ROW_IND = 'Y' AND T2.EMP_STS_TYP_CDE IN ('A') AND T1.EFF_END_TMSP IS NULL AND T2.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL AND T2.JOB_TYP_CDE IN ('001001') ) T3 ON T1.ORZN_SUB_DEPT_CDE = T3.ORZN_SUB_DEPT_CDE WHERE T1.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL --AND T1.RFO_ZONE_STS_CDE IN ('A') --AND T3.EMP_STS_TYP_CDE = 'A' --AND SUBSTRING(T1.ORZN_ZONE_CDE,6,2) <> '00' AND T1.ORZN_ZONE_CDE <> 'UKWN' ORDER BY T1.ORZN_SUB_DEPT_CDE; ']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH ([RFO_CDE] = '0001      ' OR [RFO_CDE] = '0115      ' OR [RFO_CDE] = '0190      ' OR [RFO_CDE] = '0283      ' OR [RFO_CDE] = '0291      ' OR [RFO_CDE] = '0361      ' OR [RFO_CDE] = '0383      ' OR [RFO_CDE] = '0384      ' OR [RFO_CDE] = '0435      ' OR [RFO_CDE] = '0525      ' OR [RFO_CDE] = '0716      ') AND ([MVP_ID] <> '          ' AND [MVP_ID] <> '0529-00   '))IN    #'FILTERED ROWS

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

def extract_source_tables(query):
    # Define pattern to capture full table names with schemas, brackets, and optional spaces
    table_pattern = re.compile(r"(FROM|JOIN)\s+((?:[a-zA-Z0-9_]+\.){0,2}(?:\[[^\]]+\]|\w+))", re.IGNORECASE)
    
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