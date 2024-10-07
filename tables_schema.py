import re
import sqlparse

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
FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN ( SELECT DISTINCT T1.SALE_HIER_ID, T1.ORZN_ZONE_CDE, T1.ORZN_DEPT_DSCR, T1.ORZN_SUB_DEPT_CDE, T2.ORZN_DEPT_CDE, T2.EMP_ID, T2.EMP_STS_TYP_CDE, T2.TRMN_DT, T2.JOB_TYP_CDE, T2.JOB_TYP_DSCR, T2.EMP_NM FROM SEMANTIC.SALES_HIERARCHY_DIMENSION T1 LEFT JOIN HUMAN_RESOURCES.CNF_EMP_DIM_DTL_CURR_CFDL T2 ON T1.SALE_HIER_ID = T2.EMP_ID WHERE T1.CURR_ROW_IND = 'Y' AND T2.EMP_STS_TYP_CDE IN ('A') AND T1.EFF_END_TMSP IS NULL AND T2.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL AND T2.JOB_TYP_CDE IN ('001001') ) T3 ON T1.ORZN_SUB_DEPT_CDE = T3.ORZN_SUB_DEPT_CDE 
WHERE T1.CURR_ROW_IND = 'Y' AND T1.EFF_END_TMSP IS NULL --AND T1.RFO_ZONE_STS_CDE IN ('A') --AND T3.EMP_STS_TYP_CDE = 'A' --AND SUBSTRING(T1.ORZN_ZONE_CDE,6,2) <> '00' AND T1.ORZN_ZONE_CDE <> 'UKWN' ORDER BY T1.ORZN_SUB_DEPT_CDE; ']),    #'FILTERED ROWS' = TABLE.SELECTROWS(SOURCE, EACH ([RFO_CDE] = '0001      ' OR [RFO_CDE] = '0115      ' OR [RFO_CDE] = '0190      ' OR [RFO_CDE] = '0283      ' OR [RFO_CDE] = '0291      ' OR [RFO_CDE] = '0361      ' OR [RFO_CDE] = '0383      ' OR [RFO_CDE] = '0384      ' OR [RFO_CDE] = '0435      ' OR [RFO_CDE] = '0525      ' OR [RFO_CDE] = '0716      ') AND ([MVP_ID] <> '          ' AND [MVP_ID] <> '0529-00   '))IN    #'FILTERED ROWS
"""

def extract_tables(query):
    result = ""
    # Parse the SQL query
    parsed = sqlparse.parse(query)[0]
    # Set to hold extracted table names (unique names)
    tables = set()
    # Patterns to identify table names in FROM and JOIn clauses
    table_patterns = [
        re.compile(r"\bFROM\s+([a-zA-Z0-9_\.]+)", re.IGNORECASE), 
    # FROM clause
        re.compile(r"\bJOIN\s+([a-zA-Z0-9_\.]+)", re.IGNORECASE),
    # JOIN clause
        re.compile(r"\bWITH\s+([a-zA-Z0-9_\.]+)\s+AS", re.IGNORECASE)
    # WITH clause (CTEs)
    ]
    # Clean and split the query into components
    cleaned_query = clean_sql_query(query)

    #Loop through each pattern and extract the table names
    for pattern in table_patterns:
        matches = re.findall(pattern, cleaned_query)
        for match in matches:
            #Check if schema is present
            if '.' in match:
                schema_name, table_name = match.split(".")
                tables.add((schema_name, table_name))
            else:
                # If no schema, add only the table name
                tables.add((None, match))

    #Output the extracted tables with schema (if present)
    i=0
    for schema, table in tables:    
        if i > 0:
            result = result+","    
        if schema:
            result = f"{result}{schema}:{table}"
        else:
            result = f"{result}{table}"
        i=i+1     

    return result

def clean_sql_query(query):
    # Helper function to clean the query
    #Strip comments, normalize whitespace, etc.
    query = re.sub(r"(--[^\n]*)", "", query)
    # Remove block comments
    query = " ".join(query.split())
    #Normalize whitespace
    return query
    
# Running the function
tables = extract_tables(query)