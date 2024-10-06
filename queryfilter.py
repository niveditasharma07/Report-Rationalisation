import re
import sqlparse

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}
query = """SELECT   
	A.[ORGANIZATION DIMENSION SURROGATE KEY]       ,
	A.[ORGANIZATION DIMENSION VERSION NUMBER]       ,
	A.[ORGANIZATION LEAD DIMENSION SURROGATE KEY]       ,
	A.[ORGANIZATION LEAD DIMENSION VERSION NUMBER]       ,
	A.[PACE PERFORMANCE TARGET DIMENSION SURROGATE KEY]       ,
	A.[PACE PERFORMANCE TARGET VERSION NUMBER]       ,
	A.[PROCESS DATE]       ,
	CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END AS [TENURE DATA]          ,
	DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + 1, -1)) AS MONTH          --,
	DATEDIFF(MONTH, [TENURE DATA], DATEADD (DD, +14, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()) +1, 0))) AS EOQ_MONTH    ,
	DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END, CASE WHEN GETDATE()>DATEFROMPARTS(YEAR(GETDATE()),10,16) THEN (DATEADD (DD, -1, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()) + 1, 0)))    ELSE DATEADD (DD, +14, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()-15) + 1, 0))    END) AS EOQ_MONTH    --,
	(DATEADD (DD, -1, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()) + 1, 0))) AS CURRENT_QTR    /*,
	CASE WHEN GETDATE()>DATEFROMPARTS(YEAR(GETDATE()),10,16) THEN (DATEADD (DD, -1, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()) + 1, 0)))    ELSE DATEADD (DD, +14, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()-15) + 1, 0))    END AS QUARTER_END*/    --,
	DATEADD (DD, +14, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()), 0)) AS PREVIOUS_QTR          ,
	DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END, DATEADD(YY, DATEDIFF(YY, 0, GETDATE()) + 1, -1)) AS EOY_MONTH          ,
	DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END, [PROCESS DATE]) -           (DATEPART(DD,CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS74049','TS71560','TS77919','TS74642') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END)*1.0-1.0)/DAY(EOMONTH(CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS74049','TS71560','TS77919','TS74642') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END)) +           (DATEPART(DD,[PROCESS DATE])*1.0)/DAY(EOMONTH([PROCESS DATE])) AS TENURE_MONTHS         ,
	DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END,DATEADD(YY, DATEDIFF(YY, 0, GETDATE()) + 1, -1)) - 1 +   1-1.0*(DAY(CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END)-1)/ DAY(DATEADD(M, DATEDIFF(M,-1, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS74049','TS71560','TS77919','TS74642') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END), -1))   + 1.0*(DAY(DATEADD(YY, DATEDIFF(YY, 0, GETDATE()) + 1, -1))-1)/ DAY(DATEADD(M, DATEDIFF(M,-1, DATEADD(YY, DATEDIFF(YY, 0, GETDATE()) + 1, -1)), -1)) AS EOY_TENURE         ,
	DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END,CASE WHEN GETDATE()>DATEFROMPARTS(YEAR(GETDATE()),10,16) THEN (DATEADD (DD, -1, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()) + 1, 0)))    ELSE DATEADD (DD, +14, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()-15) + 1, 0))    END) - 1 +   1-1.0*(DAY(CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END)-1)/ DAY(DATEADD(M, DATEDIFF(M,-1, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS74049','TS71560','TS77919','TS74642') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END), -1))   + 1.0*(DAY(CASE WHEN GETDATE()>DATEFROMPARTS(YEAR(GETDATE()),10,16) THEN (DATEADD (DD, -1, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()) + 1, 0)))    ELSE DATEADD (DD, +14, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()-15) + 1, 0))    END)-1)/ DAY(DATEADD(M, DATEDIFF(M,-1, CASE WHEN GETDATE()>DATEFROMPARTS(YEAR(GETDATE()),10,16) THEN (DATEADD (DD, -1, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()) + 1, 0)))    ELSE DATEADD (DD, +14, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()-15) + 1, 0))    END), -1)) AS EOQ_TENURE        ,
	DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END,DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + 1, -1)) - 1 +   1-1.0*(DAY(CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END)-1)/ DAY(DATEADD(M, DATEDIFF(M,-1, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS74049','TS71560','TS77919','TS74642') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END), -1))   + 1.0*(DAY(DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + 1, -1))-1)/ DAY(DATEADD(M, DATEDIFF(M,-1, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + 1, -1)), -1)) AS EOM_TENURE       ,
	A.[PRODUCTION DATE]       ,
	A.[SNAPSHOT TYPE CODE]       ,
	A.[BUSINESS INTERRUPTION DAY VALUE]          ,
	B.[SOURCE SYSTEM KEY TEXT] AS TSID          ,
	CONCAT(B.[FIRST NAME],' ',B.[LAST NAME]) AS FP_NAME          ,
	B.[JOB TYPE CODE]          ,
	CONCAT((LEFT(SALE_HIER_DIM.[ORZN_ZONE_CDE],4)),'-',SALE_HIER_DIM.ORZN_DEPT_DSCR) AS TAG       ,
	SALE_HIER_DIM.ORZN_ZONE_CDE [MARKET]          ,
	C.[SOURCE SYSTEM KEY TEXT] AS LEADER_TSID          ,
	CONCAT(C.[FIRST NAME],' ',C.[LAST NAME]) AS LEADER_NAME       ,
	A.[LIFE AND HEALTH GRID CREDIT AMOUNT]       ,
	A.[LIFE AND HEALTH PRORATED GRID CREDIT AMOUNT] AS LH_PACE_TARGET       ,
	A.[GRID CREDIT AMOUNT]       ,
	[PRORATED GRID CREDIT AMOUNT] AS PACE_TARGET          ,
	D.[GRID CREDIT AMOUNT] AS EOY_TARGET          ,
	D.[GRID CREDIT RANGE AMOUNT] AS EOY_RANGE_AMT          ,
	E.[GRID CREDIT AMOUNT] AS EOQ_TARGET          ,
	E.[GRID CREDIT RANGE AMOUNT] AS EOQ_RANGE_AMT          ,
	F.[GRID CREDIT AMOUNT] AS EOM_TARGET          ,
	F.[GRID CREDIT RANGE AMOUNT] AS EOM_RANGE_AMT          ,
	CASE WHEN [PRIOR ADVISOR EXPERIENCE INDICATOR] = 'Y' OR B.[JOB TYPE CODE] = '002010' THEN ''          WHEN A.[GRID CREDIT AMOUNT] > A.[PRORATED GRID CREDIT AMOUNT] THEN 'ABOVE'          WHEN A.[GRID CREDIT AMOUNT] < A.[PRORATED GRID CREDIT AMOUNT] THEN 'BELOW'    ELSE ''          END AS ABOVE_BELOW          ,
	CASE WHEN [PRIOR ADVISOR EXPERIENCE INDICATOR] = 'Y' OR B.[JOB TYPE CODE] = '002010' THEN ''          WHEN A.[LIFE AND HEALTH GRID CREDIT AMOUNT] > A.[LIFE AND HEALTH PRORATED GRID CREDIT AMOUNT] THEN 'ABOVE'          WHEN A.[LIFE AND HEALTH GRID CREDIT AMOUNT] < A.[LIFE AND HEALTH PRORATED GRID CREDIT AMOUNT] THEN 'BELOW'    ELSE ''          END AS ABOVE_BELOW_LH      ,
	[PRIOR ADVISOR EXPERIENCE INDICATOR]        ,
	CASE WHEN A.[PROCESS DATE] = MAX_PROCESS_DATE.[MAX PROCESS DATE]THEN 'Y'          ELSE 'N'          END AS CURR_WK_IND   
FROM [ENTERPRISEDATAMART].[DM_01].[PACE PERFORMANCE SNAPSHOT FACT] A   INNER JOIN [ENTERPRISEDATAMART].[DM_01].[ORGANIZATION DIMENSION] B    ON (A.[ORGANIZATION DIMENSION SURROGATE KEY]=B.[ORGANIZATION DIMENSION SURROGATE KEY] AND        A.[PROCESS DATE]>=B.[EFFECTIVE BEGIN DATE] AND A.[PROCESS DATE]<=B.[EFFECTIVE END DATE])   INNER JOIN (SELECT MAX([PROCESS DATE]) [MAX PROCESS DATE] FROM [DM_01].[PACE PERFORMANCE SNAPSHOT FACT]) AS MAX_PROCESS_DATE ON 'A' = 'A'   INNER JOIN [DM_01].[DATE DIMENSION] DD ON (A.[PROCESS DATE]=DD.[CALENDAR DATE DATE])    LEFT JOIN [ENTERPRISEDATAMART].[DM_01].[SALE_HIER_DIM] SALE_HIER_DIM     ON ((     (SALE_HIER_DIM.EFF_BEG_DT<=A.[PROCESS DATE])         OR         (         (A.[PROCESS DATE]<(SELECT MIN(EFF_BEG_DT) FROM [ENTERPRISEDATAMART].[DM_01].[SALE_HIER_DIM] SUBQ WHERE B.[SOURCE SYSTEM KEY TEXT]=SUBQ.[SALE_HIER_ID] AND SUBQ.CURR_ROW_IND = 'Y'))         AND         SALE_HIER_DIM.EFF_BEG_DT=(SELECT MIN(EFF_BEG_DT) FROM [ENTERPRISEDATAMART].[DM_01].[SALE_HIER_DIM] SUBQ WHERE B.[SOURCE SYSTEM KEY TEXT]=SUBQ.[SALE_HIER_ID] AND SUBQ.CURR_ROW_IND = 'Y')         ))         AND          (A.[PROCESS DATE]<= DATEADD(DAY,-1,SALE_HIER_DIM.EFF_END_DT))         AND SALE_HIER_DIM.CURR_ROW_IND = 'Y'         AND B.[SOURCE SYSTEM KEY TEXT]=SALE_HIER_DIM.[SALE_HIER_ID])             INNER JOIN [ENTERPRISEDATAMART].[DM_01].[ORGANIZATION DIMENSION] C    ON (A.[ORGANIZATION LEAD DIMENSION SURROGATE KEY]=C.[ORGANIZATION DIMENSION SURROGATE KEY] AND     A.[PROCESS DATE]>=C.[EFFECTIVE BEGIN DATE] AND A.[PROCESS DATE]<=C.[EFFECTIVE END DATE])   LEFT JOIN [ENTERPRISEDATAMART].[DM_01].[PACE PERFORMANCE TARGET DIMENSION] D    ON (DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END, DATEADD(YY, DATEDIFF(YY, 0, GETDATE()) + 1, -1))=D.[CALENDAR YEAR MONTH NUMBER])   LEFT JOIN [ENTERPRISEDATAMART].[DM_01].[PACE PERFORMANCE TARGET DIMENSION] E    ON (DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END,CASE WHEN GETDATE()>DATEFROMPARTS(YEAR(GETDATE()),10,16) THEN (DATEADD (DD, -1, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()) + 1, 0)))    ELSE DATEADD (DD, +14, DATEADD(QQ, DATEDIFF(QQ, 0, GETDATE()-15) + 1, 0))    END)=E.[CALENDAR YEAR MONTH NUMBER])   LEFT JOIN [ENTERPRISEDATAMART].[DM_01].[PACE PERFORMANCE TARGET DIMENSION] F    ON (DATEDIFF(MONTH, CASE WHEN B.[SOURCE SYSTEM KEY TEXT] IN ('TS77919','TS78250','TS75194') THEN B.[ADJUSTED SERVICE DATE] ELSE A.[TENURE DATA] END, DATEADD(MM, DATEDIFF(MM, 0, GETDATE()) + 1, -1))=F.[CALENDAR YEAR MONTH NUMBER])']),    #'ADDED CUSTOM' = TABLE.ADDCOLUMN(SOURCE, 'ABOVE/BELOW', EACH IF [TENURE_MONTHS] <= 2.25 THEN '' ELSE [ABOVE_BELOW]),    #'REMOVED COLUMNS' = TABLE.REMOVECOLUMNS(#'ADDED CUSTOM',{'ABOVE_BELOW'}),    #'REORDERED COLUMNS' = TABLE.REORDERCOLUMNS(#'REMOVED COLUMNS',{'ORGANIZATION DIMENSION SURROGATE KEY', 'ORGANIZATION DIMENSION VERSION NUMBER', 'ORGANIZATION LEAD DIMENSION SURROGATE KEY', 'ORGANIZATION LEAD DIMENSION VERSION NUMBER', 'PACE PERFORMANCE TARGET DIMENSION SURROGATE KEY', 'PACE PERFORMANCE TARGET VERSION NUMBER', 'PROCESS DATE', 'TENURE DATA', 'MONTH', 'EOQ_MONTH', 'EOY_MONTH', 'TENURE_MONTHS', 'EOY_TENURE', 'EOQ_TENURE', 'EOM_TENURE', 'PRODUCTION DATE', 'SNAPSHOT TYPE CODE', 'BUSINESS INTERRUPTION DAY VALUE', 'TSID', 'FP_NAME', 'JOB TYPE CODE', 'TAG', 'MARKET', 'LEADER_TSID', 'LEADER_NAME', 'LIFE AND HEALTH GRID CREDIT AMOUNT', 'LH_PACE_TARGET', 'GRID CREDIT AMOUNT', 'PACE_TARGET', 'EOY_TARGET', 'EOY_RANGE_AMT', 'EOQ_TARGET', 'EOQ_RANGE_AMT', 'EOM_TARGET', 'EOM_RANGE_AMT', 'ABOVE/BELOW', 'ABOVE_BELOW_LH', 'PRIOR ADVISOR EXPERIENCE INDICATOR', 'CURR_WK_IND'}),    #'RENAMED COLUMNS' = TABLE.RENAMECOLUMNS(#'REORDERED COLUMNS',{{'ABOVE/BELOW', 'ABOVE_BELOW'}}),    #'FILTERED ROWS' = TABLE.SELECTROWS(#'RENAMED COLUMNS', EACH [TENURE_MONTHS] >= 0),    #'MERGED QUERIES' = TABLE.NESTEDJOIN(#'FILTERED ROWS', {'TSID'}, #'12_31 PACE EXPORT', {'TSID'}, '12_31 PACE EXPORT', JOINKIND.LEFTOUTER),    #'EXPANDED 12_31 PACE EXPORT' = TABLE.EXPANDTABLECOLUMN(#'MERGED QUERIES', '12_31 PACE EXPORT', {'EOY PACE TARGET', 'ABOVE/BELOW EOY'}, {'EOY PACE TARGET', 'ABOVE/BELOW EOY'})IN    #'EXPANDED 12_31 PACE EXPORT
"""

# Function to clean and simplify the SQL query
def clean_sql_query(query):
    query = re.sub(r"(\n|\t|\r)", " ", query)  # Remove new lines and tabs
    query = re.sub(r"\s+", " ", query)  # Replace multiple spaces with a single space
    return query.strip()

# Function to extract the main source table
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

# Function to extract columns from the SELECT part of the query and their source tables
def extract_columns_and_tables(query):
    # Set to hold extracted columns and tables
    columns = []
    tables = set()

    # Use SQLParse to break the query into tokens
    parsed = sqlparse.parse(query)[0]
    from_seen = False
    select_seen = False
    current_table = None
    alias_mapping = {}

    # Iterate over the parsed tokens
    for token in parsed.tokens:
        # Identify the SELECT clause and capture columns
        if token.ttype is sqlparse.tokens.DML and token.value.upper() == "SELECT":
            select_seen = True

        if select_seen and token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
            select_seen = False
            from_seen = True
            continue

        if select_seen:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    # Remove brackets and aliases
                    col = identifier.get_real_name()
                    alias = identifier.get_alias()
                    columns.append((col, alias if alias else col))
            elif isinstance(token, sqlparse.sql.Identifier):
                col = token.get_real_name()
                alias = token.get_alias()
                columns.append((col, alias if alias else col))

        # Capture tables from the FROM and JOIN clauses
        if from_seen:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    table_name = identifier.get_real_name()
                    table_alias = identifier.get_alias()
                    tables.add(table_name)
                    if table_alias:
                        alias_mapping[table_alias] = table_name
            elif isinstance(token, sqlparse.sql.Identifier):
                table_name = token.get_real_name()
                table_alias = token.get_alias()
                tables.add(table_name)
                if table_alias:
                    alias_mapping[table_alias] = table_name
            # Stop once we reach a WHERE or JOIN keyword
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ("WHERE", "JOIN", "LEFT JOIN", "RIGHT JOIN"):
                from_seen = False

    print(f"Source Tables: {tables}")
    print(f"Columns and Aliases: {columns}")
    return columns, tables, alias_mapping

# Function to map columns to their respective tables
def map_columns_to_tables(columns, alias_mapping):
    column_table_mapping = {}
    
    for col, alias in columns:
        # Check if the column has a table prefix (e.g., table.column)
        if "." in col:
            table_name, col_name = col.split(".", 1)
            # Map to the correct alias if exists
            if table_name in alias_mapping:
                table_name = alias_mapping[table_name]
            column_table_mapping[col_name] = table_name
        else:
            # Map columns without a prefix
            column_table_mapping[col] = alias_mapping.get(alias, "Unknown Table")

    print(f"Column-Table Mapping: {column_table_mapping}")
    return column_table_mapping

# Clean the input SQL query
cleaned_query = clean_sql_query(query)

# Extract main table
main_table = extract_main_table(cleaned_query)

# Extract columns and tables from the query
columns, tables, alias_mapping = extract_columns_and_tables(cleaned_query)

# Map columns to their respective tables
column_table_map = map_columns_to_tables(columns, alias_mapping)
