import re
import sqlparse
 
def extract_tables(queries):
    tables = []
    for query in queries:
        cleaned_query = query.strip()
        # Use regex to extract FROM and JOIN clauses
        from_matches = re.findall(r"\bFROM\s+([a-zA-Z0-9_\[\]\.\s]+)", cleaned_query)
        join_matches = re.findall(r"\bJOIN\s+([a-zA-Z0-9_\[\]\.\s]+)", cleaned_query)
 
        # Process FROM clause with sqlparse
        if from_matches:
            from_clause = from_matches[0]
            tables.extend(parse_sql_with_sqlparse(from_clause))
 
        # Process JOIN clauses with sqlparse
        for join_clause in join_matches:
            tables.extend(parse_sql_with_sqlparse(join_clause))
 
    # Remove duplicates and clean table names
    cleaned_tables = list(set(filter_valid_tables(tables)))
    return cleaned_tables
 
def parse_sql_with_sqlparse(sql_fragment):
    """Use sqlparse to extract table names from a SQL fragment."""
    tables = []
    parsed = sqlparse.parse(sql_fragment)[0]  # Parse the SQL fragment
    for token in parsed.tokens:
        if isinstance(token, sqlparse.sql.IdentifierList):
            for identifier in token.get_identifiers():
                tables.append(clean_table_name(str(identifier)))
        elif isinstance(token, sqlparse.sql.Identifier):
            tables.append(clean_table_name(str(token)))
    return tables
 
def clean_table_name(table_name):
    # Remove alias (e.g., "AS A" or "B")
    table_name = re.sub(r"\s+(AS\s+[a-zA-Z0-9_]+|[a-zA-Z0-9_]+)$", "", table_name, flags=re.IGNORECASE)
    # Remove extra square brackets if present
    table_name = table_name.replace("[", "").replace("]", "")
    # Normalize spacing and remove any leading/trailing spaces
    return table_name.strip()
 
def filter_valid_tables(tables):
    """Filters out column-like entries and keeps only valid table names."""
    valid_tables = []
    for table in tables:
        # Exclude entries that look like column references (e.g., 'T1.COLUMN')
        # Exclude any tables that start with a single alias like 'T1.' or 'A.'
        if re.match(r"[a-zA-Z0-9_\.\s]+", table) and not re.search(r"^[A-Z][0-9]*\.[a-zA-Z_]+", table):
            valid_tables.append(table)
    return valid_tables
 
# Example usage
queries = [
    """
SELECT DISTINCT       [Department Identifier] RFO_CODE      ,[Department Name] RFO_NM      ,concat(trim([Department Identifier]),'-',[Department Name]) as NM   FROM [EnterpriseDataMart].[DM_01].[Organization Dimension]   where [Department Identifier] in ('0283', '0435','0115', '0190',                 '0361', '0384','0291', '0525','0001','0383','0716','0708')        and [Current Row Indicator] = 'Y'        and [Effective Begin Date] > '1/1/2019'        and [Effective End Date] = '12/31/9999'']),    #'Changed Type' = Table.TransformColumnTypes(Source,{{'RFO_CODE', Int64.Type}})in    #'Changed Type'
    """
]
 
extracted_tables = extract_tables(queries)
print(extracted_tables)
 
 