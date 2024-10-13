# import sqlparse
# import pandas as pd

# def parse_sql(query):
#     # Parse the query using sqlparse
#     parsed = sqlparse.parse(query)[0]
#     tokens = parsed.tokens

#     # Initialize sections for select, from, and where clauses
#     select_clause = []
#     from_clause = []
#     where_clause = []

#     # Helper function to clean up the tokens
#     def clean_tokens(token_list):
#         return [str(token).strip() for token in token_list if str(token).strip()]

#     # Iterate through tokens and classify them
#     current_section = None
#     for token in tokens:
#         if token.ttype is None and str(token).lower().startswith('select'):
#             current_section = 'select'
#             continue
#         elif token.ttype is None and str(token).lower().startswith('from'):
#             current_section = 'from'
#             continue
#         elif token.ttype is None and str(token).lower().startswith('where'):
#             current_section = 'where'
#             continue
        
#         # Add token to the corresponding section
#         if current_section == 'select':
#             select_clause.append(token)
#         elif current_section == 'from':
#             from_clause.append(token)
#         elif current_section == 'where':
#             where_clause.append(token)

#     # Clean up and format the results
#     select_clause = clean_tokens(select_clause)
#     from_clause = clean_tokens(from_clause)
#     where_clause = clean_tokens(where_clause)

#     # Structure the output as a dictionary
#     parsed_query = {
#         "select": ', '.join(select_clause),
#         "table": ', '.join(from_clause),
#         "where": ', '.join(where_clause)
#     }

#     return parsed_query

# def save_to_excel(query, parsed_data, file_name='parsed_queries.xlsx'):
#     # Create a DataFrame with the query and parsed data
#     data = {
#         "Query": [query],
#         "Select Clause": [parsed_data["select"]],
#         "Table Clause": [parsed_data["table"]],
#         "Where Clause": [parsed_data["where"]]
#     }
#     df = pd.DataFrame(data)

#     # Write the DataFrame to an Excel file
#     with pd.ExcelWriter(file_name, mode='a', if_sheet_exists='overlay') as writer:
#         df.to_excel(writer, sheet_name='ParsedQueries', index=False, header=not writer.sheets)

#     print(f"Data has been saved to {file_name}.")

# # Example complex SQL query
# sql_query = """
# SELECT 
#     PERS_DIM.[SOURCE SYSTEM KEY TEXT] AS THRIVENTID,        
#     PERS_DIM.[FIRST NAME] + ' ' + PERS_DIM.[LAST NAME] AS CLIENTNAME,        
#     APPT_FCT.[WORKER APPOINTMENT ASSOCIATION],        
#     APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY],        
#     APPT_FCT.[PERSON DIMENSION SURROGATE KEY],        
#     SUM(APPT_FCT.[APPOINTMENT OCCURS]) [APPT_OCCURS],       
#     MAX(APPT_FCT.[PERSON OCCURS]) [PERSON OCCURS],        
#     MBR_DIM.[MEMBERSHIP TYPE DESCRIPTION]
# FROM ENTERPRISEDATAMART.DM_01.[APPOINTMENT COMBINE FACT] APPT_FCT
# LEFT JOIN ENTERPRISEDATAMART.DM_01.[PERSON DIMENSION] PERS_DIM
#     ON APPT_FCT.[PERSON DIMENSION SURROGATE KEY] = PERS_DIM.[PERSON DIMENSION SURROGATE KEY]
# WHERE APPT_FCT.[PERSON DIMENSION SURROGATE KEY] <> 0
# AND APPT_FCT.[APPOINTMENT STATUS NAME] <> 'DECLINED'
# """

# # Parse the query
# parsed_query = parse_sql(sql_query)

# # Save to Excel
# save_to_excel(sql_query, parsed_query)






# import sqlparse

# def parse_sql(query):
#     # Parse the query using sqlparse
#     parsed = sqlparse.parse(query)[0]
#     tokens = parsed.tokens

#     # Initialize sections for select, from, and where clauses
#     select_clause = []
#     from_clause = []
#     where_clause = []

#     # Helper function to clean up the tokens
#     def clean_tokens(token_list):
#         return [str(token).strip() for token in token_list if str(token).strip()]

#     # Iterate through tokens and classify them
#     current_section = None
#     for token in tokens:
#         if token.ttype is None and str(token).lower().startswith('select'):
#             current_section = 'select'
#             continue
#         elif token.ttype is None and str(token).lower().startswith('from'):
#             current_section = 'from'
#             continue
#         elif token.ttype is None and str(token).lower().startswith('where'):
#             current_section = 'where'
#             continue
        
#         # Add token to the corresponding section
#         if current_section == 'select':
#             select_clause.append(token)
#         elif current_section == 'from':
#             from_clause.append(token)
#         elif current_section == 'where':
#             where_clause.append(token)

#     # Clean up and format the results
#     select_clause = clean_tokens(select_clause)
#     from_clause = clean_tokens(from_clause)
#     where_clause = clean_tokens(where_clause)

#     # Structure the output as a dictionary
#     parsed_query = {
#         "select": select_clause,
#         "table": from_clause,
#         "where": where_clause
#     }

#     return parsed_query

# # Example complex SQL query
# sql_query = """
# SELECT 
#     PERS_DIM.[SOURCE SYSTEM KEY TEXT] AS THRIVENTID,        
#     PERS_DIM.[FIRST NAME] + ' ' + PERS_DIM.[LAST NAME] AS CLIENTNAME,        
#     APPT_FCT.[WORKER APPOINTMENT ASSOCIATION],        
#     APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY],        
#     APPT_FCT.[PERSON DIMENSION SURROGATE KEY],        
#     SUM(APPT_FCT.[APPOINTMENT OCCURS]) [APPT_OCCURS],       
#     MAX(APPT_FCT.[PERSON OCCURS]) [PERSON OCCURS],        
#     MBR_DIM.[MEMBERSHIP TYPE DESCRIPTION]
# FROM ENTERPRISEDATAMART.DM_01.[APPOINTMENT COMBINE FACT] APPT_FCT
# LEFT JOIN ENTERPRISEDATAMART.DM_01.[PERSON DIMENSION] PERS_DIM
#     ON APPT_FCT.[PERSON DIMENSION SURROGATE KEY] = PERS_DIM.[PERSON DIMENSION SURROGATE KEY]
# WHERE APPT_FCT.[PERSON DIMENSION SURROGATE KEY] <> 0
# AND APPT_FCT.[APPOINTMENT STATUS NAME] <> 'DECLINED'
# """

# # Parse the query and print the results
# parsed_query = parse_sql(sql_query)
# print("Select Clause:", parsed_query['select'])
# print("Table Clause:", parsed_query['table'])
# print("Where Clause:", parsed_query['where'])





# import sqlparse
# from sqlparse.sql import IdentifierList, Identifier
# from sqlparse.tokens import Keyword, DML

# def parse_sql(query):
#     # Parse the query using sqlparse
#     parsed = sqlparse.parse(query)[0]
#     tokens = parsed.tokens

#     # Initialize sections for select, from, and where clauses
#     select_clause = []
#     from_clause = []
#     where_clause = []

#     # Helper function to extract identifiers from token
#     def extract_identifiers(token_list):
#         identifiers = []
#         for token in token_list:
#             if isinstance(token, IdentifierList):
#                 for identifier in token.get_identifiers():
#                     identifiers.append(str(identifier).strip())
#             elif isinstance(token, Identifier):
#                 identifiers.append(str(token).strip())
#             else:
#                 identifiers.append(str(token).strip())
#         return identifiers

#     # Iterate through tokens and classify them
#     current_section = None
#     for token in tokens:
#         # Identify the start of a section by checking token type
#         if token.ttype is DML and token.value.upper() == 'SELECT':
#             current_section = 'select'
#             continue
#         elif token.ttype is Keyword and token.value.upper() == 'FROM':
#             current_section = 'from'
#             continue
#         elif token.ttype is Keyword and token.value.upper() == 'WHERE':
#             current_section = 'where'
#             continue
        
#         # Add token to the corresponding section
#         if current_section == 'select':
#             select_clause.append(token)
#         elif current_section == 'from':
#             from_clause.append(token)
#         elif current_section == 'where':
#             where_clause.append(token)

#     # Extract identifiers for each clause
#     select_clause = extract_identifiers(select_clause)
#     from_clause = extract_identifiers(from_clause)
#     where_clause = extract_identifiers(where_clause)

#     # Structure the output as a dictionary
#     parsed_query = {
#         "select": select_clause,
#         "table": from_clause,
#         "where": where_clause
#     }

#     return parsed_query

# # Example complex SQL query
# sql_query = """
# SELECT 
#     PERS_DIM.[SOURCE SYSTEM KEY TEXT] AS THRIVENTID,        
#     PERS_DIM.[FIRST NAME] + ' ' + PERS_DIM.[LAST NAME] AS CLIENTNAME,        
#     APPT_FCT.[WORKER APPOINTMENT ASSOCIATION],        
#     APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY],        
#     APPT_FCT.[PERSON DIMENSION SURROGATE KEY],        
#     SUM(APPT_FCT.[APPOINTMENT OCCURS]) [APPT_OCCURS],       
#     MAX(APPT_FCT.[PERSON OCCURS]) [PERSON OCCURS],        
#     MBR_DIM.[MEMBERSHIP TYPE DESCRIPTION]
# FROM ENTERPRISEDATAMART.DM_01.[APPOINTMENT COMBINE FACT] APPT_FCT
# LEFT JOIN ENTERPRISEDATAMART.DM_01.[PERSON DIMENSION] PERS_DIM
#     ON APPT_FCT.[PERSON DIMENSION SURROGATE KEY] = PERS_DIM.[PERSON DIMENSION SURROGATE KEY]
# WHERE APPT_FCT.[PERSON DIMENSION SURROGATE KEY] <> 0
# AND APPT_FCT.[APPOINTMENT STATUS NAME] <> 'DECLINED'
# """

# # Parse the query and print the results
# parsed_query = parse_sql(sql_query)
# print("Select Clause:", parsed_query['select'])
# print("Table Clause:", parsed_query['table'])
# print("Where Clause:", parsed_query['where'])









import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML

def parse_sql(query):
    # Parse the query using sqlparse
    parsed = sqlparse.parse(query)[0]
    tokens = parsed.tokens

    # Initialize sections for select, from, and where clauses
    select_clause = []
    from_clause = []
    where_clause = []

    # Helper function to extract identifiers from token
    def extract_identifiers(token_list):
        identifiers = []
        for token in token_list:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    identifiers.append(str(identifier).strip())
            elif isinstance(token, Identifier):
                identifiers.append(str(token).strip())
            else:
                identifiers.append(str(token).strip())
        return identifiers

    # Iterate through tokens and classify them
    current_section = None
    for token in tokens:
        # Identify the start of a section by checking token type
        if token.ttype is DML and token.value.upper() == 'SELECT':
            current_section = 'select'
            continue
        elif token.ttype is Keyword and token.value.upper() == 'FROM':
            current_section = 'from'
            continue
        elif isinstance(token, Where) or (token.ttype is Keyword and token.value.upper() == 'WHERE'):
            current_section = 'where'
            where_clause.append(token)
            continue
        
        # Add token to the corresponding section
        if current_section == 'select':
            select_clause.append(token)
        elif current_section == 'from':
            from_clause.append(token)

    # Extract identifiers for each clause
    select_clause = extract_identifiers(select_clause)
    from_clause = extract_identifiers(from_clause)
    # For WHERE clause, handle the conditions together
    where_clause = [str(token).strip() for token in where_clause if str(token).strip()]

    # Structure the output as a dictionary
    parsed_query = {
        "select": select_clause,
        "table": from_clause,
        "where": where_clause
    }

    return parsed_query

# Example complex SQL query
sql_query = """
SELECT 
    PERS_DIM.[SOURCE SYSTEM KEY TEXT] AS THRIVENTID,        
    PERS_DIM.[FIRST NAME] + ' ' + PERS_DIM.[LAST NAME] AS CLIENTNAME,        
    APPT_FCT.[WORKER APPOINTMENT ASSOCIATION],        
    APPT_FCT.[APPOINTMENT DIMENSION SURROGATE KEY],        
    APPT_FCT.[PERSON DIMENSION SURROGATE KEY],        
    SUM(APPT_FCT.[APPOINTMENT OCCURS]) [APPT_OCCURS],       
    MAX(APPT_FCT.[PERSON OCCURS]) [PERSON OCCURS],        
    MBR_DIM.[MEMBERSHIP TYPE DESCRIPTION]
FROM ENTERPRISEDATAMART.DM_01.[APPOINTMENT COMBINE FACT] APPT_FCT
LEFT JOIN ENTERPRISEDATAMART.DM_01.[PERSON DIMENSION] PERS_DIM
    ON APPT_FCT.[PERSON DIMENSION SURROGATE KEY] = PERS_DIM.[PERSON DIMENSION SURROGATE KEY]
WHERE APPT_FCT.[PERSON DIMENSION SURROGATE KEY] <> 0
AND APPT_FCT.[APPOINTMENT STATUS NAME] <> 'DECLINED'
"""

# Parse the query and print the results
parsed_query = parse_sql(sql_query)
print("Select Clause:", parsed_query['select'])
print("Table Clause:", parsed_query['table'])
print("Where Clause:", parsed_query['where'])

