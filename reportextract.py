import pandas as pd
import sqlparse
import re

# Function to clean and simplify the SQL query
def clean_sql_query(query):
    query = re.sub(r"['\"]", "", query)  # Remove single and double quotes
    query = re.sub(r"(\n|\t|\r)", " ", query)  # Remove new lines and tabs
    query = re.sub(r"\s+", " ", query)  # Replace multiple spaces with a single space
    return query.strip()

# Function to extract tables and columns from the SELECT part of the query
def extract_columns_and_tables(query):
    parsed = sqlparse.parse(query)[0]

    # Set to hold extracted columns and tables
    columns = set()
    tables = set()

    # Patterns to identify source columns and tables
    table_pattern = re.compile(r"FROM\s+([\w.]+)", re.IGNORECASE)
    join_pattern = re.compile(r"JOIN\s+([\w.]+)", re.IGNORECASE)
    column_pattern = re.compile(r"SELECT\s+(.*?)\s+FROM", re.IGNORECASE)

    # Clean and split the query into components
    cleaned_query = clean_sql_query(query)

    # Extract tables from FROM and JOIN clauses
    tables.update(re.findall(table_pattern, cleaned_query))
    tables.update(re.findall(join_pattern, cleaned_query))

    # Extract columns from SELECT clause
    select_match = re.search(column_pattern, cleaned_query)
    if select_match:
        # Split the columns, handle aliases, and strip extra spaces
        columns_clause = select_match.group(1)
        for col in columns_clause.split(","):
            col = col.strip()
            if " AS " in col.upper():
                col = col.split(" AS ")[0].strip()
            columns.add(col)

    return columns, tables

# Function to identify the SQL query column if not explicitly provided
def find_sql_query_column(df):
    # List of common SQL start keywords
    sql_keywords = ["SELECT", "WITH", "INSERT", "UPDATE", "DELETE"]

    # Check each column in the DataFrame
    for column in df.columns:
        for cell in df[column]:
            if isinstance(cell, str) and cell.strip().split(" ")[0].upper() in sql_keywords:
                return column
    return None

# Function to read SQL queries from an Excel file and extract columns and tables
def read_excel_and_parse_queries(input_file):
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Identify the SQL query column dynamically
    query_column = find_sql_query_column(df)
    
    if query_column is None:
        raise ValueError("No valid SQL queries found in the Excel file.")
    
    # Create a list to store results
    results = []

    # Loop through each row in the identified SQL query column
    for index, row in df.iterrows():
        query = row[query_column]
        if isinstance(query, str):
            # Extract columns and tables from the query
            extracted_columns, extracted_tables = extract_columns_and_tables(query)
            # Store the results in a structured format
            results.append({
                'Query': query,
                'Source Columns': ", ".join(extracted_columns),
                'Source Tables': ", ".join(extracted_tables)
            })

    return results

# Function to save the extracted information into a new Excel file
def save_to_excel(parsed_data, output_file):
    # Create a pandas DataFrame from the parsed data
    df_output = pd.DataFrame(parsed_data)
    # Write the DataFrame to an Excel file
    df_output.to_excel(output_file, index=False)

# Main execution flow
if __name__ == "__main__":
    # Path to the input Excel file
    input_file = 'queries.xlsx'
    
    # Path to the output Excel file
    output_file = 'parsed_sql_output.xlsx'

    # Read, parse, and extract SQL information
    parsed_data = read_excel_and_parse_queries(input_file)

    # Save the extracted data to a new Excel file
    save_to_excel(parsed_data, output_file)

    print(f"Parsing completed! Extracted data has been saved to '{output_file}'.")
