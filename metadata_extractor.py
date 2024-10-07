import re
import json
import codecs
import pandas as pd
import re
from sqlglot import parse_one, exp
import sqlparse
from collections import OrderedDict
from tables_schema import extract_tables

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}

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

    query = result.group(1).upper() + "'"
    query = query[1:]
    query = query.replace("#(LF)", " ")
    query = query.replace("#(TAB)", " ")
    if query.endswith("'"):
        query = query[:-1]

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
        

# Purpose: The JSON parser will extract the information required to build
# the table for Report generation - source_data
# The JSON contains a list of Workspaces. Each workspace can either contain
# a list of Reports or list of Dashboards.
# Two separate mapping tables are created to map the Report/Dashboard
# against the Workspace to which it belongs.

def replace_str( s ):
    s = s.replace('(', ' ')
    s = s.replace(')', ' ')
    s = s.replace(',', ' ')
    return s
    
def get_source_table( expression ):
    expression = expression.replace("from", "FROM")
    expression = expression.replace("left", "LEFT")
    expression = expression.replace("right", "RIGHT")
    expression = expression.replace("inner", "INNER")

    val = ""
    result = re.search('FROM (.*) LEFT', expression)
    if result is not None:
        val = result.group(1)
        arr = val.split(" ")
        i = 0
        for a in arr:
            if a == "LEFT":
                break
            i = i+1
        val = arr[i-1]
        
    if val != "":
        return val
        
    result = re.search('FROM (.*) INNER', expression)
    if result is not None:
        val = result.group(1)
        arr = val.split(" ")
        i = 0
        for a in arr:
            if a == "INNER":
                break
            i = i+1
        val = arr[i-1]

    if val != "":
        return val
        
    result = re.search('FROM (.*)#', expression)
    if result is not None:
        val = result.group(1)
        arr = val.split("#")
        arr = arr[0].split(" ")
        n = len(arr)
        val = arr[n-1]
        if val == "START":
            val = ""
        
    return val        

report_id_list = []
report_name_list = []
table_name_list = []
source_table_name_list = []
column_name_list = []
column_type_list = []
source_column_name_list = []
data_type_list = []
file_list = []
db_list = []
source_list = []
reports_with_database_source = []

FILE_LOC = "metadatafile.txt"
print()
print("Invoking JSON parser...")

file = open(FILE_LOC, 'r', encoding='utf-8')
lines = file.readlines()

workspaceReportMap = pd.DataFrame()
workspaceDashboardMap = pd.DataFrame()

for line in lines:

    line = line.replace("\\\\\\\\", "//")
    line = line.replace("\\\\\\\"", "'")
    line = line.replace("\\\"", "\"")
    line = line.replace("\\\\n", "")

    # Remove the first and last quotes
    line = line.replace("}]}\"", "}]}")
    line = line.replace("\"{\"workspaces\"", "{\"workspaces\"")


    decoded_data = codecs.decode(line.encode(), 'utf-8-sig')
    file_json = open('file.json', 'w', encoding="utf-8")
    file_json.write(decoded_data)
    file_json.close()
    json_object = json.loads(decoded_data)
    workspaces = json_object['workspaces']
    for workspace in workspaces:
        workspaceId = workspace['id']
        workspaceName = workspace['name']

        reports = workspace['reports']
        if len(reports) == 0:
            df2 = pd.DataFrame(
                [["", "", workspaceId, workspaceName]],
                columns=['reportId', 'reportName', 'workspaceId', 'workspaceName'])
            workspaceReportMap = workspaceReportMap._append(df2)

        for report in reports:
            reportId = report['id']
            reportName = report['name']
            # Check if there is originalReportObjectId and use it
            if 'originalReportObjectId' in report:
                originalReportObjectId = report['originalReportObjectId']
                reportId = originalReportObjectId
            df2 = pd.DataFrame(
                [[reportId, reportName, workspaceId, workspaceName]],
                columns=['reportId', 'reportName', 'workspaceId', 'workspaceName'])
            workspaceReportMap = workspaceReportMap._append(df2)

        dashboards = workspace['dashboards']
        if len(dashboards) == 0:
            df2 = pd.DataFrame(
                [["", "", workspaceId, workspaceName]],
                columns=['dashboardId', 'dashboardName', 'workspaceId', 'workspaceName'])
            workspaceDashboardMap = workspaceDashboardMap._append(df2)

        for dashboard in dashboards:
            dashboardId = dashboard['id']
            dashboardName = dashboard['displayName']
            df2 = pd.DataFrame(
                [[dashboardId, dashboardName, workspaceId, workspaceName]],
                columns=['dashboardId', 'dashboardName', 'workspaceId', 'workspaceName'])
            workspaceDashboardMap = workspaceDashboardMap._append(df2)

    for workspace in workspaces:
        totalReports = 0
        currentReportIDs = []
        currentReportNames = []
        reports = workspace['reports']
        reportIDMap = {}
        dataSetIDMap = {}
        for report in reports:
            currentReportIDs.append(report['id'])
            currentReportNames.append(report['name'])
            reportIDMap[report['name']] = report['id']
            if 'datasetId' in report:
                dataSetIDMap[report['datasetId']] = report['id']
            totalReports = totalReports + 1

        if (totalReports > 0):
            datasets = workspace['datasets']
            index = 0
            for dataset in datasets:
                found = True
                reportName = dataset['name']
                dataSetId = dataset['id']
                if reportName in reportIDMap:
                    rep_id = reportIDMap[reportName]
                elif dataSetId in dataSetIDMap:
                    rep_id = dataSetIDMap[dataSetId]
                else:
                    found = False

                if found:
                    tables = dataset["tables"]
                    #print(dataset)
                    for table in tables:
                        tableName = table["name"]
                        columns = table['columns']
                        sourceType = ""
                        # Get file name
                        file = ""
                        datasource_value = ""
                        db_connection = ""
                        
                        columnNames = []
                        columnNameMap = {}
                        
                        # Get columns
                        for col in columns:
                            if (col["columnType"] == "Data" or col["columnType"] == "CalculatedTableColumn" 
                                    or col["columnType"] == "Calculated"):
                                colName = col['name']
                                column_name_list.append(colName)
                                column_type_list.append(col["columnType"])
                                columnNames.append(colName)
                                dataType = col['dataType']
                                #print(id+":"+reportName+":"+tableName+":"+colName+":"+dataType)
                                report_id_list.append(rep_id)
                                report_name_list.append(reportName)
                                data_type_list.append(dataType)


                        if ("source" in table):
                            expression = table['source'][0]['expression']

                            if "Database" not in expression:                                        
                                # Get columns
                                for col in columnNames:
                                    table_name_list.append(tableName)
                                
                                
                            if "File.Contents" in expression:
                                # print(expression)
                                file = re.findall(r'Excel.Workbook\(File.Contents\(\'(.*?)\'\),', expression)
                                if (len(file) > 0):
                                    file = file[0]
                                    sourceType = "Excel_File"
                                else:
                                    file = re.findall(r'Json.Document\(File.Contents\(\'(.*?)\'\)\)', expression)
                                    if (len(file) > 0):
                                        file = file[0]
                                        sourceType = "JSON_File"

                                if (len(file) == 0):
                                    file = re.findall(r'Csv.Document\(File.Contents\(\'(.*?)\'\),', expression)
                                    if (len(file) > 0):
                                        file = file[0]
                                        sourceType = "CSV_File"
                                    else:
                                        file = ""

                                file = file.replace("://", ":\\\\")
                                file = file.replace("//", "\\")
                                datasource_value = file
                                # print(file)
                            elif "CALENDAR" in expression:
                                datasource_value = expression
                                sourceType = "Calendar"
                            elif "Folder.Files" in expression:
                                result = re.search('Folder.Files\(\'(.*)\',', expression)                                
                                part_1 = result.group(1)
                                if "'" in part_1:
                                    part_1 = part_1.split("'")[0]    
                                datasource_value = part_1
                                sourceType = "Folder.Files" 
                            elif "SharePoint.Files" in expression:
                                result = re.search('SharePoint.Files\(\'(.*)\',', expression)                                
                                part_1 = result.group(1)
                                if "'" in part_1:
                                    part_1 = part_1.split("'")[0]    
                                datasource_value = part_1
                                sourceType = "SharePoint.Files"   
                            elif "Excel.Workbook" in expression:
                                result = re.search('Web.Contents\(\'(.*)\',', expression)                                
                                part_1 = result.group(1)
                                if "'" in part_1:
                                    part_1 = part_1.split("'")[0]    
                                datasource_value = part_1
                                sourceType = "Excel.Workbook"    
                            elif "PowerBI.Dataflows" in expression:
                                result = re.search('dataflowId=\'(.*)\'', expression)                                
                                part_1 = result.group(1)
                                if "'" in part_1:
                                    part_1 = part_1.split("'")[0]    
                                datasource_value = "dataflowId=" + part_1
                                sourceType = "PowerBI.Dataflows"                                    
                            elif "Snowflake.Databases" in expression:                           
                                result = re.search('Snowflake.Databases\(\'(.*)\',', expression)
                                if(result is not None):
                                    part_1 = result.group(1)
                                    if "'" in part_1:
                                        part_1 = part_1.split("'")[0]                                
                                    result = re.search('\',\'(.*)\'\)', expression)
                                    if result:
                                        part_2 = result.group(1)
                                    else:
                                        result = re.search('\', \'(.*)\',', expression)                                
                                        part_2 = result.group(1)       
                                    if "'" in part_2:
                                        part_2 = part_2.split("'")[0]                                             
                                    result = re.search('Name=\'(.*)\'', expression)
                                    part_3 = result.group(1)
                                    if "'" in part_3:
                                        part_3 = part_3.split("'")[0]                                         
                                    db_connection = f"Server={part_1};Database={part_2};Schema={part_3}"   
                                    sourceType = "Database"   
                                else :
                                    db_connection = ""   
                                    sourceType = "Database" 
                            elif "Database" in expression:
                                sourceType = "Database"
                                # print(expression)
                                result = re.search('Database\(\'(.*)\'\, \'', expression)
                                db_server = result.group(1)

                                result = re.search('\', \'(.*)\'\)', expression) 
                                db_name = ""
                                if(result is not None):
                                    db_name = result.group(1)
                                else:
                                    result = re.search('\', \'(.*)\',', expression) 
                                    db_name = result.group(1)                                       
                                    
                                if "'" in db_name:
                                    db_name = db_name.split("'")[0]                                                                            
                                    
                                schema_name = ""
                                result = re.search('Item=\'(.*)\'\]', expression)
                                if(result is not None):
                                    schema_name = result.group(1)
                                else:
                                    result = re.search('Schema=\'(.*)\',', expression)
                                    if(result is not None):
                                        schema_name = result.group(1) 
                                    
                                if db_server.startswith("."):
                                    db_server = ""
                                else:
                                    if "'," in db_server:
                                        db_server = db_server.split("',")[0]

                                if not db_server:
                                    db_connection = "ODBC"
                                else:
                                    db_connection = f"Server={db_server};Database={db_name};Schema={schema_name}"
                                    if "'" in db_connection:
                                        db_connection = db_connection.split("'")[0]                                      
                                reports_with_database_source.append({
                                    "reportId": rep_id,      # Assuming 'report_id' is available
                                    "reportName": reportName   # Assuming 'report_name' is available
                                        })  
                                for report in reports_with_database_source:
                                    print(f"Report ID: {report['reportId']}, Report Name: {report['reportName']}")  
                                print(f"Total number of reports with database source: {len(reports_with_database_source)}")

                            else:
                                sourceType = "Report metadata"
                                datasource_value = expression

                                
                            if (sourceType == "Database") and ("Query=" in expression):
                            
                                try :
                                    print("report ids are---------",rep_id)
                                    result = re.search('Query=(.*)\'', expression)
                                    query = result.group(1).upper() + "'"
                                    clean_sql_query(query)

                                    # Get Table Names
                                    source_table_names = extract_tables(query)

                                    columns = extract_columns(query)
                                    all_columns = {}
                                    for column in columns:
                                        input_columns, output_column = split_column_and_alias(column)
                                        all_columns[output_column] = input_columns
                                    for meta_col in columnNames:
                                        col_val = meta_col
                                        if meta_col in all_columns:
                                            value = all_columns[meta_col]
                                            if value:
                                                col_val = str(value) 
                                        source_column_name_list.append(col_val)
                                        if source_table_names:
                                            source_table_name_list.append(source_table_names)
                                        else:
                                            source_table_name_list.append(tableName)
                                        table_name_list.append(tableName)
                                except Exception as e:
                                    print(f"Error processing report {rep_id}: {str(e)}")
                                    for col in columnNames:
                                        source_column_name_list.append("") 
                                        source_table_name_list.append("")  
                                        table_name_list.append(tableName)
                            else:                                
                                if sourceType == "Database":
                                    for col in columnNames:
                                        table_name_list.append(tableName)
                                            
                                for col in columnNames:
                                    source_column_name_list.append(col) 
                                    source_table_name_list.append(tableName)
                                    

                        if sourceType == "Database" and datasource_value == "":
                            datasource_value = expression
                            
                        # Get columns
                        for col in columnNames:
                            file_list.append(datasource_value)
                            db_list.append(db_connection)
                            source_list.append(sourceType)                                                                                                   
                            
                    index = index + 1

dict = {
    'ReportId': report_id_list,
    'ReportName': report_name_list,
    'PBI TableName': table_name_list,
    'PBI ColumnName': column_name_list,
    'Source TableName': source_table_name_list,
    'Source ColumnName': source_column_name_list,
    'DataType': data_type_list,
    'ColumnType': column_type_list,
    'Source': source_list,
    'DB_Connection': db_list,
    'Datasource': file_list
}

# Writing in excel
with pd.ExcelWriter(f"workspace_report_map.xlsx") as writer:
    workspaceReportMap.to_excel(writer, sheet_name='mapping', index=False)

# Writing in excel
with pd.ExcelWriter(f"workspace_dashboard.xlsx") as writer:
    workspaceDashboardMap.to_excel(writer, sheet_name='mapping', index=False)


df = pd.DataFrame(data=dict)
with pd.ExcelWriter(f"source_data_latest.xlsx") as writer:
    df.to_excel(writer, sheet_name='source_data', index=False)

df = pd.DataFrame(reports_with_database_source)    
with pd.ExcelWriter(f"DB_Reports.xlsx") as writer:
    df.to_excel(writer, sheet_name='reports_db_data', index=False)
    print("Data Saved!")