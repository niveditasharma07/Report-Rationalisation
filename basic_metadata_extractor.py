import re
import json
import codecs
import pandas as pd
import re
from collections import OrderedDict
from sqlmetadata_extractor import extractTables

KEYWORDS = {"SUM", "MIN", "MAX", "AVG", "CASE", "WHEN", "AND", "THEN", "END", "INTEGER", "FLOOR", "CURRENT", "DATE"}
LF="#(lf)"
TAB="#(tab)"
QUOTE="\u0027"

def get_db_connection( expression, is_snowflake):
    if is_snowflake:
        db_connection = ""   
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
    else :
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
    return db_connection

# Function to clean and simplify the SQL query
def clean_sql_query(query):

    query = result.group(1) + "'"
    query = query[1:]
    query = query.replace(LF, "\n")
    query = query.replace(TAB, "\t")
    query = query.replace(QUOTE, "'")
    query = query.upper()
    if query.endswith("'"):
        query = query[:-1]

    return query.strip()



# Purpose: The JSON parser will extract the information required to build
# the table for Report generation - source_data
# The JSON contains a list of Workspaces. Each workspace can either contain
# a list of Reports or list of Dashboards.
# Two separate mapping tables are created to map the Report/Dashboard
# against the Workspace to which it belongs.

report_id_list = []
report_name_list = []
workspace_id_list = []
workspace_name_list = []
table_name_list = []
source_table_name_list = []
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
                        sourceType = ""
                        # Get file name
                        file = ""
                        datasource_value = ""
                        db_connection = ""
                        
                        workspace_id_list.append(workspaceId)
                        workspace_name_list.append(workspaceName)
                        report_id_list.append(rep_id)
                        report_name_list.append(reportName)
                        table_name_list.append(tableName)

                        if ("source" in table):
                            expression = table['source'][0]['expression']

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
                            else:
                                sourceType = "Report metadata"
                                datasource_value = expression
                                
                            source_list.append(sourceType) 
                            file_list.append(datasource_value)
                            db_list.append(db_connection)

                            source_table_names = ""
                            if "Query=" in expression:                            
                                # print("report ids are---------",rep_id)
                                result = re.search('Query=(.*)\'', expression)
                                query = result.group(1) + "'"     
                                query = clean_sql_query(query)

                                source_table_names = extractTables(query) 
                                total_tables = len(source_table_names)

                                if total_tables == 0:
                                    source_table_name_list.append(tableName)        
                                elif total_tables == 1:
                                    source_table_name_list.append(source_table_names[0])  
                                elif total_tables > 1:
                                    source_table_name_list.append(source_table_names[0])  
                                    for i in range(1,total_tables):
                                        workspace_id_list.append(workspaceId)
                                        workspace_name_list.append(workspaceName)
                                        report_id_list.append(reportId)
                                        report_name_list.append(reportName)
                                        table_name_list.append(tableName)
                                        source_list.append(sourceType) 
                                        file_list.append(datasource_value)
                                        db_list.append(db_connection)                                        
                                        source_table_name_list.append(source_table_names[i])

                            if source_table_names == "":
                                source_table_name_list.append(tableName)                            
                                
                                                        
                    index = index + 1

dict = {
    'WorkspaceId': workspace_id_list,
    'WorkspaceName': workspace_name_list,
    'ReportId': report_id_list,
    'ReportName': report_name_list,
    'PBI TableName': table_name_list,
    'Source TableName': source_table_name_list,
    'Source': source_list,
    'DB_Connection': db_list,
    'Datasource': file_list,
}

df = pd.DataFrame(data=dict)
def split_source_table_name(df):
    # Split the Source TableName into two parts
    split_columns = df['Source TableName'].str.extract(r'(?:(.*)\.)?(.*)$')
    df['Schema.Database'] = split_columns[0]  # Part before the last dot
    df['Table Name'] = split_columns[1]       # Part after the last dot
    return df
 
# Apply the splitting function to the DataFrame
df = split_source_table_name(df)



# Function to extract database name from DB_Connection
def extract_database_name(db_connection):
    match = re.search(r"Database=([^;]+);", db_connection)
    return match.group(1) if match else ""
 
# Update 'Source' column for rows where 'Source' is 'Database'
df['Source'] = df.apply(
    lambda row: extract_database_name(row['DB_Connection']) if row['Source'] == "Database" else row['Source'],
    axis=1
)
 
# Save the updated DataFrame to Excel
with pd.ExcelWriter(f"basic_metadata_extract.xlsx") as writer:
    df.to_excel(writer, sheet_name='source_data', index=False)


with pd.ExcelWriter(f"basic_metadata_extract.xlsx") as writer:
    df.to_excel(writer, sheet_name='source_data', index=False)
