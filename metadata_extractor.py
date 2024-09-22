import re
import json
import codecs
import pandas as pd
import re
from sqlglot import parse_one, exp


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
                                sourceType = "Unknown"
                                datasource_value = expression

                                
                            if (sourceType == "Database") and ("Query=" in expression):
                            
                                try :
                                    print("report ids are---------",rep_id)
                                    result = re.search('Query=(.*)\'', expression)
                                    query = result.group(1).upper() + "'"
                                    query = query[1:]
                                    query = query.replace("#(LF)", " ")
                                    query = query.replace("#(TAB)", " ")
                                    if query.endswith("'"):
                                        query = query[:-1]
                                    
                                    if "WHERE" in query:
                                        result = re.search('(.*) WHERE', query)
                                        query = result.group(1)

                                    if ('[' in query) and (']' in query):
                                        for col in columnNames:
                                            source_column_name_list.append("") 
                                            source_table_name_list.append("")
                                            table_name_list.append(tableName)
                                    else:
                                        table_names = []
                                        table_aliases = []
                                        table_name_alias_map = {}
                                        all_table_alias_names = []
                                        all_column_names = []
                                        column_names_with_alias = []
                                        column_expression_map = {}
                                        alias_column_table_alias_mapping = {}
                                        column_name_to_alias_name_mapping = {}
                                        column_name_to_source_table_mapping = {}
                                        table_schemas = {}

                                        for table in parse_one(query).find_all(exp.Table):
                                            table_names.append(table.name)

                                        # find all tables (x, y, z)
                                        for table in parse_one(query).find_all(exp.Table): #Condition that checks is there even an alias for a table or not
                                            if table.alias:    
                                                table_aliases.append(table.alias)
                                            else:
                                                table_aliases.append(table.name)    

                                        num_of_entries = len(table_names)
                                        for i in range (0, num_of_entries):
                                            table_name_alias_map[table_aliases[i]] = table_names[i]
                                            if table_names[i] not in all_table_alias_names:
                                                all_table_alias_names.append(table_aliases[i])

                                        all_table_alias_names = list(dict.fromkeys(all_table_alias_names))
                                            
                                        alias_column_names = [i.alias for i in parse_one(query).expressions]

                                        aliased_and_non_aliased_column_names = [i.alias if i.alias else i.sql() for i in parse_one(query).expressions]

                                        aliase_columns_with_expressions = [i.sql() for i in parse_one(query).expressions]

                                        num_of_entries = len(alias_column_names)
                                        for i in range (num_of_entries): #Removed '0'
                                            alias_name = alias_column_names[i]
                                            alias_or_non_alias_name = aliased_and_non_aliased_column_names[i]
                                            if alias_name:    
                                                expr = aliase_columns_with_expressions[i]
                                                column_expression_map[alias_name] = expr
                                                found = False
                                                for tbl in all_table_alias_names:
                                                    if tbl+"." in expr:
                                                        alias_column_table_alias_mapping[alias_name] = tbl
                                                        found = True
                                                        break
                                                        
                                                if not found:
                                                    alias_column_table_alias_mapping[alias_name] = ""
                                                    expr = replace_str(expr)
                                                    arr = expr.split(" ")
                                                    for a in arr:
                                                        if "." in a:
                                                            arr2 = a.split(".")
                                                            alias_column_table_alias_mapping[alias_name] = arr2[0]
                                                            found = True
                                                            break            
                                                column_names_with_alias.append(alias_name)
                                                all_column_names.append(alias_name)
                                            else:
                                                name = alias_or_non_alias_name
                                                table_alias = ""
                                                if "." in alias_or_non_alias_name:
                                                    arr = alias_or_non_alias_name.split(".")
                                                    name = arr[1]
                                                    table_alias = arr[0]
                                                else:
                                                    table_alias = table_names[i] #Fallback to table name if alias is not present    
                                                alias_column_table_alias_mapping[name] = table_alias
                                                all_column_names.append(name)

                                        num_of_entries = len(all_table_alias_names)

                                        for column in all_column_names:
                                            if column in column_names_with_alias:
                                                result = ""
                                                expr = column_expression_map[column]
                                                expr = replace_str(expr)
                                                arr = expr.split(" ")
                                                for a in arr:
                                                    if "." in a:
                                                        arr2 = a.split(".")
                                                        if arr2[0] == alias_column_table_alias_mapping[column]:
                                                            result = arr2[1]
                                                            break
                                                column_name_to_alias_name_mapping[column] = result
                                                
                                                outval = ""
                                                for i in range (0, num_of_entries):
                                                    val1 = all_table_alias_names[i] + "." + result + " "
                                                    val2 = all_table_alias_names[i] + "." + result + ","
                                                    if val1.upper() in query.upper() or val2.upper() in query.upper():
                                                        outval = table_name_alias_map[all_table_alias_names[i]] 
                                                        break
                                                        
                                                column_name_to_source_table_mapping[column] = outval
                                            else:
                                                column_name_to_alias_name_mapping[column] = column      

                                                outval = ""
                                                for i in range (0, num_of_entries):
                                                    val1 = all_table_alias_names[i] + "." + column + " "
                                                    val2 = all_table_alias_names[i] + "." + column + ","
                                                    if val1.upper() in query.upper() or val2.upper() in query.upper():
                                                        outval = table_name_alias_map[all_table_alias_names[i]] 
                                                        break
                                                        
                                                column_name_to_source_table_mapping[column] = outval 
                                                
                                        arr = query.split(" ")
                                        for alias_name in table_name_alias_map:
                                            table_name = table_name_alias_map[alias_name]
                                            for a in arr:
                                                if "." in a:
                                                    arr2 = a.split(".")
                                                    if arr2[1] == table_name:
                                                        table_schemas[table_name] = arr2[0]
                                                        break
                                        
                                        for colName in columnNames:
                                            if colName in alias_column_table_alias_mapping: 
                                                table_name_list.append(alias_column_table_alias_mapping[colName])
                                            else:
                                                table_name_list.append(tableName)
                                                
                                            if colName in column_name_to_alias_name_mapping: 
                                                val = column_name_to_alias_name_mapping[colName]
                                                source_column_name_list.append(val)
                                            else:
                                                source_column_name_list.append("")
                                            
                                            if colName in column_name_to_source_table_mapping: 
                                                table_name_source = column_name_to_source_table_mapping[colName]
                                                result = table_name_source
                                                if table_name_source in table_schemas:
                                                    result = result + ":" + table_schemas[table_name_source] 

                                                if result:
                                                    source_table_name_list.append(result)
                                                else:
                                                    source_table_name_list.append("")
                                            else:
                                                source_table_name_list.append("")      

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
with pd.ExcelWriter(f"source_data.xlsx") as writer:
    df.to_excel(writer, sheet_name='source_data', index=False)

df = pd.DataFrame(reports_with_database_source)    
with pd.ExcelWriter(f"DB_Reports.xlsx") as writer:
    df.to_excel(writer, sheet_name='reports_db_data', index=False)
    print("Data Saved!")