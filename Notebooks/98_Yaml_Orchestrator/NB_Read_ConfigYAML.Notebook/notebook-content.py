# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

%run CommonFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import yaml
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

#Parameters definition

#Name of the model
model = "framework_crossworkspace"
modelReset = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaceId, bronze_lakehouse_id, _, _ = GetFabricIds()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve the variable library
vl = notebookutils.variableLibrary.getLibrary("vl-modernBI-fabricFramework-01")

try:
    eventhouseName = vl.eventhouseName
    databaseName = vl.eventhouseDatabase
    eventhouseWorkspaceId = vl.eventhouseWorkspaceId
except:
    eventhouseName = ""
    databaseName = ""
    eventhouseWorkspaceId = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (eventhouseName != ""):
    SetSessionLogURI(eventhouseName, databaseName, eventhouseWorkspaceId)
    spark.conf.set(f"UseYaml","true")
    log_eventhouse_id = spark.conf.get("fabric.log_eventhouse_id", None)
    log_kqldatabase_id = spark.conf.get("fabric.log_kqldatabase_id", None)
    kustoURI = spark.conf.get("kustoUri", None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetLastModelExecution(model: str) -> dict:

    try:
        kustoUri = spark.conf.get(f"kustoUri")
        database = spark.conf.get(f"kustoDatabase")
        
        query = f"LastFailedBatchProcesses('{model}')"
        df = spark.read.format("com.microsoft.kusto.spark.synapse.datasource") \
            .option("kustoCluster", kustoUri) \
            .option("kustoDatabase", database) \
            .option("kustoQuery", query) \
            .option("accessToken", notebookutils.credentials.getToken('kusto'))\
            .load()
        
        
        # Convert to dictionary with tableName as keys and startDate as values
        # return {row["tableName"]: row["startDate"].replace(microsecond=0) for row in filtered_df.collect()}
        return df
    except:
        print("no kusto URI or database defined")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batchExecution=str(uuid.uuid4())
status_map = {}

if not modelReset:
    df_LastExecution = GetLastModelExecution(model)
    # Convert dataframe to a dict {processInternalId -> status}
    status_map = {row["processInternalId"]: row["status"] for row in df_LastExecution.collect()}

    # Check if dataframe has rows
    if df_LastExecution.head(1):
        # Get first row's batchExecution
        batchExecution = df_LastExecution.first()["batchExecution"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load YAML file
yaml_file_path = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files/config/{model}.yml"

# Read YAML file from Lakehouse as text
yaml_content = spark.read.text(yaml_file_path).collect()

# Convert rows to a single string
yaml_string = "\n".join(row.value for row in yaml_content)

# Parse YAML
config = yaml.safe_load(yaml_string)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sections = ["raw", "silver", "gold"]
dag_activities = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetStartDates(model: str, scope: str) -> dict:

    try:
        kustoUri = spark.conf.get(f"kustoUri")
        database = spark.conf.get(f"kustoDatabase")
        
        df = spark.read.format("com.microsoft.kusto.spark.synapse.datasource") \
            .option("kustoCluster", kustoUri) \
            .option("kustoDatabase", database) \
            .option("kustoQuery", "lastDatesConfig") \
            .option("accessToken", notebookutils.credentials.getToken('kusto'))\
            .load()
        
        # Apply filters
        filtered_df = df.filter(
            (col("model") == model) &
            (col("scope") == scope)
        )
        
        # Convert to dictionary with tableName as keys and startDate as values
        return {row["tableName"]: row["startDate"].replace(microsecond=0) for row in filtered_df.collect()}

    except:
        print("no kusto URI or database defined")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetCopyDataConfig(entry):
    # from datetime import datetime
    start_date = Dtime.datetime.now()
    

    # Function to generate SQL command
    def generate_source_read_command(entry):
        source_system_type = entry.get("sourceSystemType", "").lower()
        extract_type = entry.get("extractType", "").lower()
        source_object_name = entry.get("sourceObjectName", "")
        source_select_columns = entry.get("sourceSelectColumns",None)
        delta_date_column = entry.get("deltaDateColumn",None)
        if extract_type == "delta": 
            delta_start_date = extractionDates.get(source_object_name,'1900-01-01') #get_start_date(model,source_object_name,'Extraction')
        else:
            delta_start_date = None
        delta_end_date = entry.get("deltaEndDate",None)
        delta_filter_condition = entry.get("deltaFilterCondition",None)
        flag_block = entry.get("flagBlock", 0)
        
        end_date = Dtime.datetime.now()

        if source_system_type == "xlsx" and flag_block == 0:
            return "Not Applicable"

        # SQL Extract Type: FULL
        if source_system_type == "sql" and extract_type == "full":
            if source_select_columns:
                return f"SELECT {source_select_columns} FROM {source_object_name}"
            else:
                return f"SELECT * FROM {source_object_name}"

        # SQL Extract Type: DELTA
        if source_system_type == "sql" and extract_type == "delta":
            # Construct WHERE clause
            where_clauses = []
            if delta_end_date:
                end_date = delta_end_date

            if delta_filter_condition:
                where_clauses.append(delta_filter_condition)

            if "deltaDateColumn" in entry:
                where_clauses.append(f"{delta_date_column} >= '{delta_start_date}'")
                where_clauses.append(f"{delta_date_column} < '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'")

            # condition = f"{delta_date_column} >= '{delta_start_date}' AND {delta_date_column} < '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'"
            where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            if source_select_columns:
                return f"SELECT {source_select_columns} FROM {source_object_name} {where_clause}"
            else:
                return f"SELECT * FROM {source_object_name}  {where_clause}"

        return None

    # Convert to JSON structure
    # result_list = []
    destination_object_pattern = f"{entry['destinationObjectPattern']}_{start_date.strftime('%Y%m%d_%H%M')}.{entry['destinationObjectType']}"
    destination_directory_pattern = f"{entry['destinationDirectoryPattern']}{start_date.strftime('%Y/%m/%d/%H/%M')}/"

    result_entry = {
        "model": f'{model}',
        "sourceSystemName": entry["sourceSystemName"],
        "sourceSystemType": entry["sourceSystemType"],
        "sourceLocationName": entry["sourceLocationName"],
        "sourceObjectName": entry["sourceObjectName"],
        "sourceSelectColumns": entry.get("sourceSelectColumns",None),
        "sourceKeyColumns": entry.get("sourceKeyColumns",None),
        "destinationSystemName": entry["destinationSystemName"],
        "destinationSystemType": entry["destinationSystemType"],
        "destinationObjectPattern": destination_object_pattern,
        "destinationDirectoryPattern": destination_directory_pattern,
        "destinationObjectType": entry["destinationObjectType"],
        "extractType": entry["extractType"],
        "deltaStartDate": entry.get("deltaStartDate",None),
        "deltaDateColumn": entry.get("deltaDateColumn",None),
        "flagBlock": entry.get("flagBlock",None),
        "flagActive": entry.get("flagActive"),
        "sourceReadCommand": generate_source_read_command(entry),
        "startDate": start_date.strftime('%Y-%m-%d %H:%M:%S'),
    }
    # result_list.append(result_entry)

    # Convert to JSON
    json_output = json.dumps(result_entry, indent=4)
    # print(json_output)

    return json_output


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def BuildGroupDependencyMap(config):

    group_dependency_map = {}

    for section, entries in config.items():
        if isinstance(entries, dict):  # Ensure entries is a dictionary before calling .items()
            for entry_name, entry_data in entries.items():
                if isinstance(entry_data, dict):  # Ensure entry_data is also a dictionary
                    group = entry_data.get("group")
                    if group:
                        if group not in group_dependency_map:
                            group_dependency_map[group] = []
                        group_dependency_map[group].append(f"{section}_{entry_name}")

    return group_dependency_map

# Build group dependency map once
group_dependency_map = BuildGroupDependencyMap(config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ProcessEntryWithWrapper(name, entry, section, config, sections, status_map):
    if entry.get("flagActive", 0) == 1:

        """Process each entry and generate a DAG activity."""
        dependencies = set(dep.replace(".", "_") for dep in entry.get("dependsOn", []))

        if "dependsOnGroup" in entry:
            for group in entry["dependsOnGroup"]:
                dependencies.update(group_dependency_map.get(group, set()))

        # Filter dependencies to only include active ones
        valid_dependencies = []
        for dep in dependencies:
            # Parse dependency to get section and name
            if "_" in dep:
                dep_section, dep_name = dep.split("_", 1)
                if dep_section in sections and dep_section in config:
                    dep_entry = config[dep_section].get(dep_name)
                    if dep_entry and dep_entry.get("flagActive", 0) == 1:
                        # Check if this dependency should be included based on status_map logic
                        if not status_map:
                            valid_dependencies.append(dep)
                        elif dep in status_map and status_map[dep].lower() == "failed":
                            valid_dependencies.append(dep)

        activity = {
            "name": f"{section}_{name}",
            "path": "NB_Wrapper",
            "timeoutPerCellInSeconds": 9000,
            "args": {},
            "dependencies": valid_dependencies 
        }
        
        artifact_type = entry.get("artifactType")
        artifact_name = entry.get("artifactName")
        artifact_workspace = entry.get("artifactWorkspace", "")  # Get workspace, default to empty (current workspace)
        
        artifact_params = {}
        if "artifactParams" in entry:
            for source_attr, target_param in entry["artifactParams"].items():
                if source_attr == "model":
                    artifact_params[target_param] = model
                else:
                    artifact_params[target_param] = entry.get(source_attr, None)

        artifact_params["batchExecution"] = batchExecution

        if artifact_type == "pipeline":
            if section=="raw":
                copyDataConfigJson = GetCopyDataConfig(entry)
                artifact_params["copyDataConfig"] = copyDataConfigJson
                artifact_params["param_workspace_id"] = workspaceId
                artifact_params["param_lh_bronze_id"] = bronze_lakehouse_id
                artifact_params["param_kqldatabase_id"] = log_kqldatabase_id
                artifact_params["param_kusto_uri"] = kustoURI

            activity["args"] = {
                "model": model,
                "batchExecution": batchExecution,
                "notebook":"NB_RunPipeline",
                "processName": f"{section}_{name}",
                "pipelineName": artifact_name,
                "pipelineParams": json.dumps(artifact_params),
                "targetWorkspace": artifact_workspace  # Pass the workspace parameter
            }
        elif artifact_type == "notebook":
            # activity["path"] = artifact_name
            # artifact_params.append(notebookParams)
            # activity["args"] = artifact_params
            activity["args"] = {
                "model": model,
                "batchExecution": batchExecution,
                "notebook": artifact_name,
                "notebookParams": json.dumps(artifact_params),
                "processName": f"{section}_{name}",
                "targetWorkspace": artifact_workspace  # Pass the workspace parameter
            }
            
        dag_activities.append(activity)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check model name and active status
if config.get("model") == f"{model}" and config.get("active", False):

    extractionDates = GetStartDates(model,"Extraction")
    
    for section in sections:
        if section in config:
            if not status_map:
                for name, entry in config[section].items():
                    ProcessEntryWithWrapper(name, entry, section, config, sections, status_map)
            else:
                for name, entry in config[section].items():
                    processId=f"{section}_{name}"
                    if processId in status_map and status_map[processId].lower() == "failed":
                        ProcessEntryWithWrapper(name, entry, section, config, sections, status_map)                        
                        # process_entry(name, entry, section)
else:
    print("Model is not active or incorrect model name.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities": dag_activities,
    "timeoutInSeconds": 43200,  # max timeout for the entire DAG
    "concurrency": 4  # max number of notebooks to run concurrently
}

print(json.dumps(DAG, indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# notebookutils.notebook.runMultiple(["NotebookSimple", "NotebookSimple2"])
if notebookutils.notebook.validateDAG(DAG):
    notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
