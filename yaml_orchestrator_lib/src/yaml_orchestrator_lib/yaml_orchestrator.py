"""YAML Orchestrator Library

This module provides YAML-based orchestration functions for Microsoft Fabric workflows.
"""

import yaml
import json
import uuid
import datetime as Dtime
from typing import Dict, List, Any, Optional
from pyspark.sql.functions import col

# Fabric-specific globals - these are set during initialization
notebookutils = None

# Control what gets imported with "from common_functions import *"
# Explicitly exclude global variables to prevent overriding notebook globals
__all__ = [
    'configure_fabric_globals', 
    'GetLastModelExecution',
    'GetStartDates',
    'GetCopyDataConfig',
    'BuildGroupDependencyMap',
    'ProcessEntryWithWrapper',
    'load_yaml_config',
    'generate_dag_from_config', 
    
    # Note: notebookutils are intentionally excluded
    # to prevent overriding the notebook's global variables
]

def _get_spark_session():
    """Get active Spark session with proper error handling."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.getOrCreate()
        return spark
    except Exception as e:
        print(f"Warning: Could not get Spark session: {e}")
        return None

def configure_fabric_globals(notebook_utils=None):
    """Configure Fabric-specific global utilities.
    
    Args:
        notebook_utils: The notebookutils object from Fabric notebook context
    """
    global notebookutils
    notebookutils = notebook_utils

def GetLastModelExecution(model: str) -> Optional[Any]:
    """Get the last execution details for a specific model from KQL.
    
    Args:
        model (str): The name of the model to query
        
    Returns:
        DataFrame or None: DataFrame containing last execution details or None if error
    """
    try:
        spark_session = _get_spark_session()
        if not spark_session:
            return None
            
        kustoUri = spark_session.conf.get(f"kustoUri")
        database = spark_session.conf.get(f"kustoDatabase")
        
        query = f"LastFailedBatchProcesses('{model}')"
        df = spark_session.read.format("com.microsoft.kusto.spark.synapse.datasource") \
            .option("kustoCluster", kustoUri) \
            .option("kustoDatabase", database) \
            .option("kustoQuery", query) \
            .option("accessToken", notebookutils.credentials.getToken('kusto'))\
            .load()
        
        return df
    except Exception as e:
        print(f"Error getting last model execution: {e}")
        print("No kusto URI or database defined")
        return None

def GetStartDates(model: str, scope: str) -> Optional[Dict[str, Any]]:
    """Get start dates for model processing from KQL.
    
    Args:
        model (str): The name of the model
        scope (str): The scope of the processing
        
    Returns:
        dict or None: Dictionary with tableName as keys and startDate as values
    """
    try:
        spark_session = _get_spark_session()
        if not spark_session:
            return None
            
        kustoUri = spark_session.conf.get(f"kustoUri")
        database = spark_session.conf.get(f"kustoDatabase")
        
        df = spark_session.read.format("com.microsoft.kusto.spark.synapse.datasource") \
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

    except Exception as e:
        print(f"Error getting start dates: {e}")
        print("No kusto URI or database defined")
        return None

def GetCopyDataConfig(entry: Dict[str, Any], model: str, workspace_id: str, bronze_lakehouse_id: str, extraction_dates: Dict[str, Any]) -> str:
    """Generate copy data configuration for a given entry.
    
    Args:
        entry (dict): Configuration entry containing source and destination details
        model (str): The name of the model
        workspace_id (str): The workspace ID
        bronze_lakehouse_id (str): The bronze lakehouse ID
        extraction_dates (dict): Dictionary of extraction dates
        
    Returns:
        str: JSON string of the copy data configuration
    """
    start_date = Dtime.datetime.now()
    
    def generate_source_read_command(entry):
        """Generate SQL command for data extraction."""
        source_system_type = entry.get("sourceSystemType", "").lower()
        extract_type = entry.get("extractType", "").lower()
        source_object_name = entry.get("sourceObjectName", "")
        source_select_columns = entry.get("sourceSelectColumns", None)
        delta_date_column = entry.get("deltaDateColumn", None)
        
        if extract_type == "delta": 
            delta_start_date = extraction_dates.get(source_object_name, '1900-01-01')
        else:
            delta_start_date = None
            
        delta_end_date = entry.get("deltaEndDate", None)
        delta_filter_condition = entry.get("deltaFilterCondition", None)
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
            where_clauses = []
            if delta_end_date:
                end_date = delta_end_date

            if delta_filter_condition:
                where_clauses.append(delta_filter_condition)

            if "deltaDateColumn" in entry:
                where_clauses.append(f"{delta_date_column} >= '{delta_start_date}'")
                where_clauses.append(f"{delta_date_column} < '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'")

            where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            if source_select_columns:
                return f"SELECT {source_select_columns} FROM {source_object_name} {where_clause}"
            else:
                return f"SELECT * FROM {source_object_name} {where_clause}"

        return None

    # Generate destination patterns
    destination_object_pattern = f"{entry['destinationObjectPattern']}_{start_date.strftime('%Y%m%d_%H%M')}.{entry['destinationObjectType']}"
    destination_directory_pattern = f"{entry['destinationDirectoryPattern']}{start_date.strftime('%Y/%m/%d/%H/%M')}/"

    result_entry = {
        "model": f'{model}',
        "sourceSystemName": entry["sourceSystemName"],
        "sourceSystemType": entry["sourceSystemType"],
        "sourceLocationName": entry["sourceLocationName"],
        "sourceObjectName": entry["sourceObjectName"],
        "sourceSelectColumns": entry.get("sourceSelectColumns", None),
        "sourceKeyColumns": entry.get("sourceKeyColumns", None),
        "destinationSystemName": entry["destinationSystemName"],
        "destinationSystemType": entry["destinationSystemType"],
        "destinationObjectPattern": destination_object_pattern,
        "destinationDirectoryPattern": destination_directory_pattern,
        "destinationObjectType": entry["destinationObjectType"],
        "extractType": entry["extractType"],
        "deltaStartDate": entry.get("deltaStartDate", None),
        "deltaDateColumn": entry.get("deltaDateColumn", None),
        "flagBlock": entry.get("flagBlock", None),
        "flagActive": entry.get("flagActive"),
        "sourceReadCommand": generate_source_read_command(entry),
        "startDate": start_date.strftime('%Y-%m-%d %H:%M:%S'),
    }

    # Convert to JSON
    json_output = json.dumps(result_entry, indent=4)
    return json_output

def BuildGroupDependencyMap(config: Dict[str, Any]) -> Dict[str, List[str]]:
    """Build a mapping of groups to their dependent activities.
    
    Args:
        config (dict): The YAML configuration dictionary
        
    Returns:
        dict: Mapping of group names to lists of activities
    """
    group_dependency_map = {}

    for section, entries in config.items():
        if isinstance(entries, dict):  # Ensure entries is a dictionary
            for entry_name, entry_data in entries.items():
                if isinstance(entry_data, dict):  # Ensure entry_data is also a dictionary
                    group = entry_data.get("group")
                    if group:
                        if group not in group_dependency_map:
                            group_dependency_map[group] = []
                        group_dependency_map[group].append(f"{section}_{entry_name}")

    return group_dependency_map

def ProcessEntryWithWrapper(name: str, entry: Dict[str, Any], section: str, config: Dict[str, Any], 
                           sections: List[str], status_map: Dict[str, str], model: str, 
                           batch_execution: str, workspace_id: str, bronze_lakehouse_id: str, 
                           log_kqldatabase_id: str, kusto_uri: str, 
                           group_dependency_map: Dict[str, List[str]], 
                           extraction_dates: Dict[str, Any], 
                           timeoutPerCellInSeconds: int = 9000) -> Optional[Dict[str, Any]]:
    """Process a configuration entry and generate a DAG activity.
    
    Args:
        name (str): Name of the entry
        entry (dict): Configuration entry data
        section (str): Section name (raw, silver, gold)
        config (dict): Complete configuration dictionary
        sections (list): List of all sections
        status_map (dict): Status mapping for resume functionality
        model (str): Model name
        batch_execution (str): Batch execution ID
        workspace_id (str): Workspace ID
        bronze_lakehouse_id (str): Bronze lakehouse ID
        log_kqldatabase_id (str): Log KQL database ID
        kusto_uri (str): KQL URI
        group_dependency_map (dict): Group dependency mapping
        extraction_dates (dict): Extraction dates dictionary
        timeoutPerCellInSeconds (int): Timeout per cell in seconds (default: 9000)
        
    Returns:
        dict or None: DAG activity dictionary if entry is active, None otherwise
    """
    if entry.get("flagActive", 0) == 1:
        # Process dependencies
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
            "timeoutPerCellInSeconds": timeoutPerCellInSeconds,
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

        artifact_params["batchExecution"] = batch_execution

        if artifact_type == "pipeline":
            if section == "raw":
                copy_data_config_json = GetCopyDataConfig(
                    entry, model, workspace_id, bronze_lakehouse_id, extraction_dates
                )
                artifact_params["copyDataConfig"] = copy_data_config_json
                artifact_params["param_workspace_id"] = workspace_id
                artifact_params["param_lh_bronze_id"] = bronze_lakehouse_id
                artifact_params["param_kqldatabase_id"] = log_kqldatabase_id
                artifact_params["param_kusto_uri"] = kusto_uri

            activity["args"] = {
                "model": model,
                "batchExecution": batch_execution,
                "notebook": "NB_RunPipeline",
                "processName": f"{section}_{name}",
                "pipelineName": artifact_name,
                "pipelineParams": json.dumps(artifact_params),
                "targetWorkspace": artifact_workspace  # Pass the workspace parameter
            }
        elif artifact_type == "notebook":
            # For cross-workspace notebooks, handle DAG structure according to Microsoft documentation
            if artifact_workspace and artifact_workspace.strip():
                # Cross-workspace notebook: modify activity structure to include workspace
                # Remove the wrapper approach and use direct notebook execution with workspace specification
                activity["path"] = artifact_name
                activity["workspaceName"] = artifact_workspace  # Specify target workspace in DAG activity
                activity["args"] = artifact_params
            else:
                # Local workspace notebook: use wrapper approach for consistent logging and error handling
                activity["args"] = {
                    "model": model,
                    "batchExecution": batch_execution,
                    "notebook": artifact_name,
                    "notebookParams": json.dumps(artifact_params),
                    "processName": f"{section}_{name}",
                    "targetWorkspace": ""  # Empty for local workspace
                }
            
        return activity
    
    return None

def load_yaml_config(yaml_file_path: str) -> Dict[str, Any]:
    """Load and parse YAML configuration from Fabric lakehouse.
    
    Args:
        yaml_file_path (str): Path to the YAML configuration file
        
    Returns:
        dict: Parsed YAML configuration
    """
    spark_session = _get_spark_session()
    if not spark_session:
        raise RuntimeError("Could not get Spark session")
        
    # Read YAML file from Lakehouse as text
    yaml_content = spark_session.read.text(yaml_file_path).collect()

    # Convert rows to a single string
    yaml_string = "\n".join(row.value for row in yaml_content)

    # Parse YAML
    config = yaml.safe_load(yaml_string)
    return config

def generate_dag_from_config(config: Dict[str, Any], model: str, model_reset: bool = False, 
                            workspace_id: str = None, bronze_lakehouse_id: str = None,
                            log_kqldatabase_id: str = None, kusto_uri: str = None,
                            sections: List[str] = None, timeoutInSeconds: int = 43200,
                            concurrency: int = 4) -> Dict[str, Any]:
    """Generate a complete DAG from YAML configuration.
    
    Args:
        config (dict): Parsed YAML configuration
        model (str): Model name
        model_reset (bool): Whether to reset the model (ignore previous status)
        workspace_id (str): Workspace ID
        bronze_lakehouse_id (str): Bronze lakehouse ID
        log_kqldatabase_id (str): Log KQL database ID
        kusto_uri (str): KQL URI
        sections (list): List of sections to process (default: ["raw", "silver", "gold"])
        timeoutInSeconds (int): Timeout for the entire DAG (default: 43200)
        concurrency (int): Max number of notebooks to run concurrently (default: 4)
        
    Returns:
        dict: Complete DAG configuration ready for execution
    """
    if sections is None:
        sections = ["raw", "silver", "gold"]
    dag_activities = []
    
    # Generate batch execution ID
    batch_execution = str(uuid.uuid4())
    status_map = {}

    if not model_reset:
        df_last_execution = GetLastModelExecution(model)
        if df_last_execution and df_last_execution.head(1):
            # Convert dataframe to a dict {processInternalId -> status}
            status_map = {row["processInternalId"]: row["status"] for row in df_last_execution.collect()}
            # Get first row's batchExecution
            batch_execution = df_last_execution.first()["batchExecution"]

    # Build group dependency map
    group_dependency_map = BuildGroupDependencyMap(config)
    
    # Get extraction dates
    extraction_dates = GetStartDates(model, "Extraction") or {}
    
    # Check model name and active status
    if config.get("model") == model and config.get("active", False):
        for section in sections:
            if section in config:
                if not status_map:
                    # Process all active entries
                    for name, entry in config[section].items():
                        activity = ProcessEntryWithWrapper(
                            name, entry, section, config, sections, status_map,
                            model, batch_execution, workspace_id, bronze_lakehouse_id,
                            log_kqldatabase_id, kusto_uri, group_dependency_map, extraction_dates
                        )
                        if activity:
                            dag_activities.append(activity)
                else:
                    # Process only failed entries
                    for name, entry in config[section].items():
                        process_id = f"{section}_{name}"
                        if process_id in status_map and status_map[process_id].lower() == "failed":
                            activity = ProcessEntryWithWrapper(
                                name, entry, section, config, sections, status_map,
                                model, batch_execution, workspace_id, bronze_lakehouse_id,
                                log_kqldatabase_id, kusto_uri, group_dependency_map, extraction_dates
                            )
                            if activity:
                                dag_activities.append(activity)
    else:
        raise ValueError("Model is not active or incorrect model name.")
    
    # Build final DAG
    dag = {
        "activities": dag_activities,
        "timeoutInSeconds": timeoutInSeconds,  # max timeout for the entire DAG
        "concurrency": concurrency  # max number of notebooks to run concurrently
    }
    
    return dag