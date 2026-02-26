# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import sempy.fabric as fabric
import datetime as Dtime
from time import sleep
import json
import multiprocessing
import pyspark.sql.functions as F
import delta.tables as D
from typing import Tuple, Dict, List
from dateutil.relativedelta import relativedelta
import pyspark.sql as pysparksql
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import (
    desc,
    col,
    explode_outer,
    concat_ws,
    current_timestamp,
    sha2,
    array_join,
    lit,
    row_number,
    collect_list,
    when,
    size
)
import yaml
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetVariableLibraryName() -> str:
    """
    Dynamically discovers the variable library name following the naming convention 'vl-<project>-01'.
    Caches the result in spark configuration to avoid repeated API calls.
    
    Returns:
        str: The name of the variable library matching the project naming convention
    """
    
    # Check if variable library name is already cached
    cached_name = spark.conf.get("fabric.variable_library_name", None)
    if cached_name:
        return cached_name
    
    try:
        # Get workspace ID for API call
        workspace_id = spark.conf.get("fabric.workspace_id", None)
        if not workspace_id:
            workspace_id = fabric.get_workspace_id()
            spark.conf.set("fabric.workspace_id", workspace_id)
        
        # Use Fabric REST API to get variable libraries
        client = fabric.FabricRestClient()
        response = client.get(f"/v1/workspaces/{workspace_id}/items?type=VariableLibrary")
        variable_libraries = response.json()["value"]
        
        # Find variable library matching naming convention: vl-*-01 (starts with "vl" and ends with "01")
        import re
        pattern = r'^vl-.+-01$'
        
        matching_library = next(
            (vl["displayName"] for vl in variable_libraries 
             if re.match(pattern, vl["displayName"])), 
            None
        )
        
        if matching_library:
            # Cache the result
            spark.conf.set("fabric.variable_library_name", matching_library)
            return matching_library
        else:
            raise Exception(f"Variable library matching pattern 'vl-*-01' not found in workspace. Available libraries: {[vl['displayName'] for vl in variable_libraries]}")
            
    except Exception as e:
        print(f"Error discovering variable library: {e}. Using fallback approach.")
        # Fallback: try to find any library starting with "vl" and ending with "01"
        try:
            client = fabric.FabricRestClient()
            response = client.get(f"/v1/workspaces/{workspace_id}/items?type=VariableLibrary")
            variable_libraries = response.json()["value"]
            
            # First try to find one matching the pattern vl-*-01
            fallback_library = next(
                (vl["displayName"] for vl in variable_libraries 
                 if vl["displayName"].startswith("vl-") and vl["displayName"].endswith("-01")), 
                None
            )
            
            # If no pattern match, use the first available variable library
            if not fallback_library and variable_libraries:
                fallback_library = variable_libraries[0]["displayName"]
                print(f"No variable library matching pattern found. Using first available: {fallback_library}")
            
            # If still no library found, raise an exception
            if not fallback_library:
                raise Exception("No variable libraries found in workspace")
            
            spark.conf.set("fabric.variable_library_name", fallback_library)
            return fallback_library
        except:
            # Ultimate fallback - raise an exception since we can't find any variable library
            raise Exception("Failed to discover any variable library in workspace. Please ensure at least one variable library exists.")


def GetVariableLibrary():
    """
    Gets the variable library object using the dynamically discovered name.
    
    Returns:
        Variable library object from notebookutils
    """
    library_name = GetVariableLibraryName()
    return notebookutils.variableLibrary.getLibrary(library_name)


def GetFabricIds() -> tuple:

    # Retrieves the IDs of the current workspace and associated Lakehouses from variable library.

    # This function retrieves the workspace ID and the IDs of the Bronze, Silver, and Gold 
    # Lakehouses from the variable library. These IDs can be used for accessing resources 
    # in the Lakehouses.

    # Returns:
    #     tuple: A tuple containing the following elements:
    #         - workspace_id (str): The ID of the current workspace.
    #         - bronze_lakehouse_id (str): The ID of the Bronze Lakehouse in the workspace.
    #         - silver_lakehouse_id (str): The ID of the Silver Lakehouse in the workspace.
    #         - gold_lakehouse_id (str): The ID of the Gold Lakehouse in the workspace.


    # Function to get Spark config variable or return None if not set
    def get_spark_conf(key: str) -> str:
        return spark.conf.get(key, None)  # Returns None if the key does not exist


    # Get values from Spark config (if already set)
    current_workspace_id = fabric.get_workspace_id()
    workspace_id = get_spark_conf("fabric.workspace_id")
    bronze_lakehouse_id = get_spark_conf("fabric.bronze_lakehouse_id")
    silver_lakehouse_id = get_spark_conf("fabric.silver_lakehouse_id")
    gold_lakehouse_id = get_spark_conf("fabric.gold_lakehouse_id")


    # If workspace_id is not stored, get it from variable library
    if not workspace_id:
        workspace_id = current_workspace_id
        spark.conf.set("fabric.workspace_id", workspace_id)


    def GetLakehouses(workspace_id, max_retries=10, base_delay=5) -> tuple:

        # Retrieve lakehouse IDs from variable library first
        try:
            vl = GetVariableLibrary()
            bronze_lakehouse_id = vl.bronze_lakehouseId
            silver_lakehouse_id = vl.silver_lakehouseId
            gold_lakehouse_id = vl.gold_lakehouseId
            return bronze_lakehouse_id, silver_lakehouse_id, gold_lakehouse_id
        except Exception as e:
            print(f"Could not retrieve IDs from variable library: {e}. Falling back to API calls.")
            
            # Fallback: Retrieves all Lakehouses within the given workspace, handling rate limits with retry logic.

            # Args:
            #     workspace_id (str): The ID of the workspace.
            #     max_retries (int, optional): Maximum retry attempts. Default is 10.
            #     base_delay (int, optional): Base delay in seconds for exponential backoff. Default is 5.

            # Returns:
            #     tuple: A tuple containing bronze, silver, and gold lakehouse IDs.

            retries = 0
            while retries < max_retries:
                try:
                    lakehouses = notebookutils.lakehouse.list(workspace_id)
                    lakehouse_list = [{"displayName": lh["displayName"], "id": lh["id"]} for lh in lakehouses]
                    
                    # Identify Bronze, Silver, and Gold Lakehouses dynamically (case-insensitive)
                    bronze_id = next((lh["id"] for lh in lakehouse_list if "bronze" in lh["displayName"].lower()), None)
                    silver_id = next((lh["id"] for lh in lakehouse_list if "silver" in lh["displayName"].lower()), None)
                    gold_id = next((lh["id"] for lh in lakehouse_list if "gold" in lh["displayName"].lower()), None)
                    
                    return bronze_id, silver_id, gold_id
                except Exception as e:
                    if "RequestBlocked" in str(e) or "429" in str(e):
                        retry_after = base_delay * (2**retries)  # Exponential backoff
                        print(
                            f"Rate limit exceeded. Retrying in {retry_after} seconds..."
                        )
                        time.sleep(retry_after)
                        retries += 1
                    else:
                        raise  # Re-raise the exception if it's not a rate limit issue
            raise Exception(
                f"Failed to retrieve lakehouses after {max_retries} retries."
            )


    # If lakehouse IDs are not stored, fetch them
    if not bronze_lakehouse_id or not silver_lakehouse_id or not gold_lakehouse_id or (workspace_id!=current_workspace_id):
        bronze_lakehouse_id, silver_lakehouse_id, gold_lakehouse_id = GetLakehouses(workspace_id)

        # Store IDs in Spark config for future use
        if bronze_lakehouse_id:
            spark.conf.set("fabric.bronze_lakehouse_id", bronze_lakehouse_id)
        if silver_lakehouse_id:
            spark.conf.set("fabric.silver_lakehouse_id", silver_lakehouse_id)
        if gold_lakehouse_id:
            spark.conf.set("fabric.gold_lakehouse_id", gold_lakehouse_id)


    return workspace_id, bronze_lakehouse_id, silver_lakehouse_id, gold_lakehouse_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Global Variables for Fabric IDs
# These variables will be available throughout the notebook without repeatedly calling GetFabricIds()

# Initialize global variables
WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID = GetFabricIds()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def SetSessionLogURI(eventhouseName, databaseName, eventhouseWorkspaceId=''):
    if (eventhouseName != "" and databaseName != ""):
        try:
            # Try to get eventhouse information from variable library first
            vl = GetVariableLibrary()
            eventhouse_id = vl.eventhouseId
            # For kusto URI, we can construct it or use a fallback to API
            
            # Fallback to API for kustoUri as it's not in variable library
            workspaceId = spark.conf.get("trident.workspace.id")
            if not workspaceId:
                workspaceId = vl.workspaceId
            
            if eventhouseWorkspaceId!="":
                workspaceId = eventhouseWorkspaceId
   
            client = fabric.FabricRestClient()
            ws_eventhouses = fabric.FabricRestClient().get(f"/v1/workspaces/{workspaceId}/eventhouses").json()["value"]
            eh_Logging = [eh for eh in ws_eventhouses if eh["displayName"]==eventhouseName]
            kustoUri = eh_Logging[0]["properties"]["queryServiceUri"]
            kqldatabase_id = eh_Logging[0]["properties"]["databasesItemIds"][0]
            
        except Exception as e:
            print(f"Could not retrieve eventhouse info from variable library: {e}. Using API fallback.")
            # Fallback to original API-based approach
            workspaceId = spark.conf.get("trident.workspace.id")
            client = fabric.FabricRestClient()
            ws_eventhouses = fabric.FabricRestClient().get(f"/v1/workspaces/{workspaceId}/eventhouses").json()["value"]
            
            eh_Logging = [eh for eh in ws_eventhouses if eh["displayName"]==eventhouseName]
            kustoUri = eh_Logging[0]["properties"]["queryServiceUri"]
            eventhouse_id = eh_Logging[0]["id"]
            kqldatabase_id = eh_Logging[0]["properties"]["databasesItemIds"][0]

        spark.conf.set("fabric.log_eventhouse_id", eventhouse_id)
        spark.conf.set("fabric.log_kqldatabase_id", kqldatabase_id)
        spark.conf.set(f"kustoUri",kustoUri)
        spark.conf.set(f"kustoDatabase",databaseName)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetConfigMetadataFromYaml(model: str, objectName: str, layer: str) -> dict:
    """
    Retrieves configuration metadata for a specific model, object, and data layer from YAML configuration files.

    This function reads configuration metadata from YAML files in the ConfigFiles directory structure.
    It filters for active configurations matching the given `model` and `objectName`
    and returns the matching configuration as a dictionary.

    Args:
        model (str): The name of the model to filter on.
        objectName (str): The name of the object to filter on.
        layer (str): The data layer to retrieve metadata from. 
                     Accepted values: "raw", "silver", or "gold".
        environment (str): The environment to read configuration from (default: "dev").

    Returns:
        dict: A dictionary containing the configuration metadata for the specified parameters.
              Returns `None` if no matching record is found.

    Raises:
        FileNotFoundError: If the YAML configuration file doesn't exist.
        yaml.YAMLError: If there's an error parsing the YAML file.
        KeyError: If the specified layer or objectName is not found in the configuration.
    """
    
    # Use global variables instead of calling GetFabricIds()
    global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID
    
    # Construct the path to the YAML configuration file
    config_file_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Files/config/{model}.yml"
    
    # Read YAML file from Lakehouse as text
    yaml_content = spark.read.text(config_file_path).collect()

    # Convert rows to a single string
    yaml_string = "\n".join(row.value for row in yaml_content)

    try:
        # Read and parse the YAML
        config_data = yaml.safe_load(yaml_string)
        
        # Validate that the model is active
        if not config_data.get('active', False):
            return None
        
        # Map layer names for compatibility with existing code
        layer_mapping = {
            "Bronze": "raw",
            "Silver": "silver", 
            "Gold": "gold"
        }
        
        # Use mapped layer name if it exists, otherwise use the original
        yaml_layer = layer_mapping.get(layer, layer.lower())
        
        # Get the layer configuration
        layer_config = config_data.get(yaml_layer)
        if not layer_config:
            raise KeyError(f"Layer '{yaml_layer}' not found in configuration")
        
        # Find the object configuration
        object_config = None
        
        # For raw layer, match against sourceObjectName (like the original function)
        if yaml_layer == "raw":
            for obj_name, obj_config in layer_config.items():
                if obj_config.get('sourceObjectName') == objectName:
                    object_config = obj_config.copy()
                    object_config['objectName'] = obj_name  # Add the key as objectName
                    break
        else:
            # For silver and gold layers, match against the object key directly
            object_config = layer_config.get(objectName)
            if object_config:
                object_config = object_config.copy()
                object_config['objectName'] = objectName
            else:
                for obj_name, obj_config in layer_config.items():
                    if obj_config.get('sourceObjectName') == objectName:
                        object_config = obj_config.copy()
                        object_config['objectName'] = obj_name  # Add the key as objectName
                        break
                        
            if yaml_layer == "silver":
                if not 'partitionColumns' in object_config:
                    object_config['partitionColumns'] = None
                if not 'keyColumns' in object_config:
                    object_config['keyColumns'] = None
        
        if not object_config:
            return None
        
        # Check if the object is active
        if not object_config.get('flagActive', 0):
            return None
        
        # Add model information to the configuration
        object_config['model'] = model
        
        # Handle deltaProcessing section - flatten to root level for backward compatibility
        if 'deltaProcessing' in object_config:
            delta_config = object_config.pop('deltaProcessing')
            
            # Add deltaProcessing attributes to the root level with defaults
            object_config['fullProcess'] = delta_config.get('fullProcess', False)
            if 'dateType' in delta_config:
                object_config['dateType'] = delta_config.get('dateType', 'Day')
            if 'dateUnit' in delta_config:
                object_config['dateUnit'] = delta_config.get('dateUnit', 1)
            if 'dateColumnFormat' in delta_config:
                object_config['dateColumnFormat'] = delta_config.get('dateColumnFormat', 'yyyy/MM/dd')
            
            # Only add filterColumn if it exists and fullProcess is False
            if not object_config['fullProcess'] and 'filterColumn' in delta_config:
                object_config['filterColumn'] = delta_config['filterColumn']
        
        # Update destination paths for Silver and Gold layers with full OneLake paths
        if yaml_layer in ["silver", "gold"]:
            lakehouse_id = SILVER_LAKEHOUSE_ID if yaml_layer == "silver" else GOLD_LAKEHOUSE_ID
            destination_pattern = object_config.get('destinationObjectPattern')
            
            if destination_pattern:
                object_config['destinationObjectPattern'] = (
                    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
                    f"{lakehouse_id}/Tables/{destination_pattern}"
                )
        
        return object_config
        
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file {config_file_path}: {str(e)}")
    except Exception as e:
        raise Exception(f"Error reading configuration from {config_file_path}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetConfigMetadata(model: str, objectName: str, layer: str) -> dict:

    # Retrieves configuration metadata for a specific model, object, and data layer.

    # This function reads configuration metadata from the appropriate table in the specified Lakehouse.
    # It filters for active configurations (`flag_active = 1`) matching the given `model` and `objectName`
    # and returns the first matching record as a dictionary.

    # Args:
    #     model (str): The name of the model to filter on.
    #     objectName (str): The name of the object to filter on.
    #     layer (str): The data layer to retrieve metadata from. 
    #                  Accepted values: "Bronze", "Silver", or "Gold".

    # Returns:
    #     dict: A dictionary containing the configuration metadata for the specified parameters.
    #           Returns `None` if no matching record is found.

    # Check if we should use YAML configuration
    use_yaml = spark.conf.get("UseYaml", "false").lower() == "true"

    if use_yaml:
        # Use YAML-based configuration
        return GetConfigMetadataFromYaml(model, objectName, layer)
    else:
        # Use global variables instead of calling GetFabricIds()
        global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID

        # Determine configuration table path based on the data layer
        config_table_path = (
            f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE_ID}/Tables/config/silverGoldConfig"
            if layer in ["Silver", "Gold"]
            else f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Tables/config/copyDataConfig"
        )

        # Determine the correct filtering condition for object names
        filter_objectName = (
            f"objectName == '{objectName}'"
            if layer in ["Silver", "Gold"]
            else f"sourceObjectName == '{objectName}'"
        )

        # Read and filter configuration data
        df = (
            spark.read.format("delta")
            .load(config_table_path)
            .filter(f"flagActive = 1 AND model = '{model}' AND {filter_objectName}")
            .first()
            .asDict()
        )

        # Update destination directory pattern for Silver and Gold layers
        if layer in ["Silver", "Gold"]:
            df[
                "destinationObjectPattern"
            ] = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE_ID if layer == 'Silver' else GOLD_LAKEHOUSE_ID}/Tables/{df.get('destinationObjectPattern')}"

        return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Attention!
# **Column names are case sensitive, adjust the case of "ctr" columns according to the desired naming conventions**
# 
# **Admin** in Fabric is not an allowed schema name, shortcuts in lakehouse were created under config schema

# MARKDOWN ********************

# # Technical Columns

# CELL ********************

# Add technical columns (based in all columns from sourceDF)
def AddMetadataColumns(sourceDF, state):
    auxDF = sourceDF
    if state == True:
        auxDF = sourceDF.withColumn(
            "ctr_hash_validate", F.sha2(F.concat_ws("|", *sourceDF.columns), 256)
        ).na.fill({"ctr_hash_validate": "null from temp"})

    destDF = auxDF.withColumn("ctr_ins_date", F.current_timestamp()).withColumn(
        "ctr_last_operation_date", F.current_timestamp()
    )
    return destDF


# Add hash_dim_key column - based in one, or more, key columns
def AddSkColumn(sourceDF, keyColumns):
    destDF = sourceDF.withColumn(
        "ctr_hash_dim_key", F.sha2(F.concat_ws("|", *keyColumns), 256)
    )
    return destDF


# Add techinacal columns to dimensions
def AddDimTechnicalColumns(sourceDF, keyColumns):
    metadataDF = AddMetadataColumns(sourceDF, state=True)
    skDF = AddSkColumn(metadataDF, keyColumns)
    return skDF


# Add techinical columns to facts
def AddFactTechnicalColumns(sourceDF):
    metadataDF = AddMetadataColumns(sourceDF, state=False)
    return metadataDF

# add metadata columns to a DataFrame representing a table, considering different types of slowly changing dimensions (SCDs)
# Input:
#   sourceDF: The DataFrame representing the table to be enhanced with metadata columns.
#   scdType: A string indicating the type of slowly changing dimension (SCD). It can be either 'Type2' or 'Type3'.
#   state: A boolean indicating whether to add state validation metadata. If True, state validation metadata will be added.
#   historicalType2Columns: A list of column names representing the historical type 2 columns used for dimension history tracking.
#   historicalType3Columns: A list of column names representing the historical type 3 columns used for dimension history tracking. Only applicable if scdType is 'Type3'
# Output: The DataFrame representing the enhanced table with metadata and timestamp columns.
def AddMetadataColumnsHistory(sourceDF, scdType, state, historicalType2Columns, historicalType3Columns = []):
    auxDF = sourceDF
    if (state == True):
        if(scdType == 'Type2'):
            # If historicalType2Columns list is empty, all columns are considered type 2
            if(len(historicalType2Columns) == 0):
                hashValidateColumns = sourceDF.columns
                hashValidateColumns.remove('ctr_date_key')
                
                auxDF = sourceDF\
                        .withColumn('ctr_hash_validate_history', F.sha2(F.concat_ws('|', *hashValidateColumns), 256))\
                        .withColumn('ctr_', F.sha2(F.concat_ws('|', *hashValidateColumns), 256))\
                        .withColumn('ctr_hash_validate', F.sha2(F.concat_ws('|', *hashValidateColumns), 256))\
                        .na.fill({'ctr_hash_validate_history': 'null from stage',\
                                 'ctr_hash_validate_non_history': 'null from stage',\
                                 'ctr_hash_validate': 'null from stage'})
            else:
                # columns inside historicalType2Columns list are considered type 2 and the rest type 1
                hashNonHistoryColumns = sourceDF.columns
                hashValidateColumns = sourceDF.columns
                hashValidateColumns.remove('ctr_date_key')
                
                for i in historicalType2Columns:
                    hashNonHistoryColumns.remove(i)
                    
                if 'ctr_date_key' in historicalType2Columns:
                    historicalType2Columns.remove('ctr_date_key')
                if 'ctr_date_key' in hashNonHistoryColumns:
                    hashNonHistoryColumns.remove('ctr_date_key')
                
                auxDF = sourceDF\
                    .withColumn('ctr_hash_validate_history', F.sha2(F.concat_ws('|', *historicalType2Columns), 256))\
                    .withColumn('ctr_hash_validate_non_history', F.sha2(F.concat_ws('|', *hashNonHistoryColumns), 256))\
                    .withColumn('ctr_hash_validate', F.sha2(F.concat_ws('|', *hashValidateColumns), 256))\
                    .na.fill({'ctr_hash_validate': 'null from stage'})
        else:
            #Type 6
            newType3Columns = []
            
            for i in historicalType3Columns:
                newType3Columns.append(i)
                newType3Columns.append(i+"_current")            
                
            # If historicalType2Columns list is empty, all columns are considered type 2
            if(len(historicalType2Columns) == 0):
                hashValidateColumns = sourceDF.columns
                hashValidateColumns.remove('ctr_date_key')
    
                auxDF = sourceDF\
                        .withColumn('ctr_hash_validate_history', F.sha2(F.concat_ws('|', *hashValidateColumns), 256))\
                        .withColumn('ctr_hash_validate_non_history', F.sha2(F.concat_ws('|', *hashValidateColumns), 256))\
                        .withColumn('ctr_hash_validate_type3', F.sha2(F.concat_ws('|', *newType3Columns), 256))\
                        .withColumn('ctr_hash_validate', F.sha2(F.concat_ws('|', *hashValidateColumns), 256))\
                        .na.fill({'ctr_hash_validate_history': 'null from stage',\
                                 'ctr_hash_validate_non_history': 'null from stage',\
                                 'ctr_hash_validate_type3': 'null from stage',\
                                 'ctr_hash_validate': 'null from stage'})
            else:
                # columns inside historyColumns are considered type 2 and the rest type 1
                # gets all columns from source except the type 3 columns
                hashNonHistoryColumns = sourceDF.drop(*newType3Columns).columns
                hashNonHistoryColumns.remove('ctr_date_key')
                hashValidateColumns = sourceDF.columns
                hashValidateColumns.remove('ctr_date_key')
                
                #Gets the type 1 columns that are not in historyColumns list
                hashNonHistoryColumns = list(set(hashNonHistoryColumns).difference(set(historicalType2Columns)))               
            
                auxDF = sourceDF\
                    .withColumn('ctr_hash_validate_history', F.sha2(F.concat_ws('|', *historicalType2Columns), 256))\
                    .withColumn('ctr_hash_validate_non_history', F.sha2(F.concat_ws('|', *hashNonHistoryColumns), 256))\
                    .withColumn('ctr_hash_validate_type3', F.sha2(F.concat_ws('|', *newType3Columns), 256))\
                    .withColumn('ctr_hash_validate', F.sha2(F.concat_ws('|', *hashValidateColumns), 256))\
                    .na.fill({'ctr_hash_validate': 'null from stage'})                

    destDF = auxDF\
            .withColumn('ctr_ins_date', F.current_timestamp())\
            .withColumn('ctr_last_operation_date', F.current_timestamp())
        
    return destDF


# Enhance a DataFrame representing either a dimension or a fact table (sourceDF) with technical metadata columns, including support for different types of slowly changing dimensions (SCDs)
# The parameter tableType is only required to pass in the notebook of Historical Facts with the value 'Fact'. If it is not passed the default value will be 'Dim'
# Input:
#   sourceDF: The DataFrame representing the table to be enhanced.
#   keyColumns: A list of column names or expressions representing the key columns used to generate the surrogate key.
#   historicalType2Columns: A list of column names or expressions representing the historical type 2 columns used for dimension history tracking.
#   scdType: A string indicating the type of slowly changing dimension (SCD). It can be either 'Type2' or 'Type3'.
#   tableType: A string indicating the type of table ('Dim' for dimension or 'Fact' for fact). Default value is 'Dim'.
#   historicalType3Columns: A list of column names or expressions representing the historical type 3 columns used for dimension history tracking. Only applicable if scdType is 'Type3'
# Output: The DataFrame representing the enhanced table with technical metadata columns and a surrogate key column
def AddTechnicalColumnsHistory(sourceDF, keyColumns, historicalType2Columns, scdType, tableType = 'Dim', historicalType3Columns = []):
    if scdType == 'Type2':
        dfMetadata = AddMetadataColumnsHistory(sourceDF, scdType, state = True, historicalType2Columns = historicalType2Columns)
    else:
        dfMetadata = AddMetadataColumnsHistory(sourceDF, scdType, state = True, historicalType2Columns = historicalType2Columns, historicalType3Columns = historicalType3Columns)
        
    if tableType == 'Fact':
        dfSk = dfMetadata\
           .withColumn("ctr_hash_fact_key", F.sha2(F.concat_ws('|', *keyColumns), 256))
    else:
        dfSk = dfMetadata\
           .withColumn("ctr_hash_dim_key", F.sha2(F.concat_ws('|', *keyColumns), 256))
    return dfSk

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Get Source File Path
# # Get the last filePath to selected tableName
# # Get last file path to selected tableName by snapshot date
# # Get last file path to selected tableName by begin date

# CELL ********************

# Get the last filePath to selected tableName - from admin.admin_copydatalog
def GetSourcePathLastFile(
    workspace_id: str,
    bronze_lakehouse_id: str,
    silver_lakehouse_id: str,
    model: str,
    sourceSystem: str,
    objectName: str
) ->str :

    # Retrieves the latest extracted file path for a given model and object.

    # This function queries the `copydata_log` table in the Silver Lakehouse to find the most recent 
    # successful extraction for the specified `model` and `objectName`. It then constructs and 
    # returns the full path to the corresponding file in the Bronze Lakehouse.

    # Args:
    #     workspace_id (str): The ID of the workspace in Microsoft Fabric.
    #     bronze_lakehouse_id (str): The ID of the Bronze Lakehouse.
    #     silver_lakehouse_id (str): The ID of the Silver Lakehouse.
    #     model (str): The name of the model to filter on.
    #     objectName (str): The name of the object to filter on.

    # Returns:
    #     str: The full path to the latest extracted file in the Bronze Lakehouse.

    # Raises:
    #     AttributeError: If no successful extraction is found in the `copyDataLog` table.


    path = df = (
        spark.read.format("delta")
        .load(
            f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_id}/Tables/log/copyDataLog"
        )
        .filter(
            f"objectName = '{objectName}' and sourceLocationName = '{sourceSystem}' and model = '{model}' and status = 'Succeeded'"
        )
        .orderBy(desc(col("startDate")))
        .first()
    )["destinationPath"]

    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files/{path}/"


# Get last file path to selected tableName by snapshot date - from admin.admin_copydatalog

def GetSourcePathLastFileByDates(
    workspace_id: str,
    bronze_lakehouse_id: str,
    silver_lakehouse_id: str,
    model: str,
    sourceSystem: str,
    objectName: str):

    """
    Retrieves a list of extracted file paths for a given model and object, filtered by processing dates.

    This function first queries the `process_dates_config` table in the Silver Lakehouse to determine 
    the applicable processing date range. It then fetches all successful extractions from the `copydata_log`
    table within that range and returns a list of file paths from the Bronze Lakehouse.

    Args:
        workspace_id (str): The ID of the workspace in Microsoft Fabric.
        bronze_lakehouse_id (str): The ID of the Bronze Lakehouse.
        silver_lakehouse_id (str): The ID of the Silver Lakehouse.
        model (str): The name of the model to filter on.
        objectName (str): The name of the object to filter on.

    Returns:
        list: A list of full paths to extracted files in the Bronze Lakehouse.

    Raises:
        KeyError: If required columns are missing from the configuration table.
    """

    row = (
        spark.read.format("delta")
        .load(
            f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_id}/Tables/config/processDatesConfig"
        )
        .filter(
            f"tableName = '{objectName}' and model = '{model}' and lower(scope) = 'silver'"
        )
    ).first()


    if row is None:
        date_to_process = datetime.date.today()
    else:
        if row["date"] is not None:
            date_to_process = row["date"]
        else:
            today = datetime.date.today()

            if row.dateType == "Year":
                year_date = today - relativedelta(years=int(row.dateUnit))
                date_to_process = year_date.replace(day=1)

            elif row.dateType == "Month":
                month_date = today - relativedelta(months=int(row.dateUnit))
                date_to_process = month_date.replace(day=1)

            elif row.dateType == "Day":
                date_to_process = today - relativedelta(days=int(row.dateUnit))

    bronze_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files/"

    filter_table = (
        f"objectName = '{objectName}' and model = '{model}' and sourceLocationName = '{sourceSystem}' and status = 'Succeeded'"
        if (row is None or row["fullProcess"])
        else f"objectName = '{objectName}' and model = '{model}' and sourceLocationName = '{sourceSystem}' and status = 'Succeeded' and date_format(startDate, 'yyyy-MM-dd') between date_format('{date_to_process}', 'yyyy-MM-dd') and '2999-12-31'"
    )
    print(filter_table)

    paths = [
        bronze_path + path.destinationPath
        for path in (
            spark.read.format("delta")
            .load(
                f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_id}/Tables/log/copyDataLog"
            )
            .filter(filter_table)
            .orderBy(desc(col("startDate")))
        ).collect()
    ]

    return paths


# Get last file path to selected tableName by begin date - from admin.admin_copydatalog
# Used for reprocesseing files or to snapshot if you need last file for each month for example
def GetSourcePathFileByRangeDates(
    workspace_id: str,
    bronze_lakehouse_id: str,
    silver_lakehouse_id: str,
    model: str,
    sourceSystem = None,
    objectName = None,
    beginDate = None,
    endDate="29991231",
    interval=""):
    
    if interval == "" or interval == "D":
        dateFormat = "yyyyMMdd"

    if interval == "M":
        dateFormat = "yyyyMM"

    if interval == "Y":
        dateFormat = "yyyy"

    bronze_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files/"

    filter_table = (
        f"objectName = '{objectName}' and model = '{model}' and sourceLocationName = '{sourceSystem}' and status = 'Succeeded'"
        if (row is None or row["fullProcess"])
        else f"""objectName = '{objectName}' and model = '{model}' and sourceLocationName = '{sourceSystem}' and status = 'Succeeded' 
                and date_format(startDate, '{dateFormat}') between date_format('{beginDate}', '{dateFormat}') and date_format('{endDate}', '{dateFormat}')"""
    )

    paths = [
        bronze_path + path.destinationPath
        for path in (
            spark.read.format("delta")
            .load(
                f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_id}/Tables/log/copyDataLog"
            )
            .filter(filter_table)
            .orderBy(desc(col("startDate")))
        ).collect()
    ]

    return paths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetSourcePathLastFileFromKQL(
    model: str,
    sourceSystem: str,
    objectName: str
) -> str:
    """
    Retrieves the latest extracted file path for a given model and object from KQL database.

    This function queries the `copyDataLog` table in the specified KQL database to find the most recent 
    successful extraction for the specified `model` and `objectName`. It then constructs and 
    returns the full path to the corresponding file in the Bronze Lakehouse.

    Args:
        model (str): The name of the model to filter on.
        sourceSystem (str): The name of the source system to filter on.
        objectName (str): The name of the object to filter on.
        kql_database (str): The name of the KQL database (default: "eh_modernBI_fabricFramework_01").

    Returns:
        str: The full path to the latest extracted file in the Bronze Lakehouse.

    Raises:
        Exception: If no successful extraction is found in the `copyDataLog` table.
        Exception: If there's an error executing the KQL query.
    """
    
    # Use global variables instead of calling GetFabricIds()
    global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID
    
    try:
        # Construct KQL query to get the latest successful extraction
        kql_query = f"""
        copyDataLog
        | where objectName == '{objectName}' 
            and sourceLocationName == '{sourceSystem}' 
            and model == '{model}' 
            and status == 'Succeeded'
        | top 1 by startDate desc
        | project destinationPath
        """
        
        kustoUri = spark.conf.get(f"kustoUri")
        # The database to write the data
        database = spark.conf.get(f"kustoDatabase")

        # Execute KQL query using Spark's KQL connector
        result_df = (
            spark.read.format("com.microsoft.kusto.spark.synapse.datasource") \
            .option("kustoCluster", kustoUri) \
            .option("kustoDatabase", database) \
            .option("kustoQuery", kql_query)
            .option("accessToken", notebookutils.credentials.getToken('kusto'))\
            .load()
        )
        
        # Get the first row result
        first_row = result_df.first()
        
        if first_row is None:
            raise Exception(f"No successful extraction found for model='{model}', sourceSystem='{sourceSystem}', objectName='{objectName}'")
        
        destination_path = first_row["destinationPath"]
        
        # Construct and return the full Bronze Lakehouse path
        full_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Files/{destination_path}/"
        
        return full_path
        
    except Exception as e:
        raise Exception(f"Error querying KQL database '{kql_database}': {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetDateToProcessFromKQL(model: str, objectName: str, scope: str):
    # First, get the process dates configuration from KQL database
    date_config_query = f"""
    lastDatesConfig
    | where tableName == '{objectName}' 
        and model == '{model}' 
        and tolower(scope) == tolower('{scope}')
    """
    
    kustoUri = spark.conf.get(f"kustoUri")
    # The database to write the data
    database = spark.conf.get(f"kustoDatabase")

    # Execute date configuration query using Spark Kusto connector
    date_config_df = (
        spark.read.format("com.microsoft.kusto.spark.synapse.datasource") \
        .option("kustoCluster", kustoUri) \
        .option("kustoDatabase", database) \
        .option("kustoQuery", date_config_query)
        .option("accessToken", notebookutils.credentials.getToken('kusto'))\
        .load()
    )

    # Check if we have configuration data
    date_config_rows =  date_config_df.collect()

    # Get configs from yaml
    layer = "silver" if scope.lower() == "silver" else "gold"
    objectName = objectName.split('.')[-1]

    config_items = GetConfigMetadata(model, objectName, layer)
    
    full_process = config_items.get("fullProcess",False)
    dateType = config_items.get("dateType",None)
    dateUnit = config_items.get("dateUnit",None)

    if not date_config_rows and not dateType:
        print("No date configuration found, run full process")
        full_process = True
        date_to_process = "1900-01-01"

    else:
        if not date_config_rows:
            date_to_process = Dtime.date.today()
                     
            today = Dtime.date.today()
                
            if dateType == "Year":
                year_date = today - relativedelta(years=int(dateUnit))
                date_to_process = year_date.replace(day=1)
            elif dateType == "Month":
                month_date = today - relativedelta(months=int(dateUnit))
                date_to_process = month_date.replace(day=1)
            elif dateType == "Day":
                date_to_process = today - relativedelta(days=int(dateUnit))
            else:
                date_to_process = today
        else:
            config_row = date_config_rows[0]
            full_process = False
            
            date_to_process = config_row['startDate']

    return date_to_process, full_process, config_items.get("dateColumnFormat",None), config_items.get("filterColumn",None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetSourcePathLastFilesByDatesFromKQL(
    model: str,
    sourceSystem: str,
    objectName: str
) -> list:
    """
    Retrieves multiple file paths for delta processing from KQL database using Spark Kusto connector.
    Optimized for batch operations without using notebookutils.kusto.
    
    Args:
        model (str): The name of the model to filter on.
        sourceSystem (str): The name of the source system to filter on.
        objectName (str): The name of the object to filter on.
        kql_database (str): The name of the KQL database (default: "eh_modernBI_fabricFramework_01").

    Returns:
        list: A list of full paths to extracted files in the Bronze Lakehouse.

    Raises:
        Exception: If there's an error executing the KQL query or no data is found.
    """
    
    # Use global variables instead of calling GetFabricIds()
    global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID

    kustoUri = spark.conf.get(f"kustoUri")
    # The database to write the data
    database = spark.conf.get(f"kustoDatabase")

    # Check if we have configuration data
    date_to_process, full_process, _, _ = GetDateToProcessFromKQL(model, objectName, "silver")        
    
    # Construct the main query based on whether it's full process or date-filtered
    if full_process:
        # Full process - no date filtering
        main_query = f"""
        copyDataLog
        | where objectName == '{objectName}' 
            and sourceLocationName == '{sourceSystem}' 
            and model == '{model}' 
            and status == 'Succeeded'
        | order by startDate desc
        | project destinationPath, startDate
        """
    else:
        # Date-filtered process
        # Format the date for KQL (assuming startDate is datetime)
        date_threshold = date_to_process.strftime('%Y-%m-%d')
        
        main_query = f"""
        copyDataLog
        | where objectName == '{objectName}' 
            and sourceLocationName == '{sourceSystem}' 
            and model == '{model}' 
            and status == 'Succeeded'
            and todatetime(startDate) >= todatetime('{date_threshold}')
        | order by startDate desc
        | project destinationPath, startDate
        """
    
    # print(f"KQL Query: {main_query}")
    
    # Execute main query using Spark Kusto connector
    result_df = (
        spark.read.format("com.microsoft.kusto.spark.synapse.datasource") \
        .option("kustoCluster", kustoUri) \
        .option("kustoDatabase", database) \
        .option("kustoQuery", main_query)
        .option("accessToken", notebookutils.credentials.getToken('kusto'))\
        .load()
    )
    
    # Collect results
    result_rows = result_df.collect()
    
    # Check if results were returned
    if not result_rows:
        print(f"No successful extractions found for model='{model}', sourceSystem='{sourceSystem}', objectName='{objectName}'")
        return []
    
    # Construct full paths
    bronze_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Files/"
    paths = [bronze_path + row['destinationPath'] + "/" for row in result_rows]
    
    print(f"Found {len(paths)} file paths for processing")
    return paths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetLakehouseSourcePath(
    model: str, sourceSystem: str, objectName: str, extractType: str
) -> str | list:
    """
    Retrieves the source path(s) for a model and object based on the extraction type.

    This function determines whether to perform a "full" or "delta" extraction:
    - "full" extraction retrieves the latest extracted file path.
    - "delta" extraction retrieves a list of file paths filtered by processing dates.

    Args:
        model (str): The name of the model to filter on.
        objectName (str): The name of the object to filter on.
        extract_type (str): The type of extraction. Expected values: "full" or "delta".

    Returns:
        str | list:
            - For "full" extraction: A single string representing the latest file path.
            - For "delta" extraction: A list of strings representing file paths filtered by dates.

    Raises:
        ValueError: If `extract_type` is not "full" or "delta".
        SystemExit: If no extracted files are found.
    """
    # Use global variables instead of calling GetFabricIds()
    global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID

    use_yaml = spark.conf.get("UseYaml", "false").lower() == "true"
    
    if use_yaml:
        # Use YAML-based configuration
        if extractType == "full":
            paths = GetSourcePathLastFileFromKQL(
                model, sourceSystem, objectName
            )
        else:  # extractType == "delta"
            paths = GetSourcePathLastFilesByDatesFromKQL(
                model, sourceSystem, objectName
            )
    else:
        if extractType == "full":
            paths = GetSourcePathLastFile(
                WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, model, sourceSystem, objectName
            )
        else:  # extractType == "delta"
            paths = GetSourcePathLastFileByDates(
                WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, model, sourceSystem, objectName
            )
    
    if paths:
        return paths
    else:
        mssparkutils.notebook.exit("Empty Dataframe received. Exiting task...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Insert -1 and -2 in dimensions according to schema in dataframe
def UnkValues(inputDF = None):
    
    # Use global variables instead of calling GetFabricIds()
    global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID

    use_yaml = spark.conf.get("UseYaml", "false").lower() == "true"
    
    if use_yaml:
        # Use YAML-based configuration
        return UnkValuesFromYaml(inputDF)
    else:

        # unkConfigDF = spark.table(f"config.inferredMembersConfig")
        unkConfigDF = spark.read.format("delta").load(
                            f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE_ID}/Tables/config/inferredMembersConfig"
                        )

        # Check schema
        inputSchema = inputDF.schema
        initDF = spark.createDataFrame(sc.emptyRDD(), inputSchema)

        emptyDF = spark.createDataFrame([("a",)], ["aux"])

        finalDF = initDF
        unkDistinctDF = unkConfigDF.select(F.col("skValue")).distinct()

        for rows in unkDistinctDF.collect():
            unkDF = emptyDF

            # For each column in inputDF define the values to insert
            for col in inputSchema.fields:

                # Force -1/-2 for the SOURCE_ID column
                if col.name == "source_id":
                    result = (
                        unkConfigDF.filter(
                            (unkConfigDF.columnType == "IntegerType")
                            & (unkConfigDF.skValue == rows.skValue)
                        )
                        .select(unkConfigDF.columnValue)
                        .first()["columnValue"]
                    )

                # Force -1/-2 for the COD columns
                elif col.name[0:3] == "cod":
                    result = (
                        unkConfigDF.filter(
                            (unkConfigDF.columnType == "IntegerType")
                            & (unkConfigDF.skValue == rows.skValue)
                        )
                        .select(unkConfigDF.columnValue)
                        .first()["columnValue"]
                    )

                # Other columns are filled with the value of their schema
                else:
                    result = (
                        unkConfigDF.filter(
                            (unkConfigDF.columnType == col.dataType.__class__.__name__)
                            & (unkConfigDF.skValue == rows.skValue)
                        )
                        .select(unkConfigDF.columnValue)
                        .first()["columnValue"]
                    )

                unkDF = unkDF.withColumn(col.name, F.lit(result).cast(col.dataType))

            unkDF = unkDF.drop(F.col("aux"))
            finalDF = finalDF.union(unkDF)

        return finalDF

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def UnkValuesFromYaml(inputDF=None, configPath="config/inferredMembersConfig.yml"):
    """
    Insert -1 and -2 inferred members in dimensions according to schema in DataFrame using YAML configuration.
    
    This function replaces the table-based UnkValues function with a YAML configuration approach.
    It reads inferred member configurations from a YAML file and generates appropriate default values
    for each column type in the input DataFrame.
    
    Args:
        inputDF: Input DataFrame to generate inferred members for
        configPath: Path to the YAML configuration file (default: "config/inferredMembersConfig.yml")
        
    Returns:
        DataFrame: Union of original DataFrame and inferred member rows
    """
    
    
    # Use global variables for lakehouse paths
    global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID
    
    # Construct full path to YAML configuration file
    full_config_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Files/{configPath}"
    
    try:
        # Read YAML configuration file
        yaml_content = spark.read.text(full_config_path).collect()
        yaml_string = "\n".join(row.value for row in yaml_content)
        config_data = yaml.safe_load(yaml_string)
        
        print(f"Successfully loaded inferred members configuration from: {configPath}")
        
    except Exception as e:
        print(f"Error loading YAML configuration: {str(e)}")
        print("Falling back to original UnkValues function...")
        return UnkValues(inputDF)
    
    # Get DataFrame schema
    inputSchema = inputDF.schema
    initDF = spark.createDataFrame(sc.emptyRDD(), inputSchema)
    
    # Get inferred member configurations
    inferred_members = config_data.get('inferredMembers', {})
    type_mapping = config_data.get('typeMapping', {})
    
    # Generate inferred member records for -1 and -2
    finalDF = initDF
    
    for sk_value in [-1, -2]:
        # Create row data for current SK value
        row_data = {}
        
        for field in inputSchema.fields:
            column_name = field.name
            column_type = field.dataType
            
            # Map Spark data types to configuration types
            config_type = None
            if isinstance(column_type, StringType):
                config_type = "StringType"
            elif isinstance(column_type, IntegerType):
                config_type = "IntegerType"
            elif isinstance(column_type, LongType):
                config_type = "LongType"
            elif isinstance(column_type, (DoubleType, FloatType)):
                config_type = "DoubleType"
            elif isinstance(column_type, BooleanType):
                config_type = "BooleanType"
            elif isinstance(column_type, DateType):
                config_type = "DateType"
            elif isinstance(column_type, TimestampType):
                config_type = "TimestampType"
            else:
                # Default to StringType for unknown types
                config_type = "StringType"
                print(f"Warning: Unknown data type {column_type} for column {column_name}, defaulting to StringType")
            
            # Get the appropriate value for this SK and data type
            column_value = None
            
            # First try to find exact match in inferredMembers
            if config_type in inferred_members:
                for member in inferred_members[config_type]:
                    if member.get('skValue') == sk_value:
                        column_value = member.get('columnValue')
                        break
            
            # If not found, try typeMapping as fallback
            if column_value is None and config_type in type_mapping:
                if sk_value == -1:
                    column_value = type_mapping[config_type].get('default')
                elif sk_value == -2:
                    column_value = type_mapping[config_type].get('alternative')
            
            # Final fallback values if configuration is missing
            if column_value is None:
                if isinstance(column_type, StringType):
                    column_value = "Unk" if sk_value == -1 else "N/A"
                elif isinstance(column_type, (IntegerType, LongType)):
                    column_value = sk_value
                elif isinstance(column_type, (DoubleType, FloatType)):
                    column_value = float(sk_value)
                elif isinstance(column_type, BooleanType):
                    column_value = False
                elif isinstance(column_type, DateType):
                    column_value = "31/12/2999" if sk_value == -1 else "30/12/2999"
                elif isinstance(column_type, TimestampType):
                    column_value = "00:00"
                else:
                    column_value = None
            
            # Convert string dates to proper date format if needed
            if isinstance(column_type, DateType) and isinstance(column_value, str):
                try:
                    # Convert DD/MM/YYYY to date
                    from datetime import datetime
                    date_obj = datetime.strptime(column_value, "%d/%m/%Y").date()
                    column_value = date_obj
                except:
                    # If conversion fails, use a default date
                    from datetime import date
                    column_value = date(2999, 12, 31) if sk_value == -1 else date(2999, 12, 30)
            
            row_data[column_name] = column_value
        
        # Create DataFrame for this SK value
        sk_df = spark.createDataFrame([tuple(row_data[field.name] for field in inputSchema.fields)], inputSchema)
        
        # Union with final result
        finalDF = finalDF.union(sk_df)
        
        print(f"Generated inferred member record for SK value {sk_value}")
    
    print(f"Successfully generated {finalDF.count()} inferred member records using YAML configuration")
    
    return finalDF

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load Dims
# *  First load in dim table into persistent zone

# CELL ********************

def LoadDims(tempTable = None, skName = None, codColumn = None, tableFilesPath = None):
    auxTempDimDF = spark.table(tempTable)

    # Add sk column to all rows except inferred members
    tempDF = (
        auxTempDimDF.filter((F.col(codColumn) != "-1") & (F.col(codColumn) != "-2"))
        .withColumn(
            skName, F.row_number().over(Window.partitionBy().orderBy(codColumn))
        )
        .select(skName, *auxTempDimDF)
    )

    # Add sk column to inferred members
    unkDf = (
        auxTempDimDF.filter((F.col(codColumn) == "-1") | (F.col(codColumn) == "-2"))
        .withColumn(skName, F.col(codColumn))
        .select(skName, *auxTempDimDF)
    )

    # Create dataframe to load
    tempDimDF = tempDF.union(unkDf).withColumn(skName, F.col(skName).cast("int"))

    tempDimDF.write.format("delta").option("overwriteSchema", "true").mode(
        "overwrite"
    ).save(tableFilesPath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Merge Dims Type 1
# # Dims type 1 merge into persistent

# CELL ********************

# Dims type 1 merge into persistent
def MergeDims(tempTable, skColumn, codColumn, tableFilePath):
    tempDF = spark.table(tempTable)
    persistentDimDF = spark.read.format("delta").load(tableFilePath)

    # Updated temp column ctr_ins_date with persistent value and add sk column
    auxTempDimDF = (
        tempDF.join(
            persistentDimDF,
            persistentDimDF.ctr_hash_dim_key == tempDF.ctr_hash_dim_key,
            "left",
        )
        .select(
            skColumn,
            *tempDF,
            persistentDimDF.ctr_hash_validate.alias("ctr_hash_validate_persistent"),
            persistentDimDF.ctr_ins_date.alias("ctr_ins_date_persistent")
        )
        .withColumn(
            "ctr_ins_date",
            F.coalesce(F.col("ctr_ins_date_persistent"), F.col("ctr_ins_date")),
        )
        .drop("ctr_ins_date_persistent")
    )

    # Only for new and updated records
    tempDimDF = auxTempDimDF.filter(
        (auxTempDimDF.ctr_hash_validate_persistent != auxTempDimDF.ctr_hash_validate)
        & (F.col(skColumn).isNotNull())
    )
    
    # Update records with merge
    if tempDimDF.first():
        dimDT = D.DeltaTable.forPath(spark, tableFilePath)

        dimDT.alias("dim").merge(
            tempDimDF.alias("temp"),
            "temp.ctr_hash_dim_key = dim.ctr_hash_dim_key",
        ).whenMatchedUpdateAll().execute()
    else:
        print("No updated records")

    # Check for new records
    tempNewRecordsDF = auxTempDimDF.filter(
        auxTempDimDF.ctr_hash_validate_persistent.isNull()
    ).drop(skColumn)

    # Insert new recods
    if tempNewRecordsDF.first():
        maxSk = persistentDimDF.agg({skColumn: "max"}).collect()[0][0]
        newRecordsDF = tempNewRecordsDF.select(
            (maxSk + F.row_number().over(Window.orderBy(codColumn))).alias(skColumn),
            *tempNewRecordsDF
        ).drop("ctr_hash_validate_persistent")

        newRecordsDF.write.format("delta").option("overwriteSchema", "true").mode(
            "append"
        ).save(tableFilePath)

    else:
        print("No new records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dim Type 2 Delta or Full Load

# CELL ********************

# Determines the processing date and Delta status for loading dimension Type 2 data:
#   1. Retrieves the processing date from the admin process dates config.
#   2. Checks if the persistent table exists and determines the minimum CTR_INSERT_DATE from its history.
#   3. Sets Delta to True if the processing date is greater than or equal to the minimum date in the persistent table.
#   4. Returns the processing date and Delta status.
# Input:
#   model: Model name.
#   tempTable: Temp table name.
#   persistentDatabase: Persistent DB name.
#   persistentTable: Persistent table name.
#   skName: Surrogate key column name.
# Output:
#   minDateToProcess: processing date
#   Delta: Boolean value indicating delta status
def DimType2DeltaFullLoad(model = None, tempTable = None, persistentDatabase = None, persistentTable = None, skName = None):

    # Use global variables instead of calling GetFabricIds()
    global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID

    # get date to process
    minDateToProcess = GetDatesToProcess(model, tempTable, "DimType2")
    # (
    #     spark.read.format("delta").load(
    #                     f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE_ID}/Tables/config/processDatesConfig"
    #                 )
    #     .filter(
    #         (F.col("modelName") == model)
    #         & (F.col("tableName") == tempTable)
    #         & (F.col("scope") == "DimType2")
    #     )
    #     .select("date")
    #     .rdd.flatMap(lambda x: x)
    #     .collect()[0]
    # )

    print("Temp " + str(minDateToProcess))
    
    # if persistent table exists, check Delta state
    if ExistsDeltaTable(persistentDatabase, persistentTable):

        dimTable = persistentDatabase + "." + persistentTable

        # Get info from Persistent table
        persistentDimDF = spark.table(dimTable).drop(skName)

        # Get min CTR_INSERT_DATE from persistent table
        minDateHistoryPersistent = (
            persistentDimDF.agg({"ctr_start_date": "min"})
            .select(F.date_format("min(ctr_start_date)", "yyyyMMdd"))
            .collect()[0][0]
        )

        print("Data in persistent from " + minDateHistoryPersistent)

        if int(minDateToProcess) >= int(minDateHistoryPersistent):
            Delta = True

        else:
            Delta = False

    # if persistent tables does not exists, set Delta to False
    else:
        Delta = False

    print("Delta: ", Delta)
    return minDateToProcess, Delta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Merge Historical Dim

# CELL ********************

# #Function to merge the Historical Dimensions with new records and/or type 2 columns that changed
# # 1. Retrieves distinct snapshot dates from the stage table.
# # 2. Iterates over each snapshot date:
# #    a. Filters the stage data for the current snapshot date.
# #    b. For the first load:
# #       - If the destination table doesn't exist, loads the first snapshot date and inferred members.
# #    c. For subsequent loads:
# #       - Retrieves the persistent table.
# #       - Inserts new records from the stage data.
# #       - Updates historical data.
# #       - Updates non-historical data.
# #    d. For Type 6 Slowly Changing Dimensions:
# #       - Updates historical Type 3 data.
# # Input:
# #  stageTable: Stage table name.
# #  tableDest: Destination table name.
# #  skName: Surrogate key column name.
# #  codColumn: Column name for inferred members.
# #  historicalType2Columns: List of historical Type 2 columns.
# #  scdType: Type of Slowly Changing Dimension (default: 'Type2').
# #  historicalType3Columns: List of historical Type 3 columns.
# def mergeHistoricalDim_OLD(stageTable=None, tableDest=None, skName=None, codColumn=None, historicalType2Columns=[], scdType = 'Type2', historicalType3Columns = []):
#     stageDF = spark.table(stageTable).drop('ctr_hash_validate')
#     listSnapshotDt = stageDF\
#                 .select('ctr_date_key')\
#                 .distinct()\
#                 .filter(F.col('ctr_date_key') > 0)\
#                 .orderBy("ctr_date_key").collect()
#     
#     # print(listSnapshotDt)
#     
#     for dateItem in listSnapshotDt:
#         date = dateItem[0]
#         print('Date to process: ' + str(date))
#         stageDtDF = stageDF.filter(F.col("ctr_date_key") == date)
#         
#         # just for the first load 
#         if not D.DeltaTable.isDeltaTable(spark,tableDest):
#             print('Table does not exists.')
#             # define DF for first load -> include first date + inferred members
#             firstLoadDF = stageDF.filter((F.col("ctr_date_key") == date) |\
#                                    (F.col("ctr_date_key") == '-1') |\
#                                    (F.col("ctr_date_key") == '-2'))
#             # do first load
#             LoadDimsType2 (firstLoadDF, skName, codColumn, tableDest, int(date))
#             
#         else:
#             persistentDF = D.DeltaTable.forPath(spark,tableDest).toDF()
#             # insert new records      
#             InsertNewRecords(tableDest, stageDtDF, persistentDF, int(date), skName, codColumn)
#             # update records from historical columns
#             UpdateHistoricalData(stageDtDF, persistentDF, tableDest, int(date))
#             # update records from non historical columns
#             UpdateNonHistoricalData(stageDtDF, persistentDF, tableDest, int(date), skName, historicalType2Columns)
#             
#             if scdType == 'Type6':
#                 UpdateHistoricalType3Data(stageDtDF, persistentDF, stageTable, tableDest, int(date), skName, historicalType3Columns)


# Performs the first Load in a Type 2 dimension:
    # 1. Adds a surrogate key (SK) column to all rows except inferred members.
    # 2. Adds a SK column to inferred members if SK name is provided.
    # 3. Creates dataframes for both SK-assigned and inferred member rows.
    # 4. Combines both dataframes into a single dataframe.
    # 5. Creates a database if it doesn't exist.
    # 6. Writes the dataframe to a Delta table if tableFilesPath is provided.
    # 7. Creates or replaces a table in the destination database if tableFilesPath is not provided.
# Params:
# - stageDimDF: Dataframe containing stage dimension data.
# - skName: Surrogate key column name.
# - codColumn: Column name to check for inferred members and used to generate the surrogate key order.
# - tableDest: Destination table name.
# - dateToProcess: Date for start date of the records.
def LoadDimsType2 (stageDimDF=None, skName=None, codColumn=None, tableDest=None, dateToProcess=None):

    if (skName):
        # Add sk column to all rows except inferred members
        stageDF = stageDimDF\
              .filter((F.col(codColumn) != '-1') & (F.col(codColumn) != '-2'))\
              .withColumn(skName, F.row_number().over(Window.partitionBy().orderBy(codColumn)))\
              .select(skName, *stageDimDF)\
              .drop('ctr_date_key', 'ctr_hash_validate')\
              .withColumn(skName, F.col(skName).cast('int'))\
              .withColumn('ctr_flag_active', F.lit(1))\
              .withColumn('ctr_start_date', F.lit(dateToProcess))\
              .withColumn('ctr_end_date', F.lit(29991231))
    
        # Add sk column to inferred members  
        unkDf = stageDimDF\
              .filter((F.col(codColumn) == '-1') | (F.col(codColumn) == '-2'))\
              .withColumn(skName, F.col(codColumn))\
              .select(skName, *stageDimDF)\
              .drop('ctr_date_key', 'ctr_hash_validate')\
              .withColumn(skName, F.col(skName).cast('int'))\
              .withColumn('ctr_flag_active', F.lit(1))\
              .withColumn('ctr_start_date', F.lit(19000101))\
              .withColumn('ctr_end_date', F.lit(29991231))

    else:
        # Add sk column to all rows except inferred members
        stageDF = stageDimDF\
              .filter((F.col(codColumn) != '-1') & (F.col(codColumn) != '-2'))\
              .select(*stageDimDF)\
              .drop('ctr_date_key', 'ctr_hash_validate')\
              .withColumn('ctr_flag_active', F.lit(1))\
              .withColumn('ctr_start_date', F.lit(dateToProcess))\
              .withColumn('ctr_end_date', F.lit(29991231))
    
        # Add sk column to inferred members  
        unkDf = stageDimDF\
              .filter((F.col(codColumn) == '-1') | (F.col(codColumn) == '-2'))\
              .select(*stageDimDF)\
              .drop('ctr_date_key', 'ctr_hash_validate')\
              .withColumn('ctr_flag_active', F.lit(1))\
              .withColumn('ctr_start_date', F.lit(19000101))\
              .withColumn('ctr_end_date', F.lit(29991231))
    
    # Create dataframe to load  
    tempDimDF = stageDF.union(unkDf)
    tempDimDF.write.format('delta').option('overwriteSchema', 'true').mode('overwrite').save(tableDest)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Merge Historical Common Functions

# CELL ********************

# update/insert records from historical columns
# tableType parameter is optional for dimensions and required for the Historical Facts. If the parameter is not passed the default value is 'Dim'
# HASH_KEY_COLUMN parameter is optional for dimensions and required for the Historical Facts. If the parameter is not passed the default value is 'HASH_DIM_KEY'
# 1. Calculates start and end dates to close updated records based on the ExecutionFrequency.
# 2. Renames the hash key column to 'ctr_hash_key' in both stage and persistent dataframes.
# 3. Validates which records were updated based on the hash key and history columns.
# 4. Executes a merge operation only for active records in the persistent Delta table.
# 5. Sets the end date for updated records to close them.
# Params:
# - stageDF: Stage dataframe containing updated historical data.
# - persistentDF: Persistent dataframe containing existing historical data.
# - persistentTable: Name of the persistent Delta table.
# - startDate: Start date for the updated records.
# - tableType: Type of table ('Dim' for dimension, 'Fact' for fact) (default: 'Dim').
# - HASH_KEY_COLUMN: Name of the hash key column (default: 'ctr_hash_dim_key' for dimensions, 'ctr_hash_fact_key' for facts).
# - ExecutionFrequency: Frequency of execution ('H' for hourly, None for daily) (default: None).
def UpdateHistoricalData(stageDF, persistentDF, persistentTable, startDate, tableType = 'Dim', HASH_KEY_COLUMN = 'ctr_hash_dim_key', ExecutionFrequency = None):
    
    if ExecutionFrequency == 'H':
        # calculate start/end time to close updated records  
        start_dt = Dtime.datetime.strptime(str(startDate), '%Y-%m-%d %H:%M:%S.%f')
        end_dt = str(start_dt - Dtime.timedelta(seconds = 1))
    else:
        # calculate start/end time to close updated records  
        start_dt = Dtime.datetime.strptime(str(int(startDate)), '%Y%m%d')
        end_dt = str(start_dt - Dtime.timedelta(days = 1)).split(" ")[0].replace("-", "")
    
    stageDF = stageDF.withColumnRenamed(HASH_KEY_COLUMN, "ctr_hash_key")
    persistentDF = persistentDF.withColumnRenamed(HASH_KEY_COLUMN, "ctr_hash_key")

    stageDF = stageDF.alias("stage")
    persistentDF = persistentDF.alias("persistentDF")    

    # get updated records to insert
    updateHistoryDF = stageDF\
        .join(persistentDF, (stageDF["ctr_hash_key"] == persistentDF["ctr_hash_key"]) &
            (stageDF["ctr_hash_validate_history"] != persistentDF["ctr_hash_validate_history"]) &
            (persistentDF["ctr_flag_active"] == 1), 'inner')\
        .select([stageDF[col] for col in stageDF.columns if col != 'ctr_date_key'])

    # # validate which records were updated (history columns)  
    # updateHistoryDF = stageDF\
    #             .join(persistentDF, (stageDF.ctr_hash_key == persistentDF.ctr_hash_key) &\
    #                   (stageDF.ctr_hash_validate_history != persistentDF.ctr_hash_validate_history) &\
    #                   (persistentDF.ctr_flag_active == 1), 'inner')\
    #             .select(*stageDF).drop('ctr_date_key')
    
    updateHistoryDF = updateHistoryDF.withColumnRenamed("ctr_hash_key", HASH_KEY_COLUMN)
    updatedHistoricRecords = updateHistoryDF.count()
    
    
    mergeCondition = ''
    if tableType == 'Dim': 
        mergeCondition = "persistent.ctr_hash_dim_key = updateHistory.ctr_hash_dim_key AND persistent.ctr_flag_active = 1 AND updateHistory.ctr_hash_validate_history != persistent.ctr_hash_validate_history"
    else: 
        mergeCondition = "persistent.ctr_hash_fact_key = updateHistory.ctr_hash_fact_key AND persistent.ctr_flag_active = 1 AND updateHistory.ctr_hash_validate_history != persistent.ctr_hash_validate_history"
        
    # execute merge only for active records
    persistentDeltaTable = D.DeltaTable.forPath(spark, persistentTable)

    windowSpec = Window.partitionBy("ctr_hash_dim_key").orderBy(F.desc("ctr_last_operation_date"))

    updateHistoryDF = (
        updateHistoryDF.withColumn("rn", F.row_number().over(windowSpec))
        .filter("rn = 1")
        .drop("rn")
    )

    persistentDeltaTable.alias("persistent")\
                .merge(source = updateHistoryDF.alias("updateHistory"),
                condition = mergeCondition)\
                .whenMatchedUpdate(set =\
                        {"ctr_flag_active": '0',\
                         "ctr_last_operation_date": updateHistoryDF.ctr_last_operation_date,\
                         "ctr_end_date": f"'{end_dt}'"}).execute()

    # persistentDeltaTable.alias("persistent")\
    #             .merge(source = updateHistoryDF.alias("updateHistory"),
    #             condition = mergeCondition)\
    #             .whenMatchedUpdate(set =\
    #                     {"ctr_flag_active": '0',\
    #                      "ctr_last_operation_date": updateHistoryDF.ctr_last_operation_date,\
    #                      "ctr_end_date": f"'{end_dt}'"}).execute()
    print(startDate, ': ', updatedHistoricRecords, ' history updated records.')

# InsertNewRecords function inserts new records into a dimension or fact table:
# tableType paramter is optional for dimensions and required for the Historical Facts. If the parameter is not passed the default value is 'Dim'
# partitionColumnsList and partitionValue are required parameters only for Historical Facts. If the parameters are not passed the default values are empty and None respectively
# ctr_hash_key_COLUMN parameter is optional for dimensions and required for the Historical Facts. If the parameter is not passed the default value is 'ctr_hash_dim_key'
# 1. Determines the table type based on the existence of skName (for dimensions) or its absence (for facts).
# 2. Renames the hash key column to 'ctr_hash_key' in both stage and persistent dataframes.
# 3. Retrieves new records to insert by left joining the stage and persistent dataframes and filtering records with null hash keys in the persistent dataframe.
# 4. Retrieves updated records to insert by joining the stage and persistent dataframes based on the hash key and history columns.
# 5. Prepares dataframes for insertion, including flags, start and end dates, and sequence keys for dimensions.
# 6. Inserts new records into the table using Delta Lake functionality or SQL INSERT INTO statements for unity catalog tables.
# Params:
# - tableDest: Name of the destination table.
# - stageDF: Stage dataframe containing new records.
# - persistentDF: Persistent dataframe containing existing records.
# - startDate: Start date for the new records.
# - skName: Name of the sequence key column (for dimensions).
# - codColumn: Name of the code column.
# - TableType: Type of table ("Dim" for dimension, "Fact" for fact) (default: "Dim").
# - partitionColumnsList: List of partition columns.
# - partitionValue: Partition value.
# - HASH_KEY_COLUMN: Name of the hash key column (default: "ctr_hash_dim_key").
# - ExecutionFrequency: Frequency of execution ("H" for hourly, None for daily) (default: None).
def InsertNewRecords(tableDest=None, stageDF=None, persistentDF=None, startDate=None, skName=None, codColumn=None, TableType = 'Dim', partitionColumnsList = [], partitionValue = None, HASH_KEY_COLUMN = 'ctr_hash_dim_key', ExecutionFrequency = None):

    if skName == None:
        TableType = 'Fact'
    else:
        TableType = 'Dim'
    
    stageDF = stageDF.withColumnRenamed(HASH_KEY_COLUMN, "ctr_hash_key")
    persistentDF = persistentDF.withColumnRenamed(HASH_KEY_COLUMN, "ctr_hash_key")
    
    # get new recods to insert
    insertNewRecordsDF = stageDF\
                .join(persistentDF, stageDF.ctr_hash_key == persistentDF.ctr_hash_key, 'left')\
                .filter(persistentDF.ctr_hash_key.isNull())\
                .select(*stageDF).drop('ctr_date_key')
    
    # get updated records to insert   
    updateHistoryDF =  stageDF\
                .join(persistentDF, (stageDF.ctr_hash_key == persistentDF.ctr_hash_key) &\
                      (stageDF.ctr_hash_validate_history != persistentDF.ctr_hash_validate_history) &\
                      (persistentDF.ctr_flag_active == 1), 'inner')\
                .select(*stageDF).drop('ctr_date_key')
    
    insertNewRecordsDF = insertNewRecordsDF.withColumnRenamed("ctr_hash_key", HASH_KEY_COLUMN)
    updateHistoryDF = updateHistoryDF.withColumnRenamed("ctr_hash_key", HASH_KEY_COLUMN)
    
    persistentDF = persistentDF.withColumnRenamed("ctr_hash_key", HASH_KEY_COLUMN)
    
    if TableType == 'Dim':
        # get all new records to insert - add flag, start/end dates  
        insertRecordsDF = insertNewRecordsDF.union(updateHistoryDF)\
                                          .withColumn('ctr_flag_active', F.lit(1))\
                                          .withColumn('ctr_start_date', F.lit(int(startDate)))\
                                          .withColumn('ctr_end_date', F.lit(29991231))\
                                          .withColumn(skName, F.row_number().over(Window.partitionBy().orderBy(codColumn)))

        # get max SK from persistent  
        oldMaxSk = persistentDF.agg({skName: "max"}).collect()[0][0]
        # add sk columns to new records  
        insertRecordsSkDF = insertRecordsDF.withColumn(skName, F.col(skName) + F.lit(oldMaxSk))
        # order columns to execute merge, according to persistent columns order  
        persistentColumns = persistentDF.columns  
        insertRecordsOrderColumnsDF = insertRecordsSkDF.select(*persistentColumns)
        insertedRecords = insertRecordsOrderColumnsDF.count()
        # insert new records into table

        insertRecordsOrderColumnsDF.write.format('delta').mode('append').save(tableDest)
    
    #TableType == 'Fact'
    else: 
        
        if ExecutionFrequency == 'H':
            # get all new records to insert - add flag, start/end dates  
            insertRecordsDF = insertNewRecordsDF.union(updateHistoryDF)\
                                              .withColumn('ctr_flag_active', F.lit(1))\
                                              .withColumn('ctr_start_date', F.lit(startDate).cast("timestamp"))\
                                              .withColumn('ctr_end_date', F.lit('2999-12-31 23:59:59.999').cast("timestamp"))
        else:
            # get all new records to insert - add flag, start/end dates  
            insertRecordsDF = insertNewRecordsDF.union(updateHistoryDF)\
                                          .withColumn('ctr_flag_active', F.lit(1))\
                                          .withColumn('ctr_start_date', F.lit(int(startDate)))\
                                          .withColumn('ctr_end_date', F.lit(29991231))
            
        # order columns to execute merge, according to persistent columns order  
        persistentColumns = persistentDF.columns
        insertRecordsOrderColumnsDF = insertRecordsDF.select(*persistentColumns)
        insertedRecords = insertRecordsOrderColumnsDF.count()
        
        insertRecordsDF.write\
            .format("delta")\
            .option("overwriteSchema", True)\
            .option("replaceWhere", "{0} IN ({1})".format(partitionColumnsList[0], partitionValue)) \
            .mode("append")\
            .partitionBy(partitionColumnsList)\
            .save(tableDest)
            
    print(startDate, ': ', insertedRecords, ' inserted records.')


# updateNonHistoricalData function updates non-historical data in the persistent table using Delta Lake merge functionality  - This function is only used in dimensions type 2:
# 1. Determines the list of columns to be updated based on the presence of historicalType2Columns and skName.
# 2. Drops the columns to be updated from the stage dataframe to avoid redundant updates.
# 3. Joins the filtered stage dataframe with the persistent dataframe based on the hash key and active flag.
# 4. Selects necessary columns from both dataframes for the merge operation.
# 5. Orders columns in the update dataframe according to the persistent columns order.
# 6. Executes a merge operation using Delta Lake, updating non-historical records in the persistent table.
# Params:
# - stageDF: Stage dataframe containing new records.
# - persistentDF: Persistent dataframe containing existing records.
# - persistentTable: Name of the persistent table.
# - startDate: Start date for the records.
# - skName: Name of the sequence key column.
# - historicalType2Columns: List of historical columns.
def UpdateNonHistoricalData(stageDF, persistentDF, persistentTable, startDate, skName, historicalType2Columns):
    
    if(len(historicalType2Columns) == 0):
        print(startDate, ': ', '0 non-history updated records.')
    if skName == None:
        listPersistentColumns = ['ctr_ins_date', 'ctr_hash_validate_history', 'ctr_flag_active', 'ctr_start_date', 'ctr_end_date']
    else:
        listPersistentColumns = [skName, 'ctr_ins_date', 'ctr_hash_validate_history', 'ctr_flag_active', 'ctr_start_date', 'ctr_end_date']
            
    listPersistentColumns.extend(historicalType2Columns)
    tempStageDF = stageDF.drop(*listPersistentColumns)

    # get non-history records to update. select SK, technical columns and history columns from persistent to not be updated by the stage  
    updateNonHistoryDF = tempStageDF\
                    .join(persistentDF, (tempStageDF.ctr_hash_dim_key == persistentDF.ctr_hash_dim_key) &\
                          (persistentDF.ctr_flag_active == 1) &\
                          (tempStageDF.ctr_hash_validate_non_history != persistentDF.ctr_hash_validate_non_history), 'inner')\
                    .select(*tempStageDF,*listPersistentColumns).drop(tempStageDF.ctr_date_key)

    # order columns to execute merge, according to persistent columns order  
    persistentColumns = persistentDF.columns
    updateNonHistoryOrderColumnsDF = updateNonHistoryDF.select(*persistentColumns)
    updateRecords = updateNonHistoryOrderColumnsDF.count()

    # merge  
    persistentDeltaTable = D.DeltaTable.forPath(spark, persistentTable)

    persistentDeltaTable.alias("persistent")\
               .merge(updateNonHistoryOrderColumnsDF.alias("updateNonHistoryDF"), 
                      condition = "persistent.ctr_hash_dim_key = updateNonHistoryDF.ctr_hash_dim_key")\
               .whenMatchedUpdateAll()\
               .execute()

    print(startDate, ': ', updateRecords, ' non-history updated records.')


# updateHistoricalType3Data function updates historical type 3 "CURRENT" data with the most recent values in the persistent table using Delta Lake merge functionality - This function is only used in dimensions type 6:
# 1. Constructs a list of new columns based on the historicalType3Columns.
# 2. Joins the stage dataframe with the persistent dataframe based on the hash key and type 3 validation hash.
# 3. Selects necessary columns from the joined dataframe for the update operation.
# 4. Creates a temporary view from the update dataframe for SQL merge operation.
# 5. If no records need updating, prints a message indicating no updates.
# 6. Constructs an update string for merging, including setting current values to historical columns and updating the last operation date.
# 7. Executes a merge operation using SQL merge statement to update historical type 3 records in the persistent table.

# Params:
# - stageDF: Stage dataframe containing new records.
# - persistentDF: Persistent dataframe containing existing records.
# - stageTable: Name of the stage table.
# - persistentTable: Name of the persistent table.
# - startDate: Start date for the records.
# - skName: Name of the sequence key column.
# - historicalType3Columns: List of historical type 3 columns.
def UpdateHistoricalType3Data(stageDF, persistentDF, stageTable, persistentTable, startDate, skName, historicalType3Columns):

    newType3Columns = []
    for i in historicalType3Columns:
        newType3Columns.append(i)
        newType3Columns.append(i+"_current")
    
    # get type3-history records to update
    updateHistoryType3DF = stageDF\
                    .join(persistentDF, (stageDF.ctr_ctr_hash_dim_key == persistentDF.ctr_hash_dim_key) &\
                          (stageDF.ctr_hash_validate_type3 != persistentDF.ctr_hash_validate_type3), 'inner')\
                    .select(*stageDF,skName).drop(stageDF.DATE_KEY)
    
    updateRecords = updateHistoryType3DF.count()
    
    updateHistoryType3DF.createOrReplaceTempView(stageTable.replace(".","_"))
    
    if updateRecords == 0:
        print(startDate, ': ', '0 type3-history updated records.')
    else:
        updateCols = ", ".join(f"{str(col)}_current = s.{str(col)}" for col in historicalType3Columns)+", ctr_last_operation_date = s.ctr_last_operation_date"
        
        destDF = D.DeltaTable.forPath(spark,persistentTable).toDF()
        destDF.alias('target')\
            .merge(updateHistoryType3DF.alias('updates'),
            f'target.{skName} == updates.{skName}'
            )\
            .whenMatchedUpdate(set =
                {updateCols}\
            )\
            .execute()

        # #merge 
        # spark.sql(f"""
        # MERGE INTO {persistentTable} p
        # USING {stageTable.replace(".","_")} s
        # ON p.{skName} == s.{skName}
        # WHEN MATCHED THEN UPDATE SET {updateCols}""")
        
        print(startDate, ': ', updateRecords, ' type3-history updated records.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PERFORMANCE OPTIMIZED VERSION
# High-Performance Merge Historical Dimension Function
# 
# This optimized version of mergeHistoricalDim eliminates the date-by-date loop that causes performance issues.
# Instead, it processes all snapshot dates in batch operations using window functions and advanced Delta Lake features.
#
# Performance improvements:
# 1. Eliminates the loop over snapshot dates (major bottleneck)
# 2. Uses window functions to handle temporal ordering across all dates
# 3. Batch processes all changes in single operations
# 4. Leverages Delta Lake's MERGE capabilities more efficiently
# 5. Reduces the number of Delta table reads/writes from N (number of dates) to 1
#
# Functionality maintained:
# - Type 2 and Type 6 SCD support
# - Proper temporal ordering and history tracking
# - Inferred members handling
# - All technical columns and flags

def MergeHistoricalDim(stageTable=None, tableDest=None, skName=None, codColumn=None, historicalType2Columns=[], scdType='Type2', historicalType3Columns=[]):
    """
    High-performance version of mergeHistoricalDim that processes all snapshot dates in batch operations.
    
    Args:
        stageTable: Stage table name
        tableDest: Destination table name  
        skName: Surrogate key column name
        codColumn: Column name for inferred members
        historicalType2Columns: List of historical Type 2 columns
        scdType: Type of Slowly Changing Dimension ('Type2' or 'Type6')
        historicalType3Columns: List of historical Type 3 columns
    """
    
    print("Starting optimized historical dimension merge...")
    
    # Load stage data once
    stageDF = spark.table(stageTable).drop('ctr_hash_validate')
    
    # Get all snapshot dates in order
    snapshotDates = stageDF\
        .select('ctr_date_key')\
        .distinct()\
        .filter(F.col('ctr_date_key') > 0)\
        .orderBy("ctr_date_key")\
        .collect()
    
    if len(snapshotDates) == 0:
        print("No snapshot dates found to process.")
        return
    
    dateList = [row[0] for row in snapshotDates]
    print(f"Processing {len(dateList)} snapshot dates: {dateList}")
    
    # Check if this is the first load
    if not D.DeltaTable.isDeltaTable(spark, tableDest):
        print('Table does not exist. Performing first load...')
        _performFirstLoadOptimized(stageDF, tableDest, skName, codColumn, dateList[0])
        # Remove the first date from processing list since it's already loaded
        dateList = dateList[1:]
        
    if len(dateList) == 0:
        print("First load completed. No additional dates to process.")
        return
    
    # For subsequent dates, process all at once using batch operations
    print(f"Performing batch processing for remaining {len(dateList)} dates...")
    
    # Load persistent table once
    persistentDF = D.DeltaTable.forPath(spark, tableDest).toDF()
    
    # Prepare stage data with temporal ordering for batch processing
    stageWithOrder = _prepareStageDataWithTemporalOrder(stageDF, dateList)
    
    # Batch process all changes
    _batchProcessHistoricalChanges(
        stageWithOrder, 
        persistentDF, 
        tableDest, 
        skName, 
        codColumn, 
        historicalType2Columns,
        scdType,
        historicalType3Columns,
        dateList
    )
    
    print("Optimized historical dimension merge completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _performFirstLoadOptimized(stageDF, tableDest, skName, codColumn, firstDate):
    """Optimized first load that includes first date + inferred members"""
    
    print(f'Performing first load for date: {firstDate}')
    
    # Include first date + inferred members
    firstLoadDF = stageDF.filter(
        (F.col("ctr_date_key") == firstDate) |
        (F.col("ctr_date_key") == '-1') |
        (F.col("ctr_date_key") == '-2')
    )
    
    # Use existing LoadDimsType2 function
    LoadDimsType2(firstLoadDF, skName, codColumn, tableDest, int(firstDate))
    print(f'First load completed for date: {firstDate}')


def _prepareStageDataWithTemporalOrder(stageDF, dateList):
    """
    Prepare stage data with temporal ordering information for batch processing.
    This adds row numbers and ordering to handle temporal sequencing across all dates.
    """
    
    # Filter stage data to only include dates we're processing
    filteredStageDF = stageDF.filter(F.col("ctr_date_key").isin(dateList))
    
    # Add temporal ordering within each entity across all dates
    # This helps us understand the sequence of changes for each record
    windowSpec = Window.partitionBy("ctr_hash_dim_key").orderBy("ctr_date_key")
    windowSpecDesc = Window.partitionBy("ctr_hash_dim_key").orderBy(F.desc("ctr_date_key"))
    
    stageWithOrder = filteredStageDF.withColumn(
        "temporal_order", 
        F.row_number().over(windowSpec)
    ).withColumn(
        "is_last_change",
        F.when(
            F.row_number().over(windowSpecDesc) == 1,
            True
        ).otherwise(False)
    )
    
    return stageWithOrder

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _batchProcessHistoricalChanges(stageWithOrder, persistentDF, tableDest, skName, codColumn, historicalType2Columns, scdType, historicalType3Columns, dateList):
    """
    Batch process all historical changes across all dates in optimized operations.
    This is the core optimization that replaces the date-by-date loop.
    """
    
    print("Starting batch processing of historical changes...")
    
    # Cache the persistent DataFrame to ensure consistent state
    persistentDF = persistentDF.cache()
    persistentCount = persistentDF.count()  # Force evaluation of cache
    print(f"Cached persistent table with {persistentCount} records")
    
    # Step 1: Identify all changes that need to be processed
    changesDF = _identifyAllChanges(stageWithOrder, persistentDF, skName)
    
    # CACHE THE DATAFRAME to prevent recomputation
    changesDF = changesDF.cache()
    
    # Trigger evaluation to materialize the cache
    changeCount = changesDF.count()
    if changesDF.count() == 0:
        print("No changes detected across all dates.")
        changesDF.unpersist()
        persistentDF.unpersist()
        return
    
    print(f"Found {changeCount} changes to process")

    # Debug: Show change distribution and force evaluation
    changeCounts = changesDF.groupBy("change_type").count().collect()
    print(f"Change type distribution: {[(row.change_type, row['count']) for row in changeCounts]}")
    
    # FORCE COMPLETE MATERIALIZATION by writing to a temporary location
    # This ensures changesDF is completely independent of any Delta table references
    temp_path = f"{tableDest}_temp_changes"
    print(f"Materializing changes to temporary location: {temp_path}")
    
    try:
        # Write changesDF to temporary Delta table to fully materialize it
        changesDF.write.format("delta").mode("overwrite").save(temp_path)
        
        # Read it back as a completely independent DataFrame
        materializedChangesDF = spark.read.format("delta").load(temp_path).cache()
        materializedChangeCount = materializedChangesDF.count()
        print(f"Materialized {materializedChangeCount} changes successfully")
        
        # Clean up the original changesDF cache since we have a materialized version
        changesDF.unpersist()
        
        # Use the materialized version for all subsequent operations
        changesDF = materializedChangesDF
        
    except Exception as e:
        print(f"Error materializing changes: {str(e)}")
        # Fall back to original approach if materialization fails
        pass

    # Step 2: Process record closures (Type 2 changes) in batch
    _batchProcessRecordClosures(changesDF, persistentDF, tableDest, historicalType2Columns)
    
    # Step 3: Process new record insertions in batch  
    _batchProcessNewRecords(changesDF, persistentDF, tableDest, skName, codColumn, dateList)
    
    # Step 4: Process non-historical updates in batch
    # _batchProcessNonHistoricalUpdates(changesDF, persistentDF, tableDest, skName, historicalType2Columns, dateList)
    
    # Step 5: Process Type 6 changes if applicable
    if scdType == 'Type6':
        _batchProcessType6Changes(changesDF, persistentDF, tableDest, skName, historicalType3Columns, dateList)
    
    # Clean up caches
    changesDF.unpersist()
    persistentDF.unpersist()

    # Clean up temporary materialization
    try:
        # Remove the temporary files using notebookutils
        notebookutils.fs.rm(temp_path, True)
        print(f"Cleaned up temporary materialization at {temp_path}")
    except Exception as cleanup_error:
        print(f"Warning: Could not clean up temporary files: {str(cleanup_error)}")
        # Don't fail the entire process if cleanup fails
        pass

    print("Batch processing of historical changes completed.")


def _identifyAllChanges(stageWithOrder, persistentDF, skName):
    """
    Identify all records that have changes across all dates.
    This creates a comprehensive view of what needs to be processed.
    """
    
    # Create completely separate DataFrames to avoid any column ambiguity
    # First, create a clean stage DataFrame with renamed hash key
    stageClean = stageWithOrder.withColumnRenamed("ctr_hash_dim_key", "stage_hash_key")
    
    # Create a minimal persistent DataFrame with only the columns we need for comparison
    persistentSelectCols = [
        F.col("ctr_hash_dim_key").alias("persistent_hash_key"),
        F.col("ctr_hash_validate_history").alias("persistent_hash_validate_history"), 
        F.col("ctr_hash_validate_non_history").alias("persistent_hash_validate_non_history"), 
        "ctr_flag_active"
    ]
    
    # Add surrogate key if needed
    if skName and skName in persistentDF.columns:
        persistentSelectCols.append(F.col(skName).alias("persistent_sk"))
    
    persistentClean = persistentDF.select(*persistentSelectCols)
    
    # Perform the join to identify changes
    changesDF = stageClean.join(
        persistentClean,
        stageClean.stage_hash_key == persistentClean.persistent_hash_key,
        "left"
    ).withColumn(
        "change_type",
        F.when(persistentClean.persistent_hash_key.isNull(), "NEW_RECORD")
        .when(
            (persistentClean.persistent_hash_key.isNotNull()) &
            (stageClean.ctr_hash_validate_history != persistentClean.persistent_hash_validate_history) &
            (persistentClean.ctr_flag_active == 1),
            "HISTORICAL_CHANGE"
        )
        .when(
            (persistentClean.persistent_hash_key.isNotNull()) &
            (stageClean.ctr_hash_validate_non_history != persistentClean.persistent_hash_validate_non_history) &
            (persistentClean.ctr_flag_active == 1),
            "NON_HISTORICAL_CHANGE"
        )
        .otherwise("NO_CHANGE")
    ).filter(F.col("change_type") != "NO_CHANGE")
    
    # Now create a clean result with only the stage columns plus change metadata
    # Get all original stage columns (excluding the renamed hash key)
    stageColumns = [col for col in stageClean.columns 
                   if col not in ["stage_hash_key"]]
    
    # Select stage columns plus the change information we need
    cleanChangesDF = changesDF.select(
        F.col("stage_hash_key"),  # Keep the renamed hash key for tracking
        *stageColumns,  # All original stage columns
        F.col("persistent_hash_key"),  # For updates/closures
        F.col("change_type")  # The type of change identified
    )
    
    return cleanChangesDF

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _batchProcessRecordClosures(changesDF, persistentDF, tableDest, historicalType2Columns):
    """
    Batch process all record closures for Type 2 changes.
    This closes old records that have been superseded by new versions.
    """
    
    print("Processing record closures in batch...")
    
    # Get records that need to be closed (historical changes)
    closureRecords = changesDF.filter(F.col("change_type") == "HISTORICAL_CHANGE")
    
    if closureRecords.count() == 0:
        print("No records to close.")
        return
    
    # Calculate end dates for each closure based on the new effective date
    closureWithEndDates = closureRecords.withColumn(
        "calculated_end_date",
        F.when(
            F.col("ctr_date_key").cast("int") > 0,
            (F.col("ctr_date_key").cast("int") - 1).cast("string")
        ).otherwise("29991231")
    )
    
    # Group by hash key and get the latest closure information per entity
    windowSpec = Window.partitionBy("persistent_hash_key").orderBy(F.desc("ctr_date_key"))
    
    latestClosures = closureWithEndDates.withColumn(
        "rn", F.row_number().over(windowSpec)
    ).filter(F.col("rn") == 1).drop("rn")
    
    # Execute batch closure using Delta merge
    persistentDeltaTable = D.DeltaTable.forPath(spark, tableDest)
    
    closureUpdates = latestClosures.select(
        F.col("persistent_hash_key").alias("ctr_hash_dim_key"),
        F.col("calculated_end_date").alias("new_end_date"),
        F.current_timestamp().alias("new_last_operation_date")
    ).distinct()

    closureUpdatesCount = closureUpdates.count()
    
    if closureUpdatesCount > 0:
        persistentDeltaTable.alias("persistent").merge(
            closureUpdates.alias("closures"),
            "persistent.ctr_hash_dim_key = closures.ctr_hash_dim_key AND persistent.ctr_flag_active = 1"
        ).whenMatchedUpdate(set={
            "ctr_flag_active": "0",
            "ctr_end_date": "closures.new_end_date", 
            "ctr_last_operation_date": "closures.new_last_operation_date"
        }).execute()
        
        print(f"Closed {closureUpdatesCount} record versions.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _batchProcessNewRecords(changesDF, persistentDF, tableDest, skName, codColumn, dateList):
    """
    Batch process all new record insertions across all dates.
    This handles both completely new entities and new versions of existing entities.
    """

    print("Processing new records in batch...")
    
    # Get all records that need to be inserted (new + historical changes)
    insertRecords = changesDF.filter(
        F.col("change_type").isin(["NEW_RECORD", "HISTORICAL_CHANGE"])
    )
    
    insertCount = insertRecords.count()
    print(f"Records to insert: {insertCount}")
    
    if insertCount == 0:
        print("No new records to insert.")
        return
    
   
    # Since changesDF is now clean, we can work directly with it
    # Rename stage_hash_key back to ctr_hash_dim_key
    cleanInsertRecords = insertRecords.withColumnRenamed("stage_hash_key", "ctr_hash_dim_key")
    
    # Drop helper columns but keep ctr_date_key for start date calculation
    cleanInsertRecords = cleanInsertRecords.drop("persistent_hash_key", "change_type", "temporal_order", "is_last_change")
    
    # Add technical columns for new records
    newRecordsWithTechnical = cleanInsertRecords.withColumn(
        "ctr_flag_active", F.lit(1)
    ).withColumn(
        "ctr_start_date", F.col("ctr_date_key").cast("int") 
    ).withColumn(
        "ctr_end_date", F.lit(29991231)
    ).withColumn(
        "ctr_ins_date", F.current_timestamp()
    ).withColumn(
        "ctr_last_operation_date", F.current_timestamp()
    ).drop("ctr_date_key")  # Drop ctr_date_key after using it for start_date
    
    # Generate surrogate keys for new records
    if skName:
        # Get current max SK from persistent table
        maxSK = persistentDF.agg({skName: "max"}).collect()[0][0] or 0
        
        # Add surrogate keys to new records
        if codColumn and codColumn in newRecordsWithTechnical.columns:
            windowSpec = Window.orderBy(F.col(codColumn))
        else:
            windowSpec = Window.orderBy(F.col("ctr_hash_dim_key"))
            
        finalNewRecords = newRecordsWithTechnical.withColumn(
            skName, 
            F.row_number().over(windowSpec) + F.lit(maxSK)
        )
    else:
        finalNewRecords = newRecordsWithTechnical
    
    # Select only columns that exist in persistent table schema
    persistentColumns = persistentDF.columns
    finalColumns = [col for col in persistentColumns if col in finalNewRecords.columns]
    finalNewRecords = finalNewRecords.select(*finalColumns)
    
    # Insert all new records in batch
    recordCount = finalNewRecords.count()
    print(f"Final record count to insert: {recordCount}")
    
    if recordCount > 0:
        print("About to insert records...")        
        try:
            finalNewRecords.write.format('delta').mode('append').save(tableDest)
            print(f"Successfully inserted {recordCount} new records.")
        except Exception as e:
            print(f"Error during insert: {str(e)}")
            raise e
    else:
        print("No records to insert after final processing.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _batchProcessNonHistoricalUpdates(changesDF, persistentDF, tableDest, skName, historicalType2Columns, dateList):
    """
    Batch process all non-historical column updates.
    This updates records where only non-historical attributes changed.
    """
    
    print("Processing non-historical updates in batch...")
    
    if len(historicalType2Columns) == 0:
        print("No historical type 2 columns defined, skipping non-historical updates.")
        return
    
    # Get records with non-historical changes
    nonHistoricalChanges = changesDF.filter(F.col("change_type") == "NON_HISTORICAL_CHANGE")
    
    if nonHistoricalChanges.count() == 0:
        print("No non-historical changes to process.")
        return
    
    # Prepare update data (excluding historical columns)
    excludeColumns = [skName, 'ctr_ins_date', 'ctr_hash_validate_history', 'ctr_flag_active', 'ctr_start_date', 'ctr_end_date'] + historicalType2Columns
    excludeColumns = [col for col in excludeColumns if col is not None]
    excludeColumns.extend(["change_type", "temporal_order", "is_last_change", "stage_hash_key"])
    
    updateColumns = [col for col in nonHistoricalChanges.columns if col not in excludeColumns]
    
    updateData = nonHistoricalChanges.select(
        F.col("persistent_hash_key").alias("ctr_hash_dim_key"),
        *[F.col(col) for col in updateColumns if col in nonHistoricalChanges.columns],
        F.current_timestamp().alias("ctr_last_operation_date")
    ).distinct()
    
    # Execute batch update using Delta merge
    if updateData.count() > 0:
        persistentDeltaTable = D.DeltaTable.forPath(spark, tableDest)
        
        persistentDeltaTable.alias("persistent").merge(
            updateData.alias("updates"),
            "persistent.ctr_hash_dim_key = updates.ctr_hash_dim_key AND persistent.ctr_flag_active = 1"
        ).whenMatchedUpdateAll().execute()
        
        print(f"Updated {updateData.count()} records with non-historical changes.")


def _batchProcessType6Changes(changesDF, persistentDF, tableDest, skName, historicalType3Columns, dateList):
    """
    Batch process Type 6 (Type 3 + Type 2) changes.
    This updates current values in historical records for Type 6 dimensions.
    """
    
    print("Processing Type 6 changes in batch...")
    
    if len(historicalType3Columns) == 0:
        print("No Type 3 columns defined for Type 6 processing.")
        return
    
    # Get records with Type 3 changes
    type3Changes = changesDF.filter(
        F.col("change_type").isin(["HISTORICAL_CHANGE", "NON_HISTORICAL_CHANGE"])
    )
    
    if type3Changes.count() == 0:
        print("No Type 6 changes to process.")
        return
    
    # Process Type 3 current value updates
    # This updates all historical versions of a record with current values
    type3Columns = [col for col in historicalType3Columns if col in type3Changes.columns]
    
    if len(type3Columns) == 0:
        print("Type 3 columns not found in changes dataframe.")
        return
    
    type3UpdateData = type3Changes.select(
        F.col("persistent_hash_key").alias("update_hash_key"),
        *[F.col(col).alias(f"{col}_current") for col in type3Columns],
        F.current_timestamp().alias("ctr_last_operation_date")
    ).distinct()
    
    if type3UpdateData.count() > 0:
        # Create update expressions for Type 3 columns
        updateExpressions = {f"{col}_current": f"updates.{col}_current" for col in type3Columns}
        updateExpressions["ctr_last_operation_date"] = "updates.ctr_last_operation_date"
        
        persistentDeltaTable = D.DeltaTable.forPath(spark, tableDest)
        
        persistentDeltaTable.alias("persistent").merge(
            type3UpdateData.alias("updates"),
            "persistent.ctr_hash_dim_key = updates.update_hash_key"
        ).whenMatchedUpdate(set=updateExpressions).execute()
        
        print(f"Updated {type3UpdateData.count()} records with Type 6 changes.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Insert Fact Tables

# CELL ********************

def InsertOverwriteFactTables(
    factDF = None,
    tablePath = None,
    overwriteSchema=False,
    partitionColumnsList=[],
    partitionValue=None,
):
    firstRow = factDF.head(1)

    # Write into file if dataframe has values
    if firstRow:

        # Insert for partitionValue
        if partitionValue:
            factDF.write.format("delta").option(
                "overwriteSchema", str(overwriteSchema).lower()
            ).option(
                "replaceWhere",
                "{0} IN ({1})".format(partitionColumnsList[0], partitionValue),
            ).mode(
                "overwrite"
            ).partitionBy(
                partitionColumnsList
            ).save(
                tablePath
            )

            print(
                f"Inserted on: {tablePath}, replace partition {partitionColumnsList}: {partitionValue}."
            )

        # Full load
        else:

            # Write to file/table
            factDF.write.format("delta").option(
                "overwriteSchema", str(overwriteSchema).lower()
            ).mode("overwrite").partitionBy(partitionColumnsList).save(tablePath)

            print(f"Inserted on: {tablePath}")

    # If dataframe has no data
    else:
        print(f"No data to insert on: {tablePath}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace Sk NULL in fact dataframe
def ReplaceFactSkNull(factDF):
  skUnk = -1
  skDateUnk = 29991231

  for col in factDF.schema:
    if(col.name.startswith('sk_date')):
      factDF = factDF.na.fill({col.name: skDateUnk})
      
    if((col.name.startswith('sk_')) & (~col.name.startswith('sk_date'))):
      factDF = factDF.na.fill({col.name: skUnk})
  return factDF

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ProcessDelta(modelName = None, tableName = None, DF = None):

    date, fullProcess, dateColumnFormat, filterColumn = GetDatesToProcess(modelName,tableName,"fact")

    # Return full dataframe if full process == 1
    if fullProcess == 1:
        filterInitialDF = DF
        print("No filter applied, FullProcess = 1.")     

    else:

        # Format date according to column date format
        filterDate = F.date_format(F.lit(date), dateColumnFormat)

        # Filter initial dataframe according to SQl admin table
        filterInitialDF = DF.filter(F.col(filterColumn) >= filterDate)
        print("Only retrive date where {0} >= {1}.".format(filterColumn, date))

    return filterInitialDF

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def UpdateProcessingDate(modelName, tableName, processingDate, scope=""):
    import sempy.fabric as fabric

    use_yaml = spark.conf.get("UseYaml", "false").lower() == "true"
    
    if use_yaml:
        UpdateProcessingDateIntoKQL(modelName, tableName, processingDate, scope)
        return "Completed"
    else:
        pipelineName = 'pl_updateProcessingDate'

        # Get pipeline Id
        pipelines = fabric.list_items().query("Type == 'DataPipeline'")
        pipeline = pipelines[pipelines["Display Name"]==f'{pipelineName}']
        pipelineId = pipeline['Id'].iloc[0]

        # Init the sempy client
        client = fabric.FabricRestClient()

        # Use global variables instead of calling GetFabricIds()
        global WORKSPACE_ID

        # Get the warehouse connection string from variable library, fallback to API call
        try:
            vl = GetVariableLibrary()
            warehouseConnString = vl.warehouseConnectionString
        except Exception as e:
            print(f"Could not retrieve warehouse info from variable library: {e}. Using API fallback.")
            # Fallback to original API approach
            response = client.get(f"/v1/workspaces/{WORKSPACE_ID}/warehouses")
            warehouseId = json.loads(response.text)["value"][0]["id"]
            warehouseConnString = json.loads(response.text)["value"][0]["properties"]["connectionString"]

        itemId = f"{pipelineId}"
        jobType = f"Pipeline"
        payload = {
                "executionData": {
                    "parameters": {
                        "model": f"{modelName}",
                        "tableName": f"{tableName}",
                        "date": f"{processingDate}",
                        "param_official_workspace_id": f"{WORKSPACE_ID}",
                        "param_official_warehouse_id": f"{warehouseId}",
                        "param_official_warehouse_connstring": f"{warehouseConnString}"
                        }
                    }
                }

        # Run with payload / pipeline parameters
        response = client.post(f"/v1/workspaces/{WORKSPACE_ID}/items/{itemId}/jobs/instances?jobType={jobType}",json= payload)

        itemJobUrl = response.headers["Location"]
        jobStatus=""

        while jobStatus in ("InProgress", "NotStarted", ""):
            sleep(5)
            getResponse = client.get(itemJobUrl)
            responseJson = json.loads(getResponse.text)
            jobStatus = responseJson["status"]

        return jobStatus

        # Possible status values:
        # Cancelled	
        # Completed	
        # Deduped	
        # Failed	
        # InProgress	
        # NotStarted

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def UpdateProcessingDateIntoKQL(modelName, tableName, processingDate, scope):
    # check if kustoURI is defined
    try:
        kustoUri = spark.conf.get(f"kustoUri")
        # The database to write the data
        database = spark.conf.get(f"kustoDatabase")
    except:
        print("no kusto URI or database defined")
        return 0

    # The table to write the data
    table    = "datesConfig"
    # The access credentials for the write
    accessToken = notebookutils.credentials.getToken('kusto')

    df = spark.createDataFrame(
        [
            (modelName, tableName, scope, processingDate)
        ],
        ["model", "tableName", "scope", "startDate"]
    ).withColumn("timestamp", F.current_timestamp())

    WriteToKustoWithRetries(df,kustoUri,database,table,accessToken)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def RunPipeline(pipelineName, pipelineParameters, targetWorkspace=None):
    import sempy.fabric as fabric

    # Determine target workspace ID
    if targetWorkspace:
        # If workspace name provided, resolve to workspace ID
        workspaces = fabric.list_workspaces()
        target_ws = workspaces[workspaces["Name"] == targetWorkspace]
        if target_ws.empty:
            raise ValueError(f"Workspace '{targetWorkspace}' not found")
        target_workspace_id = target_ws['Id'].iloc[0]
    else:
        # Use current workspace
        global WORKSPACE_ID
        target_workspace_id = WORKSPACE_ID

    # Get pipeline Id from target workspace
    pipelines = fabric.list_items(workspace=target_workspace_id).query("Type == 'DataPipeline'")
    pipeline = pipelines[pipelines["Display Name"]==f'{pipelineName}']
    if pipeline.empty:
        raise ValueError(f"Pipeline '{pipelineName}' not found in workspace '{targetWorkspace or 'current'}'") 
    pipelineId = pipeline['Id'].iloc[0]

    # Init the sempy client
    client = fabric.FabricRestClient()

    itemId = f"{pipelineId}"
    jobType = f"Pipeline"
    payload = {
            "executionData": {
                "parameters": pipelineParameters
                }
            }

    # Run with payload / pipeline parameters
    response = client.post(f"/v1/workspaces/{target_workspace_id}/items/{itemId}/jobs/instances?jobType={jobType}",json= payload)

    itemJobUrl = response.headers["Location"]
    jobStatus=""

    while jobStatus in ("InProgress", "NotStarted", ""):
        sleep(5)
        getResponse = client.get(itemJobUrl)
        responseJson = json.loads(getResponse.text)
        jobStatus = responseJson["status"]

    return jobStatus

    # Possible status values:
    # Cancelled	
    # Completed	
    # Deduped	
    # Failed	
    # InProgress	
    # NotStarted

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def FlattenArrayAndStruct(dataframe: DataFrame) -> DataFrame:
    """
    Recursively flattens array and struct columns in a PySpark DataFrame.

    - Struct columns are expanded into individual fields.
    - Array columns are exploded into separate rows.
    - If an array contains structs, it is first exploded, and then its fields are flattened.
    - The process continues recursively until no struct or array columns remain.

    Args:
        dataframe (DataFrame): The input PySpark DataFrame containing nested structures.

    Returns:
        DataFrame: A transformed DataFrame with all struct fields expanded and array elements separated into rows.

    Example:
        Given a DataFrame with a nested structure like:
        ```
        +---+------------------+
        | id| user            |
        +---+------------------+
        |  1| {name: "Alice", address: {city: "NY", zip: "10001"}} |
        |  2| {name: "Bob", address: {city: "SF", zip: "94105"}}   |
        +---+------------------+
        ```
        The output will be:
        ```
        +---+------+----------+------+
        | id|user_name|user_address_city|user_address_zip|
        +---+------+----------+------+
        |  1| Alice | NY       | 10001 |
        |  2| Bob   | SF       | 94105 |
        +---+------+----------+------+
        ```
    """

    # Define a function to recursively flatten structs and arrays
    def process_columns(df):
        for column in df.schema:
            if isinstance(column.dataType, StructType):
                # Flatten struct fields into individual columns
                for field in column.dataType.fields:
                    df = df.withColumn(
                        f"{column.name}_{field.name}".lower(),
                        col(f"{column.name}.{field.name}"),
                    )
                # Drop the original struct column
                df = df.drop(column.name)

            elif isinstance(column.dataType, ArrayType):
                # Check if the array contains structs
                if isinstance(column.dataType.elementType, StructType):
                    # Explode the array of structs into separate rows
                    df = df.withColumn(column.name, explode_outer(col(column.name)))
                    # Flatten the struct fields after the explosion
                    for field in column.dataType.elementType.fields:
                        df = df.withColumn(
                            f"{column.name}_{field.name}".lower(),
                            col(f"{column.name}.{field.name}"),
                        )
                    # Drop the original array column
                    df = df.drop(column.name)
                else:
                    # Explode the array of primitive types into separate rows
                    df = df.withColumn(column.name, explode_outer(col(column.name)))
        return df

    # Start with the original dataframe and apply the process_columns function
    dataframe = process_columns(dataframe)

    # After the first pass, check if there are any remaining StructType or ArrayType columns
    # If there are, continue processing until no more remain
    while any(
        isinstance(field.dataType, (StructType, ArrayType))
        for field in dataframe.schema
    ):
        dataframe = process_columns(dataframe)

    return dataframe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def WriteToKustoWithRetries(df,kustoUri,database,table,accessToken):
    attempt = 0
    MAX_RETRIES = 5  # Maximum retry attempts
    RETRY_DELAY = 10  # Initial delay in seconds
    
    while attempt < MAX_RETRIES:
        try:
            df.write.\
                format("com.microsoft.kusto.spark.synapse.datasource").\
                option("kustoCluster", kustoUri).\
                option("kustoDatabase", database).\
                option("kustoTable", table).\
                option("accessToken", accessToken).\
                option("tableCreateOptions", "CreateIfNotExist").\
                mode("Append").save()

            print("✅ Write to Kusto successful.")
            return  # Exit the function if successful

        except Exception as e:
            if "throttling" in str(e).lower():  # Check for throttling errors
                attempt += 1
                wait_time = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                print(f"⚠️ Kusto throttling detected. Retrying in {wait_time} seconds... (Attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait_time)
            else:
                print("❌ Non-throttling error encountered:", str(e))
                raise  # If it's not throttling, re-raise the error immediately

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def GetDatesToProcess(model: str, tableName: str, scope: str):

    use_yaml = spark.conf.get("UseYaml", "false").lower() == "true"
    
    if use_yaml:
        date_to_process, full_process, dateColumnFormat,filterColumn  = GetDateToProcessFromKQL(model, tableName, scope)
        if isinstance(date_to_process, str):
            date_to_process = Dtime.datetime.fromisoformat(date_to_process).date()
        return date_to_process, full_process, dateColumnFormat,filterColumn
    else:
        # Use global variables instead of calling GetFabricIds()
        global WORKSPACE_ID, GOLD_LAKEHOUSE_ID

        # get date to process
        datesDF = (
            spark.read.format("delta")
                    .load(
                        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE_ID}/Tables/config/processDatesConfig"
                    )
                .filter(
                    (F.col("model") == model)
                    & (F.col("tableName") == tableName)
                    & (F.col("scope") == scope)
                )
        )
        row = datesDF.first()

        # Return full dataframe if full process == 1
        if row.fullProcess == 1 or datesDF.count() == 0:

            return row.date, row.fullProcess, row.dateColumnFormat, row.filterColumn

        # Calculate date if full process == 0
        else:

            # If process date is defined
            if row.date!=None:
                date = row.date
            else:
                today = Dtime.date.today()

                # Calculate date based on SQL admin table
                if row.dateType == "Year":
                    yearDate = today - relativedelta(years=int(row.dateUnit))
                    date = yearDate.replace(day=1)

                elif row.dateType == "Month":
                    monthDate = today - relativedelta(months=int(row.dateUnit))
                    date = monthDate.replace(day=1)

                elif row.dateType == "Day":
                    date = today - relativedelta(days=int(row.dateUnit))

            return date, row.fullProcess, row.dateColumnFormat, row.filterColumn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
