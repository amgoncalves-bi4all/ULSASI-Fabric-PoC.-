# Introduction 
The project goal is to provide a metadata driven framework for a Fabric project. It's not a full feature solution but an accelerator for an analytics project following a medallion architecture.
The naming conventions used here are suggestions that should be adapted to the clients current practices if any already in place.
Because PySpark notebooks are case-sensitive, make sure you adjust the name of the control columns in the commonFunctions notebook to the adopted naming conventions.

# Getting Started
- Clone this repo to the clients DevOps environment.
- Setup Fabric source control and point it to the cloned repo.
- Choose your configuration approach (see Configuration Options below).
- Add configurations using your chosen method.
- Create shortcuts (if using warehouse config tables).

## Configuration Options

This framework supports two distinct configuration approaches. Choose the one that best fits your project requirements:

### Option 1: Database Config Tables (Traditional)
- **Storage**: Fabric warehouse with structured config tables
- **Management**: SQL-based configuration management
- **Dependencies**: Requires shortcuts to be created in lakehouses
- **Orchestration**: Performed by a master pipeline (pl_modernBI_fabricFramework_master)
- **Logging**: Uses warehouse tables for logging only for the pipelines
- **Workpace Folders Required**: Lakehouses, Pipelines, Warehouses, Notebooks (except sub-folder Yaml_Orchestrator)
- **See**: [Config Database](#config-database) section below for detailed table structure

### Option 2: YAML Config Files (Modern)
- **Storage**: YAML files stored in the lakehouse Files area
- **Management**: File-based configuration with version control support
- **Dependencies**: No shortcuts required
- **Orchestration**: Performed by NB_YamlOrchestrator notebook
- **Logging**: Supports notebook execution logging and uses Eventhouse for centralized logging and monitoring
- **Cross-Workspace Support**: Supports executing pipelines and notebooks located in different workspaces using the optional `artifactWorkspace` property
- **Workpace Folders Required**: Eventhouses, Lakehouses, Pipelines_NoSQL, Notebooks
- **See**: `configFiles/README.md` for detailed YAML schema and examples

**Note**: Both approaches achieve the same functionality. The YAML approach is recommended for new implementations as it provides better version control, easier deployment across environments, simplified maintenance, resume after error capability, and enhanced logging capabilities through Eventhouse integration.
The CU consumption of both options are equivalent with only a slight advantage to the Yaml side. So it's not a relevant factor for decision.

**Important Notes:**
- **Cross-Workspace Execution**: The YAML orchestrator supports executing artifacts (pipelines and notebooks) in different workspaces by specifying the optional `artifactWorkspace` property in the configuration. When omitted, the current workspace is used. For considerations when using cross-workspace execution, see the [Microsoft documentation on notebook utilities](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities).
- At the time of writing the sync of the pipelines will fail when the connections don't exist in the target environment. The connection Id must be replaced in the pl_copy_from_adventureWorks2022 pipeline to an existing connection Id **before** executing the Sync.
- Some artifacts (pipelines, notebooks,...) will be pointing to current workspace, lakehouse, etc. Review all the connections after cloning.
- For YAML config option, the inferred members configuration is also based on a YAML file called inferredMembersConfig.yaml. See file sample in: ConfigFiles/environments/dev
- In lower SKU's reduce the paralelism in order to avoid throttling in Eventhouse or writing conflicts in Warehouse. In pipeline activities that read/write in either storages, adjust the retry property to your needs.
- For option 1, the warehouse can be replaced by SQL database to better handle concurrency. But at the time of writing this alternative is still in preview and has a larger CU footprint.

## Shortcuts (Required only for Database Config Tables approach)
The following shortcuts to the warehouse config tables must be created in the lakehouses when using the database config tables approach.

**Silver lakehouse**:
- Schema: config
- Shortcut to: processDatesConfig, silverGoldConfig, silverGoldDependency

- Schema: log
- Shortcut to: copyDataLog

**Gold lakehouse**:

- Schema: log
- Shortcut to: inferredMembersConfig, processDatesConfig, silverGoldConfig, silverGoldDependency.

# Configuration Implementation

## Database Config Tables Approach

### Config Database
Config database is currently setup in a Fabric warehouse because Fabric databases are still in preview and some problems prevented the use of databases.
But the goal is to migrate the config database to Fabric databases when possible.

The goal of the config database is to provide an area to dynamically configure the analytics solution without requiring code modifications. This way the solution will be more robust and easier to maintain by any support engineer.
The config tables rely on the concept of "Model" to group all the related workload configurations to a common branch, therefore the "model" column exists in every table and is mandatory.

The database has the following schemas:
	- admin: Has all the configuration tables. The shortcuts created in the lakehouses for theses tables were created in the config schema, because "admin" is a reserved word and cannot be used for schema naming in the lakehouses
	- log: Has the log tables
	- temp: Has the temporary table for data ingestion
	

The database has the following tables:

### admin.copyDataConfig
Configuration table for the ingestion process. Has a record for each table/file that needs to be extracted from a data source system.
	
| Column Name | Description | Allowed Values |  
|-----------|:-----------|:-----------|  
| configId | Unique row identifier. Because warehouses don't yet support identity types, make sure the values are unique | N/A |  
| model | Name of the model | N/A |
| sourceSystemName | Name of source system. It's just a documentation reference. (Not currently used as variable in the pipelines)  | N/A |
| sourceSystemType | Type of source system. | sql, xlsx |
| sourceLocationName | Name of the location of the source system (e.g. Database name). Used in the pipeline to switch to the correct extraction pipelines | N/A |
| sourceObjectName | Name of the table/file in the source system to be extracted. | N/A |
| sourceSelectColumns | List of source column names to be used in the extraction select statement | N/A |
| sourceKeyColumns | Not currently used | N/A |
| destinationSystemName | Name of the destination system (can be a lakehouse, warehouse,...) - (Not currently used as variable in the pipelines) | N/A |
| destinationSystemType | Type of destination system - (Not currently used as variable in the pipelines)| lakehouse, warehouse,... |
| destinationObjectPattern | Name of the destination object | N/A |
| destinationDirectoryPattern | Pattern to be used in the destination object directory (e.g. adventureworks2022/Person/Person/) | N/A |
| destinationObjectType | Type of object to be created - (Not currently used as variable in the pipelines) | parquet, delimited text, avro,... |
| extractType | Type of extraction | delta, full |
| deltaStartDate | Start date of a delta extraction. Only required for delta extract types | N/A |
| deltaEndDate | End date of a delta extraction. Only required for delta extract types | N/A |
| deltaDateColumn | Name of the column in the source table to be used in the filter of a delta extraction. Only required for delta extract types | N/A |
| deltaFilterCondition | Filter condition to be used in a select statement to filter a delta extraction. Only required for delta extract types | N/A |
| flagBlock | Not currently used | N/A |
| blockSize | Not currently used | N/A |
| blockColumn | Not currently used | N/A |
| flagActive | Flag to enable/disable the configuration line | 0,1 |
| createDate | Date of the creation of the configuration line  | N/A |
| lastModifiedDate | Date of the last modification of the configuration line  | N/A |

### admin.inferredMembersConfig
Configuration table to store the values available for the inferred members rows
	
| Column Name | Description | Allowed Values |  
|-----------|:-----------|:-----------|  
| columnType | Name of the data type  | DateType, DoubleType, TimestampType, BooleanType, IntegerType, LongType, StringType |  
| columnValue | Column value to be populated for the configured inferred member value (sk_value) | N/A |
| sk_value | Inferred member | -1, -2, -3 |

### admin.processDatesConfig
Configuration table to store the date of the next processing for the Silver/Gold layers. Used for delta loading.
	
| Column Name | Description | Allowed Values |  
|-----------|:-----------|:-----------|  
| model | Name of the model  | N/A |  
| tableName | Name of the table | N/A |
| scope | Scope of usage for this configuration | DimType2, Fact |
| fullProcess | Flag that indicates if a full process should take place | 0, 1 |
| dateType | Type of loading interval. (Optional - To be used with dateUnit) | Day, Month, Year |
| filterColumn | Name of the source column to be filtered | N/A |
| dateColumnFormat | Format of the date in the column to be filtered (e.g. dd/MM/yyyy) | N/A |
| dateUnit | Number of units of the date type to be used to calculate the delta. Optional. If this column is used, the processing date will be calculated using relative delta with the dateType and the dateUnit from the current date. | N/A |
| date | Date to be used to filter the source data. (Optional. Can be used instead of dateUnit/dateType columns, but must be updated with the new value after each processing)   | N/A |

### admin.silverGoldConfig
Configuration table to store the silver and gold layers artifacts that should be executed.
For the loading of Silver layer, a generic notebook "NB_Load_Silver" reads data from bronze and creates a table with the same structure in the Silver layer using this config table. If more customization is needed for a particular table, a specific notebook can be developed and configured in this table.
For the gold layer is assumed that all tables need specific transformations, therefore, most columns in this table aren't required, only the name of the specific notebook.
	
| Column Name | Description | Allowed Values |  
|-----------|:-----------|:-----------|  
| model | Name of the model  | N/A |  
| sourceSystemName | Name of the source system (e.g. lakehouse name) - Required only for silver layer config | N/A |
| sourceLocationName | Name of the schema in the source system. Must match the one configured in the copyDataConfig - Required only for silver layer config | N/A |
| sourceDirectoryPattern | Directory pattern of the source data - Must match the one configured in destinationDirectoryPattern of copyDataConfig  - Required only for silver layer config | N/A |
| objectName | Name of the table to be created. Include the schema name if used. - Required only for silver layer config | N/A  |
| keyColumns | List of business key columns to be used for delta loading. - Required only for silver layer config | N/A |
| partitionColumns | List of columns to be used for partitioning. - Required only for silver layer config | N/A |
| extractType | Type of extraction used for the bronze layer. - Required only for silver layer config | delta, full |
| loadType | Type of loading in the silver layer. - Required only for silver layer config | N/A |
| destinationObjectPattern | Name of the schema and table in the destination lakehouse | N/A |
| destinationDatabase | Name of the destination lakehouse | N/A |
| notebookName | Name of the notebook to be executed | N/A |
| layer | Name of the layer (e.g. Silver, Gold) | N/A |
| flagActive | Flag to enable/disable the configuration line | 0,1 |

### admin.silverGoldDependency
Configuration table for the dependencies of notebooks that load the Silver and Gold layers.
For the loading of Silver layer, a generic notebook "NB_Load_Silver" reads data from bronze and creates a table with the same structure in the Silver layer using this config table. If more customization is needed for a particular table, a specific notebook can be developed and configured in this table.
For the gold layer is assumed that all tables need specific transformations, therefore, most columns in this table aren't required, only the name of the specific notebook.
	
| Column Name | Description | Allowed Values |  
|-----------|:-----------|:-----------|  
| model | Name of the model  | N/A |
| layer | Name of the layer (e.g. Silver, Gold) | N/A |
| objectName | Name of the table being loaded - Must match the destinationObjectPattern of silverGoldConfig | N/A |
| dependencyObjectName | Name of the table that needs to be loaded before objectName - Must match the destinationObjectPattern of silverGoldConfig | N/A |
| systemDateUpdate | Date of configuration creation/update | N/A |

### log.copyDataLog
Stores logging information about extractions from data source systems. Used by Silver layer notebooks to get the location of the extracted files.

| Column Name | Description | Allowed Values |  
|-----------|:-----------|:-----------|  
| model | Name of the model  | N/A |
| destinationPath | Path for the location of the files in the destination system | N/A |
| sourceLocationName | Name of the source location (eg. Database name) | N/A |
| objectName | Name of the source object (e.g. schema and table name) | N/A |
| status | Status of the extraction | Succeeded, Failed |
| startDate | Timestamp for the start of the operation | N/A |
| rowCount | Number of rows copied | N/A |
| sourceReadCommand | Sql statement used for the extraction | N/A |
| endDate | Timestamp for the end of the operation | N/A |
| duration | Duration of the operation in seconds | N/A |

### temp.copyDataConfig
Stores temporary data required for the ingestion execution of a given model.
The records of a model are deleted when the ingestion pipeline execution starts and new records are added for the extractions that will be executed.
The table is populated by the admin.uspSelectTempCopyDataConfig stored procedure.

# Notebooks
Several template notebooks are available for most common use cases. They are grouped in folders according to the target layer.
The notebooks **NB_RunSilverNotebooks** and **NB_RunGoldNotebooks** execute the notebooks configured in the silverGoldConfig table. The notebook will create a DAG based on the configured dependencies and use the notebookutils runMultiple function to execute the DAG and will try to execute in parallel as much notebooks as possible.
In order to be able to execute all notebooks in the same spark session, is advisable not to define the default lakehouse in the notebook. This will also be useful in the deployment pipelines, since there will be no need to point the notebooks to the lakehouse in the each environment

# Pipeline connections
Natively, pipeline connections are absolute, this means that when moving to upper environments the connection will still point to the resource in the original workspace. And at the time of writing, deployment rules in deployment pipelines aren't yet supported.
To avoid having to manually change all pipeline connections in each environment after deployment, the pipeline connections to data stores in Fabric workspaces are now dynamic. The pipeline pl_get_fabric_ids is responsible to fetch the workspace, lakehouses and warehouses Id's using Fabric API.
This pipeline must be called before the "master" pipeline to pass all the necessary id's as parameters.

# Contribute
If you developed features in your projects that might be helpful and reused in other analytics projects, reach out the projects admins, request a Contributor role to this repo, create your branch and add your features.

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)