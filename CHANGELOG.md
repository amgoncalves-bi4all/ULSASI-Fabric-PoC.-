# Change Log

## [Unreleased] - 2026-02-02

### Added
- **Cross-Workspace Execution Support**: Added optional `artifactWorkspace` property to YAML configuration schema for all layers (raw, silver, gold)
- Support for executing pipelines and notebooks located in different Microsoft Fabric workspaces
- Updated `RunPipeline` function in CommonFunctions to accept optional `targetWorkspace` parameter
- Enhanced NB_RunPipeline notebook to pass workspace parameter to pipeline execution
- Updated NB_Wrapper notebook to handle cross-workspace notebook execution using Fabric REST API
- Added comprehensive validation for the new `artifactWorkspace` property in schema files
- Updated documentation with cross-workspace execution examples and requirements

### Changed
- Modified YAML orchestrator (NB_Read_ConfigYAML) to extract and pass workspace information to DAG activities
- Enhanced validation scripts to support the new optional property
- Updated example configuration files with cross-workspace usage examples
- Updated CommonFunctions setSessionLogURI function to store kqldatabase_id in session variable
- Updated **NB_YamlOrchestrator** notebook to pass kqldatabase_id to pipelines as parameter
- **pl_copyFrom_adventureWorks_kql_yaml** updated KQL activity to support dynamic connection
- **pl_copyDataLog_kql** updated KQL activity to support dynamic connection

### Technical Details
- When `artifactWorkspace` is specified: artifacts execute in the named workspace
- When `artifactWorkspace` is omitted/empty: artifacts execute in current workspace (backward compatible)
- Workspace names are resolved to IDs at runtime with appropriate error handling

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

## 2025-12-24

### Added
- **GetVariableLibraryName()**: Added dynamic discovery function for Variable Library names following naming convention 'vl-<project>-01' with pattern-based matching using regex
- **GetVariableLibrary()**: Added helper function to get Variable Library object using dynamically discovered name
- **Variable Library Integration**: Enhanced framework to use Variable Library as primary source for workspace, lakehouse, eventhouse, and warehouse information

### Changed
- **GetFabricIds()**: Modified to use Variable Library first before falling back to Fabric API calls for retrieving workspace and lakehouse IDs
- **SetSessionLogURI()**: Updated to use Variable Library for eventhouse information with API fallback for kustoUri
- **UpdateProcessingDate()**: Enhanced warehouse-related code to use Variable Library for warehouse ID and connection string
- **fabric_common_lib**: Updated library to reflect all Variable Library integration changes from CommonFunctions notebook
- **Dynamic Discovery**: Implemented robust fallback mechanisms - pattern matching → string matching → first available library → API calls
- **Performance**: Added caching in Spark configuration to avoid repeated API calls for Variable Library discovery

### Fixed
- **Cross-Environment Compatibility**: Eliminated hard-coded Variable Library names, enabling seamless deployment across different environments
- **Error Handling**: Enhanced error handling with comprehensive fallback tiers and informative messages

## 2025-08-25

### Added
- **eh_modernBI_fabricFramework_01**: Added UDF LastFailedBatchProcesses to support model error resume.
- **inferredMembersConfig.yaml**: Yaml file to configure inferred members as a replacement for the inferredMembers table when Yaml config files are used.
- **pl_run_fabricFramework_master**: Pipeline to extract the Fabric Id's and pass them as parameters to the master pipeline.

### Changed 
- **Yaml configuration orchestration notebooks**: Moved to folder Notebooks/98_Yaml_Orchestrator and it's now a fully functional orchestration option.
- **pl_copy_from_adventureWorks2022_kql_yaml**: Added fabric Id's parameters for dynamic connections
- **CommonFunctions**: Standardized functions naming; Modified MergeHistoricalDim for improved performance. Added/modified functions to work with either Yaml/Config Tables.
- **NB_YamlOrchestrator**: Added new parameter (modelReset) and logic to resume model execution from last error. If modelReset=False and last execution ended in error, the model will run only the processess that failed or didn't executed. Dependencies will be adjusted accordingly.

### Fixed
- Pipelines parameters

## 2025-07-30

### Added
- **ConfigFiles folder and content**: Environment organization for yaml configuration files, validation schema, scrips for validation, testing and deployment

## Changed
- deploy-configFiles.yml deployment pipeline config

### Fixed


## 2025-06-09

### Added
- **pl_get_fabric_ids**: this pipeline call fabric API to retrieve workspace, lakehouses and warehouse Id's

### Changed
- **pl_modernBI_fabricFramework_master**: Added new parameters, changed connections to dynamic
- **pl_updateProcessingDate**: Added new parameters, changed connections to dynamic
- **pl_copyDataLog**: Added new parameters, changed connections to dynamic
- **pl_modernBI_fabricFramework_copyFromSource**: Added new parameters, changed connections to dynamic
- **pl_copyFrom_adventureWorks2022**: Added new parameters, changed connections to dynamic
- **pl_copyFrom_file**: Added new parameters, changed connections to dynamic
- **commonFunctions notebook**: modified function updateProcessingDate

## 2025-03-26

### Added
- NB_DIM_DATE notebook
- NB_DIM_TIME notebook

### Changed

### Fixed
- **NB_DIM_SCD1 notebook**: Corrected MergeDims call
- **NB_DIM_Store notebook**: Corrected MergeDims call
- **NB_Load_Silver notebook**: Added call to updateProcessingDate function. Bugfixes
- **NB_Load_Store_Silver notebook**: Bugfixes
- **NB_RunGoldNotebooks notebook**: Modified logic in silverGoldConfig/silverGoldDependency added filter to consider dependency flag_active
- **NB_RunSilverNotebooks notebook**: Modified logic in silverGoldConfig/silverGoldDependency added filter to consider dependency flag_active


## 2025-03-17

### Added

### Changed

### Fixed
- **uspSelectTempCopyDataConfig**: Bugfix in between condition


## 2025-02-19

### Added

### Changed
- **All notebooks**: Removed all usage of spark.sql to remove the need to have default lakehouse defined. This allows that any notebook can run in the same spark session
- **CommonFunctions notebook**: Added new functions to support the removal of spark.sql.

### Fixed
- **CommonFunctions notebook**: Bugfix in MergeDim function


## 2025-01-28

### Added
- Utilities notebooks:
	- Copydata_config_script: Notebook to help populate the CopydataConfig table
	- MetadataSync: Notebook to sync the SQL endpoint metadata from Lakehouse

### Changed
- **CommonFunctions notebook**: Modified **updateProcessingDate** date function to wait for pipeline completion and return pipeline execution status
- **NB_FACT_DELTA notebook**: Modified call to **updateProcessingDate** to retrieve result and raise exception if status <> "Completed"

### Fixed