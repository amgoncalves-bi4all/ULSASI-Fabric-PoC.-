# GitHub Copilot Instructions - Modern BI Fabric Framework

## Architecture Overview
This is a **metadata-driven Microsoft Fabric framework** for Microsoft Fabric, implementing medallion architecture (Bronze→Silver→Gold layers) with two configuration approaches:

### Configuration Patterns
- **Option 1 (Database Tables)**: Warehouse-based config with SQL management, requires shortcuts between lakehouses
- **Option 2 (YAML Files)**: File-based config stored in lakehouse Files area, orchestrated by `NB_YamlOrchestrator`
- **Detection**: Use `spark.conf.get("UseYaml", "false").lower() == "true"` to determine active mode

### Core Components
- **Lakehouses**: Bronze (raw ingestion), Silver (cleansed), Gold (business-ready)  
- **Eventhouse**: KQL-based logging and process tracking (YAML mode only)
- **Warehouses**: Configuration storage and SQL-based operations (Database mode)
- **Notebooks**: PySpark notebooks with shared `CommonFunctions` import pattern
- **Variable libraries**: For artifact id's management with different environments
- **Environments**: For custom python librarys deployment (Optional for notebooks with library usage)

## Critical Development Patterns

### Notebook Structure
```python
# Standard import pattern - ALWAYS use at top of notebooks
%run CommonFunctions

# Global fabric IDs are automatically initialized
# WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID

# Configuration mode detection
use_yaml = spark.conf.get("UseYaml", "false").lower() == "true"
```

### Configuration Access Patterns
```python
# Get metadata for any object across layers
config = GetConfigMetadata(model="framework", objectName="Person", layer="Silver")

# YAML-specific function (when UseYaml=true)  
config = GetConfigMetadataFromYaml(model="framework", objectName="Person", layer="silver")

# Eventhouse logging setup (YAML mode)
SetSessionLogURI("eh_modernBI_fabricFramework_01", "eh_modernBI_fabricFramework_01")
```

### Path Construction Patterns
```python
# OneLake paths follow this structure:
f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{schema}/{table}"
f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/{folder}/{file}"

# YAML configs are stored at:
f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Files/config/{model}.yml"
```

### Pipeline Orchestration
```python
# YAML mode: Use DAG-based notebook orchestration
notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})

# Database mode: Use pipeline calls
RunPipeline("pl_modernBI_fabricFramework_master", parameters)
```

## File Organization
- `Notebooks/99_Administration/CommonFunctions.Notebook/` - Shared functions, always import first
- `ConfigFiles/environments/{dev|test|prod}/` - YAML configurations per environment  
- `Notebooks/98_Yaml_Orchestrator/` - YAML-specific orchestration notebooks
- `Pipelines/` - Traditional pipeline approach artifacts
- `Pipelines_NoSQL/` - YAML approach pipeline artifacts

## Configuration Schema (YAML Mode)
```yaml
model: "framework"
active: true
raw:          # Data ingestion layer
  objectName:
    flagActive: 1
    artifactType: "pipeline|notebook"
    artifactName: "artifact_name"
    dependsOn: ["other_objects"]
silver:       # Cleansed data layer  
  objectName:
    flagActive: 1
    dependsOn: ["raw_objectName"]
gold:         # Business layer
  objectName:
    flagActive: 1
    group: "dims|facts"  # For dependency grouping
    dependsOnGroup: ["dims"]
```

## Development Workflows

### Local Development
- Configure Python environment: Use project `.venv` 
- YAML validation: `python ConfigFiles/scripts/validate_config.py framework.yml`
- Schema location: `ConfigFiles/scripts/config-schema.json`

### Deployment Pipeline
- Triggers on `ConfigFiles/environments/**` changes
- Validates all YAML files against schema
- Deploys to dev→test→prod environments sequentially  
- Uses Azure DevOps pipeline: 
    - `deploy-configFiles.yml` for the deployment of yaml config files in the target environment lakehouse
    - `deploy-cross-environment.yml` for cross-environment deployments of Fabric artifacts

### Dependency Management
- **Critical**: When adding dependencies, ensure referenced objects have `flagActive: 1`
- Dependencies are filtered automatically to prevent invalid DAG references
- Use `dependsOnGroup` for group-level dependencies (e.g., all dimensions)

## Common Integration Points
- **Fabric REST API**: Use `fabric.FabricRestClient()` for workspace operations
- **KQL Integration**: Eventhouse queries via `spark.read.format("com.microsoft.kusto.spark.synapse.datasource")`
- **Cross-Environment**: Dynamic workspace/lakehouse ID resolution via `GetFabricIds()`

## Anti-Patterns to Avoid
- Don't hardcode workspace/lakehouse IDs - use global variables
- Don't mix configuration modes - detect via `UseYaml` spark config
- Don't define default lakehouse in notebooks (breaks multi-environment deployment)
- Don't add dependencies to inactive objects (`flagActive: 0`)

## Key Files for Reference
- `Notebooks/99_Administration/CommonFunctions.Notebook/notebook-content.py` - Core framework functions
- `ConfigFiles/environments/dev/framework.yml` - Example YAML configuration
- `README.md` - Architecture decision rationale and setup instructions