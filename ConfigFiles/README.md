# Metadata Configuration Schema and Validation

This directory contains schema files and validation tools for Modern BI Fabric Framework YAML configuration files.

## Files Overview

### Schema Files
- **`config-schema.json`** - JSON Schema definition for configuration files validation
- **`config-schema.yaml`** - YAML version of the schema (same structure, different format)

### Validation Scripts
- **`validate_config.py`** - Python-based validation script with advanced features
- **`Validate-Config.ps1`** - PowerShell-based validation script for Windows environments

### Deployment Scripts
- **`deploy-config-.ps1`** - Azure PowerShell deployment script for ADLS Gen2

### Azure DevOps Pipeline
- **`../deploy-configFiles.yml`** - Multi-environment CI/CD pipeline for automated configuration deployment

### Configuration Files
- **`framework.yml`** - Example configuration file

## Schema Structure

The schema defines validation rules for three main layers:

### 1. Raw Layer (`raw`)
Data ingestion from source systems to bronze/raw storage.

**Required Properties:**
- `sourceLocationName` - Source environment name
- `sourceSystemType` - Type of source (sql, file, api, nosql)
- `sourceSystemName` - Source system identifier
- `sourceObjectName` - Source object/table name
- `destinationSystemName` - Target system name
- `destinationSystemType` - Target type (lakehouse, warehouse, eventhouse)
- `destinationObjectPattern` - Naming pattern for destination
- `destinationDirectoryPattern` - Directory structure pattern
- `destinationObjectType` - File format (parquet, delta, csv, json)
- `extractType` - Extraction method (full, delta, incremental)
- `flagBlock` - Processing block flag (0/1)
- `flagActive` - Active status flag (0/1)
- `artifactType` - Processing artifact type (pipeline, notebook, dataflow)
- `artifactName` - Artifact name
- `artifactParams` - Parameters for the artifact
- `artifactWorkspace` - **Optional** workspace name where the artifact is located. If not specified, current workspace is used

**Optional Properties:**
- `sourceSelectColumns` - Specific columns to extract
- `sourceKeyColumns` - Key columns for the source
- `deltaDateColumn` - Date column for delta extraction (required when extractType=delta)

### 2. Silver Layer (`silver`)
Data transformation and cleansing from bronze to silver storage.

**Required Properties:**
- `sourceSystemName` - Source system (usually bronze layer)
- `sourceLocationName` - Source location
- `sourceDirectoryPattern` - Source directory pattern
- `sourceObjectName` - Source object name
- `extractType` - Extraction type
- `loadType` - Loading strategy (overwrite, merge, append)
- `destinationObjectPattern` - Destination naming pattern
- `destinationDatabase` - Target database
- `flagActive` - Active status flag
- `artifactType` - Processing artifact type
- `artifactName` - Artifact name
- `artifactWorkspace` - **Optional** workspace name where the artifact is located
- `artifactParams` - Artifact parameters

**Optional Properties:**
- `keyColumns` - Key columns for the object
- `dependsOn` - Dependencies on other objects

### 3. Gold Layer (`gold`)
Business-ready data marts and aggregations.

**Required Properties:**
- `destinationObjectPattern` - Destination naming pattern
- `destinationDatabase` - Target database
- `flagActive` - Active status flag
- `artifactType` - Processing artifact type
- `artifactName` - Artifact name
- `artifactWorkspace` - **Optional** workspace name where the artifact is located

**Optional Properties:**
- `group` - Object grouping (e.g., 'dims', 'facts')
- `artifactParams` - Artifact parameters
- `dependsOn` - Dependencies on other objects
- `dependsOnGroup` - Dependencies on object groups

## Cross-Workspace Execution

The framework supports executing pipelines and notebooks located in different workspaces through the optional `artifactWorkspace` property. This enables:

- **Centralized Orchestration**: Run a single orchestrator while executing artifacts across multiple workspaces
- **Shared Resources**: Utilize common pipelines or notebooks stored in dedicated workspaces
- **Environment Separation**: Execute development artifacts from production orchestrator for testing

### Configuration

Add the `artifactWorkspace` property to any artifact definition:

```yaml
raw:
  MyTable:
    # ... other properties ...
    artifactType: pipeline
    artifactName: pl_my_pipeline
    artifactWorkspace: shared-pipelines-workspace  # Optional: specify target workspace

silver:
  ProcessedData:
    # ... other properties ...
    artifactType: notebook
    artifactName: NB_ProcessData
    artifactWorkspace: analytics-notebooks-workspace  # Optional: specify target workspace
```

### Behavior

- **When `artifactWorkspace` is specified**: The artifact is executed in the named workspace
- **When `artifactWorkspace` is omitted or empty**: The artifact is executed in the current workspace
- **Workspace Resolution**: Workspace names are resolved to IDs at runtime
- **Error Handling**: Execution fails if the specified workspace or artifact is not found

### Requirements

- User must have appropriate permissions in target workspaces
- Target workspace must contain the specified artifact
- Network connectivity between workspaces

## Using the Validation Scripts

### Python Validation Script

**Prerequisites:**
```bash
pip install jsonschema pyyaml
```

**Basic Usage:**
```bash
python validate_framework.py framework.yml
```

**Advanced Usage:**
```bash
# Use custom schema file
python validate_framework.py framework.yml --schema custom-schema.json

# Enable strict validation (includes dependency checks)
python validate_framework.py framework.yml --strict

# Show help
python validate_framework.py --help
```

### PowerShell Validation Script

**Prerequisites:**
```powershell
Install-Module powershell-yaml -Force
```

**Basic Usage:**
```powershell
.\Validate-Config.ps1 -YamlFile "framework.yml"
```

**Advanced Usage:**
```powershell
# Use custom schema file
.\Validate-Config.ps1 -YamlFile "framework.yml" -SchemaFile "custom-schema.json"

# Enable strict validation
.\Validate-Config.ps1 -YamlFile "framework.yml" -Strict

# Show help
.\Validate-Config.ps1 -Help
```

## Validation Features

### Schema Validation
- **Type checking** - Ensures correct data types for all properties
- **Required properties** - Validates that all mandatory fields are present
- **Enum validation** - Checks that enum values are within allowed sets
- **Conditional validation** - Rules like "deltaDateColumn required when extractType=delta"

### Custom Validation (Strict Mode)
- **Dependency validation** - Checks that all `dependsOn` references point to existing objects
- **Cross-layer validation** - Ensures dependencies between layers are valid

## VS Code Integration

To enable schema validation in VS Code:

1. Install the "YAML" extension by Red Hat
2. Add this to your VS Code settings.json:

```json
{
  "yaml.schemas": {
    "./ConfigFiles/scripts/config-schema.json": "ConfigFiles/*.yml"
  }
}
```

This will provide:
- Real-time validation while editing
- IntelliSense/autocomplete
- Hover documentation
- Error highlighting

## Example Validation Output

### Successful Validation
```
✅ Validation successful! The YAML file is valid.
```

### Failed Validation
```
❌ Schema Validation Errors:
  1. Path 'raw.Person': Missing required property: flagActive
  2. Path 'silver.Person.extractType': Property should be one of: full, delta, incremental

❌ Custom Validation Errors:
  1. Gold layer 'fact_sales': dependency 'silver.NonExistentTable' not found in configuration

❌ Validation failed with 3 error(s).
```

## Schema Customization

To customize the schema for your specific needs:

1. Copy `config-schema.json` to a new file
2. Modify the properties, required fields, or enums as needed
3. Update validation scripts to use your custom schema
4. Consider adding custom validation rules in the scripts

## Best Practices

1. **Always validate** configuration files before deployment
2. **Use strict mode** in CI/CD pipelines to catch dependency issues
3. **Version control** your schema files alongside configuration files
4. **Document changes** when modifying the schema
5. **Test validation** scripts with both valid and invalid configuration files

## Troubleshooting

### Common Issues

1. **Missing dependencies**: Ensure all required packages are installed
2. **File not found**: Use absolute paths or ensure working directory is correct
3. **YAML syntax errors**: Check for proper indentation and structure
4. **Schema errors**: Verify the schema file is valid JSON/YAML

### Getting Help

- Check the validation output for specific error messages
- Use the `--help` option with scripts for usage information
- Validate your YAML syntax using online YAML validators
- Test against the provided example `framework.yml` file

## Deployment Pipeline

The Modern BI Fabric Framework includes a comprehensive Azure DevOps pipeline (`deploy-configFiles.yml`) for automated deployment of configuration files across multiple environments.

### Pipeline Overview

The pipeline follows a multi-stage approach:

1. **Discovery & Validation** - Discovers available models and validates configurations
2. **Deploy Dev** - Deploys to development environment
3. **Deploy Test** - Deploys to test environment (after dev success)
4. **Deploy Prod** - Deploys to production environment (after test success)

### Pipeline Stages

#### Stage 1: DiscoverAndValidate
- **DiscoverModels Job**: Scans configuration directories to identify available models
- **ValidateConfigurations Job**: Validates all YAML files against the schema

#### Stage 2-4: Deployment Stages (Dev/Test/Prod)
Each deployment stage:
- Uses Azure PowerShell (`AzurePowerShell@5` task)
- Executes `deploy-configOnelake.ps1` script
- Uploads configuration files to ADLS Gen2 storage
- Supports environment-specific configurations

### DevOps Configuration Requirements

#### Service Connection
- **Name**: `DevOps-Fabric Connection`
- **Type**: Azure Resource Manager
- **Authentication**: Service Principal
- **Permissions Required**:
  - Storage Blob Data Contributor on target storage accounts
  - Reader on resource groups containing storage accounts

#### Variable Group
Create a variable group named **`FabricVariables`** with the following variables:

```yaml
# Development Environment
DEV_WORKSPACE_ID: "your-dev-workspaceId"
DEV_LAKEHOUSE_ID: "your-dev-lakehouseId"

# Test Environment  
TEST_STORAGE_ACCOUNT_NAME: "your-test-workspaceId"
TEST_STORAGE_ACCOUNT_CONTAINER: "your-test-lakehouseId"

# Production Environment
PROD_STORAGE_ACCOUNT_NAME: "your-prod-workspaceId"
PROD_STORAGE_ACCOUNT_CONTAINER: "your-prod-lakehouseId"
```

#### Repository Structure
The pipeline expects this folder structure:
```
ConfigFiles/
├── environments/
│   ├── dev/
│   │   └── *.yml (development configurations)
│   ├── test/
│   │   └── *.yml (test configurations)
│   └── prod/
│       └── *.yml (production configurations)
└── scripts/
    ├── config-schema.json
    ├── validate_config.py
    ├── Validate-Config.ps1
    └── deploy-configOnelake.ps1
```

### Deployment Process

#### 1. Model Discovery
The pipeline automatically discovers available models by:
- Scanning the `ConfigFiles/environments/` directory
- Identifying unique model names across all environments
- Creating a deployment matrix for processing

#### 2. Configuration Validation
- Validates YAML syntax and structure
- Checks against JSON schema requirements
- Performs cross-reference validation in strict mode
- Fails the pipeline if validation errors are found

#### 3. Environment Deployment
For each environment (dev/test/prod):
- Authenticates using the Azure service connection
- Creates storage context using Azure PowerShell
- Uploads configuration files to OneLake
- Places files in the lakehouse file structure inside the "config" folder

#### 4. File Organization
Configuration files are stored in the target lakehouse with this structure:
```
Files/config:
├── model1.yml
├── model2.yml
└── modelN.yml
```

### Pipeline Triggers

The pipeline is configured to trigger on:
- **Main branch pushes**: Full deployment to all environments
- **Pull requests**: Validation only (no deployment)
- **Manual triggers**: Can be run manually from Azure DevOps

### Environment-Specific Configurations

#### Development Environment
- Automatic deployment on main branch commits
- Uses development storage account and container
- Intended for development and testing of new configurations

#### Test Environment  
- Deploys after successful development deployment
- Uses dedicated test storage resources
- Environment for integration testing and validation

#### Production Environment
- Deploys after successful test deployment
- Uses production storage resources
- Requires all previous stages to complete successfully

### Security Considerations

1. **Service Principal Permissions**: Add Devops service connection principal to "Member" role in workspaces
2. **Variable Groups**: Mark sensitive variables as secrets in Azure DevOps
3. **Environment Protection**: Consider adding approval gates for production deployments
4. **Access Control**: Restrict who can modify the pipeline and variable groups

### Monitoring and Troubleshooting

#### Pipeline Monitoring
- Check Azure DevOps pipeline runs for deployment status
- Review detailed logs for each stage and job
- Monitor file upload success in ADLS Gen2 storage

#### Common Deployment Issues
1. **Authentication Failures**: Verify service connection configuration
2. **Storage Access Denied**: Check service principal permissions on storage accounts
3. **Variable Not Found**: Ensure all required variables exist in FabricVariables group
4. **File Upload Failures**: Verify storage account URLs and container names

#### Debugging Steps
1. Check pipeline execution logs in Azure DevOps
2. Verify service connection has appropriate permissions
3. Confirm variable group values are correct
4. Test storage account connectivity manually
5. Validate YAML configuration files locally before deployment

### Pipeline Customization

#### Adding New Environments
1. Create new environment folder under `ConfigFiles/environments/`
2. Add environment-specific variables to FabricVariables group
3. Update pipeline YAML to include new deployment stage
4. Configure environment-specific approval requirements if needed

#### Modifying Deployment Logic
- Edit `deploy-configOnelake.ps1` for deployment behavior changes
- Update pipeline stages for workflow modifications
- Consider adding additional validation steps or custom deployment logic

### Best Practices

1. **Environment Separation**: Use different workspaces for each environment
2. **Configuration Management**: Maintain environment-specific configurations
3. **Version Control**: Keep all pipeline and script changes in source control
4. **Testing**: Test pipeline changes in development environment first
5. **Documentation**: Keep deployment documentation updated with any changes
