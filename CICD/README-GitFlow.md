# GitFlow CI/CD for Microsoft Fabric

This directory contains the enhanced CI/CD pipelines that implement **Microsoft Fabric Git Integration documentation** and uses the recommended patterns from Microsoft's samples.

## 📋 Overview

This implementation follows the **Microsoft Fabric Git Integration documentation** and uses the recommended patterns from Microsoft's samples.

### **Environment Mappings**
- `main` branch → **Development** environment (direct Git connection - no pipeline)
- `test` branch → **Test** environment (pipeline deployment - optional if `TEST_WORKSPACE_ID` configured)
- `prod` branch → **Production** environment (pipeline deployment)

### **Key Advantages Over Legacy Approach**
- ✅ **Direct Git Integration**: Uses Fabric native Git APIs instead of ID replacement
- ✅ **Automatic Workspace Updates**: No manual refresh required in Fabric workspaces  
- ✅ **Microsoft Recommended**: Follows GitFlow best practices for Fabric
- ✅ **Enhanced Error Handling**: Comprehensive status checking and automatic fallback mechanisms
- ✅ **Simplified Maintenance**: Based on documentation and samples
- ✅ **Flexible Environment Support**: Automatically detects 2-env or 3-env setup

### 1. **Pipeline Architecture**

#### **Automatic Pipeline: `deploy-gitflow.yml`**
- **Trigger**: Pull requests to `test` and `prod` branches (proper GitFlow)
- **Path Exclusions**: CICD/**, Scripts/**, documentation files
- **Authentication**: Uses dedicated service principal for Fabric Git APIs
- **Git Integration**: Microsoft Fabric APIs with automatic connection capability

#### **Manual Pipeline: `deploy-gitflow-manual.yml`**  
- **Trigger**: Manual execution only
- **Parameters**: Target environment selection, force update option
- **Authentication**: Uses same dedicated service principal
- **Enhanced**: Comprehensive error handling and status reporting

#### **Authentication Architecture**
Following Microsoft's recommended approach:
- **Dedicated Service Principal**: Created specifically for Fabric Git operations
- **OAuth2 Client Credentials Flow**: Standard authentication pattern for service-to-service
- **Consistent Authentication**: Same service principal for both connection and updates
- **Enhanced Security**: No mixed authentication scenarios (personal + service principal)

### 2. **API Integration Pattern**

**Step 1: Authentication**
```powershell
Initialize-FabricAuthentication -ClientId $clientId -ClientSecret $clientSecret -TenantId $tenantId -PrincipalType "ServicePrincipal"
```

**Step 2: Workspace Validation**
```powershell
$workspace = Get-FabricWorkspace -WorkspaceId $workspaceId
```

**Step 3: Git Status Check**
```powershell
$gitStatus = Get-FabricGitStatus -WorkspaceId $workspaceId
```

**Step 4: Update or Connect**
```powershell
# If already connected
$result = Update-WorkspaceFromGit -WorkspaceId $workspaceId -ConflictResolution "Repository"

# If not connected (automatic fallback)
$connectionResult = Connect-WorkspaceToGit -WorkspaceId $workspaceId -GitProviderType "AzureDevOps" ...
```

### 3. **Deployment Flow**

#### **Automatic (PR-triggered)**
1. **PR created** to `test` or `prod` branch
2. **Pipeline validates** configuration and determines target environment
3. **Authenticates** using service connection
4. **Checks Git status** of target workspace
5. **Updates workspace** from Git using APIs
6. **Reports results** with comprehensive status information

#### **Manual Deployment**
1. **User selects** target environment and options
2. **Pipeline authenticates** and validates workspace access
3. **Attempts automatic connection** if workspace not connected to Git
4. **Updates workspace** content with selected conflict resolution
5. **Provides detailed feedback** and troubleshooting guidance

## 🏗️ Architecture

### Fabric Git Integration
- **Microsoft APIs**: Uses Fabric Git REST APIs for all operations
- **No Manual Refresh**: Workspaces update automatically without manual intervention
- **Conflict Resolution**: Configurable handling of workspace vs repository conflicts
- **Automatic Connection**: Can automatically connect workspaces to Git if needed
- **Enhanced Error Handling**: Comprehensive validation and fallback mechanisms

### Branch Strategy
```
main (development - direct Git connection)
├── test (test environment - pipeline deployment)  
├── prod (production - pipeline deployment)
├── feature/new-feature (merge to main)
├── feature/another-feature (merge to main)
└── hotfix/critical-fix (can deploy to any environment)
```

## 📁 Pipeline Files

### `deploy-gitflow.yml`
**Automatic GitFlow deployment pipeline**
- **Triggers**: Commits to `test` or `prod` branches only
- **Behavior**: Automatically deploys to corresponding environment
- **Method**: Uses Fabric Git APIs for direct workspace updates
- **Validation**: Pre-deployment configuration and access validation
- **Note**: `main` branch uses direct Git connection (no pipeline needed)

### `deploy-gitflow-manual.yml` 
**Manual deployment pipeline with branch selection**
- **Triggers**: Manual execution only
- **Parameters**: 
  - Source branch selection
  - Target environment selection  
  - Force update option
- **Use Cases**: Hotfixes, emergency deployments, testing scenarios

### `deployment-config.json`
**Enhanced configuration with GitFlow settings**
- **GitFlow Configuration**: Branch-to-environment mappings
- **Git Integration**: Repository connection details
- **Environment Settings**: Workspace patterns and variable mappings
- **Artifact Patterns**: File types and ID replacement rules

## 🚀 Setup Instructions

### 1. Azure DevOps Configuration

Ensure your Azure DevOps project has:
- **Service Principal**: Dedicated service principal for Fabric Git integration (following Microsoft documentation)
- **Variable Groups**: 
  - `FabricVariables` with environment workspace IDs
  - `FabricServicePrincipal` with service principal credentials

#### **Service Principal Setup**

Following Microsoft's documentation for [Fabric Git integration with service principal](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-with-service-principal), configure:

#### **Variable Group: FabricServicePrincipal**
```yaml
FABRIC_CLIENT_ID: "your-service-principal-client-id"
FABRIC_CLIENT_SECRET: "your-service-principal-client-secret"  
FABRIC_TENANT_ID: "your-azure-tenant-id"
```

**Important**: Follow Microsoft's guide to:
1. Create service principal in Azure AD
2. Grant necessary Fabric permissions 
3. Add service principal to workspaces with Admin role

### **Flexible Environment Configuration**

The GitFlow pipelines automatically detect your environment setup:

##### **2-Environment Setup** (main → dev, prod → prod)
```yaml
# Variable Group: FabricVariables  
DEV_WORKSPACE_ID: "your-dev-workspace-id"
PROD_WORKSPACE_ID: "your-prod-workspace-id"
# TEST_WORKSPACE_ID: Leave empty or don't configure

# Optional lakehouse IDs for YAML config deployment
DEV_BRONZE_LAKEHOUSE_ID: "your-dev-bronze-id"
DEV_SILVER_LAKEHOUSE_ID: "your-dev-silver-id"
DEV_GOLD_LAKEHOUSE_ID: "your-dev-gold-id"
PROD_BRONZE_LAKEHOUSE_ID: "your-prod-bronze-id"
PROD_SILVER_LAKEHOUSE_ID: "your-prod-silver-id"
PROD_GOLD_LAKEHOUSE_ID: "your-prod-gold-id"
```

**Branch Flow:**
- `main` → Development environment (direct Git connection - no pipeline)
- `prod` → Production environment (pipeline deployment)
- **No test branch needed**

##### **3-Environment Setup** (main → dev, test → test, prod → prod)
```yaml
# Variable Group: FabricVariables
DEV_WORKSPACE_ID: "your-dev-workspace-id"  
TEST_WORKSPACE_ID: "your-test-workspace-id"  # ← Add this for 3-env setup
PROD_WORKSPACE_ID: "your-prod-workspace-id"

# Optional lakehouse IDs for YAML config deployment
DEV_BRONZE_LAKEHOUSE_ID: "your-dev-bronze-id"
DEV_SILVER_LAKEHOUSE_ID: "your-dev-silver-id"
DEV_GOLD_LAKEHOUSE_ID: "your-dev-gold-id"
TEST_BRONZE_LAKEHOUSE_ID: "your-test-bronze-id"
TEST_SILVER_LAKEHOUSE_ID: "your-test-silver-id"
TEST_GOLD_LAKEHOUSE_ID: "your-test-gold-id"
PROD_BRONZE_LAKEHOUSE_ID: "your-prod-bronze-id"
PROD_SILVER_LAKEHOUSE_ID: "your-prod-silver-id"
PROD_GOLD_LAKEHOUSE_ID: "your-prod-gold-id"
```

**Branch Flow:**
- `main` → Development environment (direct Git connection - no pipeline)
- `test` → Test environment (pipeline deployment)  
- `prod` → Production environment (pipeline deployment)

##### **Dynamic Detection**
The pipeline automatically detects your setup by checking if `TEST_WORKSPACE_ID` is configured in the variable group. No code changes needed to switch between 2-env and 3-env setups!

### 2. Repository Branch Setup

Create and configure the GitFlow branches based on your environment setup:

#### **For 2-Environment Setup:**
```bash
# main branch already exists and connects directly to dev workspace
# Create prod branch from main
git checkout main
git checkout -b prod
git push -u origin prod

# Set branch protection rules in Azure DevOps for main and prod
```

#### **For 3-Environment Setup:**
```bash
# main branch already exists and connects directly to dev workspace
# Create test branch from main
git checkout main
git checkout -b test
git push -u origin test

# Create prod branch from main  
git checkout main
git checkout -b prod
git push -u origin prod

# Set branch protection rules in Azure DevOps for main, test, and prod
```

**Note:** 
- `main` branch is directly connected to the dev workspace (no pipeline needed)
- `test` branch is only needed if you configure `TEST_WORKSPACE_ID`
- Only `test` and `prod` branches trigger pipeline deployments

### 3. Fabric Workspace Preparation

For each target workspace:
1. **Ensure workspace exists** and is accessible by the service connection
2. **Verify permissions** - service connection needs Admin access
3. **Clear existing Git connections** (if any) or ensure they point to correct repository

### 4. Pipeline Import

Import both pipelines into Azure DevOps:
1. Go to **Pipelines** > **Create Pipeline**
2. Select **Azure Repos Git** 
3. Choose your repository
4. Select **Existing Azure Pipelines YAML file**
5. Choose `CICD/deploy-gitflow.yml` for automatic deployment
6. Repeat for `CICD/deploy-gitflow-manual.yml` for manual deployment

## 🔄 Workflow Examples

### **2-Environment Development Flow** (main → dev, prod → prod)
```bash
# 1. Create feature branch from main
git checkout main
git pull origin main
git checkout -b feature/new-lakehouse-config

# 2. Make changes and commit
git add .
git commit -m "Add new lakehouse configuration"
git push origin feature/new-lakehouse-config

# 3. Create pull request to main
# 4. After approval and merge to main -> Auto-updates DEV workspace (direct Git connection)

# 5. When ready for production
git checkout prod
git merge main
git push origin prod  # -> Triggers pipeline deployment to PROD
```

### **3-Environment Development Flow** (main → dev, test → test, prod → prod)
```bash
# 1. Create feature branch from main
git checkout main
git pull origin main
git checkout -b feature/new-lakehouse-config

# 2. Make changes and commit
git add .
git commit -m "Add new lakehouse configuration"
git push origin feature/new-lakehouse-config

# 3. Create pull request to main
# 4. After approval and merge to main -> Auto-updates DEV workspace (direct Git connection)

# 5. When ready for testing
git checkout test
git merge main
git push origin test  # -> Triggers pipeline deployment to TEST

# 6. When ready for production
git checkout prod
git merge test  # or merge from main if test validation passed
git push origin prod  # -> Triggers pipeline deployment to PROD
```

### **Emergency Hotfix Flow**

#### **2-Environment Hotfix** (main → dev, prod → prod)
```bash
# 1. Create hotfix branch from main or prod (depending on urgency)
git checkout main  # or prod for critical production fixes
git checkout -b hotfix/critical-security-fix

# 2. Make critical changes
git add .
git commit -m "Fix critical security vulnerability"
git push origin hotfix/critical-security-fix

# 3. Test hotfix in DEV first (merge to main for direct Git update)
git checkout main
git merge hotfix/critical-security-fix
git push origin main  # -> Auto-updates DEV workspace (direct Git connection)

# 4. After validation, deploy to production
git checkout prod
git merge main  # or merge hotfix branch directly if urgent
git push origin prod  # -> Triggers pipeline deployment to PROD

# 5. Cleanup - delete hotfix branch after successful deployment
git branch -d hotfix/critical-security-fix
git push origin --delete hotfix/critical-security-fix
```

#### **3-Environment Hotfix** (main → dev, test → test, prod → prod)
```bash
# 1. Create hotfix branch from main or prod
git checkout main  # or prod for critical production fixes
git checkout -b hotfix/critical-security-fix

# 2. Make critical changes
git add .
git commit -m "Fix critical security vulnerability"
git push origin hotfix/critical-security-fix

# 3. Test hotfix in DEV first (merge to main for direct Git update)
git checkout main
git merge hotfix/critical-security-fix
git push origin main  # -> Auto-updates DEV workspace (direct Git connection)

# 4. Deploy to TEST environment for validation
git checkout test
git merge main
git push origin test  # -> Triggers pipeline deployment to TEST

# 5. After validation, deploy to production
git checkout prod
git merge test  # or merge from main if test passed
git push origin prod  # -> Triggers pipeline deployment to PROD

# 6. Cleanup - delete hotfix branch after successful deployment
git branch -d hotfix/critical-security-fix
git push origin --delete hotfix/critical-security-fix
```

## 🔧 Configuration Details

### GitFlow Settings (`deployment-config.json`)
```json
{
  "gitFlow": {
    "enabled": true,
    "primaryBranches": {
      "main": "development",
      "test": "test", 
      "prod": "production"
    },
    "pipelineTriggeredBranches": {
      "test": "test",
      "prod": "production"
    },
    "directGitBranches": {
      "main": "development"
    },
    "gitIntegration": {
      "organizationName": "$(System.TeamProject)",
      "projectName": "$(System.TeamProject)", 
      "repositoryName": "$(Build.Repository.Name)"
    }
  }
}
```

### Environment Mappings
Each environment configuration includes:
- **Primary Branch**: The dedicated branch for this environment
- **Workspace Pattern**: How to identify the workspace name
- **Variable Mappings**: Azure DevOps variables for workspace/lakehouse IDs

### 6. **Enhanced Error Handling**

**Git Connection Issues**:
- Automatic detection of connection problems
- Fallback to automatic connection using service principal
- Clear manual setup instructions if automation fails
- Reference to Microsoft documentation

**Authentication Issues**:
- Validation of Azure context and service connections
- Clear error messages for missing credentials
- Support for both interactive and automated scenarios

**Workspace Issues**:
- Validation of workspace access and permissions
- Clear identification of workspace connectivity problems
- Detailed status reporting throughout the process


## Required Setup

### **📋 Prerequisites Overview**
The updated GitFlow implementation requires proper Git credentials configuration for Service Principal access to workspace Git operations. This follows the correct Microsoft Fabric Git API flow.

### **🔑 Service Principal Authentication** 

#### **1. Create Service Principal**
```bash
# Using Azure CLI (recommended method from Microsoft docs)
az ad sp create-for-rbac --name "fabric-git-automation" --role contributor
```

#### **2. Grant Fabric Permissions**
- Add service principal to Fabric tenant settings
- Enable service principal access in Fabric admin portal
- Grant **Fabric Administrator** or **Power BI Service Admin** role
- Grant necessary API permissions in Azure AD

### **🔗 Git Connection Setup (CRITICAL)**

Following Microsoft's documentation, the service principal must have proper Git credentials configured:

#### **1. Create Git Connection with Service Principal**
📖 **Documentation**: [Git Integration with Service Principal](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-with-service-principal)

1. **Create Git Provider Connection**:
   - Go to Fabric Admin Portal → Connections
   - Create new Azure DevOps connection using service principal credentials
   - Note the **Connection ID** for later use

#### **2. Configure Workspace Git Connection**
📖 **Documentation**: [Connect workspace to Git](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started)

1. **Connect Each Workspace to Git**:
   - Use Fabric Portal → Workspace Settings → Git Integration
   - Connect to your repository using the service principal connection
   - Configure appropriate branches (main→dev, test→test, prod→prod)

#### **3. Grant Permissions** 

**Workspace Permissions** (for each workspace):
- Grant service principal **Admin** or **Contributor** access to each workspace

**Connection Permissions**:  
- Ensure service principal can access the Git connection created in step 1

**Repository Permissions**:
- Grant service principal **Contribute** permissions to the Azure DevOps repository
- Ensure access to all relevant branches (main, test, prod)

**Fabric API Permissions**:
- Service principal needs **Fabric Administrator** role for Git API access
- Alternative: **Power BI Service Administrator** with Fabric API permissions

### **🔧 Configure Variable Groups**

#### **FabricServicePrincipal** group must contain:
- `FABRIC_CLIENT_ID` - Service Principal Application (Client) ID
- `FABRIC_CLIENT_SECRET` - Service Principal Client Secret
- `FABRIC_TENANT_ID` - Azure Active Directory Tenant ID

#### **FabricVariables** group must contain:
- `DEV_WORKSPACE_ID` - Development workspace ID
- `TEST_WORKSPACE_ID` - Test workspace ID (optional for 2-env setup)
- `PROD_WORKSPACE_ID` - Production workspace ID
- Additional lakehouse ID variables as defined in configuration

### **📊 Git Credentials Flow**
The pipeline now follows the correct Microsoft API flow:

1. **Get Token** → Service principal authentication
2. **List Connections** → Find appropriate Git connection by repository
3. **Update Git Credentials** → Associate service principal with workspace Git connection  
4. **Verify Git Credentials** → Confirm service principal has access
5. **Get Git Status** → Now works correctly with proper credentials

### **⚠️ Common Issues & Solutions**

**"Git status API fails"** → Service principal lacks Git credentials for workspace
- **Solution**: Ensure Git connection is created with service principal credentials
- **Verify**: Use `Get-WorkspaceGitCredentials` to check credential configuration

**"No Git connections found"** → Service principal can't access Git connections
- **Solution**: Create Git connection using service principal, not personal account
- **Verify**: Service principal has access to the connection in Fabric Admin Portal

**"Workspace not connected to Git"** → Manual connection required
- **Solution**: Connect workspace to Git using Fabric Portal with the service principal connection
- **Verify**: Check workspace Git settings show the correct repository and branch

### **🚀 Fabric Workspace Setup**
**Prerequisites**: Service principal Git connection and workspace Git connection must be configured manually before pipeline execution.

**Development Environment**: 
- Direct Git connection to `main` branch with service principal connection
- Automatic sync enabled for immediate updates
- No pipeline deployment needed

**Test/Production Environments**: 
- Connected to respective branches (`test`, `prod`) with service principal connection
- Pipeline handles deployment via Git API
- Service principal manages Git operations through proper credentials flow

## 🛠️ PowerShell Module Functions

The `FabricGitDeployment.psm1` module implements the correct Microsoft Fabric Git API flow:

### **Authentication & Workspace Management**
- **`Initialize-FabricAuthentication`**: Service principal OAuth2 authentication
- **`Get-FabricWorkspace`**: Workspace access validation using API

### **Git Credentials Management**
- **`Get-FabricConnections`**: Lists available Git provider connections
- **`Update-WorkspaceGitCredentials`**: Associates service principal with workspace Git connection
- **`Get-WorkspaceGitCredentials`**: Verifies service principal Git access
- **`Initialize-WorkspaceGitCredentials`**: Complete credentials initialization flow

### **Git Operations** 
- **`Get-FabricGitConnection`**: Gets workspace Git connection details
- **`Get-FabricGitStatus`**: Gets workspace Git items status (works after credentials setup)
- **`Update-WorkspaceFromGit`**: Updates workspace content using API pattern
- **`Connect-WorkspaceToGit`**: Connects workspace to Git repository (legacy - use manual setup)

### **Master Orchestration**
- **`Invoke-FabricGitDeployment`**: Complete deployment workflow with proper credentials flow

## Benefits

### **Reliability**
- Uses Microsoft APIs instead of custom implementations
- Follows Microsoft's recommended patterns and best practices
- Automatic fallback mechanisms for connection issues

### **Maintainability** 
- Based on documentation and samples
- Reduced custom code that needs maintenance
- Clear separation between infrastructure and business logic

### **Visibility**
- Comprehensive status reporting at each step
- Clear error messages with actionable guidance
- Detailed logging of all operations

### **Flexibility**
- Supports both automatic and manual deployment scenarios
- Handles both connected and unconnected workspace scenarios
- Works with various authentication methods

## Testing Recommendations

### **Manual Pipeline Testing**
1. Run manual pipeline with `development` environment
2. Verify authentication and workspace access
3. Test with both connected and unconnected workspaces
4. Validate error handling with invalid parameters

### **Automatic Pipeline Testing**  
1. Create test PR to `test` branch with actual content changes
2. Verify pipeline triggers correctly on PR creation
3. Validate proper environment detection and deployment
4. Test path exclusions work correctly (CICD/** changes ignored)

### **Error Scenario Testing**
1. Test with invalid workspace IDs
2. Test with missing service connection permissions  
3. Test with workspace not connected to Git
4. Verify automatic connection fallback works

## 🚨 Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify service connection has Fabric API access
   - Check workspace permissions for service connection
   - Ensure service principal has proper Fabric permissions

2. **Git Integration Failures** 
   - Ensure workspace isn't already connected to different repository
   - Verify branch exists and is accessible
   - Check repository permissions
   - Try automatic connection fallback via pipeline

3. **Workspace Not Found**
   - Validate workspace ID variables in FabricVariables group
   - Ensure workspace exists and is accessible
   - Check service connection permissions to workspace

4. **Conflict Resolution**
   - Use manual pipeline with Force Update for overriding workspace conflicts
   - Check Git status before deployment using APIs

### Monitoring Deployments

1. **Pipeline Logs**: Check Azure DevOps pipeline execution logs with enhanced error reporting
2. **Fabric Workspace**: Monitor workspace refresh status in Fabric portal
3. **Git Status**: Use `Get-FabricGitStatus` function to check connection state
4. **API Status**: Enhanced logging shows detailed API response information

## References

- **Microsoft Fabric Git Integration**: https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-automation
- **Fabric Samples**: https://github.com/microsoft/fabric-samples/tree/main/features-samples/git-integration
- **Service Principal Setup**: https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-with-service-principal
- **Fabric REST APIs**: https://learn.microsoft.com/en-us/rest/api/fabric/core/git

## 📈 Benefits of GitFlow Approach

1. **Microsoft APIs**: Uses supported and maintained APIs instead of custom implementations
2. **Automatic Deployment**: No manual refresh needed in workspaces
3. **Branch Protection**: Each environment has dedicated branch protection
4. **Enhanced Error Handling**: Comprehensive validation and automatic fallback mechanisms
5. **Audit Trail**: Complete Git history for all environment changes
6. **Rollback Capability**: Easy rollback using Git history
7. **Parallel Development**: Multiple environments can be developed independently
8. **Automatic Connection**: Can automatically connect workspaces to Git when needed

## 🔮 Future Enhancements

- **Rollback Pipeline**: Automated rollback to previous Git commits
- **Environment Promotion**: Streamlined promotion between environments
- **Validation Gates**: Pre-deployment validation and testing
- **Notification Integration**: Teams/Slack notifications for deployments
- **Performance Monitoring**: Deployment time and success rate tracking
- **Advanced Conflict Resolution**: More sophisticated merge strategies

---

For questions or issues with the GitFlow setup, consult the pipeline logs and Fabric workspace Git status, or refer to Microsoft's Fabric CI/CD documentation and the enhanced troubleshooting guidance above.