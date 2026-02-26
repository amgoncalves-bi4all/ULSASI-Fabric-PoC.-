# Deployment script using Azure PowerShell for ADLS Gen2
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("dev", "test", "prod")]
    [string]$Environment,
    
    [Parameter(Mandatory = $true)]
    [string]$SourcePath,
    
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,
    
    [Parameter(Mandatory = $true)]
    [string]$LakehouseId,
    
    [Parameter(Mandatory = $false)]
    [string]$AvailableModels = "[]"
)

Write-Host "Deploying configuration for environment: $Environment" -ForegroundColor Green
Write-Host "Source path: $SourcePath" -ForegroundColor Cyan
Write-Host "WorkspaceId: $WorkspaceId" -ForegroundColor Cyan
Write-Host "LakehouseId: $LakehouseId" -ForegroundColor Cyan

# Parse available models from JSON
$models = @()
if ($AvailableModels -and $AvailableModels -ne "[]") {
    try {
        $models = $AvailableModels | ConvertFrom-Json
        Write-Host "Available models: $($models -join ', ')" -ForegroundColor Cyan
    }
    catch {
        Write-Warning "Could not parse available models, discovering from source path"
    }
}

# If no models provided or parsing failed, discover from source path
if ($models.Count -eq 0) {
    $configFiles = Get-ChildItem -Path $SourcePath -Filter "*.yml" -File
    if ($configFiles.Count -eq 0) {
        Write-Warning "No YAML configuration files found in $SourcePath"
        Write-Host "This might be expected if $Environment environment configs don't exist yet" -ForegroundColor Yellow
        exit 0
    }
    # Exclude inferredMembersConfig from model discovery as it's not a model configuration
    $models = $configFiles | ForEach-Object { 
        $fileName = [System.IO.Path]::GetFileNameWithoutExtension($_.Name)
        if ($fileName -ne "inferredMembersConfig") {
            $fileName
        }
    } | Where-Object { $_ -ne $null }
}

$deployedCount = 0
$skippedCount = 0

# Function to upload file to ADLS Gen2 using Azure PowerShell
function Invoke-ADLSGen2Upload {
    param(
        [string]$ContainerName,
        [string]$FilePath,
        [string]$BlobPath,
        [object]$StorageContext
    )
    
    try {
        Write-Host "Uploading file using Azure PowerShell cmdlets..."
        Write-Host "  Container: $ContainerName"
        Write-Host "  Destination Path: $BlobPath"
        Write-Host "  Source File: $FilePath"
        
        # Use New-AzDataLakeGen2Item to upload the file
        $result = New-AzDataLakeGen2Item -Context $StorageContext -FileSystem $ContainerName -Path $BlobPath -Source $FilePath -Force
        
        if ($result) {
            Write-Host "Successfully uploaded to OneLake ADLS Gen2" -ForegroundColor Green
            Write-Host "  File URI: $($result.Path)" -ForegroundColor Gray
            return $true
        } else {
            Write-Error "New-AzDataLakeGen2Item returned null result"
            return $false
        }
    }
    catch {
        Write-Error "ADLS Gen2 upload failed: $($_.Exception.Message)"
        Write-Error "Full error details: $($_.Exception.ToString())"
        return $false
    }
}

# Install and import required Azure PowerShell modules
Write-Host "Checking Azure PowerShell modules..." -ForegroundColor Yellow

try {
    # Check if Az.Storage module is available
    $azStorageModule = Get-Module -ListAvailable -Name "Az.Storage" | Select-Object -First 1
    if (-not $azStorageModule) {
        Write-Host "Installing Az.Storage module..." -ForegroundColor Yellow
        Install-Module -Name Az.Storage -Force -AllowClobber -Scope CurrentUser
    }
    
    # Import the module
    Import-Module Az.Storage -Force
    Write-Host "Az.Storage module loaded successfully" -ForegroundColor Green
}
catch {
    Write-Error "Failed to install/import Az.Storage module: $($_.Exception.Message)"
    exit 1
}

# Get Azure context and create storage context
Write-Host "Setting up Azure PowerShell context..." -ForegroundColor Yellow

try {
    # Get current Azure context
    $azContext = Get-AzContext
    if (-not $azContext) {
        Write-Error "No Azure context found. Please ensure Azure CLI or Azure PowerShell is authenticated."
        exit 1
    }
    
    Write-Host "Using Azure context: $($azContext.Account.Id)" -ForegroundColor Green
    
    # Create storage context using the current Azure credentials
    $storageContext = New-AzStorageContext -StorageAccountName 'onelake' -UseConnectedAccount -endpoint 'fabric.microsoft.com'

    Write-Host "Storage context created successfully" -ForegroundColor Green
}
catch {
    Write-Error "Failed to create storage context: $($_.Exception.Message)"
    exit 1
}

# Prepare container details
$actualContainer = $WorkspaceId
$containerPath = "$LakehouseId/Files/config"

Write-Host "Container details:"
Write-Host "  Actual container: $actualContainer"
Write-Host "  Container path: $containerPath"

# Deploy inferredMembersConfig.yml separately (it's not a model configuration)
$inferredMembersFile = Join-Path $SourcePath "inferredMembersConfig.yml"
if (Test-Path $inferredMembersFile) {
    Write-Host "Deploying inferredMembersConfig.yml to $Environment environment..." -ForegroundColor Yellow
    
    # Define the blob path for inferredMembersConfig
    $inferredMembersBlobPath = if ($containerPath) {
        "$containerPath/inferredMembersConfig.yml"
    } else {
        "inferredMembersConfig.yml"
    }
    
    Write-Host "Uploading inferredMembersConfig.yml to ADLS Gen2:"
    Write-Host "  Storage Account: OneLake"
    Write-Host "  Container: $actualContainer"
    Write-Host "  Blob Path: $inferredMembersBlobPath"
    Write-Host "  Source: $inferredMembersFile"
    
    $success = Invoke-ADLSGen2Upload -ContainerName $actualContainer -FilePath $inferredMembersFile -BlobPath $inferredMembersBlobPath -StorageContext $storageContext
    
    if ($success) {
        Write-Host "Successfully deployed inferredMembersConfig.yml" -ForegroundColor Green
        $deployedCount++
    } else {
        Write-Error "Failed to deploy inferredMembersConfig.yml"
        exit 1
    }
} else {
    Write-Host "inferredMembersConfig.yml not found in $Environment environment - skipping" -ForegroundColor Yellow
    Write-Host "  This is expected if inferredMembersConfig hasn't been configured for $Environment yet" -ForegroundColor Gray
    $skippedCount++
}

# Deploy each model configuration
foreach ($model in $models) {
    $configFile = Join-Path $SourcePath "$model.yml"
    
    if (Test-Path $configFile) {
        Write-Host "Deploying $model.yml to $Environment environment..." -ForegroundColor Yellow
        
        # Define the blob path
        $blobPath = if ($containerPath) {
            "$containerPath/$model.yml"
        } else {
            "$model.yml"
        }
        
        Write-Host "Uploading to ADLS Gen2:"
        Write-Host "  Storage Account: OneLake"
        Write-Host "  Container: $actualContainer"
        Write-Host "  Blob Path: $blobPath"
        Write-Host "  Source: $configFile"
        
        $success = Invoke-ADLSGen2Upload -ContainerName $actualContainer -FilePath $configFile -BlobPath $blobPath -StorageContext $storageContext
        
        if ($success) {
            Write-Host "Successfully deployed $model.yml" -ForegroundColor Green
            $deployedCount++
        } else {
            Write-Error "Failed to deploy $model.yml"
            exit 1
        }
    } else {
        Write-Host "Skipping $model.yml - file not found in $Environment environment" -ForegroundColor Yellow
        Write-Host "  This is expected if the model hasn't been configured for $Environment yet" -ForegroundColor Gray
        $skippedCount++
    }
}

Write-Host "Configuration deployment summary for $Environment environment:" -ForegroundColor Green
Write-Host "  Deployed: $deployedCount files" -ForegroundColor Green
Write-Host "  Skipped: $skippedCount files" -ForegroundColor Yellow

if ($deployedCount -eq 0 -and $skippedCount -gt 0) {
    Write-Host "No configurations were deployed. This might be expected for a new environment." -ForegroundColor Yellow
    exit 0
} elseif ($deployedCount -gt 0) {
    Write-Host "Configuration deployment completed successfully for $Environment environment" -ForegroundColor Green
    exit 0
} else {
    Write-Warning "No configuration files found to deploy"
    exit 1
}
