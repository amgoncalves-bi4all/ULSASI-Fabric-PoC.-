# Framework YAML Validator (PowerShell)
# 
# This script validates Modern BI Fabric Framework YAML configuration files
# against the defined JSON schema using PowerShell.
#
# Usage:
#   .\Validate-Config.ps1 -YamlFile "framework.yml"
#   .\Validate-Config.ps1 -YamlFile "framework.yml" -SchemaFile "custom-schema.json" -Strict

param(
    [Parameter(Mandatory = $true)]
    [string]$YamlFile,
    
    [Parameter(Mandatory = $false)]
    [string]$SchemaFile = "config-schema.json",
    
    [Parameter(Mandatory = $false)]
    [switch]$Strict,
    
    [Parameter(Mandatory = $false)]
    [switch]$Help
)

if ($Help) {
    Write-Host @"
Framework YAML Validator

This script validates Modern BI Fabric Framework YAML configuration files
against the defined JSON schema.

USAGE:
    .\Validate-Config.ps1 -YamlFile <path> [-SchemaFile <path>] [-Strict] [-Help]

PARAMETERS:
    -YamlFile    : Path to the YAML configuration file to validate (required)
    -SchemaFile  : Path to the JSON schema file (default: config-schema.json)
    -Strict      : Enable strict validation (includes custom dependency checks)
    -Help        : Show this help message

EXAMPLES:
    .\Validate-Framework.ps1 -YamlFile "framework.yml"
    .\Validate-Framework.ps1 -YamlFile "framework.yml" -Strict
    .\Validate-Framework.ps1 -YamlFile "framework.yml" -SchemaFile "custom-schema.json"

REQUIREMENTS:
    - PowerShell 5.1 or later
    - powershell-yaml module (Install-Module powershell-yaml)
"@
    exit 0
}

# Check if required modules are installed
if (-not (Get-Module -ListAvailable -Name powershell-yaml)) {
    Write-Host "❌ Error: powershell-yaml module is required but not installed." -ForegroundColor Red
    Write-Host "Please install it with: Install-Module powershell-yaml -Force" -ForegroundColor Yellow
    exit 1
}

Import-Module powershell-yaml -Force

function Test-FileExists {
    param([string]$FilePath, [string]$FileType)
    
    if (-not (Test-Path $FilePath)) {
        Write-Host "❌ Error: $FileType file not found at '$FilePath'" -ForegroundColor Red
        exit 1
    }
}

function Get-JsonSchema {
    param([string]$SchemaPath)
    
    try {
        $schemaContent = Get-Content $SchemaPath -Raw -Encoding UTF8
        return $schemaContent | ConvertFrom-Json
    }
    catch {
        Write-Host "❌ Error: Failed to load or parse schema file: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
}

function Get-YamlData {
    param([string]$YamlPath)
    
    try {
        $yamlContent = Get-Content $YamlPath -Raw -Encoding UTF8
        return $yamlContent | ConvertFrom-Yaml
    }
    catch {
        Write-Host "❌ Error: Failed to load or parse YAML file: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
}

function Test-RequiredProperties {
    param($Data, $Schema, [string]$Path = "")
    
    $errors = @()
    
    if ($Schema.required) {
        foreach ($requiredProp in $Schema.required) {
            if (-not $Data.PSObject.Properties.Name.Contains($requiredProp)) {
                $fullPath = if ($Path) { "$Path.$requiredProp" } else { $requiredProp }
                $errors += "Missing required property: $fullPath"
            }
        }
    }
    
    return $errors
}

function Test-PropertyTypes {
    param($Data, $Schema, [string]$Path = "")
    
    $errors = @()
    
    if ($Schema.properties) {
        foreach ($propName in $Data.PSObject.Properties.Name) {
            if ($Schema.properties.PSObject.Properties.Name.Contains($propName)) {
                $propSchema = $Schema.properties.$propName
                $propValue = $Data.$propName
                $fullPath = if ($Path) { "$Path.$propName" } else { $propName }
                
                # Basic type checking
                if ($propSchema.type) {
                    $actualType = if ($propValue -eq $null) { "null" } 
                                  elseif ($propValue -is [bool]) { "boolean" }
                                  elseif ($propValue -is [int] -or $propValue -is [long]) { "integer" }
                                  elseif ($propValue -is [double] -or $propValue -is [float]) { "number" }
                                  elseif ($propValue -is [string]) { "string" }
                                  elseif ($propValue -is [array]) { "array" }
                                  elseif ($propValue -is [hashtable] -or $propValue.GetType().Name -eq "PSCustomObject") { "object" }
                                  else { "unknown" }
                    
                    if ($propSchema.type -ne $actualType -and -not ($propSchema.type -eq "number" -and $actualType -eq "integer")) {
                        $errors += "Property '$fullPath' should be of type '$($propSchema.type)', but is '$actualType'"
                    }
                }
                
                # Enum validation
                if ($propSchema.enum -and $propValue -notin $propSchema.enum) {
                    $enumValues = $propSchema.enum -join ", "
                    $errors += "Property '$fullPath' has invalid value '$propValue'. Valid values are: $enumValues"
                }
                
                # Recursive validation for objects
                if ($propSchema.type -eq "object" -and $propValue -is [PSCustomObject]) {
                    $errors += Test-RequiredProperties $propValue $propSchema $fullPath
                    $errors += Test-PropertyTypes $propValue $propSchema $fullPath
                }
            }
        }
    }
    
    return $errors
}

function Test-DependencyReferences {
    param($YamlData)
    
    $errors = @()
    $availableObjects = @()
    
    # Collect all available objects from all layers
    foreach ($layer in @('raw', 'silver', 'gold')) {
        if ($YamlData.PSObject.Properties.Name.Contains($layer)) {
            foreach ($objName in $YamlData.$layer.PSObject.Properties.Name) {
                $availableObjects += "$layer.$objName"
            }
        }
    }
    
    # Check dependencies in silver layer
    if ($YamlData.PSObject.Properties.Name.Contains('silver')) {
        foreach ($objName in $YamlData.silver.PSObject.Properties.Name) {
            $objConfig = $YamlData.silver.$objName
            if ($objConfig.PSObject.Properties.Name.Contains('dependsOn')) {
                foreach ($dep in $objConfig.dependsOn) {
                    if ($dep -notin $availableObjects) {
                        $errors += "Silver layer '$objName': dependency '$dep' not found in configuration"
                    }
                }
            }
        }
    }
    
    # Check dependencies in gold layer
    if ($YamlData.PSObject.Properties.Name.Contains('gold')) {
        foreach ($objName in $YamlData.gold.PSObject.Properties.Name) {
            $objConfig = $YamlData.gold.$objName
            if ($objConfig.PSObject.Properties.Name.Contains('dependsOn')) {
                foreach ($dep in $objConfig.dependsOn) {
                    if ($dep -notin $availableObjects) {
                        $errors += "Gold layer '$objName': dependency '$dep' not found in configuration"
                    }
                }
            }
        }
    }
    
    return $errors
}

function Test-ArtifactParamsConfig {
    param($YamlData)
    
    $errors = @()
    
    function Test-LayerArtifactParams {
        param($LayerName, $LayerData)
        
        foreach ($objName in $LayerData.PSObject.Properties.Name) {
            $objConfig = $LayerData.$objName
            if ($objConfig.PSObject.Properties.Name.Contains('artifactParams')) {
                $artifactParams = $objConfig.artifactParams
                
                # Check if it's object format (only supported format)
                if ($artifactParams -is [PSCustomObject]) {
                    foreach ($property in $artifactParams.PSObject.Properties) {
                        $sourceAttr = $property.Name
                        $targetParam = $property.Value
                        
                        if ([string]::IsNullOrWhiteSpace($sourceAttr)) {
                            $errors += "$($LayerName.Substring(0,1).ToUpper() + $LayerName.Substring(1)) layer '$objName': artifactParams source attribute key cannot be empty"
                        } elseif (-not ($sourceAttr -match '^[a-zA-Z_][a-zA-Z0-9_-]*$')) {
                            $errors += "$($LayerName.Substring(0,1).ToUpper() + $LayerName.Substring(1)) layer '$objName': artifactParams source attribute '$sourceAttr' contains invalid characters"
                        }
                        
                        if (-not ($targetParam -is [string]) -or [string]::IsNullOrWhiteSpace($targetParam)) {
                            $errors += "$($LayerName.Substring(0,1).ToUpper() + $LayerName.Substring(1)) layer '$objName': artifactParams target parameter '$targetParam' cannot be empty"
                        } elseif (-not ($targetParam -match '^[a-zA-Z_][a-zA-Z0-9_]*$')) {
                            $errors += "$($LayerName.Substring(0,1).ToUpper() + $LayerName.Substring(1)) layer '$objName': artifactParams target parameter '$targetParam' is not a valid parameter name"
                        }
                    }
                }
                else {
                    $errors += "$($LayerName.Substring(0,1).ToUpper() + $LayerName.Substring(1)) layer '$objName': artifactParams must be an object mapping from source attributes to target parameter names"
                }
            }
        }
    }
    
    # Check all layers
    foreach ($layer in @('raw', 'silver', 'gold')) {
        if ($YamlData.PSObject.Properties.Name.Contains($layer)) {
            Test-LayerArtifactParams $layer $YamlData.$layer
        }
    }
    
    return $errors
}

# Main validation logic
Write-Host "Validating $YamlFile against $SchemaFile" -ForegroundColor Cyan
Write-Host ("-" * 60) -ForegroundColor Gray

# Check if files exist
Test-FileExists $YamlFile "YAML"
Test-FileExists $SchemaFile "Schema"

# Load files
$schema = Get-JsonSchema $SchemaFile
$yamlData = Get-YamlData $YamlFile

# Perform basic schema validation
$schemaErrors = @()
$schemaErrors += Test-RequiredProperties $yamlData $schema
$schemaErrors += Test-PropertyTypes $yamlData $schema

# Validate each layer if it exists
foreach ($layer in @('raw', 'silver', 'gold')) {
    if ($yamlData.PSObject.Properties.Name.Contains($layer) -and $schema.definitions.PSObject.Properties.Name.Contains("${layer}LayerObject")) {
        $layerSchema = $schema.definitions."${layer}LayerObject"
        foreach ($objName in $yamlData.$layer.PSObject.Properties.Name) {
            $objData = $yamlData.$layer.$objName
            $schemaErrors += Test-RequiredProperties $objData $layerSchema "$layer.$objName"
            $schemaErrors += Test-PropertyTypes $objData $layerSchema "$layer.$objName"
        }
    }
}

# Perform custom validations if strict mode is enabled
$customErrors = @()
if ($Strict) {
    $customErrors += Test-DependencyReferences $yamlData
    $customErrors += Test-ArtifactParamsConfig $yamlData
}

# Report results
$totalErrors = $schemaErrors.Count + $customErrors.Count

if ($schemaErrors.Count -gt 0) {
    Write-Host "❌ Schema Validation Errors:" -ForegroundColor Red
    for ($i = 0; $i -lt $schemaErrors.Count; $i++) {
        Write-Host "  $($i + 1). $($schemaErrors[$i])" -ForegroundColor Red
    }
    Write-Host ""
}

if ($customErrors.Count -gt 0) {
    Write-Host "❌ Custom Validation Errors:" -ForegroundColor Red
    for ($i = 0; $i -lt $customErrors.Count; $i++) {
        Write-Host "  $($i + 1). $($customErrors[$i])" -ForegroundColor Red
    }
    Write-Host ""
}

if ($totalErrors -eq 0) {
    Write-Host "✅ Validation successful! The YAML file is valid." -ForegroundColor Green
    exit 0
} else {
    Write-Host "❌ Validation failed with $totalErrors error(s)." -ForegroundColor Red
    exit 1
}
