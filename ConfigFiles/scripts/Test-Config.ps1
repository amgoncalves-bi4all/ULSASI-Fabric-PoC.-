# Simple Framework Validation Test
# This script performs a basic validation test without external dependencies

param(
    [string]$YamlFile = "framework.yml"
)

Write-Host "Framework Configuration Validation Test" -ForegroundColor Cyan
Write-Host "=" * 50 -ForegroundColor Gray

# Check if YAML file exists
if (-not (Test-Path $YamlFile)) {
    Write-Host "❌ Error: YAML file '$YamlFile' not found" -ForegroundColor Red
    exit 1
}

Write-Host "✅ YAML file found: $YamlFile" -ForegroundColor Green

# Read and check basic YAML structure
try {
    $content = Get-Content $YamlFile -Raw
    Write-Host "✅ YAML file readable" -ForegroundColor Green
    Write-Host "📊 File size: $($content.Length) characters" -ForegroundColor Blue
    
    # Basic structure checks
    $checks = @{
        "model:" = "Model definition"
        "active:" = "Active flag"
        "raw:" = "Raw layer"
        "silver:" = "Silver layer" 
        "gold:" = "Gold layer"
    }
    
    Write-Host "`n🔍 Basic Structure Checks:" -ForegroundColor Yellow
    foreach ($check in $checks.GetEnumerator()) {
        if ($content -match $check.Key) {
            Write-Host "  ✅ $($check.Value) found" -ForegroundColor Green
        } else {
            Write-Host "  ⚠️  $($check.Value) not found" -ForegroundColor Yellow
        }
    }
    
    # Count objects in each layer
    $rawCount = ([regex]::Matches($content, "(?<=raw:\s*\n)(\s+\w+:)", [System.Text.RegularExpressions.RegexOptions]::Multiline)).Count
    $silverCount = ([regex]::Matches($content, "(?<=silver:\s*\n)(\s+\w+:)", [System.Text.RegularExpressions.RegexOptions]::Multiline)).Count
    $goldCount = ([regex]::Matches($content, "(?<=gold:\s*\n)(\s+\w+:)", [System.Text.RegularExpressions.RegexOptions]::Multiline)).Count
    
    Write-Host "`n📈 Object Counts:" -ForegroundColor Yellow
    Write-Host "  Raw layer: $rawCount objects" -ForegroundColor Blue
    Write-Host "  Silver layer: $silverCount objects" -ForegroundColor Blue
    Write-Host "  Gold layer: $goldCount objects" -ForegroundColor Blue
    
}
catch {
    Write-Host "❌ Error reading YAML file: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Check if schema files exist
Write-Host "`n🔧 Schema Files:" -ForegroundColor Yellow
$schemaFiles = @("framework-schema.json", "framework-schema.yaml")
foreach ($schemaFile in $schemaFiles) {
    if (Test-Path $schemaFile) {
        Write-Host "  ✅ $schemaFile exists" -ForegroundColor Green
    } else {
        Write-Host "  ❌ $schemaFile missing" -ForegroundColor Red
    }
}

# Check if validation scripts exist
Write-Host "`n🔧 Validation Scripts:" -ForegroundColor Yellow
$validationFiles = @("validate_framework.py", "Validate-Framework.ps1")
foreach ($validationFile in $validationFiles) {
    if (Test-Path $validationFile) {
        Write-Host "  ✅ $validationFile exists" -ForegroundColor Green
    } else {
        Write-Host "  ❌ $validationFile missing" -ForegroundColor Red
    }
}

Write-Host "`n✅ Basic validation test completed!" -ForegroundColor Green
Write-Host "💡 To perform full validation, install required dependencies and use the validation scripts." -ForegroundColor Blue
