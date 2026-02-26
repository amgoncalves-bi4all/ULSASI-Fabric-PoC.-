# Microsoft Fabric Common Functions Library

A Python library containing common functions for Microsoft Fabric data processing operations.

## Overview

This library provides standardized functions for:
- Workspace and lakehouse operations
- Metadata management
- Data processing with PySpark and Delta Lake
- YAML configuration handling
- Technical column management

## Installation

```bash
# Install in development mode
pip install -e .

# Or build wheel
python -m build
```

## Usage

```python
import fabric_common_lib

# Optional - import all functions
from fabric_common_lib.common_functions import *

# Configure Fabric globals (CRITICAL!)
fabric_common_lib.configure_fabric_globals(notebookutils)

# Initialize Fabric context
fabric_common_lib.initialize_fabric_context()

# Get workspace and lakehouse IDs
workspace_id, bronze_id, silver_id, gold_id = fabric_common_lib.GetFabricIds()

# Use other functions
config = fabric_common_lib.GetConfigMetadataFromYaml("model", "object", "silver")
```

## Requirements

- Microsoft Fabric environment
- Python 3.8+
- PySpark 3.3+
- Delta Lake 2.4+
- sempy 0.7+

## Development

This library is designed specifically for Microsoft Fabric environments and requires the Fabric runtime context to function properly.

## Functions

### Core Functions
- `GetFabricIds()` - Get workspace and lakehouse IDs
- `initialize_fabric_context()` - Initialize global Fabric context
- `GetConfigMetadataFromYaml()` - Read YAML configuration metadata
- `SetSessionLogURI()` - Configure logging to Eventhouse

### Data Processing Functions
- `AddMetadataColumns()` - Add technical metadata columns
- `AddSkColumn()` - Add surrogate key columns
- `MergeHistoricalDim()` - Merge historical dimensions
- Various load and merge functions for standardized operations

## License

MIT License