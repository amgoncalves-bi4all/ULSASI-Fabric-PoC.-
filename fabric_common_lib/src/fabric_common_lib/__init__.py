"""Microsoft Fabric Common Functions Library

This package provides common functions for working with Microsoft Fabric,
including data processing, metadata management, and workspace operations.
"""

__version__ = "0.1.0"

# Import main functions for easy access
try:
    from .common_functions import (
        GetFabricIds,
        initialize_fabric_context,
        GetConfigMetadataFromYaml,
        GetConfigMetadata,
        SetSessionLogURI,
        AddMetadataColumns,
        AddSkColumn,
        AddDimTechnicalColumns,
        AddFactTechnicalColumns,
        MergeHistoricalDim,
        LoadDims,
        MergeDims,
        UnkValues,
        ReplaceFactSkNull,
        InsertOverwriteFactTables,
        UpdateProcessingDate,
        ProcessDelta,
        RunPipeline,
        GetLakehouseSourcePath,
        GetSourcePathLastFileFromKQL,
        GetDateToProcessFromKQL,
        GetSourcePathLastFilesByDatesFromKQL,
        GetDatesToProcess,
        FlattenArrayAndStruct,
    )
    
    from .standardized_common_functions import (
        AddMetadataColumnsStandardizedLayer,
        # Note: Other standardized functions can be imported directly from the module if needed
        # OverwriteFunction, OverwriteReplaceWhereFunction, AppendFunction, MergeFunction
    )

    # Import module reference for globals management
    from . import common_functions as _cf_module
except ImportError as e:
    _cf_module = None
    print(f"Warning: fabric_common_lib import failed: {e}")

def configure_fabric_globals(notebook_utils=None, msspark_utils=None):
    """Configure Fabric globals - clean and simple approach.
    
    Now that __all__ prevents star imports from overriding notebookutils,
    we can use a straightforward configuration method.
    
    Args:
        notebook_utils: The notebookutils object from Fabric notebook context
        msspark_utils: The mssparkutils object (deprecated but maintained for compatibility)
        
    Returns:
        bool: True if configuration successful, False otherwise
    """
    try:
        # Input validation
        if notebook_utils is not None:
            if not (hasattr(notebook_utils, 'credentials') or 
                   hasattr(notebook_utils, 'lakehouse') or
                   hasattr(notebook_utils, 'notebook')):
                print("Warning: notebook_utils doesn't appear to be valid notebookutils object")
                return False

        # Simple approach: just call the original configure function
        if _cf_module is not None and hasattr(_cf_module, 'configure_fabric_globals'):
            return _cf_module.configure_fabric_globals(notebook_utils, msspark_utils)
        else:
            print("Error: common_functions module not available")
            return False
            
    except Exception as e:
        print(f"Configuration error: {type(e).__name__} - {str(e)}")
        return False

# Expose the common_functions module for direct access patterns
common_functions = _cf_module

# Framework-compliant __all__ export
__all__ = [
    # Core Framework Functions (Most Used)
    "GetFabricIds",
    "configure_fabric_globals",
    "initialize_fabric_context", 
    "GetConfigMetadataFromYaml",
    "GetConfigMetadata",
    "SetSessionLogURI",
    
    # Technical Columns & Metadata
    "AddMetadataColumns",
    "AddSkColumn", 
    "AddDimTechnicalColumns",
    "AddFactTechnicalColumns",
    
    # Dimension Processing
    "MergeHistoricalDim",
    "LoadDims",
    "MergeDims",
    "UnkValues",
    
    # Fact Processing
    "ReplaceFactSkNull",
    "InsertOverwriteFactTables",
    
    # Processing Control
    "UpdateProcessingDate",
    "ProcessDelta",
    
    # Pipeline & Utility Functions
    "RunPipeline",
    "GetLakehouseSourcePath",
    "GetSourcePathLastFileFromKQL",
    "GetDateToProcessFromKQL",
    "GetSourcePathLastFilesByDatesFromKQL",
    "GetDatesToProcess",
    "FlattenArrayAndStruct",
    
    # Standardized Functions
    "AddMetadataColumnsStandardizedLayer",
    
    # Module access for direct patterns
    "common_functions",
]
    
# Handle case where Fabric dependencies aren't available (fallback from initial try block)
if '_cf_module' not in locals() or _cf_module is None:
    __all__ = []
    
    def _fabric_not_available(*args, **kwargs):
        raise ImportError(
            "Microsoft Fabric dependencies not available. "
            "This library requires PySpark, Delta Lake, and sempy to function properly."
        )
    
    # Create placeholder functions
    GetFabricIds = _fabric_not_available
    configure_fabric_globals = _fabric_not_available
    initialize_fabric_context = _fabric_not_available
    common_functions = None
