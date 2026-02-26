"""YAML Orchestrator Library

This package provides YAML-based orchestration functions for Microsoft Fabric workflows.
Extracted from NB_Read_ConfigYAML notebook for reusable library usage.
"""

__version__ = "0.1.0"

# Import main functions for easy access
try:
    from .yaml_orchestrator import (
        configure_fabric_globals,
        GetLastModelExecution,
        GetStartDates,
        GetCopyDataConfig,
        BuildGroupDependencyMap,
        ProcessEntryWithWrapper,
        load_yaml_config,
        generate_dag_from_config,
    )
    
except ImportError:
    # Handle import errors gracefully
    pass

# Make key functions easily accessible
__all__ = [
    'configure_fabric_globals',
    'GetLastModelExecution',
    'GetStartDates', 
    'GetCopyDataConfig',
    'BuildGroupDependencyMap',
    'ProcessEntryWithWrapper',
    'load_yaml_config',
    'generate_dag_from_config',
]