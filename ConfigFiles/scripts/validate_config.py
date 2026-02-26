#!/usr/bin/env python3
"""
Framework YAML Validator

This script validates Modern BI Fabric Framework YAML configuration files
against the defined JSON schema.

Usage:
    python validate_config.py <yaml_file_path>
    python validate_config.py --help

Requirements:
    pip install jsonschema pyyaml
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any, List

try:
    import yaml
    from jsonschema import validate, ValidationError, Draft7Validator
except ImportError as e:
    print(f"Error: Missing required packages. Please install them with:")
    print("pip install jsonschema pyyaml")
    sys.exit(1)


def load_schema(schema_path: Path) -> Dict[str, Any]:
    """Load the JSON schema file."""
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Schema file not found at {schema_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in schema file: {e}")
        sys.exit(1)


def load_yaml_file(yaml_path: Path) -> Dict[str, Any]:
    """Load and parse the YAML configuration file."""
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: YAML file not found at {yaml_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error: Invalid YAML syntax: {e}")
        sys.exit(1)


def validate_config(yaml_data: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
    """
    Validate the YAML configuration against the schema.
    Returns a list of validation errors (empty if valid).
    """
    errors = []
    validator = Draft7Validator(schema)
    
    for error in validator.iter_errors(yaml_data):
        # Format the error message with path context
        path = " -> ".join(str(p) for p in error.absolute_path)
        if path:
            error_msg = f"Path '{path}': {error.message}"
        else:
            error_msg = f"Root level: {error.message}"
        errors.append(error_msg)
    
    return errors


def check_dependency_references(yaml_data: Dict[str, Any]) -> List[str]:
    """
    Check if dependency references are valid (custom validation).
    """
    errors = []
    available_objects = set()
    
    # Collect all available objects from all layers
    for layer in ['raw', 'silver', 'gold']:
        if layer in yaml_data:
            for obj_name in yaml_data[layer].keys():
                available_objects.add(f"{layer}.{obj_name}")
    
    # Check dependencies in silver layer
    if 'silver' in yaml_data:
        for obj_name, obj_config in yaml_data['silver'].items():
            if 'dependsOn' in obj_config:
                for dep in obj_config['dependsOn']:
                    if dep not in available_objects:
                        errors.append(f"Silver layer '{obj_name}': dependency '{dep}' not found in configuration")
    
    # Check dependencies in gold layer
    if 'gold' in yaml_data:
        for obj_name, obj_config in yaml_data['gold'].items():
            if 'dependsOn' in obj_config:
                for dep in obj_config['dependsOn']:
                    if dep not in available_objects:
                        errors.append(f"Gold layer '{obj_name}': dependency '{dep}' not found in configuration")
    
    return errors


def check_delta_processing_config(yaml_data: Dict[str, Any]) -> List[str]:
    """
    Check deltaProcessing configuration for logical consistency (custom validation).
    """
    errors = []
    
    # Check silver layer deltaProcessing
    if 'silver' in yaml_data:
        for obj_name, obj_config in yaml_data['silver'].items():
            if 'deltaProcessing' in obj_config:
                delta_config = obj_config['deltaProcessing']
                
                # If fullProcess is False, filterColumn is required
                if not delta_config.get('fullProcess', False):
                    if 'filterColumn' not in delta_config or not delta_config['filterColumn']:
                        errors.append(f"Silver layer '{obj_name}': deltaProcessing.filterColumn is required when fullProcess is False")
                
                # Validate dateUnit is positive
                if 'dateUnit' in delta_config:
                    if not isinstance(delta_config['dateUnit'], int) or delta_config['dateUnit'] < 1:
                        errors.append(f"Silver layer '{obj_name}': deltaProcessing.dateUnit must be a positive integer")
    
    # Check gold layer deltaProcessing
    if 'gold' in yaml_data:
        for obj_name, obj_config in yaml_data['gold'].items():
            if 'deltaProcessing' in obj_config:
                delta_config = obj_config['deltaProcessing']
                
                # If fullProcess is False, filterColumn is required
                if not delta_config.get('fullProcess', False):
                    if 'filterColumn' not in delta_config or not delta_config['filterColumn']:
                        errors.append(f"Gold layer '{obj_name}': deltaProcessing.filterColumn is required when fullProcess is False")
                
                # Validate dateUnit is positive
                if 'dateUnit' in delta_config:
                    if not isinstance(delta_config['dateUnit'], int) or delta_config['dateUnit'] < 1:
                        errors.append(f"Gold layer '{obj_name}': deltaProcessing.dateUnit must be a positive integer")
    
    return errors


def check_artifact_params_config(yaml_data: Dict[str, Any]) -> List[str]:
    """
    Check artifactParams configuration for logical consistency (custom validation).
    Validates object mapping format only.
    """
    errors = []
    
    def validate_layer_artifact_params(layer_name: str, layer_data: Dict[str, Any]):
        for obj_name, obj_config in layer_data.items():
            if 'artifactParams' in obj_config:
                artifact_params = obj_config['artifactParams']
                
                # Check if it's object format (only supported format)
                if isinstance(artifact_params, dict):
                    # Validate object keys and values are valid parameter names
                    for source_attr, target_param in artifact_params.items():
                        if not isinstance(source_attr, str) or not source_attr.strip():
                            errors.append(f"{layer_name.capitalize()} layer '{obj_name}': artifactParams source attribute key cannot be empty")
                        elif not source_attr.replace('_', '').replace('-', '').isalnum():
                            errors.append(f"{layer_name.capitalize()} layer '{obj_name}': artifactParams source attribute '{source_attr}' contains invalid characters")
                        
                        if not isinstance(target_param, str) or not target_param.strip():
                            errors.append(f"{layer_name.capitalize()} layer '{obj_name}': artifactParams target parameter '{target_param}' cannot be empty")
                        elif not target_param.replace('_', '').isalnum() or target_param[0].isdigit():
                            errors.append(f"{layer_name.capitalize()} layer '{obj_name}': artifactParams target parameter '{target_param}' is not a valid parameter name")
                
                else:
                    errors.append(f"{layer_name.capitalize()} layer '{obj_name}': artifactParams must be an object mapping from source attributes to target parameter names")
    
    # Check all layers
    for layer in ['raw', 'silver', 'gold']:
        if layer in yaml_data:
            validate_layer_artifact_params(layer, yaml_data[layer])
    
    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Validate Modern BI Fabric Framework YAML configuration files"
    )
    parser.add_argument(
        "yaml_file",
        type=Path,
        help="Path to the YAML configuration file to validate"
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(__file__).parent / "config-schema.json",
        help="Path to the JSON schema file (default: config-schema.json in same directory)"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Enable strict validation (includes custom dependency checks)"
    )
    
    args = parser.parse_args()
    
    # Load schema and YAML file
    schema = load_schema(args.schema)
    yaml_data = load_yaml_file(args.yaml_file)
    
    print(f"Validating {args.yaml_file} against {args.schema}")
    print("-" * 60)
    
    # Perform schema validation
    schema_errors = validate_config(yaml_data, schema)
    
    # Perform custom validations if strict mode is enabled
    custom_errors = []
    if args.strict:
        custom_errors.extend(check_dependency_references(yaml_data))
        custom_errors.extend(check_delta_processing_config(yaml_data))
        custom_errors.extend(check_artifact_params_config(yaml_data))
    
    # Report results
    total_errors = len(schema_errors) + len(custom_errors)
    
    if schema_errors:
        print("❌ Schema Validation Errors:")
        for i, error in enumerate(schema_errors, 1):
            print(f"  {i}. {error}")
        print()
    
    if custom_errors:
        print("❌ Custom Validation Errors:")
        for i, error in enumerate(custom_errors, 1):
            print(f"  {i}. {error}")
        print()
    
    if total_errors == 0:
        print("✅ Validation successful! The YAML file is valid.")
        return 0
    else:
        print(f"❌ Validation failed with {total_errors} error(s).")
        return 1


if __name__ == "__main__":
    sys.exit(main())
