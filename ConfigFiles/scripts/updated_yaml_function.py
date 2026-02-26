# Updated GetConfigMetadataFromYaml function to handle deltaProcessing section

def GetConfigMetadataFromYaml(model: str, objectName: str, layer: str, environment: str = "dev") -> dict:
    """
    Retrieves configuration metadata for a specific model, object, and data layer from YAML configuration files.
    Updated to handle the deltaProcessing section properly.

    This function reads configuration metadata from YAML files in the ConfigFiles directory structure.
    It filters for active configurations matching the given `model` and `objectName`
    and returns the matching configuration as a dictionary.

    Args:
        model (str): The name of the model to filter on.
        objectName (str): The name of the object to filter on.
        layer (str): The data layer to retrieve metadata from. 
                     Accepted values: "raw", "silver", or "gold".
        environment (str): The environment to read configuration from (default: "dev").

    Returns:
        dict: A dictionary containing the configuration metadata for the specified parameters.
              Returns `None` if no matching record is found.

    Raises:
        FileNotFoundError: If the YAML configuration file doesn't exist.
        yaml.YAMLError: If there's an error parsing the YAML file.
        KeyError: If the specified layer or objectName is not found in the configuration.
    """
    
    # Use global variables instead of calling GetFabricIds()
    global WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID
    
    # Construct the path to the YAML configuration file
    config_file_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Files/ConfigFiles/environments/{environment}/{model}.yml"
    
    # Check if file exists
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Configuration file not found: {config_file_path}")
    
    try:
        # Read and parse the YAML file
        with open(config_file_path, 'r') as file:
            config_data = yaml.safe_load(file)
        
        # Validate that the model is active
        if not config_data.get('active', False):
            return None
        
        # Map layer names for compatibility with existing code
        layer_mapping = {
            "Bronze": "raw",
            "Silver": "silver", 
            "Gold": "gold"
        }
        
        # Use mapped layer name if it exists, otherwise use the original
        yaml_layer = layer_mapping.get(layer, layer.lower())
        
        # Get the layer configuration
        layer_config = config_data.get(yaml_layer)
        if not layer_config:
            raise KeyError(f"Layer '{yaml_layer}' not found in configuration")
        
        # Find the object configuration
        object_config = None
        
        # For raw layer, match against sourceObjectName (like the original function)
        if yaml_layer == "raw":
            for obj_name, obj_config in layer_config.items():
                if obj_config.get('sourceObjectName') == objectName:
                    object_config = obj_config.copy()
                    object_config['objectName'] = obj_name  # Add the key as objectName
                    break
        else:
            # For silver and gold layers, match against the object key directly
            object_config = layer_config.get(objectName)
            if object_config:
                object_config = object_config.copy()
                object_config['objectName'] = objectName
        
        if not object_config:
            return None
        
        # Check if the object is active
        if not object_config.get('flagActive', 0):
            return None
        
        # Add model information to the configuration
        object_config['model'] = model
        
        # Handle deltaProcessing section - flatten to root level for backward compatibility
        if 'deltaProcessing' in object_config:
            delta_config = object_config.pop('deltaProcessing')
            
            # Add deltaProcessing attributes to the root level with defaults
            object_config['fullProcess'] = delta_config.get('fullProcess', False)
            object_config['dateType'] = delta_config.get('dateType', 'Day')
            object_config['dateUnit'] = delta_config.get('dateUnit', 1)
            object_config['dateColumnFormat'] = delta_config.get('dateColumnFormat', 'yyyy/MM/dd')
            
            # Only add filterColumn if it exists and fullProcess is False
            if not object_config['fullProcess'] and 'filterColumn' in delta_config:
                object_config['filterColumn'] = delta_config['filterColumn']
        
        # Update destination paths for Silver and Gold layers with full OneLake paths
        if yaml_layer in ["silver", "gold"]:
            lakehouse_id = SILVER_LAKEHOUSE_ID if yaml_layer == "silver" else GOLD_LAKEHOUSE_ID
            destination_pattern = object_config.get('destinationObjectPattern')
            
            if destination_pattern:
                object_config['destinationObjectPattern'] = (
                    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
                    f"{lakehouse_id}/Tables/{destination_pattern}"
                )
        
        return object_config
        
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file {config_file_path}: {str(e)}")
    except Exception as e:
        raise Exception(f"Error reading configuration from {config_file_path}: {str(e)}")


# Additional helper function to get deltaProcessing configuration separately
def GetDeltaProcessingConfig(model: str, objectName: str, layer: str, environment: str = "dev") -> dict:
    """
    Retrieves only the deltaProcessing configuration for a specific object.
    
    Args:
        model (str): The name of the model to filter on.
        objectName (str): The name of the object to filter on.
        layer (str): The data layer to retrieve metadata from.
        environment (str): The environment to read configuration from (default: "dev").
        
    Returns:
        dict: A dictionary containing only the deltaProcessing configuration,
              or an empty dict if no deltaProcessing section exists.
    """
    
    config = GetConfigMetadataFromYaml(model, objectName, layer, environment)
    
    if not config:
        return {}
    
    # Extract deltaProcessing related attributes
    delta_config = {}
    
    delta_attributes = ['fullProcess', 'dateType', 'dateUnit', 'dateColumnFormat', 'filterColumn']
    
    for attr in delta_attributes:
        if attr in config:
            delta_config[attr] = config[attr]
    
    return delta_config


# Example usage and testing
def test_delta_processing_integration():
    """Test the updated function with deltaProcessing configuration."""
    
    # Test cases
    test_cases = [
        ("framework", "Person", "silver"),
        ("framework", "dim_person", "gold"),
    ]
    
    for model, obj_name, layer in test_cases:
        try:
            config = GetConfigMetadataFromYaml(model, obj_name, layer)
            if config:
                print(f"✅ {layer}.{obj_name} configuration loaded successfully")
                
                # Check if deltaProcessing attributes are present
                delta_attrs = ['fullProcess', 'dateType', 'dateUnit', 'dateColumnFormat']
                delta_found = any(attr in config for attr in delta_attrs)
                
                if delta_found:
                    print(f"   📅 Delta processing configuration found:")
                    for attr in delta_attrs:
                        if attr in config:
                            print(f"      {attr}: {config[attr]}")
                    if 'filterColumn' in config:
                        print(f"      filterColumn: {config['filterColumn']}")
                else:
                    print(f"   ℹ️  No delta processing configuration")
                
            else:
                print(f"❌ {layer}.{obj_name} configuration not found")
                
        except Exception as e:
            print(f"❌ Error loading {layer}.{obj_name}: {str(e)}")
        
        print("-" * 40)


if __name__ == "__main__":
    # Run integration test
    test_delta_processing_integration()
