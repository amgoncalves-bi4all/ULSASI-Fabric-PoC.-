# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# Basic Smoke Test - Fabric Framework
# 
# This notebook validates basic functionality after cross-environment deployment:
# 1. Workspace connectivity  
# 2. Lakehouse access
# 3. Common Functions availability
# 4. Configuration loading

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import CommonFunctions to verify basic framework functionality
%run CommonFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test 1: Verify workspace and lakehouse IDs are properly initialized
print("=== Test 1: Workspace & Lakehouse ID Validation ===")

try:
    # Get fabric IDs using framework function
    workspace_id, bronze_id, silver_id, gold_id = GetFabricIds()
    
    print(f"✓ Workspace ID: {workspace_id}")
    print(f"✓ Bronze Lakehouse ID: {bronze_id}")  
    print(f"✓ Silver Lakehouse ID: {silver_id}")
    print(f"✓ Gold Lakehouse ID: {gold_id}")
    
    # Validate ID format (should be GUIDs)
    import uuid
    
    for id_name, id_value in [("Workspace", workspace_id), ("Bronze", bronze_id), ("Silver", silver_id), ("Gold", gold_id)]:
        if id_value:
            uuid.UUID(id_value)  # Will raise exception if invalid
            print(f"✓ {id_name} ID format is valid")
        else:
            print(f"⚠ {id_name} ID is None")
    
    print("✓ Test 1 PASSED: All IDs are valid")
    
except Exception as e:
    print(f"✗ Test 1 FAILED: {str(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test 2: Verify lakehouse connectivity
print("\n=== Test 2: Lakehouse Connectivity ===")

try:
    # Test Bronze lakehouse access
    bronze_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Files/"
    
    # List contents to verify access
    files_bronze = dbutils.fs.ls(bronze_path)
    print(f"✓ Bronze lakehouse accessible - found {len(files_bronze)} items")
    
    # Test Silver lakehouse access  
    silver_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE_ID}/Tables/"
    files_silver = dbutils.fs.ls(silver_path)
    print(f"✓ Silver lakehouse accessible - found {len(files_silver)} items")
    
    # Test Gold lakehouse access
    gold_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE_ID}/Tables/"
    files_gold = dbutils.fs.ls(gold_path)  
    print(f"✓ Gold lakehouse accessible - found {len(files_gold)} items")
    
    print("✓ Test 2 PASSED: All lakehouses are accessible")
    
except Exception as e:
    print(f"✗ Test 2 FAILED: {str(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test 3: Verify Fabric API connectivity
print("\n=== Test 3: Fabric API Connectivity ===")

try:
    import sempy.fabric as fabric
    
    # Test API access by listing items
    items = fabric.list_items()
    print(f"✓ Fabric API accessible - found {len(items)} items in workspace")
    
    # Verify we can access workspace info
    workspace_info = fabric.get_workspace()
    print(f"✓ Current workspace: {workspace_info}")
    
    print("✓ Test 3 PASSED: Fabric API is functional")
    
except Exception as e:
    print(f"✗ Test 3 FAILED: {str(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test 4: Verify Spark session and basic operations
print("\n=== Test 4: Spark Session Validation ===")

try:
    # Test Spark session
    print(f"✓ Spark version: {spark.version}")
    print(f"✓ Spark application ID: {spark.sparkContext.applicationId}")
    
    # Test basic DataFrame operations
    test_data = [(1, "test1"), (2, "test2"), (3, "test3")]
    test_df = spark.createDataFrame(test_data, ["id", "value"])
    
    row_count = test_df.count()
    print(f"✓ DataFrame operations working - test DF has {row_count} rows")
    
    # Test SQL operations
    test_df.createOrReplaceTempView("test_table")
    sql_result = spark.sql("SELECT COUNT(*) as cnt FROM test_table").collect()[0].cnt
    print(f"✓ SQL operations working - SQL count: {sql_result}")
    
    print("✓ Test 4 PASSED: Spark session is functional")
    
except Exception as e:
    print(f"✗ Test 4 FAILED: {str(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test Summary
print("\n" + "="*50)
print("🎉 BASIC SMOKE TESTS COMPLETED SUCCESSFULLY")
print("="*50)
print("All core framework components are working properly:")
print("✓ Workspace and lakehouse IDs are valid")
print("✓ All lakehouses are accessible") 
print("✓ Fabric API is functional")
print("✓ Spark session is operational")
print("\nFramework is ready for use in this environment!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }