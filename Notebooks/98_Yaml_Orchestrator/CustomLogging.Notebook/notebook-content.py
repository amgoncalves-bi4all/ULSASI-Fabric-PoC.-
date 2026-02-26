# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, round, current_timestamp
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

processType="notebook"
MAX_RETRIES = 5  # Maximum retry attempts
RETRY_DELAY = 10  # Initial delay in seconds

TOKEN_REFRESH_THRESHOLD = 300  # Refresh token if older than 5 minutes

# Global token cache
_token_cache = {
    'token': None,
    'timestamp': None
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_fresh_kusto_token(force_refresh=False):
    global _token_cache
    
    current_time = datetime.now()
    
    # Check if we need to refresh the token
    if (force_refresh or 
        _token_cache['token'] is None or 
        _token_cache['timestamp'] is None or
        (current_time - _token_cache['timestamp']).total_seconds() > TOKEN_REFRESH_THRESHOLD):
        
        try:
            print("Refreshing Kusto access token...")
            new_token = notebookutils.credentials.getToken('kusto')
            _token_cache['token'] = new_token
            _token_cache['timestamp'] = current_time
            print("Kusto token refreshed successfully")
            return new_token
        except Exception as e:
            print(f"Failed to refresh Kusto token: {str(e)}")
            if _token_cache['token']:
                print("Using cached token as fallback")
                return _token_cache['token']
            else:
                raise Exception(f"No valid Kusto token available: {str(e)}")
    
    return _token_cache['token']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_kusto_with_retries(df,kustoUri,database,table,accessToken=None):
    attempt = 0
    current_token = accessToken

    while attempt < MAX_RETRIES:
        try:
             # Get fresh token if not provided or on retry after auth error
            if current_token is None or (attempt > 0 and "401" in str(last_error)):
                current_token = get_fresh_kusto_token(force_refresh=True)
            
            print(f"📝 Writing to Kusto (attempt {attempt + 1}/{MAX_RETRIES})...")

            df.write.\
                format("com.microsoft.kusto.spark.synapse.datasource").\
                option("kustoCluster", kustoUri).\
                option("kustoDatabase", database).\
                option("kustoTable", table).\
                option("accessToken", current_token).\
                option("tableCreateOptions", "CreateIfNotExist").\
                option("requestId", f"fabric_log_{int(time.time())}_{attempt}").\
                mode("Append").save()

            print("✅ Write to Kusto successful.")
            return  # Exit the function if successful

        except Exception as e:
            last_error = e
            error_message = str(e).lower()
            attempt += 1
            
            # Determine error type and appropriate action
            if "401" in error_message or "unauthorized" in error_message:
                print(f"Authentication error detected (attempt {attempt}/{MAX_RETRIES})")
                current_token = None  # Force token refresh on next attempt
                wait_time = 2  # Short wait for auth errors
                
            elif "throttling" in error_message or "429" in error_message:
                wait_time = RETRY_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                print(f"Kusto throttling detected. Retrying in {wait_time} seconds... (attempt {attempt}/{MAX_RETRIES})")
                
            elif "timeout" in error_message or "connection" in error_message:
                wait_time = RETRY_DELAY  # Fixed delay for connection issues
                print(f"Connection error detected. Retrying in {wait_time} seconds... (attempt {attempt}/{MAX_RETRIES})")
                
            else:
                print(f"Non-retryable error encountered: {str(e)}")
                raise  # Re-raise immediately for non-retryable errors
            
            if attempt < MAX_RETRIES:
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"All retry attempts failed. Last error: {str(e)}")
                raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def writeLog(status,processName, processInternalId="", errorMsg=""):
    # check if kustoURI is defined
    try:
        kustoUri = spark.conf.get(f"kustoUri")
        # The database to write the data
        database = spark.conf.get(f"kustoDatabase")
        logEnabled = True
    except:
        print("no kusto URI or database defined")
        logEnabled = False


    if (logEnabled):      
        # The table to write the data
        table    = "processExecutionLog"
        # The access credentials for the write
        accessToken = notebookutils.credentials.getToken('kusto')

        try:
            df = spark.createDataFrame(
                [
                    (model, batchExecution, processName, processInternalId, processType, status, errorMsg)
                ],
                ["model", "batchExecution", "processName", "processInternalId", "processType", "status", "errorMessage"]  # add your column names here
            )

            df = df.withColumn("timestamp", current_timestamp())

            write_to_kusto_with_retries(df,kustoUri,database,table,accessToken)

        except Exception as e:
            print(f"Critical logging error - unable to write to Kusto: {str(e)}")
            print(f"Log details: {status} | {processName} | {processInternalId} | {errorMsg}")
            
            # Don't raise the exception - logging failures shouldn't break the main process
            # But print enough detail for debugging
            import traceback
            print("Full error traceback:")
            traceback.print_exc()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def writeLogStarted(processName, processInternalId):
    writeLog("Started",processName, processInternalId)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def writeLogFinishedSuccess(processName, processInternalId):
    writeLog("Succeeded",processName, processInternalId)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def writeLogFinishedError(processName, processInternalId, errorMessage):
    writeLog("Failed",processName, processInternalId,errorMessage)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
