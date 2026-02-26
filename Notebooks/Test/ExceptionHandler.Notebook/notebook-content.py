# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# Não parece funcionar :(

# CELL ********************

import sys
import traceback

def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Handle uncaught exceptions globally."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)  # Allow interrupts
        return
    
    # Format the error message
    error_message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    
    # Log or display the error
    print("🚨 Notebook-wide Exception Caught:")
    print(error_message)

    # Optionally, exit the notebook to prevent further execution
    notebookutils.notebook.exit(f"Notebook failed due to: {exc_value}")

# Set the custom exception handler
sys.excepthook = global_exception_handler

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
