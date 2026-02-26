# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Function to generate dynamic data using *args
def generate_dynamic_sql_data(*args):
    # Unpack the arguments
    (
        model,
        source_system_name,
        source_system_type,
        source_location_name,
        source_object_name,
        source_select_columns,
        source_key_columns,
        destination_system_name,
        destination_system_type,
        destination_object_pattern,
        destination_directory_pattern,
        destination_object_type,
        extract_type,
        delta_start_date,
        delta_end_date,
        delta_date_column,
        delta_filter_condition,
        flag_block,
        block_size,
        block_column,
        flag_active,
        create_date,
        last_modified_date,
    ) = args

    return (
        model,
        source_system_name,
        source_system_type,
        source_location_name if source_location_name != "" else source_object_name,
        source_object_name,
        source_select_columns,
        source_key_columns,
        destination_system_name,
        destination_system_type,
        destination_object_pattern if destination_object_pattern != "" else source_object_name,
        f"{destination_directory_pattern}{source_object_name}",
        destination_object_type,
        extract_type,
        delta_start_date,
        delta_end_date,
        delta_date_column,
        delta_filter_condition,
        flag_block,
        block_size,
        block_column,
        flag_active,
        create_date,
        last_modified_date,
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to create a SQL insert statement for all rows in the DataFrame
def generate_sql_insert_all_rows(df, table_name):
    # Extract column names
    columns = df.schema.names

    # Start the SQL statement
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n"

    # Collect all rows and format them for SQL
    values = []
    for row in df.collect():
        formatted_row = ", ".join(
            [
                f"'{row[col]}'"
                if isinstance(row[col], str)
                else f"'{row[col].strftime('%Y-%m-%d %H:%M:%S')}'"
                if isinstance(row[col], datetime)
                else f"{row[col]}"
                if row[col] is not None
                else "NULL"
                for col in columns
            ]
        )
        values.append(f"({formatted_row})")

    # Join all rows with commas and complete the statement
    sql += ",\n".join(values) + ";"
    return sql

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
