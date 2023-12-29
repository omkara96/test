import cx_Oracle
import psycopg2
import pandas as pd

# Function to connect to Oracle database
def connect_to_oracle(username, password, host, port, service):
    dsn = cx_Oracle.makedsn(host, port, service_name=service)
    connection = cx_Oracle.connect(username, password, dsn)
    return connection

# Function to connect to Redshift database
def connect_to_redshift(username, password, host, port, database):
    connection = psycopg2.connect(
        user=username,
        password=password,
        host=host,
        port=port,
        database=database
    )
    return connection

# Function to retrieve data from a table based on a column name
def retrieve_data(connection, table_name, column_name):
    query = f"SELECT {column_name} FROM {table_name}"
    df = pd.read_sql(query, connection)
    return df

# Function to compare tables based on primary key columns
def compare_tables(oracle_df, redshift_df, primary_key_columns):
    # Sort DataFrames based on primary key columns
    oracle_df.sort_values(by=primary_key_columns, inplace=True)
    redshift_df.sort_values(by=primary_key_columns, inplace=True)

    # Find differing records
    differing_records = oracle_df[~oracle_df.isin(redshift_df.to_dict(orient='list'))].dropna()

    # Find missing records in Oracle
    missing_records_oracle = redshift_df[~redshift_df.isin(oracle_df.to_dict(orient='list'))].dropna()

    return differing_records, missing_records_oracle

# Example usage
if __name__ == "__main__":
    # Oracle database connection details
    oracle_username = "your_username"
    oracle_password = "your_password"
    oracle_host = "your_oracle_host"
    oracle_port = "your_oracle_port"
    oracle_service = "your_oracle_service"

    # Redshift database connection details
    redshift_username = "your_username"
    redshift_password = "your_password"
    redshift_host = "your_redshift_host"
    redshift_port = "your_redshift_port"
    redshift_database = "your_redshift_database"

    # Table and column details
    table_name = "your_table_name"
    column_name = "your_column_name"
    primary_key_columns = ["primary_key_column1", "primary_key_column2"]

    # Connect to Oracle database
    oracle_connection = connect_to_oracle(oracle_username, oracle_password, oracle_host, oracle_port, oracle_service)

    # Connect to Redshift database
    redshift_connection = connect_to_redshift(redshift_username, redshift_password, redshift_host, redshift_port, redshift_database)

    # Retrieve data from Oracle and Redshift
    oracle_data = retrieve_data(oracle_connection, table_name, column_name)
    redshift_data = retrieve_data(redshift_connection, table_name, column_name)

    # Compare tables based on primary key columns
    differing_records, missing_records_oracle = compare_tables(oracle_data, redshift_data, primary_key_columns)

    # Save differing records to a file
    differing_records.to_csv('differing_records.csv', index=False)

    # Save missing records in Oracle to a file
    missing_records_oracle.to_csv('missing_records_oracle.csv', index=False)

    print("Comparison completed. Differing records and missing records saved to files.")

    # Close connections
    oracle_connection.close()
    redshift_connection.close()
