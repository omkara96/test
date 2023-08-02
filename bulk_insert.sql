import os
import psycopg2

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def bulk_insert_data(connection_string, sql_files_dir):
    try:
        # Connect to the Redshift database
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()

        for root, _, files in os.walk(sql_files_dir):
            for file in files:
                if file.endswith('.sql'):
                    file_path = os.path.join(root, file)
                    sql_query = read_sql_file(file_path)

                    # Execute the COPY command to bulk insert data
                    cursor.execute(sql_query)
                    connection.commit()

                    # Get the total number of lines in the SQL file for progress tracking
                    total_lines = len(sql_query.splitlines())
                    print(f"Data from {file_path} inserted successfully. ({total_lines} lines)")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: ", error)

    finally:
        # Close the connection
        if connection:
            cursor.close()
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    # Replace these with your actual Redshift database credentials
    host = 'your_redshift_host'
    port = 'your_redshift_port'
    database = 'your_redshift_database'
    user = 'your_redshift_username'
    password = 'your_redshift_password'

    connection_string = f"host='{host}' port='{port}' dbname='{database}' user='{user}' password='{password}'"

    # Replace 'your_sql_files_directory' with the directory path containing your .sql files
    sql_files_dir = 'your_sql_files_directory'

    bulk_insert_data(connection_string, sql_files_dir)
