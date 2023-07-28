import cx_Oracle
import os

def generate_insert_scripts(schema_name, table_name, cursor):
    # Generate the insert scripts with data
    insert_script = f"INSERT INTO {schema_name}.{table_name} ("
    values_script = "VALUES ("

    # Fetch table columns
    cursor.execute(f"SELECT * FROM {schema_name}.{table_name} WHERE ROWNUM = 1")
    columns = [col[0] for col in cursor.description]

    for column in columns:
        insert_script += f"{column}, "
        values_script += f":{column}, "

    insert_script = insert_script[:-2] + ")"
    values_script = values_script[:-2] + ")"

    # Fetch all rows from the table
    cursor.execute(f"SELECT * FROM {schema_name}.{table_name}")
    rows = cursor.fetchall()

    insert_scripts = f"{insert_script}\n"

    for row in rows:
        insert_values = values_script
        for column, value in zip(columns, row):
            # If the value is None, set it to NULL in the script
            if value is None:
                value = "NULL"
            else:
                # If the value is a string, wrap it in single quotes
                if isinstance(value, str):
                    value = value.replace("'", "''")
                    value = f"'{value}'"
                else:
                    value = str(value)
            insert_values = insert_values.replace(f":{column}", value, 1)

        insert_scripts += f"{insert_values};\n"

    return insert_scripts

def main():
    # Replace with your Oracle connection information
    username = "YOUR_USERNAME"
    password = "YOUR_PASSWORD"
    host = "YOUR_HOST"
    port = "YOUR_PORT"
    service_name = "YOUR_SERVICE_NAME"
    schema_name = "YOUR_SCHEMA_NAME"

    # Read table list from the text file
    with open("table_list.txt", "r") as file:
        tables = [line.strip() for line in file.readlines()]

    # Establish a connection to the Oracle database
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    connection = cx_Oracle.connect(username, password, dsn)

    try:
        with connection.cursor() as cursor:
            # Process each table and generate the corresponding insert scripts
            for table_name in tables:
                # Generate and write insert scripts to the output file with the same table name
                insert_scripts = generate_insert_scripts(schema_name, table_name, cursor)
                
                output_file_path = os.path.join("output_files", f"{table_name}_insert_scripts.sql")
                with open(output_file_path, "w") as output_file:
                    output_file.write(insert_scripts)

    except cx_Oracle.Error as error:
        print("Oracle Database Error:", error)

    finally:
        connection.close()

if __name__ == "__main__":
    main()
