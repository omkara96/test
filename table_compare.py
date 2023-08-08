import psycopg2
from psycopg2 import sql
import difflib

def get_table_structure(connection, schema_name, table_name):
    query = sql.SQL("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s;")
    
    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name))
        columns = cursor.fetchall()
        
    return columns

def generate_comparison_report(table_name, diff):
    report = f"Comparison Report for Table: {table_name}\n\n"
    report += "\n".join(diff)
    report += "\n\n" + "=" * 80 + "\n\n"
    return report

def main():
    db1_params = {
        'dbname': 'database_name',
        'user': 'username',
        'password': 'password',
        'host': 'host_name',
        'port': '5439'  # Redshift default port
    }

    db2_params = {
        'dbname': 'database_name',
        'user': 'username',
        'password': 'password',
        'host': 'host_name',
        'port': '5439'  # Redshift default port
    }

    schema_name = input("Enter schema name: ")
    table_list_file = input("Enter path to the file containing table names: ")

    with open(table_list_file, 'r') as file:
        table_names = [line.strip() for line in file.readlines()]

    try:
        conn1 = psycopg2.connect(**db1_params)
        conn2 = psycopg2.connect(**db2_params)

        for table_name in table_names:
            structure1 = get_table_structure(conn1, schema_name, table_name)
            structure2 = get_table_structure(conn2, schema_name, table_name)

            diff = compare_table_structures(structure1, structure2)

            comparison_report = generate_comparison_report(table_name, diff)

            with open(f"{table_name}_comparison_report.txt", "w") as report_file:
                report_file.write(comparison_report)

    except Exception as e:
        print("An error occurred:", e)
    finally:
        conn1.close()
        conn2.close()

if __name__ == "__main__":
    main()
