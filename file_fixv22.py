import csv

def add_double_quotes(input_file, output_file, columns, delimiter):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter=delimiter)
        writer = csv.writer(outfile, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # Write the header with double quotes
        writer.writerow(['"' + column + '"' for column in columns])

        # Process each row and add double quotes to each value
        for row in reader:
            processed_row = ['"' + value + '"' for value in row]
            writer.writerow(processed_row)

if __name__ == "__main__":
    input_file_path = input("Enter the path to the input CSV file: ")
    output_file_path = input("Enter the path to the output CSV file: ")
    column_list = input("Enter the column names as a comma-separated list: ").split(',')
    delimiter_char = input("Enter the delimiter used in the CSV file (e.g., ',', ';', '\\t'): ")

    add_double_quotes(input_file_path, output_file_path, column_list, delimiter_char)
