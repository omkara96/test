import csv
import configparser
import os

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Open the input CSV file
with open('mycsv.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)

    # Iterate through each row in the input CSV file
    for row in reader:
        # Determine the starting word of the row
        starting_word = row[0].split()[0]

        # Check if there is a configuration for the starting word
        if starting_word in config:
            # Get the output filename from the configuration
            output_filename = config[starting_word]['filename']

            # Create the output file if it doesn't exist
            if not os.path.exists(output_filename):
                with open(output_filename, 'w', newline='') as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow(row)

            # Append the row to the output file
            with open(output_filename, 'a', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(row)
