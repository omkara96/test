def fix_delimited_file(input_file, output_file, delimiter):
    encodings = ['utf-8', 'shift_jis', 'cp932']  # Add more encodings if needed

    for encoding in encodings:
        try:
            with open(input_file, 'r', encoding=encoding) as file:
                lines = file.readlines()

            fixed_lines = []
            prev_line = ''

            for line in lines:
                line = line.replace('\n', '').replace('\r', '')
                words = line.split(delimiter)

                if len(words) < 5:
                    prev_line += line
                else:
                    fixed_lines.append(prev_line)
                    prev_line = line

            fixed_lines.append(prev_line)

            with open(output_file, 'w', encoding=encoding) as file:
                for line in fixed_lines:
                    file.write(line + '\n')

            print(f'Successfully processed the file with encoding: {encoding}')
            return

        except UnicodeDecodeError:
            pass

    print('Unable to process the file with any of the specified encodings.')


# Usage example
input_file_path = 'file.txt'  # Replace with your input file path
output_file_path = 'output.txt'  # Replace with your desired output file path
delimiter = '\t'  # Replace with the delimiter used in your file

fix_delimited_file(input_file_path, output_file_path, delimiter)
