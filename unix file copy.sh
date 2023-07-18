#!/bin/bash

# Check if all runtime arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Error: Insufficient arguments. Please provide the source path, file regex, and target path."
    exit 1
fi

# Extract the runtime arguments
source_path="$1"
file_regex="$2"
target_path="$3"

# Display the runtime arguments
echo "Source Path: $source_path"
echo "File Regex: $file_regex"
echo "Target Path: $target_path"

# Check if the source path exists
if [ ! -d "$source_path" ]; then
    echo "Error: Source path '$source_path' does not exist."
    exit 1
fi

# Check if any files match the given regex
shopt -s nullglob
files=($source_path/$file_regex)
shopt -u nullglob

if [ ${#files[@]} -eq 0 ]; then
    echo "Error: No files found matching the regex '$file_regex' in the source path '$source_path'."
    exit 1
fi

# Copy the files to the target path
for file in "${files[@]}"; do
    cp "$file" "$target_path"

    # Check for any errors during the copy process
    if [ "$?" -ne 0 ]; then
        echo "Error: Failed to copy the file '$file' to the target path '$target_path'."
    else
        echo "File '$file' copied successfully to the target path '$target_path'."
    fi
done

# Check if the files were successfully copied
for file in "${files[@]}"; do
    copied_file="$target_path/$(basename "$file")"

    if [ ! -f "$copied_file" ]; then
        echo "Error: The file '$file' was not copied to the target path '$target_path'."
    fi
done
