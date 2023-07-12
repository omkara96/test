import os
import fileinput

def replace_words_in_files(directory, old_words, new_words):
    for filename in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, filename)):
            filepath = os.path.join(directory, filename)
            with fileinput.FileInput(filepath, inplace=True) as file:
                for line in file:
                    new_line = line
                    for i in range(len(old_words)):
                        old_word = old_words[i]
                        new_word = new_words[i]
                        new_line = new_line.replace(old_word, new_word, -1)
                        new_line = new_line.replace(old_word.lower(), new_word.lower(), -1)
                        new_line = new_line.replace(old_word.upper(), new_word.upper(), -1)
                    print(new_line, end='')

# Provide the directory path, old words, and new words
directory_path = "."  # Current directory
old_words = ["old_word1", "old_word2", "old_word3"]
new_words = ["new_word1", "new_word2", "new_word3"]

replace_words_in_files(directory_path, old_words, new_words)
