import os, json

# Function to add a new entry to the JSON object
def append_to_json(file_path, new_key, new_value):
    # Check if file is empty
    if os.path.getsize(file_path) == 0:
        with open(file_path, 'w') as file:
            json.dump({new_key: new_value}, file, indent=4)
    else:
        with open(file_path, 'r+') as file:
            file.seek(0, os.SEEK_END)  # Move to the end of the file
            file.seek(file.tell() - 1, os.SEEK_SET)  # Move one character back to overwrite the closing brace

            if file.tell() > 1:  # Check if not at the start of the file
                file.write(',\n')  # Add a comma to separate the new object
            
            # Write the new key-value pair and close the JSON object
            file.write(f'"{new_key}": ')
            json.dump(new_value, file, indent=4)
            file.write('\n}')


def overwrite_json(file_path, new_data):
    with open(file_path, 'w') as file:
        json.dump(new_data, file, indent=4)


def clear_json(file_path):
    with open(file_path, 'w') as file:
        file.write('{}')