# fuck sql naming standards, all caps for tables, all lowercase for fields
import re

# changes airtable tables and fields into sql tables and fields
def changeName(input_string, isField: bool):
    # Remove text within parentheses
    input_string = re.sub(r'\([^)]*\)', '', input_string)
    
    # Remove leading and trailing whitespace
    input_string = input_string.strip()
    
    # Replace spaces with empty string
    input_string = input_string.replace(" ", "")

    if isField == True:
        formattedName = input_string.lower()
    else:
        formattedName = input_string.upper()
    return formattedName

def createJunctionTableName(table1, table2):
    return f'{table1}_{table2}'

def createPrimaryKey(table):
    return f'{changeName(table, True)}_id'