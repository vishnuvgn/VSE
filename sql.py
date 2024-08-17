import psycopg2
import formatName, airtables 

def testConnection(credentials):
    try:
        conn = psycopg2.connect(
            host=credentials['host'],
            database=credentials['database'],
            user=credentials['user'],
            password=credentials['password']
        )
        conn.close()
        return 1
    except Exception as e:
        print(e)
        return 0

'''
input: string
output: string
fields and tables are surrounded by double quotes to preserve case sensitivity in pgdb
'''
def writeQuery(table, cols, num_rows=1):
    pgTablePk = formatName.createPrimaryKey(table)
    columnsQuery = '('
    placeholders = ''
    numOfFields = len(cols)
    for i in range(numOfFields):
        col = cols[i]
        if i == 0:  # first field
            columnsQuery += f'"{col}"'
        else:
            columnsQuery += f', "{col}"'

    columnsQuery += ')'  # Close the column names parentheses

    row_placeholders = '(' + ', '.join(['%s'] * numOfFields) + ')'
    placeholders = ', '.join([row_placeholders] * num_rows)
    # fieldnames / excluded query
    excludedQuery = ''
    first = True

    for i in range(numOfFields):
        col = cols[i]
        if col != pgTablePk:
            if not first:
                excludedQuery += ', '
            excludedQuery += f'"{col}" = excluded."{col}"'
            first = False
            
    
    query = f'''
    INSERT INTO "{table}" {columnsQuery}
    VALUES {placeholders}
    ON CONFLICT ("{pgTablePk}") DO UPDATE
    SET {excludedQuery};'''

    return query

# tableUrlsDict = {"Members" : "https://airtable.com/app03GWdFHFCFlo9u/tblyIeCi2GxlIAG49/viwRpq5VEnQboWjJV?blocks=hide", "Skills" : "https://airtable.com/app03GWdFHFCFlo9u/tblUczxW6cEIp6Hv1/viwU9EtFwgzGHSzKA?blocks=hide"}

def create_audit_table(credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()
    cur.execute(f"SET search_path TO {schema}")
    
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = 'audit_log'
        );
    """, (schema,))
    table_exists = cur.fetchone()[0]
    
    if not table_exists:
        query = '''
        CREATE TABLE IF NOT EXISTS audit_log (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL UNIQUE,
            airtable_name VARCHAR(255) NOT NULL UNIQUE,
            base_id VARCHAR(255) NOT NULL,
            atpat VARCHAR(255) NOT NULL,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            airtableSyncFields text[]
        );
        '''
        cur.execute(query)
        conn.commit()
        print("Audit table created.")

    cur.close()
    conn.close()

def getBaseId(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = '''
    SELECT base_id
    FROM audit_log
    WHERE table_name = %s;
    '''
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (table,))

    # Fetch all the results
    baseId = cur.fetchone()[0]

    cur.close()
    conn.close()

    return baseId
def getATPAT(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = '''
    SELECT atpat
    FROM audit_log
    WHERE table_name = %s;
    '''
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (table,))

    # Fetch all the results
    atpat = cur.fetchone()[0]

    cur.close()
    conn.close()

    return atpat

def getAirtable(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = '''
    SELECT airtable_name
    FROM audit_log
    WHERE table_name = %s;
    '''
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (table,))

    # Fetch all the results
    airtable = cur.fetchone()[0]

    cur.close()
    conn.close()

    return airtable


def insert_audit_table(sqlTable, airTable, syncFields, baseId, atpat, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()
    syncFields_str = '{' + ','.join(f'"{item}"' for item in syncFields) + '}' 
    query = '''
    INSERT INTO audit_log (table_name, airtable_name, airtableSyncFields, update_time, base_id, atpat)
    VALUES (%s, %s, %s::TEXT[], CURRENT_TIMESTAMP, %s, %s)
    ON CONFLICT (table_name)
    DO NOTHING;
    '''

    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (sqlTable, airTable, syncFields_str, baseId, atpat))
    conn.commit()
    cur.close()
    conn.close()
    print("Audit table entry inserted.")

def updateTime_audit_log(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = '''
    UPDATE audit_log
    SET update_time = CURRENT_TIMESTAMP
    WHERE table_name = %s;
    '''

    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (table,))
    conn.commit()
    cur.close()
    conn.close()
    print("Audit table update time updated.")

def delete_audit_table(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = '''
    DELETE FROM audit_log
    WHERE table_name = %s;
    '''

    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (table,))
    conn.commit()
    cur.close()
    conn.close()
    print("Audit table entry deleted.")

def getAirFields(sqltable, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = '''
    SELECT airtableSyncFields
    FROM audit_log
    WHERE table_name = %s;
    '''
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (sqltable,))

    # Fetch all the results
    airFields = cur.fetchone()[0]

    cur.close()
    conn.close()

    return airFields



def getColNames(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = f'''
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = %s 
    AND table_name = %s;
    '''
    cur.execute(query, (schema, table))
    rows = cur.fetchall()

    colnames = [row[0] for row in rows]
    cur.close()
    conn.close()
    return colnames

def getRows(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()
    query = f'SELECT * FROM {schema}."{table}"'
    cur.execute(query)
    rows = cur.fetchall()

    colnames = [desc[0] for desc in cur.description]
    records = [dict(zip(colnames, row)) for row in rows]

    # json_data = json.dumps(records)
    cur.close()
    conn.close()
    return records


def upsertRows(table, cols, values, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()
    
    num_rows = len(values)
    flattened_values = tuple([item for sublist in values for item in sublist])

    query = writeQuery(table, cols, num_rows)
    
    
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, flattened_values)
    conn.commit()
    cur.close()
    conn.close()
    

def createTable(airTable, airFields, baseId, atpat, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    junction_table_pairs = []

    sqlTable = formatName.changeName(airTable, False)
    pgTablePk = formatName.createPrimaryKey(sqlTable)
    
    query = f'CREATE TABLE IF NOT EXISTS "{sqlTable}" ('
    fieldDefinitions = []

    for airField in airFields:
        sqlField = formatName.changeName(airField, True)
        if airField[-3:].lower() == "M2M".lower():
            junction_table_pairs.append((sqlTable, formatName.changeName(airField[:-4], False)))
            continue

        fieldDefinitions.append(f'"{sqlField}" TEXT')
    
    
    fieldDefinitions.append(f'"{pgTablePk}" TEXT PRIMARY KEY')

    query += ", ".join(fieldDefinitions)
    query += ');'
    
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query)
    print(f'created {sqlTable}')
    conn.commit()
    cur.close()
    conn.close()
    insert_audit_table(sqlTable, airTable, airFields, baseId, atpat, credentials)
    return junction_table_pairs

def createJunctionTable(table1, table2, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()
  
    table1_pk = formatName.createPrimaryKey(table1)
    table2_pk = formatName.createPrimaryKey(table2)

    junction_table_name = formatName.createJunctionTableName(table1, table2)

    # query still has constraints because it will be populated last anyways
    query = f'''
    CREATE TABLE IF NOT EXISTS "{junction_table_name}" (
        "{table1_pk}" TEXT,
        "{table2_pk}" TEXT,
        PRIMARY KEY ("{table1_pk}", "{table2_pk}"),
        FOREIGN KEY ("{table1_pk}") REFERENCES "{table1}"("{table1_pk}") ON DELETE CASCADE,
        FOREIGN KEY ("{table2_pk}") REFERENCES "{table2}"("{table2_pk}") ON DELETE CASCADE
    );
    '''
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query)
    print(f'created {junction_table_name}')
    conn.commit()
    cur.close()
    conn.close()
    insert_audit_table(junction_table_name, ['not a native airtable table'], credentials)

def getRowsFromJunction(junctionTableName, searchPk, searchId, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = f'''
    SELECT * FROM "{junctionTableName}"
    WHERE "{searchPk}" = %s
    '''
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (searchId,))
    rows = cur.fetchall()

    conn.commit()
    cur.close()
    conn.close()

    return [x[1] for x in rows] if rows else []




# only checking first part of pk tuple
def countRows(table, searchPk, searchId, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()
    query = f'''
    SELECT COUNT(*) 
    FROM "{table}"  
    WHERE "{searchPk}" = %s;
    '''
    
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (searchId,)) # has to be a single element tuple
    
    count = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return count

def deleteRows(table, searchPk, searchId, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()
    
    # Define the delete query
    query = f'''
    DELETE FROM "{table}"
    WHERE "{searchPk}" = %s;
    '''
    
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query, (searchId,))  # has to be a single element tuple
    
    conn.commit()  
    
    cur.close()
    conn.close()


def populateJunctionTable(table1, table2, table1Id, table2Ids, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()
    junction_table_name = formatName.createJunctionTableName(table1, table2)
    table1_pk = formatName.createPrimaryKey(table1)
    table2_pk = formatName.createPrimaryKey(table2)

    deleteRows(junction_table_name, table1_pk, table1Id, credentials)

    
    for table2Id in table2Ids:
        query = f'''
        INSERT INTO "{junction_table_name}" ("{table1_pk}", "{table2_pk}")
        VALUES (%s, %s)
        ON CONFLICT ("{table1_pk}", "{table2_pk}")
        DO UPDATE SET "{table1_pk}" = EXCLUDED."{table1_pk}", "{table2_pk}" = EXCLUDED."{table2_pk}"
        '''
        
        cur.execute(f"SET search_path TO {schema}")
        cur.execute(query, (table1Id, table2Id))


    print(f'populated {junction_table_name}')
    conn.commit()
    cur.close()
    conn.close()
    return

# be fucking careful
def deleteTable(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = f'DROP TABLE IF EXISTS "{table}" CASCADE'
    
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    print(f'deleted {table}') 
    delete_audit_table(table, credentials)

def listTables(credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()


    query = '''
        SELECT table_name, update_time, airtablesyncfields
        FROM audit_log
        ORDER BY table_name;
    '''

    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query)
    tables = cur.fetchall()

    table_info = {
        'nativeTables': [],
        'junctionTables': []
    }

    for table in tables:
        tblName = table[0]
        updateTime = table[1]
        fields = table[2]
        if 'not a native airtable table' in fields:
            table_info['junctionTables'].append({
                'name': tblName,
                'last_update_time': updateTime,
            })
        else:
            table_info['nativeTables'].append({
                'name': tblName,
                'last_update_time': updateTime
            })

    cur.close()
    conn.close()

    return table_info

def clearTable(table, credentials):
    conn = psycopg2.connect(
        host=credentials['host'],
        database=credentials['database'],
        user=credentials['user'],
        password=credentials['password']
    )
    schema = credentials['schema']
    cur = conn.cursor()

    query = f'DELETE FROM "{table}"'
    
    cur.execute(f"SET search_path TO {schema}")
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    print(f'cleared {table}')
    updateTime_audit_log(table, credentials)

def createTables(table_fields_dict: dict, baseId, atpat, credentials):
    try:
        junction_table_pairs_total = []
        # tableFields = airtables.fillTableFields(tableUrlsDict, username, password) # webscraper ## NO WEBSCRAPING
        # print(tableFields, type(tableFields))
        airtbls_fields = table_fields_dict.items()
        for airtbl, airfields in airtbls_fields:
            pairs = createTable(airtbl, airfields, baseId, atpat, credentials)
            junction_table_pairs_total += pairs
        for pair in junction_table_pairs_total:
            createJunctionTable(pair[0], pair[1], credentials)
        return 1
    except Exception as e:
        print(e)
        return 0

def deleteTables(credentials):
    try:
        tables_info = listTables(credentials) # nested dict
        nativeTables = tables_info['nativeTables']
        junctionTables = tables_info['junctionTables']
        
        for tblDetails in nativeTables+junctionTables:
            deleteTable(tblDetails['name'], credentials)
        return 1
    except Exception as e:
        print(e)
        return 0
    

def clearTables(credentials):
    try:
        tbls = listTables(credentials)
        for tbl in tbls:
            clearTable(tbl)
        return 1
    except Exception as e:
        print(e)
        return 0   
     
def restart(tableUrlsDict, credentials):
    try:
        deleteTables(credentials)
        createTables(tableUrlsDict, credentials)
        return 1
    except Exception as e:
        print(e, "restart")
        return 0
    