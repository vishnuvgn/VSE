import sync, sql, formatName

M2M_MAPS = {}
BATCH_SIZE = 10000 # number of records to send to sql at a time

def push(airtableName, baseId, atpat, credentials):
    sqlTable = formatName.changeName(airtableName, False)
    pgTablePk=formatName.createPrimaryKey(sqlTable)

    totalRecords = sync.getRecords(airtableName, baseId, atpat)
    syncFields = set(sql.getAirFields(sqlTable, credentials))
    whittledRecords = sync.whittle(syncFields, totalRecords)
    
    
    oldRecords = sql.getRows(sqlTable, credentials)
    updated = False
    deletedIds, addedIds, changedIds = sync.findChanges(airtableName, oldRecords, whittledRecords, pgTablePk, credentials)
    if len(deletedIds) > 0 or len(addedIds) > 0 or len(changedIds) > 0:
        updated = True
    else:
        return f"no updates to be made to the selected table {airtableName}"
    all_values = tuple()
    batch_count  = 0
    for record in whittledRecords:
        id = record["id"]

        if id in deletedIds: # deleted records
            sql.deleteRows(sqlTable, formatName.createPrimaryKey(sqlTable), id, credentials)
            print(f"Deleted record: {record}")
        elif id in changedIds or id in addedIds:
            print(id)
            airFields = set(record["fields"].keys())
            pgTablePk = formatName.createPrimaryKey(sqlTable) # record_id val will go here
            PGCols = [pgTablePk]
            record_values = (id,)
            for field in airFields:
                val = record["fields"][field]
                
                M2M_flag = False

                if field[-3:] == "M2M": # value is a list
                    M2M_flag = True
                    print(f'found M2M: {field}')
                    refTable = field[:-4] # has to be spelled right in airtable
                    junctionTables = (sqlTable, formatName.changeName(refTable, False)) # tuple is key in dict
                    if junctionTables not in M2M_MAPS:
                        M2M_MAPS[junctionTables] = {
                            id : val # will be list of rec ids for the ref table
                        }
                    else:
                        M2M_MAPS[junctionTables][id] = val

                    print(val)

                if type(val) == list and len(val) == 1:
                    val = val[0]
                # elif type(val) == int or type(val) == float:
                #     val = str(val)
                elif type(val) == dict:
                    with open("error.txt", "w") as f:
                        f.write(f'error {field}: {val}')
                    val = ""
                elif val == None:
                    val = ""
                
                if M2M_flag == False: # M2M not a field in pg sql table. will be handled in the junction table which can only be populated after all tables populated
                    record_values += (val, )

                    PGField = formatName.changeName(field, True)
                    # print(f'PGField = {PGField}')
                    PGCols.append(PGField)

            all_values += (record_values,)
            batch_count += 1

            if batch_count == BATCH_SIZE:
                sql.upsertRows(sqlTable, PGCols, all_values, credentials)
                updated=True
                all_values = tuple()
                batch_count = 0

    if batch_count > 0:
        sql.upsertRows(sqlTable, PGCols, all_values, credentials)
    if updated:
        sql.updateTime_audit_log(sqlTable, credentials)
    
    return f"Updated {airtableName} table"
    

    
def updateTables(airTableNames:list, baseId:str, atpat:str, credentials:dict):
    
    for tableName in airTableNames:
        push(tableName, baseId, atpat, credentials)
    
    for junctionTables, recordMap in list(M2M_MAPS.items()):
        # tbl1Id is the upstream_id of the record in the main table
        # tbl2Ids is a list of upstream_ids of the records in the ref table
        for tbl1Id, tbl2Ids in recordMap.items():
            tbl1, tbl2 = junctionTables
            # EX: "MEMBERS", "SKILLS", "mem1", ["skill1", "skill2"] (these will obviously be real upstream ids)
            sql.populateJunctionTable(tbl1, tbl2, tbl1Id, tbl2Ids, credentials)
            sql_junction_tabl = formatName.createJunctionTableName(tbl1, tbl2)
            sql.updateTime_audit_log(sql_junction_tabl, credentials)

        del M2M_MAPS[junctionTables]
# main()