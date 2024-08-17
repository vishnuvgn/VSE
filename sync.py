import requests, os
import formatName, sql

def getRecords(tableName, baseId, atpat):
    url = f"https://api.airtable.com/v0/{baseId}/{tableName}"
    headers = {
        "Authorization": f"Bearer {atpat}"
    }

    count = 0
    offset = ""
    totalRecords = []
    while True:
        print(count)
        if count == 0:
            response = requests.get(url, headers=headers)
        else:
            response = requests.get(url, headers=headers, params={"offset": offset})

        data = response.json()
        records = data["records"]
        for record in records:
            totalRecords.append(record)
        if "offset" not in data:
            break
        offset = data["offset"]
        count += 1
    return totalRecords
    
# sync fields is the list of Airtable fields (stored in postgres audit_log tbl)
def whittle(syncFields, totalRecords):
    whittledRecords = []
    for record in totalRecords:
        fields = record["fields"] # fields  is a dictionary
        whittledRecord = {}
        whittledRecord["id"] = record["id"]
        whittledRecord["fields"] = {}
        for sfield in syncFields:
            if sfield not in fields:
                if sfield[-3:] == "M2M":
                    whittledRecord["fields"][sfield] = []
                else:
                    whittledRecord["fields"][sfield] = ""
            else:
                whittledRecord["fields"][sfield] = fields[sfield]
        whittledRecords.append(whittledRecord)
    return whittledRecords

def findChanges(airTableName, oldRecords, newRecords, pgTablePk, credentials):
    oldIds = {record[pgTablePk] for record in oldRecords}
    newIds = {record["id"] for record in newRecords}

    deletedIds = oldIds - newIds
    addedIds = newIds - oldIds

    remainingIds = oldIds & newIds

    changedIds = set()
    for id in remainingIds:
        matching_record_old = next((record for record in oldRecords if record[pgTablePk] == id), None)
        matching_record_new = next((record for record in newRecords if record["id"] == id), None)
        
        # old will be sequilized fields, new will be airtable fields
        for airfield in matching_record_new['fields']:
            sqlfield = formatName.changeName(airfield, True)
            val_new = matching_record_new['fields'][airfield]
            val_old = ''
            if sqlfield[-3:].lower() == "M2M".lower():
                junctionTable = formatName.createJunctionTableName(formatName.changeName(airTableName, False), formatName.changeName(airfield[:-4], False))
                val_old = sql.getRowsFromJunction(junctionTable, formatName.createPrimaryKey(airTableName), id, credentials)

            elif type(val_new) == list and len(val_new) == 1: # --> fk (not sure about what other kind of field is sent as a list but _fk's for sure)
                val_new = str(val_new[0])
            elif type(val_new) == int or type(val_new) == float: # all values in postgres are strings
                val_new = str(val_new)
            
            if val_old == '':
                val_old = matching_record_old[sqlfield]
            elif type(val_new) == list:
                val_new = val_new.sort()
                val_old = val_old.sort()

            if val_new != val_old:
                print(f"Field: {sqlfield}")
                print(f"Old: {val_old}, New: {val_new}")
                changedIds.add(id)

        
        # if matching_record_old != matching_record_new:
        #     changedIds.add(id)
    
    
    
    print(deletedIds, addedIds, changedIds)
    return deletedIds, addedIds, changedIds