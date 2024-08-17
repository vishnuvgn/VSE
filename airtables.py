import extractFields#, jsonFunctions
import os, shutil
# from dotenv import load_dotenv
# load_dotenv()

CSV_DIR = '/home/workmerk/syncFlask/csvs'

'''
Fields in use for each table mentioned above
'''


# human input -- all views are titled SyncFields
TABLE_URLS = {
  "Members" : "https://airtable.com/app03GWdFHFCFlo9u/tblyIeCi2GxlIAG49/viwRpq5VEnQboWjJV?blocks=hide", 
  "Requirements" : "https://airtable.com/app03GWdFHFCFlo9u/tblekjtTS5jgzlaOf/viwzYSqVBFQHj3VI0?blocks=hide",
  "Squadrons" : "https://airtable.com/app03GWdFHFCFlo9u/tblb2XprFTBTuaFxs/viwMoYR6NnrmBhXZ2?blocks=hide",
  "Groups" : "https://airtable.com/app03GWdFHFCFlo9u/tbl9bBfgnpZ4TayI2/viw7CSt71Lxw2FdIH?blocks=hide",
  "AppointmentType" : "https://airtable.com/app03GWdFHFCFlo9u/tblNbT8qOYf025FeM/viw7isa0MUmN30NsV?blocks=hide",
  "Rifle Training" : "https://airtable.com/app03GWdFHFCFlo9u/tblO5KfZ3ftqs5YDK/viwUqYPkuuCIAw8aW?blocks=hide",
  "M249 Training" : "https://airtable.com/app03GWdFHFCFlo9u/tblJruRooZJFuEZCD/viwadAzHeXXZjgbrO?blocks=hide",
  "M4 Training" : "https://airtable.com/app03GWdFHFCFlo9u/tblUpb4DjsyBpurUg/viwYfTfgDGIdXSj6a?blocks=hide",
  "Grenade Training" : "https://airtable.com/app03GWdFHFCFlo9u/tbleavKIXnAlF8OrK/viwW1P53dZZmho8VX?blocks=hide",
  "M240 Training" : "https://airtable.com/app03GWdFHFCFlo9u/tbli4bZDcPs7mIbnZ/viwWu9osY2YvUACvV?blocks=hide",
  "Roles" : "https://airtable.com/app03GWdFHFCFlo9u/tblysjQlQLUIATTMe/viwRyQhJYgkvDLqGy?blocks=hide",
  "Jobs" : "https://airtable.com/app03GWdFHFCFlo9u/tblrEzbWHpayymtsb/viwIQcSMEOW8FQK4s?blocks=hide",
  "Vehicle Equipment" : "https://airtable.com/app03GWdFHFCFlo9u/tbldnEsi6tuEtcvbp/viwu3EELPSho0KBoO?blocks=hide",
  "Maintenance Equipment" : "https://airtable.com/app03GWdFHFCFlo9u/tblgkK8NOV6MJI5JN/viwew83ONwC1miy3y?blocks=hide",
  "Communications Equipment" : "https://airtable.com/app03GWdFHFCFlo9u/tblSEx6VFXXwqSSL0/viw9DgmGD9xKlHN3n?blocks=hide",
  "Aircraft Equipment" : "https://airtable.com/app03GWdFHFCFlo9u/tblUoJWnEcObkoOIY/viwj2mUKBVJ1XMohQ?blocks=hide",
  "Slots" : "https://airtable.com/app03GWdFHFCFlo9u/tblcOInOa2VmKBTO4/viwGThlqYZNK6V4BX?blocks=hide",
  "Skills" : "https://airtable.com/app03GWdFHFCFlo9u/tblUczxW6cEIp6Hv1/viwU9EtFwgzGHSzKA?blocks=hide"
}

def clear_directory(dir_path):
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')


def fillTableFields(tableUrlsDict, username, password): 
    clear_directory(CSV_DIR)    
    print("inside fillTableFields")
    for table, url in tableUrlsDict.items():
        loginCount = 0
        
        # loginTest
        while loginCount < 5:
            try:
                driver = extractFields.initiateRemote()
                driver = extractFields.login(driver, username, password, url)
            except:
                print("will try logging in again")
                loginCount += 1
        if loginCount >= 5:
            return "Failed to login"

        completed = False
        while completed == False:
            try: 
                tableFields = extractFields.compileFieldList(table, driver)
            except:
                print("will try again")
            else:
                completed = True

    
    return tableFields # dict of table names and their fields