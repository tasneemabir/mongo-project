import crud
import uuid
import pymongo
from faker import Faker
from random import randint
from pymongo import MongoClient

#connect to db
client = pymongo.MongoClient("mongodb+srv://user:pass@cluster0.ovfat.mongodb.net/proj?retryWrites=true&w=majority")
db = client.cse716
collection = db["collection1"]

def find_all_id():
    results = collection.find()
    for x in results:
        print(x)
        
# =====     Update Data     =====
def update_entry(id, key, value):
    collection.update_one({"_id": id}, {"$set": {key: value}})


# =====     Update multiple fields in a row     =====
def update_multiple_document_values(id, key_value_pairs):
    collection.update_one({"_id": id}, {"$set": key_value_pairs})


# =====     Remove the field defined from all the rows ====
def remove_specific_field_from_all_rows(key):
    collection.update({}, {"$unset": {key:""} } , multi=True)


# =====     add one to phone number after 017 ===
def add_one_to_phone_number(phone_number): 
    if phone_number is None:
        return
    return phone_number[:3] + str(1) + phone_number[3:] #


# =====     splits name into first name and last name ===
def split_name(name): #
    splitted_name = name.split() 
    name_len = len(splitted_name)
    if(len(splitted_name) > 2):
        first_name = splitted_name.pop(0)+" "+splitted_name.pop(0)
        last_name = " ".join(splitted_name) 
    else:
        first_name = splitted_name.pop(0)
        last_name = " ".join(splitted_name)
    #print(first_name+"\n"+last_name)
    return first_name, last_name
    
def name_to_fname_lname():
    print("Changing single column name to two column, first name and last name")
    for x in collection.find():
        first_name, last_name = split_name(x['name'])
        data = {
            'first_name' : first_name,
            'last_name' : last_name
        }
        update_multiple_document_values(id=x['_id'], key_value_pairs=data)
        
    print("updated values")
    for x in collection.find():
        print("First name: "+x['first_name']+" Last name: "+x['last_name'])
    
    remove_specific_field_from_all_rows('name')
    
def migrate():
    rows = crud.get_count()
    print("Number of documents in collection: "+str(rows))
    print("Migration starting")
    name_to_fname_lname()
    print("Updating phone numbers...")
    for x in collection.find():
        new_phone_number=add_one_to_phone_number(x['phone'])
        #print(new_phone_number)
        update_entry(x['_id'], 'phone', new_phone_number)
    
    remove_specific_field_from_all_rows('brthcrt')
    '''
    
    '''

def main():
    #collection.delete_many({})
    #crud.insert_n(10)
    migrate()
    #print(crud.get_count())
    client.close()

if __name__ == "__main__":
    main()
