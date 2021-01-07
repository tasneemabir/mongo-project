import uuid
import pymongo
from faker import Faker
from random import randint
from pymongo import MongoClient

#connect to db
client = pymongo.MongoClient("mongodb+srv://user:pass@cluster0.ovfat.mongodb.net/proj?retryWrites=true&w=majority")
db = client.cse716
collection = db["collection1"]
            
# =====     Insert Data    =====
def insert_n(n):
    for _ in range(n):
        row = faker_()
        print("adding data ",row)
        collection.insert_one(row)

def insert_one():
    row = faker_()
    print("adding data ",row)
    collection.insert_one(row)
    
        
# =====     Retrieve Data    =====
def retrieve(key, value):
    results = collection.find_one({key: value})
    print(results)
    
    
# =====     Update Data     =====  
def update_entry(id, key, value):
    collection.update_one({"_id": id}, {"$set": {key: value}})
    

# =====     Delete one row     =====
def delete_one(key, value):    
    collection.delete_one({key: value})


# =====     Get count of row     =====
def get_count():
    post_count = collection.count_documents({})
    return post_count

def faker_():
    fake_entry = {"_id":None,"name":None,"addr":None,"nid":None,"brthcrt":None,"phone":None}
    fake = Faker()
    id = uuid.uuid4()
    id = str(id)
    phone = "017"
    phone = phone+str(randint(1000000,9999999))
    
    fake_entry.update({"_id":randint(100000,999999)})
    fake_entry.update({"name":fake.name()})
    fake_entry.update({"addr":fake.address()})
    fake_entry.update({"nid":fake.ssn()})
    fake_entry.update({"brthcrt":str(randint(10000000,99999999))})
    fake_entry.update({"phone":phone})
    
    return fake_entry