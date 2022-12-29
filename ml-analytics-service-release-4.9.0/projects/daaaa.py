import csv
from bson.objectid import ObjectId
import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["sl-proddata"]
mycol = mydb["observationSubmissions"]
data=[]
def sam(lsts, key):
  return [x.get(key) for x in lsts]
with open('/home/ajay/Downloads/data.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        data.append(row)
    pr_id=sam(data,'program_id')
# i=
for i in pr_id:
    for x in mycol.find({"programId": ObjectId(i)}):
        print(x)
