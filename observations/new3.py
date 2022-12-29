import json

from pymongo import MongoClient
import datetime
from datetime import timedelta
from pytz import timezone
import json
from bson import ObjectId, json_util

client = MongoClient()
#Step 1: Connect to MongoDB - Note: Change connection string as needed
client = MongoClient("mongodb://localhost:27017")
db=client["ml-survey"]
print(db.observationSubmissions.find())
from_date = datetime.datetime(2022, 4, 27, 0, 0, 0, 0)
to_date = datetime.datetime(2022, 5, 2, 0, 0, 0, 0)
i=0
# print(db.observationSubmissions.find({"createdAt": {"$gte": from_date, "$lt": to_date}}).count())
for data in db.observationSubmissions.find({"createdAt": {"$gte": from_date, "$lt": to_date}}):
    i=i+1
    # print(data)
print(i)



    # class JSONEncoder(json.JSONEncoder):
    #     def default(self, o):
    #         if isinstance(o, ObjectId):
    #             return str(o)
    #         return json.JSONEncoder.default(self, o)
    #
    #
    # data1=JSONEncoder().encode(data)
    # with open("/home/ajay/shikshalokam/sample.json", "w") as outfile:
    #     json.dump(data1, outfile, default=json_util.default)
    # print(data)