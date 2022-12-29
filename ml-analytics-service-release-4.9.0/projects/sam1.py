import os
from datetime import datetime, timedelta
from configparser import ConfigParser,ExtendedInterpolation
import csv
import requests
import json
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
current_dt = datetime.now()
past_week_dt = current_dt.today() - timedelta(days=60)
interval = str(past_week_dt.date()) + "T06:30:00.00/" + str(current_dt.date()) + "T06:30:00.00"
data=[]
import json

from pymongo import MongoClient
import datetime
from datetime import timedelta
from pytz import timezone
import json
from bson import ObjectId, json_util
schema =StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("userName", StringType(), True),
                StructField("districtName", StringType(), True),
                StructField("blockName", StringType(), True),
                StructField("schoolName", StringType(), True),
                StructField("questionName", StringType(), True),
                StructField("questionExternalId", StringType(), True),
                StructField("schoolExternalId", StringType(), True),
                StructField("questionResponseLabel", StringType(), True),
                StructField("evidences", StringType(), True),
                StructField("observationSubmissionId", StringType(), True),
                StructField("completedDate", StringType(), True),
                StructField("solutionName", StringType(), True),
                StructField("programName", StringType(), True),
                StructField("programId", StringType(), True),
                StructField("location_validated_with_geotag", StringType(), True)
            ]
        )
def sam(lsts, key):
  return [x.get(key) for x in lsts]
with open('/home/ajay/Downloads/data.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    # line_count = 0
    for row in csv_reader:
        # print(type(row.values()))
        data.append(row)
    pr_id=sam(data,'program_id')
print(pr_id)
for i in pr_id:
    client = MongoClient()
    # Step 1: Connect to MongoDB - Note: Change connection string as needed
    client = MongoClient("mongodb://localhost:27017")
    db = client["darpan_data"]
    # print(db.sl_darpan.find())
    data1 = []
    for dar in db.sl_darpan.find({'programId': i}):
        data1.append(dar)
    # print(data1)
    spark = SparkSession.builder.appName("content_data_app").getOrCreate()

    sc = spark.sparkContext

    df_rdd = sc.parallelize(data1)
    df = spark.createDataFrame(df_rdd, schema)
    # df.show()
    df_2 = df.select(
        df["user_id"].alias("user_id"),
        df["userName"].alias("user_name"),
        df["districtName"].alias("district"),
        df["blockName"].alias("block"),
        df["schoolName"].alias("school"),
        df["questionName"].alias("question"),
        df["questionExternalId"].alias("question_external_id"),
        df["schoolExternalId"].alias("school_external_id"),
        df["questionResponseLabel"].alias("question_response_label"),
        df["evidences"].alias("evidences"),
        df["observationSubmissionId"].alias("observation_submission_id"),
        df["completedDate"].alias("completed_date"),
        df["solutionName"].alias("solution_name"),
        df["programName"].alias("program_name"),
        df["programId"].alias("program_id"),
        df["location_validated_with_geotag"].alias("GeoTag")
    )

    df_2 = df_2.na.fill(value='ProgramName_notdefined', subset=["program_name"])
    unique_solutions_list = df_2.select(df_2["solution_name"]).distinct().rdd.flatMap(lambda x: x).collect()
    unique_solutions_list1 = df_2.select(df_2["program_name"]).distinct().rdd.flatMap(lambda x: x).collect()
    print(unique_solutions_list1)
    df_final = [df_2.where(df_2["solution_name"] == x) for x in unique_solutions_list]
    df_final1 = [df_2.where(df_2["program_name"] == x) for x in unique_solutions_list1]
    for j in range(len(df_final1)):
        program_name = '_'.join(j.lower() for j in unique_solutions_list1[j].split())
        # program_name = '_'.join(j for j in unique_solutions_list1[j].split())

        # print(program_name)
        for i in range(len(df_final)):
            solution_name = '_'.join(i.lower() for i in unique_solutions_list[i].split())
            # df_final[i].program_name
            df_finall = df_final[i].filter(df_final[i].program_name == unique_solutions_list1[j])
            df_finall.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save(
                "/home/ajay/obs_punjab2/" + program_name + "/" + solution_name + "/"
            )
        # print(type(i))


print(df_2.select(df_2["program_name"]).show())


# clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
# db = clientProd[config.get('MONGO', 'database_name')]


# pr_id = ["600ab53cc7de076e6f993724", "601429016a1ef53356b1d714"]
# for i in pr_id:
#   url = 'http://localhost:8888/druid/v2'
#   body ={"queryType": "scan", "dataSource": "druid_data_sam", "resultFormat": "list",
#             "columns": ["program_name","createdBy"],
#             "intervals": ['2009-12-31T00:00:00.000Z/2010-01-02T00:00:00.000Z'],"granularity":"day",
#             "filter":{
#                 "type": "and",
#                 "fields": [
#                     {
#                         "type": "selector",
#                         "dimension": "program_id",
#                         "value": f"{i}"}
#                 ]}
#          }
# print(body)
#   response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(body))
#   print(response.status_code)
#   print(response.text)
# progarm_id = ["60ed4d5c294d1726ee39085f", "60ed4d5c294d1726ee39085f", "5f1c5be0499e7a357a6c2786"]
# test run
# current_dt = datetime.now() - timedelta(days=1)
# past_week_dt = current_dt.today() - timedelta(days=26)
# interval = str(past_week_dt.date()) + "T06:30:00.00/" + str(current_dt.date()) + "T06:30:00.00"
# pr_id = ["601429016a1ef53356b1d714", "602d11f0473c59287ad538e1","605083ba09b7bd61555580fb","60549338acf1c71f0b2409c3","607d320de9cce45e22ce90c0","61120cda9174bc4e6ee05f05","6124984e5435a115cac9f120","61558efa27e9cd00077530b5","6172a6e58cf7b10007eefd21","61dd77a459e49a00074aecae","62034f90841a270008e82e46","623ea13a372eae00075fe8b8","625575f4aac5a900076825e5"]
# for i in pr_id:
# url = 'http://localhost:8888/druid/v2'
# body = {"queryType": "scan", "dataSource": "p_data", "resultFormat": "list",
#             "columns":["user_id", "userName", "districtName", "blockName", "schoolName", "questionName", "questionExternalId", "questionResponseLabel", "evidences", "observationSubmissionId", "completedDate", "solutionName", "schoolExternalId","location_validated_with_geotag"],
#             "intervals": [interval], "filter": {
#             "type": "and",
#             "fields": [
#                 {
#                     "type": "selector",
#                     "dimension": "isAPrivateProgram",
#                     "value": "false"
#                 }]}}
#
# response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(body))
# print(response)
# data_list = response.json()
# print(data_list)

