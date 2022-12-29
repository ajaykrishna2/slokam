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
    db = client[""]
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


