# Weekly Report Generation for Punjab Organisation
import csv

import requests
import json
from datetime import datetime, timedelta

# from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F


interval='1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00'



schema = StructType([
    StructField("events", ArrayType(
        StructType(
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
        )), True
                )
])
data=[]
def sam(lsts, key):
  return [x.get(key) for x in lsts]
with open('/home/ajay/Downloads/data.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        data.append(row)
    pr_id=sam(data,'program_id')

# print(pr_id)
for i in pr_id:

    url = 'http://localhost:8888/druid/v2'
    body = {"queryType": "scan", "dataSource": "p_data1", "resultFormat": "list",
            "columns": ["user_id", "userName", "districtName", "schoolExternalId", "blockName", "schoolName",
                        "questionName", "questionExternalId", "questionResponseLabel", "evidences",
                        "observationSubmissionId", "completedDate", "programName", "programId", "solutionName",
                        "schoolExternalId", "location_validated_with_geotag"],
            "intervals": [interval], "filter": {
            "type": "and",
            "fields": [ {
                        "type": "selector",
                        "dimension": "programId",
                        "value": f"{i}"},
                {
                    "type": "selector",
                    "dimension": "isAPrivateProgram",
                    "value": "false"
                }]}}

    response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(body))
    # print(response)
    data_list = response.json()
    # print(data_list)
    spark = SparkSession.builder.appName("content_data_app").getOrCreate()

    sc = spark.sparkContext

    df_rdd = sc.parallelize(data_list)
    df = spark.createDataFrame(df_rdd, schema)
    df.show()
    df = df.withColumn("exploded_events", F.explode_outer(F.col("events")))

    df_2 = df.select(
        df["exploded_events"]["user_id"].alias("user_id"),
        df["exploded_events"]["userName"].alias("user_name"),
        df["exploded_events"]["districtName"].alias("district"),
        df["exploded_events"]["blockName"].alias("block"),
        df["exploded_events"]["schoolName"].alias("school"),
        df["exploded_events"]["questionName"].alias("question"),
        df["exploded_events"]["questionExternalId"].alias("question_external_id"),
        df["exploded_events"]["schoolExternalId"].alias("school_external_id"),
        df["exploded_events"]["questionResponseLabel"].alias("question_response_label"),
        df["exploded_events"]["evidences"].alias("evidences"),
        df["exploded_events"]["observationSubmissionId"].alias("observation_submission_id"),
        df["exploded_events"]["completedDate"].alias("completed_date"),
        df["exploded_events"]["solutionName"].alias("solution_name"),
        df["exploded_events"]["programName"].alias("program_name"),
        df["exploded_events"]["programId"].alias("program_id"),
        df["exploded_events"]["location_validated_with_geotag"].alias("GeoTag")
    )

    df_2 = df_2.na.fill(value='ProgramName_notdefined', subset=["program_name"])
    unique_solutions_list = df_2.select(df_2["solution_name"]).distinct().rdd.flatMap(lambda x: x).collect()
    unique_solutions_list1 = df_2.select(df_2["program_name"]).distinct().rdd.flatMap(lambda x: x).collect()
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
                "/home/ajay/obs_punjab/" + program_name + "/" + solution_name + "/"
            )

