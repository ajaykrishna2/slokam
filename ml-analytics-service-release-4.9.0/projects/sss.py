# report Generation for Punjab Organisation

import requests
import json
from datetime import datetime, timedelta
import csv
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import split, col,substring,regexp_replace
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.message import MIMEMessage
import smtplib, os, shutil
from email import encoders
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(interpolation=ExtendedInterpolation())

config.read('/opt/sparkjobs/source/reports/config.ini')

# Taking default Druid interval
interval = '1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00'

# Reading data from csv
data = []


def sam(lsts, key):
    return [x.get(key) for x in lsts]


with open('program_ID.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        data.append(row)
    pr_id = sam(data, 'program_id')
    so_id = sam(data, 'solution_id')
    pr_id1 = list(set(pr_id))
    so_id1 = list(set(so_id))

print(pr_id1)

for i in pr_id1:
    print(f"printing the id from csv {i}")
    url = 'http://10.0.225.175:8082/druid/v2'

    body = {"queryType": "scan", "dataSource": "sl_observation_org_0128398709898772481", "resultFormat": "list",
            "columns": ["user_id", "userName", "districtName", "blockName",
                        "schoolName", "questionName", "questionExternalId", "questionResponseLabel", "evidences",
                        "observationSubmissionId", "completedDate", "solutionName", "programName",
                        "schoolExternalId", "location_validated_with_geotag"], "intervals": [interval],
            "filter": {"type": "and", "fields": [{"type": "selector", "dimension": "programId", "value": f"{i}"},
                                                 {"type": "selector", "dimension": "isAPrivateProgram",
                                                  "value": "false"}]}}

    response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(body))

    data_list = response.json()

    schema = StructType([
        StructField("events", ArrayType(
            StructType(
                [
                    StructField("user_id", StringType(), False),
                    StructField("userName", StringType(), False),
                    StructField("districtName", StringType(), True),
                    StructField("blockName", StringType(), True),
                    StructField("schoolName", StringType(), True),
                    StructField("questionName", StringType(), False),
                    StructField("questionExternalId", StringType(), False),
                    StructField("questionResponseLabel", StringType(), True),
                    StructField("evidences", StringType(), True),
                    StructField("observationSubmissionId", StringType(), True),
                    StructField("completedDate", StringType(), False),
                    StructField("solutionName", StringType(), True),
                    StructField("programName", StringType(), True),
                    StructField("location_validated_with_geotag", StringType(), True)
                ]
            )), True
                    )
    ])

    # load spark session
    spark = SparkSession.builder.appName("content_data_app").getOrCreate()

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.endpoint", "s3-ap-south-1.amazonaws.com")
    hadoop_conf.set("fs.s3a.access.key", "AKIASKKP73WSU6CCR67E")
    hadoop_conf.set("fs.s3a.secret.key", "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")

    df_rdd = sc.parallelize(data_list)
    df = spark.createDataFrame(df_rdd, schema)

    df = df.withColumn("exploded_events", F.explode_outer(F.col("events")))

    df_2 = df.select(
        df["exploded_events"]["user_id"].alias("user_id"),
        df["exploded_events"]["userName"].alias("user_name"),
        df["exploded_events"]["districtName"].alias("district"),
        df["exploded_events"]["blockName"].alias("block"),
        df["exploded_events"]["schoolName"].alias("school"),
        df["exploded_events"]["questionName"].alias("question"),
        df["exploded_events"]["questionExternalId"].alias("question_external_id"),
        df["exploded_events"]["questionResponseLabel"].alias("question_response_label"),
        df["exploded_events"]["evidences"].alias("evidences"),
        df["exploded_events"]["observationSubmissionId"].alias("observation_submission_id"),
        df["exploded_events"]["completedDate"].alias("completed_date"),
        df["exploded_events"]["solutionName"].alias("solution_name"),
        df["exploded_events"]["programName"].alias("program_name"),
        df["exploded_events"]["location_validated_with_geotag"].alias("GeoTag")
    )

    df_2 = df_2.na.fill(value='ProgramName_notdefined', subset=["program_name"])
    df_2 = df_2.withColumn('date', split(df_2['completed_date'], 'T').getItem(0)).withColumn('time_stamp', split(
        df_2['completed_date'], 'T').getItem(1))
    # df_2 = df_2.withColumn("time_stamp", F.concat(F.lit("T"),F.col("time_stamp")))
    df_2 = df_2.drop('completed_date')
    # taking distinct solution names
    unique_solutions_list = df_2.select(df_2["solution_name"]).distinct().rdd.flatMap(lambda x: x).collect()

    # taking distinct program names
    unique_program_list = df_2.select(df_2["program_name"]).distinct().rdd.flatMap(lambda x: x).collect()

    print("printing the program name and solution name")
    print(unique_program_list)
    print(unique_solutions_list)

    df_final_sol = [df_2.where(df_2["solution_name"] == x) for x in unique_solutions_list]

    df_final_pro = [df_2.where(df_2["program_name"] == x) for x in unique_program_list]

    for j in range(len(df_final_pro)):

        program_name = '_'.join(j.lower() for j in unique_program_list[j].split())
        # program_name = '_'.join(j for j in unique_solutions_list1[j].split())

        for i in range(len(df_final_sol)):
            solution_name = '_'.join(i.lower() for i in unique_solutions_list[i].split())

            df_finall = df_final_sol[i].filter(df_final_sol[i].program_name == unique_program_list[j])

            df_finall.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save(
                "/opt/sparkjobs/source/reports/obs_punjab_dump/" + program_name + "/" + solution_name + "/")

shutil.make_archive('/opt/sparkjobs/source/reports/obs_punjab_dump_report/' + program_name, 'zip',
                    '/opt/sparkjobs/source/reports/obs_punjab_dump/'+ program_name)

shutil.rmtree("/opt/sparkjobs/source/reports/obs_punjab_dump")



