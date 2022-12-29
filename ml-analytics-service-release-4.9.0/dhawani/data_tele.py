import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.message import MIMEMessage
import smtplib, os, shutil
from email import encoders
from configparser import ConfigParser, ExtendedInterpolation
from pyspark.sql.functions import split, col, substring, regexp_replace
from pyspark.sql.functions import col, max as max_

Dhwani_Schema = StructType([
    StructField("events", ArrayType(
        StructType(
            [
                StructField("Course_name", StringType(), True),
                StructField("User_FirstName", StringType(), True),
                StructField("course_progress", StringType(), True),
                StructField("section_name", StringType(), True),
                StructField("section_progress", StringType(), True),
                StructField("user_login_id", StringType(), True),
                StructField("__time", StringType(), True),
                StructField("User_id", StringType(), True)

            ]
        )), True
                )
])

url = "http://localhost:8888/druid/v2?pretty"
superset_query = {"queryType": "scan", "dataSource": "testf",
                  "dimensions": ["section_progress", "course_progress", "__time", "user_login_id",
                                 "User_id", "section_name", "Course_name", "User_FirstName"], "granularity": "all",
                  "intervals": ["1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00"], "limitSpec": {"type": "default",
                                                                                                   "limit": 10000,
                                                                                                   "columns": [{
                                                                                                                   "dimension": "count",
                                                                                                                   "direction": "descending"}]}}
response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(superset_query))
data_list = response.json()
spark = SparkSession.builder.appName("content_data_app").getOrCreate()

sc = spark.sparkContext

dataframe_rdd = sc.parallelize(data_list)
dataframe = spark.createDataFrame(dataframe_rdd, Dhwani_Schema)

dataframe = dataframe.withColumn("events", F.explode_outer(F.col("events")))

final_dataframe = dataframe.select(
    dataframe["events"]["Course_name"].alias("Title of the Course"),
    dataframe["events"]["User_FirstName"].alias("Full Name of the Learner"),
    dataframe["events"]["course_progress"].alias("Course Completion Percentage"),
    dataframe["events"]["section_name"].alias("Name of the resources in the course"),
    dataframe["events"]["section_progress"].alias("Percentage completion of the resources"),
    dataframe["events"]["user_login_id"].alias("User login Id"),
    dataframe["events"]["__time"].alias("date"),
    dataframe["events"]["User_id"].alias("User ID")
)
# dataframe1 = final_dataframe.withColumn("datetime", col("Date").cast("timestamp")).groupBy("Title of the Course","Name of the quizzes in the course", "User_id").agg(max_("datetime").alias("Time Stamp of Completing the quiz"))
# final_dataframe = final_dataframe.withColumnRenamed("Date","Time Stamp of Completing the quiz")
final_dataframe=final_dataframe.withColumn("datetime",
                                 F.from_utc_timestamp(F.to_timestamp(final_dataframe["date"] / 1000),
                                                      'UTC'))

df2 = final_dataframe.groupBy("Title of the Course","Name of the resources in the course","User login Id").agg(max_("datetime"))
df2.show()
dataframe2 = final_dataframe.join(df2, how='inner')
print(df2.count())
final_dataframe.show()

final_dataframe.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save("/home/ajay/Downloads/oppppp.csv")