# Weekly Report Generation for dhwani Organisation
import csv

import requests
import json
from datetime import datetime, timedelta

# from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col, max as max_
from pyspark.sql.functions import avg, first


interval='1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00'



schema = StructType([
    StructField("events", ArrayType(
        StructType(
            [
                StructField("course_name", StringType(), True),
                StructField("User_Name", StringType(), True),
                StructField("progress", StringType(), True),
                StructField("content_name", StringType(), True),
                StructField("Event_duration", StringType(), True),
                StructField("__time", StringType(), True),
                StructField("User_id", StringType(), True)
            ]
        )), True
                )
])


# print(pr_id)


url = 'http://localhost:8888/druid/v2'
body = {
  "queryType": "scan",
  "dataSource": "sl_quiz_telemetry",
  "dimensions": [
    "progress",
    "User_Name",
    "Event_duration",
    "content_name",
    "course_name",
      "__time",
      "User_id"
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ],
  "granularity": "all",
  "postAggregations": [],
  "intervals": '2022-07-28T00:00:00+00:00/2022-08-04T00:00:00+00:00',
  "limitSpec": {
    "type": "default",
    "limit": 10000,
    "columns": [
      {
        "dimension": "count",
        "direction": "descending"
      }
    ]
  }
}

response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(body))
data_list = response.json()

spark = SparkSession.builder.appName("content_data_app").getOrCreate()

sc = spark.sparkContext

df_rdd = sc.parallelize(data_list)
df = spark.createDataFrame(df_rdd, schema)
df = df.withColumn("exploded_events", F.explode_outer(F.col("events")))
df_2 = df.select(
        df["exploded_events"]["course_name"].alias("Title of the Course"),
        df["exploded_events"]["User_Name"].alias("Full Name of the Learner"),
        df["exploded_events"]["progress"].alias("Course Completion Percentage"),
        df["exploded_events"]["content_name"].alias("Name of the quizzes in the course"),
        df["exploded_events"]["Event_duration"].alias("Percentage completion of the quizes"),
        df["exploded_events"]["__time"].alias("__time"),
        df["exploded_events"]["User_id"].alias("User_id")
    )

df_2=df_2.withColumn("Date", F.to_utc_timestamp(F.from_unixtime(F.col("__time")/1000,'yyyy-MM-dd HH:mm:ss'),'IST'))
df_3=df_2.withColumn("datetime", col("Date").cast("timestamp")).groupBy("Title of the Course","Name of the quizzes in the course", "User_id").agg(max_("Percentage completion of the quizes").alias("Percentage completion of the quizes"),max_("datetime").alias("Time Stamp of Completing the quiz"))
df_2=df_2.withColumnRenamed("Date","Time Stamp of Completing the quiz")
df_res=df_3.join(df_2,on=["Title of the Course","Name of the quizzes in the course", "User_id","Percentage completion of the quizes"]).drop(df_3["Time Stamp of Completing the quiz"])
df_res=df_res.select("Title of the Course","Name of the quizzes in the course","User_id","Full Name of the Learner","Course Completion Percentage","Percentage completion of the quizes","Time Stamp of Completing the quiz")
df_res=df_res.sort(["Time Stamp of Completing the quiz","Title of the Course","Name of the quizzes in the course", "User_id"], ascending=[0,1, 1,1])
df_res=df_res.drop_duplicates(["Title of the Course","Name of the quizzes in the course", "User_id"])
df_res.coalesce(1).write.mode('overwrite').option('header', 'true').csv("/home/ajay/sl_dhawani_telemetry1/")
