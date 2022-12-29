import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_


import json

import requests
from pyspark.sql.types import StructField, StringType, ArrayType, StructType

url = "http://localhost:8082/druid/v2"

payload={
  "queryType": "scan",
  "dataSource": "sl_course_progress_dhwani",
  "dimensions": [
    "section_progress",
    "course_progress",
    "batch_name",
    "section_name",
    "Course_name",
    "User_FirstName"
  ],
  "aggregations": [
    {
      "type": "count",
      "name": "count"
    }
  ],
  "granularity": "all",
  "postAggregations": [],
  "intervals": "2009-07-24T00:00:00+00:00/2022-08-31T00:00:00+00:00",
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
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

# print(response.text)
res = response.json()
spark = SparkSession.builder.appName("ps_course_wise_completion").getOrCreate()

sc = spark.sparkContext
df_rdd = sc.parallelize(res)
schema = StructType([
        StructField("events", ArrayType(
            StructType(
                [
                    StructField("Course_name", StringType(), True),
                    StructField("User_FirstName", StringType(), True),
                    StructField("course_progress", StringType(), True),
                    StructField("section_name", StringType(), True),
                    StructField("section_progress", StringType(), True),
                    StructField("__time", StringType(), True),
                    StructField("User_id", StringType(), True),
                    StructField("user_login_id", StringType(), True)
                ]
            )), True
        )
    ])
df = spark.createDataFrame(df_rdd,schema)
df = df.withColumn("events", F.explode_outer("events"))
df1 = df.select(df["events"]["Course_name"].alias("Title of the Course"),
               df["events"]["User_FirstName"].alias("Full Name of the Learner"),
               df["events"]["course_progress"].alias("Course Completion Percentage"),
               df["events"]["section_name"].alias("Name of the resources in the course"),
               df["events"]["section_progress"].alias("Percentage completion of the resources"),
                F.from_utc_timestamp(F.to_timestamp(df["events"]["__time"]/ 1000),'UTC').alias("Time"),
               df["events"]["User_id"].alias("User_id"),
                df["events"]["user_login_id"].alias("User login Id"))
df2 = df.groupBy(df["events"]["Course_name"],df["events"]["section_name"],df["events"]["User_id"]).agg(max_(df["events"]["course_progress"])).alias("max_course_progress")

df3 = df1.join(df2,[df1["Title of the Course"]==df2["events[Course_name]"],df1["Name of the resources in the course"]==df2["events[section_name]"],df1["User_id"]==df2["events[User_id]"],df1["Course Completion Percentage"]==df2["max(events[course_progress])"]],"inner").select(df1['*'],df2["max(events[course_progress])"])
df3 = df3.drop("Course Completion Percentage for each learner in the batch")
df3 = df3.dropDuplicates()
# df3.show(100)
df4 = df.withColumn("Time stamp of completing the course",  F.from_utc_timestamp(F.to_timestamp(df["events"]["__time"]/ 1000),'UTC')).groupBy(df["events"]["Course_name"].alias("Title of the Course2"),df["events"]["User_id"].alias("User_id2")).agg(max_("Time stamp of completing the course"))
df4 = df3.join(df4,[df3["Title of the Course"]==df4["Title of the Course2"],df3["User_id"]==df4["User_id2"]],"inner").select(df3['*'],df4[("max(Time stamp of completing the course)")])
df4 = df4.drop("User_id")

df4 =df4.withColumnRenamed("Time","Time stamp of completing the resources")
df4 =df4.drop("max(events[course_progress])")
df4 =df4.withColumnRenamed("max(Time stamp of completing the course)","Time stamp of completing the course")
# df4.printSchema()
df4.show(100)

df4.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('course wise completion',header = 'true')