# Daily  Report Generation for Punjab Organisation
 
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
import smtplib,os, shutil
from email import encoders
from configparser import ConfigParser,ExtendedInterpolation
from pyspark.sql.functions import split, col,substring,regexp_replace

#
# config = ConfigParser(interpolation=ExtendedInterpolation())
#
# config.read('/opt/sparkjobs/source/reports/config.ini')

# Date time calculation and interval, used to get the required information from data_sources.
current_dt = datetime.now()
past_day_dt = current_dt.today() - timedelta(days=1)
interval = str(past_day_dt.date()) + "T11:30:00.00/" + str(current_dt.date()) + "T11:30:00.00"

#print(interval)

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
                StructField("__time", StringType(), True),
                StructField("solutionName", StringType(), True),
                StructField("programName", StringType(), True),
                StructField("location_validated_with_geotag", StringType(), True)

            ]
        )), True
                )
])

url = 'http://localhost:8888/druid/v2'
body = {"queryType": "scan", "dataSource": "p_data1", "resultFormat": "list",
            "columns":["user_id", "userName", "districtName", "schoolExternalId","blockName", "schoolName", "questionName", "questionExternalId", "questionResponseLabel", "evidences", "observationSubmissionId", "__time","programName", "solutionName", "schoolExternalId","location_validated_with_geotag"],
            "intervals": [interval], "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "isAPrivateProgram",
                    "value": "false"
                }]}}

response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(body))
print(response)
data_list = response.json()
# print(data_list)
spark = SparkSession.builder.appName("content_data_app").getOrCreate()

sc = spark.sparkContext
# hadoop_conf = sc._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
# hadoop_conf.set("fs.s3a.endpoint", "s3-ap-south-1.amazonaws.com")
# hadoop_conf.set("fs.s3a.access.key", "AKIASKKP73WSU6CCR67E")
# hadoop_conf.set("fs.s3a.secret.key", "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")

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
    df["exploded_events"]["__time"].alias("completed_date"),
    df["exploded_events"]["solutionName"].alias("solution_name"),
    df["exploded_events"]["programName"].alias("program_name"),
    df["exploded_events"]["location_validated_with_geotag"].alias("GeoTag")
)


df_2 = df_2.withColumn('date',split(df_2['completed_date'],'T').getItem(0)).withColumn('time_stamp', split(df_2['completed_date'],'T').getItem(1))
#df_2 = df_2.withColumn("time_stamp", F.concat(F.lit("T"),F.col("time_stamp")))
df_2 = df_2.drop('completed_date')


unique_solutions_list = df_2.select(df_2["solution_name"]).distinct().rdd.flatMap(lambda x: x).collect()
# print(unique_solutions_list)
unique_solutions_list1 = df_2.select(df_2["program_name"]).distinct().rdd.flatMap(lambda x: x).collect()
# print(unique_solutions_list1)
df_final = [df_2.where(df_2["solution_name"] == x) for x in unique_solutions_list]
df_final1 = [df_2.where(df_2["program_name"] == x) for x in unique_solutions_list1]
# print(df_final)
# df_final2 = [df_2.where((df_2["program_name"] == x)&(df_2["solution_name"] == x)) for x in [unique_solutions_list,unique_solutions_list1]]
# print(len(df_final2))
for j in range(len(df_final1)):
    # program_name = '_'.join(j.lower() for j in unique_solutions_list1[j].split())
    program_name = '_'.join(j for j in unique_solutions_list1[j].split())
    print(program_name)
    # print(j)
    # print(i)
    # df_final1[j].coalesce(1).write.format("csv").option("header", True).mode("overwrite").save(
    #     "/home/ajay/" + program_name + "/"
    # _
    for i in range(len(df_final)):
        solution_name = '_'.join(i.lower() for i in unique_solutions_list[i].split())
        df_finall = df_final[i].filter(df_final[i].program_name == program_name)
        # df_finall=df_final[i].filter(df_final[i].program_name == 'ka')
        # print(df_finall.show())
        df_finall.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save(
        "/home/ajay/primary_punjab_obs_report_daily/"+program_name+"/"+solution_name+"/"
        )

# new = None
# new = MIMEMultipart('mixed')
# body = MIMEMultipart('alternative')
#
# greetings_text = MIMEText('<html><p>Dear All</p></html>', 'html')
# new.attach(greetings_text)
#
# content_msg = None
# content_msg = MIMEText('<html><p>Please find the Daily attached Punjab observation data reports</p></html>', 'html')
# new.attach(content_msg)
#
# closure_msg = MIMEText('<html><p>Thank you</p><br/></html>', 'html')
# new.attach(closure_msg)
#
# message_disclaimer = MIMEText('<html><p style="color:red;"><b>*** THIS IS AUTO GENERATED. ANY COMMUNICATION TO THIS EMAIL ADDRESS WILL GO UNNOTICED. PLEASE USE <a href="https://seva.shikshalokam.org/" style="color:blue;">SEVA.SHIKSHALOKAM.ORG</a> FOR ALL COMMUNICATION RELATED***</b></p></html>', 'html')
# new.attach(message_disclaimer)
#
# sender = config.get("email","username")
# recipients = config.get('email','recipientEmailIsPunjab').split(',')
# new['Subject'] = str(current_dt.date()) + "_Daily Punjab Observation Reports for Primary Program"
# new['To'] = ", ".join(recipients)
# new['From'] = sender
#
# sampleAttachFolderPath = ["/tmp/reports/primary_punjab_obs_report.zip"]
# for att in sampleAttachFolderPath:
#        email_message = None
#        email_message = MIMEBase('application', 'zip')
#        email_message.set_payload(open(att, "rb").read())
#        encoders.encode_base64(email_message)
#        email_message.add_header('Content-Disposition', 'attachment; filename='+os.path.basename(att))
#        new.attach(MIMEMessage(email_message))
#
# server = smtplib.SMTP(config.get('email', 'smtpLibServer'), config.get('email', 'smtpLibPort'))
# server.ehlo()
# server.starttls()
# server.login(config.get('email', 'username'),config.get('email', 'password'))
# server.sendmail(sender,recipients, new.as_string())
# server.quit()
#
# for rem in sampleAttachFolderPath:
#     is_dir = os.path.isdir(rem)
#     if is_dir:
#         os.rmdir(rem)
#     else:
#         os.remove(rem)
#
#
