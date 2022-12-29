# -----------------------------------------------------------------
# Name : pyspark_observation_status_batch.py
# Author : Shakthieshwari.A
# Description : Extracts the Status of the observation submissions
#  either notStarted / In-Progress / Completed along with the users
#  entity information
# -----------------------------------------------------------------

import requests
import json, csv, sys, os, time
import datetime
from datetime import date
from configparser import ConfigParser, ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.objectid import ObjectId
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel
import databricks.koalas as ks
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob import ContentSettings
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from pyspark.sql import DataFrame
from typing import Iterable
from pyspark.sql.functions import element_at, split, col

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
   config.get('LOGS','observation_status_success')
)
successBackuphandler = TimedRotatingFileHandler(
   config.get('LOGS','observation_status_success'),
   when="w0",
   backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
   config.get('LOGS','observation_status_error')
)
errorBackuphandler = TimedRotatingFileHandler(
   config.get('LOGS','observation_status_error'),
   when="w0",
   backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
   def removeduplicate(it):
      seen = []
      for x in it:
         if x not in seen:
            yield x
            seen.append(x)
except Exception as e:
   errorLogger.error(e, exc_info=True)

try:
   def chunks(l, n):
      for i in range(0, len(l), n):
         yield l[i:i + n]
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
   def convert_to_row(d: dict) -> Row:
      return Row(**OrderedDict(sorted(d.items())))
except Exception as e:
   errorLogger.error(e,exc_info=True)

try:
 def melt(df: DataFrame,id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str="variable", value_name: str="value") -> DataFrame:

    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)
except Exception as e:
   errorLogger.error(e,exc_info=True)

clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
obsSubmissionsCollec = db[config.get('MONGO', 'observation_sub_collection')]
solutionCollec = db[config.get('MONGO', 'solutions_collection')]
userRolesCollec = db[config.get("MONGO", 'user_roles_collection')]
programCollec = db[config.get("MONGO", 'programs_collection')]
entitiesCollec = db[config.get('MONGO', 'entities_collection')]

#observation submission dataframe
obs_sub_cursorMongo = obsSubmissionsCollec.aggregate(
   [{
      "$project": {
         "_id": {"$toString": "$_id"},
         "entityId": {"$toString": "$entityId"},
         "status": 1,
         "entityExternalId": 1,
         "entityInformation": {"name": 1},
         "entityType": 1,
         "createdBy": 1,
         "solutionId": {"$toString": "$solutionId"},
         "solutionExternalId": 1,
         "updatedAt": 1,
         "completedDate": 1,
         "programId": {"$toString": "$programId"},
         "programExternalId": 1,
         "appInformation": {"appName": 1},
         "isAPrivateProgram": 1,
         "isRubricDriven":1,
         "criteriaLevelReport":1,
         "ecm_marked_na": {
            "$reduce": {
               "input": "$evidencesStatus",
               "initialValue": "",
               "in": {
                  "$cond": [
                     {"$eq": [{"$toBool":"$$this.notApplicable"},True]},
                     {"$concat" : ["$$value", "$$this.name", ";"]},
                     "$$value"
                  ]
               }
            }
         },
         "userRoleInformation": 1,
         "answers":1,
         "userProfile": 1
      }
   }]
)

#schema for the observation submission dataframe
obs_sub_schema = StructType(
   [
      StructField('status', StringType(), True),
      StructField('entityExternalId', StringType(), True),
      StructField('entityId', StringType(), True),
      StructField('entityType', StringType(), True),
      StructField('createdBy', StringType(), True),
      StructField('solutionId', StringType(), True),
      StructField('solutionExternalId', StringType(), True),
      StructField('programId', StringType(), True),
      StructField('programExternalId', StringType(), True),
      StructField('_id', StringType(), True),
      StructField('updatedAt', TimestampType(), True),
      StructField('completedDate', TimestampType(), True),
      StructField('isAPrivateProgram', BooleanType(), True),
      StructField(
         'entityInformation',
         StructType([StructField('name', StringType(), True)])
      ),
      StructField(
         'appInformation',
         StructType([StructField('appName', StringType(), True)])
      ),
      StructField('isRubricDriven',StringType(),True),
      StructField('criteriaLevelReport',StringType(),True),
      StructField('ecm_marked_na', StringType(), True),
      StructField(
          'userRoleInformation',
          StructType([
              StructField('state', StringType(), True),
              StructField('block', StringType(), True),
              StructField('district', StringType(), True),
              StructField('cluster', StringType(), True),
              StructField('school', StringType(), True),
              StructField('role', StringType(), True)
         ])
      ),
      # StructField(
      #    'evidences'
      #    ,StructField(['evidences',ArrayType(
      #    ),True),
      StructField('answers',StringType() ,True),
      # StructField('answers',ArrayType(StructType([
      #         StructField('qid', StringType(), True),
      #         StructField('responseType', Type(), True)
      #    ]))),

         StructField(
          'userProfile',
          StructType([StructField('userLocations',ArrayType(StructType([
              StructField('name', StringType(), True),
              StructField('type', StringType(), True),
              StructField('id', StringType(), True),
              StructField('code', StringType(), True)
         ])
      )),
              StructField('rootOrgId', StringType(), True),
              StructField('userName', StringType(), True),
              StructField('userId', StringType(), True),
              StructField(
                  'framework',
                  StructType([
                    StructField('board',ArrayType(StringType()), True)
                ])
             ),
             StructField(
                'organisations',ArrayType(
                     StructType([
                        StructField('organisationId', StringType(), True),
                        StructField('orgName', StringType(), True)
                     ]), True)
             )
          ])
      )
   ]
)

spark = SparkSession.builder.appName(
   "obs_sub_status"
).config(
   "spark.driver.memory", "50g"
).config(
   "spark.executor.memory", "100g"
).config(
   "spark.memory.offHeap.enabled", True
).config(
   "spark.memory.offHeap.size", "32g"
).getOrCreate()

sc=spark.sparkContext

obs_sub_rdd = spark.sparkContext.parallelize(list(obs_sub_cursorMongo))
obs_sub_df1 = spark.createDataFrame(obs_sub_rdd,obs_sub_schema)

obs_sub_df1 = obs_sub_df1.withColumn(
   "app_name",
   F.when(
      obs_sub_df1["appInformation"]["appName"].isNull(),
      F.lit(config.get("ML_APP_NAME", "survey_app"))
   ).otherwise(
      lower(obs_sub_df1["appInformation"]["appName"])
   )
)

obs_sub_df1 = obs_sub_df1.withColumn(
   "private_program",
   F.when(
      (obs_sub_df1["isAPrivateProgram"].isNotNull() == True) &
      (obs_sub_df1["isAPrivateProgram"] == True),
      "true"
   ).when(
      (obs_sub_df1["isAPrivateProgram"].isNotNull() == True) &
      (obs_sub_df1["isAPrivateProgram"] == False),
      "false"
   ).otherwise("true")
)

obs_sub_df1 = obs_sub_df1.withColumn(
   "solution_type",
   F.when(
      (obs_sub_df1["isRubricDriven"].isNotNull() == True) &
      (obs_sub_df1["isRubricDriven"] == True) &
      (obs_sub_df1["criteriaLevelReport"] == True),
      "observation_with_rubric"
   ).when(
      (obs_sub_df1["isRubricDriven"].isNotNull() == True) &
      (obs_sub_df1["isRubricDriven"] == True) &
      (obs_sub_df1["criteriaLevelReport"] == False),
      "observation_with_out_rubric"
   ).when(
      (obs_sub_df1["isRubricDriven"].isNotNull() == True) &
      (obs_sub_df1["isRubricDriven"] == False),
      "observation_with_out_rubric"
   ).otherwise("observation_with_out_rubric")
)
print(obs_sub_df1.show())
obs_sub_df1 = obs_sub_df1.withColumn("parent_channel",F.lit("SHIKSHALOKAM"))
obs_sub_df = obs_sub_df1.select(
   "status",
   obs_sub_df1["entityExternalId"].alias("entity_externalId"),
   obs_sub_df1["entityId"].alias("entity_id"),
   obs_sub_df1["entityType"].alias("entity_type"),
   obs_sub_df1["createdBy"].alias("user_id"),
   obs_sub_df1["solutionId"].alias("solution_id"),
   obs_sub_df1["solutionExternalId"].alias("solution_externalId"),
   obs_sub_df1["_id"].alias("submission_id"),
   obs_sub_df1["entityInformation"]["name"].alias("entity_name"),
   "completedDate",
   obs_sub_df1["programId"].alias("program_id"),
   obs_sub_df1["programExternalId"].alias("program_externalId"),
   obs_sub_df1["app_name"],
   obs_sub_df1["private_program"],
   obs_sub_df1["solution_type"],
   obs_sub_df1["ecm_marked_na"],
   "updatedAt",
   obs_sub_df1["userRoleInformation"]["role"].alias("role_title"),
   obs_sub_df1["userRoleInformation"]["state"].alias("state_externalId"),
   obs_sub_df1["userRoleInformation"]["block"].alias("block_externalId"),
   obs_sub_df1["userRoleInformation"]["district"].alias("district_externalId"),
   obs_sub_df1["userRoleInformation"]["cluster"].alias("cluster_externalId"),
   obs_sub_df1["userRoleInformation"]["school"].alias("school_externalId"),
   obs_sub_df1["userProfile"]["rootOrgId"].alias("channel"),
   obs_sub_df1["userProfile"]["userName"].alias("userName"),
   obs_sub_df1["userProfile"]["userId"].alias("userId"),
   obs_sub_df1["answers"],
   obs_sub_df1["parent_channel"],
   concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
   element_at(col('userProfile.organisations.orgName'), -1).alias("organisation_name"),
   element_at(col('userProfile.organisations.organisationId'), -1).alias("organisation_id")
)
# print(obs_sub_df.show())
# obs_sub_dfn1=obs_sub_df.withColumn(
#    "exploded_evidences",F.explode_outer(F.col('evidences'))
# )
# print(obs_sub_dfn1)
# print(obs_sub_df1.select(obs_sub_df1["exploded_evidences"]).show())
# obs_sub_dfn1=obs_sub_df.withColumn(
#    "exploded_answers",F.explode_outer(F.col('answers'))
# )
# obs_sub_df1.collect()
df1=obs_sub_df.filter(obs_sub_df["answers"].isNull())
df2=obs_sub_df.filter(obs_sub_df["answers"].isNotNull())
print(df2.show())
schema = StructType(
    [
        StructField('qid', StringType(), True),
        StructField('responseType', StringType(), True)
    ]
)
dn=df2.withColumn("answers", from_json("answers", schema))\
    .select( col('answers.*'))
dn.select(dn['qid'].isNotNull()).show()
dn.filter(dn['qid'].isNotNull()==True).show()
# schma = obs_sub_df["answers"].schema.json()
# print(schma)
# df3=df2.select(df2["exploded_evidences"])
# df3.show()

# print(df3
#   .select("answers")
#   .rdd
#   .map(lambda x: json.loads(x))
#   .toDF()

# df2.withColumn("answers", split(col("answers"), ",").cast("array<long>"))
# print(df2.dtypes)
# output=df2.agg(collect_list(col("name"))).collect()[0][0]

# print(df2.select('answers').flatMap(lambda x: x).collect())


#['CA', 'NY', 'CA', 'FL']

for i in df2.select('answers'):
   print(i)
# row_list = df2.select('answers').collect()
# sno_id_array = [ row.sno_id for row in row_list]
# print(sno_id_array)
# print(df2.select(df2["answers"]).rdd.flatMap(lambda x: x).collect())
# print([data for data in ])
# if obs_sub_df.select(obs_sub_df["answers"])  :
#    print(obs_sub_df.select(obs_sub_df["answers"]).show())
# else:
#    obs_sub_df.select(obs_sub_df["answers"][0]).show()

# #observation solution dataframe
# obs_sol_cursorMongo = solutionCollec.aggregate(
#    [
#       {"$match": {"type":"observation"}},
#       {"$project": {"_id": {"$toString": "$_id"}, "name":1}}
#    ]
# )
#
# #schema for the observation solution dataframe
# obs_sol_schema = StructType([
#    StructField('name', StringType(), True),
#    StructField('_id', StringType(), True)
# ])
#
# obs_soln_rdd = spark.sparkContext.parallelize(list(obs_sol_cursorMongo))
# obs_soln_df = spark.createDataFrame(obs_soln_rdd,obs_sol_schema)
# obs_sol_cursorMongo.close()
#
# #match solution id from solution df to submission df to fetch the solution name
# obs_sub_soln_df = obs_sub_df.join(
#    obs_soln_df,
#    obs_sub_df.solution_id==obs_soln_df._id,
#    'inner'
# ).drop(obs_soln_df["_id"])
# obs_sub_soln_df = obs_sub_soln_df.withColumnRenamed("name", "solution_name")
#
# #observation program dataframe
# obs_pgm_cursorMongo = programCollec.aggregate(
#    [{"$project": {"_id": {"$toString": "$_id"}, "name": 1}}]
# )
#
# #schema for the observation program dataframe
# obs_pgm_schema = StructType([
#    StructField('name', StringType(), True),
#    StructField('_id', StringType(), True)
# ])
#
# obs_pgm_rdd = spark.sparkContext.parallelize(list(obs_pgm_cursorMongo))
# obs_pgm_df = spark.createDataFrame(obs_pgm_rdd,obs_pgm_schema)
# obs_pgm_cursorMongo.close()
#
# #match solution id from solution df to submission df to fetch the solution name
# obs_sub_pgm_df = obs_sub_soln_df.join(
#    obs_pgm_df,
#    obs_sub_soln_df.program_id==obs_pgm_df._id,
#    'inner'
# ).drop(obs_pgm_df["_id"])
# obs_sub_pgm_df = obs_sub_pgm_df.withColumnRenamed("name", "program_name")
#
# # roles dataframe from mongodb
# roles_cursorMongo = userRolesCollec.aggregate(
#    [{"$project": {"_id": {"$toString": "$_id"}, "title": 1}}]
# )
#
# #schema for the observation solution dataframe
# roles_schema = StructType([
#    StructField('title', StringType(), True),
#    StructField('_id', StringType(), True)
# ])
#
# roles_rdd = spark.sparkContext.parallelize(list(roles_cursorMongo))
# roles_df = spark.createDataFrame(roles_rdd, roles_schema)
# roles_cursorMongo.close()
#
# # user roles along with entity from elastic search
# userEntityRoleArray = []
#
# try:
#    def elasticSearchJson(userEntityJson) :
#       for user in userEntityJson :
#          try:
#             if len(user["_source"]["data"]["roles"]) > 0 :
#                for roleObj in user["_source"]["data"]["roles"]:
#                   try:
#                      if len(roleObj["entities"]) > 0:
#                         for ent in roleObj["entities"]:
#                            entObj = {}
#                            entObj["userId"] = user["_source"]["data"]["userId"]
#                            entObj["roleId"] = roleObj["roleId"]
#                            entObj["roleCode"] =roleObj["code"]
#                            entObj["entityId"] = ent
#                            userEntityRoleArray.append(entObj)
#                      else :
#                         entNoObj = {}
#                         entNoObj["userId"] = user["_source"]["data"]["userId"]
#                         entNoObj["roleId"] = roleObj["roleId"]
#                         entNoObj["roleCode"] = roleObj["code"]
#                         entNoObj["entityId"] = None
#                         userEntityRoleArray.append(entNoObj)
#                   except KeyError :
#                      entNoEntObj = {}
#                      entNoEntObj["userId"] = user["_source"]["data"]["userId"]
#                      entNoEntObj["roleId"] = roleObj["roleId"]
#                      entNoEntObj["roleCode"] = roleObj["code"]
#                      entNoEntObj["entityId"] = None
#                      userEntityRoleArray.append(entNoEntObj)
#                      pass
#          except KeyError :
#             pass
# except Exception as e:
#    errorLogger.error(e, exc_info=True)
#
# headers_user = {'Content-Type': 'application/json'}
#
# obs_sub_df1.cache()
# obs_sub_df.cache()
# obs_soln_df.cache()
# roles_df.cache()
#
# final_df = obs_sub_pgm_df.dropDuplicates()
# print(final_df.show())
# final_df1=final_df.sort(col("district_name"),col("state_name")).show(truncate=False)
# final_df1.show()
obs_sub_df.coalesce(1).write.format("json").mode("overwrite").save(
   config.get("OUTPUT_DIR", "observation_status")+"/"
)

for filename in os.listdir(config.get("OUTPUT_DIR", "observation_status")+"/"):
   if filename.endswith(".json"):
      os.rename(
         config.get("OUTPUT_DIR", "observation_status") + "/" + filename,
         config.get("OUTPUT_DIR", "observation_status") + "/sl_observation_status.json"
      )

local_path = config.get("OUTPUT_DIR", "observation_status")

