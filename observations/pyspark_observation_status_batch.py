# -----------------------------------------------------------------
# Name : pyspark_observation_status_batch.py
# Author : Shakthieshwari.A
# Description : Extracts the Status of the observation submissions 
#  either notStarted / In-Progress / Completed along with the users 
#  entity information
# -----------------------------------------------------------------

import requests
import json, csv, sys, os, time, redis
import datetime
from datetime import date
from configparser import ConfigParser, ExtendedInterpolation
from pymongo import MongoClient
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
# print(entitiesCollec)
# redis cache connection 
redis_connection = redis.ConnectionPool(
   host=config.get("REDIS", "host"),
   decode_responses=True,
   port=config.get("REDIS", "port"),
   db=config.get("REDIS", "db_name")
)
datastore = redis.StrictRedis(connection_pool=redis_connection)

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
         "userProfile":1
      }
   }]
)
print()

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
      StructField(
          'userProfile',StructType([StructField('userLocations',ArrayType(StructType([
              StructField('name', StringType(), True),
              StructField('type', StringType(), True),
              StructField('id', StringType(), True)
         ])
      ))]),True)
   ]
)
print(obs_sub_schema)
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
#obs_sub_dfn = obs_sub_df1.select(obs_sub_df1["userProfile.userLocation"])

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

   # obs_sub_df1["userProfile"][""].alias("c_name")
)

print(obs_sub_df)
obs_sub_cursorMongo.close()

obs_entities_id_df = obs_sub_df.select("state_externalId","block_externalId","district_externalId","cluster_externalId","school_externalId")
entitiesId_obs_status_df_before = []
entitiesId_arr = []
uniqueEntitiesId_arr = []
entitiesId_obs_status_df_before = obs_entities_id_df.toJSON().map(lambda j: json.loads(j)).collect()
print(entitiesId_obs_status_df_before)
for eid in entitiesId_obs_status_df_before:
   try:
    entitiesId_arr.append(eid["state_externalId"])
   except KeyError :
    pass
   try:
    entitiesId_arr.append(eid["block_externalId"])
   except KeyError :
    pass
   try:
    entitiesId_arr.append(eid["district_externalId"])
   except KeyError :
    pass
   try:
    entitiesId_arr.append(eid["cluster_externalId"])
   except KeyError :
    pass
   try:
    entitiesId_arr.append(eid["school_externalId"])
   except KeyError :
    pass

# print(entitiesId_arr)
uniqueEntitiesId_arr = list(removeduplicate(entitiesId_arr))
print(uniqueEntitiesId_arr)
ent_cursorMongo = entitiesCollec.aggregate(
   [{"$match": {"$or":[{"registryDetails.locationId":{"$in":uniqueEntitiesId_arr}},{"registryDetails.code":{"$in":uniqueEntitiesId_arr}}]}},
    {
      "$project": {
         "_id": {"$toString": "$_id"},
         "entityType": 1,
         "metaInformation": {"name": 1},
         "registryDetails": 1
      }
    }
   ])
ent_schema = StructType(
        [
            StructField("_id", StringType(), True),
            StructField("entityType", StringType(), True),
            StructField("metaInformation",
                StructType([StructField('name', StringType(), True)])
            ),
            StructField("registryDetails",
                StructType([StructField('locationId', StringType(), True),
                            StructField('code',StringType(), True)
                        ])
            )
        ]
    )
entities_rdd = spark.sparkContext.parallelize(list(ent_cursorMongo))
entities_df = spark.createDataFrame(entities_rdd,ent_schema)
entities_df = melt(entities_df,
        id_vars=["_id","entityType","metaInformation.name"],
        value_vars=["registryDetails.locationId", "registryDetails.code"]
    ).select("_id","entityType","name","value"
            ).dropDuplicates()
entities_df = entities_df.withColumn("variable",F.concat(F.col("entityType"),F.lit("_externalId")))
obs_sub_df_melt = melt(obs_sub_df,
        id_vars=["status","entity_externalId","entity_id","entity_type","user_id","solution_id",
            "solution_externalId","submission_id","entity_name","completedDate","program_id",
            "program_externalId","app_name","private_program","solution_type","ecm_marked_na",
            "updatedAt","role_title"],
        value_vars=["state_externalId","block_externalId","district_externalId","cluster_externalId","school_externalId"]
        )
obs_ent_sub_df_melt = obs_sub_df_melt\
                 .join(entities_df,["variable","value"],how="left")\
                 .select(obs_sub_df_melt["*"],entities_df["name"],entities_df["_id"].alias("entity_ids"))
obs_ent_sub_df_melt = obs_ent_sub_df_melt.withColumn("flag",F.regexp_replace(F.col("variable"),"_externalId","_name"))
obs_ent_sub_df_melt = obs_ent_sub_df_melt.groupBy(["status","submission_id"])\
                               .pivot("flag").agg(first(F.col("name")))

obs_sub_df_final = obs_sub_df.join(obs_ent_sub_df_melt,["status","submission_id"],how="left")
print(obs_sub_df_final.show())
ent_cursorMongo.close()

obs_sol_cursorMongo = solutionCollec.aggregate(
   [
      {"$match": {"type":"observation"}},
      {"$project": {"_id": {"$toString": "$_id"}, "name":1}}
   ]
)

#schema for the observation solution dataframe
obs_sol_schema = StructType([
   StructField('name', StringType(), True),
   StructField('_id', StringType(), True)
])

obs_soln_rdd = spark.sparkContext.parallelize(list(obs_sol_cursorMongo))
obs_soln_df = spark.createDataFrame(obs_soln_rdd,obs_sol_schema)
obs_sol_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_soln_df = obs_sub_df_final.join(
   obs_soln_df,
   obs_sub_df_final.solution_id==obs_soln_df._id,
   'inner'
).drop(obs_soln_df["_id"])
obs_sub_soln_df = obs_sub_soln_df.withColumnRenamed("name", "solution_name")

#observation program dataframe
obs_pgm_cursorMongo = programCollec.aggregate(
   [{"$project": {"_id": {"$toString": "$_id"}, "name": 1}}]
)

#schema for the observation program dataframe
obs_pgm_schema = StructType([
   StructField('name', StringType(), True),
   StructField('_id', StringType(), True)
])

obs_pgm_rdd = spark.sparkContext.parallelize(list(obs_pgm_cursorMongo))
obs_pgm_df = spark.createDataFrame(obs_pgm_rdd,obs_pgm_schema)
obs_pgm_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_pgm_df = obs_sub_soln_df.join(
   obs_pgm_df,
   obs_sub_soln_df.program_id==obs_pgm_df._id,
   'inner'
).drop(obs_pgm_df["_id"])
obs_sub_pgm_df = obs_sub_pgm_df.withColumnRenamed("name", "program_name")

integrated_app_column_list = []
survey_app_column_list = []
integrated_app_column_list = obs_sub_status_df_integrated_app.columns


missing_col_in_integrated_app_list = []
missing_col_in_integrated_app_list = list(
   set(integrated_app_column_list) - set(survey_app_column_list)
)

final_df = obs_sub_status_df_integrated_app.dropDuplicates()

final_df.coalesce(1).write.format("json").mode("overwrite").save(
   config.get("OUTPUT_DIR", "observation_status")+"/"
)

for filename in os.listdir(config.get("OUTPUT_DIR", "observation_status")+"/"):
   if filename.endswith(".json"):
      os.rename(
         config.get("OUTPUT_DIR", "observation_status") + "/" + filename, 
         config.get("OUTPUT_DIR", "observation_status") + "/sl_observation_status.json"
      )

# blob_service_client = BlockBlobService(
#    account_name=config.get("AZURE", "account_name"),
#    sas_token=config.get("AZURE", "sas_token")
# )
# container_name = config.get("AZURE", "container_name")
local_path = config.get("OUTPUT_DIR", "observation_status")
# blob_path = config.get("AZURE", "observation_blob_path")

# for files in os.listdir(local_path):
#    if "sl_observation_status.json" in files:
#       blob_service_client.create_blob_from_path(
#          container_name,
#          os.path.join(blob_path,files),
#          local_path + "/" + files
#       )

# sl_status_spec = {}
# sl_status_spec = json.loads(config.get("DRUID","observation_status_injestion_spec"))
# datasources = [sl_status_spec["spec"]["dataSchema"]["dataSource"]]
# ingestion_specs = [json.dumps(sl_status_spec)]
#
# for i,j in zip(datasources,ingestion_specs):
#    druid_end_point = config.get("DRUID", "metadata_url") + i
#    druid_batch_end_point = config.get("DRUID", "batch_url")
#    headers = {'Content-Type': 'application/json'}
#    get_timestamp = requests.get(druid_end_point, headers=headers)
#    successLogger.debug(get_timestamp)
#    if get_timestamp.status_code == 200 :
#       successLogger.debug("Successfully fetched time stamp of the datasource " + i)
#       timestamp = get_timestamp.json()
#       #calculating interval from druid get api
#       minTime = timestamp["segments"]["minTime"]
#       maxTime = timestamp["segments"]["maxTime"]
#       min1 = datetime.datetime.strptime(minTime,"%Y-%m-%dT%H:%M:%S.%fZ")
#       max1 = datetime.datetime.strptime(maxTime,"%Y-%m-%dT%H:%M:%S.%fZ")
#       new_format = "%Y-%m-%d"
#       min1.strftime(new_format)
#       max1.strftime(new_format)
#       minmonth = "{:02d}".format(min1.month)
#       maxmonth = "{:02d}".format(max1.month)
#       min2 = str(min1.year) + "-" + minmonth + "-" + str(min1.day)
#       max2 = str(max1.year) + "-" + maxmonth  + "-" + str(max1.day)
#       interval = min2 + "_" + max2
#       successLogger.debug(interval)
#
#       time.sleep(50)
#
#       disable_datasource = requests.delete(druid_end_point, headers=headers)
#       if disable_datasource.status_code == 200:
#          successLogger.debug("successfully disabled the datasource " + i)
#          time.sleep(300)
#
#          delete_segments = requests.delete(
#             druid_end_point + "/intervals/" + interval, headers=headers
#          )
#          if delete_segments.status_code == 200:
#             successLogger.debug("successfully deleted the segments " + i)
#             time.sleep(300)
#
#             enable_datasource = requests.get(druid_end_point, headers=headers)
#             if enable_datasource.status_code == 204:
#                successLogger.debug("successfully enabled the datasource " + i)
#
#                time.sleep(300)
#
#                start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
#                successLogger.debug("ingest data")
#                if start_supervisor.status_code == 200:
#                   successLogger.debug(
#                      "started the batch ingestion task sucessfully for the datasource " + i
#                   )
#                   time.sleep(50)
#                else:
#                   errorLogger.error(
#                      "failed to start batch ingestion task" + str(start_supervisor.status_code)
#                   )
#             else:
#                errorLogger.error("failed to enable the datasource " + i)
#          else:
#             errorLogger.error("failed to delete the segments of the datasource " + i)
#       else:
#          errorLogger.error("failed to disable the datasource " + i)
#
#    elif get_timestamp.status_code == 204:
#       start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
#       if start_supervisor.status_code == 200:
#          successLogger.debug(
#             "started the batch ingestion task sucessfully for the datasource " + i
#          )
#          time.sleep(50)
#       else:
#          errorLogger.error(
#             "failed to start batch ingestion task" + str(start_supervisor.status_code)
#          )
#          errorLogger.error(start_supervisor.json())
#    else:
#       errorLogger.error("failed to get the timestamp of the datasource " + i)
