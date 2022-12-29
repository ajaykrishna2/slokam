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
# print()
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
              StructField('id', StringType(), True),
              StructField('code', StringType(), True)

         ])
      ))]),True)
   ]
)
# print(obs_sub_schema)
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
# obs_sub_dfn = obs_sub_df1.select(obs_sub_df1["userProfile.userLocations"])

obs_sub_dfn1=obs_sub_df1.withColumn(
   "exploded_userProfile",F.explode_outer(F.col('userProfile.userLocations'))
)
obs_sub_dfn1.select(obs_sub_dfn1["exploded_userProfile"]['name'],obs_sub_dfn1["exploded_userProfile"]['type']).show()
# obs_sub_dfn1.show()
# sys.exit()
df=obs_sub_dfn1.take(1)
# print(df)
# obs_sub_dfn2=obs_sub_dfn.withColumn(
#    "exploded_userProfile",F.explode_outer(F.col('userLocations.type'))
# )
# print(type(obs_sub_dfn1["exploded_userProfile"]['name']))

# print(obs_sub_dfn1.select(obs_sub_dfn1["exploded_userProfile"]['name']))
# print(obs_sub_dfn1.show())
# print(obs_sub_dfn2.show())
entities_df = melt(obs_sub_dfn1,
        id_vars=["exploded_userProfile.name","exploded_userProfile.id","exploded_userProfile.type"],
        value_vars=["exploded_userProfile.code"]
    ).select("name","value","id","type").dropDuplicates()
print(entities_df.show())
entities_df = entities_df.withColumn("variable",F.concat(F.col("type"),F.lit("_externalId")))
entities_df.show()
sys.exit()
# print(entities_df.take(1))
# entities_df.show()
# print('below is filtered')
# entities_df[entities_df['value']=='district'].filter(F.col('id') == 'a87aa157-0d73-4aa3-9623-cb2f8a3ecf32').show()
# print('above is filtered')

# print(entities_df[entities_df['value']=='state'].show())
# print(entities_df[entities_df['value']=='block'].)
# print(entities_df[entities_df['value']=='school'].count())
entities_df[entities_df['value']=='district']
obs_sub_df = entities_df.select(
entities_df[entities_df['value']=='district'].alias('district_name')
)
# print(obs_sub_df)
# entities_df[entities_df['value']=='district'].coalesce(1).write.format('district.csv').save('/home/ajay/output/district.csv',header = 'true')
# entities_df[entities_df['value']=='district']\
#   .write \
#   .mode('overwrite') \
#   .option('header', 'true') \
#   .csv('/home/ajay/output/district.csv')
# entities_df[entities_df['value']=='state']\
#   .write \
#   .mode('overwrite') \
#   .option('header', 'true') \
#   .csv('/home/ajay/output/state.csv')
# entities_df[entities_df['value']=='block']\
#   .write \
#   .mode('overwrite') \
#   .option('header', 'true') \
#   .csv('/home/ajay/output/block.csv')
# entities_df[entities_df['value']=='school']\
#   .write \
#   .mode('overwrite') \
#   .option('header', 'true') \
#   .csv('/home/ajay/output/school.csv')

# for i in entities_df.collect():
#    for j in i:
#       print(j)