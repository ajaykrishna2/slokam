import shutil

import requests
import json, csv, sys, os, time
import datetime
from datetime import date
from configparser import ConfigParser, ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
import functools
from pyspark.sql import DataFrame
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

data=[]
def sam(lsts, key):
  return [x.get(key) for x in lsts]
with open('/home/ajay/Downloads/data2.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        data.append(row)
    pr_id=sam(data,'program_id')
# print(pr_id)
data1=[]
for i in pr_id:
    # print(type(i))
    obs_sol_cursorMongo = solutionCollec.aggregate(
       [
          {"$match": {"type": "observation"}},

          {"$project": {"_id": {"$toString": "$_id"}, "programId": {"$toString": "$programId"},"name": {"$toString": "$name"}}},
          {"$match": {"programId": f'''{i}'''}}
       ]
    )
    # schema for the observation solution dataframe
    obs_sol_schema = StructType([
       StructField('programId', StringType(), True),
       StructField('_id', StringType(), True),
       StructField('name', StringType(), True)
    ])
    obs_soln_rdd = spark.sparkContext.parallelize(list(obs_sol_cursorMongo))
    obs_soln_df = spark.createDataFrame(obs_soln_rdd, obs_sol_schema)
    row_list=obs_soln_df.select("_id").collect()
    daata = [row._id for row in row_list]
    # print(sno_id_array)
    # daata=obs_soln_df.select("YOUR_COLUMN_NAME").rdd.map(r => r(0)).collect()
    # print(daata)
    data1.append(daata)

flat_list = []
for xs in data1:
    for x in xs:
        flat_list.append(x)

output_dfs = []
emp_RDD = spark.sparkContext.emptyRDD()

# Create an expected schema
columns = StructType([StructField('state_name',StringType(), True),StructField('block_name',StringType(), True),StructField('school_name',StringType(), True),StructField('cluster_name',StringType(), True),StructField('district_name',StringType(), True)])

# Create an empty RDD with expected schema
df = spark.createDataFrame(data=emp_RDD,schema=columns)
for k in flat_list:
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
                "isRubricDriven": 1,
                "criteriaLevelReport": 1,
                "ecm_marked_na": {
                    "$reduce": {
                        "input": "$evidencesStatus",
                        "initialValue": "",
                        "in": {
                            "$cond": [
                                {"$eq": [{"$toBool": "$$this.notApplicable"}, True]},
                                {"$concat": ["$$value", "$$this.name", ";"]},
                                "$$value"
                            ]
                        }
                    }
                },
                "userRoleInformation": 1,
                "answers":1,
                "userProfile": {"qid": 1}
            }
        },{"$match": {"solutionId": f'''{k}'''}}]
    )
    # obs_sub_cursorMongo.printschema()
    obs_sub_schema = StructType(
        [
            StructField('status', StringType(), True),
            StructField('solutionId', StringType(), True),
            StructField('solutionExternalId', StringType(), True),
            StructField('programId', StringType(), True),
            StructField('programExternalId', StringType(), True),
            StructField('_id', StringType(), True),

            StructField('completedDate', TimestampType(), True),

            StructField(
                'userRoleInformation',
                StructType([
                    StructField('state', StringType(), True),
                    StructField('block', StringType(), True),
                    StructField('district', StringType(), True),
                    StructField('cluster', StringType(), True),
                    StructField('school', StringType(), True)
                ])
            ),
            # StructField(
            #    'evidences'
            #    ,StructField(['evidences',ArrayType(
            #    ),True),
            StructField('answers', MapType(StringType(),StructType([
                    StructField('qid', StringType(), True),
                    StructField('responseType', StringType(), True),
                    StructField('gpsLocation', StringType(), True),
                    StructField('payload',StructType([
                    StructField('question', StringType(), True),
                    StructField('labels', StringType(), True)
                    ]))
               ]), True)),

            # StructField('answers',StructType([
            #         StructField('qid', StringType(), True),
            #         StructField('responseType', StringType(), True)
            #    ])),
            StructField(
                'userProfile',
                StructType([
                    StructField('rootOrgId', StringType(), True),
                    StructField('userName', StringType(), True),
                    StructField('userId', StringType(), True),
                    StructField(
                        'framework',
                        StructType([
                            StructField('board', ArrayType(StringType()), True)
                        ])
                    ),
                    StructField(
                        'organisations', ArrayType(
                            StructType([
                                StructField('organisationId', StringType(), True),
                                StructField('orgName', StringType(), True)
                            ]), True)
                    ),
                    StructField('userLocations', ArrayType(StructType([
                        StructField('name', StringType(), True),
                        StructField('type', StringType(), True),
                        StructField('id', StringType(), True),
                        StructField('code', StringType(), True)
                    ])
                    ))
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

    sc = spark.sparkContext

    obs_sub_rdd = spark.sparkContext.parallelize(list(obs_sub_cursorMongo))
    obs_sub_df1 = spark.createDataFrame(obs_sub_rdd, obs_sub_schema)


    obs_sub_df = obs_sub_df1.select(
        "status",
        obs_sub_df1["solutionId"].alias("solution_id"),
        obs_sub_df1["solutionExternalId"].alias("solution_externalId"),
        obs_sub_df1["_id"].alias("submission_id"),
        "completedDate",
        obs_sub_df1["programId"].alias("program_id"),
        obs_sub_df1["programExternalId"].alias("program_externalId"),
        obs_sub_df1["userRoleInformation"]["state"].alias("state_externalId"),
        obs_sub_df1["userRoleInformation"]["block"].alias("block_externalId"),
        obs_sub_df1["userRoleInformation"]["district"].alias("district_externalId"),
        obs_sub_df1["userRoleInformation"]["cluster"].alias("cluster_externalId"),
        obs_sub_df1["userRoleInformation"]["school"].alias("school_externalId"),
        obs_sub_df1["userProfile"]["userLocations"].alias("userProfile"),
        obs_sub_df1["userProfile"]["userName"].alias("userName"),
        obs_sub_df1["userProfile"]["userId"].alias("userId"),
        obs_sub_df1["answers"],
        obs_sub_df1["completedDate"],
        concat_ws(",", F.col("userProfile.framework.board")).alias("board_name"),
        element_at(col('userProfile.organisations.orgName'), -1).alias("organisation_name"),
        element_at(col('userProfile.organisations.organisationId'), -1).alias("organisation_id")
    )
    obs_entities_id_df = obs_sub_df.select("state_externalId", "block_externalId", "district_externalId",
                                           "cluster_externalId", "school_externalId")
    entitiesId_obs_status_df_before = []
    entitiesId_arr = []
    uniqueEntitiesId_arr = []
    entitiesId_obs_status_df_before = obs_entities_id_df.toJSON().map(lambda j: json.loads(j)).collect()
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
    uniqueEntitiesId_arr = list(removeduplicate(entitiesId_arr))
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
            id_vars=["status","solution_id","userName","userId","solution_externalId","submission_id","completedDate","program_id","program_externalId","board_name","organisation_name","organisation_id","answers"],
            value_vars=["state_externalId","block_externalId","district_externalId","cluster_externalId","school_externalId"]
            )
    obs_ent_sub_df_melt = obs_sub_df_melt\
                     .join(entities_df,["variable","value"],how="left")\
                     .select(obs_sub_df_melt["*"],entities_df["name"],entities_df["_id"].alias("entity_ids"))
    obs_ent_sub_df_melt = obs_ent_sub_df_melt.withColumn("flag",F.regexp_replace(F.col("variable"),"_externalId","_name"))
    obs_ent_sub_df_melt = obs_ent_sub_df_melt.groupBy(["status","submission_id"])\
                                   .pivot("flag").agg(first(F.col("name")))

    obs_sub_df_final = obs_sub_df.join(obs_ent_sub_df_melt,["status","submission_id"],how="left")
    obs_sol_cursorMongo = solutionCollec.aggregate(
        [
            {"$match": {"type": "observation"}},
            {"$project": {"_id": {"$toString": "$_id"}, "name": 1}}
        ]
    )

    # schema for the observation solution dataframe
    obs_sol_schema = StructType([
        StructField('name', StringType(), True),
        StructField('_id', StringType(), True)
    ])

    obs_soln_rdd = spark.sparkContext.parallelize(list(obs_sol_cursorMongo))
    obs_soln_df = spark.createDataFrame(obs_soln_rdd, obs_sol_schema)
    obs_sol_cursorMongo.close()

    # match solution id from solution df to submission df to fetch the solution name
    obs_sub_soln_df = obs_sub_df_final.join(
        obs_soln_df,
        obs_sub_df_final.solution_id == obs_soln_df._id,
        'inner'
    ).drop(obs_soln_df["_id"])
    obs_sub_soln_df = obs_sub_soln_df.withColumnRenamed("name", "solution_name")

    # observation program dataframe
    obs_pgm_cursorMongo = programCollec.aggregate(
        [{"$project": {"_id": {"$toString": "$_id"}, "name": 1}}]
    )

    # schema for the observation program dataframe
    obs_pgm_schema = StructType([
        StructField('name', StringType(), True),
        StructField('_id', StringType(), True)
    ])

    obs_pgm_rdd = spark.sparkContext.parallelize(list(obs_pgm_cursorMongo))
    obs_pgm_df = spark.createDataFrame(obs_pgm_rdd, obs_pgm_schema)
    obs_pgm_cursorMongo.close()

    # match solution id from solution df to submission df to fetch the solution name
    obs_sub_pgm_df = obs_sub_soln_df.join(
        obs_pgm_df,
        obs_sub_soln_df.program_id == obs_pgm_df._id,
        'inner'
    ).drop(obs_pgm_df["_id"])
    obs_sub_pgm_df = obs_sub_pgm_df.withColumnRenamed("name", "program_name")
    obs_sub_df_final = obs_sub_pgm_df
    if 'state_name' not in obs_sub_df_final.columns:
        obs_sub_df_final = obs_sub_df_final.withColumn('state_name', lit(None).cast(StringType()))
    if 'block_name' not in obs_sub_df_final.columns:
        obs_sub_df_final = obs_sub_df_final.withColumn('block_name', lit(None).cast(StringType()))
    if 'cluster_name' not in obs_sub_df_final.columns:
        obs_sub_df_final = obs_sub_df_final.withColumn('cluster_name', lit(None).cast(StringType()))
    if 'school_name' not in obs_sub_df_final.columns:
        obs_sub_df_final = obs_sub_df_final.withColumn('school_name', lit(None).cast(StringType()))
    if 'district_name' not in obs_sub_df_final.columns:
        obs_sub_df_final = obs_sub_df_final.withColumn('district_name', lit(None).cast(StringType()))

    obs_sub_df12 = obs_sub_df_final.filter(obs_sub_df["answers"].isNull())
    obs_sub_df11=obs_sub_df_final.filter(obs_sub_df["answers"].isNotNull())
    obs_sub_df1=obs_sub_df11.select( "submission_id",explode('answers'))

    obs_sub_df2=obs_sub_df1.select("submission_id","value", obs_sub_df1.value['qid'].alias('qid'), obs_sub_df1.value['responseType'].alias('responseType'), obs_sub_df1.value['gpsLocation'].alias('gpsLocation'),obs_sub_df1.value['payload'].alias('payload'))
    # user_schema = ArrayType(
    #     StructType([
    #         StructField("name", ArrayType(StringType()), True),
    #         StructField("sourcePath", ArrayType(StringType()), True)
    #     ])
    # )
    # obs_sub_df3 = (obs_sub_df2.withColumn("fileName1", F.from_json("fileName", user_schema))
    #        .selectExpr("inline(fileName)"))
    # obs_sub_df3.show()
    obs_sub_df3=obs_sub_df2.select("submission_id","payload" ,obs_sub_df2.payload['question'].alias('question'), obs_sub_df2.payload['labels'].alias('labels'))
    # obs_sub_df4 = obs_sub_df2.select("submission_id", "fileName", obs_sub_df2.fileName['sourcePath'].alias('evidences'))

    obs_sub_dfn=obs_sub_df11.join(obs_sub_df2, ["submission_id"],how='left')
    obs_sub_dfn1 = obs_sub_dfn.join(obs_sub_df3, ["submission_id"],how='left')
    # obs_sub_dfn2 = obs_sub_dfn1.join(obs_sub_df4, ["submission_id"], how='left')

    obs_sub_dfn1 = obs_sub_dfn1.select("solution_id","userName","userId","solution_name","submission_id","completedDate","program_id","program_name","board_name","organisation_name","organisation_id","answers",  'qid', 'responseType',  'question', 'labels','completedDate',"state_name","block_name","district_name","cluster_name","school_name",'state_externalId', 'block_externalId', 'district_externalId', 'cluster_externalId', 'school_externalId','gpsLocation')
    obs_sub_df12= obs_sub_df12.withColumn('qid', lit(None).cast(StringType()))
    obs_sub_df12 = obs_sub_df12.withColumn('responseType', lit(None).cast(StringType()))
    obs_sub_df12 = obs_sub_df12.withColumn('question', lit(None).cast(StringType()))
    obs_sub_df12 = obs_sub_df12.withColumn('labels', lit(None).cast(StringType()))
    obs_sub_df12 = obs_sub_df12.withColumn('gpsLocation', lit(None).cast(StringType()))
    obs_sub_df12 = obs_sub_df12.select("solution_id","userName","userId","solution_name","submission_id","completedDate","program_id","program_name","board_name","organisation_name","organisation_id","answers",  'qid', 'responseType',  'question', 'labels','completedDate',"state_name","block_name","district_name","cluster_name","school_name",'state_externalId', 'block_externalId', 'district_externalId', 'cluster_externalId', 'school_externalId','gpsLocation')
    obs_sub_df=obs_sub_dfn1.union(obs_sub_df12)
    output_dfs.append(obs_sub_df)

df_output = functools.reduce(DataFrame.union, output_dfs)
df_output=df_output.select("solution_id","userName","userId","solution_name","submission_id","completedDate","program_id","program_name","board_name",  'qid', 'responseType',  'question', 'labels',"state_name","block_name","district_name","cluster_name","school_name", 'school_externalId','gpsLocation')
# df_output.show()
# # daaaa = df_output.select(df_output["gpsLocation"]).rdd.flatMap(lambda x: x).collect()
# print('daaaa')
row_list1=df_output.select("program_id").distinct().collect()
daata11 = [row.program_id for row in row_list1]
output = []
for x in daata11:
    if x not in output:
        output.append(x)

for i in output:
    df_output = df_output.na.fill(value='ProgramName_notdefined', subset=["program_name"])
    # df_output.select(df_output["program_name"]).distinct().rdd.flatMap(lambda x: x).collect()
    unique_solutions_list = df_output.select(df_output["solution_name"]).distinct().rdd.flatMap(lambda x: x).collect()
    unique_solutions_list1 = df_output.select(df_output["program_name"]).distinct().rdd.flatMap(lambda x: x).collect()


    df_final = [df_output.where(df_output["solution_name"] == x) for x in unique_solutions_list]
    df_final1 = [df_output.where(df_output["program_name"] == x) for x in unique_solutions_list1]
    for j in range(len(df_final1)):
        program_name = '_'.join(j.lower() for j in unique_solutions_list1[j].split())
        for i in range(len(df_final)):
            solution_name = '_'.join(i.lower() for i in unique_solutions_list[i].split())
            df_finall = df_final[i].filter(df_final[i].program_name == unique_solutions_list1[j])
            df_finall.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save(
                "/home/ajay/obs_punjab/" + program_name + "/" + solution_name + "/")
            shutil.make_archive('/home/ajay/punjab_obs_report/' + program_name, 'zip','/home/ajay/obs_punjab/' + program_name)