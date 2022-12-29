# -----------------------------------------------------------------
# Name : pyspark_project_batch.py
# Author :Shakthiehswari, Ashwini
# Description : Extracts the Status of the Project submissions
#  either Started / In-Progress / Submitted along with the users
#  entity information
# -----------------------------------------------------------------

import json, sys, time
from configparser import ConfigParser, ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
import databricks.koalas as ks
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob import ContentSettings
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import datetime
from datetime import date
from pyspark.sql import DataFrame
from typing import Iterable
from udf_func import *
from pyspark.sql.functions import element_at, split, col

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'project_success')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'project_success'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'project_error')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'project_error'),
    when="w0",
    backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
    def convert_to_row(d: dict) -> Row:
        return Row(**OrderedDict(sorted(d.items())))
except Exception as e:
    errorLogger.error(e, exc_info=True)

spark = SparkSession.builder.appName("projects").config("spark.driver.memory", "25g").getOrCreate()

clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]
entitiesCollec = db[config.get('MONGO', 'entities_collection')]

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
    def melt(df: DataFrame, id_vars: Iterable[str], value_vars: Iterable[str],
             var_name: str = "variable", value_name: str = "value") -> DataFrame:

        _vars_and_vals = array(*(
            struct(lit(c).alias(var_name), col(c).alias(value_name))
            for c in value_vars))

        # Add to the DataFrame and explode
        _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

        cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
        return _tmp.select(*cols)
except Exception as e:
    errorLogger.error(e, exc_info=True)

spark = SparkSession.builder.appName("projects").config(
    "spark.driver.memory", "50g"
).config(
    "spark.executor.memory", "100g"
).config(
    "spark.memory.offHeap.enabled", True
).config(
    "spark.memory.offHeap.size", "32g"
).getOrCreate()

sc = spark.sparkContext

projects_cursorMongo = projectsCollec.aggregate(
    [{
        "$project": {
            "_id": {"$toString": "$_id"},
            "projectTemplateId": {"$toString": "$projectTemplateId"},
            "solutionInformation": {"name": 1, "_id": {"$toString": "$solutionInformation._id"}},
            "title": {
                "$reduce": {
                    "input": {"$split": ["$title", "\n"]},
                    "initialValue": "",
                    "in": {"$concat": ["$$value", " ", "$$this"]}
                }
            },
            "remarks": 1,
            "attachments": 1,
            "programId": {"$toString": "$programId"},
            "programInformation": {"name": 1},
            "metaInformation": {"duration": 1, "goal": 1},
            "syncedAt": 1,
            "updatedAt": 1,
            "isDeleted": 1,
            "categories": 1,
            "tasks": 1,
            "status": 1,
            "userId": 1,
            "description": {
                "$reduce": {
                    "input": {"$split": ["$description", "\n"]},
                    "initialValue": "",
                    "in": {"$concat": ["$$value", " ", "$$this"]}
                }
            },
            "createdAt": 1,
            "programExternalId": 1,
            "isAPrivateProgram": 1,
            "hasAcceptedTAndC": 1,
            "userRoleInformation": 1,
            "userProfile": 1
        }
    }]
)

projects_schema = StructType([
    StructField('_id', StringType(), True),
    StructField('projectTemplateId', StringType(), True),
    StructField(
        'solutionInformation',
        StructType([StructField('name', StringType(), True),
                    StructField('_id', StringType(), True)])
    ),
    StructField('title', StringType(), True),
    StructField('programId', StringType(), True),
    StructField('programExternalId', StringType(), True),
    StructField(
        'programInformation',
        StructType([StructField('name', StringType(), True)])
    ),
    StructField(
        'metaInformation',
        StructType([StructField('duration', StringType(), True),
                    StructField('goal', StringType(), True)
                    ])
    ),
    StructField('updatedAt', TimestampType(), True),
    StructField('syncedAt', TimestampType(), True),
    StructField('isDeleted', BooleanType(), True),
    StructField('status', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('description', StringType(), True),
    StructField('createdAt', TimestampType(), True),
    StructField('isAPrivateProgram', BooleanType(), True),
    StructField('hasAcceptedTAndC', BooleanType(), True),
    StructField(
        'categories',
        ArrayType(
            StructType([StructField('name', StringType(), True)])
        ), True
    ),
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
        'userProfile',
        StructType([
            StructField('rootOrgId', StringType(), True),
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
    ),
    StructField(
        'taskarr',
        ArrayType(
            StructType([
                StructField('tasks', StringType(), True),
                StructField('_id', StringType(), True),
                StructField('sub_task_id', StringType(), True),
                StructField('sub_task', StringType(), True),
                StructField('sub_task_date', TimestampType(), True),
                StructField('sub_task_status', StringType(), True),
                StructField('sub_task_end_date', StringType(), True),
                StructField('sub_task_deleted_flag', BooleanType(), True),
                StructField('task_evidence', StringType(), True),
                StructField('remarks', StringType(), True),
                StructField('assignee', StringType(), True),
                StructField('startDate', StringType(), True),
                StructField('endDate', StringType(), True),
                StructField('syncedAt', TimestampType(), True),
                StructField('status', StringType(), True),
                StructField('task_evidence_status', StringType(), True),
                StructField('deleted_flag', StringType(), True),
                StructField('sub_task_start_date', StringType(), True),
                StructField('prj_remarks', StringType(), True),
                StructField('prj_evidence', StringType(), True),
                StructField('prjEvi_type', StringType(), True),
                StructField('taskEvi_type', StringType(), True)
            ])
        ), True
    ),
    StructField('remarks', StringType(), True),
    StructField('evidence', StringType(), True)
])


func_return = recreate_task_data(projects_cursorMongo)
prj_rdd = spark.sparkContext.parallelize(list(func_return))
projects_df = spark.createDataFrame(prj_rdd,projects_schema)

projects_df = projects_df.withColumn(
    "project_created_type",
    F.when(
        projects_df["projectTemplateId"].isNotNull() == True,
        "project imported from library"
    ).otherwise("user created project")
)

projects_df = projects_df.withColumn(
    "project_title",
    F.when(
        projects_df["solutionInformation"]["name"].isNotNull() == True,
        regexp_replace(projects_df["solutionInformation"]["name"], "\n", " ")
    ).otherwise(regexp_replace(projects_df["title"], "\n", " "))
)


projects_df = projects_df.withColumn(
    "project_deleted_flag",
    F.when(
        (projects_df["isDeleted"].isNotNull() == True) &
        (projects_df["isDeleted"] == True),
        "true"
    ).when(
        (projects_df["isDeleted"].isNotNull() == True) &
        (projects_df["isDeleted"] == False),
        "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn(
    "private_program",
    F.when(
        (projects_df["isAPrivateProgram"].isNotNull() == True) &
        (projects_df["isAPrivateProgram"] == True),
        "true"
    ).when(
        (projects_df["isAPrivateProgram"].isNotNull() == True) &
        (projects_df["isAPrivateProgram"] == False),
        "false"
    ).otherwise("true")
)

projects_df = projects_df.withColumn(
    "project_terms_and_condition",
    F.when(
        (projects_df["hasAcceptedTAndC"].isNotNull() == True) &
        (projects_df["hasAcceptedTAndC"] == True),
        "true"
    ).when(
        (projects_df["hasAcceptedTAndC"].isNotNull() == True) &
        (projects_df["hasAcceptedTAndC"] == False),
        "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn(
    "project_completed_date",
    F.when(
        projects_df["status"] == "submitted",
        projects_df["updatedAt"]
    ).otherwise(None)
)
projects_df = projects_df.withColumn(
    "exploded_categories", F.explode_outer(F.col("categories"))
)

category_df = projects_df.groupby('_id').agg(collect_list('exploded_categories.name').alias("category_name"))
category_df = category_df.withColumn("categories_name", concat_ws(", ", "category_name"))

projects_df = projects_df.join(category_df, "_id", how="left")

projects_df = projects_df.withColumn("parent_channel", F.lit("SHIKSHALOKAM"))

projects_df = projects_df.withColumn(
    "exploded_taskarr", F.explode_outer(projects_df["taskarr"])
)

projects_df = projects_df.withColumn(
    "task_evidence", F.when(
        (projects_df["exploded_taskarr"]["task_evidence"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["taskEvi_type"] != "link"),
        F.concat(
            F.lit(config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')),
            projects_df["exploded_taskarr"]["task_evidence"]
        )
    ).otherwise(projects_df["exploded_taskarr"]["task_evidence"])
)

projects_df = projects_df.withColumn(
    "task_deleted_flag",
    F.when(
        (projects_df["exploded_taskarr"]["deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["deleted_flag"] == True),
        "true"
    ).when(
        (projects_df["exploded_taskarr"]["deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["deleted_flag"] == False),
        "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn(
    "sub_task_deleted_flag",
    F.when((
                   projects_df["exploded_taskarr"]["sub_task_deleted_flag"].isNotNull() == True) &
           (projects_df["exploded_taskarr"]["sub_task_deleted_flag"] == True),
           "true"
           ).when(
        (projects_df["exploded_taskarr"]["sub_task_deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["sub_task_deleted_flag"] == False),
        "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn(
    "project_evidence", F.when(
        (projects_df["exploded_taskarr"]["prj_evidence"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["prjEvi_type"] != "link"),
        F.concat(
            F.lit(config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')),
            projects_df["exploded_taskarr"]["prj_evidence"]
        )
    ).otherwise(projects_df["exploded_taskarr"]["prj_evidence"])
)

projects_df = projects_df.withColumn("project_goal", regexp_replace(F.col("metaInformation.goal"), "\n", " "))
projects_df = projects_df.withColumn("area_of_improvement", regexp_replace(F.col("categories_name"), "\n", " "))
projects_df = projects_df.withColumn("tasks", regexp_replace(F.col("exploded_taskarr.tasks"), "\n", " "))
projects_df = projects_df.withColumn("sub_task", regexp_replace(F.col("exploded_taskarr.sub_task"), "\n", " "))
projects_df = projects_df.withColumn("program_name", regexp_replace(F.col("programInformation.name"), "\n", " "))
projects_df = projects_df.withColumn("task_remarks", regexp_replace(F.col("exploded_taskarr.remarks"), "\n", " "))
projects_df = projects_df.withColumn("project_remarks",
                                     regexp_replace(F.col("exploded_taskarr.prj_remarks"), "\n", " "))

projects_df_cols = projects_df.select(
    projects_df["_id"].alias("project_id"),
    projects_df["project_created_type"],
    projects_df["project_title"],
    projects_df["title"].alias("project_title_editable"),
    projects_df["programId"].alias("program_id"),
    projects_df["programExternalId"].alias("program_externalId"),
    projects_df["program_name"],
    projects_df["metaInformation"]["duration"].alias("project_duration"),
    projects_df["syncedAt"].alias("project_last_sync"),
    projects_df["updatedAt"].alias("project_updated_date"),
    projects_df["project_deleted_flag"],
    projects_df["area_of_improvement"],
    projects_df["status"].alias("status_of_project"),
    projects_df["userId"].alias("createdBy"),
    projects_df["description"].alias("project_description"),
    projects_df["project_goal"], projects_df["project_evidence"],
    projects_df["parent_channel"],
    projects_df["createdAt"].alias("project_created_date"),
    projects_df["exploded_taskarr"]["_id"].alias("task_id"),
    projects_df["tasks"], projects_df["project_remarks"],
    projects_df["exploded_taskarr"]["assignee"].alias("task_assigned_to"),
    projects_df["exploded_taskarr"]["startDate"].alias("task_start_date"),
    projects_df["exploded_taskarr"]["endDate"].alias("task_end_date"),
    projects_df["exploded_taskarr"]["syncedAt"].alias("tasks_date"),
    projects_df["exploded_taskarr"]["status"].alias("tasks_status"),
    projects_df["task_evidence"],
    projects_df["exploded_taskarr"]["task_evidence_status"].alias("task_evidence_status"),
    projects_df["exploded_taskarr"]["sub_task_id"].alias("sub_task_id"),
    projects_df["exploded_taskarr"]["sub_task"].alias("sub_task"),
    projects_df["exploded_taskarr"]["sub_task_status"].alias("sub_task_status"),
    projects_df["exploded_taskarr"]["sub_task_date"].alias("sub_task_date"),
    projects_df["exploded_taskarr"]["sub_task_start_date"].alias("sub_task_start_date"),
    projects_df["exploded_taskarr"]["sub_task_end_date"].alias("sub_task_end_date"),
    projects_df["private_program"],
    projects_df["task_deleted_flag"], projects_df["sub_task_deleted_flag"],
    projects_df["project_terms_and_condition"],
    projects_df["task_remarks"],
    projects_df["project_completed_date"],
    projects_df["solutionInformation"]["_id"].alias("solution_id"),
    projects_df["userRoleInformation"]["role"].alias("designation"),
    projects_df["userProfile"]["rootOrgId"].alias("channel"),
    projects_df["userProfile"]["userLocations"].alias("userProfile"),
    concat_ws(",", F.col("userProfile.framework.board")).alias("board_name"),
    element_at(col('userProfile.organisations.orgName'), -1).alias("organisation_name"),
    element_at(col('userProfile.organisations.organisationId'), -1).alias("organisation_id"),

)


obs_sub_dfn1=projects_df_cols.withColumn(
   "exploded_userProfile",F.explode_outer(F.col('userProfile'))
)


entities_df = melt(obs_sub_dfn1,
        id_vars=["project_id","exploded_userProfile.name","exploded_userProfile.type","exploded_userProfile.id"],
        value_vars=["exploded_userProfile.code"]
    ).select("project_id","name","value","type","id").dropDuplicates()


entities_df = entities_df.withColumn("variable",F.concat(F.col("type"),F.lit("_externalId")))
entities_df = entities_df.withColumn("variable1",F.concat(F.col("type"),F.lit("_name")))
entities_df = entities_df.withColumn("variable2",F.concat(F.col("type"),F.lit("_code")))

entities_df_id=entities_df.groupBy("project_id").pivot("variable").agg(first("id"))

entities_df_name=entities_df.groupBy("project_id").pivot("variable1").agg(first("name"))

entities_df_value=entities_df.groupBy("project_id").pivot("variable2").agg(first("value"))

entities_df_med=entities_df_id.join(entities_df_name,["project_id"],how='outer')
entities_df_res=entities_df_med.join(entities_df_value,["project_id"],how='outer')

entities_df_res=entities_df_res.drop('null')
entities_df.show()

entities_df.unpersist()
entities_df_res.unpersist()
projects_df_cols=projects_df_cols.select(["project_id", "project_created_type", "project_title", "project_title_editable", "program_id", "program_externalId", "program_name", "project_duration", "project_last_sync", "project_updated_date", "project_deleted_flag", "area_of_improvement", "status_of_project", "createdBy", "project_description", "project_goal", "parent_channel", "project_created_date", "task_id", "tasks", "task_assigned_to", "task_start_date", "task_end_date", "tasks_date", "tasks_status", "task_evidence", "task_evidence_status", "sub_task_id", "sub_task", "sub_task_status", "sub_task_date", "sub_task_start_date", "sub_task_end_date", "private_program", "task_deleted_flag", "sub_task_deleted_flag", "project_terms_and_condition", "task_remarks", "project_completed_date", "solution_id", "designation","project_remarks","project_evidence","channel","board_name","organisation_name","organisation_id"])

projects_df_final = projects_df_cols.join(entities_df_res,["project_id"],how='left')



# projects_df_final=projects_df_final.select(["project_id", "project_created_type", "project_title", "project_title_editable", "program_id", "program_externalId", "program_name", "project_duration", "project_last_sync", "project_updated_date", "project_deleted_flag", "area_of_improvement", "status_of_project", "createdBy", "project_description", "project_goal", "parent_channel", "project_created_date", "task_id", "tasks", "task_assigned_to", "task_start_date", "task_end_date", "tasks_date", "tasks_status", "task_evidence", "task_evidence_status", "sub_task_id", "sub_task", "sub_task_status", "sub_task_date", "sub_task_start_date", "sub_task_end_date", "private_program", "task_deleted_flag", "sub_task_deleted_flag", "project_terms_and_condition", "task_remarks", "project_completed_date", "solution_id", "designation","project_remarks","project_evidence","channel","board_name","organisation_name","organisation_id","state_code","block_code","district_code","cluster_code","school_code","state_name","block_name","district_name","cluster_name","school_name","state_externalid", "school_externalid", "cluster_externalid", "block_externalid", "district_externalid"])

projects_df_final.unpersist()

final_projects_df = projects_df_final.dropDuplicates()
projects_df_final.unpersist()



final_projects_df.coalesce(1).write.format("json").mode("overwrite").save(
    config.get("OUTPUT_DIR", "project") + "/"
)

for filename in os.listdir(config.get("OUTPUT_DIR", "project") + "/"):
    if filename.endswith(".json"):
        os.rename(
            config.get("OUTPUT_DIR", "project") + "/" + filename,
            config.get("OUTPUT_DIR", "project") + "/sl_projects.json"
        )

local_path = config.get("OUTPUT_DIR", "project")
