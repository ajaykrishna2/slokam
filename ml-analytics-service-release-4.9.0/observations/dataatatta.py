# -----------------------------------------------------------------
# Name : sl_py_observation_streaming.py
# Author : Ashwini.E , Shakthieshwari.A
# Description : Program to read data from one kafka topic and
#   produce it to another kafka topic
# -----------------------------------------------------------------

import faust
from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json
import datetime
import requests
# from kafka import KafkaConsumer, KafkaProducer
from configparser import ConfigParser, ExtendedInterpolation
import logging
import logging.handlers
import time
from logging.handlers import TimedRotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'observation_streaming_success')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'observation_streaming_success'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'observation_streaming_error')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'observation_streaming_error'),
    when="w0",
    backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

domArr = []

client = MongoClient(config.get('MONGO', 'mongo_url'))
db = client[config.get('MONGO', 'database_name')]
solCollec = db[config.get('MONGO', 'solutions_collection')]
obsCollec = db[config.get('MONGO', 'observations_collection')]
questionsCollec = db[config.get('MONGO', 'questions_collection')]
entitiesCollec = db[config.get('MONGO', 'entities_collection')]
criteriaQuestionsCollec = db[config.get('MONGO', 'criteria_questions_collection')]
criteriaCollec = db[config.get('MONGO', 'criteria_collection')]
programsCollec = db[config.get('MONGO', 'programs_collection')]
prid="600ab53cc7de076e6f993724"
num=0
print(obsCollec.find({"programId": ObjectId(prid)}))
for obs in obsCollec.find({"programId": ObjectId(prid)}):

#     num=num+1
# print(num)



