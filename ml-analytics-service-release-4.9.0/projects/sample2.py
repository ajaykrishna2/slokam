from pymongo import MongoClient
from bson.objectid import ObjectId
import csv,os
import json
import boto3
import datetime
from datetime import date,time
import requests
import argparse
from kafka import KafkaConsumer
from kafka import KafkaProducer
from configparser import ConfigParser,ExtendedInterpolation
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
 #db production
 clientdev = MongoClient(config.get('MONGO','mongo_url'))
 dbdev = clientdev[config.get('MONGO','database_name')]

 observationSubmissionsDevCollec = dbdev[config.get('MONGO','observation_sub_collec')]
 solutionsDevCollec = dbdev[config.get('MONGO','solutions_collec')]
 observationDevCollec = dbdev[config.get('MONGO','observations_collec')]
 entityTypeDevCollec = dbdev[config.get('MONGO','entity_type_collec')]
 questionsDevCollec = dbdev[config.get('MONGO','questions_collec')]
 criteriaDevCollec = dbdev[config.get('MONGO','criteria_collec')]
 entitiesDevCollec = dbdev[config.get('MONGO','entities_collec')]
 programsDevCollec = dbdev[config.get('MONGO','programs_collec')]
except Exception as e:
  errorLogger.error(e,exc_info=True)

