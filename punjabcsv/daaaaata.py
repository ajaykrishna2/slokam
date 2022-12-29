# -----------------------------------------------------------------
# Name : py_observation_evidence_streaming.py
# Author : Shakthieshwari.A
# Description : Extracts the Evidence or Files Attached at each question level
#               during the observation submission
# -----------------------------------------------------------------
from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json
import datetime
# # from datetime import date, time
# from kafka import KafkaConsumer, KafkaProducer
from configparser import ConfigParser, ExtendedInterpolation
import faust
import logging
import logging.handlers
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

  # kafka_url = config.get("KAFKA", "url")
  # app = faust.App(
  #  'ml_observation_evidence_faust',
  #  broker='kafka://'+kafka_url,
  #  value_serializer='raw',
  #  web_port=7002,
  #  broker_max_poll_records=500
  # )
  # rawTopicName = app.topic(config.get("KAFKA", "observation_raw_topic"))
  # producer = KafkaProducer(bootstrap_servers=[kafka_url])

  #db production
  clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
  db = clientProd[config.get('MONGO', 'database_name')]
  obsSubmissionsCollec = db[config.get('MONGO', 'observation_sub_collection')]
  solutionCollec = db[config.get('MONGO', 'solutions_collection')]
  userRolesCollec = db[config.get("MONGO", 'user_roles_collection')]
  programCollec = db[config.get("MONGO", 'programs_collection')]
  entitiesCollec = db[config.get('MONGO', 'entities_collection')]



except Exception as e:
  errorLogger.error(e, exc_info=True)

try :
  def convert(lst):
    return ','.join(lst)
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  def evidence_extraction(obSub):

    if 'isAPrivateProgram' in obSub :
      # successLogger.debug("Observation Evidence Submission Id : " + obSub['_id'])
      try:
        completedDate = obSub['completedDate']
      except KeyError:
        completedDate = ''
      evidence_sub_count = 0

      try:
        answersArr = [ v for v in obSub['answers'].values()]
      except KeyError:
        pass

      for ans in answersArr:
        try:
          if len(ans['fileName']):
            evidence_sub_count = evidence_sub_count + len(ans['fileName'])
        except KeyError:
          if len(ans['instanceFileName']):
            for instance in ans['instanceFileName']:
              evidence_sub_count = evidence_sub_count + len(instance)

    if completedDate :
      for answer in answersArr:
       try:
        if answer['qid']:
          observationSubQuestionsObj = {}
          observationSubQuestionsObj['completedDate'] = completedDate
          observationSubQuestionsObj['total_evidences'] = evidence_sub_count
          observationSubQuestionsObj['observationSubmissionId'] = str(obSub['_id'])
          observationSubQuestionsObj['entity'] = str(obSub['entityId'])
          observationSubQuestionsObj['entityExternalId'] = obSub['entityExternalId']
          observationSubQuestionsObj['entityName'] = obSub['entityInformation']['name']
          observationSubQuestionsObj['entityTypeId'] = str(obSub['entityTypeId'])
          observationSubQuestionsObj['entityType'] = str(obSub['entityType'])
          observationSubQuestionsObj['createdBy'] = obSub['createdBy']
          observationSubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
          observationSubQuestionsObj['solutionId'] = str(obSub['solutionId'])
          observationSubQuestionsObj['observationId'] = str(obSub['observationId'])

          try :
            observationSubQuestionsObj['appName'] = obSub["appInformation"]["appName"].lower()
          except KeyError :
            observationSubQuestionsObj['appName'] = config.get("ML_APP_NAME", "survey_app")

          fileName = []
          fileSourcePath = []

          try:
            observationSubQuestionsObj['remarks'] = answer['remarks']
          except KeyError:
            observationSubQuestionsObj['remarks'] = ''
          observationSubQuestionsObj['questionId'] = str(answer['qid'])

          #-------------------------------------------------------------------------------------------------------------------- code changed here

          # questionsCollec = quesCollec.find({'_id':ObjectId(observationSubQuestionsObj['questionId'])})
          # for ques in questionsCollec:
          #   observationSubQuestionsObj['questionExternalId'] = ques['externalId']
          #   observationSubQuestionsObj['questionName'] = ques["question"][0]

          observationSubQuestionsObj['questionExternalId'] = answer['qid']      # here both "questionId" and  "questionExternalId" same

          observationSubQuestionsObj['questionName'] = answer['payload']['question'][0]


          # questionsArr = [k for k in answer['payload'].values()]
          # observationSubQuestionsObj['questionName'] = str(questionsArr[0])

          #-----------------------------------------------------------------------------------------------------------------------



          observationSubQuestionsObj['questionResponseType'] = answer['responseType']
          evidence = []
          evidenceCount = 0

          try:
            if answer['fileName']:
              evidence = answer['fileName']
              observationSubQuestionsObj['evidence_count'] = len(evidence)
              evidenceCount = len(evidence)
          except KeyError:
            if answer['instanceFileName']:
              for inst in answer['instanceFileName']:
                evidence.extend(inst)
              observationSubQuestionsObj['evidence_count'] = len(evidence)
              evidenceCount = len(evidence)

          for evi in evidence:
            fileName.append(evi['name'])
            fileSourcePath.append(evi['sourcePath'])
          observationSubQuestionsObj['fileName'] = convert(fileName)
          observationSubQuestionsObj['fileSourcePath'] = convert(fileSourcePath)
          if evidenceCount > 0:
              print(observationSubQuestionsObj)
            # producer.send(
            #   (config.get("KAFKA", "observation_evidence_druid_topic")),
            #   json.dumps(observationSubQuestionsObj).encode('utf-8')
            # )
            # producer.flush()
            # successLogger.debug("Send Obj to Kafka")
       except KeyError:
        pass
except Exception as e:
  errorLogger.error(e, exc_info=True)

# this for testing --------------------------------------------------------------------------------------

for msg_data in obsSubCollec.find({ "_id" : ObjectId('60111ac82d0bbd2f0c3229ca')}):
  # print(msg_data)
    evidence_extraction(msg_data)

# after testing uncomment this----------------------------------------------------------------------------------

# try:
#  @app.agent(rawTopicName)
#  async def observationEvidenceFaust(consumer):
#    async for msg in consumer :
#      msg_val = msg.decode('utf-8')
#      msg_data = json.loads(msg_val)
#      successLogger.debug("========== START OF OBSERVATION EVIDENCE SUBMISSION ========")
#      evidence_extraction(msg_data)
#      successLogger.debug("********* END OF OBSERVATION EVIDENCE SUBMISSION ***********")
# except Exception as e:
#  errorLogger.error(e, exc_info=True)
#
# if __name__ == '__main__':
#  app.main()



#----------------------------------------------------------------------------------
# output = {'completedDate': datetime.datetime(2022, 5, 4, 7, 36, 13, 286000), 'total_evidences': 4, 'observationSubmissionId': '627225e509446e00072c6c12', 'entity': '5fd098e2e049735a86b748ad', 'entityExternalId': 'D_AP-D002', 'entityName': 'VIZIANAGARAM', 'entityTypeId': '5f32d8228e0dc8312404056c', 'entityType': 'district', 'createdBy': 'cf19bc3b-8d08-42d3-a32b-8805be018184', 'solutionExternalId': '5541ab5c-67ad-11eb-ac26-a08cfd79f8b7-OBSERVATION-TEMPLATE_CHILD', 'solutionId': '601d41607d4c835cf8b724ad', 'observationId': '62627e00ab4552000717aa84', 'appName': 'diksha', 'remarks': 'wwaaaaaaaaaaaa', 'questionId': '601d41607d4c835cf8b724a1', 'questionExternalId': '601d41607d4c835cf8b724a1', 'questionName': 'What type of device is available at home?', 'questionResponseType': 'multiselect', 'evidence_count': 4, 'fileName': '1651648298672.jpg,1651648315182.jpg,1651648451833.jpg,1651648554965.pdf', 'fileSourcePath': 'survey/627225e509446e00072c6c12/cf19bc3b-8d08-42d3-a32b-8805be018184/9c6d7b60-c47f-48b9-9ce9-d11a21c3819e/1651648298672.jpg,survey/627225e509446e00072c6c12/cf19bc3b-8d08-42d3-a32b-8805be018184/9c6d7b60-c47f-48b9-9ce9-d11a21c3819e/1651648315182.jpg,survey/627225e509446e00072c6c12/cf19bc3b-8d08-42d3-a32b-8805be018184/9c6d7b60-c47f-48b9-9ce9-d11a21c3819e/1651648451833.jpg,survey/627225e509446e00072c6c12/cf19bc3b-8d08-42d3-a32b-8805be018184/9c6d7b60-c47f-48b9-9ce9-d11a21c3819e/1651648554965.pdf'}