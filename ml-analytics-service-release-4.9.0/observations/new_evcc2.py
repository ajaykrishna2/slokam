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
# from datetime import date, time
from configparser import ConfigParser, ExtendedInterpolation
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
    config.get('LOGS', 'observation_streaming_evidence_success')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'observation_streaming_evidence_success'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'observation_streaming_evidence_error')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'observation_streaming_evidence_error'),
    when="w0",
    backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:

    # db production
    clientdev = MongoClient(config.get('MONGO', 'mongo_url'))
    db = clientdev[config.get('MONGO', 'database_name')]
    obsSubCollec = db[config.get('MONGO', 'observation_sub_collection')]
    quesCollec = db[config.get('MONGO', 'questions_collection')]
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def convert(lst):
        return ','.join(lst)
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def evidence_extraction(obSub):
        if 'isAPrivateProgram' in obSub:
            # successLogger.debug("Observation Evidence Submission Id : " + str(msg_id))
            try:
                completedDate = str(obSub['completedDate'])
            except KeyError:
                completedDate = ''
            evidence_sub_count = 0

            try:
                answersArr = [v for v in obSub['answers'].values()]
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
        # for obSub in obsSubCollec.find({'_id': ObjectId(msg_id)}):


        if completedDate:
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

                        try:
                            observationSubQuestionsObj['appName'] = obSub["appInformation"]["appName"].lower()
                        except KeyError:
                            observationSubQuestionsObj['appName'] = config.get("ML_APP_NAME", "survey_app")

                        fileName = []
                        fileSourcePath = []

                        try:
                            observationSubQuestionsObj['remarks'] = answer['remarks']
                        except KeyError:
                            observationSubQuestionsObj['remarks'] = ''
                        observationSubQuestionsObj['questionId'] = str(answer['qid'])

                        # questionsCollec = quesCollec.find({'_id': ObjectId(observationSubQuestionsObj['questionId'])})
                        # for ques in questionsCollec:
                        #     observationSubQuestionsObj['questionExternalId'] = ques['externalId']
                        #     observationSubQuestionsObj['questionName'] = ques["question"][0]
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
                        print(observationSubQuestionsObj)
                        if evidenceCount > 0:
                            print(observationSubQuestionsObj)
                            # json.dump(observationSubQuestionsObj, f)
                            # f.write("\n")
                            # successLogger.debug("Send Obj to Azure")
                except KeyError:
                    pass
except Exception as e:
    errorLogger.error(e, exc_info=True)
for msg_data in obsSubCollec.find({ "_id" : ObjectId('60111acb2d0bbd2f0c3229cb')}):
  # print(msg_data)
  evidence_extraction(msg_data)

# with open('sl_observation_evidence.json', 'w') as f:
#     for msg_data in obsSubCollec.find({"status": "completed"}):
#         obj_arr = evidence_extraction(msg_data['_id'])
#
# blob_service_client = BlockBlobService(
#     account_name=config.get("AZURE", "account_name"),
#     sas_token=config.get("AZURE", "sas_token")
# )
# container_name = config.get("AZURE", "container_name")
# local_path = config.get("OUTPUT_DIR", "observation")
# blob_path = config.get("AZURE", "observation_evidevce_blob_path")
#
# for files in os.listdir(local_path):
#     if "sl_observation_evidence.json" in files:
#         blob_service_client.create_blob_from_path(
#             container_name,
#             os.path.join(blob_path, files),
#             local_path + "/" + files
#         )
#
# payload = {}
# payload = json.loads(config.get("DRUID", "observation_evidence_injestion_spec"))
# datasource = [payload["spec"]["dataSchema"]["dataSource"]]
# ingestion_spec = [json.dumps(payload)]
# for i, j in zip(datasource, ingestion_spec):
#     druid_end_point = config.get("DRUID", "metadata_url") + i
#     druid_batch_end_point = config.get("DRUID", "batch_url")
#     headers = {'Content-Type': 'application/json'}
#     get_timestamp = requests.get(druid_end_point, headers=headers)
#     if get_timestamp.status_code == 200:
#         successLogger.debug("Successfully fetched time stamp of the datasource " + i)
#         timestamp = get_timestamp.json()
#         # calculating interval from druid get api
#         minTime = timestamp["segments"]["minTime"]
#         maxTime = timestamp["segments"]["maxTime"]
#         min1 = datetime.datetime.strptime(minTime, "%Y-%m-%dT%H:%M:%S.%fZ")
#         max1 = datetime.datetime.strptime(maxTime, "%Y-%m-%dT%H:%M:%S.%fZ")
#         new_format = "%Y-%m-%d"
#         min1.strftime(new_format)
#         max1.strftime(new_format)
#         minmonth = "{:02d}".format(min1.month)
#         maxmonth = "{:02d}".format(max1.month)
#         min2 = str(min1.year) + "-" + minmonth + "-" + str(min1.day)
#         max2 = str(max1.year) + "-" + maxmonth + "-" + str(max1.day)
#         interval = min2 + "_" + max2
#         time.sleep(50)
#
#         disable_datasource = requests.delete(druid_end_point, headers=headers)
#
#         if disable_datasource.status_code == 200:
#             successLogger.debug("successfully disabled the datasource " + i)
#             time.sleep(300)
#
#             delete_segments = requests.delete(
#                 druid_end_point + "/intervals/" + interval, headers=headers
#             )
#             if delete_segments.status_code == 200:
#                 successLogger.debug("successfully deleted the segments " + i)
#                 time.sleep(300)
#
#                 enable_datasource = requests.get(druid_end_point, headers=headers)
#                 if enable_datasource.status_code == 204:
#                     successLogger.debug("successfully enabled the datasource " + i)
#
#                     time.sleep(300)
#
#                     start_supervisor = requests.post(
#                         druid_batch_end_point, data=j, headers=headers
#                     )
#                     successLogger.debug("ingest data")
#                     if start_supervisor.status_code == 200:
#                         successLogger.debug(
#                             "started the batch ingestion task sucessfully for the datasource " + i
#                         )
#                         time.sleep(50)
#                     else:
#                         errorLogger.error(
#                             "failed to start batch ingestion task" + str(start_supervisor.status_code)
#                         )
#                 else:
#                     errorLogger.error("failed to enable the datasource " + i)
#             else:
#                 errorLogger.error("failed to delete the segments of the datasource " + i)
#         else:
#             errorLogger.error("failed to disable the datasource " + i)
#
#     elif get_timestamp.status_code == 204:
#         start_supervisor = requests.post(
#             druid_batch_end_point, data=j, headers=headers
#         )
#         if start_supervisor.status_code == 200:
#             successLogger.debug(
#                 "started the batch ingestion task sucessfully for the datasource " + i
#             )
#             time.sleep(50)
#         else:
#             errorLogger.error(start_supervisor.text)
#             errorLogger.error(
#                 "failed to start batch ingestion task" + str(start_supervisor.status_code)
#             )


