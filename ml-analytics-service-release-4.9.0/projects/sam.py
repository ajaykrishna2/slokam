from datetime import datetime, timedelta
import csv
import requests
import json
current_dt = datetime.now()
past_week_dt = current_dt.today() - timedelta(days=60)
interval = str(past_week_dt.date()) + "T06:30:00.00/" + str(current_dt.date()) + "T06:30:00.00"
data=[]
def sam(lsts, key):
  return [x.get(key) for x in lsts]
with open('/home/ajay/Downloads/data.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    # line_count = 0
    for row in csv_reader:
        print(type(row.values()))
        data.append(row)
    print(sam(data,'program_id'))
# pr_id = ["600ab53cc7de076e6f993724", "601429016a1ef53356b1d714"]
# for i in pr_id:
#   url = 'http://localhost:8888/druid/v2'
#   body ={"queryType": "scan", "dataSource": "druid_data_sam", "resultFormat": "list",
#             "columns": ["program_name","createdBy"],
#             "intervals": ['2009-12-31T00:00:00.000Z/2010-01-02T00:00:00.000Z'],"granularity":"day",
#             "filter":{
#                 "type": "and",
#                 "fields": [
#                     {
#                         "type": "selector",
#                         "dimension": "program_id",
#                         "value": f"{i}"}
#                 ]}
#          }
# print(body)
#   response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(body))
#   print(response.status_code)
#   print(response.text)
# progarm_id = ["60ed4d5c294d1726ee39085f", "60ed4d5c294d1726ee39085f", "5f1c5be0499e7a357a6c2786"]
# test run
# current_dt = datetime.now() - timedelta(days=1)
# past_week_dt = current_dt.today() - timedelta(days=26)
# interval = str(past_week_dt.date()) + "T06:30:00.00/" + str(current_dt.date()) + "T06:30:00.00"
# pr_id = ["601429016a1ef53356b1d714", "602d11f0473c59287ad538e1","605083ba09b7bd61555580fb","60549338acf1c71f0b2409c3","607d320de9cce45e22ce90c0","61120cda9174bc4e6ee05f05","6124984e5435a115cac9f120","61558efa27e9cd00077530b5","6172a6e58cf7b10007eefd21","61dd77a459e49a00074aecae","62034f90841a270008e82e46","623ea13a372eae00075fe8b8","625575f4aac5a900076825e5"]
# for i in pr_id:

url = 'http://localhost:8888/druid/v2'
body = {"queryType": "scan", "dataSource": "p_data", "resultFormat": "list",
            "columns":["user_id", "userName", "districtName", "blockName", "schoolName", "questionName", "questionExternalId", "questionResponseLabel", "evidences", "observationSubmissionId", "completedDate", "solutionName", "schoolExternalId","location_validated_with_geotag"],
            "intervals": [interval], "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "isAPrivateProgram",
                    "value": "false"
                }]}}

response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(body))
# print(response)
print(response)
data_list = response.json()
# print(data_list)
