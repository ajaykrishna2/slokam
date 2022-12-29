import csv
import os
import requests
import json
import datetime
from datetime import date
from datetime import timedelta
from datetime import datetime
import time
#page-id=organizationalEntity
url = 'https://api.linkedin.com/v2/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:14506952'
my_headers = {'Authorization': 'Bearer AQWllLIb_g067YlKVswhCJOp-QdfWV1cARuklQ-aYBMzCrqwKV-9C8o6hxXH2FhhSSAIiw9-8DSzpaIWGMpABCSF43kJyTcu9mHuQqkMCQgLKFyQNFTys0APns4YxvaHc2_K-GtWwV1JYXsMYp8tI1yI4WRphZUaeTJj28SuJg0mcK87FZzG-lESKizKuhp4rFlIRZx8MlbwaqgNOO0XmU5jHdUgJErbc2AXQ0hKZV0hFHnJ83pN84t93d7UuGWEoV1yilbnfHSrwFYvS8tDW13WiG9jT8xkyqmuox4QnjV4y_xzHmirBTLpZD9X6ulJd1XLwdnt1ZlkwjDX2qxUiI3iwszeNg'}
results = requests.get(url, headers=my_headers)
results = results.json()
info = []
follower_count = 0
for j in results['elements'][0]['followerCountsByAssociationType']:
                total_followers = j['followerCounts']['organicFollowerCount']+j['followerCounts']['paidFollowerCount']
                follower_count = follower_count+total_followers

url = 'https://api.linkedin.com/v2/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn%3Ali%3Aorganization%3A14506952&timeIntervals=(timeRange:(start:1640975400000,end:1647801000000),timeGranularityType:DAY)'
my_headers = {'X-Restli-Protocol-Version': '2.0.0',
'Authorization': 'Bearer AQWllLIb_g067YlKVswhCJOp-QdfWV1cARuklQ-aYBMzCrqwKV-9C8o6hxXH2FhhSSAIiw9-8DSzpaIWGMpABCSF43kJyTcu9mHuQqkMCQgLKFyQNFTys0APns4YxvaHc2_K-GtWwV1JYXsMYp8tI1yI4WRphZUaeTJj28SuJg0mcK87FZzG-lESKizKuhp4rFlIRZx8MlbwaqgNOO0XmU5jHdUgJErbc2AXQ0hKZV0hFHnJ83pN84t93d7UuGWEoV1yilbnfHSrwFYvS8tDW13WiG9jT8xkyqmuox4QnjV4y_xzHmirBTLpZD9X6ulJd1XLwdnt1ZlkwjDX2qxUiI3iwszeNg'}
results = requests.get(url, headers=my_headers)
results = results.json()
path = os.path.abspath(f'/home/sharmashbee/PycharmProjects/Dmap_project/page_statistics.csv')
with open(path, 'w', encoding='UTF8', newline='') as f:
    fieldnames = ['page_id','new_follwers','start_date','end_date','followers']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    f.close()
with open(path, 'a+', encoding='UTF8', newline='') as f1:
    fieldnames = ['page_id','new_follwers','start_date','end_date','followers']
    writer = csv.DictWriter(f1, fieldnames=fieldnames)
    info = []
    for i in results['elements']:
        start = i['timeRange']['start']/1000.0
        start_obj = datetime.fromtimestamp(int(start)).strftime('%Y-%m-%d')
        end = i['timeRange']['end']/1000.0
        end_obj = datetime.fromtimestamp(int(end)).strftime('%Y-%m-%d')

        new_followers = i['followerGains']['organicFollowerGain']+i['followerGains']['paidFollowerGain']
        rows={
            'page_id':i['organizationalEntity'][20:],
              'new_follwers':new_followers,
              'start_date':start_obj,
              'end_date':end_obj,
              'followers':follower_count
        }
        info.append(rows)
    writer.writerows(info)
    f.close()


# today = date.today()
# yesterday = today - timedelta(days=1)
# timestamp = time.mktime(datetime.strptime(str(yesterday),
#                                              "%Y-%m-%d").timetuple())
# ms_timestamp = timestamp*1000
