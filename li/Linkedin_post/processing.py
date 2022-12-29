from .schema import *
from .db_connection import *
import csv
import collections
import requests
import json
# import datetime
from datetime import datetime
from pytz import timezone
import pytz
class Li_process:
    db_con = db_con()
    @classmethod
    def date_range(cls,date):
        format = "%Y-%m-%d %H:%M:%S,%f"
        start_date1 = date.astimezone(timezone('Asia/Kolkata'))
        date1 = start_date1.strftime(format)
        date11 = date1[:22]
        dt_obj = datetime.strptime(date11, '%Y-%m-%d %H:%M:%S,%f')
        millisec = dt_obj.timestamp() * 1000
        return int(millisec)

    @classmethod
    def sample(cls, lists):
        lists1 = []
        for i in lists:
            if i != None:
                lists1.append(i)
        return lists1

    @classmethod
    def post_id(cls):
        INFO = cls.db_con.db_creds()
        for i in INFO:
            url = 'https://api.linkedin.com/v2/shares?q=owners&owners=urn%3Ali%3Aorganization%3A{}'.format(
                i['org_id']) + '&sortBy=LAST_MODIFIED&sharesPerOwner=1000&count=50'
            my_headers = {
                'Authorization': 'Bearer {}'.format(i['token'])}
            results = requests.get(url, headers=my_headers)
            results = results.json()
            data = []
            l1 = cls.db_con.db_posts()
            l1.sort()
            for j1 in results['elements']:
                    dd = int(j1['id'])
                    if dd not in l1:
                                li_posts = PostIds()
                                obj = j1['created']['time'] / 1000.0
                                timeobj = datetime.fromtimestamp(int(obj)).strftime('%H:%M:%S')
                                dateobj = datetime.fromtimestamp(int(obj)).strftime('%Y-%m-%d')
                                li_posts.post_id= dd
                                li_posts.date_coulmn=dateobj
                                li_posts.time_column=timeobj
                                li_posts.created_by='sa'
                                data.append(li_posts)
            return data

    @classmethod
    def post_metric_count(cls, data):
        postid = []
        # getting ist timestamp in ms
        start = cls.date_range(datetime(2022, 3, 1))
        end = cls.date_range(datetime(2022, 3, 31))
        for id in range(0, (len(data))):
            string = "shares[{}]".format(id) + "=urn:li:share:{}".format(data[id]) + "&"
            postid.append(string)
        string1 = ""
        for item in postid:
            string1 += item
        INFO = cls.db_con.db_creds()
        for k in INFO:
            url = 'https://api.linkedin.com/v2/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:{}'.format(k['org_id'])+'&{}'.format(
            string1) + 'timeIntervals.timeGranularityType=DAY&timeIntervals.timeRange.start={}'.format(
            start) + '&timeIntervals.timeRange.end={}'.format(end)
            headers = {
            'Authorization': 'Bearer {}'.format(k['token'])}
            results = requests.get(url, headers=headers)
            results = results.json()
            cluster = cls.db_con.db_read()
            for i in cluster:
                    info11 = []
                    met_list = [i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10]]
                    m_list = cls.sample(met_list)
                    for j in results['elements']:
                        li_rawdata = LiRawData()
                        li_rawdata.cluster_id=i[0]
                        li_rawdata.cluster_group=i[1]
                        li_rawdata.cust_id=k['cust_id']
                        li_rawdata.post_id = j['share'][13:]
                        li_rawdata.org_id = j['organizationalEntity'][20:]  # need to rename org_id as page_id
                        for k1 in range(1, (len(m_list) + 1)):
                            value = j['totalShareStatistics'][m_list[k1 - 1]]
                            exec(f'li_rawdata.m{k1} = value')
                        li_rawdata.created_by = 'sa'
                        obj = j['timeRange']['start'] / 1000.0
                        dateobj = datetime.fromtimestamp(int(obj)).strftime('%Y-%m-%d')
                        li_rawdata.post_start_date = dateobj
                        obj1 = j['timeRange']['end'] / 1000.0
                        dateobj1 = datetime.fromtimestamp(int(obj1)).strftime('%Y-%m-%d')
                        li_rawdata.post_end_date = dateobj1
                        if j['totalShareStatistics']['clickCount'] != 0:
                            li_rawdata.post_ctr = j['totalShareStatistics']['clickCount'] / j['totalShareStatistics'][
                                'impressionCount']
                        info11.append(li_rawdata)
                    return info11


