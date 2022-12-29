import time

import requests
from datetime import datetime, timezone, date, timedelta
# import datetime
import re
import csv
import pandas as pd
from dateutil import parser
from .db_con import *
from .schema import *
import os.path

class fb_pages:
    @classmethod
    def date_range1(cls):
            today = date.today()
            yesterday = today - timedelta(days=1)
            timestamp = time.mktime(datetime.strptime(str(yesterday), "%Y-%m-%d").timetuple())
            ms_timestamp = timestamp
            ms_timestamp1 = int(ms_timestamp) + 86400
            return [int(ms_timestamp), int(ms_timestamp1)]

    @classmethod
    def date_range2(cls):
            today = date.today()
            yesterday = today - timedelta(days=3)
            timestamp = time.mktime(datetime.strptime(str(yesterday), "%Y-%m-%d").timetuple())
            ms_timestamp = timestamp
            ms_timestamp1 = int(ms_timestamp) + (86400 * 3)
            return [int(ms_timestamp), int(ms_timestamp1)]

    @classmethod
    def process(cls):
        INFO = db_con.db_read()
        for j in INFO:
            x = re.findall("Pages", j[1])
            if x:
                cluster_id = j[0]
                cluster_group = j[1]
        INFO1 = db_con.db_creds()
        for i in INFO1:
            if (i['cust_name'] == 'Wizrdom'):
                cust=i['cust_id']
                id=i['page_id']
                params = {
                'metric': 'page_fans',
                'period': 'day',
                 'since': 1648751400,
                'until':  1650479400,
                'access_token':i['page_token']
                 }

        response = requests.get('https://graph.facebook.com/v13.0/102663271454005/insights', params=params)
        dat = []
        dataaa=[]
        for i in response.json()['data']:
            for j in i['values']:
                date_time1 = parser.parse(j['end_time'])
                data = date_time1.strftime('%Y-%m-%d')
                rows = {
                            'id': id,
                            'page_total_followers': j['value'],
                            'date': data
                        }
                dat.append(rows)
                df = pd.DataFrame(dat)
                params1 = {
                    'metric': 'page_fans',
                    'period': 'day',
                    'since': 1648751400,
                    'until':  1650479400,
                    'access_token': ,
                }
                response1 = requests.get('https://graph.facebook.com/v13.0/102663271454005/insights', params=params1)
                dat1=[]
                for l in response1.json()['data']:
                    for m in l['values']:
                        date_time1 = parser.parse(m['end_time'])
                        dataa = date_time1.strftime('%Y-%m-%d')
                        rows1 = {
                            'id': id,
                            'page_total_followers': m['value'],
                            'date': dataa
                        }
                        dat1.append(rows1)
                df1 = pd.DataFrame(dat1)
                df1['page_new_followers'] = df1['page_total_followers'] - df1['page_total_followers'].shift(1)
                dff = pd.merge(df, df1)
                dff['page_new_followers'] = dff['page_new_followers'].fillna(0)
                dff['page_new_followers']=pd.to_numeric(dff['page_new_followers'], errors='coerce').astype(int)
                # dff['page_new_followers'] = dff['page_new_followers'].astype(int)
                print(dff)
                for i in dff['page_new_followers']:
                    new_follow=i
                date_time = parser.parse(j['end_time'])
                data = date_time.strftime('%Y-%m-%d')
                fb_rawdata = FbRawData()
                fb_rawdata.cluster_id = cluster_id
                fb_rawdata.cluster_group = cluster_group
                fb_rawdata.cust_id = cust
                fb_rawdata.page_id =id
                fb_rawdata.page_total_followers=j['value']
                fb_rawdata.page_date=data
                fb_rawdata.page_new_followers =new_follow
                fb_rawdata.created_by = 'sa'
                dataaa.append(fb_rawdata)
            return dataaa




