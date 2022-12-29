import json
import configparser
import logging
import os
import re

import psycopg2
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_serializer import SerializerMixin
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import DATE
configuration_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))+"/config/config.ini"
config = configparser.ConfigParser()
config.read(configuration_path)
HOST = config['postgresql']['HOST']
PORT = config['postgresql']['PORT']
DATABASE = config['postgresql']['DB']
USER = config['postgresql']['USER']
PASSWORD = config['postgresql']['PASSWORD']


class db_con:
    @classmethod
    def db_creds(cls):
        try:
            connection = psycopg2.connect(user=USER,
                                          password=PASSWORD,
                                          host=HOST,
                                          port=PORT,
                                          database=DATABASE)
            cursor = connection.cursor()
            cursor.execute("select * from dmap_entsm.fb_connection")
            data = []
            for i in cursor.fetchall():
                x = re.findall("Wizrdom", i[2])
                if x:
                    info = {
                        'page_id': i[3],
                        'cust':i[1],
                        'token': i[6]
                    }
                    data.append(info)

            return data
        except Exception as e:
            logging.exception(e)
    @classmethod
    def db_read(cls):
        try:
            global_list = []
            connection = psycopg2.connect(user=USER,
                                          password=PASSWORD,
                                          host=HOST,
                                          port=PORT,
                                          database=DATABASE)
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM dmap_entsm.fb_cluster")
            for i in cursor.fetchall():
                global_list.append(i)
            return global_list
        except Exception as e:
            print(e)


    @classmethod
    def db_save(cls, data):
        try:
            db = create_engine(
                'postgresql+psycopg2://' + USER + ':' + PASSWORD + '@' + HOST + ':' + PORT + '/' + DATABASE)
            base = declarative_base(metadata=MetaData(schema="dmap_entsm"))
            base.metadata.create_all(db)
            Session = sessionmaker(db)
            session = Session()
            session.bulk_save_objects(data)
            session.commit()
        except Exception as e:
            print(e)




