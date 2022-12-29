import json
import configparser
import logging
import os
import psycopg2
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_serializer import SerializerMixin
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import DATE
configuration_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/config/config.ini"
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
            cursor.execute("select * from dmap_entsm.li_connection")
            data = []
            for i in cursor.fetchall():
                info = {
                'cust_id':i[1],
                'org_id':i[2],
                'org_name':i[3],
                'token':i[4]
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
            cursor.execute("SELECT * FROM dmap_entsm.li_cluster")
            for i in cursor.fetchall():
                global_list.append(i)
            return global_list
        except Exception as e:
            print(e)

    @classmethod
    def db_posts(cls):
        try:
            post_list = []
            connection = psycopg2.connect(user=USER,
                                          password=PASSWORD,
                                          host=HOST,
                                          port=PORT,
                                          database=DATABASE)
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM dmap_entsm.li_posts")
            for i in cursor.fetchall():

                post_list.append(i[1])
            return post_list
        except Exception as e:
            print(e)

    @classmethod
    def db_save(cls,data):
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
