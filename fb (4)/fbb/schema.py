from sqlalchemy import Column, String, Integer
from sqlalchemy.dialects.postgresql import DATE
import configparser
import os
import psycopg2
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
configuration_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))+"/config/config.ini"

config = configparser.ConfigParser()
config.read(configuration_path)
HOST = config['postgresql']['HOST']
PORT = config['postgresql']['PORT']
DATABASE = config['postgresql']['DB']
USER = config['postgresql']['USER']
PASSWORD = config['postgresql']['PASSWORD']
db = create_engine('postgresql+psycopg2://' + USER + ':' + PASSWORD + '@' + HOST + ':' + PORT + '/' + DATABASE)
base = declarative_base(metadata=MetaData(schema="dmap_entsm"))
base.metadata.create_all(db)
class FbRawData(base):
    __tablename__ = "fb_rawdata"
    id = Column(Integer, autoincrement=True, primary_key=True)
    cluster_id = Column(Integer)
    cluster_group = Column(String(20))
    cust_id = Column(Integer)
    page_id=Column(Integer)
    page_new_followers=Column(Integer)
    page_total_followers=Column(Integer)
    page_date = Column(DATE())
    created_by=Column(String(20))