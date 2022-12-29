from sqlalchemy import Column, String, Integer
from sqlalchemy.dialects.postgresql import DATE
import configparser
import os
import psycopg2
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
configuration_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+"/config/config.ini"
print(configuration_path)
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
class PostIds(base):
    __tablename__ = "li_posts"
    id = Column(Integer, autoincrement=True, primary_key=True)
    post_id = Column(String(30))
    date_coulmn = Column(DATE())
    time_column = Column(String(30))
    post_name = Column(String(30))
    created_by = Column(String(20))
class LiRawData(base):
    __tablename__ = "li_rawdata"
    id = Column(Integer, autoincrement=True, primary_key=True)
    cluster_id = Column(Integer)
    cluster_group = Column(String(20))
    cust_id = Column(Integer)
    org_id = Column(Integer)
    post_id = Column(Integer)
    campaign_id=Column(Integer)
    page_id=Column(Integer)
    m1 = Column(Integer)
    m2 = Column(Integer)
    m3 = Column(Integer)
    m4 = Column(Integer)
    m5 = Column(Integer)
    m6 = Column(Integer)
    m7 = Column(Integer)
    m8 = Column(Integer)
    m9 = Column(Integer)
    post_ctr=Column(Integer)
    post_start_date = Column(DATE())
    post_end_date = Column(DATE())
    campaign_ctr = Column(Integer)
    campaign_avg_CPC=Column(Integer)
    campaign_year = Column(Integer)
    campaign_month=Column(Integer)
    campaign_day=Column(Integer)
    page_new_follwers=Column(Integer)
    page_total_followers=Column(Integer)
    page_start_date=Column(DATE())
    page_end_date = Column(DATE())
    created_by=Column(String(20))
