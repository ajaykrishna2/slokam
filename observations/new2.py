import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,asc,desc
import pyspark.sql.functions as F
import datetime
# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# read json file
data = spark.read.json('/home/ajay/shikshalokam/output/sl_observation_status.json')
data1 = data.filter((col('district_name').isNotNull()) & (col('state_name').isNotNull()))
data2 = data.filter((col('district_name').isNull()) & (col('state_name').isNull()))
df1=data1.select('*').orderBy(col('state_name'),col('district_name'))
df_final=df1.union(data2)

# display json data
# df_final.show()
# df_final.write.option("header",True).save("/home/ajay/shikshalokam/output/data")
df_final=df_final['app_name','block_externalId','board_name','channel','cluster_externalId','completedDate','district_externalId','ecm_marked_na','entity_externalId','entity_id','entity_name','entity_type','organisation_id','organisation_name','parent_channel','private_program','program_externalId','program_id','program_name','role_title','school_externalId','solution_externalId','solution_id','solution_name','solution_type','state_externalId','status','submission_id','updatedAt','user_id','state_name','district_name','block_name','school_name']
from_date = datetime.datetime(2022, 3, 1, 0, 0, 0, 0)
to_date = datetime.datetime(2022, 5, 2, 0, 0, 0, 0)
df_final=df_final.filter(df_final['updatedAt'] >= from_date).filter(df_final['updatedAt'] < to_date)
print(df_final.count())
df_final.repartition(1).write.format('csv').mode('overwrite').save('/home/ajay/shikshalokam/output' + '/data',header=True)