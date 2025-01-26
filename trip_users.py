from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('trip_users').getOrCreate()

users = spark.read.format('csv').option('header',True).load('Users.csv')
trips = spark.read.format('parquet').load('Trips.parquet')

#Finding the unbanned users
users = users.filter(col('banned')!= 'Yes').select(col('users_id'))

trips = trips.join(broadcast(users),users.users_id == trips.client_id,how='inner').\
    select('client_id','driver_id','status','request_at'
           )
final_df = trips.join(broadcast(users),users.users_id == trips.driver_id,how='inner').\
    select('client_id','driver_id','status','request_at'
           )
final_df.show()
total_df = final_df.\
    groupBy(col('request_at')).agg(count(col('status')).alias('completed_count'))

cancelled_df = final_df.filter(col('status').like('%cancelled%')).\
    groupBy(col('request_at')).agg(count(col('status')).alias('cancelled_count'))

final_df = total_df.join(broadcast(cancelled_df),total_df.request_at == cancelled_df.request_at,how='left').\
    select(total_df.request_at,col('completed_count'),col('cancelled_count'))

final_df = final_df.fillna(value=0,subset=['cancelled_count'])
final_df.select(col('request_at'),round(col('cancelled_count')/col('completed_count'),2).alias('cancellation_rate')).show()
