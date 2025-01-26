from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.\
    appName("activity").\
    master("local[*]").\
    config('spark.executor.memory','6g').\
    config('spark.driver.memory','2g').\
    getOrCreate()

gameplay = spark.read.format('csv').option('header',True).load('activity.csv')
gameplay.createOrReplaceTempView('gameplay_table')

def gameplay_analysis(gameplay):
    
    window = Window.partitionBy(col('player_id')).orderBy(col('event_date')).rowsBetween(Window.unboundedPreceding,Window.currentRow)
    gameplay = gameplay.withColumn('game_played',sum(col('games_played')).over(window).cast("integer"))
    return gameplay
    #gameplay.show()
    input("press Enter to terminate")

if __name__ == "__main__":
    gameplay_analysis(gameplay)
    