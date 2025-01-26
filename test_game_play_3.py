import pytest
from pyspark.sql import SparkSession
from game_play_analysis_3 import gameplay_analysis

@pytest.fixture
def spark_creation():
    spark = SparkSession.builder.appName('test').\
        master("local[*]").\
        config('spark.executor.memory','6g').\
        getOrCreate()


def test_game_play(spark):
    data = [[1, 2, '2016-03-01', 5],
        [1, 2, '2016-05-02', 6],
        [1, 3, '2017-06-25', 1]]
    columns=['player_id', 'device_id', 'event_date', 'games_played']
    df = spark.createDataFrame(data,columns)
    result_df = gameplay_analysis(df)
    lis = result_df.collect()
    assert len(lis)>0
    assert lis[0]['games_played'] == 5

    
