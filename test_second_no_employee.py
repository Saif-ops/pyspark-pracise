import pytest
import sys
from pyspark.sql import SparkSession
from second_no_employee import second_highest_salary


@pytest.fixture(scope='module')
def spark_create():
    return SparkSession.builder.appName('unit').getOrCreate()
def test_second_highest_salary(spark_create):
    data = [
        ("1", 100),
        ("2", 200),
        ("3", 300),
        ("4", 400),
    ]
    columns = ["id", "salary"]
    employee_df = spark_create.createDataFrame(data, schema=columns)
    result_df = second_highest_salary(employee_df)
    result = result_df.collect()

    assert len(result) == 1
    assert result[0]['id'] == 3
