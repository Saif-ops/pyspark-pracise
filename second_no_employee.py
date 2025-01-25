from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName('second').getOrCreate()
employee_df = spark.read.format('csv').option('header',True).load('employee_data.csv')

def second_highest_salary(employee_df):
    window = Window.orderBy(desc(col('salary')))
    employee_df = employee_df.withColumn('row_number',row_number().over(window))
    result_df = employee_df.filter(col('row_number') == 2).select('id')
    result_df.show()

if __name__ == '__main__':
    second_highest_salary(employee_df)

