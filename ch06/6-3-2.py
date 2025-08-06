from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

rows = [{'name': 'joseph', 'age':35}, {'name':'jina', 'age':30}, {'name':'julian', 'age':15}]

schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True)
])

spark = SparkSession.builder.appName('Spark SQl Test Python').master('local[*]').getOrCreate()

df = spark.createDataFrame(rows, schema)

df.printSchema()
df.show()

spark.stop()

