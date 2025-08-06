from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('Spark SQl Test Python').master('local[*]').getOrCreate()

schema = StructType([
    StructField('brand', StringType(), True),
    StructField('price', IntegerType(), True)
])

df_json = spark.read.format('json').schema(schema).load('file:///home/ubuntu/dev/workspace/hadoop/data/test.json')
df_json.printSchema()
df_json.show()

spark.stop()

