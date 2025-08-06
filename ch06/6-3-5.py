from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


cars = [{'brand': 'Genesis', 'price': 100, 'type': 'Sedan'},
        {'brand': 'BMW', 'price': 90, 'type': 'SUV'},
        {'brand': 'GM', 'price': 70, 'type': 'Sedan'}
]

spark = SparkSession.builder.appName('Spark SQl Test Python').master('local[*]').getOrCreate()

schema = StructType([
    StructField('brand', StringType(), True),
    StructField('price', IntegerType(), True),
    StructField('type', StringType(), True)
])


df = spark.createDataFrame(cars, schema)

df.printSchema()
df.show()
≠
# cache 메서드
df_cache = df.cache()
print(df_cache)

spark.stop()

