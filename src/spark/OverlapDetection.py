from pyspark.sql import SparkSession
from pyspark.sql.functions import array_sort, asc

# Get spark session
spark = SparkSession.builder\
                    .appName("WeeklySnapshot")\
                    .getOrCreate()

# Read csv to df
# And sort the vertices to make sure that they are in the right order:
# down-left, top-left, down-right, top-right
df = spark.read.format("parquet")\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("./data/shapes.parquet")\
            .withColumn("vertices", array_sort("vertices"))

df = df.alias("df1").crossJoin(df.alias("df2"))\
    .where("df1.shape_id != df2.shape_id AND\
            df1.vertices[1][0] < df2.vertices[2][0] AND\
            df1.vertices[1][1] > df2.vertices[2][1] AND\
            df1.vertices[2][0] > df2.vertices[1][0] AND\
            df1.vertices[2][1] < df2.vertices[1][1]")\
    .orderBy(asc("df1.shape_id"), asc("df2.shape_id"))

df.show(15, truncate=False)
df.selectExpr("count(1) as cnt").show()