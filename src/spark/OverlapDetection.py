from pyspark.sql import SparkSession
from pyspark.sql.functions import array_sort, asc, expr

# Get spark session
spark = SparkSession.builder\
                    .appName("WeeklySnapshot")\
                    .getOrCreate()


# Read csv to df
df = spark.read.format("parquet")\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("./data/shapes.parquet")\
            

# Sort the vertices to make sure that they are in the right order:
# down-left, top-left, down-right, top-right
# This works since Spark attempts to compare the arrays in a lexicographical manner 
# and I have checked the datatype of the array elements are long
df = df.withColumn("vertices", array_sort("vertices"))


# Finding overlapping shapes
df = df.alias("df1").crossJoin(df.alias("df2"))\
    .withColumn("shape_1", expr("CAST(SUBSTRING(df1.shape_id, 7, LENGTH(df1.shape_id)-1) as INT)"))\
    .withColumn("shape_2", expr("CAST(SUBSTRING(df2.shape_id, 7, LENGTH(df2.shape_id)-1) as INT)"))\
    .where("shape_1 < shape_2 AND\
            df1.vertices[1][0] < df2.vertices[2][0] AND\
            df1.vertices[1][1] > df2.vertices[2][1] AND\
            df1.vertices[2][0] > df2.vertices[1][0] AND\
            df1.vertices[2][1] < df2.vertices[1][1]")\
    .orderBy(asc("shape_1"), asc("shape_2"))\
    .select("shape_1", "shape_2")\
    

# Show and write output to file
df.show(15, truncate=False)
df.write.option("header", True).mode("overwrite").csv("output")