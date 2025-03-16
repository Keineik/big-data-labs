from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import sum, to_date, expr, asc

# Get spark session
spark = SparkSession.builder\
                    .appName("WeeklySnapshot")\
                    .getOrCreate()


# Read csv to df
df = spark.read.format("csv")\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("./data/AmazonSaleReport.csv")

df.createOrReplaceTempView("amz");

# Total quantity (Qty) of items shipped for
# each SKU in the last 7 days, but only report this total every Monday
df.withColumn("Date", to_date("Date", "MM-dd-yy"))\
    .withColumn("total_quantity", sum("Qty").over(Window.partitionBy("SKU")
                                                        .orderBy(expr("DATEDIFF(Date, '0')"))
                                                        .rangeBetween(-6, 0)))\
    .selectExpr("DATE_FORMAT(Date, 'dd/MM/yyyy') AS report_date", "SKU AS sku", "total_quantity")\
    .where("DAYOFWEEK(Date) = 2")\
    .distinct()\
    .orderBy(asc("Date"), asc("SKU"))\
    .show(15)
    # .write.mode("overwrite").csv("output")