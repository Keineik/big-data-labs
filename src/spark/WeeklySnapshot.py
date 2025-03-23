from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Get spark session
spark = SparkSession.builder\
                    .appName("WeeklySnapshot")\
                    .getOrCreate()


# Read csv to df
df = spark.read.format("csv")\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("./data/AmazonSaleReport.csv")


# Convert from String to DateType
df = df.withColumn("Date", to_date("Date", "MM-dd-yy"))


# Get DataFrame of report dates
report_conf = df.selectExpr(
                    "MIN(Date) AS range_begin",
                    "DATEDIFF(DATEADD(MAX(Date), 6), MIN(Date)) AS range_size"
                ).collect()

range_begin = report_conf[0]["range_begin"]
range_size = report_conf[0]["range_size"]

report_dates_df = spark.range(0, range_size)\
                       .withColumn("report_date", dateadd(lit(range_begin), col("id").cast("int")))\
                       .where(dayofweek("report_date") == 2)


# Total quantity (Qty) of items shipped for
# each SKU in the last 7 days, but only report this total every Monday
report_df = report_dates_df\
    .join(df, how="left", on=expr("DATEDIFF(report_date, Date) BETWEEN 0 AND 6"))\
    .groupBy("report_date", "SKU")\
    .agg(sum("Qty").alias("total_quantity"))\
    .orderBy(asc("report_date"), asc("sku"))\
    .selectExpr("DATE_FORMAT(report_date, 'dd/MM/yyyy') AS report_date", "sku", "total_quantity")


# Show and write output to file
report_df.show(15, truncate=False)
report_df.write.option("header", True).mode("overwrite").csv("output")