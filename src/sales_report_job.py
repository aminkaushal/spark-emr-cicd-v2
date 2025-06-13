from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SalesReportJob").getOrCreate()

data = [("North", 1000), ("South", 1500), ("East", 1300), ("West", 900)]
df = spark.createDataFrame(data, ["region", "sales"])
df.groupBy("region").sum("sales").show()

spark.stop()
