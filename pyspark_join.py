import pyspark.sql.functions as F
from pyspark.sql import SparkSession

MONG_CONN_STR: str = "mongodb+srv://101512611:nxXrrEuGOgXBTzcZ@cluster0.sbzycsq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
AWS_ACCESS_KEY: str = "REPLACE_WITH_ACCESS_KEY"
AWS_SECRET_KEY: str = "REPLACE_WITH_SECRET_KEY"

spark = SparkSession \
    .builder \
    .appName("tutorial") \
    .config("spark.mongodb.input.uri", MONG_CONN_STR) \
    .config("spark.mongodb.output.uri", MONG_CONN_STR) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config(
        "spark.jars.packages", 
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        "/content/spark-snowflake_2.12-2.12.0-spark_3.4.jar,/content/snowflake-jdbc-3.13.30.jar",
        "/usr/local/spark/jars/mysql-connector-j-8.2.0.jar",
        "spark.hadoop.fs.defaultFS", 
        "hdfs://localhost:9000",
        "org.apache.hadoop:hadoop-aws:3.3.1",
        "com.microsoft.sqlserver:mssql-jdbc:10.2.0.jre8",
    ) \
    .getOrCreate()

# Load Prices data from Mongo
df_ed_prices = spark.read \
  .format("mongo") \
  .option("database", "big_data2") \
  .option("collection", "toronto_housing") \
  .load()

df_ed_prices = df_ed_prices.withColumn('lat_round', F.round(F.col('lat'), 1))
df_ed_prices = df_ed_prices.withColumn('long_round', F.round(F.col('long'), 1))


# Load Postal Code data from S3
S3_PATH = f"s3a://big_data2/postal_codes.csv"

df_postal = spark.read.csv(S3_PATH, header=True, inferSchema=True)
df_postal = df_postal.withColumn('lat_round', F.round(F.col('latitude'), 1))
df_postal = df_postal.withColumn('long_round', F.round(F.col('longitude'), 1))

df_join = df_ed_prices.join(df_postal, on=['lat_round', 'long_round'], how="full_outer")


# Load Restaurant data from MySQL
JDBC_MYSQL_URL = "jdbc:mysql://localhost:3306/TorontoRestaurants"

MYSQL_CONN_PROP = {
    "user": "root",
    "password" : "AzurePassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df_rest = spark.read.jdbc(url=JDBC_MYSQL_URL, table="restaurants", properties=MYSQL_CONN_PROP)

df_join = df_join.join(df_rest, df_join.POSTCODE == df_rest.Postal_Code, how="full_outer")


# Load Crime data from Hadoop
HDFS_PATH = "hdfs://localhost:9000/azureuser/hadoop/Major_Crime_Indicators.csv"

df_crime = spark.read.csv(HDFS_PATH, header=True, inferSchema=True)
df_crime = df_crime.withColumn('lat_round', F.round(F.col('LAT_WGS84'), 1))
df_crime = df_crime.withColumn('long_round', F.round(F.col('LONG_WGS84'), 1))

df_join = df_join.join(df_crime, on=['lat_round', 'long_round'], how="full_outer")


# Load Precipitaton data from Snowflake
sfOptions = {
    "sfURL" : "https://fl06095.canada-central.azure.snowflakecomputing.com",
    "sfAccount" : "fl06095",
    "sfUser" : "ryankimura",
    "sfPassword" : "Abcd1989",
    "sfDatabase" : "PRECIPITATION_DATA",
    "sfSchema" : "PUBLIC",
    "sfWarehouse" : "COMPUTE_WH",
    "sfRole" : "ACCOUNTADMIN",
    "sfDriver" : "net.snowflake.client.jdbc.SnowflakeDriver",
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df_prec = spark.read \
.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbtable", "PRECIPITATION_DATA_TB") \
    .load()

df_prec = df_prec.withColumn('lat_round', F.round(F.col('LATITUDE'), 1))
df_prec = df_prec.withColumn('long_round', F.round(F.col('LONGITUDE'), 1))
df_prec = df_prec.withColumn('day', F.substring(F.col('DATE'), 1, 10))
df_prec = df_prec.groupBy("day", "lat_round", "long_round").agg(F.avg("RAINFALL").alias("rainfall"))

df_join = df_join.join(df_prec, on=['lat_round', 'long_round'], how="full_outer")


# Load Mortgages data from Azure SQL
AZURE_SQL_JDBC_URL = "jdbc:sqlserver://localhost:1433;databaseName=your_azure_sql_database"
AZURE_SQL_CONN_PROP = {
    "user": "root",
    "password": "AzurePassword",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df_mort = spark.read.jdbc(
    url=AZURE_SQL_JDBC_URL, 
    table="cmhc_mortgage", 
    properties=AZURE_SQL_CONN_PROP
)

df_join = df_join.withColumn('month', F.substring('day', 1, 7))
df_join = df_join.join(df_mort, df_join.month == df_mort.REF_DATE, how="full_outer")


# Load Real Estate Price Index data from Azure SQL
df_real = spark.read.jdbc(
    url=AZURE_SQL_JDBC_URL, 
    table="residential_property_prices", 
    properties=AZURE_SQL_CONN_PROP
)

df_join = df_join.join(df_real, df_join.day == df_real.date, how="full_outer")


# Load Traffic Collision data from Azure SQL
df_traffic = spark.read.jdbc(
    url=AZURE_SQL_JDBC_URL, 
    table="traffic_collisions_opendata", 
    properties=AZURE_SQL_CONN_PROP
)

df_traffic = df_traffic.withColumn('lat_round', F.round(F.col('LAT_WGS84'), 1))
df_traffic = df_traffic.withColumn('long_round', F.round(F.col('LONG_WGS84'), 1))

df_join = df_join.join(df_traffic, on=['lat_round', 'long_round'], how="full_outer")


# Load TTC Stops data from Azure SQL
df_df_stop = spark.read.jdbc(
    url=AZURE_SQL_JDBC_URL, 
    table="ttc_stops", 
    properties=AZURE_SQL_CONN_PROP
)

df_stop = spark.sql("SELECT * FROM Lakehouse_BD2.ttc_stops")
df_stop = df_stop.withColumn('lat_round', F.round(F.col('stop_lat'), 1))
df_stop = df_stop.withColumn('long_round', F.round(F.col('stop_lon'), 1))

df_join = df_join.join(df_stop, on=['lat_round', 'long_round'], how="full_outer")


# Output the joined data
output_path = "combined_toronto_data.csv"
df_join.write.option("header", "true").csv(output_path)
