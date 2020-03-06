from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext, SparkConf

# Configure Spark
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)
spark = SparkSession.builder.appName("Yelp").config("spark.some.config.option", "some-value").getOrCreate()

# Set data folder, inputs and output
folder_name = "./data/"
input_businesses = "yelp_businesses.csv.gz"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
input_users = "yelp_top_users_friendship_graph.csv.gz"
output_file = "result5.tsv"

# Load into 3 separate dataframes
# First businesses
rdd_businesses = sc.textFile(folder_name + input_businesses).map(lambda l: l.split("\t"))
header_businesses = rdd_businesses.first()
fields_businesses = [StructField(field_name, StringType(), True) for field_name in header_businesses]
schema_businesses = StructType(fields_businesses)
rdd_businesses = rdd_businesses.filter(lambda row: row != header_businesses)
df_businesses = spark.createDataFrame(rdd_businesses, schema=schema_businesses)

# Then reviewers
rdd_reviewers = sc.textFile(folder_name + input_reviewers).map(lambda l: l.split("\t"))
header_reviewers = rdd_reviewers.first()
fields_reviewers = [StructField(field_name, StringType(), True) for field_name in header_reviewers]
schema_reviewers = StructType(fields_reviewers)
rdd_reviewers = rdd_reviewers.filter(lambda row: row != header_reviewers)
df_reviewers = spark.createDataFrame(rdd_reviewers, schema=schema_reviewers)

# Then users
rdd_users = sc.textFile(folder_name + input_users).map(lambda l: l.split("\t"))
header_users = rdd_users.first()
fields_users = [StructField(field_name, StringType(), True) for field_name in header_users]
schema_users = StructType(fields_users)
rdd_users = rdd_users.filter(lambda row: row != header_users)
df_users = spark.createDataFrame(rdd_users, schema=schema_users)
