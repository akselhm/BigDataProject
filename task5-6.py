import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext, SparkConf

# To avoid error with ascii-encoding
reload(sys)
sys.setdefaultencoding('utf-8')

# Configure Spark
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)
spark = SparkSession.builder.appName("Yelp").config("spark.some.config.option", "some-value").getOrCreate()

# Set data folder, inputs and output
folder_name = "./data/"
input_businesses = "yelp_businesses.csv.gz"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
input_users = "yelp_top_users_friendship_graph.csv.gz"
output_file = "result6.tsv"


# Task 5: Load into 3 separate dataframes
# ===============================================
def load_dataframe(input_file, delimiter):
    """
    Reads in a file with a given delimiter and returns a dataframe

    :param input_file: File to read
    :param delimiter: Delimiter separating values
    :return: Dataframe created from file
    """
    rdd = sc.textFile(folder_name + input_file).map(lambda l: l.split(delimiter))  # First load RDD
    header = rdd.first()                            # Get header for dataframe
    fields = [StructField(field_name, StringType(), True) for field_name in header]
    schema = StructType(fields)                     # Make schema
    rdd = rdd.filter(lambda row: row != header)     # Filter out header to avoid duplicates
    df = spark.createDataFrame(rdd, schema=schema)  # Create dataframe with schema
    return df


df_businesses = load_dataframe(input_businesses, "\t")  # Load businesses into dataframe
df_reviewers = load_dataframe(input_reviewers, "\t")    # Load reviewers into dataframe
df_users = load_dataframe(input_users, ",")             # Load users into dataframe


# Task 6:
# a) Inner join review table and business table
# ===============================================
inner_join = df_reviewers.join(df_businesses, df_reviewers[2] == df_businesses[0], 'inner')

# b) Save the new table in a temporary table
# ===============================================
inner_join.createOrReplaceTempView("innerJoinBusinessesAndReviewers")

# c) Number of reviews for each user in the review table
# =======================================================
reviews_per_user = df_reviewers.groupBy(df_reviewers[1]).count()
reviews_per_user = reviews_per_user.orderBy(reviews_per_user[1].desc())
reviews_per_user = reviews_per_user.limit(20).collect()

lines = [reviews_per_user]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_file)
