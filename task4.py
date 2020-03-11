from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np
import unicodedata

# Configure Spark
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)

# Set data folder, inputs and output
folder_name = "./data/"
input_users = "yelp_top_users_friendship_graph.csv.gz"
output_file = "result4.tsv"

# Create RDD object from .gz-file
rdd = sc.textFile(folder_name + input_users)
rdd = rdd.map(lambda line: line.split(','))

# Filter out header
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)

# a)
# ==============================================
followers = rdd.map(lambda node: (node[1], 1)).reduceByKey(add)
top_ten_in = followers.takeOrdered(10, lambda x: -x[1])

following = rdd.map(lambda node: (node[0], 1)).reduceByKey(add)
top_ten_out = following.takeOrdered(10, lambda x: -x[1])

# b)
# ==============================================
# Mean
followers_average = float(rdd.count()) / followers.count() #tot conections/users with connections "in"
following_average = float(rdd.count()) / following.count()

# Median
followers_values = followers.map(lambda v: (v[1]))              # Map the column we're using for the median
followers_array = np.array(followers_values.collect())          # Convert to numpy array
followers_median = np.median(followers_array.astype(np.float))  # Find median

following_values = following.map(lambda v: (v[1]))              # Map the column we're using for the median
following_array = np.array(following_values.collect())          # Convert to numpy array
following_median = np.median(following_array.astype(np.float))  # Find median

lines = [top_ten_in, top_ten_out, followers_average, following_average, followers_median, following_median]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_file)
