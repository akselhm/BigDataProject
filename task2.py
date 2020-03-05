from pyspark import SparkContext, SparkConf
from datetime import datetime
from operator import add

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

folder_name = "./data/"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
output_file = "result2.tsv"

reviewersRDD = sc.textFile(folder_name + input_reviewers)
reviewers_lines_rdd = reviewersRDD.map(lambda line: line.split('\t'))
reviewers_lines_rdd.cache()

# a) number of distinct users
distinct_users = reviewers_lines_rdd.map(lambda fields: fields[1]).distinct()
#print("users ",distinct_users.count())

# b) average char in user review
#av_chars = reviewers_lines_rdd.map(
#    lambda fields: (len(fields[3]), 1)).reduce(
#    lambda (chars_1, reviews_1), (chars2, reviews_2) : (chars_1 + chars_2, reviews_1 + reviews_2))

# TODO: c)

most_reviews = reviewers_lines_rdd.map(lambda node: (node[2], 1)).reduceByKey(add).sortByValue()
top_ten_reviews = most_reviews.topByKey(10)

# d) reviews per year
per_year = reviewers_lines_rdd.map(lambda year: (datetime.fromtimestamp(year[4]).Year, 1)).reduceByKey(add).sortByKey()
#print("per year ",per_year)

# TODO: e)

# f) PCC

#reviews per user
per_user = reviewers_lines_rdd.map(lambda user: (user[1], 1)).reduceByKey(add).sortByKey()

#tot chars in a users reviews
chars_per_user = reviewers_lines_rdd.map(lambda user: (user[1], len(user[3]))).reduceByKey(add).sortByKey()

###### TODO: find avg chars per review for a user, and compute PCC




#print("dt_object =", dt_object)
#print("type(dt_object) =", type(dt_object)))
"""
lines = [distinct_users.count()]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_file)
"""