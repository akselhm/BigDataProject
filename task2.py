from pyspark import SparkContext, SparkConf
import base64
import datetime
from operator import add

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)

folder_name = "./data/"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
output_file = "result2.tsv"

# Create RDD object from .gz-file
reviewersRDD = sc.textFile(folder_name + input_reviewers)
reviewers_lines_rdd = reviewersRDD.map(lambda line: line.split('\t'))

# Filter out header
header = reviewers_lines_rdd.first()
reviewers_lines_rdd = reviewers_lines_rdd.filter(lambda line: line != header)

# a) Number of distinct users
# ==============================================
distinct_users = reviewers_lines_rdd.map(lambda fields: fields[1]).distinct()
distinct_users_count = distinct_users.count()

# b) Average char in user review
# ==============================================
characters_and_reviews_count = reviewers_lines_rdd.map(lambda fields: (len(base64.b64decode(fields[3])), 1)).reduce(
    lambda (chars_1, reviews_1), (chars_2, reviews_2): (chars_1 + chars_2, reviews_1 + reviews_2))
average_characters_per_review = characters_and_reviews_count[0] / characters_and_reviews_count[1]

# c) Businesses with the most number of reviews
# ==============================================
most_reviews = reviewers_lines_rdd.map(lambda node: (node[2], 1)).reduceByKey(add)
top_ten_reviews = most_reviews.takeOrdered(10, key=lambda x: -x[1])


# d) reviews per year
# ==============================================
per_year = reviewers_lines_rdd.map(lambda year: (datetime.datetime.fromtimestamp(float(year[4])).year, 1)).reduceByKey(add).sortByKey()

"""
# TODO: e)
# ==============================================

# f) PCC
# ==============================================

# Reviews per user
per_user = reviewers_lines_rdd.map(lambda user: (user[1], 1)).reduceByKey(add).sortByKey()

# Tot chars in a users reviews
chars_per_user = reviewers_lines_rdd.map(lambda user: (user[1], len(user[3]))).reduceByKey(add).sortByKey()

###### TODO: find avg chars per review for a user, and compute PCC


# print("dt_object =", dt_object)
# print("type(dt_object) =", type(dt_object)))
lines = [distinct_users.count()]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_file)
"""
