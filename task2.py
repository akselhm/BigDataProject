from pyspark import SparkContext, SparkConf
import base64
import datetime
from operator import add
import numpy as np

# Configure Spark
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)

# Set data folder, inputs and output
folder_name = "./data/"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
output_file = "result2.tsv"

# Create RDD object from .gz-file
reviewersRDD = sc.textFile(folder_name + input_reviewers)
rdd = reviewersRDD.map(lambda line: line.split('\t'))

# Filter out header
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)

# a) Number of distinct users
# ==============================================
distinct_users = rdd.map(lambda fields: fields[1]).distinct()
distinct_users_count = distinct_users.count()

# b) Average char in user review
# ==============================================
characters_and_reviews_count = rdd.map(lambda fields: (len(base64.b64decode(fields[3])), 1)).reduce(
    lambda (chars_1, reviews_1), (chars_2, reviews_2): (chars_1 + chars_2, reviews_1 + reviews_2))
average_characters_per_review = characters_and_reviews_count[0] / characters_and_reviews_count[1]

print average_characters_per_review
# c) Businesses with the most number of reviews
# ==============================================
most_reviews = rdd.map(lambda node: (node[2], 1)).reduceByKey(add)
top_ten_reviews = most_reviews.takeOrdered(10, key=lambda x: -x[1])

# d) reviews per year
# ==============================================
per_year = rdd.map(lambda year: (datetime.datetime.fromtimestamp(float(year[4])).year, 1)).reduceByKey(add).sortByKey()

#  e)
# ==============================================
first_date = rdd.map(lambda time: time[4]).reduce(lambda date1, date2: date1 if date1 < date2 else date2)
fd = datetime.datetime.fromtimestamp(float(first_date))

last_date = rdd.map(lambda time: time[4]).reduce(lambda date1, date2: date1 if date1 > date2 else date2)
ld = datetime.datetime.fromtimestamp(float(last_date))


# f) PCC
# ==============================================

# Key: (userId) Value: (tot. chars for user, tot. reviews for user (X_i))

# Key: (userId) Value: (Y_i, X_i)
chars_and_reviews = rdd.map(lambda user: [user[1], (len(base64.b64decode(fields[3])), 1)]).reduceBykey(
    lambda a, b: float(a[0]) + float(b[0]), a[1] + b[1]).mapValues(
    lambda v: (float(v[0]) / float(v[1])), v[1])
"""
# average characters in a users review (Y_i)
users_avg_chars = chars_and_reviews.mapValues(
    lambda v: (float(v[0]) / float(v[1])))
"""
#mean Y and X
tot_chars_and_reviews = chars_and_reviews.reduse(
    lambda a, b: float(a[0]) + float(b[0]), a[1] + b[1])

mean_X = float(tot_chars_and_reviews[1])/chars_and_reviews.count()

mean_Y = float(tot_chars_and_reviews[0])/chars_and_reviews.count()

PCC = chars_and_reviews.reduce(
    lambda a, b: (((float(a[1] - mean_X)) * (float(a[0] - mean_Y))) + ((float(b[1] - mean_X)) * (float(b[0] - mean_Y)))) /
                 (np.sqrt(((float(a[1] - mean_X)) * (float(b[1] - mean_X)))**2) * np.sqrt(((float(a[0] - mean_Y)) * (float(b[0] - mean_Y)))**2))
)


"""
# print("dt_object =", dt_object)
# print("type(dt_object) =", type(dt_object)))
"""
lines = [distinct_users_count, average_characters_per_review, top_ten_reviews, per_year, fd, ld ]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_file)

