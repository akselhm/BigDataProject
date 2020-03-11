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

"""
# a) Number of distinct users
# ==============================================
distinct_users = rdd.map(lambda fields: fields[1]).distinct()
distinct_users_count = distinct_users.count()

# b) Average char in user review
# ==============================================
characters_and_reviews_count = rdd.map(lambda fields: (len(base64.b64decode(fields[3])), 1)).reduce(
    lambda (chars_1, reviews_1), (chars_2, reviews_2): (chars_1 + chars_2, reviews_1 + reviews_2))
average_characters_per_review = characters_and_reviews_count[0] / characters_and_reviews_count[1]

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

"""
# f) PCC
# ==============================================

# Key: (userId) Value: (tot. chars for user, tot. reviews for user (X_i))

tot_chars_and_reviews = rdd.map(lambda user: [user[1], (len(base64.b64decode(user[3])), 1)]).reduceByKey(
    lambda (length_1, count_1), (length_2, count_2): (float(length_1) + float(length_2), count_1 + count_2))

# Key: (userId) Value: (Y_i, X_i)
avg_chars_and_reviews = tot_chars_and_reviews.mapValues(lambda v: [(v[0] / float(v[1])), v[1]])

x = avg_chars_and_reviews.map(lambda user: [user[0], user[1][0]])
y = tot_chars_and_reviews.map(lambda user: [user[0], user[1][1]])

tot_X = x.values().sum()
tot_Y = y.values().sum()

# mean Y and X
mean_X = tot_X / (avg_chars_and_reviews.count()*10)


mean_Y = tot_Y / avg_chars_and_reviews.count()

print avg_chars_and_reviews.take(20)

pcc = avg_chars_and_reviews.map(lambda user: [user[1][0], user[1][1]])

print mean_X
print mean_Y

pcc_numerator = pcc.reduce(lambda a, b: ((a[0] - mean_X) * (a[1] - mean_Y) + (b[0] - mean_X) * (b[1] - mean_Y), 1))
pcc_numerator = pcc_numerator[0]
pcc_denominator_1 = pcc.reduce(lambda a, b: (((a[0] - mean_X) ** 2 + (b[0] - mean_X) ** 2), 1))
pcc_denominator_1 = np.sqrt(pcc_denominator_1[0])
pcc_denominator_2 = pcc.reduce(lambda a, b: (((a[1] - mean_Y) ** 2 + (b[1] - mean_Y) ** 2), 1))
pcc_denominator_2 = np.sqrt(pcc_denominator_2[0])

print pcc_numerator
print pcc_denominator_1
print pcc_denominator_2
"""
# print("dt_object =", dt_object)
# print("type(dt_object) =", type(dt_object)))
lines = [distinct_users_count, average_characters_per_review, top_ten_reviews, per_year, fd, ld]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_file)
"""
