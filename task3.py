from pyspark import SparkContext, SparkConf
from operator import add

# Configure Spark
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)

# Set data folder, inputs and output
folder_name = "./data/"
input_businesses = "yelp_businesses.csv.gz"
output_file = "result3.tsv"

# Create RDD object from .gz-file
textFile = sc.textFile(folder_name + input_businesses)
rdd = textFile.map(lambda line: line.split('\t'))

# Filter out header
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)

# a) Average rating for businesses in each city
# ==============================================
city_and_rating = rdd.map(lambda x: [x[3], float(x[8])])  # The two columns we will use
city_and_rating = city_and_rating.mapValues(lambda v: (v, 1)).reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda v: float(v[0]) / v[1]).sortByKey()

# b) Top 10 most frequent categories
# ==============================================
number_per_category = rdd.map(lambda x: [x[10], 1])  # The two columns we will use
number_per_category = number_per_category.reduceByKey(add)

top_ten_categories = number_per_category.takeOrdered(10, key=lambda x: -x[1])

# c) Geographical centroid of the region
# ==============================================
region_with_coordinates = rdd.map(lambda x: [x[5], (x[6], x[7], 1)])  # Map to (postal code, latitude, longitude, 1)

geographical_centroid = region_with_coordinates.reduceByKey(
    lambda a, b: (float(a[0]) + float(b[0]), float(a[1]) + float(b[1]), a[2] + b[2])).mapValues(
    lambda v: (float(v[0]) / float(v[2]), float(v[1]) / float(v[2])))

print geographical_centroid.collect()

# rdd.saveAsTextFile(folder_name + output_file)
