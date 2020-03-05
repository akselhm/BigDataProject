from pyspark import SparkContext, SparkConf
from operator import add

# Configure Spark
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)

# Set data folder, inputs and output
folder_name = "./data/"
input_businesses = "yelp_businesses.csv.gz"
output_file = "result3.tsv"

textFile = sc.textFile(folder_name + input_businesses)
rdd = textFile.map(lambda line: line.split('\t'))

# Filter out header
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)

rdd = rdd.map(lambda x: [x[3], float(x[8])])  # The two columns we will use

rdd = rdd.mapValues(lambda v: (v, 1)).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).mapValues(
    lambda v: float(v[0]) / v[1]).sortByKey()

rdd.saveAsTextFile(folder_name + output_file)
