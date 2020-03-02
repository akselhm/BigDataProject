from pyspark import SparkContext, SparkConf

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)
folder_name = "./data/"
input_businesses = "yelp_businesses.csv.gz"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
input_users = "yelp_top_users_friendship_graph.csv.gz"
output_file_name = "result_1.tsv"

