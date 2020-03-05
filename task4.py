from pyspark import SparkContext, SparkConf

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

folder_name = "./data/"
input_users = "yelp_top_users_friendship_graph.csv.gz"
output_file_name = "result_1.tsv"

usersRDD = sc.textFile(folder_name + input_users)
users_lines_rdd = usersRDD.map(lambda line: line.split('\t'))


# a)

# TODO: return only top ten (topByKey?)

most_in = users_lines_rdd.map(lambda node: (node[1], 1)).reduceByKey(add).sortByValue()
top_ten_in = most_in.topByKey(10)

most_out = users_lines_rdd.map(lambda node: (node[0], 1)).reduceByKey(add).sortByValue()
top_ten_out = most_out.topByKey(10)

# b) TODO: find mean and median of users in and out



