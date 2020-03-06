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

most_in = users_lines_rdd.map(lambda node: (node[1], 1)).reduceByKey(add)
top_ten_in = most_in.takeOrdered(10, lambda x: -x[1])

most_out = users_lines_rdd.map(lambda node: (node[0], 1)).reduceByKey(add)
top_ten_out = most_out.takeOrdered(10, lambda x: -x[1])

# b) TODO: find mean and median of users in and out

# FIND MEAN

avg_in = float(users_lines_rdd.count())/most_in.count() #tot conections/users with connections "in"
avg_out = float(users_lines_rdd.count())/most_out.count()

# FIND MEDIAN







