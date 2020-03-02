from pyspark import SparkContext, SparkConf

# Configure Spark
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# Set data folder, inputs and output
folder_name = "./data/"
input_businesses = "yelp_businesses.csv.gz"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
input_users = "yelp_top_users_friendship_graph.csv.gz"
output_file = "result1.tsv"

# Set data folder, inputs and output
businessesRDD = sc.textFile(folder_name + input_businesses)
reviewersRDD = sc.textFile(folder_name + input_reviewers)
usersRDD = sc.textFile(folder_name + input_users)

# Count lines in each RDD and save to text file
businesses_number_of_records = businessesRDD.count()
reviewers_number_of_records = reviewersRDD.count()
users_number_of_records = usersRDD.count()

f = open(folder_name + output_file, 'w')
f.write("Businesses: " + str(businesses_number_of_records) + "\n\n")
f.write("Reviewers: " + str(reviewers_number_of_records) + "\n\n")
f.write("Users: " + str(users_number_of_records) + "\n\n")
