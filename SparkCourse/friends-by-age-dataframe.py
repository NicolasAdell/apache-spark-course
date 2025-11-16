from pyspark.sql import SparkSession
from pyspark.sql import functions as func


def main():
    spark = SparkSession.builder.appName("FriendsByAgeDataframe").getOrCreate()
    lines = spark.read.option("header", "true").option("inferSchema", "true")\
        .csv("file:///SparkCourse/datasets/fakefriends-header.csv")
    
    print("Inferred Schema")
    lines.printSchema()

    friendsByAge = lines.select("age", "friends")
    friendsByAge.groupBy("age").avg("friends").sort("age").show()
    friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

    spark.stop()
if __name__ == "__main__":
    main()