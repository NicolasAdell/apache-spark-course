from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs


def main():
    spark = SparkSession.builder.appName("Movie recommendations als").getOrCreate()

    movieSchema = StructType([
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", IntegerType(), True)
    ])

    names = loadMovieNames()

    ratings = spark.read.option("sep", "\t").schema(movieSchema).csv("file:///SparkCourse/datasets/ml-100k/u.data")

    print("Training recommendation model...")

    als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID").setRatingCol("rating")

    model = als.fit(ratings)

    # Construct a dataframe of the user ID to get recommendations
    userID = int(sys.argv[1])
    userSchema = StructType([StructField("userID", IntegerType(), True)])
    users = spark.createDataFrame([[userID,]], userSchema)

    recommendations = model.recommendForUserSubset(users, 10).collect()

    print(f"Top 10 recommendations for user ID: {userID}")

    for userRecs in recommendations:
        myRecs = userRecs[1]
        for rec in myRecs:
            movie = rec[0]
            rating = rec[1]
            movieName = names[movie]
            print(f"{movieName}: {rating}")


def loadMovieNames():
    movieNames = {}

    with codecs.open("C:/SparkCourse/datasets/ml-100k/u.ITEM", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]

    return movieNames


if __name__ == "__main__":
    main()




