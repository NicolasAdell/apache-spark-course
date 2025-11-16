from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def main():
    spark = SparkSession.builder.appName("Popular Movies Nice dataframe").getOrCreate()

    namesDict = spark.sparkContext.broadcast(loadMovieNames())

    schema = StructType([
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])

    moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/datasets/ml-100k/u.data")
    movieCounts = moviesDF.groupBy("movieID").count()
    lookUpNameUDF = func.udf(lambda movie_id: namesDict.value[movie_id])
    moviesWithNames = movieCounts.withColumn("movieTitle", lookUpNameUDF(func.col("movieID")))
    sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

    sortedMoviesWithNames.show(10, False)

    spark.stop()


def loadMovieNames():
    movieNames = {}
    with codecs.open("C:/SparkCourse/datasets/ml-100k/u.ITEM", 'r', encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames

if __name__ == "__main__":
    main()