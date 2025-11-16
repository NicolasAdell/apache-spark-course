from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType


def main():
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    schema = StructType([
            StructField("userID", IntegerType(), True),
            StructField("movieID", IntegerType(), True),
            StructField("rating", IntegerType(), True),
            StructField("timestamp", LongType(), True)
            ])
    
    moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/datasets/ml-100k/u.data")
    topMoviesIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))
    
    topMoviesIDs.show(10, False)

    spark.stop()

if __name__ == "__main__":
    main()
