from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def main():
    spark = SparkSession.builder.appName("Most Popular Superhero Dataframe").getOrCreate()

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    names = spark.read.option("sep", " ").schema(schema).csv("file:///SparkCourse/datasets/Marvel+Names")
    lines = spark.read.text("file:///SparkCourse/datasets/Marvel+Graph")

    connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
        .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
        .groupBy("id").agg(func.sum("connections").alias("connections"))
    
    mostPopular = connections.sort(func.col("connections").desc()).first()
    mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

    print(f"{mostPopularName} is the most popular superhero with {mostPopular[1]} connections")


if __name__ == "__main__":
    main()