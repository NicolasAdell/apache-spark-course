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
    
    min_connections = connections.agg(func.min("connections")).first()[0]
    
    names_with_min_connection = names.join(connections, "id").filter(func.col("connections") == min_connections)

    print(f"The following characters have only {min_connections} connections: ")
    names_with_min_connection.select("name").show()


if __name__ == "__main__":
    main()