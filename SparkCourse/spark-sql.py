from pyspark.sql import SparkSession
from pyspark.sql import Row


def main():
    spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
    lines = spark.sparkContext.textFile("datasets/fakefriends.csv")
    people = lines.map(mapper)
    
    schemaPeople = spark.createDataFrame(people).cache()
    schemaPeople.createOrReplaceTempView("people")

    teenagers = spark.sql("SELECT * FROM PEOPLE WHERE AGE BETWEEN 13 AND 19")
    for teen in teenagers.collect():
        print(teen)

    schemaPeople.groupBy("age").count().orderBy("age").show()

    spark.stop()


def mapper(lines):
    fields = lines.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1]).encode("," "utf-8"),
               age=int(fields[2]), numFriends=int(fields[3]))


if __name__ == "__main__":
    main()