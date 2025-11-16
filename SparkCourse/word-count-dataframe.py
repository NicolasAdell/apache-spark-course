from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def main():
    spark = SparkSession.builder.appName("WordCountSQL").getOrCreate()
    inputDF = spark.read.text("file:///SparkCourse/datasets/book.txt")

    words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
    words_without_empty_strings = words.filter(words.word != "").alias("word")

    lowercase_words = words_without_empty_strings.select(func.lower(words_without_empty_strings.word).alias("word"))

    word_counts = lowercase_words.groupBy("word").count()

    word_counts_sorted = word_counts.sort("count")

    word_counts_sorted.show(word_counts_sorted.count())

    spark.stop()

if __name__ == "__main__":
    main()
