from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, udf
from pyspark.sql.types import IntegerType
import re

# User-Defined Table Function
@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text: str):
        '''Extracts hashtags from the input text'''
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield(hashtag,)

# User-Defined Function
@udf(returnType=IntegerType())
def count_hashtags(text: str):
    '''Counts number of hashtags in the input text'''
    if text:
        return len(re.findall(r"#\w+", text))
    return 0

def main():
    spark = SparkSession.builder \
            .appName("UDTF and UDF Example") \
            .config("spark.sql.execution.pythonUDTF.enabled", "true") \
            .getOrCreate()
    
    spark.udtf.register("extract_hashtags", HashtagExtractor)
    spark.udf.register("count_hashtags", count_hashtags)

    print("\nHashtag extractor example: ")
    spark.sql("SELECT * FROM extract_hashtags('Welcome to #ApacheSpark and #BigData!')").show()

    print("\nHastag counter example: ")
    spark.sql("SELECT count_hashtags('Welcome to #ApacheSpark and #BigData!') AS hastag_count").show()

    # Apply UDF in a DataFrame query
    data = [("Learning #AI with #ML",), ("Explore #DataScience",), ("No hastags here",)]
    df = spark.createDataFrame(data, ["text"])
    df.selectExpr("text", "count_hashtags(text) AS num_hashtags").show()

    # Apply UDTF with a LATERAL JOIN
    print("\nUsing UDTF with LATERAL JOIN: ")
    df.createOrReplaceTempView("tweets")
    spark.sql(
        "SELECT text, hashtag FROM tweets, LATERAL extract_hashtags(text)"
    ).show()

    spark.stop()

if __name__ == "__main__":
    main()