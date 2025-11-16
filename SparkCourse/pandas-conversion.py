from pyspark.sql import SparkSession
import os

os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

import pandas as pd
import pyspark.pandas as ps


def main():
    spark = SparkSession.builder \
            .appName("Pandas Integration with Pyspark") \
            .config("spark.sql.ansi.enabled", "false") \
            .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
            .getOrCreate()
    
    pandas_df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
        "age": [25, 30, 35, 40, 45]
    })

    print("Pandas DataFrame:")
    print(pandas_df)

    # Convert Pandas Dataframe to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)

    print("Schema of Spark DataFrame:")
    spark_df.printSchema()

    print("\nSpark DataFrame:")
    spark_df.show()

    # Perform transformations on Spark Dataframe
    filtered_spark_df = spark_df.filter(spark_df.age > 30)
    print("\nFiltered Spark DataFrame (age > 30):")
    filtered_spark_df.show()

    # Convert Spark DataFrame to Pandas DataFrame
    converted_pandas_df = filtered_spark_df.toPandas()
    print("\nConverted Pandas DataFrame:")
    print(converted_pandas_df)

    # Use pandas-on-Spark for scalable Pandas operations
    ps_df = ps.DataFrame(spark_df)

    print("\nUsing pandas-on-Spark (incrementing age by 1):")
    ps_df["age"] += 1
    print(ps_df)

    # Convert pandas-on-Spark DataFrame to Spark DataFrame
    converted_spark_df = ps_df.to_spark()
    print("\nConverted Spark DataFrame form pandas-on-Spark:")
    converted_spark_df.show()

    spark.stop()


if __name__ == "__main__":
    main()