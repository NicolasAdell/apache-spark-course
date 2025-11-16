from pyspark.sql import SparkSession
import os

os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

import pandas as pd
import pyspark.pandas as ps


def main():
    spark = SparkSession.builder \
            .appName("Pandas transform apply on Spark") \
            .config("spark.sql.ansi.enabled", "false") \
            .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
            .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
            .config("spark.python.worker.faulthandler.enabled", "true") \
            .getOrCreate()

    ps_df = ps.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
        "age": [25, 30, 35, 40, 45],
        "salary": [50000, 60000, 75000, 80000, 120000]
    })

    print("Original pandas-on-Spark dataframe")
    print(ps_df)

    ps_df["age_in_10_years"] = ps_df["age"].transform(lambda x: x + 10)
    print("\nDataFrame after transform (age + 10)")
    print(ps_df)

    ps_df["salary_category"] = ps_df["salary"].apply(categorize_salary)
    print("\nDataFrame with salary category")
    print(ps_df)

    ps_df["name_with_age"] = ps_df.apply(format_row, axis=1)
    print("\nDataframe after apply on rows (name_with_age)")
    print(ps_df)

    spark.stop()


def categorize_salary(salary):
    if salary < 60000:
        return "Low"
    elif salary < 100000:
        return "Medium"
    else:
        return "High"
    
def format_row(row):
    return "{} ({} years old)".format(row["name"], row["age"])


if __name__ == "__main__":
    main()