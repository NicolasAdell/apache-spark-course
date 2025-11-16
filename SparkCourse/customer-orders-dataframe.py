from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType


def main():
    spark = SparkSession.builder.appName("CustomerOrdersDataframe").getOrCreate()

    schema = StructType([
        StructField("cust_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("amount_spent", FloatType(), True)
    ])

    df = spark.read.schema(schema).csv("file:///SparkCourse/datasets/customer-orders.csv")

    customers_by_amount_spent = df.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent"))
    customers_by_amount_spent_sorted = customers_by_amount_spent.sort("total_spent")

    customers_by_amount_spent_sorted.show(customers_by_amount_spent_sorted.count())

    spark.stop()


if __name__ == "__main__":
    main()