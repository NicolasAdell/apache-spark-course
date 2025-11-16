from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def main():
    spark = SparkSession.builder.appName("MinTemperaturesDataframe").getOrCreate()

    schema = StructType([
        StructField("stationID", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True)
    ])

    df = spark.read.schema(schema).csv("file:///SparkCourse/datasets/1800.csv")
    df.printSchema()

    min_temps = df.filter(df.measure_type == "TMIN")
    station_temps = min_temps.select("stationID", "temperature")
    min_temps_by_station = station_temps.groupBy("stationID").min("temperature")
    min_temps_by_station.show()

    min_temps_by_station_C = min_temps_by_station.withColumn("temperature", func.round(func.col("min(temperature)") * 0.1, 2))\
                                                             .select("stationID", "temperature").sort("temperature")

    results = min_temps_by_station_C.collect()
    for res in results:
        print("{} + {:.2f}C".format(res[0], res[1]))

    spark.stop()

if __name__ == "__main__":
    main()