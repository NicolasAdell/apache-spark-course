from __future__ import print_function
from pyspark.ml.regression import DecisionTreeRegressor

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler


def main():
    spark = SparkSession.builder.appName("Decision Tree").getOrCreate()

    # Convert to what MiLib expects
    data = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/datasets/realestate.csv")
    
    assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]).setOutputCol("features")

    df = assembler.transform(data).select("PriceOfUnitArea", "features")

    # Split into testing and training
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Create model with hyperparamaters
    dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")

    model = dtr.fit(trainingDF)

    # Extract predictions and labels
    fullPredictions = model.transform(testDF).cache()
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

    # Zip them together
    predictionsLabels = predictions.zip(labels).collect()

    for prediction in predictionsLabels:
        print(prediction)

    spark.stop()


if __name__ == "__main__":
    main()