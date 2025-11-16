from __future__ import print_function
from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors


def main():
    spark = SparkSession.builder.appName("Linear Regression").getOrCreate()

    # Convert to what MiLib expects
    inputLines = spark.sparkContext.textFile("file:///SparkCourse/datasets/regression.txt")
    data = inputLines.map(lambda x: x.split(',')).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

    # Convert RDD to dataframe
    df = data.toDF(["label", "features"])

    # Split into testing and training
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Create model with hyperparamaters
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    model = lir.fit(trainingDF)

    # Extract predictions and labels
    fullPredictions = model.transform(testDF).cache()
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    # Zip them together
    predictionsLabels = predictions.zip(labels).collect()

    for prediction in predictionsLabels:
        print(prediction)

    spark.stop()


if __name__ == "__main__":
    main()