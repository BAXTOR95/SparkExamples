from __future__ import print_function

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == '__main__':

    # Create a SparkSession
    spark = SparkSession.builder.appName('LinearRegression').getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    input_lines = spark.sparkContext.textFile('regression.txt')
    data = input_lines.map(lambda x: x.split(',')).map(
        lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

    # Convert this RDD to a DataFrame
    col_names = ['label', 'features']
    df = data.toDF(col_names)

    # Note, there are lots of cases where you can avoid going from an
    # RDD to a DataFrame. Perhaps you're importing data from a real database.
    # Or you are using structured streaming to get your data.

    # Let's split our data into training data and testing data
    train_test = df.randomSplit([0.5, 0.5])
    training_df = train_test[0]
    test_df = train_test[1]

    # Now create our linear regression model
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Train the model using our training data
    model = lir.fit(training_df)

    # Now see if we can predict values in our test data.
    # Generate predictions using our linear regression model for all
    # features in our test dataframe:
    full_predictions = model.transform(test_df).cache()

    # Extract the predictions and the 'known' correct labels.
    predictions = full_predictions.select('prediction').rdd.map(lambda x: x[0])
    labels = full_predictions.select('label').rdd.map(lambda x: x[0])

    # Zip them together
    prediction_and_label = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in prediction_and_label:
        print(prediction)

    # Stop the session
    spark.stop()
