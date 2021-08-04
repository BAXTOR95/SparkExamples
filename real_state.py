from __future__ import print_function

from pyspark.ml.regression import DecisionTreeRegressor

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == '__main__':

    # Create a SparkSession
    spark = SparkSession.builder.appName('DecisionTree').getOrCreate()

    # Read the file as dataframe
    data = spark.read.option('header', 'true').option('inferSchema', 'true').csv(
        'file:///Users/brian/code/from_courses/SparkCourse/realestate.csv'
    )

    # Creating the vector assembler to create a new column containing
    # all the features in an array-like object
    assembler = VectorAssembler(outputCol='features').setInputCols(
        ['HouseAge', 'DistanceToMRT', 'NumberConvenienceStores'])

    # Transforming the data using the assembler and leave only
    # the required fields (field to predict, features)
    df = assembler.transform(data).select('PriceOfUnitArea', 'features')

    # Split our data into training data and testing data
    train_test = df.randomSplit([0.5, 0.5])
    training_df = train_test[0]
    test_df = train_test[1]

    # Now create out Decision Tree Regressor model
    dt = DecisionTreeRegressor(maxDepth=2)

    # Change the default feature and label of the model to ours
    dt.setFeaturesCol('features')
    dt.setLabelCol('PriceOfUnitArea')

    # Train the model using out training data
    model = dt.fit(training_df)

    # Now see if we can predict values in our test data.
    # Generate predictions using out Decision Tree Regressor model for all
    # features in our test dataframe:
    full_predictions = model.transform(test_df).cache()

    # Extract the predictions and the 'known' correct labels.
    predictions = full_predictions.select('prediction').rdd.map(lambda x: x[0])
    labels = full_predictions.select('PriceOfUnitArea').rdd.map(lambda x: x[0])

    # Zip them together
    prediction_and_label = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each Price of Unit Area
    for prediction in prediction_and_label:
        print(prediction)

    # Stop the session
    spark.stop()
