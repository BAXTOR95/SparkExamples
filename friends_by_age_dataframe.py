from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

# Create a SparkSession
spark = SparkSession.builder.appName('SparkSQL').getOrCreate()

lines = spark.read.option('header', 'true').option('inferSchema', 'true')\
    .csv('file:///Users/brian/code/from_courses/SparkCourse/fakefriends-header.csv')

# Select only age and friends columns
friends_by_age = lines.select('age', 'friends')

# From friends_by_age we group by 'age' and then compute average
friends_by_age.groupBy('age').avg('friends').show()

# Sorted
friends_by_age.groupBy('age').avg('friends').sort('age').show()

# Formatted more nicely
friends_by_age.groupBy('age').agg(func.round(
    func.avg('friends'), 2)).sort('age').show()

# With a custom column name
friends_by_age.groupBy('age').agg(func.round(
    func.avg('friends'), 2).alias('friends_avg')).sort('age').show()

spark.stop()
