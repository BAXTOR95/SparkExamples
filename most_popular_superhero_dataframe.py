from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('MostPopularSuperhero').getOrCreate()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True)])

names = spark.read.schema(schema).option('sep', ' ').csv(
    'file:///Users/brian/code/from_courses/SparkCourse/Marvel-Names')

lines = spark.read.text(
    'file:///Users/brian/code/from_courses/SparkCourse/Marvel-Graph')

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn('id', func.split(func.trim(func.col('value')), ' ')[0]) \
    .withColumn('connections', func.size(func.split(func.trim(func.col('value')), ' ')) - 1) \
    .groupBy('id').agg(func.sum('connections').alias('connections'))

most_popular = connections.sort(func.col('connections').desc()).first()

most_popular_name = names.filter(
    func.col('id') == most_popular[0]).select('name').first()

print(most_popular_name[0] + ' is the most popular superhero with ' +
      str(most_popular[1]) + ' co-appearances.')
