from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

# Create schema when reading u.data
schema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('movie_id', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', LongType(), True)])

# Load up movie data as dataframe
movies_df = spark.read.option('sep', '\t').schema(schema).csv(
    'file:///Users/brian/code/from_courses/SparkCourse/ml-100k/u.data')

# Some SQL-style magic to sort all movies by popularity in one line!
top_movie_ids = movies_df.groupBy(
    'movie_id').count().orderBy(func.desc('count'))

# Grab the top 10
top_movie_ids.show(10)

# Stop the session
spark.stop()
