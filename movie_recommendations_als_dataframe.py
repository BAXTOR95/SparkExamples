from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs


def load_movie_names():
    movie_names = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open(
        'C:/Users/brian/code/SparkExamples/ml-100k/u.item',
        'r',
        encoding='ISO-8859-1',
        errors='ignore'
    ) as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


spark = SparkSession.builder.appName('ALSExample').getOrCreate()

movies_schema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('movie_id', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', LongType(), True)])

names = load_movie_names()

ratings = spark.read.option('sep', '\t').schema(movies_schema) \
    .csv('file:///Users/brian/code/SparkExamples/ml-100k/u.data')

print('Training recommendation model...')

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol('user_id').setItemCol(
    'movie_id').setRatingCol('rating')

model = als.fit(ratings)

# Manually construct a dataframe of the user ID's we want recs for
user_id = int(sys.argv[1])
user_schema = StructType([StructField('user_id', IntegerType(), True)])
users = spark.createDataFrame([[user_id, ]], user_schema)

recommendations = model.recommendForUserSubset(users, 10).collect()

print('Top 10 recommendations for user ID ' + str(user_id))

for user_recs in recommendations:
    # user_recs is (user_id, [Row(movie_id, rating), Row(movie_id, rating)...])
    my_recs = user_recs[1]
    for rec in my_recs:  # my Recs is just the column of recs for the user
        # For each rec in the list, extract the movie ID and rating
        movie = rec[0]
        rating = rec[1]
        movie_name = names[movie]
        print(movie_name, str(rating))

# Stop the session
spark.stop()
