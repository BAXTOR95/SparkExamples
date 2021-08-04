from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys


def compute_cosine_similarity(spark, data):
    # Compute xx, xy and yy columns
    pair_scores = data \
        .withColumn('xx', func.col('rating1') * func.col('rating1')) \
        .withColumn('yy', func.col('rating2') * func.col('rating2')) \
        .withColumn('xy', func.col('rating1') * func.col('rating2'))

    # Compute numerator, denominator and num_pairs columns
    calculate_similarity = pair_scores \
        .groupBy('movie1', 'movie2') \
        .agg(
            func.sum(func.col('xy')).alias('numerator'),
            (func.sqrt(func.sum(func.col('xx'))) *
             func.sqrt(func.sum(func.col('yy')))).alias('denominator'),
            func.count(func.col('xy')).alias('num_pairs')
        )

    # Calculate score and select only needed columns
    # (movie1, movie2, score, num_pairs)
    result = calculate_similarity \
        .withColumn('score',
                    func.when(func.col('denominator') != 0, func.col(
                        'numerator') / func.col('denominator'))
                    .otherwise(0)
                    ).select('movie1', 'movie2', 'score', 'num_pairs')

    return result

# Get movie name by given movie id


def get_movie_name(movie_names, movie_id):
    result = movie_names.filter(func.col('movie_id') == movie_id) \
        .select('movie_title').collect()[0]

    return result[0]


spark = SparkSession.builder.appName(
    'MovieSimilarities').master('local[*]').getOrCreate()

movie_names_schema = StructType([
    StructField('movie_id', IntegerType(), True),
    StructField('movie_title', StringType(), True)
])

movies_schema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('movie_id', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', LongType(), True)])


# Create a broadcast dataset of movie_id and movie_title.
# Apply ISO-885901 charset
movie_names = spark.read \
    .option('sep', '|') \
    .option('charset', 'ISO-8859-1') \
    .schema(movie_names_schema) \
    .csv('file:///Users/brian/code/from_courses/SparkCourse/ml-100k/u.item')

# Load up movie data as dataset
movies = spark.read \
    .option('sep', '\t') \
    .schema(movies_schema) \
    .csv('file:///Users/brian/code/from_courses/SparkCourse/ml-100k/u.data')

# Leave only the required fields and filter bad movies from 1 to 3 stars
ratings = movies.select('user_id', 'movie_id', 'rating').filter(
    func.col('rating') > 3)

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
movie_pairs = ratings.alias('ratings1') \
    .join(ratings.alias('ratings2'),
          (func.col('ratings1.user_id') == func.col('ratings2.user_id'))
          & (func.col('ratings1.movie_id') < func.col('ratings2.movie_id'))) \
    .select(func.col('ratings1.movie_id').alias('movie1'),
            func.col('ratings2.movie_id').alias('movie2'),
            func.col('ratings1.rating').alias('rating1'),
            func.col('ratings2.rating').alias('rating2'))


movie_pair_similarities = compute_cosine_similarity(spark, movie_pairs).cache()

if (len(sys.argv) > 1):
    score_threshold = 0.97
    co_occurrence_threshold = 50.0

    movie_id = int(sys.argv[1])

    # Filter for movies with this sim that are 'good' as defined by
    # our quality thresholds above
    filtered_results = movie_pair_similarities.filter(
        ((func.col('movie1') == movie_id) | (func.col('movie2') == movie_id)) &
        (func.col('score') > score_threshold) & (func.col('num_pairs') > co_occurrence_threshold))

    # Sort by quality score.
    results = filtered_results.sort(func.col('score').desc()).take(10)

    print('Top 10 similar movies for ' + get_movie_name(movie_names, movie_id))

    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similar_movie_id = result.movie1
        if (similar_movie_id == movie_id):
            similar_movie_id = result.movie2

        print(get_movie_name(movie_names, similar_movie_id) + '\tscore: '
              + str(result.score) + '\tstrength: ' + str(result.num_pairs))
