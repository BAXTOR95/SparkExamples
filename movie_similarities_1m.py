import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

# To run on EMR successfully + output results for Star Wars:
# aws s3 cp s3://sundog-spark/MovieSimilarities1M.py ./
# aws s3 sp c3://sundog-spark/ml-1m/movies.dat ./
# spark-submit --executor-memory 1g MovieSimilarities1M.py 260


def load_movie_names():
    movie_names = {}
    with open('./ml-1m/movies.dat') as f:
        for line in f:
            fields = line.split('::')
            movie_names[int(fields[0])] = fields[1].encode().decode('ascii', 'ignore')
    return movie_names


def make_pairs(user, ratings):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))


def filter_duplicates(userID, ratings):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def compute_cosine_similarity(rating_pairs):
    num_pairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for rating_x, rating_y in rating_pairs:
        sum_xx += rating_x * rating_x
        sum_yy += rating_y * rating_y
        sum_xy += rating_x * rating_y
        num_pairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, num_pairs)


conf = SparkConf()
sc = SparkContext(conf=conf)

print('\nLoading movie names...')
name_dict = load_movie_names()

data = sc.textFile('./ml-1m/ratings.dat')

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split('::')).map(
    lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
ratings_partitioned = ratings.partitionBy(100)
joined_ratings = ratings_partitioned.join(ratings_partitioned)

# At this point our RDD consists of userID => ((movie_id, rating), (movie_id, rating))

# Filter out duplicate pairs
unique_joined_ratings = joined_ratings.filter(filter_duplicates)

# Now key by (movie1, movie2) pairs.
movie_pairs = unique_joined_ratings.map(make_pairs).partitionBy(100)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
movie_pair_ratings = movie_pairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
movie_pair_similarities = movie_pair_ratings.mapValues(
    compute_cosine_similarity).persist()

# Save the results if desired
movie_pair_similarities.sortByKey()
movie_pair_similarities.saveAsTextFile('movie-sims')

# Extract similarities for the movie we care about that are 'good'.
if (len(sys.argv) > 1):

    score_threshold = 0.97
    co_occurrence_threshold = 1000

    movie_id = int(sys.argv[1])

    # Filter for movies with this sim that are 'good' as defined by
    # our quality thresholds above
    filtered_results = movie_pair_similarities.filter(
        lambda pair, sim:
        (pair[0] == movie_id or pair[1]
         == movie_id)
        and sim[0] > score_threshold and sim[1] > co_occurrence_threshold)

    # Sort by quality score.
    results = filtered_results.map(lambda pair, sim: (
        sim, pair)).sortByKey(ascending=False).take(10)

    print('Top 10 similar movies for ' + name_dict[movie_id])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similar_movie_id = pair[0]
        if (similar_movie_id == movie_id):
            similar_movie_id = pair[1]
        print(name_dict[similar_movie_id] + '\tscore: ' +
              str(sim[0]) + '\tstrength: ' + str(sim[1]))
