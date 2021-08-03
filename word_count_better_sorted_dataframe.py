from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName('WordCount').getOrCreate()

# Read each line of my book into a dataframe
input_df = spark.read.text(
    'file:///Users/brian/code/from_courses/SparkCourse/book.txt')

# Split using a regular expression that extracts words
words = input_df.select(func.explode(
    func.split(input_df.value, '\\W+')).alias('word'))
words = words.filter(words.word != '')

# Normalize everything to lowercase
lowercase_words = words.select(func.lower(words.word).alias('word'))

# Count up the occurrences of each word
word_counts = lowercase_words.groupBy('word').count()

# Sort by counts
word_counts_sorted = word_counts.sort('count')

# Show the results.
word_counts_sorted.show(word_counts_sorted.count())
