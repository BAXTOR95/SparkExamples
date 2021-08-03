import re
from pyspark import SparkConf, SparkContext

def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster('local').setAppName('WordCount')
sc = SparkContext(conf = conf)

input = sc.textFile('file:///Users/brian/code/from_courses/SparkCourse/Book')
words = input.flatMap(normalize_words)
word_counts = words.countByValue()

for word, count in word_counts.items():
    clean_word = word.encode('ascii', 'ignore')
    if (clean_word):
        print(clean_word.decode() + ' ' + str(count))
