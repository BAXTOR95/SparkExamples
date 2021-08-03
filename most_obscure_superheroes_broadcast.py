from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import codecs


def load_superhero_names():
    superhero_names = {}
    with codecs.open(
        'C:/Users/brian/code/from_courses/SparkCourse/Marvel-Names',
        'r',
        encoding='ISO-8859-1',
        errors='ignore'
    ) as f:
        for line in f:
            fields = line.split(' ')
            superhero_names[int(fields[0])] = fields[1]
    return superhero_names


spark = SparkSession.builder.appName('MostObscureSuperheroes').getOrCreate()

name_dict = spark.sparkContext.broadcast(load_superhero_names())

lines = spark.read.text(
    'file:///Users/brian/code/from_courses/SparkCourse/Marvel-Graph'
)

connections = lines.withColumn('id', func.split(func.trim(func.col('value')), ' ')[0]) \
    .withColumn('connections', func.size(func.split(func.trim(func.col('value')), ' ')) - 1) \
    .groupBy('id').agg(func.sum('connections').alias('connections'))

min_connection = connections.select('connections').agg(
    func.min('connections')).first()[0]

most_obscures = connections.sort(func.col('connections').asc()).filter(
    func.col('connections') == min_connection)

# Create a user-defined function to look up superhero names from
# broadcast dictionary.


def lookup_name(superhero_id):
    return name_dict.value[int(superhero_id)]


lookup_name_udf = func.udf(lookup_name)

# Add a superhero_name column using our new udf
most_obscures_names = most_obscures.withColumn(
    'superhero_name', lookup_name_udf(func.col('id')))

most_obscures_names.show()

# Stop the session
spark.stop()
