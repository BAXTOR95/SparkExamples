from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import codecs


spark = SparkSession.builder.appName('MostObscureSuperheroes').getOrCreate()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True)])

names = spark.read.schema(schema).option('sep', ' ').csv(
    'file:///Users/brian/code/from_courses/SparkCourse/Marvel-Names')

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

# Add a superhero_name column using our new udf
most_obscures_names = most_obscures.join(names, 'id').select('name')

print('The most obscure superheroes of all time with ' +
      str(min_connection) + ' connection(s) are:')
most_obscures_names.show()

# Stop the session
spark.stop()
