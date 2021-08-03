from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('MinTemperatures').getOrCreate()

schema = StructType([
    StructField('station_id', StringType(), True),
    StructField('date', IntegerType(), True),
    StructField('measure_type', StringType(), True),
    StructField('temperature', FloatType(), True)])

# Read the file as dataframe
df = spark.read.schema(schema).csv(
    'file:///Users/brian/code/from_courses/SparkCourse/1800.csv')
df.printSchema()

# Filter out all but TMIN entries
min_temps = df.filter(df.measure_type == 'TMIN')

# Select only station_id and temperature
station_temps = min_temps.select('station_id', 'temperature')

# Aggregate to find minimum temperature for every station
min_temps_by_station = station_temps.groupBy('station_id').min('temperature')
min_temps_by_station.show()

# Convert temperature to fahrenheit and sort the dataset
min_temps_by_station_f = min_temps_by_station.withColumn('temperature',
                                                         func.round(
                                                             func.col('min(temperature)') * 0.1 * (9.0 / 5.0) + 32.0, 2))\
    .select('station_id', 'temperature').sort('temperature')

# Collect, format, and print the results
results = min_temps_by_station_f.collect()

for result in results:
    print(result[0] + '\t{:.2f}F'.format(result[1]))

spark.stop()
