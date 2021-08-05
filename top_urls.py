from pyspark.sql import SparkSession

import pyspark.sql.functions as func

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName('StructuredStreaming').getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines
# as access_lines
access_lines = spark.readStream.text('logs')

# Parse out the common log format to a DataFrame
content_size_exp = r'\s(\d+)$'
status_exp = r'\s(\d{3})\s'
general_exp = r'\'(\S+)\s(\S+)\s*(\S*)\''
time_exp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
host_exp = r'(^\S+\.[\S+\.]+\S+)\s'

logs_df = access_lines.select(
    func.regexp_extract('value', host_exp, 1).alias('host'),
    func.regexp_extract('value', time_exp, 1).alias('timestamp'),
    func.regexp_extract('value', general_exp, 1).alias('method'),
    func.regexp_extract('value', general_exp, 2).alias('endpoint'),
    func.regexp_extract('value', general_exp, 3).alias('protocol'),
    func.regexp_extract('value', status_exp, 1).cast(
        'integer').alias('status'),
    func.regexp_extract('value', content_size_exp, 1).cast(
        'integer').alias('content_size')
)

logs_df2 = logs_df.withColumn('eventTime', func.current_timestamp())

# Keep a running count of endpoints
endpoint_counts = logs_df2.groupBy(
    func.window(
        func.col('eventTime'), '30 seconds', '10 seconds'
    ), func.col('endpoint')).count()

sorted_endpoint_counts = endpoint_counts.orderBy(func.col('count').desc())

# Display the stream to the console
query = sorted_endpoint_counts.writeStream.outputMode('complete')\
    .format('console').queryName('counts').start()

# Wait until we terminate the scripts
query.awaitTermination()

# Stop the session
spark.stop()
