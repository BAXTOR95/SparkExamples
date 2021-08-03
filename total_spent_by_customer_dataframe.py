from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName(
    'TotalSpentByCustomerDF').master('local[*]').getOrCreate()

schema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('item_id', IntegerType(), True),
    StructField('amount', FloatType(), True)
])

# Read the file as dataframe
df = spark.read.schema(schema).csv(
    'file:///Users/brian/code/from_courses/SparkCourse/customer-orders.csv'
)

# Select only user_id and amount
amount_spent = df.select('user_id', 'amount')

# Aggregate to find total spent amount for every user
total_spent_amount = amount_spent.groupBy('user_id').agg(
    func.round(func.sum('amount'), 2).alias('total_amount')).sort('total_amount')

results = total_spent_amount.collect()

for result in results:
    print(result[0], '\t${:.2f}'.format(result[1]))

spark.stop()
