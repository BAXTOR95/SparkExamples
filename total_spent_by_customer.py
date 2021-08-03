from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('TotalSpentByCustomer')
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    user_id = int(fields[0])
    amount = float(fields[2])
    return (user_id, amount)


lines = sc.textFile(
    'file:///Users/brian/code/from_courses/SparkCourse/customer-orders.csv')
parsed_lines = lines.map(parse_line)
total_spent_amount = parsed_lines.reduceByKey(lambda x, y: x + y)
results = total_spent_amount.collect()

for result in results:
    print(result[0], '\t${:.2f}'.format(result[1]))
