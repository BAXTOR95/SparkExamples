from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('MinTemperatures')
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (station_id, entry_type, temperature)


lines = sc.textFile(
    'file:///Users/brian/code/from_courses/SparkCourse/1800.csv')
parsed_lines = lines.map(parse_line)
min_temps = parsed_lines.filter(lambda x: 'TMIN' in x[1])
station_temps = min_temps.map(lambda x: (x[0], x[2]))
min_temps = station_temps.reduceByKey(lambda x, y: min(x, y))
results = min_temps.collect()

for result in results:
    print(result[0] + '\t{:.2f}F'.format(result[1]))
