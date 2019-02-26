from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer")
sc = SparkContext(conf = conf)

def mapping(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

lines = sc.textFile("./customer-orders.csv")

mappedInput = lines.map(mapping)

totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

results = totalByCustomer.collect()

for result in results:
    print(result)