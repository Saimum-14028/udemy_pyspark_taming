from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer")
sc = SparkContext(conf = conf)

def mapping(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

lines = sc.textFile("./customer-orders.csv")

Cost = lines.map(mapping)

totalByCustomer = Cost.reduceByKey(lambda x, y: x + y)

swap = totalByCustomer.map(lambda x: (x[1], x[0]))

totalByCustomerSorted = swap.sortByKey()

results = totalByCustomerSorted.collect()

for result in results:
    print(result)