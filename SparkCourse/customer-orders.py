from pyspark import SparkConf, SparkContext


def main():
    conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
    sc = SparkContext(conf = conf)
    csv_file = sc.textFile("datasets/customer-orders.csv")
    orders = csv_file.map(extract_custumer_price)
    total_by_customer = orders.reduceByKey(lambda x, y: x + y)
    sorted_total_by_customer = total_by_customer.map(lambda x: (x[1], x[0])).sortByKey()
    results = sorted_total_by_customer.collect()

    for r in results:
        print(r[1], r[0])


def extract_custumer_price(lines):
    fields = lines.split(',')
    return (int(fields[0]), float(fields[2]))

if __name__ == "__main__":
    main()