from pyspark import SparkConf, SparkContext

def parseLine(lines):
    fields = lines.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

def main():
    conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("file:///SparkCourse/datasets/fakefriends.csv")
    rdd = lines.map(parseLine)
    totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + x[1], y[0] + y[1]))
    averageByAge = totalsByAge.mapValues(lambda x: x[0] / x[1]).sortByKey()
    results = averageByAge.collect()

    for result in results:
        print(result)

if __name__ == "__main__":
    main()