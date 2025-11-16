from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("file:///SparkCourse/datasets/1800.csv")
    rdd = lines.map(parseLines)
    min_temps = rdd.filter(lambda x: "TMAX" in x[1])
    stations_temps = min_temps.map(lambda x: (x[0], x[2]))
    min_temps = stations_temps.reduceByKey(lambda x, y: max(x, y)).sortByKey()
    results = min_temps.collect()
    for res in results:
        print(res[0] + "\t{:.2f} C".format(res[1]))


def parseLines(lines):
    fields = lines.split(',')
    station_id = fields[0]
    type = fields[2]
    temp = float(fields[3]) * 0.1

    return (station_id, type, temp)


if __name__ == "__main__":
    main()