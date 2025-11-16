from pyspark import SparkConf, SparkContext
import re


def main():
    conf = SparkConf().setMaster("local").setAppName("WorkCountBetterSorted")
    sc = SparkContext(conf = conf)
    text = sc.textFile("datasets/book.txt")
    words = text.flatMap(normalizeWords)

    wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

    results = wordCountsSorted.collect()

    for result in results:
        count = result[0]
        clean_word = result[1].encode("ascii", "ignore")
        if clean_word:
            print("{}:\t\t{}".format(clean_word, count))


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


if __name__ == "__main__":
    main()