from pyspark import SparkConf, SparkContext
import re


def main():
    conf = SparkConf().setMaster("local").setAppName("WorkCount")
    sc = SparkContext(conf = conf)
    text = sc.textFile("datasets/book.txt")
    words = text.flatMap(normalizeWords)

    wordCounts = words.countByValue()

    for word, count in wordCounts.items():
        clean_word = word.encode("ascii", "ignore")
        if clean_word:
            print("{}: {}".format(clean_word, count))


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


if __name__ == "__main__":
    main()