from pyspark import SparkConf, SparkContext


def main():
    conf = SparkConf().setMaster("local").setAppName("WorkCount")
    sc = SparkContext(conf = conf)
    text = sc.textFile("datasets/book.txt")
    words = text.flatMap(lambda x: x.split())

    wordCounts = words.countByValue()

    for word, count in wordCounts.items():
        clean_word = word.encode("ascii", "ignore")
        if clean_word:
            print(clean_word, count)

if __name__ == "__main__":
    main()