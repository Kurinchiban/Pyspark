from pyspark.context import SparkContext
from pyspark.context import SparkConf

conf = SparkConf()
sc = SparkContext(master='local[*]', appName="Read_text_file", conf=conf)

lines = sc.textFile('/home/kurinchiban/Desktop/Pyspark/Pyspark/Word_count_spark_application/Sample_data.txt')

words = lines.flatMap(lambda line: line.split(' '))

words_count = words.map(lambda x: (x, 1))
word_count = words_count.reduceByKey(lambda a, b: a + b)
result = word_count.collect()

for (word, count) in result:
    print(f"{word}: {count}")

sc.stop()


# commend to run this program 

# ./bin/spark-submit /home/kurinchiban/Desktop/Pyspark/Pyspark/word_count_spark_application/word_count.py