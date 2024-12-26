from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import logging
from collections import namedtuple
import time
import matplotlib.pyplot as plt
import seaborn as sns


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

sc = SparkContext()

ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)

socket_stream = ssc.socketTextStream("127.0.0.1", 9999)

lines = socket_stream.window(20)

fields = ("tag", "count")
Tweet = namedtuple("Tweet", fields)

(
    lines.flatMap(lambda text: text.split(" "))
    .filter(lambda word: word.lower().startswith("#"))
    .map(lambda word: (word.lower(), 1))
    .reduceByKey(lambda a, b: a + b)
    .map(lambda rec: Tweet(rec[0], rec[1]))
    .foreachRDD(lambda rdd: rdd.toDF.sort(desc("count")))
    .limit(10).registerTempTable("tweets")
)

ssc.start()

count = 0
while count < 10:
    time.sleep(3)
    top_10_tweets = sqlContext.sql("SELECT tag, count FROM tweets")
    top_10_df = top_10_tweets.toPandas()
    plt.figure(figsize=(10, 8))
    sns.barplot(
        x="count",
        y="tag",
        data=top_10_df
    )
    plt.show()

ssc.stop()