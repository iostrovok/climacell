from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
import datetime
import pymongo
import sys


KAFKA_TOPIC = 'clitopic'
BROKERS = 'kafka.ostrovok.cc:9092'

MONGO_HOSTS = ['kafka.ostrovok.cc:27017']
DB_NAME = 'climatcell'
COLLECTION = 'kafka'


class KafkaColl():

    def __init__(self, db, collection):
        self.db = db
        self.collection = collection

    def store(self, info):
        try:
            if not 'created_time' in info:
                info['created_time'] = datetime.datetime.now()

            result = self.db[self.collection].insert_one(info)
            print("result.inserted_id: ", result.inserted_id)
        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print(inst)

try:
    mconn = pymongo.MongoClient(MONGO_HOSTS, socketTimeoutMS=100, serverSelectionTimeoutMS=100)
except Exception as e:
    sys.stderr.write("Could not mconnect to MongoDB: %s\n" % e)
    sys.exit(1)

db = mconn[DB_NAME]
colection = KafkaColl(db, COLLECTION)


def sendToMongoDB(time, rdd):
    global colection

    count = rdd.count()

    if count > 0:
        out = []
        taken = rdd.take(count)
        for record in taken:
            out.append(record)
        colection.store({"stat": out})

sc = SparkContext()
ssc = StreamingContext(sc, 2)
kvs = KafkaUtils.createDirectStream(ssc, ['clitopic'], {"metadata.broker.list": BROKERS})

lines = kvs.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")).map(
    lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

counts.foreachRDD(sendToMongoDB)

ssc.start()
ssc.awaitTermination()
