#!/usr/bin/python3.5


# 27017

import pymongo
import sys
import kafka
from datetime import datetime

DB_NAME = 'climatcell'
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'clitopic'

try:
    mconn = pymongo.MongoClient(
        socketTimeoutMS=100, serverSelectionTimeoutMS=100)
except Exception as e:
    sys.stderr.write("Could not mconnect to MongoDB: %s\n" % e)
    sys.exit(1)

db = mconn[DB_NAME]


try:
    consumer = kafka.KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=[KAFKA_SERVER])
    # consumer.subscribe([KAFKA_TOPIC])
except Exception as e:
    sys.stderr.write("Could not mconnect to kafka: %s\n" % e)
    sys.exit(1)


class KafkaConsumerStatusColl():

    collection = 'status'

    def __init__(self, db):
        self.db = db

    def store(self, info):
        try:
            # coll = db[self.collection]
            if not 'created_time' in info:
                info['created_time'] = datetime.now()
            result = self.db.status.insert_one(info)
            print("result.inserted_id: ", result.inserted_id)
        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print(inst)


class StatusColl():

    collection = 'status'

    def __init__(self, db):
        self.db = db

    def store(self, info):
        try:
            # coll = db[self.collection]
            if not 'created_time' in info:
                info['created_time'] = datetime.now()

            result = self.db.status.insert_one(info)
            print("result.inserted_id: ", result.inserted_id)
        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print(inst)

status = StatusColl(db)
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                       message.offset, message.key,
    #                                       message.value))
    print("%s:%d:%d: key=%s" %
          (message.topic, message.partition, message.offset, message.key))
    status.store({"key": message.key.decode("utf-8"), "value": message.value.decode("utf-8")})


consumer.close()
