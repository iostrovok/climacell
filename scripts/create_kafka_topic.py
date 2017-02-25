#!/usr/bin/python3.5

import kafka

KAFKA_TOPIC = 'clitopic'

client = kafka.client.KafkaClient(bootstrap_servers='localhost:9092')
client.add_topic(KAFKA_TOPIC)
