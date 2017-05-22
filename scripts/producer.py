#!/usr/bin/python3.5

import kafka
import time
import fileinput
import sys
import random
import subprocess
import hashlib

KAFKA_TOPIC = 'clitopic'

SCRIPT_FILE = '/home/ubuntu/climatcell/scripts/check_if_great.py'
WORDS_FILE = '/home/ubuntu/climatcell/words.txt'

producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')


def messageID(value):
    value += str(round(time.time() * 1000000)).encode("utf-8")
    # .encode("utf-8")
    return b"mykey:" + hashlib.sha224(value).hexdigest().encode("utf-8")

def load_words(file):
    content = []

    with open(file) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    return content


def add_many_words(words, count):
    line = ""
    while count > 0:
        line = line + " " + random.choice(words)
        count -= 1

    return line

print("load words...")
words = load_words(WORDS_FILE)
print("...words are loaded")

while True:
    add_line = add_many_words(words, 20)
    output = subprocess.Popen(
        [SCRIPT_FILE, "--cpu=90", "--memory=90"], stdout=subprocess.PIPE).communicate()[0]
    send_value = output + add_line.encode("utf-8")
    send_key = hashlib.sha224(
        str(round(time.time() * 1000000)).encode("utf-8")).hexdigest().encode("utf-8")
    print("output: ", send_key, output)
    res = producer.send(KAFKA_TOPIC, key=send_key, value=send_value)
    print("res: ", res)
    producer.flush()
    time.sleep(5)
