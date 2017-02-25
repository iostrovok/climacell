#!/usr/bin/python3.5

import subprocess

COMMAND = ['/opt/Spark/bin/spark-submit',
           '--packages',
           'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0',
           './scripts/wordcount_spark.py'
           ]

print(' '.join(COMMAND))
proc = subprocess.Popen(COMMAND, stdout=subprocess.PIPE)

for line in iter(proc.stdout.readline, ''):
    res = line.rstrip()
    if res != b'':
        print(res)
