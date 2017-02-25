#!/usr/bin/env python

import os

'''
52.37.88.252 172.31.19.143 kafka.ostrovok.cc
54.213.140.254 172.31.28.45 spark.ostrovok.cc
'''

os.system('rsync -Pav -e "ssh -i ~/.ssh/my_keys.pem" /Users/ostrovok/test-tasks/climacell.co/* ubuntu@kafka.ostrovok.cc:/home/ubuntu/climacell/')
os.system('rsync -Pav -e "ssh -i ~/.ssh/my_keys.pem" /Users/ostrovok/test-tasks/climacell.co/* ubuntu@spark.ostrovok.cc:/home/ubuntu/climacell/')

