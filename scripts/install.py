#!/usr/bin/python3

import os
import time
import fileinput
import sys
import importlib
import subprocess
import getopt
import urllib.request
from subprocess import call

DOWNLOAD_SPARK = True

KAFKA_TOPIC = 'clitopic'

MONGODB_CONF = '/etc/mongodb.conf'

# TODO: MUST FIND SOLUTION FOR ALL USERS
HOME_DIR = '/home/ubuntu'
BASHRC_FILE = HOME_DIR + '/.bashrc'

KAFKA_URL = "http://mirror.fibergrid.in/apache/kafka/0.10.2.0/kafka_2.12-0.10.2.0.tgz"
KAFKA_DIR = '/opt/Kafka/'
KAFKA_CONF = KAFKA_DIR + 'config/server.properties'
KAFKA_START_SRCIPT = KAFKA_DIR + 'bin/kafka-server-start.sh'

KAFKA_START_CMD = [KAFKA_START_SRCIPT, KAFKA_CONF]
SPARK_URL = 'http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz'
# SPARK_URL = 'http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz'
# SPARK_URL = 'http://d3kbcqa49mib13.cloudfront.net/spark-1.6.3-bin-hadoop2.6.tgz'
SPARK_DIR = '/opt/Spark/'
# SPARK_MASTER_NODA = 'spark.ostrovok.cc'
SPARK_MASTER_NODA = '172.31.28.45'
# SPARK_MASTER_NODA = 'localhost'
SPARK_MASTER_PORT = '7077'

python_package_install = ['platform', 'psutil', 'urllib.request', 'fileinput']

# Waht do we want to load?
is_install = False
is_start = False
is_kafka = False
is_spark = False
is_mongodb = False
is_all = False


def init_params():
    global is_all, is_kafka, is_spark, is_mongodb, is_start, is_install

    try:
        opts, args = getopt.getopt(sys.argv[1:], "si:o", ["all", "kafka", "spark", "mongodb", "mongo"])
    except getopt.GetoptError as err:
        # print error, help information and exit:
        print(err)
        print("install.py -s -i --all --kafka --spark --mongodb")
        sys.exit(2)

    for o, a in opts:
        if o == '-i' or a == '-i':
            is_install = True
        if o == '-s' or a == '-s':
            is_start = True
        if o == "--kafka" or a == "--kafka":
            is_kafka = True
        if o == "--spark" or a == "--spark":
            is_spark = True
        if o in ("--mongodb", "--mongo") or a in ("--mongodb", "--mongo"):
            is_mongodb = True
        if o == '--all' or a == '--all':
            is_all = True


def replaceListInFile(replace_list):
    for one in replace_list:
        print("replaceListInFile ==> ", one['file'], one['search'], one['replace'])
        replaceAllInFile(one['file'], one['search'], one['replace'])


def replaceAllInFile(file, searchExp, replaceExp):
    for line in fileinput.input(file, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        sys.stdout.write(line)


def download_and_untar(url, path):

    tmp = './tmp.tgz'
    try:
        os.remove(tmp)
    except OSError:
        pass

    print("..... download " + url)
    urllib.request.URLopener().retrieve(url, tmp)
    print("..... untar & unzip")
    call(['tar', '-zxf', tmp, '-C', path, '--strip-components=1'])
    os.remove(tmp)


def make_new_dir(newdir):
    print("... (Re)Create directory:s " + newdir)
    os.system('rm -R ' + newdir)
    os.makedirs(newdir)
    os.chmod(newdir, 0o777)  # TODO: change it
    # os.system('mkdir ' + newdir)  # TODO: change it
    # os.system('chmod -R 777 ' + newdir)  # TODO: change it


def stop_by_name(name):
    # stop old processes
    print("..... stop " + name)
    cmd = 'pgrep -f ' + name + ' | xargs kill -9'
    os.system(cmd)


def addToFile(file, addline):
    is_find = False
    with open(file) as f:
        content = f.readlines()
        for line in content:
            if addline in line:
                is_find = True

    if is_find:
        return

    with open(file, "a") as f:
        f.write(addline + "\n")


def install_mongodb():
    print("... Install MONGODB")

    try:
        call(['/etc/init.d/mongodb', 'stop'])
    except Exception:
        pass

    call(['apt', 'install', 'mongodb'])
    install_and_import('pymongo')
    replaceAllInFile(MONGODB_CONF, "bind_ip = 127.0.0.1", "#bind_ip = 127.0.0.1")


def start_mongodb():
    print("... Start MONGODB")

    call(['/etc/init.d/mongodb', 'stop'])
    call(['/etc/init.d/mongodb', 'start'])


def install_kafka():
    print("... Install KAFKA")

    stop_by_name(KAFKA_DIR)
    stop_by_name('kafka-server-start.sh')

    # install and start zookeeperd
    print("... Install zookeeper")
    try:
        call(['/etc/init.d/zookeeper', 'stop'])
    except Exception:
        pass

    call(['pip', 'install', '--upgrade', 'kafka-python'])
    call(['pip3', 'install', '--upgrade', 'kafka-python'])

    make_new_dir(KAFKA_DIR)
    download_and_untar(KAFKA_URL, KAFKA_DIR)

    replace_list = [
        {'file': KAFKA_START_SRCIPT, 'search': 'Xmx1G', 'replace': 'Xmx512M'},
        {'file': KAFKA_START_SRCIPT, 'search': 'Xms1G', 'replace':  'Xms512M'},
        {'file': KAFKA_CONF, 'search': 'socket.request.max.bytes=104857600', 'replace': 'socket.request.max.bytes=10485760'},
        {'file': KAFKA_CONF, 'search': 'socket.receive.buffer.bytes=102400', 'replace':  'socket.receive.buffer.bytes=51200'},
        {'file': KAFKA_CONF, 'search':  'socket.send.buffer.bytes=102400', 'replace':  'socket.send.buffer.bytes=51200'},
        {'file': KAFKA_CONF, 'search': 'num.io.threads=8', 'replace': 'num.io.threads=2'},
        {'file': KAFKA_CONF, 'search':  'num.network.threads=3', 'replace':   'num.network.threads=1'},
    ]
    replaceListInFile(replace_list)


def start_kafka():
    print("... START KAFKA")

    call(['/etc/init.d/zookeeper', 'stop'])
    call(['/etc/init.d/zookeeper', 'start'])
    time.sleep(5)

    print("... Start kafka")
    print(KAFKA_START_CMD)
    os.system(
        'sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &')
    # subprocess.Popen(KAFKA_START_CMD)
    time.sleep(5)
    print("... Create kafka topic: " + KAFKA_TOPIC)
    call(['./scripts/create_kafka_topic.py'])


def start_spark():
    stop_by_name('org.apache.spark.deploy')
    print(SPARK_DIR + 'sbin/start-master.sh',  '-h', SPARK_MASTER_NODA, '-p', SPARK_MASTER_PORT)
    print(SPARK_DIR + 'sbin/start-slave.sh',  'spark://' + SPARK_MASTER_NODA + ':' + SPARK_MASTER_PORT)

    call([SPARK_DIR + 'sbin/start-master.sh',  '-h', SPARK_MASTER_NODA, '-p', SPARK_MASTER_PORT])
    call([SPARK_DIR + 'sbin/start-slave.sh',  'spark://' + SPARK_MASTER_NODA + ':' + SPARK_MASTER_PORT])


def install_spark():
    print("... Install SPARK")

    # init git and scala
    call(['apt', 'install', 'git'])
    call(['apt', 'install', 'scala'])
    make_new_dir(SPARK_DIR)
    download_and_untar(SPARK_URL, SPARK_DIR)

    # Set environment variables
    addToFile(BASHRC_FILE, "export SPARK_HOME='" + SPARK_DIR + "'")
    addToFile(BASHRC_FILE, "export PATH=$SPARK_HOME/bin:$PATH")
    addToFile(BASHRC_FILE, "export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH")
    addToFile(
        BASHRC_FILE, "export PATH=$SPARK_HOME/bin:/usr/lib/jvm/java-8-openjdk-amd64:$PATH")
    addToFile(BASHRC_FILE, "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64")

    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_HOME"] = SPARK_DIR

    path = SPARK_DIR + "bin:/usr/lib/jvm/java-8-openjdk-amd64"
    if "PATH" in os.environ:
        path = path + ":" + os.environ["PATH"]

    os.environ["PATH"] = path

    pypath = SPARK_DIR + "python"
    if "PYTHONPATH" in os.environ:
        pypath = pypath + ":" + os.environ["PYTHONPATH"]

    os.environ["PYTHONPATH"] = pypath

    # Install python libs
    call(['pip3', 'install', '--upgrade', 'pyspark'])
    call(['pip3', 'install', '--upgrade', 'py4j'])


def install_sys():
    print("... Common instalation")

    # SYSTEM common case. may delete
    call(['apt', 'update'])
    call(['apt', 'upgrade'])

    # PIP3 common case. may delete
    call(['apt', 'install', 'python3-pip'])
    call(['pip3', 'install', '--upgrade', 'pip'])

    call(['apt', 'install', 'python3-kafka'])

    addToFile(BASHRC_FILE, "export python=/usr/bin/python3")

    for pk in python_package_install:
        install_and_import(pk)


def install_and_import(package):
    try:
        importlib.import_module(package)
        print(package, " is already installed")
    except ImportError:
        import pip
        print(" Start installing ", package)
        pip.main(['install', package])


init_params()

print('... install: %r' % is_install)
print('... start: %r' % is_start)
print('... ALL: %r' % is_all)
print('... mongodb: %r' % is_mongodb)
print('... kafka: %r' % is_kafka)
print('... spark: %r' % is_spark)


if not (is_mongodb or is_kafka or is_spark or is_all) and not (is_start or is_install):
    print('....... EMPTY !')
    sys.exit(0)

if is_install:
    install_sys()
    if is_mongodb or is_all:
        install_mongodb()
    if is_kafka or is_all:
        install_kafka()
    if is_spark or is_all:
        install_spark()

if is_start:
    if is_mongodb or is_all:
        start_mongodb()
    if is_kafka or is_all:
        start_kafka()
    if is_spark or is_all:
        start_spark()


'''
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list  
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823  
sudo apt-get update  
sudo apt-get install sbt  
'''
