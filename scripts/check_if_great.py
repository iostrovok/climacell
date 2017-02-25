#!/usr/bin/python3

# ./check_if_great.sh --cpu=100 --memory=30

import sys, getopt, platform, psutil, urllib.request


max_memory = 60.0
max_swap = 90.0
max_cpu = 60.0

def printf(format, *args):
    sys.stdout.write(format % args)

def init_params():
    global max_memory, max_cpu, max_swap
    opts = []
    args = []
    try:
        opts, args = getopt.getopt(sys.argv[1:],"ihi:o:",["cpu=","memory=","swap="])
    except getopt.GetoptError:
        print("check_if_great.py --cpu=100 --memory=30")
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('Use: check_if_great.py --cpu=100 --memory=30')
            sys.exit()
        elif opt == '-i':
            system_info()
            sys.exit()
        elif opt in ("-c", "-cpu", "--cpu"):
            max_cpu = float(arg)
        elif opt in ("-m", "-memory", "--memory"):
            max_memory = float(arg)
        elif opt in ("-s", "-swap", "--swap"):
            max_swap = float(arg)


def system_info():
    # CPU info
    printf("Processor %s. Loading: %.1f%%\n", platform.processor(), psutil.cpu_percent())
    printf("Count CPU/CPU-LOGICAL: %d/%d\n", psutil.cpu_count(logical=False), psutil.cpu_count(logical=True))
    # Memory info
    m = psutil.virtual_memory()
    printf("Memory. [%.1f%%] Total: %d, used: %d, available: %d\n", m.percent, m.total, m.used, m.available)

    # Swap info
    s = psutil.swap_memory()
    printf("Swap. [%.1f%%] Total: %d, used: %d, free: %d\n", s.percent, s.total, s.used,  s.free)

    # Net info
    n = psutil.net_io_counters(pernic=False)
    printf("Net. [errin/errout: %d/%d] bytes_sent: %d, bytes_recv: %d\n", n.errin, n.errout, n.bytes_sent, n.bytes_recv)


def send_info(cpu, memory):
    print(platform.processor())

def processor_info():
    global max_cpu 
    out = psutil.cpu_percent()
    return {'percent': out,'ok': max_cpu > out } 

def memory_info():
    global max_memory
    m = psutil.virtual_memory()
    out = m.percent
    return {'percent': out,'ok': max_memory > out } 

def swap_info():
    global max_swap
    m = psutil.swap_memory()
    out = m.percent
    return {'percent': out,'ok': max_swap > out } 


def net_info():
    global max_memory, max_cpu, max_swap
    # print('psutil.net_io_counters(pernic=True):', psutil.net_io_counters(pernic=True))
    print('----> ', psutil.net_io_counters(pernic=False))

def internet_on():
    try:
        urllib.request.urlopen('http://216.58.192.142', timeout=1)
        return { 'ok': True }
    except urllib.request.URLError as err:
        return { 'ok': False } 

init_params()

cpu = processor_info()
memory = memory_info()
swap = swap_info()
net = internet_on()




print("---------------")
print("cpu: ", cpu)
print("memory: ", memory)
print("swap: ", swap)
print("net: ", net)






