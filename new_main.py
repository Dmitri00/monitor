#!/usr/bin/python3
import time
import threading
import os
import sys
from collections import deque
from queue import Queue
from hash_client import hash_thread, client_thread
from radiorec2 import station_thread, ffmpeg_thread, stations_debug
import signal
from data_requests import db_stationurl_get_by_name
from config import log_file,lock_file
import logging
import atexit

def sigusr1_handler(a,b):
    logging.info("SIGUSR1 was catched - exitig")
    sys.exit(0)

def unlock_daemon(lock_file):
    logging.info("Freeing lock at %s"%  lock_file)
    try:
        os.unlink(lock_file)
    except OSError as e:
        sys.stderr.write("errno=%d (%s)\n" % (e.errno, e.strerror))

def daemonize(station_id):
    try: 
        pid = os.fork() 
        if pid > 0:
            # exit first parent
            sys.exit(0) 
    except OSError as e: 
        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)
    
    # decouple from parent environment
    os.chdir("/") 
    os.setsid() 
    os.umask(0) 

    # do second fork
    try: 
        pid = os.fork() 
        if pid > 0:
            # exit from second parent
            sys.exit(0) 
    except OSError as e: 
        sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1) 
       

    # redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    os.close(sys.stdin.fileno())
    os.open('/dev/null',os.O_RDONLY)
    os.close(sys.stdout.fileno())
    os.open('/dev/null',os.O_WRONLY)
    os.close(sys.stderr.fileno())
    os.open('/home/dmitri/quinta_error',os.O_WRONLY|os.O_CREAT,0o644)

    # set handlers for SIG_CHLD, SIG_HUP
    
    signal.signal(signal.SIGCHLD,signal.SIG_IGN)
    signal.signal(signal.SIGHUP,signal.SIG_IGN)

    # write pidfile
    lock_file = '/home/dmitri/'+station_id
    logging.debug("Lock file = %s" % lock_file)
    try:
        lockfd = os.open(lock_file, os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
        os.write(lockfd,bytes(str(os.getpid()),encoding='utf-8'))
        os.close(lockfd)
    except OSError as e:
        logging.error('Daemon for station %s is already running. Closing this instance.'%station_id)
        sys.stderr.write("lock failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)
    signal.signal(signal.SIGUSR1,sigusr1_handler)
    atexit.register(lambda : unlock_daemon(lock_file))




if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify station name and url: radirec2.py name");
        sys.exit(0);
    station_name = sys.argv[1]
    station = db_stationurl_get_by_name(station_name)
    if station == None:
        print('Station with name %s doesn\'t exist' % staion_name)
    logging.basicConfig(filename=log_file,format='%(asctime)s %(threadName)s : %(levelno)s  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

    station_url = station[0][1]
    station_id = station[0][0]
    logging.info('Попытка подключения к %s, %s ' % (station_url, station_id))
    daemonize(str(station_id))


    
    # ask unix core to call wait of child zombies (ffmpeg) autoatically
    # ffmpeg_thread creates childs for running ffmpeg
    # it is possible to place signal handler in main thread

    hash_queue = Queue()

    client_queue = Queue()

    t = threading.Thread(target=station_thread, args=(station_name,station_url,hash_queue))
    t.start()
    

    t = threading.Thread(name='Hasher',target=hash_thread, args=(hash_queue, client_queue))
    #t = threading.Thread(name='Hasher',target=hash_thread, args=(hash_queue, None))
    t.start()

    t = threading.Thread(name='Client',target=client_thread, args=(client_queue, None))
    t.start()
