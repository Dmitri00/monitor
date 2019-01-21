#!/usr/bin/python3
import time
import threading
import os
import sys
from collections import deque
from hash_client import hash_thread, client_thread
from radiorec2 import station_thread, ffmpeg_thread, stations_debug
import signal
from data_requests import db_stationurl_get_by_name
from config import log_file
import logging

def unlock_daemon(lock_file):
    try:
        os.unlink(lock_file)
    except OSError e:
            logging.error("os.unlink failed: %d (%s)\nUnable to remove daemon\s lock" % (e.errno, e.strerror))
            os.exit(1)

def daemonize(station_name):
    try:
        lock_file = '/tmp/' + station_name
        os.is_file(lock_file):
            logging.info('Daemon for station %s is already running. Closing this instance.')
            sys.exit(1)
        file(lock_file,'r').close()
        atexit.register(lambda x: unlock_daemon(lock_file))
    except OSError e:
            logging.error("os.is_file failed: %d (%s)\nUnable to run station\'s daemon" % (e.errno, e.strerror))
            sys.exit(1)
    try: 
        pid = os.fork() 
        if pid > 0:
            # exit first parent
            sys.exit(0) 
    except OSError, e: 
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
    except OSError, e: 
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
    os.open('/dev/null',os.O_WRONLY)

    # set handlers for SIG_CHLD, SIG_HUP
    
    signal.signal(signal.SIGCHLD,signal.SIG_IGN)
    signal.signal(signal.SIGHUP,signal.SIG_IGN)

    # write pidfile




if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify station name and url: radirec2.py name");
        sys.exit(0);
    station_name = sys.argv[1]
    station_url = db_stationurl_get_by_name(station_name)
    if station_url == None:
        print('Station with name %s doesn\'t exist' % staion_name)
    logging.basicConfig(filename=log_file,format='%(asctime)s %(threadName)s : %(levelno)s  %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

    station_url = station_url[0][0]
    logging.info('Попытка подключения к %s ' % station_url)
    daemonize(station_name)


    
    # ask unix core to call wait of child zombies (ffmpeg) autoatically
    # ffmpeg_thread creates childs for running ffmpeg
    # it is possible to place signal handler in main thread
    ffmpeg_event = threading.Event()       
    ffmpeg_event.clear()
    ffmpeg_queue = deque()

    hash_event = threading.Event()       
    hash_event.clear()
    hash_queue = deque()

    client_event = threading.Event()       
    client_event.clear()
    client_queue = deque()

    t = threading.Thread(target=station_thread, args=(station_name,station_url,
        ffmpeg_queue,ffmpeg_event))
    t.start()
    
    t = threading.Thread(target=ffmpeg_thread, args=(ffmpeg_queue, ffmpeg_event, 
        hash_queue, hash_event))
    t.start()

    t = threading.Thread(name='Hasher',target=hash_thread, args=(hash_queue, hash_event,
        client_queue, client_event))
    t.start()

    t = threading.Thread(name='Client',target=client_thread, args=(client_queue,client_event,
        None, None))
    t.start()
