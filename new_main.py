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






if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify station name and url: radirec2.py name");
        sys.exit(0);
    station_name = sys.argv[1]
    station_url = db_stationurl_get_by_name(station_name)
    if station_url == None:
        print('Station with name %s doesn\'t exist' % staion_name)
    station_url = station_url[0][0]
    print(station_url)


    
    # ask unix core to call wait of child zombies (ffmpeg) autoatically
    # ffmpeg_thread creates childs for running ffmpeg
    # it is possible to place signal handler in main thread
    signal.signal(signal.SIGCHLD,signal.SIG_IGN)
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
