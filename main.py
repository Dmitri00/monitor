import time
import threading
import os
from collections import deque
from hash_client import hash_thread, client_thread
from radiorec2 import station_thread, ffmpeg_thread, stations_debug
import signal






if __name__ == '__main__':

    
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

    t = threading.Thread(target=station_thread, args=(stations_debug,
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
