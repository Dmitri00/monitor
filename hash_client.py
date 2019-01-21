############ IMPORTS ############
# container for thread's input and output queue
from collections import deque
# for running threads in main for test
import threading
# parse byte array 
import struct
# calculate echologging.info hash of audiosample
import echologging.info
# for managing HTTP Requests
import urllib,urllib.request
import json
import sqlite3
from data_requests import *
import os
import os.path

from config import *
import logging
#### Hasher thread ####

#----------------------------------------------------------------------#
# Input queue   # input event       # Output queue      # output event #
#----------------------------------------------------------------------#
# hasher_queue  # hasher_event      # client_queue      # client_event #        
#raw audio file #                   # bytes of hash
#----------------------------------------------------------------------#

def hash_thread(queue, event, next_queue, next_event):
   """thread hash function"""
   operation_num=0
   # hash thread reads files, made by ffmpeg.
   # python ffmpeg thread executes ffmpeg and add ffmpeg output file
   # to the hash buffer. A confilict can occure: filename is
   # in hash_thread buffer, but it is not physically written by ffmpeg
   # So hash thread will process files from previous period of observation.
   # In order to do that, additional event.wait() and tracking of new files
   # will be used
   logging.info('hasher: waiting for event')
#   event.wait()
#   event.clear()
#   ready_files = len(queue)
   while True:
       logging.info('hasher: waiting for event')
       event.wait()
       event.clear()
       ready_files = len(queue)
       # process that filenames, that was added by ffmpeg thread at
       # previous period
       logging.info("ready_files =",ready_files)
       for _ in range(ready_files):
           rawaudio_filename = queue.popleft()
           data=getter_sound(rawaudio_filename)
           if next_queue!=None:
               next_queue.append((data,rawaudio_filename))
               logging.info('hasher: data appended')
           logging.info('Complited hash operation number {0}'.format(operation_num))
           logging.info(rawaudio_filename)
           # it is time to delete mp3 and raw files of the recorded audio fragment:
           os.remove(rawaudio_filename)
           mp3_file = rawaudio_filename[:-3] + 'mp3'
           os.remove(mp3_file)
       if next_event!=None:
           next_event.set()
           logging.info('hasher: event set')
       # rest of the files was added on current time interval atmost 
       # and should be ready in the next period (if there are not too much 
       # stations)
       ready_files = len(queue) 

def getter_sound(filename):

    logging.info( u'Start record')
    

    samples = []

    with open(filename, 'rb') as f:
        buf = f.read(512)
        while len(buf) == 512:
            samples.extend(struct.unpack('128f',buf))
            buf = f.read(512)
        if len(buf) > 0:
            fmt = '{}f'.format(len(buf)//4)
            samples.extend(struct.unpack(fmt,buf))
            
    d = echologging.info.codegen(samples, 0)
    logging.info( u'Complited.')
    return d['code']
#### End of hasher thread ####

# Output queue      # output event #
#client_queue      # client_event #        




#### Client thread ####

#----------------------------------------------------------------------#
# Input queue   # input event       # Output queue      # output event #
#----------------------------------------------------------------------#
# client_queue  # client_event      # None              # None         #        
# bytes of hash #                 #                   #
#----------------------------------------------------------------------#
import numpy as np
def client_thread(queue,event,next_queue,next_event):
    time_start = ''
    time_end = ''
    index_prev = None
    """thread client function"""
    #if not os.paggh.exists(target_dir):
    #    os.mkdir(target_dir)
    while True:
        logging.info('client: waiting for event')
        event.wait()
        event.clear()
        while len(queue)>0:
            #Get values from queue
            (track_hash,filename)= queue.popleft()
            logging.info('client: event occured, queue len=',len(queue))

            try:
                ############ http request for recognition ############
                response = echologging.info_recognize(track_hash)
                response = json.loads(response)['results']
                ############ end http request for recognition ########
                logging.info('Client: ',response)
                
                #### Find recognized track as score outlier #########
                best_match = None
                if len(response) == 0:
                    pass
                elif len(response) == 1:
                    best_match = response[0]
                    if best_match['score'] < 100:
                        best_match['index'] = -1
                else:
                    # get index of biggest outlier in set of scores
                    # Algorithm for finding outliers - Z-score
                    scores = np.array(list(map(lambda x: x['score'],response)))
                    outlier_index = find_outlier(scores)
                    logging.info("outlier:",outlier_index)
                    if outlier_index >= 0:
                        best_match = response[outlier_index]
                    else:
                        best_match = {'index':-1}

                ############ Save info to log if outliers was found
                # Format is: time, station,score,artist,title
                flag_need_save = False
                if index_prev != best_match['index']:
                    ############## Extract timestamp and station name from filename ###########
                    # filename is a full filename of raw audio fragemnt, 
                    # 1. Extract actual name of the file (withot derectories)
                    filename_splitted = filename.rsplit('/',1)[-1]
                    # 2. Remove extension part of filename
                    filename_splitted = filename_splitted.split('.')[0]
                    # now filename contains timestamp and name of the station:
                    # timestamp_station
                    filename_splitted = filename_splitted.rsplit('_',1)
                    stamp = filename_splitted[0].replace('_',' ')
                    ############## Timestamp is extracted #########

                    # extract station name from filename
                    station = filename_splitted[1]
                    
                    end_stamp = stamp

                    if index_prev != None and index_prev != -1:
                        logging.info("Track is finished")
                        #open_mode = 'a' if os.path.exists(csv_file) else 'w'
                        accident = [None,station,index_prev,start_stamp,end_stamp]
                        db_accident_insert(accident)
                            #Metadata and timestamp was saved to log ##############
                    if best_match['index'] != -1:
                        logging.info("OUTLIER FOUND!")
                        start_stamp = stamp
                        logging.info("Track is started")
                else:# at previous window track was the same
                    pass
                index_prev = best_match['index']
                logging.info("client:end of cycle")



            except urllib.error.URLError as e:
                logging.error('Exception in thread',threading.current_thread().getName(),
                        str(e))

# This function tries to find outliers
# with Z-score mathod:
# Mark as outliers those points,
# whose distance to mean is much bigger, than average distance
# returns: index of the biggest outlier in array,
# or -1 if such doesn't exist
def find_outlier(points):
        if len(points.shape) == 1:
            points = points[:,None]
        if np.max(points) - np.min(points) < 50:
            return -1
        max_outlier = -1
        max_outlier_ind = -1
        q25 = np.quantile(points,0.25)
        q75 = np.quantile(points,0.75)
        dif = q75-q25
        score_min = q25-1.5*dif
        score_max = q75+1.5*dif
        for i, score  in enumerate(points):
            if score > score_max and score > max_outlier:
                max_outlier = score
                max_outlier_ind = i
        return max_outlier_ind
#End of client thread ####

# Output queue      # output event #
# None              # None         #        


if __name__ == '__main__':
    threads = []
    hash_event = threading.Event()       
    hash_queue = deque()
    hash_event.set()
    dir_name = '/home/dmitri/quinta-v2/samples_d'
    i = 0;
    for name in os.listdir(dir_name):
        if name.split('.')[-1]=='raw':
            if i > 40:
                break
            i+=1
            logging.info(name)
            hash_queue.append(os.path.join(dir_name,name))
    client_event = threading.Event()       
    client_event.clear()
    client_queue = deque()
    t = threading.Thread(name='Hasher',target=hash_thread, args=(hash_queue, hash_event,
        client_queue, client_event))
    t.start()
    t = threading.Thread(name='Client',target=client_thread, args=(client_queue,client_event,
        None, None))
    t.start()
    hash_event.clear()
    hash_event.set()
    
