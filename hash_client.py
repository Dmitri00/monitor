############ IMPORTS ############
# container for thread's input and output queue
from collections import deque
# for running threads in main for test
import threading
# parse byte array 
import struct
# calculate echoprint hash of audiosample
import echoprint
# for managing HTTP Requests
import urllib,urllib.request
import json

import os.path

############ DEFINED CONSTANTS ###########
OUTLIER_THRESHOLD = 5
query_url = 'http://echoprint.music365.pro:5678'
recognize_method_name = 'query/set_int'
index_url = 'http://echoprint.music365.pro:5000'
index_method_name = 'tracks'
csv_file = 'report.csv'
report_header = 'datetime,station,score,index\n'
report_format = '{},{},{},{} - {}\n'

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
   event.wait()
   event.clear()
   ready_files = len(queue)
   while True:
       event.wait()
       event.clear()
       # process that filenames, that was added by ffmpeg thread at
       # previous period
       for _ in range(ready_files):
           rawaudio_filename = queue.popleft()
           data=getter_sound(rawaudio_filename)
           if next_queue!=None:
               next_queue.append((data,rawaudio_filename))
               print('hasher: data appended')
           print('Complited hash operation number {0}'.format(operation_num))
           print(rawaudio_filename)
       if next_event!=None:
           next_event.set()
           print('hasher: event set')
       # rest of the files was added on current time interval atmost 
       # and should be ready in the next period (if there are not too much 
       # stations)
       ready_files = len(queue) 

def getter_sound(filename):

    print( u'Start record')
    

    samples = []

    with open(filename, 'rb') as f:
        buf = f.read(512)
        while len(buf) == 512:
            samples.extend(struct.unpack('128f',buf))
            buf = f.read(512)
        if len(buf) > 0:
            fmt = '{}f'.format(len(buf)//4)
            samples.extend(struct.unpack(fmt,buf))
            
    d = echoprint.codegen(samples, 0)
    print( u'Complited.')
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
    """thread client function"""
    #if not os.paggh.exists(target_dir):
    #    os.mkdir(target_dir)

    while True:
        print('client: waiting for event')
        event.wait()
        event.clear()
        while len(queue)>0:
            print('client: event occured')
            #Get values from queue
            (track_hash,filename)= queue.popleft()

            try:
                ############ http request for recognition ############
                url = query_url+ '/' + recognize_method_name
                params = 'echoprint='+track_hash
                response = urllib.request.urlopen(url, data=params.encode('ascii')).read().decode('ascii')
                response = json.loads(response)['result']
                ############ end http request for recognition ########
                print('Client: ',response)
                
                ############ Find recognized track as score outlier #########
                best_match = None
                if len(scores) == 0:
                    pass
                elif len(scores) == 1:
                    best_match = response[0]
                else:
                    scores = np.array(list(map(lambda x: return x['score'],response)))
                    # get index of biggest outlier in set of scores
                    # Algorithm for finding outliers - Z-score
                    outlier_index = mad_based_outlier(scores,OUTLIER_THRESHOLD)
                    if outlier_index >= 0:
                        best_match = response[outlier_index]

                ############ Save info to log if outliers was found
                # Format is: time, station,score,artist,title
                if best_match != None:
                    ############## Extract timestamp and station name form filename ###########
                    # filename is a full filename of raw audio fragemnt, 
                    # 1. Extract actual name of the file (withot derectories)
                    filename_splitted = filename.rsplit('/',1)[-1]
                    # 2. Remove extension part of filename
                    filename_splitted = filename_splitted.split('.')[0]
                    # now filename contains timestamp and name of the station:
                    # timestamp_station
                    filename_splitted = filename_splitted.rsplit('_',1)
                    stamp = filename_splitted[0]
                    ############## Timestamp is extracted #########

                    # extract station name from filename
                    station = filename_splitted[1]
                    
                    ############### HTTP request for track metadata(artist and title) by id ##########
                    url = index_url + '/' + index_method_name
                    params = 'id='+str(best_match['id'])
                    response = urllib.request.urlopen(url, data=params.encode('ascii')).read().decode('ascii')
                    response = json.loads(response)['result'][0]
                    artist = response['metadata']['artist']
                    title = response['metadata']['title']
                    ############### Metadata received #############

                    open_mode = 'a' if os.path.exists(csv_file) else 'w'
                    with open(csv_file, open_mode) as freport:
                        freport.write(report_format.format(stamp,station,best_match['score'],artist, title))
                    #Metadata and timestamp was saved to log ##############

                    print('Complited dispatch data number')
            except urllib.error.URLError as e:
                #print('Exception in thread',threading.current_thread.getName(),
                #        str(e))
                queue.append((track_hash,filename))

# This function tries to find outliers
# with Z-score mathod:
# Mark as outliers those points,
# whose distance to mean is much bigger, than average distance
# returns: index of the biggest outlier in array,
# or -1 if such doesn't exist
def mad_based_outlier(points, thresh=3.5):
        if len(points.shape) == 1:
            points = points[:,None]
        max_outlier = -1
        max_outlier_ind = -1
        median = np.median(points, axis=0)
        diff = np.sum((points - median)**2, axis=-1)
        diff = np.sqrt(diff)
        med_abs_deviation = np.median(diff)
        for i, diff_i in enumerate(diff):
            modified_z_score = 0.6745 * diff_i / med_abs_deviation
            if modified_z_score > thresh:
                if modified_z_score > max_outlier:
                    max_outlier = modified_z_score
                    max_outlier_ind = i
        return max_outlier_ind
#End of client thread ####

# Output queue      # output event #
# None              # None         #        


if __name__ == '__main__':
    threads = []
    hash_event = threading.Event()       
    hash_event.set()
    hash_queue = deque()
    hash_queue.append('/home/dmitri/quinta/fifos/2018-11-12T09_16_07_dlf.raw')
    client_event = threading.Event()       
    client_event.clear()
    client_queue = deque()
    t = threading.Thread(name='Hasher',target=hash_thread, args=(hash_queue, hash_event,
        client_queue, client_event))
    t.start()
    t = threading.Thread(name='Client',target=client_thread, args=(client_queue,client_event,
        None, None))
    t.start()
