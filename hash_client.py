import time
#Protobuf sector
from collections import deque
import time
import struct
import echoprint
import urllib,urllib.request
import json




#Service sector

import os.path


import threading


url_base = 'http://echoprint.music365.pro:5678/query/set_int'
csv_file = 'report.csv'
report_header = 'datetime,station,score,index\n'
report_format = '{},{},{},{}\n'

#### Hasher thread ####

# Input queue   # input event       # Output queue      # output event #
# hasher_queue  # hasher_event      # client_queue      # client_event #        

def hash_thread(queue, event, next_queue, next_event):
   """thread hash function"""
   operation_num=0
   # hash thread reads files, made by ffmpeg.
   # python ffmpeg thread executes ffmpeg and add ffmpeg outpuut
   # file to the hash buffer. A confilict can occure: filename is
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

# Input queue   # input event       # Output queue      # output event #
# client_queue  # client_event      # None              # None         #        
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

            params = 'echoprint='+track_hash
            try:
                response = urllib.request.urlopen(url_base, data=params.encode('ascii')).read().decode('ascii')
                response = json.loads(response)['results']
                print('Client: ',response)
                best_match = max(response,key=lambda x:x['score'])
                print(best_match)
               
                # filename is a full filename of raw audio fragemnt, 
                # extract from it name of the file without extension
               
                filename_splitted =filename.rsplit('/',1)[-1].split('.')[0]
                # now filename contains timestamp as string and name of the station:
                # timestamp_station
                filename_splitted = filename_splitted.rsplit('_',1)
                stamp = filename_splitted[0]
                station = filename_splitted[1]
                open_mode = 'a' if os.path.exists(csv_file) else 'w'

                with open(csv_file, open_mode) as freport:
                    freport.write(report_format.format(stamp,station,best_match['score'],best_match['index']))
                print('Complited dispatch data number')
            except urllib.error.URLError as e:
                #print('Exception in thread',threading.current_thread.getName(),
                #        str(e))
                queue.append((track_hash,filename))

#### End of client thread ####

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
