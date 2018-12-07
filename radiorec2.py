#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4


import datetime
import os
import stat
import sys
import threading
import urllib.request
from collections import deque

REMOVE_MP3 = False
RECORD_PERIOD = 30
#target_dir = '/home/dmitri/quinta/fifos'
target_dir  = 'd:\quinta'
stations  = {'brklassik':'http://streams.br-online.de/br-klassik_2.m3u',
            'dkultur': 'http://www.deutschlandradio.de/streaming/dkultur.m3u',
            'dlf':'http://www.deutschlandradio.de/streaming/dlf.m3u',
            'dwissen':'http://dradio_mp3_dwissen_m.akacast.akamaistream.net/7/728/142684/v1/gnl.akacast.akamaistream.net/dradio_mp3_dwissen_m',
            'erfplus': 'http://c14000-l.i.core.cdn.streamfarm.net/14000cina/live/3212erf_96/live_de_96.mp3',
            'mdrklassik': 'http://avw.mdr.de/livestreams/mdr_klassik_live_128.m3u',
            'radioeins': 'http://www.radioeins.de/live.m3u',
            'swr2': 'http://mp3-live.swr.de/swr2_m.m3u',
            'wdr3': 'http://www.wdr.de/wdrlive/media/mp3/wdr3_hq.m3u'
            }
stations_debug  = {'brklassik':'http://streams.br-online.de/br-klassik_2.m3u',
            'dlf':'http://www.deutschlandradio.de/streaming/dlf.m3u',
            }



#### Data provider thread ####

# Input         # Output queue      # output event #
# stations list # ffmpeg_queue      # ffmpeg_event #

def station_thread(station_name,station_url, next_queue, next_event):
    conn_dict  = {}
    conn,extension = connect_to_station(station_url)
    if conn != None:
        name = station_name+extension
        print('connection established')    
        save_stream(name, conn, next_queue, next_event)
    
def connect_to_station(streamurl):
    # this part of code, that checks m3u playlist, was just copied from 
    # https://github.com/beedaddy/radiorec/blob/master/radiorec.py
    if streamurl.endswith('.m3u'):
        print('Seems to be an M3U playlist. Trying to parse...')
        try:
            with urllib.request.urlopen(streamurl) as remotefile:
                for line in remotefile:
                    if not line.decode('utf-8').startswith('#') and len(line) > 1:
                        tmpstr = line.decode('utf-8')
                        break
            streamurl = tmpstr
        except urllib.error.URLError:
            print('Error during connection to ',streamurl)
            return None,None
    
    # establich connection and check format of the stream
    try:
        conn = urllib.request.urlopen(streamurl)
    except urllib.error.URLError:
        print('Error during connection to ',streamurl)
        return None,None
        
    content_type = conn.getheader('Content-Type')
    stream_type = ''
    if(content_type == 'audio/mpeg'):
        stream_type = '.mp3'
    elif(content_type == 'application/ogg' or content_type == 'audio/ogg'):
        stream_type = '.ogg'
    elif(content_type == 'audio/x-mpegurl'):
        print('Sorry, M3U playlists are currently not supported')
        sys.exit()
    else:
        print('Unknown content type "' + content_type + '". Assuming mp3.')
        stream_type = '.mp3'
    print('Succesessfull connection to ',streamurl)
    return conn, stream_type

def save_stream(station_name, conn, next_queue, next_event):
    # stub for threading.Timer callback
    stub = lambda : 0==0
    while 1:
        # files_dict contains mapping {tcp connection object} -> {file descriptor object}
        # and the code below saves stream to the corresp. file
        cur_dt_string = datetime.datetime.now().strftime('%Y-%m-%dT%H_%M_%S')
        filename = target_dir + os.sep + cur_dt_string + "_" + station_name
        mp3file = open(filename,'wb')
        timer = threading.Timer(RECORD_PERIOD,stub)
        timer.start()
        # copy stream from tcp to file
        while timer.is_alive() and not conn.closed:
            buf = conn.read(1024)
            if len(buf) < 1024:
                print("Error: from url {} read {} bytes,\
                        but expected {}".format('',len(buf),1024))
            readn = mp3file.write(buf)
            if readn< 1024:
                print("Error: {} bytes written, but expected {}".format('',readn,1024))
        mp3file.close()
        next_queue.append(mp3file)
        # Signal handler of the next_queue
        next_event.set()
################# end of data provider thread ############

# Output queue      # output event #
# ffmpeg_queue      # ffmpeg_event #




#### ffmpeg thread ####

# Input queue   # input event       # Output queue      # output event #
# ffmpeg_queue  # ffmpeg_event      # hasher_queue      # hasher_event #        


ffmpeg_event = threading.Event()       
ffmpeg_event.clear()
ffmpeg_queue = deque()

def ffmpeg_thread(queue,event,next_queue, next_event):
    while 1:
        event.wait()
        event.clear()
        while len(queue) > 0:
            infilename = queue.popleft()
            outfilename = infilename[:-3] + 'raw'
            if os.fork() == 0:
                os.execlp('ffmpeg','ffmpeg',
                        '-i',infilename,    #input filename
                        '-ac','1',          # number of channels
                        '-ar','11025',      # frequncy of output signal
                        '-f','f32le',       # format of output signal - floating p
                        outfilename,
                        '-loglevel','quiet')   # remove all output from ffmpeg itself
            if next_queue!=None:
                next_queue.append(outfilename)
            if REMOVE_MP3:
                os.remove(infilename)
        if next_event!=None:
            next_event.set()
#### end of ffmpeg thread ####

# Output queue      # output event #
# hasher_queue      # hasher_event #
        




def main():
    t = threading.Thread(target=station_thread, args=('dlf',stations_debug['dlf'], ffmpeg_queue,ffmpeg_event))
    t.start()
    #t = threading.Thread(target=ffmpeg_thread, args=(ffmpeg_queue, stream_files_ready,None, None))
    #t.start()

    

if __name__ == '__main__':
    main()
