#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4


import datetime
import os
import stat
import sys
import threading
import urllib.request
from collections import deque
from queue import Queue
from config import RECORD_PERIOD, target_dir
import logging
import subprocess
#target_dir  = 'd:\quinta'
stations_debug = {'brklassik': 'http://streams.br-online.de/br-klassik_2.m3u',
                  'dlf': 'http://www.deutschlandradio.de/streaming/dlf.m3u',
                  }


#### Data provider thread ####

# Input         # Output queue      # output event #
# stations list # ffmpeg_queue      # ffmpeg_event #

def station_thread(station_name, station_url, next_queue):
    conn_dict = {}
    conn, extension = connect_to_station(station_url)
    if conn != None:
        name = station_name+extension
        logging.info('connection established')
        save_stream(name, conn, next_queue)


def connect_to_station(streamurl):
    # this part of code, that checks m3u playlist, was just copied from
    # https://github.com/beedaddy/radiorec/blob/master/radiorec.py
    if streamurl.endswith('.m3u'):
        logging.info('Seems to be an M3U playlist. Trying to parse...')
        try:
            with urllib.request.urlopen(streamurl) as remotefile:
                for line in remotefile:
                    if not line.decode('utf-8').startswith('#') and len(line) > 1:
                        tmpstr = line.decode('utf-8')
                        break
            streamurl = tmpstr
        except urllib.error.URLError:
            logging.error('Error during connection to %s', streamurl)
            return None, None

    # establich connection and check format of the stream
    try:
        conn = urllib.request.urlopen(streamurl)
    except urllib.error.URLError:
        logging.error('Error during connection to %s', streamurl)
        return None, None

    content_type = conn.getheader('Content-Type')
    stream_type = ''
    if content_type == 'audio/mpeg':
        stream_type = '.mp3'
    elif content_type == 'application/ogg' or content_type == 'audio/ogg' or content_type == 'application/octet-stream':
        stream_type = '.ogg'
    elif content_type == 'audio/x-mpegurl':
        logging.info('Sorry, M3U playlists are currently not supported')
        sys.exit()
    else:
        logging.info('Unknown content type "' +
                     content_type + '". Assuming mp3.')
        stream_type = '.mp3'
    logging.info('Succesessfull connection to %s', streamurl)
    return conn, stream_type


def save_stream(station_name, conn, next_queue):
    # stub for threading.Timer callback
    def stub(): return 0 == 0
    while 1:
        # files_dict contains mapping {tcp connection object} -> {file descriptor object}
        # and the code below saves stream to the corresp. file
        cur_dt_string = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        filename = target_dir + os.sep + cur_dt_string + "__" + station_name

        logging.debug('start of record')
        mp3file = open(filename, 'wb')
        timer = threading.Timer(RECORD_PERIOD, stub)
        timer.start()
        # copy stream from tcp to file
        while timer.is_alive() and not conn.closed:
            buf = conn.read(1024)
            if len(buf) < 1024:
                logging.error("Error: from url {} read {} bytes,\
                        but expected {}".format('', len(buf), 1024))
            readn = mp3file.write(buf)
            if readn < 1024:
                logging.error(
                    "Error: {} bytes written, but expected {}".format('', readn, 1024))
        mp3file.close()
        next_queue.put(filename)
################# end of data provider thread ############

# Output queue      # output event #
# ffmpeg_queue      # ffmpeg_event #


#### ffmpeg thread ####

# Input queue   # input event       # Output queue      # output event #
# ffmpeg_queue  # ffmpeg_event      # hasher_queue      # hasher_event #


def ffmpeg_thread(queue, next_queue):
    while 1:
        event.wait()
        event.clear()
        while len(queue) > 0:
            infilename = queue.get()
            logging.info(infilename)
            outfilename = infilename[:-3] + 'raw'
            os.mkfifo(outfilename, 0o600)
            logging.info(outfilename)
            if os.fork() == 0:
                os.execlp('ffmpeg','ffmpeg',
                                '-i', infilename,  # input filename
                                '-ac', '1',          # number of channels
                                '-ar', '11025',      # frequncy of output signal
                                '-f', 'f32le',       # format of output signal - floating p
                                outfilename
                                # ,'-loglevel','quiet'
                                )   # remove all output from ffmpeg itself
            if next_queue != None:
                next_queue.put(outfilename)
#### end of ffmpeg thread ####

# Output queue      # output event #
# hasher_queue      # hasher_event #


def main():
    if len(sys.argv) < 3:
        print("Please specify station name and url: radirec2.py name url")
        return
    station_name = sys.argv[1]
    station_url = sys.argv[2]
    t = threading.Thread(target=station_thread, args=(
        station_name, station_url, ffmpeg_queue, ffmpeg_event))
    t.start()
    t = threading.Thread(target=ffmpeg_thread, args=(
        ffmpeg_queue, ffmpeg_event, None, None))
    t.start()


if __name__ == '__main__':
    main()
