############ IMPORTS ############
# container for thread's input and output queue
from collections import deque
from queue import Queue
# for running threads in main for test
import threading
# parse byte array
import struct
# calculate echologging.info hash of audiosample
# for managing HTTP Requests
import urllib
import urllib.request
import json
import os
import os.path
import logging
import subprocess
import re

from data_requests import echoprint_recognize, db_accident_insert
import numpy as np
from config import echoprint_codegen_path, filename_rgexp, REMOVE_MP3, minimal_track_len, OUTLIER_THRESHOLD

#### Hasher thread ####

#----------------------------------------------------------------------#
# Input queue   # input event       # Output queue      # output event #
#----------------------------------------------------------------------#
# hasher_queue  # hasher_event      # client_queue      # client_event #
# raw audio file #                   # bytes of hash
#----------------------------------------------------------------------#


def hash_thread(queue, next_queue):
    """thread hash function"""
    while True:
        logging.debug('hasher: waiting for event')
        audio_filename = queue.get()
        echoprint_process = subprocess.run([echoprint_codegen_path,audio_filename], stdout=subprocess.PIPE)
        try:
            track_hash = json.loads(echoprint_process.stdout)[0]['code']
            if next_queue != None:
                next_queue.put((track_hash,audio_filename))
        except KeyError:
            logging.error('Модуль echoprint-codegen возвратил json без поля code. Файл %s' % audio_filename)



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
def client_thread(queue, next_queue):
    """thread client function"""
    filename_re = re.compile(filename_rgexp)
    recognized_files = deque()
    curr_track = None
    track_len = 0
    time_start = ''
    time_end = ''
    while True:
        logging.debug('client: waiting for event')
            # Get values from queue
        (track_hash, filename) = queue.get()
        logging.debug('client: event occured')
        need_save_track = False
        try:
            ############ http request for recognition ############
            best_match = get_bestMatch(track_hash)
            assert best_match != None

            # Save info to log if outliers was found
            # Format is: time, station,score,artist,title

            # now we need to figure out whatever we need to store info about track into db

            # if track in the current window the same as in previous, than just mark it and skip
            if curr_track == best_match['index']:
                if curr_track != -1:
                    track_len += 1
                    recognized_files.append(filename)
            else:
                # now we know, that atleast something new happened
                # it could be an initial state of the algorithm, end of speech or of a track
                # So, station name and time stamp should be parsed
                filename_match = filename_re.search(filename)
                stamp = filename_match['timestamp'].replace('_', ' ')
                station = filename_match['station_name']

                # Figuring out, what to do with previously recognized track
                #if it is the initial state (no tracks were recognized in the past)
                # then there is nothing to do at this step
                if track_len == 0:
                    pass
                #else if it is not and initial state and previous track was not a speech
                #additionally check, that previous track was being observed long enough to be confident
                # then save info to db, clear track length counter
                elif curr_track >= 0:
                    if REMOVE_MP3:
                        for track in recognized_files:
                            os.unlink(track)
                    recognized_files.clear()
                    if track_len >= minimal_track_len:
                        end_stamp = stamp
                        accident = [None, station, index_prev,
                                    start_stamp, end_stamp]
                        db_accident_insert(accident)
                        logging.info("Track is finished")
                    track_len = 0
                #finally, if current index is not -1 (not a speech or unrecognized)
                # then mark timestamp as beginning of the new track and initialize track length
                if best_match['index'] != -1:
                    start_stamp = stamp
                    curr_track = best_match['index']
                    track_len = 1
                    recognized_files.append(filename)
                    logging.info("Track is started")
                else:
                    curr_track = -1
                    track_len = 0

        except urllib.error.URLError as e:
            logging.error('Exception %s' % str(e))


# This function tries to find outliers
# with Z-score mathod:
# Mark as outliers those points,
# whose distance to mean is much bigger, than average distance
# returns: index of the biggest outlier in array,
# or -1 if such doesn't exist


def find_outlier(points):
    if len(points.shape) == 1:
        points = points[:, None]
    if np.max(points) - np.min(points) < 3:
        return -1
    max_outlier = -1
    max_outlier_ind = -1
    q25 = np.quantile(points, 0.25)
    q75 = np.quantile(points, 0.75)
    dif = q75-q25
    score_min = q25-OUTLIER_THRESHOLD*dif
    score_max = q75+OUTLIER_THRESHOLD*dif
    for i, score in enumerate(points):
        if score > score_max and score > max_outlier:
            max_outlier = score
            max_outlier_ind = i
    return max_outlier_ind


def get_bestMatch(track_hash):
    response = echoprint_recognize(track_hash)
    response = json.loads(response)['results']
    ############ end http request for recognition ########
    logging.debug('Client:%s ' % response)

    #### Find recognized track as score outlier #########
    best_match = None
    if len(response) < 4:
        logging.error("Too few scores were received from echoprint")
        sys.exit(1)
    else:
        # get index of biggest outlier in set of scores
        # Algorithm for finding outliers - Z-score
        scores = np.array(list(map(lambda x: x['score'], response)))
        outlier_index = find_outlier(scores)
        logging.debug("outlier:%s" % outlier_index)
        if outlier_index >= 0:
            best_match = response[outlier_index]
        else:
            best_match = {'index': -1}

    return best_match

#End of client thread ####


# Output queue      # output event #
# None              # None         #


if __name__ == '__main__':
    threads = []
    hash_event = threading.Event()
    hash_queue = deque()
    hash_event.set()
    dir_name = '/home/dmitri/quinta-v2/samples_d'
    i = 0
    for name in os.listdir(dir_name):
        if name.split('.')[-1] == 'raw':
            if i > 40:
                break
            i += 1
            logging.info(name)
            hash_queue.append(os.path.join(dir_name, name))
    client_event = threading.Event()
    client_event.clear()
    client_queue = deque()
    t = threading.Thread(name='Hasher', target=hash_thread, args=(hash_queue, hash_event,
                                                                  client_queue, client_event))
    t.start()
    t = threading.Thread(name='Client', target=client_thread, args=(client_queue, client_event,
                                                                    None, None))
    t.start()
    hash_event.clear()
    hash_event.set()
