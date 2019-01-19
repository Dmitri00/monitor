import urllib,urllib.request
import json
import sqlite3
query_url = 'http://echoprint.music365.pro:5678'
recognize_method_name = 'query/set_int'
index_url = 'http://echoprint.music365.pro:5000'
root_dir = '/home/dmitri/quinta-v2'
index_method_name = 'tracks'
db_path = '/home/dmitri/database/music_db'
def echoprint_recognize(track_hash):
    url = query_url+ '/' + recognize_method_name
    params = 'echoprint='+track_hash
    response = urllib.request.urlopen(url, data=params.encode('ascii')).read().decode('ascii')
    print(response)
    return response
def echoprint_index(track_id):
    url = index_url + '/' + index_method_name
    params = 'id='+str(best_match['index'])
    response = urllib.request.urlopen(url, data=params.encode('ascii')).read().decode('ascii')
    return response
def db_track_index(track_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor();
    cursor.execute("select * from Tracks where TrackId = %s" % str(track_id))
    track = cursor.fetchone()
    conn.close()
    return track
def db_accident_insert(accident):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor();
    station_name = accident[1]
    cursor.execute('select StationId from Stations where StationTitle = \"%s\"' % station_name)
    station_id = cursor.fetchone()[0]
    accident[1] = station_id
    cursor.execute("insert into Accidents values(?,?,?,?,?)",accident)
    conn.commit()
    conn.close()
def db_stationurl_get_by_name(station_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor();
    cursor.execute('Select StationUrl from Stations where StationTitle = \"%s\"' % station_name)
    stations = cursor.fetchall()
    conn.close()
    return stations

    

