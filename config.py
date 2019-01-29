
############ DEFINED CONSTANTS ###########
is_daemon = True
log_level = 'debug'
OUTLIER_THRESHOLD = 3
RECORD_PERIOD = 45 
minimal_track_len = 3
REMOVE_MP3 = False

target_dir = '/tmp'
query_url = 'http://echoprint.music365.pro:5678'
recognize_method_name = 'query/set_int'
index_url = 'http://echoprint.music365.pro:5000'
root_dir = '/home/dmitri/quinta-v2'
index_method_name = 'tracks'
report_header = 'starttime,endtime,station,score,artist,title\n'
db_path = '/home/dmitri/database/music_db'
log_file = '/home/dmitri/quinta.log'
lock_file = '/home/dmitri/quinta.lock'
echoprint_codegen_path = '/home/dmitri/echoprint-codegen/echoprint-codegen'
filename_rgexp = '/(?P<timestamp>.*)__(?P<station_name>.*)\..*$'
