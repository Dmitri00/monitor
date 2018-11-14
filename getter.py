
#Getter sector

import wave
import struct
import echoprint

#Constants:
chunk = 2048
channel = 1
setsampwidth=2 #the sample width to n bytes
device_rate = 44100 #Webcam 11025+11025//2 Device 44100
echoprint_rate = 11025
full_time = 60
record_seconds = 30
norm_const = 32768.0

volume_average_threshold=2000

file_name='hash.tmp'
device_name='Device'#Webcam Device

#For hash save
hash_name=u'hash'
hash_type_name=u'.log'
count_hash=0

#For hash save
wav_file_name=u'record'
wav_file_type_name=u'.wav'
count_wav_file=0
samples_list=[]

samples_list=[list() for x in xrange(full_time/record_seconds)]

def samples_queue(samples=""):
    samples_list.pop(0)
    samples_list.append(samples)
    all_samples=list()
    for samples in samples_list:
        all_samples+=samples
    return all_samples



def music_volume_analyzer(buff=[]):

    max_samples = []
    max_average_power=record_seconds
    max_average_len=len(buff)/max_average_power
    for i in range(0, len(buff),len(buff)/max_average_power):
        max_samples.append(abs(max(buff[i:i+max_average_power])))
    max_value,max_samples_average=max(max_samples)*norm_const,sum(max_samples)/len(max_samples)*norm_const
    return max_value,max_samples_average


def getter_sound(file_name):

    print( u'Start record')
    

    samples = []
    with open(file_name, 'rb') as f:
        buf = f.read(512)
        while len(buf)==512:
            samples.extend(struct.unpack('64f',buf)
            buf = f.read(512)
        if len(buf) > 0:
        struct_format = '{}f'.format(len(buf)//4)
        samples.extend(struct.unpack(struct_format, bef))
    d = echoprint.codegen(samples_queue(samples), 0)
    max_value,samples_average=music_volume_analyzer(samples)
    print( u'Complited.')
    return d['code'],max_value,samples_average

def hash_save(hash):
    global count_hash
    count_hash=count_hash+1
    save(hash,hash_name+str(count_hash).zfill(3)+hash_type_name)


