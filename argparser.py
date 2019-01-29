#!/usr/bin/python3
import argparse
import sys
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('station', help='name of the station to monitor')
parser.add_argument('-d', action='store_true', help='call monitor as daemon')
parser.add_argument('-st', action='store_true', help='save recognized mp3 files')
parser.add_argument('-loglevel',choices=('debug','info','error'),default='info', help='minimal level of log messages')
parser.add_argument('-th',type=float,default='3', help='outlier threshold level')
parser.add_argument('--track_len',type=int,choices=range(1,6),default='3', help='minimal observed track lenth for confident recognition')
parser.add_argument('--period_len',type=int,choices=range(30,61),default='45', help='length of one record')


cases = [ ['python3','new_main.py','test'],
['python3','new_main.py','test', '-d','-loglevel','info','-st','--period_len','30']
]
for case in cases:
    print(parser.parse_args(case[2:]))
print(parser.parse_args(sys.argv[1:]))
