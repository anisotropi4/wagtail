#!/usr/bin/env python3

import sys
import pandas as pd
import numpy as np
import json
from datetime import datetime
from hashlib import md5
import os.path as path
import argparse
import os.path as path
from uuid import uuid1

DEBUG = True

filename = 'output/PATH_005'
filename = 'output/PATH_147'
filename = 'output/PATH_016'
filename = 'output/PATH_024'
filename = 'output/PATH_008'
filename = 'output/PATH_090'
filename = 'output/AA_132'
filename = 'output/PATH_004'
filename = 'output/AA_003'
filename = 'output/HD_001'
filename = 'output/TR_002'
filename = 'output/PATH_004'
filename = 'output/PATH_030'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process trains services \
file to json')
    parser.add_argument('inputfile', type=str, help='name of working \
timetable file to parse')
    args = parser.parse_args()
    filename = args.inputfile
    DEBUG = False

filestub = path.basename(filename)

if DEBUG:
    print(filename)
    pd.set_option('display.max_columns', None)

ISO8601_DATE = datetime(1900, 1, 1)
DAY = pd.offsets.Day()
MONDAY = pd.offsets.Week(weekday=0)

def header_date(this_column):
    return pd.to_datetime(this_column, format='%d%m%y').dt.strftime('%Y-%m-%d')

def wtt_date(this_column):
    return pd.to_datetime(this_column, format='%y%m%d').dt.strftime('%Y-%m-%d')

def wtt_datetime(this_column):
    return this_column.dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def wtt_time(this_column, format='%H%M%S'):
    this_column = this_column.str.replace('H', '30').str.replace(' ', '00')
    return pd.to_datetime(this_column, format=format)

def blank_columns(this_frame):
    return [n for n in this_frame.select_dtypes(include=['object']).columns if this_frame[n].str.isspace().all() or (this_frame[n] == '').all()]

def strip_columns(this_frame):
    return [n for n in this_frame.select_dtypes(include=['object']).columns if this_frame[n].str.isspace().any()]

def days_str(this_series):
    return pd.to_datetime(this_series).apply(lambda v: '{:b}'.format(64 >> v.weekday()).zfill(7))

def get_dates(this_df):
    no_idx = this_df['Date To'].str.isspace()
    this_df.loc[no_idx, 'Days'] = days_str(this_df.loc[no_idx, 'Date From'])
    this_df.loc[no_idx, 'Date To'] = this_df.loc[no_idx, 'Date From']
    this_df['Date From'] = pd.to_datetime(this_df['Date From'], format='%y%m%d')
    this_df['Date To'] = pd.to_datetime(this_df['Date To'], format='%y%m%d')
    this_df['Dates'] = wtt_date(this_df['Date From'] - MONDAY) + '.' + wtt_date(this_df['Date To'] + MONDAY) + '.' + this_df['Days']
    this_df['Date From'] = wtt_datetime(this_df['Date From'])
    this_df['Date To'] = wtt_datetime(this_df['Date To'])
    return this_df[['Date From', 'Date To', 'Dates', 'Days']]

def header_record(records):
    """process CIF file header record from 80-character line string"""
    this_array = [[line[0:2], line[2:22], line[22:28], line[28:32], line[32:39], line[39:46], line[46:47], line[47:48], line[48:54], line[54:60]] for line in records]
    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'File Mainframe Identity', 'Date of Extract', 'Time of Extract', 'Current File Ref', 'Last File Ref', 'Bleed off Update Ind', 'Version', 'User Extract Start Date', 'User Extract End Date'])
    this_frame['Extract Datetime'] = pd.to_datetime(this_frame['Time of Extract'] + this_frame['Date of Extract'], format='%H%M%d%m%y').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    this_frame['Extract Interval'] = header_date(this_frame['User Extract Start Date']) + '/' + header_date(this_frame['User Extract End Date'])

    this_frame = this_frame.drop(['User Extract Start Date', 'User Extract End Date', 'Time of Extract', 'Date of Extract'], axis=1)
    this_frame = this_frame.drop(blank_columns(this_frame), axis=1)
    #this_frame['id'] = [md5(x.encode()).hexdigest() for x in records]
    return this_frame

def tiploc_record(records):
    """return CIF file TIPLOC object from 80-character line string"""
    this_array = [[line[0:2],line[2:9],line[9:11],line[11:17],line[17:18],line[18:44],line[44:49],line[49:53],line[53:56],line[56:72],line[72:79]] for line in records]
    this_frame = pd.DataFrame(data=this_array, columns=['ID','TIPLOC','Capitals Identification','Nalco','NLC check character','TPS Description','Stanox','PO MCP','CRS','Description','New TIPLOC'])
    this_frame = this_frame.drop(blank_columns(this_frame), axis=1)
    #this_frame['id'] = [md5(x.encode()).hexdigest() for x in records]
    return this_frame

def notes_record(records):
    """return CIF file train notes object en route object from 80-character line string"""
    this_array = [[line[0:2],line[2:3],line[3:80]] for line in records]
    this_frame = pd.DataFrame(data=this_array, columns=['ID','Note Type','Note'])
    this_frame = this_frame.drop(blank_columns(this_frame), axis=1)
    #this_frame['id'] = [md5(x.encode()).hexdigest() for x in records]
    return this_frame

def association_record(records):
    """return CIF file train-association object from 80-character line string"""
    this_array = [[line[0:2],line[2:3],line[3:9],line[9:15],line[15:21],line[21:27],line[27:34],line[34:36],line[36:37],line[37:44],line[44:45],line[45:46],line[47:48],line[79:80]] for line in records]
    this_frame = pd.DataFrame(data=this_array, columns=['ID','Transaction','Main UID','UID','Date From','Date To','Days','Category','Indicator','Location','Base Suffix','Location Suffix','Type','STP'])
    this_frame[['Date From', 'Date To', 'Dates', 'Days']] = get_dates(this_frame)
    #this_frame = this_frame.drop(['Date From', 'Date To'], axis=1)
    this_frame = this_frame.drop(blank_columns(this_frame), axis=1)
    #this_frame['id'] = [md5(x.encode()).hexdigest() for x in records]
    return this_frame

def wtt_records(records):
    this_array = [[line[0:2],line] for line in records]
    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'Data'])
    this_frame['key'] = [md5(x.encode()).hexdigest() for x in records]
    this_frame.loc[this_frame['ID'] == 'BS', 'UUID'] = this_frame.loc[this_frame['ID'] == 'BS', 'key']
    this_frame = this_frame.fillna(method='ffill')
    return this_frame

def pa_record(this_df):
    this_array = [['PA', line[2:3], line[3:9], line[9:15], line[15:21], line[21:28], line[79:80]] for line in this_df['Data']]

    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'Transaction','UID','Date From','Date To','Days','STP'])
    this_frame[['Date From', 'Date To', 'Dates', 'Days']] = get_dates(this_frame)
    #this_frame = this_frame.drop(['Date From', 'Date To'], axis=1)
    this_frame['UUID'] = this_df['UUID'].tolist()
    this_frame['id'] = this_df['id'].tolist()
    return this_frame

def bs_record(this_df):
    this_array = [[line[0:2], line[28:29], line[29:30], line[30:32], line[32:36], line[36:40], line[41:49], line[49:50], line[50:53], line[53:57], line[57:60], line[60:66], line[66:67], line[67:68], line[68:69], line[70:74], line[74:78]] for line in this_df['Data']]
    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'Bank Holiday Running', 'Train Status', 'Train Category', 'Headcode', 'NRS Headcode', 'Train Service', 'Portion Id', 'Power Type', 'Timing Load', 'Speed', 'Characteristics', 'Seating Class', 'Sleepers', 'Reservations', 'Catering', 'Service Branding'])
    this_frame['UUID'] = this_df['UUID'].tolist()
    this_frame['id'] = this_df['id'].tolist()
    return this_frame

def bx_record(this_df):
    this_array = [[line[0:2], line[6:11], line[11:13], line[13:14], line[14:22], line[22:23]] for line in this_df['Data']]
    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'UIC', 'ATOC', 'Applicable Timetable', 'RSID', 'Data Source'])
    this_frame['UUID'] = this_df['UUID'].tolist()
    return this_frame

def origin_location(this_df):
    """return CIF file depart from origin object, updated last reported time and
train operation duration from 80-character line string, the last reported time
and the train operation duration"""
    this_array = [[line[0:2], line[2:9], line[9:10], line[10:15], line[15:19], line[19:22], line[22:25], line[25:27], line[27:29], line[29:41], line[41:43], line[43:46]] for line in this_df['Data']]
    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'TIPLOC', 'Suffix', 'Schedule', 'Public Schedule', 'Platform', 'Line', 'Engineering Allowance', 'Pathing Allowance', 'Activity', 'Performance Allowance', 'Reserved'])
    this_frame['UUID'] = this_df['UUID'].tolist()
    this_frame['id'] = this_df['id'].tolist()
    this_frame['id'] += '.0'
    this_frame['T'] = 'OD'
    this_frame['index'] = this_df.index.tolist()
    return this_frame

def intermediate_location(this_df):
    """return CIF file intermediate location object, updated last reported time
    and train operation duration from 80-character line string, the last reported
    time and the train operation duration"""
    this_array = [[line[0:2], line[2:9], line[9:10], line[10:15], line[15:20], line[20:25], line[25:29], line[29:33], line[33:36], line[36:39], line[39:42], line[42:54], line[54:56], line[56:58], line[58:60], line[60:65]] for line in this_df['Data']]
    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'TIPLOC', 'Suffix', 'Schedule Arrival', 'Schedule Departure', 'Schedule Pass', 'Public Arrival', 'Public Departure', 'Platform', 'Line', 'Path', 'Activity', 'Engineering Allowance', 'Pathing Allowance', 'Performance Allowance', 'Reserved'])
    this_frame['UUID'] = this_df['UUID'].tolist()
    this_frame['id'] = this_df['id'].tolist()
    this_frame['index'] = this_df.index.tolist()

    idx_pass = (~this_frame['Schedule Pass'].str.isspace())

    df_arrival = this_frame[~idx_pass].rename(columns={'Schedule Arrival': 'Schedule', 'Public Arrival': 'Public Schedule'})
    df_arrival = df_arrival.drop(['Schedule Departure', 'Public Departure', 'Schedule Pass'], axis=1)
    df_arrival['T'] = 'IA'
    df_arrival['id'] += '.1'

    df_departure = this_frame[~idx_pass].rename(columns={'Schedule Departure': 'Schedule', 'Public Departure': 'Public Schedule'})
    df_departure = df_departure.drop(['Schedule Arrival', 'Public Arrival', 'Schedule Pass'], axis=1)
    df_departure['T'] = 'ID'
    df_departure['id'] += '.3'

    df_pass = this_frame[idx_pass].rename(columns={'Schedule Pass': 'Schedule'})
    df_pass = df_pass.drop(['Schedule Arrival', 'Public Arrival', 'Schedule Departure', 'Public Departure'], axis=1)
    df_pass['Public Schedule'] = '0000'
    df_pass['T'] = 'IP'
    df_pass['id'] += '.2'
    return pd.concat([df_arrival, df_departure, df_pass], sort=False)

def terminus_location(this_df):
    this_array = [[line[0:2], line[2:9], line[9:10], line[10:15], line[15:19], line[19:22], line[22:25], line[25:27], line[37:40]] for line in this_df['Data']]
    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'TIPLOC', 'Suffix', 'Schedule', 'Public Schedule', 'Platform', 'Path', 'Activity', 'Reserved'])
    this_frame['UUID'] = this_df['UUID'].tolist()
    this_frame['id'] = this_df['id'].tolist()
    this_frame['T'] = 'TA'
    this_frame['id'] += '.4'
    for key in ['Line', 'Engineering Allowance', 'Pathing Allowance', 'Performance Allowance']:
        this_frame[key] = ''
    this_frame['index'] = this_df.index.tolist()
    return this_frame

def change_en_route(this_df):
    """return CIF file train change en route object from 80-character line string"""
    this_array = [[line[0:2], line[2:9], line[9:10], line[10:12], line[12:16], line[16:20], line[21:29], line[29:30], line[30:33], line[33:37], line[37:40], line[40:46], line[46:47], line[47:48], line[48:49], line[50:54], line[54:58], line[62:67], line[67:75]] for line in this_df['Data']]
    this_frame = pd.DataFrame(data=this_array, columns=['ID', 'TIPLOC', 'Suffix', 'Train Category', 'Headcode', 'NRS Headcode', 'Train Service', 'Portion Id', 'Power Type', 'Timing Load', 'Speed', 'Operating Characteristics', 'Seating Class', 'Sleepers', 'Reservations', 'Catering', 'Service Branding', 'UIC', 'Reserved'])
    this_frame['UUID'] = this_df['UUID'].tolist()
    this_frame['id'] = this_df['id'].tolist()
    return this_frame

def get_wtt(this_df):
    LO_frame = origin_location(this_df[this_df['ID'] == 'LO'])
    LI_frame = intermediate_location(this_df[this_df['ID'] == 'LI'])
    LT_frame = terminus_location(this_df[this_df['ID'] == 'LT'])
    WTT = pd.concat([LO_frame, LI_frame, LT_frame], sort=False).sort_values(by=['index', 'id']).reset_index(drop=True)
    WTT['Schedule'] = wtt_time(WTT['Schedule'])

    idx_lo = (WTT['ID'] == 'LO')
    WTT.loc[idx_lo, 'Offset'] = WTT.loc[idx_lo, 'Schedule']
    WTT['Offset'] = WTT['Schedule'] - WTT['Offset'].fillna(method='ffill')
    WTT.loc[WTT['Offset'] < pd.Timedelta(0), 'Offset'] += DAY
    WTT['Offset'] += ISO8601_DATE

    idx_ps = (WTT['Public Schedule'] != '0000')

    WTT.loc[idx_ps, 'Public Schedule'] = wtt_time(WTT.loc[idx_ps, 'Public Schedule'], format='%H%M').dt.strftime('%H:%M')
    WTT.loc[~idx_ps, 'Public Schedule'] = ''
    WTT = WTT.drop('index', axis=1)
    return WTT

def end_record(records):
    this_array = [[line[0:2]] for line in records]
    this_frame = pd.DataFrame(data=this_array, columns=['ID'])
    #this_frame['id'] = [uuid1().hex for x in records]
    return this_frame

SOLR_CONN = {}
SOLR_DATA = {}

def write_json(filename, this_df, key):
    this_df = this_df.fillna('')
    this_df.columns = [i.replace(' ', '_') for i in this_df.columns.to_list()]

    this_buffer = ''
    for _, r in this_df.iterrows():
        u = {k: (v.rstrip() if isinstance(v, str) else v)
             for k, v in r.to_dict().items() if isinstance(v, int) or v.rstrip() != ''}
        this_buffer += json.dumps(u) + '\n'
        if key == 'PATH':
            u['id'] = uuid1().hex

    with open(filename, 'w') as fout:
        fout.write(this_buffer)

OP_FN = {'HD': header_record, 'TR': tiploc_record, 'AA': association_record, 'ZZ': end_record, 'PATH': wtt_records}
#OP_FN = {'HD': header_record, 'TI': tiploc_record} #, 'AA': association_record, 'ZZ': end_record, 'PATH': wtt_records}

M = filestub[-3:]

OUTPUT = []
#fin = sys.stdin
fin = open(filename, 'r')
KEY = None
CACHE = True

SERVICE = np.array([])

[OUTPUT.append(line.strip()) for line in fin]
ID = OUTPUT[0][0:2]

KEY = ID
if ID in ['BS', 'BX', 'CR', 'LI', 'LO', 'LT']:
    KEY = 'PATH'
if ID in ['TI', 'TA', 'TD']:
    KEY = 'TR'

df1 = OP_FN[KEY](OUTPUT)
df1['id'] = df1.index
df1['id'] = M + '.' + df1['id'].apply(lambda v: str(1 + v).zfill(8))

SA = pd.DataFrame()

if KEY == 'PATH':
    idx_sa = (df1['ID'] == 'BS') | (df1['ID'] == 'BX') | (df1['ID'] == 'CR')
    SA = df1.loc[idx_sa, ['ID', 'Data', 'UUID', 'id']]
    WTT = get_wtt(df1)
    lo_idx = (WTT['ID'] == 'LO')
    lt_idx = (WTT['ID'] == 'LT')
    df1 = WTT
    df1['Schedule'] = df1['Schedule'].dt.strftime('%H:%M:%S')
    df1['Offset'] = df1['Offset'].dt.strftime('%H:%M:%S')

if KEY in ['HD', 'ZZ']:
    df1['seq'] = M

filename = 'storage/{}_{}.jsonl'.format(KEY, M)
write_json(filename, df1, KEY)
OUTPUT = []

if SA.empty:
    if DEBUG:
        1/0
    sys.exit(0)

PA = pa_record(SA[SA['ID'] == 'BS']).set_index('UUID')
df2 = WTT.loc[lo_idx, ['Schedule', 'UUID']].set_index('UUID').rename(columns={'Schedule': 'Origin'})
PA = PA.join(df2)
df2 = WTT.loc[lt_idx, ['Schedule', 'Offset', 'UUID']].set_index('UUID').rename(columns={'Schedule': 'Terminus', 'Offset': 'Duration'})
PA = PA.join(df2)

df2 = WTT.loc[lo_idx, ['Schedule', 'UUID']].set_index('UUID')
df2 = df2.join(WTT.loc[lt_idx, ['Offset', 'UUID']].set_index('UUID'))
df3 = ((pd.to_timedelta(df2['Schedule']) + pd.to_timedelta(df2['Offset'])) / DAY).apply(np.ceil).astype(int).astype(str)
df3.name = 'Op_Days'
PA = PA.join(df3).reset_index().fillna(value={'Op_Days': '0'}).fillna('')
PA['Dates'] += '.' + PA['Op_Days']

BS = bs_record(SA[SA['ID'] == 'BS'])
BX = bx_record(SA[SA['ID'] == 'BX'])
BS = BS.set_index('UUID').join(BX.set_index('UUID').drop('ID', axis=1)).reset_index(drop=False)
CR = change_en_route(SA[SA['ID'] == 'CR'])

for (k, df1) in [('PA', PA), ('BS', BS), ('CR', CR)]:
    filename = 'storage/{}_{}.jsonl'.format(k, M)
    write_json(filename, df1, k)

