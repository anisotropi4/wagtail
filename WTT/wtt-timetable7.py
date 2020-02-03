#!/usr/bin/env python3

# requirement: a unique train for each given bitmap day and UID

from app.solr import get_connection, raw_query
import json
import pandas as pd

DAY = pd.offsets.Day()
MONDAY = pd.offsets.Week(weekday=0)
WEEK = 7 * DAY
N = 0

def days_str(n):
    return '{:b}'.format(n).zfill(7)

def day_int(bitmap):
    return int(bitmap, 2)

DEBUG = True
if __name__ == '__main__':
    DEBUG = False

if DEBUG:
    pd.set_option('display.max_columns', None)

solr = get_connection('PA')
df1 = pd.DataFrame(raw_query(solr, nrows=False))

df1 = df1.drop(['_version_', 'Transaction', 'STP', 'Dates', 'Origin', 'Terminus', 'Duration'], axis=1)
df1 = df1.drop(df1[df1['ID'] != 'PA'].index).fillna(value={'Duration': '00:00:00'})

df1['ID'] = 'PT'
for KEY in ['Date_From', 'Date_To']:
    df1[KEY] = pd.to_datetime(df1[KEY])

#df1 = df1.drop_duplicates(subset=['UID', 'Dates', 'Origin', 'Duration'], keep='last')

df2 = df1[['Date_From', 'Date_To']].rename(columns={'Date_From': 'Start_Date', 'Date_To': 'End_Date'})
idx2 = df2['End_Date'].isnull()
df2.loc[idx2, 'End_Date'] = df2.loc[idx2, 'Start_Date']

idx_monday = df2['Start_Date'].dt.dayofweek == 0
df2.loc[~idx_monday, 'Start_Date'] = df2.loc[~idx_monday, 'Start_Date'] - MONDAY
df2['End_Date'] = df2['End_Date'] + MONDAY

df1 = df1.join(df2)
idx1 = df1['Days'].isna()
df1.loc[idx1, 'Days'] = '0000000'

df1 = df1.drop(['Date_From', 'Date_To'], axis=1)

df1['Actual'] = df1['Days']

def output_schedule(this_schedule):
    this_schedule['Active'] = this_schedule['Start_Date'].dt.strftime('%Y-%m-%d')  + '.' + this_schedule['End_Date'].dt.strftime('%Y-%m-%d') + '.' + this_schedule['Actual'] + '.' + this_schedule['Op_Days']

    this_schedule['id'] = this_schedule['id'] + '.' + this_schedule.groupby('id').cumcount().apply(str)
    for KEY in ['Start_Date', 'End_Date']:
        this_schedule[KEY] = this_schedule[KEY].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    this_schedule = this_schedule.fillna(value={'Origin': '', 'Terminus': ''})
    this_data = [json.dumps({k: v for k, v in path.to_dict().items() if v}) for _, path in this_schedule.iterrows()]
    print('\n'.join(this_data))

#Identify all unique UIDs in timetable
idx1 = df1['UID'].duplicated(keep=False)
SCHEDULE = pd.DataFrame(df1[~idx1]).reset_index(drop=True)
output_schedule(SCHEDULE)
DUPLICATES = df1[idx1]

# Identify all UIDs without date overlap in timetable
df2 = DUPLICATES[['UID', 'Start_Date', 'End_Date']].sort_values(['UID', 'Start_Date']).reset_index(drop=True)
df3 = df2[['UID', 'Start_Date']].rename({'UID': 'UID2', 'Start_Date': 'overlap'}, axis=1).shift(-1)
df2 = df2.join(df3)
df3 = df2[df2['UID'] == df2['UID2']].drop('UID2', axis=1).set_index('UID')

df2 = DUPLICATES.set_index('UID', drop=False)
idx3 = df3[df3['End_Date'] > df3['overlap']].index.unique()

SCHEDULE = df2.drop(idx3).reset_index(drop=True)
output_schedule(SCHEDULE)

# Identify all UIDs with date overlap and interleave
df2 = df2.loc[idx3]

def xor_bitmap(a, b):
    return b & (a ^ b)

def overlay_bits(b):
    v = list(b[::-1])
    for n in range(1, len(v)):
        v = v[:n] + [(xor_bitmap(v[n - 1], i)) for i in v[n:]]
    return tuple(v[::-1])

def interleave(these_objects):
    this_interval = [(j['Start_Date'], j['End_Date'], day_int(j['Days']), (j),) for j in these_objects]
    idx = sorted(set([j for i in this_interval for j in (i[0], i[1])]))
    all_paths = {}
    for i in this_interval:
        (m, n, bit, k) = i
        for j in range(idx.index(m), idx.index(n)):
            (k1, k2) = (idx[j], idx[j+1])
            try:
                all_paths[(k1, k2)] += ((bit, k),)
            except KeyError:
                all_paths[(k1, k2)] = ((bit, k),)
    this_schedule = []
    for (k1, k2), v in all_paths.items():
        (bits, paths) = zip(*v)
        bits = overlay_bits((bits))
        for bit, path in zip(bits, paths):
            if bit > 0:
                path = path.copy()
                path['Start_Date'] = k1
                path['End_Date'] = k2
                path['Actual'] = days_str(bit)
                this_schedule.append(path)
    return this_schedule

UPDATE = []
for UID in df2.index.unique():
    this_schedule = [i.to_dict() for _, i in df2.loc[UID].iterrows()]
    UPDATE += interleave(this_schedule)

SCHEDULE = pd.DataFrame(UPDATE)

output_schedule(SCHEDULE)
