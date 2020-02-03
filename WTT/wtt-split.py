#!/usr/bin/env python3

"""Split CIF timetable file into file chunks in the 'storage' directory by ID or if more than 131072 (or 2 ^ 17) lines long"""
import sys

N = 0
M = 0

KEY = None
NAME = None

fin = sys.stdin
fout = None
for line in fin:
    N += 1
    ID = line[0:2]
    BUFFER = False

    if ID != KEY:
        if ID in ['BS', 'BX', 'CR', 'LI', 'LO', 'LT']:
            if NAME != 'PATH':
                NAME = 'PATH'
                BUFFER = True
        elif ID in ['TI', 'TA', 'TD']:
            if NAME != 'TR':
                NAME = 'TR'
                BUFFER = True
        else:
            NAME = ID
            BUFFER = True

    if (N > 65535 and ID == 'BS') or BUFFER:
        M += 1
        if fout:
            fout.close()
        filename = 'output/{}_{}'.format(NAME, str(M).zfill(3))
        fout = open(filename, 'w')
        N = 0
    fout.write(line)

    KEY = ID

fout.close()
