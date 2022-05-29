#!/usr/bin/env python3

import sys
import argparse
import pandas as pd

PARSER = argparse.ArgumentParser(description='Converts HTML to JSON')

PARSER.add_argument('--encoding', dest='encoding', default='utf-8',
                    help='optional file encoding parameter')

PARSER.add_argument('inputfile', type=str, nargs='?', help='name of file to parse')

ARGS = PARSER.parse_args()

ENCODING = ARGS.encoding

FIN = sys.stdin

INPUTFILE = ARGS.inputfile

if INPUTFILE:
    FIN = open(INPUTFILE, 'r', encoding=ENCODING)

OUTPUT = pd.read_html(FIN)

def clean_up(this_array):
    return [v.encode('ascii', errors='ignore').decode().strip()
            for v in this_array]

for DF in OUTPUT:
    DF.columns = pd.Index(clean_up(DF.columns.array))    
    print(DF.to_json(orient='records', force_ascii=True))
    
