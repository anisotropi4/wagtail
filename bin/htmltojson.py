#!/usr/bin/env python

import sys
import json
import argparse
from xmltodict import parse
from lxml import html, etree

parser = argparse.ArgumentParser(description='Reformats an html so that sub-trees below a given tag depth are on a single line')

parser.add_argument('--depth', dest='depth', type=int, default=3,
                    help='depth to remove outer tags (default: 3)')

parser.add_argument('--stdout', dest='tostdout', action='store_true',
                    default=False,
                    help='output to STDOUT (default: False if depth > 0)')

parser.add_argument('--path', dest='output_path', type=str, default='output',
                    help='output directory file')

parser.add_argument('--encoding', dest='encoding', default='utf-8',
                    help='optional file encoding parameter')

parser.add_argument('inputfile', type=str, nargs='?', help='name of file to parse')

args = parser.parse_args()

output_path = args.output_path
depth = args.depth
encoding = args.encoding

if output_path != '':
    output_path = output_path + '/'

fin = sys.stdin

tostdout = args.tostdout

filenames = {}

if depth == 0:
    tostdout = True

fout = sys.stdout
outputfile = 'STDOUT'

inputfile = args.inputfile

if inputfile:
    fin = open(inputfile, 'r', encoding=encoding)
else:
    fin = sys.stdin

def write_file(path, item):
    if tostdout:
        print(json.dumps(item))
        return True
    rtag = 'output'
    if path:
        (rtag, _) = path[-1]
    outputfile = rtag + '.jsonl'
    if outputfile in filenames:
        fout = open(output_path + outputfile, 'a')
    else:
        fout = open(output_path + outputfile, 'w')
        filenames[outputfile] = True
    fout.write(json.dumps(item))
    fout.write('\n')
    return True

root = parse(etree.tostring(html.fromstring(str(fin.read()))).decode('utf-8'),
             attr_prefix='',
             cdata_key='value',
             item_depth=depth,
             item_callback=write_file,
             dict_constructor=dict)

if depth == 0:
    write_file([], root)
