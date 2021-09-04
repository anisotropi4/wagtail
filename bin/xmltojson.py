#!/usr/bin/env python

import sys
import re
import json
import argparse
from xmltodict import parse
from lxml import etree


arg_parser = argparse.ArgumentParser(description='Reformats an xml-filename so that sub-trees below a given tag depth are on a single line')

arg_parser.add_argument('--depth', dest='depth', type=int, default=3,
                    help='depth to remove outer tags (default: 3)')

arg_parser.add_argument('--stdout', dest='tostdout', action='store_true',
                    default=False,
                    help='output to STDOUT (default: False if depth > 0)')

arg_parser.add_argument('--path', dest='output_path', type=str,
                        default='output', help='output directory file')

arg_parser.add_argument('--encoding', dest='xmlencoding', default='utf-8',
                    help='optional file encoding parameter')

arg_parser.add_argument('inputfile', type=str, nargs='?', help='name of file to parse')

args = arg_parser.parse_args()

output_path = args.output_path
depth = args.depth
xmlencoding = args.xmlencoding

if output_path != '':
    output_path = output_path + '/'

fin = sys.stdin

tostdout = args.tostdout

filetags = {}

if depth == 0:
    tostdout = True

inputfile = args.inputfile

if inputfile:
    fin = open(inputfile, 'r', encoding=xmlencoding)
else:
    fin = sys.stdin

def clean_dict(d):
    """https://stackoverflow.com/questions/27973988/python-how-to-remove-all-empty-fields-in-a-nested-dict"""
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (clean_dict(v) for v in d) if v]
    return {k: v for k, v in ((k, clean_dict(v)) for k, v in d.items()) if v}

def write_file(path, item):
    if not item:
        return True
    if isinstance(item, str):
        return True
    if not isinstance(item, list):
        item = [item]
    if tostdout:
        print('\n'.join([json.dumps(i) for i in clean_dict(item)]))
        return True
    rtag = 'output'
    if path:
        (rtag, _) = path[-1]
    #print(rtag)
    outputfile = '{}{}.jsonl'.format(output_path, rtag)
    if rtag in filetags:
        fout = open(outputfile, 'a')
    else:
        fout = open(outputfile, 'w')
        filetags[rtag] = True
    fout.write('\n'.join([json.dumps(i) for i in clean_dict(item)]))
    fout.write('\n')
    return True

def write_xml(this_data):
    root = parse('<_wrapper>{}</_wrapper>'.format(this_data),
                 attr_prefix='',
                 item_depth=0,
                 item_callback=write_file,
                 cdata_key='value',
                 dict_constructor=dict)
    key = list(root['_wrapper']).pop()
    write_file([(key, None)], root['_wrapper'][key])

tagmatch = re.compile(r'>\s*<')
def split_line(line):
    return re.sub(tagmatch, '>\n<', line.rstrip('\n')).split('\n')

i = 0

first = True

class _target:
    #events = []
    event = None
    n = 0
    def start(self, tag, _):
        event = ('start', tag, self.n)
        self.event = event
        #self.events.append(event)
        self.tag = tag
        self.n += 1

    def end(self, tag):
        event = ('end', tag, self.n)
        self.event = event
        #self.events.append(event)
        self.n -= 1

    #def close(self):
    #    events, self.events = self.events, []
    #    return events

this_parser = etree.XMLParser(target=_target())

i = 0
fbuffer = []
otag = None
for line in (j for i in fin for j in split_line(i)):
    this_parser.feed(line.encode())
    r = this_parser.target.event
    if r:
        (event, tag, n) = r
        if n == depth and event == 'start':
            if len(fbuffer) > 262144 or (fbuffer and otag != tag):
                write_xml(''.join(fbuffer))
                fbuffer = []
            otag = tag
        if (n == depth and event != 'end') or n > depth:
            fbuffer.append(line.encode().decode('ascii', 'ignore'))

if fbuffer:
    write_xml(''.join(fbuffer))
