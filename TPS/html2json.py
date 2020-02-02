#!/usr/bin/env python3
# -*- coding: iso-8859-1 -*-
import sys
import argparse
import re
import xmltodict
import json

parser = argparse.ArgumentParser(description='Reformats an xml-filename so that sub-trees below a given tag depth are on a single line')

parser.add_argument('--depth', dest='depth', type=int, default=3,
                    help='depth to remove outer xml tags (default: 3)')

parser.add_argument('--stdout', dest='tostdout', action='store_true',
                    default=False,
                    help='output to STDOUT (default: False if depth > 0)')

parser.add_argument('--path', dest='path', type=str, default='output',
                    help='output directory file')

parser.add_argument('--badxml', dest='badxml', action='store_true',
                    default=False,
                    help='ignore tag mismatch errors')
parser.add_argument('--encoding', dest='xmlencoding', default='utf-8',
                    help='optional file encoding parameter')

parser.add_argument('inputfile', type=str, nargs='?', help='name of xml-file to parse')

args = parser.parse_args()

path = args.path
depth = args.depth
xmlencoding = args.xmlencoding

if path != '':
    path = path + '/'

fin = sys.stdin

tostdout = args.tostdout
badxml = args.badxml

filenames = {}

if depth == 0:
    tostdout = True

fout = sys.stdout
outputfile = 'STDOUT'

inputfile = args.inputfile
if inputfile:
    fin = open(inputfile, 'r', encoding=xmlencoding)
else:
    inputfile = 'STDIN'

root = []
rtag = None
stag = None
etag = None
nl = True

spacematch1 = re.compile(r'>[\s]+<')
spacematch2 = re.compile(r'[\s]+/>')
tagsplit = re.compile(r'[>\s]')

tagmatch1 = re.compile(r'>\s*<')

def split_line(line):
    return re.sub(tagmatch1, '>\n<', line.rstrip('\n')).split('\n')

fbuffer = ''

first = True
for line in (j for i in fin for j in split_line(i)):
    if first:
        line = line.encode().decode('ascii', '')
        first == False
    line = line.rstrip('\n')
    line = line.strip()
    line = re.sub(spacematch1, '><', line)
    line = re.sub(spacematch2, '/>', line)
    line = line.encode().decode('ascii', 'xmlcharrefreplace')
    print(line)
    if 'xml ' in line[:8]:
        continue

    multiline = False
    stag = None
    etag = None    

    try:
        (tag, _) = re.split(tagsplit, line[1:], 1)
    except ValueError:
        fbuffer += line
        #fout.write(line)
        continue

    if tag != '':
        if tag[0] == '/':        
            etag = tag[1:-1]
        elif line[0] == '<':
            stag = tag

    if line[-2:] == '/>':
        etag = tag
    elif '</' in line:
        (etag, _) = line[::-1][1:].split('/<', 1)        
        etag = etag[::-1]

    if stag and not etag:
        if line[(len(stag) + 2):] != '' and line[-1] != '>':
            multiline = True

    if not stag and etag:
        if line[:-(len(etag) + 3)] != '':
            multiline = True

    if not stag and not etag:
        multiline = True

    if stag and ':' in stag:
        (_, tag) = stag.split(':')
        line = line.replace(stag, tag)
        stag = tag

    if etag and ':' in etag:
        (_, tag) = etag.split(':')
        line = line.replace(etag, tag)
        etag = tag

    if stag:
        root.append(stag)

    if len(root) == (depth + 1) and not tostdout:
        if stag and stag != rtag:
            rtag = stag

            if fout and not fout.closed and fout is not sys.stdout:        
                fout.close()
            outputfile = rtag + '.jsonl'
            if outputfile in filenames:
                fout = open(path + outputfile, 'a')
            else:
                fout = open(path + outputfile, 'w')
                filenames[outputfile] = True

    if len(root) > depth:
        fbuffer += line
        #fout.write(line)
        nl = False

    if etag:
        try:
            qtag = root.pop()
        except IndexError:
            sys.stderr.write('Error: missing root tag "' + etag + '" in file "' + inputfile + '"\n')
            pass
        if etag != qtag:
            if badxml:
                root.append(qtag)
                pass
            else:
                sys.stderr.write('Error: tag mismatch between "' + qtag + '" "' + etag + '" in file "' + inputfile + '"\n')
                sys.stderr.write('"' + line + '"\n')
                sys.exit(1)
    
    if multiline and not etag and len(root) > depth:
        fbuffer += ' '
    elif len(root) == depth and not nl:
        fbuffer = '<_wrapper>{}</_wrapper>'.format(fbuffer)
        data = xmltodict.parse(fbuffer, item_depth=0, attr_prefix='')
        for key in data['_wrapper'].keys():
            fout.write(json.dumps(data['_wrapper'][key]))
            fout.write('\n')
        fbuffer = ''
        nl = True
