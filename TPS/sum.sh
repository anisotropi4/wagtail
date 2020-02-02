#!/bin/sh
for i in data/*.jsonl
do
    ID=$(basename ${i} | sed 's/.jsonl//')
    ../bin/count-documents.py ${ID}
    wc -l ${i} | awk '{ print "{\"" "'${ID}'" "\": " $1 "}"}'
done

