#!/bin/sh
for ID in AA BS CR HD PA PATH TR ZZ
do
    ../bin/count-documents.py ${ID}
    wc -l storage/${ID}_???.jsonl | tail -1 | awk '{ print "{\"" "'${ID}'" "\": " $1 "}"}'
done

