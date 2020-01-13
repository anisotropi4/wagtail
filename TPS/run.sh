#!/bin/sh

export PYTHONUNBUFFERED=1
export PATH=${PATH}:../bin

for FILE in output/*.jsonl
do
    ID=$(basename ${FILE} | sed 's/.jsonl$//')
    COUNT=$(count-documents.py ${ID} | jq -r ".${ID}")
    echo ${ID} ${COUNT}
    if [ x${COUNT} = x"missing" ]; then
        COUNT=0
    fi
    if [ x${COUNT} = "x0" ]; then
        echo Posting ${FILE} to Solr ${ID}
        < ${FILE} parallel -j 1 --files --pipe -l 65536 cat | parallel "post-all.py {} --core ${ID} --seq {#} --rename-id; rm {}; sleep 1"
    fi
done

