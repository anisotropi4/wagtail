#!/bin/sh

export SOLRHOST=localhost
export PYTHONUNBUFFERED=1
export PATH=${PATH}:../bin

count-documents.py _test_ 2> /dev/null
if [ "$?" != "0" ]; then
    start-cluster.sh
    while true
    do
        count-documents.py _test_ 2> /dev/null
        if [ "$?" = "0" ]; then
            break
        fi
    done
fi

for ID in AA BS CR HD PA PATH TR ZZ
do
    COUNT=$(count-documents.py ${ID} | jq -r ".${ID}")
    echo ${ID} ${COUNT}
    if [ x${COUNT} = x"missing" ]; then
        COUNT=0
    fi
    if [ x${COUNT} = "x0" ]; then
        ls storage/${ID}_???.jsonl | parallel post-simple.py {} --core ${ID}
    fi
done
