#!/bin/sh

export PYTHONUNBUFFERED=1
export PATH=${PATH}:../bin

for ID in AA BS CR HD PA PATH TR ZZ
do
    COUNT=$(count-documents.py ${ID} | jq -r ".${ID}")
    echo ${ID} ${COUNT}
    if [ x${COUNT} = x"missing" ]; then
        COUNT=0
    fi
    if [ x${COUNT} = "x0" ]; then
        ls storage/${ID}_???.jsonl | parallel post-all.py {} --core ${ID} --default-fields NRS_Headcode
    fi
done
