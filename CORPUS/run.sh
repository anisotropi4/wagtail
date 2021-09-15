#!/bin/sh

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

URL=https://networkrail.opendata.opentraintimes.com/mirror/corpus
if [ ! -f file-list.txt ]; then
    curl -s -L -G  ${URL}| \
        htmltojson.py --stdout --depth 5 | \
        jq -cr '.td? | select(.[1] | tonumber > 1024)? | .[0].a.value' > file-list.txt
fi

if [ ! -s CORPUSExtract.json ]; then
    FILENAME=$(tail -1 file-list.txt)
    echo Download and uncompress ${FILENAME} to CORPUSExtract.json.gz
    curl ${URL}/${FILENAME} -o CORPUSExtract.json.gz
    gzip -d CORPUSExtract.json.gz
fi

if [ ! -d data ]; then
    echo Process CORPUSExtract.json
    mkdir data
    jq -c '.TIPLOCDATA[]' CORPUSExtract.json | sed 's/\" \"/\"\"/g' > data/CORPUS.jsonl
fi

for FILE in data/*.jsonl
do
    ID=$(basename ${FILE} | sed 's/.jsonl$//')
    COUNT=$(count-documents.py ${ID} | jq -r ".${ID}")
    echo ${ID} ${COUNT}
    if [ x${COUNT} = x"missing" ]; then
        < ${FILE} parallel -j 1 --blocksize 8M --files --pipe -l 4096 cat | parallel "post-types.py {} --core ${ID} --seq {#} --rename-id --set-schema; rm {}; sleep 1"
        COUNT=0
    fi
    if [ x${COUNT} = "x0" ]; then
        echo Posting ${FILE} to Solr ${ID}
        < ${FILE} parallel -j 1 --blocksize 32M --files --pipe -l 65536 cat | parallel "post-types.py {} --core ${ID} --seq {#} --rename-id; rm {}; sleep 1"
    fi
done
