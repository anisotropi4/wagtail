#!/bin/sh

export PYTHONUNBUFFERED=1
export PATH=${PATH}:../bin

URL=https://networkrail.opendata.opentraintimes.com/mirror/tpsdata
if [ ! -f file-list.txt ]; then
    curl -k -s -L -G  ${URL} | html2json.py | \
	jq -cr '.[]."File Name"' > file-list.txt
fi

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

if [ ! -f XML_p.xml ]; then
    FILENAME=$(tail -1 file-list.txt)
    echo Download and uncompress ${FILENAME} to TPS_Data.tar.bz2
    curl -k ${URL}/${FILENAME} -o TPS_Data.tar.bz2
    tar jxvf TPS_Data.tar.bz2
fi

if [ ! -d data ]; then
    echo Process XML_p.xml
    mkdir data
    xmltojson.py --path data --depth 1 --encoding cp437 XML_p.xml
fi

for FILE in data/*.jsonl
do
    ID=$(basename ${FILE} | sed 's/.jsonl$//')
    COUNT=$(count-documents.py ${ID} | jq -r ".${ID}")
    echo ${ID} ${COUNT}
    if [ x${COUNT} = x"missing" ]; then
	< ${FILE} parallel -j 1 --blocksize 32M --files --pipe -l 65536 cat | parallel "post-types.py {} --core ${ID} --seq {#} --set-schema --rename-id; rm {}; sleep 1"
        COUNT=0
    fi
    if [ x${COUNT} = "x0" ]; then
        echo Posting ${FILE} to Solr ${ID}
        < ${FILE} parallel -j 1 --blocksize 32M --files --pipe -l 65536 cat | parallel "post-types.py {} --core ${ID} --seq {#} --rename-id; rm {}; sleep 1"
    fi
done
