#!/bin/sh

export PYTHONUNBUFFERED=1
export PATH=${PATH}:../bin

for DIRECTORY in app output
do
    if [ ! -d ${DIRECTORY} ]; then
        mkdir ${DIRECTORY}
    fi
done

if [ ! -s app/solr.py ]; then
    ln ../bin/app/solr.py app/solr.py
fi

URL="http://naptan.app.dft.gov.uk/Datarequest/naptan.ashx"
if [ ! -s naptan.zip ]; then
    wget -O naptan.zip ${URL}
fi

if [ ! -s Naptan.xml ]; then
    unzip naptan.zip
fi

if [ ! -s output/StopArea.jsonl ]; then
    xmltojson.py Naptan.xml --path output --depth 2
fi

echo Check docker cluster is running
count-documents.py _test_ 2> /dev/null
if [ "$?" != "0" ]; then
    start-cluster.sh
    if [ "$?" != "0" ]; then
        echo ERROR run.sh: unable to start Solr docker cluster
        echo
        echo NOTE: in the wagtail root directory to activate python virtual environment:
        echo $ source venv/bin/activate
        echo
        exit 2
    fi
    while true
    do
        count-documents.py _test_ 2> /dev/null
        if [ "$?" = "0" ]; then
            break
        fi
    done
fi

for FILE in output/*.jsonl
do
    ID=$(basename ${FILE} | sed 's/.jsonl$//')
    COUNT=$(count-documents.py ${ID} | jq -r ".${ID}")
    echo ${ID} ${COUNT}
    if [ x${COUNT} = x"missing" ]; then
        echo Set Schema ${FILE} to Solr ${ID}
        < ${FILE} parallel -j 1 --blocksize 8M --files --pipe -l 4096 cat | parallel "post-types.py {} --core ${ID} --seq {#} --rename-id --set-schema; rm {}"
        COUNT=0
    fi
    if [ x${COUNT} = "x0" ]; then
        echo Posting ${FILE} to Solr ${ID}
        < ${FILE} parallel -j 1 --blocksize 8M --files --pipe -l 4096 cat | parallel "post-types.py {} --core ${ID} --seq {#} --rename-id; rm {}; sleep 1"
        create-geo.py ${ID}
    fi
done
