#!/bin/bash

export PYTHONUNBUFFERED=1
export PATH=${PATH}:../bin

for DIRECTORY in app data output storage
do
    if [ ! -d ${DIRECTORY} ]; then
        mkdir ${DIRECTORY}
    fi
done

if [ ! -s app/solr.py ]; then
    ln ../bin/app/solr.py app/solr.py
fi

URL="https://networkrail.opendata.opentraintimes.com/mirror/schedule/cif/"

echo Download CIF files
echo Get file list
if [ ! -s full-file-list.txt ]; then
    curl -k -s -L -G  ${URL} | html2json.py | \
        jq -r '.[]."File Name"' > full-file-list.txt
fi

LINE=$(fgrep -n _full.gz full-file-list.txt | tail -1)
if [ ! -s file-list.txt ]; then
    N=$(echo ${LINE} | cut -d':' -f1)
    tail -n +${N} full-file-list.txt > file-list.txt
fi

for FILENAME in $(cat file-list.txt | sed 's/.gz$//')
do
    echo Process ${FILENAME} CIF file
    if [ ! -s data/${FILENAME} ]; then
        echo Download ${FILENAME} CIF file
        curl -k -o data/${FILENAME}.gz ${URL}/${FILENAME}.gz
        gzip -d data/${FILENAME}.gz
    fi
done

DATESTRING=$(tail -1 file-list.txt | cut -d'_' -f1 | cut -d':' -f2 | cut -c1-8)
echo ${DATESTRING}

echo Create timetable for ${DATESTRING}
echo Split CIF files
if [ ! -s output/HD_001 ]; then
    if [ -d output ]; then
        rm -rf output
    fi
    mkdir output
    for FILENAME in $(cat file-list.txt | sed 's/.gz$//')
    do
        cat data/${FILENAME}
    done | ./wtt-split.py
fi

echo Convert ${DATESTRING} CIF files to jsonl
if [ ! -s storage/HD_001.jsonl ]; then
    echo Create ${DATESTRING} jsonl files
    ls output/*_??? | parallel ./wtt9.py
fi

echo Converted timetable-${DATESTRING} CIF files to jsonl
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

for ID in AA BS CR HD PA PATH TR ZZ
do
    COUNT=$(count-documents.py ${ID} | jq -r ".${ID}")
    echo ${ID} ${COUNT}
    if [ x${COUNT} = x"missing" ]; then
	for FILE in $(ls storage/${ID}_???.jsonl)
	do
            echo Set Schema ${FILE} to Solr ${ID}
	    < ${FILE} parallel -j 1 --blocksize 8M --files --pipe -l 4096 cat | parallel "post-types.py {} --core ${ID} --seq {#} --rename-id --set-schema; rm {}; sleep 1"
	done
        COUNT=0
    fi
    if [ x${COUNT} = "x0" ]; then
        ls storage/${ID}_???.jsonl | parallel post-types.py {} --core ${ID}
    fi
done

echo Create PT-${DATESTRING}-7.jsonl timetable file
if [ ! -s PT-${DATESTRING}-7.jsonl ]; then
    ./wtt-timetable7.py > PT-${DATESTRING}-7.jsonl
fi
echo Created PT-${DATESTRING}-7.jsonl timetable file

ID=PT
COUNT=$(count-documents.py ${ID} | jq -r ".${ID}")
echo ${ID} ${COUNT}
if [ x${COUNT} = x"missing" ]; then
    COUNT=0
fi

if [ x${COUNT} = "x0" ]; then
    echo Post ${ID} json files to Solr
    cat PT-${DATESTRING}-7.jsonl | parallel --block 8M --pipe --cat "post-types.py --core ${ID} --set-schema"
    cat PT-${DATESTRING}-7.jsonl | parallel --block 8M --pipe --cat post-types.py --core ${ID} {}
fi
