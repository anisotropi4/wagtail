#!/bin/sh 

export SOLRHOST=localhost
export PYTHONUNBUFFERED=1
export PATH=${PATH}:../bin

for DIRECTORY in data schedule storage app
do
    if [ ! -d ${DIRECTORY} ]; then
        mkdir ${DIRECTORY}
    fi
done

if [ ! -f app/solr.py ]; then
    ln ../bin/app/solr.py app/solr.py
fi

URL="https://networkrail.opendata.opentraintimes.com/mirror/schedule/cif/"

echo Download CIF files
echo Get file list
if [ ! -f full-file-list.txt ]; then
    curl -s -L -G  ${URL} | \
    htmltojson.py --stdout --depth 3 | jq -cr '.tbody?[]? | .[].td[] | select(.a?.title?) | .a.title' > full-file-list.txt  
fi

LINE=$(fgrep -n _full.gz full-file-list.txt | tail -1)
if [ ! -f file-list.txt ]; then
    N=$(echo ${LINE} | cut -d':' -f1)
    tail -n +${N} full-file-list.txt > file-list.txt
fi

for FILENAME in $(cat file-list.txt | sed 's/.gz$//')
do
    echo Process ${FILENAME} CIF file
    if [ ! -f data/${FILENAME} ]; then
        echo Download ${FILENAME} CIF file
        curl -o data/${FILENAME}.gz ${URL}/${FILENAME}.gz
        gzip -d data/${FILENAME}.gz
    fi
done

DATESTRING=$(tail -1 file-list.txt | cut -d'_' -f1 | cut -d':' -f2 | cut -c1-8)
echo ${DATESTRING}

echo Create timetable for ${DATESTRING}
echo Split CIF files
if [ ! -f output/HD_001 ]; then
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
        COUNT=0
    fi
    if [ x${COUNT} = "x0" ]; then
        #ls storage/${ID}_???.jsonl | parallel post-simple.py {} --core ${ID}
        ls storage/${ID}_???.jsonl | parallel post-types.py {} --core ${ID}
    fi
done

echo Create PT-${DATESTRING}-7.jsonl timetable file
if [ ! -f PT-${DATESTRING}-7.jsonl ]; then
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
    #cat PT-${DATESTRING}-7.jsonl | parallel --block 8M --pipe --cat post-simple.py --core ${ID} {}
    cat PT-${DATESTRING}-7.jsonl | parallel --block 8M --pipe --cat post-types.py --core ${ID} {}
fi
