#!/bin/bash

CORENAMES=$@

if [ x"${CORENAMES}" = x ]; then
    echo "Error delete-core.sh: no CORENAME(s) supplied"
    exit 1
fi
echo Check if Solr running

while ! (docker exec --user=solr solr-instance bin/solr status -p 8983)
do
    echo Solr is not running
    docker start solr-instance
    sleep 5
done

for CORENAME in ${CORENAMES}
do
    echo Delete Solr core ${CORENAME}
    echo

    docker exec -it --user=solr solr-instance bin/solr delete -c ${CORENAME}
    echo Deleted ${CORENAME}
done
