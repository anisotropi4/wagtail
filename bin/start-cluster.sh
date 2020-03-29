#!/bin/sh

if [ x$(which docker-compose) = x ]; then
    echo ERROR start-cluster.sh: no docker-compose available
    exit 1
fi
docker-compose start
