#!/bin/sh

# Enable logging to Monitrix (ELK):
if [ "$MONITRIX_ENABLED" ]; then
    echo Attempting to send logs to Monitrix
    filebeat -v -e -c /etc/filebeat/filebeat.yml & 
else
	echo Monitrix crawl logging disabled
fi

# Set up variables, using defaults if unset:
: ${HERITRIX_USER:=heritrix}
: ${HERITRIX_PASSWORD:=heritrix}

# And fire it up:
./h3-bin/bin/heritrix -a $HERITRIX_USER:$HERITRIX_PASSWORD -b 0.0.0.0 -j /jobs 
# not supported properly: -r ${JOB_NAME:-frequent}
