#!/bin/bash

set -eu -o pipefail

USER=pi
HOST='192.168.1.2'
JSONS="$(ssh ${USER}@${HOST} 'find /home/${USER}/sensor_logging -type f -name *.json | grep -v \.venv | sort -r')"
rm -f /tmp/xmas.csv
TAIL_CMD=""
for j in $JSONS; do
    ssh ${USER}@${HOST} "cd $(dirname ${j}) && python3 /home/${USER}/sensor_logging/csvify.py ${j} ${TAIL_CMD}" | xsv select 1,10,11 | tr ',' '\t' >> /tmp/xmas.csv
    TAIL_CMD="| tail -n+2"
done
pbcopy < /tmp/xmas.csv
