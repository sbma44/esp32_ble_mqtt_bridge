#!/bin/bash

set -eu -o pipefail

JSONS="$(find $(dirname $0) -type f -name '*.json' | grep -v \.venv | sort -r)"
cat $JSONS > /tmp/xmas.json
python3 $(dirname $0)/csvify.py /tmp/xmas.json > /tmp/xmas.csv
head -n1 /tmp/xmas.csv > /tmp/xmas.csv.tmp
tail -n+2 /tmp/xmas.csv | sort >> /tmp/xmas.csv.tmp
mv /tmp/xmas.csv.tmp /tmp/xmas.csv
gzip -f /tmp/xmas.csv

$(dirname $0)/../.local/bin/aws s3 cp /tmp/xmas.csv.gz s3://sbma44/137t/sensors/environment/xmas-tree.csv --profile sensors --acl=public-read --cache-control  no-cache --content-encoding gzip
rm -f /tmp/xmas.csv
