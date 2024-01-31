import csv, json, sys, datetime
import pytz

EST = pytz.timezone('US/Eastern')

keys = {}
with open(sys.argv[1]) as f:
    for line in f.readlines():
        j = json.loads(line)
        for k in j:
            if k == 't':
                continue
            keys[k] = True

fields = list(keys.keys())
fields.sort()
fields = ['t'] + fields
writer = csv.writer(sys.stdout)
writer.writerow(fields)

with open(sys.argv[1]) as f:
    for line in f.readlines():
        row = []
        j = json.loads(line)
        for f in fields:
            row.append(j.get(f, ''))
        row[0] = datetime.datetime.fromtimestamp(row[0]).astimezone(EST).isoformat()
        writer.writerow(row)

