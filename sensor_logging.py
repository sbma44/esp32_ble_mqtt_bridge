import time
import json
import math
import os
import gzip
import boto3
import paho.mqtt.client as mqtt
from collections import defaultdict
from statistics import median
from datetime import datetime
import shutil
from local_settings import *

class Handler(object):

    def __init__(self, s3_client):
        self.data = defaultdict(list)
        self.last_segment = 0
        self.s3_client = s3_client

    def rotate(self):
        current_date = datetime.now().isoformat().split('T')[0]
        moved_something = False
        for fn in os.listdir(LOG_PATH):
            if fn.split('.')[-1] != 'json':
                continue
            d = '.'.join(os.path.basename(fn).split('.')[:-1])
            if d != current_date:
                upload_successful = False
                try:
                    with open(os.path.join(LOG_PATH, fn), 'rb') as f:
                        gz = gzip.compress(f.read())
                        self.s3_client.put_object(Body=gz, Bucket=S3_BUCKET, Key='{}50q_temp_humidity_{}.json.gz'.format(S3_PATH,d))
                    upload_successful = True
                except:
                    print('upload failed!')
                if upload_successful:
                    shutil.move(os.path.join(LOG_PATH, fn), os.path.join(LOG_PATH, 'archive', fn))
                    moved_something = True

        # rotate locally stored files
        if moved_something:
            files = [os.path.basename(f) for f in os.listdir(os.path.join(LOG_PATH, 'archive'))]
            files.sort(reverse=True)
            if len(files) > RETENTION_COUNT:
                for fn in files[RETENTION_COUNT:]:
                    os.unlink(os.path.join(LOG_PATH, 'archive', fn))

    def on_message(self, client, userdata, message):
        t = time.time()
        current_segment = math.floor(t / SEGMENT_LENGTH)
        if current_segment != self.last_segment:
            if len(self.data):
                out = {}
                for k in sorted(self.data.keys()):
                    out[k] = median(self.data[k])
                out['t'] = current_segment * SEGMENT_LENGTH
                out['t'] = current_segment * SEGMENT_LENGTH
                with open(os.path.join(LOG_PATH, '{}.json'.format(datetime.fromtimestamp(self.last_segment * SEGMENT_LENGTH).isoformat().split('T')[0])), 'a') as f:
                    f.write(json.dumps(out))
                    f.write('\n')
            self.data = defaultdict(list)
            self.last_segment = current_segment

            # check to see if logs should be uploaded/rotated
            self.rotate()

        self.data['_'.join(message.topic.split('/')[1:])].append(float(message.payload.decode("utf-8")))

if __name__ == '__main__':
    s3_client = boto3.client('s3', region_name=AWS_DEFAULT_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    h = Handler(s3_client)

    client = mqtt.Client('temp_logger')
    client.connect(MQTT_HOST)
    client.subscribe('xiaomi_mijia/#')
    client.subscribe('xmas/#')
    client.subscribe('co2/#')
    client.subscribe('aq/#')
    client.on_message = h.on_message
    client.loop_forever()
