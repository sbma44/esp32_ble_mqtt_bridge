import os
import logging
import threading
import queue

import boto3
import redis

from sensor_logging import DatabaseHandler, HttpServer, MQTTHandler
from sensor_logging.local_settings import *

log_level = os.getenv('LOG_LEVEL', 'WARNING').upper()
numeric_level = getattr(logging, log_level, None)
if not isinstance(numeric_level, int):
    raise ValueError(f'Invalid log level: {log_level}')

# Configure logging
# Example format: "2021-01-01 12:00:00,000 - name - LEVEL - Message"
logging.basicConfig(level=numeric_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def start_httpd(port, db_rx, db_tx):
    httpd = HttpServer(port, db_rx, db_tx)
    httpd.start()

def start_db(db_rx, db_tx, s3_client, config, filename):
    db = DatabaseHandler(db_rx, db_tx, s3_client, config, filename)
    db.loop()

def start_mqtt(db_rx, mqtt_host, redis_client):
    mqtt = MQTTHandler(db_rx, mqtt_host, redis_client)

if __name__ == '__main__':
    db_rx = queue.Queue()
    db_tx = queue.Queue()
    s3_client = boto3.client('s3', region_name=AWS_DEFAULT_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    redis_client = None
    if REDIS_HOST:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    config = {
        'TRIM_INTERVAL': TRIM_INTERVAL,
        'AGGREGATION_INTERVAL': AGGREGATION_INTERVAL,
        'S3_INTERVAL': S3_INTERVAL,
        'RETENTION_PERIOD': RETENTION_PERIOD,
        'S3_BUCKET': S3_BUCKET,
        'S3_PATH': S3_PATH
    }

    logging.info('starting database')
    db_thread = threading.Thread(target=start_db, args=(db_rx, db_tx, s3_client, config, SQLITE_FILENAME), daemon=True)
    db_thread.start()

    logging.info('starting http')
    http_thread = threading.Thread(target=start_httpd, args=(HTTP_PORT, db_rx, db_tx), daemon=True)
    http_thread.start()

    logging.info('starting mqtt')
    start_mqtt(db_rx, MQTT_HOST, redis_client)