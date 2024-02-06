import os
import logging
import threading

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

def run_http(port):
    httpd = HttpServer(port)
    httpd.start()

if __name__ == '__main__':
    s3_client = boto3.client('s3', region_name=AWS_DEFAULT_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

    logging.info('starting database')
    config = {
        'FLUSH_INTERVAL': FLUSH_INTERVAL,
        'AGGREGATION_INTERVAL': AGGREGATION_INTERVAL,
        'S3_INTERVAL': S3_INTERVAL,
        'RETENTION_PERIOD': RETENTION_PERIOD,
        'S3_BUCKET': S3_BUCKET,
        'S3_PATH': S3_PATH
    }
    db = DatabaseHandler(s3_client, config, SQLITE_FILENAME)

    logging.info('starting http')
    http_thread = threading.Thread(target=run_http, args=(HTTP_PORT,))
    http_thread.start()

    logging.info('starting mqtt')
    mqtt = MQTTHandler(MQTT_HOST, db, redis_client)
