import time
import json
import math
import os
import io
import gzip
import csv
import shutil
import sqlite3
import threading
import socketserver
import http.server
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from statistics import median
from datetime import datetime

import boto3
import redis
import paho.mqtt.client as mqtt

from sensor_logging.local_settings import *

lock = threading.Lock()
s3_lock = threading.Lock()

class MQTTHandler(object):
    def __init__(self, host, db, redis_client):
        self.db = db
        self.redis_client = redis_client

        self.client = mqtt.Client('sensor_logging_api')
        self.client.connect(host)
        self.client.subscribe('xiaomi_mijia/#')
        self.client.subscribe('xmas/#')
        self.client.subscribe('co2/#')
        self.client.subscribe('aq/#')
        self.client.on_message = self.on_message
        self.client.loop_forever()

    def on_message(self, client, userdata, msg):
        self.db.insert(msg.topic, msg.payload)
        self.redis_client.set(msg.topic, msg.payload.decode("utf-8"))

class DatabaseHandler(object):

    @staticmethod
    def get_sqlite_connection():
        return sqlite3.connect('file:sensor_logging?mode=memory&cache=shared', uri=True)

    def __init__(self, filename, s3_client):
        self.filename = filename
        self.s3_client = s3_client
        self.last_s3_upload = 0

        self.conn = DatabaseHandler.get_sqlite_connection()

        if os.path.exists(self.filename):
            # open existing file
            source = sqlite3.connect(self.filename)
            source.backup(self.conn)
            source.close()

        cur = self.conn.cursor()

        query = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name='data'"
        cur.execute(query)

        # If count is 1, then table exists
        res = cur.fetchone()
        if int(res[0]) != 1:
            with lock:
                sql = """
                            CREATE TABLE data (
                                t NUMERIC,
                                topic TEXT,
                                value NUMERIC
                            )
                    """
                cur.execute(sql)

        self.last_flush = time.time()

    def insert(self, topic, value):
        with lock:
            cur = self.conn.cursor()
            cur.execute('INSERT INTO data (t, topic, value) VALUES (?, ?, ?)',  (time.time(), topic, value))
            self.conn.commit()

            if time.time() - self.last_flush > FLUSH_INTERVAL:
                self.flush_to_disk()
                self.last_flush = time.time()

    def flush_to_disk(self):
        # save in-memory database to disk
        if os.path.exists('{}.tmp'.format(self.filename)):
            os.remove('{}.tmp'.format(self.filename))

        with lock:
            cur = self.conn.cursor()
            cur.execute("VACUUM main INTO '{}.tmp'".format(self.filename))
            cur.execute("DELETE FROM data WHERE t < ?", (time.time() - self.RETENTION_PERIOD,))
            self.conn.commit()

        shutil.move('{}.tmp'.format(self.filename), self.filename)

    def write_to_s3(self):
        current_interval = math.floor(time.time() / S3_INTERVAL)
        if current_interval > self.last_s3_upload:
            with s3_lock:
                period_start = current_interval * S3_INTERVAL
                period_end = period_start + S3_INTERVAL

                cur = self.conn.cursor()
                cur.execute("SELECT DISTINCT topic FROM data WHERE t >= ? AND t < ? ORDER BY topic ASC", (period_start, period_end))
                topics = [x[0] for x in cur.fetchall()]

                csv_output = io.StringIO()
                json_output = io.StringIO()
                writer = csv.writer(csv_output)
                writer.writerow(['t'] + list(sorted(topics)))

                median_sql = """
                    SELECT DISTINCT topic,
                        AVG(
                            CASE counter % 2
                            WHEN 0 THEN CASE WHEN rn IN (counter / 2, counter / 2 + 1) THEN value END
                            WHEN 1 THEN CASE WHEN rn = counter / 2 + 1 THEN value END
                            END
                        ) OVER (PARTITION BY topic) median
                    FROM (
                    SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY topic ORDER BY value) rn,
                            COUNT(*) OVER (PARTITION BY topic) counter
                    FROM data WHERE t >= ? AND t < ?
                    )"""

                # retrieve median values for each topic in each subperiod
                csv_rows = []
                subperiod_start = period_start
                while subperiod_start < period_end:
                    subperiod_end = subperiod_start + AGGREGATION_INTERVAL

                    this_row = {}
                    cur.execute(median_sql, (subperiod_start, subperiod_end))
                    for row in cur.fetchall():
                        this_row[row[0]] = float(row[1])
                    csv_rows.append(this_row)

                    json_row = this_row.copy()
                    json_row['t'] = subperiod_start
                    json_output.write(json.dumps(json_row) + '\n')

                    subperiod_start = subperiod_end

                # organize into CSV
                subperiod_start = period_start
                for row in csv_rows:
                    new_row = []
                    for topic in sorted(topics):
                        new_row.append(row.get(topic, ''))
                    writer.writerow(new_row)
                    subperiod_start = subperiod_start + AGGREGATION_INTERVAL

                # prepare to upload artifacts to S3
                date_string = datetime.fromtimestamp(subperiod_start).isoformat()

                # upload csv
                csv_output.seek(0)
                csv_gz = gzip.compress(csv_output.read().encode('utf-8'))
                self.s3_client.put_object(Body=csv_gz, Bucket=S3_BUCKET, Key='{}sensor_logging_{}.csv.gz'.format(S3_PATH, date_string))

                # upload json
                json_output.seek(0)
                json_gz = gzip.compress(json_output.read().encode('utf-8'))
                self.s3_client.put_object(Body=json_gz, Bucket=S3_BUCKET, Key='{}sensor_logging_{}.json.gz'.format(S3_PATH, date_string))

                # Close the StringIO object
                csv_output.close()
                json_output.close()

                self.last_s3_upload = current_interval

    def close(self):
        self.flush_to_disk()

        # Close the connection
        self.conn.close()

class HttpServer(object):
    def __init__(self, port):
        self.db = DatabaseHandler.get_sqlite_connection()
        self.port = port

    def start(self):
        with socketserver.TCPServer(("", self.port), self.handler_factory) as httpd:
            print("Server started at localhost:" + str(self.port))
            httpd.serve_forever()

    class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, request, client_address, server, some_custom_argument=None):
            super().__init__(request, client_address, server)

        def do_GET(self):
            # Use the existing database connection
            db = DatabaseHandler.get_sqlite_connection()
            cursor = db.cursor()

            parsed_path = urlparse(self.path)
            path = parsed_path.path
            qsparams = parse_qs(parsed_path.query)

            if path == '/time-series':
                data = self.handle_time_series(cursor, qsparams)
            elif path == '/average':
                data = self.handle_average(cursor, qsparams)
            else:
                self.send_response(404)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"404 Not Found")
                return

            db.close()

            # Prepare the response
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            # Send the response
            response = json.dumps(data)
            self.wfile.write(response.encode('utf-8'))

        def handle_time_series(self, cursor, qsparams):
            topics = ['xiaomi_mijia/M_BKROOM/temperature']
            try:
                topics = qsparams.get('topic', topics)
            except:
                pass

            out = {}
            for (i, topic) in enumerate(topics):
                topic = topic.strip()

                chunk = 60
                try:
                    chunk = int(qsparams.get('chunk', [chunk])[0])
                except:
                    pass

                since = time.time() - 24 * 60 * 60
                try:
                    since = float(qsparams.get('since', [since])[0])
                except:
                    pass

                until = time.time() + 999
                try:
                    until = float(qsparams.get('until', [until])[0])
                except:
                    pass

                cursor.execute("SELECT (round(t / ?) * ?), AVG(value) FROM data WHERE topic = ? AND t > ? AND t < ? GROUP BY round(t / ?) ORDER BY 1 ASC", (chunk, chunk, topic, since, until, chunk))

                out[topic] = cursor.fetchall()

            return out

        def handle_average(self, cursor, qsparams):
            cursor.execute("SELECT AVG(value) FROM data")
            return cursor.fetchall()

    # Define a factory function to create instances of MyHttpRequestHandler
    def handler_factory(self, *args, **kwargs):
        return self.MyHttpRequestHandler(*args, **kwargs)

def run_http(port):
    httpd = HttpServer(port)
    httpd.start()

if __name__ == '__main__':
    s3_client = boto3.client('s3', region_name=AWS_DEFAULT_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

    print('starting database')
    db = DatabaseHandler(SQLITE_FILENAME, s3_client)

    print('starting http')
    http_thread = threading.Thread(target=run_http, args=(HTTP_PORT,))
    http_thread.start()

    print('starting mqtt')
    mqtt = MQTTHandler(MQTT_HOST, db, redis_client)
