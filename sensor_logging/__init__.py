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
import logging
import http.server
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from statistics import median
from datetime import datetime

import paho.mqtt.client as mqtt

lock = threading.Lock()
s3_lock = threading.Lock()

class MQTTHandler(object):
    def __init__(self, host, db, redis_client):
        self.db = db
        self.redis_client = redis_client

        self.client = mqtt.Client('sensor_logging_api')
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(host)
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        self.client.subscribe('xiaomi_mijia/#')
        self.client.subscribe('xmas/#')
        self.client.subscribe('co2/#')
        self.client.subscribe('aq/#')

    def on_message(self, client, userdata, msg):
        self.db.insert(msg.topic, msg.payload)
        self.redis_client.set(msg.topic, msg.payload.decode("utf-8"))
        self.redis_client.set('{}_last'.format(msg.topic), str(time.time()))
        logging.debug('MQTT message: {} - {}'.format(msg.topic, msg.payload.decode('utf-8')))

class DatabaseHandler(object):

    @staticmethod
    def get_sqlite_connection():
        return sqlite3.connect('file:sensor_logging?mode=memory&cache=shared', uri=True)

    def __init__(self, s3_client, config = {}, filename = False):
        self.filename = filename
        self.s3_client = s3_client
        self.last_s3_upload = None

        self.FLUSH_INTERVAL = config.get('FLUSH_INTERVAL', 600)
        self.AGGREGATION_INTERVAL = config.get('AGGREGATION_INTERVAL', 5 * 60)
        self.S3_INTERVAL = config.get('S3_INTERVAL', 24 * 60 * 60)
        self.RETENTION_PERIOD = config.get('RETENTION_PERIOD', 7 * 24 * 60 * 60)
        self.S3_BUCKET = config.get('S3_BUCKET', 'sbma44')
        self.S3_PATH = config.get('S3_PATH', '137t/sensors/environment/')

        self.conn = DatabaseHandler.get_sqlite_connection()

        if filename and os.path.exists(self.filename):
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
            lock.acquire()
            try:
                sql = """
                            CREATE TABLE data (
                                t NUMERIC,
                                topic TEXT,
                                value NUMERIC
                            )
                    """
                cur.execute(sql)
            finally:
                lock.release()

        self.last_flush = time.time()

    def insert(self, topic, value):
        lock.acquire()
        try:
            cur = self.conn.cursor()
            cur.execute('INSERT INTO data (t, topic, value) VALUES (?, ?, ?)',  (time.time(), topic, value))
            self.conn.commit()
        finally:
            lock.release()

        # flush to disk in a separate thread executor
        if time.time() - self.last_flush > self.FLUSH_INTERVAL:
            self.flush_to_disk()

        # write to s3 if enough time has passed
        self.write_to_s3()

    def flush_to_disk(self):
        # save in-memory database to disk
        if os.path.exists('{}.tmp'.format(self.filename)):
            os.remove('{}.tmp'.format(self.filename))

        logging.info('attempting to store database to disk ({})'.format(self.filename))
        if self.filename:
            lock.acquire()
            try:
                dest = sqlite3.connect('{}.tmp'.format(self.filename))
                with dest:
                    self.conn.backup(dest)
                dest.close()
            except Exception as e:
                logging.error('error storing database to disk: {}'.format(e))
            finally:
                lock.release()
            shutil.move('{}.tmp'.format(self.filename), self.filename)
            self.last_flush = time.time()

        logging.info('trimming database')
        lock.acquire()
        try:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM data WHERE t < ?", (time.time() - self.RETENTION_PERIOD,))
            self.conn.commit()
        finally:
            lock.release()


    def write_to_s3(self):
        current_interval = math.floor(time.time() / self.S3_INTERVAL)
        if self.last_s3_upload is None:
            self.last_s3_upload = current_interval

        if current_interval > self.last_s3_upload:
            s3_lock.acquire()
            try:
                logging.info('writing to S3')

                period_end = current_interval * self.S3_INTERVAL
                period_start = period_end - self.S3_INTERVAL

                cur = self.conn.cursor()
                sql = "SELECT DISTINCT topic FROM data WHERE t >= ? AND t < ? ORDER BY topic ASC"
                cur.execute(sql, (period_start, period_end))
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
                    subperiod_end = subperiod_start + self.AGGREGATION_INTERVAL
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
                    subperiod_start = subperiod_start + self.AGGREGATION_INTERVAL

                # prepare to upload artifacts to S3
                date_string = datetime.fromtimestamp(period_start).isoformat()

                # upload csv
                csv_output.seek(0)
                csv_gz = gzip.compress(csv_output.read().encode('utf-8'))
                self.s3_client.put_object(Body=csv_gz, Bucket=self.S3_BUCKET, Key='{}sensor_logging_{}.csv.gz'.format(self.S3_PATH, date_string))

                # upload json
                json_output.seek(0)
                json_gz = gzip.compress(json_output.read().encode('utf-8'))
                self.s3_client.put_object(Body=json_gz, Bucket=self.S3_BUCKET, Key='{}sensor_logging_{}.json.gz'.format(self.S3_PATH, date_string))

                # Close the StringIO object
                csv_output.close()
                json_output.close()

                self.last_s3_upload = current_interval

            except Exception as e:
                raise e

            finally:
                s3_lock.release()

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
            logging.info("Server started at localhost:" + str(self.port))
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

            topics = qsparams.get('topic', ['xiaomi_mijia/M_BKROOM/temperature'])
            chunk = int(qsparams.get('chunk', [60])[0])
            since = float(qsparams.get('since', [24 * 60 * 60])[0])
            until = float(qsparams.get('until', [False])[0])

            out = {}
            for (i, topic) in enumerate(topics):
                topic = topic.strip()

                sql = "SELECT (round(t / ?) * ?), AVG(value) FROM data WHERE topic = ?"
                params = [chunk, chunk, topic]
                if since:
                    sql += " AND t > ?"
                    params.append(since)
                if until:
                    sql += " AND t < ?"
                    params.append(until)
                sql += " GROUP BY round(t / ?) ORDER BY 1 ASC"
                params.append(chunk)

                cursor.execute(sql, params)

                out[topic] = cursor.fetchall()

            return out

        def handle_average(self, cursor, qsparams):
            cursor.execute("SELECT AVG(value) FROM data")
            return cursor.fetchall()

    # Define a factory function to create instances of MyHttpRequestHandler
    def handler_factory(self, *args, **kwargs):
        return self.MyHttpRequestHandler(*args, **kwargs)
