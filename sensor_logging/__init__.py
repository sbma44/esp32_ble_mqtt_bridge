import time
import json
import math
import os
import io
import gzip
import csv
import shutil
import sqlite3
import uuid
import threading
import queue
import socketserver
import logging
import http.server
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from statistics import median
from datetime import datetime

import paho.mqtt.client as mqtt

class MQTTHandler(object):
    def __init__(self, queue, host, redis_client):
        self.queue = queue
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
        self.queue.put(((str(uuid.uuid4()), 'insert'), (msg.topic, msg.payload)))

        if self.redis_client:
            self.redis_client.set(msg.topic, msg.payload.decode("utf-8"))
            self.redis_client.set('{}_last'.format(msg.topic), str(time.time()))

        logging.debug('MQTT message: {} - {}'.format(msg.topic, msg.payload.decode('utf-8')))

class DatabaseHandler(object):

    def __init__(self, rx_queue, tx_queue, s3_client, config = {}, filename = False):
        self.filename = filename
        self.s3_client = s3_client
        self.last_s3_upload = None
        self.rx_queue = rx_queue
        self.tx_queue = tx_queue
        self.conn = sqlite3.connect('file:sensor_logging?mode=memory&cache=shared', uri=True)

        self.db_lock = threading.Lock()
        self.s3_lock = threading.Lock()

        self.TRIM_INTERVAL = config.get('TRIM_INTERVAL', 60 * 60)
        self.FLUSH_INTERVAL = config.get('FLUSH_INTERVAL', 8 * 60 * 60)
        self.AGGREGATION_INTERVAL = config.get('AGGREGATION_INTERVAL', 5 * 60)
        self.S3_INTERVAL = config.get('S3_INTERVAL', 24 * 60 * 60)
        self.RETENTION_PERIOD = config.get('RETENTION_PERIOD', 7 * 24 * 60 * 60)
        self.S3_BUCKET = config.get('S3_BUCKET', 'sbma44')
        self.S3_PATH = config.get('S3_PATH', '137t/sensors/environment/')

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
            self.db_lock.acquire()
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
                self.db_lock.release()

    def loop(self, until=False):

        current_interval = math.floor(time.time() / self.S3_INTERVAL)
        last_s3_upload = current_interval
        last_flush = time.time()
        last_trim = time.time()

        # until exists to facilitate testing
        while (until is False) or (time.time() < until):

            # check to see if we need to flush to disk
            current_time = time.time()
            if current_time - last_flush >= self.FLUSH_INTERVAL:
                logging.info('flushing to disk')
                self.flush_to_disk()
                last_flush = current_time

            # check to see if we need to trim db
            if current_time - last_trim >= self.TRIM_INTERVAL:
                logging.info('trimming database')
                self.trim_database()
                last_trim = current_time

            # check to see if we need to upload to S3
            current_interval = math.floor(time.time() / self.S3_INTERVAL)
            if current_interval > last_s3_upload:
                logging.info('writing to S3')
                self.write_to_s3(current_interval)
                last_s3_upload = current_interval

            try:
                # Try to get a task from the queue without blocking
                task = self.rx_queue.get(block=False)
                logging.info(f"Processing task: {task}")

                (task_id, task_type) = task[0]
                payload = task[1]

                if (task_type == 'insert'):
                    self.insert(payload[0], payload[1])
                    self.rx_queue.task_done()

                elif (task_type == 'query'):
                    result = self.handle_time_series(payload)
                    self.tx_queue.put((task_id, result))
                    self.rx_queue.task_done()

                elif (task_type == 'ping'):
                    self.tx_queue.put((task_id, 'pong'))
                    self.rx_queue.task_done()

            except queue.Empty:
                # No task available, rest a bit and continue
                time.sleep(0.1)
                continue

    def insert(self, topic, value):
        self.db_lock.acquire()
        try:
            cur = self.conn.cursor()
            cur.execute('INSERT INTO data (t, topic, value) VALUES (?, ?, ?)',  (time.time(), topic, value))
            self.conn.commit()
        finally:
            self.db_lock.release()

    def handle_time_series(self, qsparams):
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

            cursor = self.conn.cursor()
            cursor.execute(sql, params)

            out[topic] = cursor.fetchall()

        return out

    def trim_database(self, since=None):
        logging.info('trimming database')

        if since is None:
            since = time.time() - self.RETENTION_PERIOD

        self.db_lock.acquire()
        try:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM data WHERE t < ?", (since,))
            self.conn.commit()
        finally:
            self.db_lock.release()

    def flush_to_disk(self):
        # save in-memory database to disk
        if os.path.exists('{}.tmp'.format(self.filename)):
            os.remove('{}.tmp'.format(self.filename))

        logging.info('attempting to store database to disk ({})'.format(self.filename))
        if self.filename:
            self.db_lock.acquire()
            try:
                dest = sqlite3.connect('{}.tmp'.format(self.filename))
                with dest:
                    self.conn.backup(dest)
                dest.close()
            except Exception as e:
                logging.error('error storing database to disk: {}'.format(e))
            finally:
                self.db_lock.release()
            shutil.move('{}.tmp'.format(self.filename), self.filename)


    def write_to_s3(self, interval = None):
        self.s3_lock.acquire()

        if interval is None:
            interval = math.floor(time.time() / self.S3_INTERVAL)

        try:
            logging.info('writing to S3')
            period_end = interval * self.S3_INTERVAL
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

        except Exception as e:
            raise e

        finally:
            self.s3_lock.release()

    def close(self):
        # Close the connection
        self.conn.close()

    def __del__(self):
        self.close()

class HttpServer(object):
    def __init__(self, port, db_rx, db_tx, flutter_config_filename):
        self.port = port
        self.db_rx = db_rx
        self.db_tx = db_tx
        self.flutter_config_filename = flutter_config_filename

    def start(self):
        with socketserver.TCPServer(("", self.port), self.handler_factory) as httpd:
            logging.info("Server started at localhost:" + str(self.port))
            httpd.serve_forever()

    # Define a factory function to create instances of MyHttpRequestHandler
    def handler_factory(self, *args, **kwargs):
        return HttpServer.MyHttpRequestHandler(self.db_rx, self.db_tx, self.flutter_config_filename, *args, **kwargs)

    class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, db_rx, db_tx, flutter_config_filename, request, client_address, server):
            self.db_rx = db_rx
            self.db_tx = db_tx
            self.flutter_config_filename = flutter_config_filename
            super().__init__(request, client_address, server)

        def empty_queue(self, q):
            while not q.empty():
                try:
                    message = q.get_nowait()
                    q.task_done()
                except queue.Empty:
                    break

        def do_GET(self):
            # Use the existing database connection
            parsed_path = urlparse(self.path)
            path = parsed_path.path
            qsparams = parse_qs(parsed_path.query)

            if path == '/flutter/config.json':
                try:
                    with open(self.flutter_config_filename) as f:
                        self.send_response(200)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(f.read().encode('utf-8'))
                except:
                    self.send_response(500)
                    self.end_headers()
                return

            elif path == '/time-series':
                self.empty_queue(self.db_rx)

                task_id = str(uuid.uuid4())
                self.db_rx.put(((task_id, 'query'), qsparams))

                try:
                    response = self.db_tx.get(timeout=10)
                    if response[0] != task_id:
                        logging.warn('task ID mismatch')
                        self.empty_queue(self.db_tx)
                        self.send_response(500)
                        self.send_header("Content-type", "text/html")
                        self.end_headers()
                        self.wfile.write(b"task ID mismatch")
                        return
                    data = response[1]
                except queue.Empty:
                    return self.send_timeout()

            elif path == '/ping':
                self.empty_queue(self.db_rx)

                task_id = str(uuid.uuid4())
                self.db_rx.put(((task_id, 'ping'), {}))
                try:
                    response = self.db_tx.get(timeout=10)
                    if response[0] != task_id:
                        logging.warn('task ID mismatch')
                        self.cleanup_queue(self.db_tx)

                    self.send_response(200)
                    self.send_header("Content-type", "text/html")
                    self.end_headers()
                    self.wfile.write(response[1].encode('utf-8'))
                    return

                except queue.Empty:
                    return self.send_timeout()
            else:
                self.send_response(404)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"404 Not Found")
                return

            # Prepare the response
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            # Send the response
            self.wfile.write(json.dumps(data).encode('utf-8'))

        def send_timeout(self):
            self.send_response(408)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"Exceeded timeout waiting for DB handler response")
