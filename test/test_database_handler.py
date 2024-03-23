import unittest
from unittest.mock import patch, MagicMock
import time, math, gzip, os, json, tempfile, filecmp
import queue
import logging
import uuid
import threading
from sensor_logging import DatabaseHandler
from test import Accumulator, enable_fixtures

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

@enable_fixtures
class TestDatabaseHandler(unittest.TestCase):
    # @patch('sensor_logging.DatabaseHandler.flush_to_disk')
    def setUp(self):
        self.mock_s3_client = MagicMock()

        self.task_queue = queue.Queue()
        self.response_queue = queue.Queue()

        self.config = {
            'TRIM_INTERVAL': 60 * 60, # trim the database ever hour
            'FLUSH_INTERVAL': 8 * 60 * 60, # flush the database every 8 hours
            'AGGREGATION_INTERVAL': 45 * 60, # store data in 45 minute chunks
            'S3_INTERVAL': 24 * 60 * 60, # upload data 1x/day
            'RETENTION_PERIOD': 7 * 24 * 60 * 60, # only keep data for 7 days
            'S3_BUCKET': 'test-bucket',
            'S3_PATH': 'test-path/'
        }

        self.tempdir = tempfile.TemporaryDirectory()
        self.flush_filename = os.path.join(self.tempdir.name, 'test.db')

        self.db_handler = DatabaseHandler(self.task_queue, self.response_queue, self.mock_s3_client, self.config, self.flush_filename)

    def __del__(self):
        self.tempdir.cleanup()

    def reset_database_contents(self):
        cursor = self.db_handler.conn.cursor()
        cursor.execute('DELETE FROM data')
        self.db_handler.conn.commit()
        self.assertEqual(self.count_entries(), 0)

    def count_entries(self):
        cursor = self.db_handler.conn.cursor()
        return cursor.execute('SELECT COUNT(*) FROM data').fetchone()[0]

    @patch('time.time')
    def test_001_basic_inserts(self, mock_time):
        logging.info('test_001_basic_inserts')

        mock_time.return_value = 1620000000

        _insert = self.db_handler.insert
        self.db_handler.insert = MagicMock()

        self.reset_database_contents()

        db_thread = threading.Thread(target=self.db_handler.loop, kwargs={'until': 1620000000 + 100}, daemon=True)
        db_thread.start()

        self.task_queue.put(((1, 'insert'), ('topic1', 1)))
        self.task_queue.put(((2, 'insert'), ('topic2', 2)))
        self.task_queue.put(((3, 'insert'), ('topic3', 3)))

        # wait for db_thread to finish processing tasks
        while len(self.db_handler.insert.mock_calls) < 3:
            time.sleep(0.1)

        mock_time.return_value = 1620000000 + 101

        db_thread.join()

        self.compare_to_fixture(self.db_handler.insert.mock_calls, 'fixtures/db_handler_basic_inserts.json')

        self.db_handler.insert = _insert

    @patch('time.time')
    def test_002_handle_time_series(self, mock_time):

        logging.info('test_002_handle_time_series')

        start_time = 1620000000
        duration = 24 * 60 * 60

        mock_time.return_value = start_time
        self.reset_database_contents()

        acc = Accumulator()
        while mock_time() < (start_time + duration):
            self.db_handler.insert('topic1', acc.get())
            self.db_handler.insert('topic2', acc.get())
            self.db_handler.insert('topic3', acc.get())
            mock_time.return_value += 900

        for start in (0, 60 * 60 * 4, 60 * 60 * 8):
            for until in (60 * 60 * 1, 60 * 60 * 3, 60 * 60 * 5):
                for chunk in (60 * 15, 60 * 30, 60 * 45):
                    qsparams = {
                        'start': [start_time + start],
                        'chunk': [chunk],
                        'topic': ['topic1', 'topic2', 'topic3'],
                        'until': [start_time + start + until]
                    }
                    self.compare_to_fixture(self.db_handler.handle_time_series(qsparams), f'fixtures/db_handler_handle_time_series_{chunk}_{start}_{until}.json')

        qsparams = {
            'start': [start_time + (60 * 60 * 4)],
            'chunk': [60 * 15],
            'topic': ['topic1', 'topic2', 'topic3'],
            'until': [start_time + (60 * 60 * 8)]
        }
        self.compare_to_fixture(self.db_handler.handle_time_series(qsparams), 'fixtures/db_handler_handle_time_series.json')

    @patch('time.time')
    def test_003_check_events_fire(self, mock_time):
        logging.info('test_003_check_events_fire')

        mock_time.return_value = 1620000000

        _trim = self.db_handler.trim_database
        self.db_handler.trim_database = MagicMock()

        _flush = self.db_handler.flush_to_disk
        self.db_handler.flush_to_disk = MagicMock()

        _s3 = self.db_handler.write_to_s3
        self.db_handler.write_to_s3 = MagicMock()

        self.reset_database_contents()

        until = 1620000000 + (2 * max(self.config['S3_INTERVAL'], self.config['TRIM_INTERVAL'], self.config['FLUSH_INTERVAL']))
        db_thread = threading.Thread(target=self.db_handler.loop, kwargs={'until': until}, daemon=True)
        db_thread.start()

        # roll the clock forward until all events should have fired
        while mock_time.return_value < until:
            mock_time.return_value += 60 * 60 * 5 # advance in 5m increments
            logging.info(math.floor(mock_time() / self.config['S3_INTERVAL']))
            time.sleep(0.1)

        time.sleep(0.5) # wait for the db_thread to process the last time

        db_thread.join()

        self.assertTrue(self.db_handler.trim_database.called)
        self.assertTrue(self.db_handler.flush_to_disk.called)
        self.assertTrue(self.db_handler.write_to_s3.called)

        # restore mocks
        self.db_handler.trim_database = _trim
        self.db_handler.flush_to_disk = _flush
        self.db_handler.write_to_s3 = _s3

    @patch('time.time')
    def test_004_write_to_s3(self, mock_time):

        logging.info('test_004_write_to_s3')

        def jsonify_s3_call(s3_call):
            return {
                'Bucket': s3_call[1].get('Bucket'),
                'Key': s3_call[1].get('Key'),
                'Body': gzip.decompress(s3_call[1].get('Body')).decode('utf-8')
            }

        start_time = 1620000000
        duration = 24 * 60 * 60

        mock_time.return_value = start_time
        self.reset_database_contents()

        acc = Accumulator()
        while mock_time() < (start_time + duration):
            self.db_handler.insert('topic1', acc.get())
            self.db_handler.insert('topic2', acc.get())
            self.db_handler.insert('topic3', acc.get())
            mock_time.return_value += 900

        self.db_handler.write_to_s3()

        self.compare_to_fixture([jsonify_s3_call(x) for x in self.mock_s3_client.put_object.call_args_list], 'fixtures/db_handler_write_to_s3.json')


    @patch('time.time')
    def test_005_trim(self, mock_time):

        logging.info('test_005_trim')

        start_time = 1620000000
        duration = 24 * 60 * 60

        mock_time.return_value = start_time
        self.reset_database_contents()
        self.assertEqual(self.count_entries(), 0)

        acc = Accumulator()
        while mock_time() < (start_time + duration):
            self.db_handler.insert('topic1', acc.get())
            self.db_handler.insert('topic2', acc.get())
            self.db_handler.insert('topic3', acc.get())
            mock_time.return_value += 900

        self.assertEqual(self.count_entries(), 288)
        self.db_handler.trim_database(since=start_time + (duration * 0.5))
        self.assertEqual(self.count_entries(), 144)
        self.db_handler.trim_database(since=start_time + duration)
        self.assertEqual(self.count_entries(), 0)

    @patch('time.time')
    def test_006_flush(self, mock_time):

        logging.info('test_006_flush')

        start_time = 1620000000
        duration = 24 * 60 * 60

        mock_time.return_value = start_time
        self.reset_database_contents()

        acc = Accumulator()
        while mock_time() < (start_time + duration):
            self.db_handler.insert('topic1', acc.get())
            self.db_handler.insert('topic2', acc.get())
            self.db_handler.insert('topic3', acc.get())
            mock_time.return_value += 900

        self.db_handler.flush_to_disk()
        self.compare_to_fixture(self.flush_filename, 'fixtures/db_handler_flush.db', obj_is_file_path=True)

if __name__ == '__main__':
    unittest.main()