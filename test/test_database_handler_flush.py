import unittest
from unittest.mock import patch, MagicMock
import time, math, gzip, os, json, tempfile, filecmp
import logging

from sensor_logging import DatabaseHandler
from test import Accumulator

# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

class TestDatabaseHandler(unittest.TestCase):
    @patch('sensor_logging.DatabaseHandler.flush_to_disk')
    def setUp(self, mock_flush_to_disk):
        self.mock_s3_client = MagicMock()

        config = {
            'FLUSH_INTERVAL': 60 * 60, # trim the database ever hour
            'AGGREGATION_INTERVAL': 45 * 60, # store data in 45 minute chunks
            'S3_INTERVAL': 24 * 60 * 60, # upload data 1x/day
            'RETENTION_PERIOD': 7 * 24 * 60 * 60, # only keep data for 7 days
            'S3_BUCKET': 'test-bucket',
            'S3_PATH': 'test-path/'
        }
        self.config = config
        self.db_handler = DatabaseHandler(self.mock_s3_client, config, tempfile.mktemp())

    @patch('time.time')
    def test_flush_to_disk(self, mock_time):
        S3_INTERVAL = 24 * 60 * 60  # 24 hours

        current_time = 1620000000  # example timestamp
        self.db_handler.last_flush = current_time
        tempdir = tempfile.TemporaryDirectory()
        self.db_handler.filename = os.path.join(tempdir.name, 'test.db')

        # do a bunch of inserts at various times & topics
        acc = Accumulator()
        start_time = current_time
        while start_time < (current_time + S3_INTERVAL):
            mock_time.return_value = start_time
            self.db_handler.insert('topic1', acc.get())
            self.db_handler.insert('topic2', acc.get())
            self.db_handler.insert('topic3', acc.get())
            start_time += 90

        mock_time.return_value = current_time + self.db_handler.FLUSH_INTERVAL

        self.assertTrue(os.path.exists(self.db_handler.filename))

        dirpath = os.path.dirname(os.path.abspath(__file__))
        p = os.path.join(dirpath, 'fixtures', 'db_flush_fixture.db')
        with open(self.db_handler.filename, 'rb') as src:
            if (int(os.environ.get('UPDATE', 0)) == 1):
                with open(p, 'wb') as dst:
                    dst.write(src.read())
            else:
                self.assertTrue(filecmp.cmp(p, self.db_handler.filename))

        # clean up temp file
        tempdir.cleanup()

if __name__ == '__main__':
    unittest.main()
