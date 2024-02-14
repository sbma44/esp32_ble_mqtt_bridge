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
        # self.db_handler._flush_to_disk = self.db_handler.flush_to_disk
        # self.db_handler.flush_to_disk = MagicMock()

    @patch('time.time')
    def test_write_to_s3_interval(self, mock_time):
        start_time = math.floor(1620000000 / self.config['S3_INTERVAL']) * self.config['S3_INTERVAL'] # example timestamp
        mock_time.return_value = start_time

        # Set the last_s3_upload to a time earlier than the current interval
        self.db_handler.last_s3_upload = math.floor(start_time / self.config['S3_INTERVAL'])

        # do a bunch of inserts at various times & topics
        acc = Accumulator()
        while mock_time() < (start_time + self.config['S3_INTERVAL'] + 1):
            self.db_handler.insert('topic1', acc.get())
            self.db_handler.insert('topic2', acc.get())
            self.db_handler.insert('topic3', acc.get())
            mock_time.return_value += 900

        # Check if the method proceeds to upload as the interval is new
        self.mock_s3_client.put_object.assert_called()

        # check contents of calls
        for (call_i, call) in enumerate(self.mock_s3_client.put_object.call_args_list):
            obj = {
                'Bucket': call[1].get('Bucket'),
                'Key': call[1].get('Key'),
                'Body': gzip.decompress(call[1].get('Body')).decode('utf-8')
            }

            dirpath = os.path.dirname(os.path.abspath(__file__))
            p = os.path.join(dirpath, 'fixtures', obj['Key'].split('/')[-1] + '-' + str(call_i) + '.json')
            if (int(os.environ.get('UPDATE', 0)) == 1):
                with open(p, 'w') as f:
                    json.dump(obj, f)
            else:
                with open(p, 'r') as f:
                    self.assertDictEqual(obj, json.load(f))

if __name__ == '__main__':
    unittest.main()