import unittest
from unittest.mock import patch, MagicMock
import time, math, gzip, os, json

from sensor_logging import DatabaseHandler

class Accumulator(object):
    def __init__(self):
        self.value = 0

    def get(self):
        self.value = (self.value + 1) % 100
        return self.value

class TestDatabaseHandler(unittest.TestCase):
    @patch('sensor_logging.DatabaseHandler.flush_to_disk')
    def setUp(self, mock_flush_to_disk):
        self.mock_s3_client = MagicMock()
        self.db_handler = DatabaseHandler('test.db', self.mock_s3_client)

    @patch('time.time')
    def test_write_to_s3_interval(self, mock_time):
        S3_INTERVAL = 24 * 60 * 60  # 24 hours

        acc = Accumulator()

        # Set up the time to a specific value
        current_time = 1620000000  # example timestamp
        mock_time.return_value = current_time

        # Set the last_s3_upload to a time earlier than the current interval
        self.db_handler.last_s3_upload = math.floor((current_time - (24.1 * 60 * 60)) / S3_INTERVAL) # 24h earlier

        # do a bunch of inserts at various times & topics
        start_time = current_time
        accumulator = 0
        while start_time < (current_time + S3_INTERVAL):
            mock_time.return_value = start_time
            self.db_handler.insert('topic1', acc.get())
            self.db_handler.insert('topic2', acc.get())
            self.db_handler.insert('topic3', acc.get())
            start_time += 90

        # Run the method
        self.db_handler.write_to_s3()

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
            p = os.path.join(dirpath, 'fixtures', obj['Key'].split('/')[-1] + '.json')
            if (int(os.environ.get('UPDATE', 0)) == 1):
                with open(p, 'w') as f:
                    json.dump(obj, f)
            else:
                with open(p, 'r') as f:
                    self.assertDictEqual(obj, json.load(f))

    @patch('time.time')
    def test_write_to_s3_no_upload_due_to_interval(self, mock_time):
        S3_INTERVAL = 24 * 60 * 60  # 24 hours

        # Simulate the scenario where the current interval is not yet due for an upload
        current_time = 1620000000
        mock_time.return_value = current_time

        # Set the last_s3_upload to the same interval
        self.db_handler.last_s3_upload = math.floor(current_time / S3_INTERVAL)

        # Run the method
        self.db_handler.write_to_s3()

        # Check if the method skips upload due to the same interval
        self.mock_s3_client.put_object.assert_not_called()

if __name__ == '__main__':
    unittest.main()
