import unittest
from unittest.mock import patch, MagicMock
import time, math, gzip, os, json

from sensor_logging import DatabaseHandler, HttpServer
from test import Accumulator

class TestHttpServer(unittest.TestCase):
    @patch('sensor_logging.DatabaseHandler.flush_to_disk')
    def setUp(self, mock_flush_to_disk):
        # Setup your HttpServer instance here
        # Mock the database connection and cursor
        # self.db_connection_mock = MagicMock()
        # self.cursor_mock = self.db_connection_mock.cursor.return_value
        # self.server = HttpServer(9991)
        self.mock_s3_client = MagicMock()
        self.db_handler = DatabaseHandler(self.mock_s3_client)

        # Patch the DatabaseHandler.get_sqlite_connection to return the mock
        # patcher = patch('sensor_logging.DatabaseHandler.get_sqlite_connection', return_value=self.db_connection_mock)
        # self.addCleanup(patcher.stop)  # Ensure that the patcher is stopped after tests
        # self.mock_db_conn = patcher.start()

    @patch('time.time')
    def test_handle_time_series(self, mock_time):
        S3_INTERVAL = 24 * 60 * 60  # 24 hours
        acc = Accumulator()
        current_time = 1620000000  # example timestamp
        start_time = current_time
        while start_time < (current_time + S3_INTERVAL):
            mock_time.return_value = start_time
            self.db_handler.insert('topic1', acc.get())
            self.db_handler.insert('topic2', acc.get())
            self.db_handler.insert('topic3', acc.get())
            start_time += 90

        db = DatabaseHandler.get_sqlite_connection()
        cursor = db.cursor()

        # Simulate query parameters
        dirpath = os.path.dirname(os.path.abspath(__file__))
        for since_value in (1620000000, 1620000000 + 2000, False):
            for chunk_value in (60 * 60, 120 * 60, 240 * 60, False):
                for until_value in (1620000000 + (12 * 60 * 60), False):
                    qsparams = {}
                    qsparams['topic'] = ('topic1', 'topic2', 'topic3')
                    if chunk_value:
                        qsparams['chunk'] = (chunk_value,)
                    if since_value:
                        qsparams['since'] = (since_value,)
                    if until_value:
                        qsparams['until'] = (until_value,)

                    result = HttpServer.MyHttpRequestHandler.handle_time_series(self, cursor, qsparams)

                    p = os.path.join(dirpath, 'fixtures', 'test_http_server_{}_{}_{}.json'.format(since_value or 'nosince', until_value or 'nountil', chunk_value or 'nochunk'))

                    if (int(os.environ.get('UPDATE', 0)) == 1):
                        with open(p, 'w') as f:
                            json.dump(result, f)
                    else:
                        with open(p, 'r') as f:
                            j = json.load(f)

                        # detupleify
                        result = json.loads(json.dumps(result))

                        self.assertDictEqual(result, j)


if __name__ == '__main__':
    unittest.main()