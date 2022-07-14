import os
import unittest
from unittest.mock import patch, mock_open
from karton.core import Config, Task

MOCK_CONFIG = """
[minio]
access_key=xd

[redis]
host=localhost
"""


@patch('karton.core.Config', autospec=True)
class TestConfig(unittest.TestCase):
    MOCK_ENV = {
        "KARTON_MINIO_ACCESS_KEY": "xd",
        "KARTON_MINIO_SECURE": "1",
        "KARTON_REDIS_HOST": "testhost",
        "KARTON_REDIS_PORT": "2137",

        "KARTON_FOO_BAR": "aaa",
        "KARTON_FOO_BAZ": "xxx",
    }

    @patch.dict(os.environ, MOCK_ENV)
    def test_env_override(self, mock_parser):
        """ Test overriding config with variables loaded from environment """
        cfg = Config()
        assert cfg["redis"]["host"] == self.MOCK_ENV["KARTON_REDIS_HOST"]
        assert cfg["redis"]["port"] == self.MOCK_ENV["KARTON_REDIS_PORT"]
        assert cfg["minio"]["access_key"] == self.MOCK_ENV["KARTON_MINIO_ACCESS_KEY"]
        assert cfg["foo"]["bar"] == self.MOCK_ENV["KARTON_FOO_BAR"]
        assert cfg["foo"]["baz"] == self.MOCK_ENV["KARTON_FOO_BAZ"]

    @patch('os.path.isfile')
    def test_missing_config_file(self, mock_isfile, mock_parser):
        """ Test missing config file """
        mock_isfile.return_value = False
        with self.assertRaises(IOError):
            cfg = Config("this_file_doesnt_exist")

    @patch('os.path.isfile', lambda path: True)
    @patch('builtins.open', mock_open(read_data=MOCK_CONFIG))
    def test_load_from_file(self, mock_parser):
        """ Test missing config file """
        Config("wew")


class TestTask(unittest.TestCase):
    def test_matching_filters(self):
        task = Task(headers={"A": "a", "B": "b"})
        self.assertTrue(task.matches_filters([{"A": "a"}]))
        self.assertTrue(task.matches_filters([{"A": "a"}, {"A": "b", "Z": "z"}]))
        self.assertTrue(task.matches_filters([{"A": "a", "B": "b"}]))
        self.assertFalse(task.matches_filters([{"Z": "a"}]))
        self.assertFalse(task.matches_filters([{"A": "a", "Z": "a"}]))
