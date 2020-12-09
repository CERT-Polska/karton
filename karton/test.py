import os
import unittest
from unittest.mock import patch, mock_open
from karton.core import Config, Task

MOCK_CONFIG = """
[minio]
access_key = 123
secret_key = aabbcc
[redis]
host = localhost
port = 1337

[other]
hello = there
"""

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
    @patch('builtins.open', mock_open(read_data=MOCK_CONFIG))
    def test_env_override(self):
        cfg = Config()

        # Sanity check env values
        self.assertEqual(cfg["minio"]["access_key"], "xd")
        self.assertEqual(cfg["minio"]["secure"], "1")

        self.assertEqual(cfg["redis"]["host"], "testhost")
        self.assertEqual(cfg["redis"]["port"], "2137")

        # Ensure other section not overwritten
        self.assertEqual(cfg["other"]["hello"], "there")

    @patch.dict(os.environ, MOCK_ENV)
    def test_only_env_loading(self):
        cfg = Config()
        self.assertEqual(cfg["minio"]["access_key"], "xd")
        self.assertEqual(cfg["minio"]["secure"], "1")

        self.assertEqual(cfg["redis"]["host"], "testhost")
        self.assertEqual(cfg["redis"]["port"], "2137")

    def test_missing_path(self):
        with self.assertRaises(IOError):
            cfg = Config("this_file_doesnt_exist")

    @patch('builtins.open', mock_open(read_data=''))
    def test_missing_config_section(self):
        with self.assertRaises(RuntimeError):
            cfg = Config()

    @patch('builtins.open', mock_open(read_data=MOCK_CONFIG))
    def test_reading_path_file(self):
        with patch('os.path.isfile') as mock_isfile:
            mock_isfile.return_value = True
            cfg = Config('myconfig')

    @patch.dict(os.environ, MOCK_ENV)
    def test_compat_loading(self):
        cfg = Config()
        self.assertEqual(cfg["minio"], cfg.minio_config)
        self.assertEqual(cfg["redis"], cfg.redis_config)


class TestTask(unittest.TestCase):
    def test_matching_filters(self):
        task = Task(headers={"A": "a", "B": "b"})
        self.assertTrue(task.matches_filters([{"A": "a"}]))
        self.assertTrue(task.matches_filters([{"A": "a"}, {"A": "b", "Z": "z"}]))
        self.assertTrue(task.matches_filters([{"A": "a", "B": "b"}]))
        self.assertFalse(task.matches_filters([{"Z": "a"}]))
        self.assertFalse(task.matches_filters([{"A": "a", "Z": "a"}]))


if __name__ == "__main__":
    unittest.main()
