import os
import unittest
from unittest.mock import patch, mock_open
from karton.core import Config, Task

MOCK_CONFIG = """
[s3]
access_key=xd

[redis]
host=localhost
"""


@patch('karton.core.Config', autospec=True)
class TestConfig(unittest.TestCase):
    MOCK_ENV = {
        "KARTON_S3_ACCESS_KEY": "xd",
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
        assert cfg["s3"]["access_key"] == self.MOCK_ENV["KARTON_S3_ACCESS_KEY"]
        assert cfg["foo"]["bar"] == self.MOCK_ENV["KARTON_FOO_BAR"]
        assert cfg["foo"]["baz"] == self.MOCK_ENV["KARTON_FOO_BAZ"]

    @patch('os.path.isfile')
    def test_missing_config_file(self, mock_isfile, mock_parser):
        """ Test missing config file """
        mock_isfile.return_value = False
        with self.assertRaises(IOError):
            Config("this_file_doesnt_exist")

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


class TestTaskLargeNumber(unittest.TestCase):
    def test_large_number(self):
        # Case from https://github.com/CERT-Polska/karton/issues/224
        huge_n = 16500472521988697010663112438705807640072764589479002554256260695717317064262484691290598356970646828426660349407212809681822203919126081272297252018228030950728477136113932493730960235450313211028445802933417736042246471737169968152424614291341808025585752848637795661794780420312612645575283548111399362423653839834760368929525863676430601132093478720051122016787902563729760403548823660511230280753799912283221065068155277355752466002679387284450151073598015621110653783630539129630982849849675891985711599794713831157549822527748863844615219682824485519877354977586980738215172053213147055330238573803265248619061
        task = Task(
            headers={},
            payload={
                "e": 65537,
                "n": huge_n,
            }
        )
        task = Task.unserialize(task.serialize(), parse_resources=False)
        self.assertTrue(task.payload["n"] == huge_n)
