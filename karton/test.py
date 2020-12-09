import os
import unittest
from unittest.mock import patch, MagicMock, call
from karton.core import Config, Task

@patch('configparser.ConfigParser', autospec=True)
class TestConfig(unittest.TestCase):
    MOCK_ENV = {
        "KARTON_MINIO_ACCESS_KEY": "xd",
        "KARTON_MINIO_SECURE": "1",
        "KARTON_REDIS_HOST": "testhost",
        "KARTON_REDIS_PORT": "2137",

        "KARTON_FOO_BAR": "aaa",
        "KARTON_FOO_BAZ": "xxx",
    }

    def test_missing_sections(self, mock_parser):
        """ Missing MinIO and Redis sections is an error """
        parser = mock_parser.return_value

        def no_section(missing_section):
            def mock_has_section(sec):
                return sec != missing_section
            return mock_has_section

        # When no [minio]
        parser.has_section.side_effect = no_section("minio")
        with self.assertRaises(RuntimeError):
            cfg = Config()

        # When no [redis]
        parser.has_section.side_effect = no_section("redis")
        with self.assertRaises(RuntimeError):
            cfg = Config()

        # When no [something] -> no error
        parser.has_section.side_effect = no_section("something")
        cfg = Config()

    @patch.dict(os.environ, MOCK_ENV)
    def test_env_override(self, mock_parser):
        """ Test overriding config with variables loaded from environment """
        parser = mock_parser.return_value

        redis_section = MagicMock()
        minio_section = MagicMock()
        foo_section = MagicMock()
        sections = {
            "redis": redis_section,
            "minio": minio_section,
            "foo": foo_section,
        }

        def get_section(section):
            return sections[section]
        parser.__getitem__.side_effect = get_section

        def has_section(section):
            return section in ["redis", "minio"]
        parser.has_section.side_effect = has_section

        cfg = Config()
        cfg["redis"]["host"]

        redis_section.__setitem__.assert_has_calls([
            call("host", "testhost"),
            call("port", "2137"),
        ])

        minio_section.__setitem__.assert_has_calls([
            call("access_key", "xd"),
            call("secure", "1"),
        ])

        foo_section.__setitem__.assert_has_calls([
            call("bar", "aaa"),
            call("baz", "xxx"),
        ])

    @patch.dict(os.environ, MOCK_ENV)
    def test_compat_loading(self, mock_parser):
        """ Ensure legacy configration access is working """
        parser = mock_parser.return_value
        cfg = Config()

        parser.__getitem__.return_value = dict(test="xd")

        self.assertEqual(cfg.minio_config["test"], "xd")
        self.assertEqual(cfg.redis_config["test"], "xd")

    @patch('os.path.isfile')
    def test_missing_config_file(self, mock_isfile, mock_parser):
        """ Test missing config file """
        mock_isfile.return_value = False
        with self.assertRaises(IOError):
            cfg = Config("this_file_doesnt_exist")

    @patch('os.path.isfile')
    def test_load_from_file(self, mock_isfile, mock_parser):
        """ Test missing config file """
        mock_isfile.return_value = True
        cfg = Config("wew")


class TestTask(unittest.TestCase):
    task = Task(headers={"A": "a", "B": "b"})
    assert task.matches_filters([{"A": "a"}])
    assert task.matches_filters([{"A": "a"}, {"A": "b", "Z": "z"}])
    assert task.matches_filters([{"A": "a", "B": "b"}])
    assert not task.matches_filters([{"Z": "a"}])
    assert not task.matches_filters([{"A": "a", "Z": "a"}])


if __name__ == "__main__":
    unittest.main()
