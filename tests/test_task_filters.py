from karton.core import Task
import unittest


class TestTaskFilters(unittest.TestCase):
    def test_basic_filter(self):
        filters = [
            {
                "foo": "bar",
                "bar": "baz"
            },
            {
                "foo": "bar",
                "bar": "bar"
            }
        ]
        task_without_bar = Task(headers={"foo": "bar"})
        self.assertFalse(task_without_bar.matches_filters(filters))

        task_without_foo = Task(headers={"bar": "bar"})
        self.assertFalse(task_without_foo.matches_filters(filters))

        task_bar_bar = Task(headers={"foo": "bar", "bar": "bar"})
        self.assertTrue(task_bar_bar.matches_filters(filters))

        task_bar_baz = Task(headers={"foo": "bar", "bar": "baz"})
        self.assertTrue(task_bar_baz.matches_filters(filters))

        task_bar_qux = Task(headers={"foo": "bar", "bar": "qux"})
        self.assertFalse(task_bar_qux.matches_filters(filters))

    def test_single_letter_patterns(self):
        filters = [
            {
                "foo": "ba?",
                "bar": "ba[rz]",
            }
        ]
        task_bar_bar = Task(headers={"foo": "bar", "bar": "bar"})
        self.assertTrue(task_bar_bar.matches_filters(filters))

        task_barz_bar = Task(headers={"foo": "barz", "bar": "bar"})
        self.assertFalse(task_barz_bar.matches_filters(filters))

        task_bar_barz = Task(headers={"foo": "bar", "bar": "barz"})
        self.assertFalse(task_bar_barz.matches_filters(filters))

        task_baz_baz = Task(headers={"foo": "baz", "bar": "baz"})
        self.assertTrue(task_baz_baz.matches_filters(filters))

    def test_catch_all_filter(self):
        filters = [{}]

        task_with_headers = Task(headers={"foo": "bar", "bar": "baz"})
        self.assertTrue(task_with_headers.matches_filters(filters))

        task_without_headers = Task(headers={})
        self.assertTrue(task_without_headers.matches_filters(filters))

    def test_negated_filter(self):
        filters = [
            {
                "type": "sample",
                "platform": "!macos"
            },
            {
                "type": "sample",
                "platform": "!win*"
            }
        ]
        task_win32 = Task(headers={
            "type": "sample",
            "kind": "runnable",
            "platform": "win32"
        })
        self.assertFalse(task_win32.matches_filters(filters))

        task_win64 = Task(headers={
            "type": "sample",
            "kind": "runnable",
            "platform": "win64"
        })
        self.assertFalse(task_win64.matches_filters(filters))

        task_win = Task(headers={
            "type": "sample",
            "kind": "runnable",
            "platform": "win"
        })
        self.assertFalse(task_win.matches_filters(filters))

        task_linux = Task(headers={
            "type": "sample",
            "kind": "runnable",
            "platform": "linux"
        })
        self.assertTrue(task_linux.matches_filters(filters))

        task_noplatform = Task(headers={
            "type": "sample",
            "kind": "runnable",
        })
        self.assertTrue(task_noplatform.matches_filters(filters))

    def test_negate_header_existence(self):
        filters = [
            {
                "type": "sample",
                "platform": "!*"
            }
        ]
        task_linux = Task(headers={
            "type": "sample",
            "kind": "runnable",
            "platform": "linux"
        })
        self.assertFalse(task_linux.matches_filters(filters))

        task_empty_string = Task(headers={
            "type": "sample",
            "kind": "runnable",
            "platform": ""
        })
        self.assertFalse(task_empty_string.matches_filters(filters))

        task_noplatform = Task(headers={
            "type": "sample",
            "kind": "runnable",
        })
        self.assertTrue(task_noplatform.matches_filters(filters))

    def test_non_string_headers(self):
        # It's not recommended but was allowed by earlier versions
        filters = [
            {
                "block": True,
                "value": 20
            }
        ]

        task_block = Task(headers={
            "block": True,
            "value": 20
        })
        self.assertTrue(task_block.matches_filters(filters))

        task_non_block = Task(headers={
            "block": False,
            "value": 20
        })
        self.assertFalse(task_non_block.matches_filters(filters))

        task_different_value = Task(headers={
            "block": True,
            "value": 10
        })
        self.assertFalse(task_different_value.matches_filters(filters))

