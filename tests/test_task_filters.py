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

    def test_catch_none_filter(self):
        filters = []
        task_with_headers = Task(headers={"foo": "bar", "bar": "baz"})
        self.assertFalse(task_with_headers.matches_filters(filters))

        task_without_headers = Task(headers={})
        self.assertFalse(task_without_headers.matches_filters(filters))

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

    def test_negate_header_existence_but_catch_all(self):
        filters = [
            {
                "type": "sample",
                "platform": "!*"
            },
            {}
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

        # Platform requirement is defined for type=sample
        task_different_type = Task(headers={
            "type": "different",
            "platform": "hehe",
        })
        self.assertTrue(task_different_type.matches_filters(filters))

    def test_require_header_existence(self):
        filters = [
            {
                "type": "sample",
                "platform": "*"
            }
        ]
        task_linux = Task(headers={
            "type": "sample",
            "kind": "runnable",
            "platform": "linux"
        })
        self.assertTrue(task_linux.matches_filters(filters))

        task_empty_string = Task(headers={
            "type": "sample",
            "kind": "runnable",
            "platform": ""
        })
        self.assertTrue(task_empty_string.matches_filters(filters))

        task_noplatform = Task(headers={
            "type": "sample",
            "kind": "runnable",
        })
        self.assertFalse(task_noplatform.matches_filters(filters))

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

    def test_negated_filter_for_different_type(self):
        filters = [
            {
                "type": "sample",
                "platform": "win32"
            },
            {
                "type": "different",
                "platform": "!win32"
            }
        ]

        task_sample = Task(headers={
            "type": "sample",
            "platform": "win32"
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_different_win32 = Task(headers={
            "type": "different",
            "platform": "win32"
        })
        self.assertFalse(task_different_win32.matches_filters(filters))

        task_different_win64 = Task(headers={
            "type": "different",
            "platform": "win64"
        })
        self.assertTrue(task_different_win64.matches_filters(filters))

        task_sample_win64 = Task(headers={
            "type": "sample",
            "platform": "win64"
        })
        self.assertFalse(task_sample_win64.matches_filters(filters))

    def test_list_contains(self):
        filters = [
            {
                "type": "sample",
                "platform": {"$in": ["win32", "linux"]},
            },
        ]

        task_sample = Task(headers={
            "type": "sample",
            "platform": "win32"
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_different_win32 = Task(headers={
            "type": "sample",
            "platform": "linux"
        })
        self.assertTrue(task_different_win32.matches_filters(filters))

        task_different_win64 = Task(headers={
            "type": "different",
            "platform": "win32"
        })
        self.assertFalse(task_different_win64.matches_filters(filters))

    def test_element_is_contained(self):
        filters = [
            {
                "type": "sample",
                "tags": "emotet",
            },
        ]

        task_sample = Task(headers={
            "type": "sample",
            "tags": ["emotet"],
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "tags": ["emotet", "dump"],
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "tags": ["nymaim", "dump"],
        })
        self.assertFalse(task_sample.matches_filters(filters))

    def test_multiple_elements_are_contained(self):
        filters = [
            {
                "type": "sample",
                "tags": {"$all": ["emotet", "dump"]},
            },
        ]

        task_sample = Task(headers={
            "type": "sample",
            "tags": ["emotet"],
        })
        self.assertFalse(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "tags": ["emotet", "dump"],
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "tags": ["emotet", "dump", "needs-inspection"],
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "tags": ["nymaim", "dump"],
        })
        self.assertFalse(task_sample.matches_filters(filters))

    def test_comparison(self):
        filters = [
            {
                "type": "sample",
                "version": {"$gt": 3},
            },
        ]

        task_sample = Task(headers={
            "type": "sample",
            "version": 2,
        })
        self.assertFalse(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "version": 4,
        })
        self.assertTrue(task_sample.matches_filters(filters))

    def test_basic_wildcard(self):
        filters = [
            {
                "type": "sample",
                "platform": "win*",
            },
        ]

        task_sample = Task(headers={
            "type": "sample",
            "platform": "linux",
        })
        self.assertFalse(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "platform": "win32",
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "platform": "win",
        })
        self.assertTrue(task_sample.matches_filters(filters))

    def test_regex_match(self):
        filters = [
            {
                "type": "sample",
                "platform": {"$regex": "win.*"}
            },
        ]

        task_sample = Task(headers={
            "type": "sample",
            "platform": "linux",
        })
        self.assertFalse(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "platform": "win32",
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "platform": "win",
        })
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "type": "sample",
            "platform": "karton keeps on winning",
        })
        # no anchors in the regex, so this should actually match
        self.assertTrue(task_sample.matches_filters(filters))

    def test_example_from_convert(self):
        # Test for a literal example used in the convert method documentation
        oldstyle = [{"platform": "!win32"}, {"platform": "!linux"}]
        wrong = [{"platform": {"$not": "win32"}}, {"platform": {"$not": "linux"}}]
        good = [{"platform": {"$not": {"$or": ["win32", "linux"]}}}]

        task_linux = Task(headers={
            "type": "sample",
            "platform": "linux",
        })
        task_win32 = Task(headers={
            "type": "sample",
            "platform": "win32",
        })
        task_macos = Task(headers={
            "type": "sample",
            "platform": "macos",
        })
        tasks = [task_linux, task_win32, task_macos]

        def assertExpect(tasks, filters, results):
            for task, result in zip(tasks, results):
                self.assertEqual(task.matches_filters(filters), result)

        assertExpect(tasks, oldstyle, [False, False, True])
        assertExpect(tasks, wrong, [True, True, True])
        assertExpect(tasks, good, [False, False, True])

    def test_nested_oldstyle(self):
        # Old-style wildcards, except negative filters, don't mix
        filters = [
            {
                "platform": {"$or": ["win*", "linux*"]}
            },
        ]

        task_sample = Task(headers={
            "platform": "linux",
        })
        self.assertFalse(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "platform": "linux*",
        })
        self.assertTrue(task_sample.matches_filters(filters))

    def test_newstyle_flip(self):
        # It's not recommended, but mongo syntax is allowed at the top level too
        # Example: match type:sample, when either platform:win32 or kind:runnable
        filters = [
            {
                "$and": [
                    {"type": "sample"},
                    {
                        "$or": [
                            {"platform": "win32"},
                            {"kind": "runnable"},
                        ]
                    },
                ]
            },
        ]

        task_sample = Task(
            headers={"type": "sample", "platform": "linux", "kind": "runnable"}
        )
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(
            headers={
                "type": "sample",
                "platform": "win32",
            }
        )
        self.assertTrue(task_sample.matches_filters(filters))

        task_sample = Task(
            headers={
                "type": "sample",
                "platform": "linux",
            }
        )
        self.assertFalse(task_sample.matches_filters(filters))

    def test_oldstyle_wildcards(self):
        # Old-style wildcards, except negative filters, don't mix
        filters = [{"foo": "ba[!rz]"}]

        task_sample = Task(headers={
            "foo": "bar",
        })
        self.assertFalse(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "foo": "bat",
        })
        self.assertTrue(task_sample.matches_filters(filters))

    def test_wildcards_anchored(self):
        # Old-style wildcards, except negative filters, don't mix
        filters = [{"foo": "bar"}]

        task_sample = Task(headers={
            "foo": "rabarbar",
        })
        self.assertFalse(task_sample.matches_filters(filters))

        task_sample = Task(headers={
            "foo": "bar",
        })
        self.assertTrue(task_sample.matches_filters(filters))
