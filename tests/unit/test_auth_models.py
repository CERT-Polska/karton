import unittest

from karton.core.auth.models import (
    ConsumeLogOperation,
    ConsumeTaskOperation,
    EnableDebugOperation,
    ProduceTaskOperation,
    RegisterBindOperation,
)


class TestOperationCovers(unittest.TestCase):
    def test_simple_operations_cover_same_type(self):
        for operation in (
            ConsumeTaskOperation(),
            ConsumeLogOperation(),
            EnableDebugOperation(),
        ):
            self.assertTrue(operation.covers(operation.__class__()))

    def test_simple_operations_dont_cover_other_types(self):
        simple_operations = [
            ConsumeTaskOperation(),
            ConsumeLogOperation(),
            EnableDebugOperation(),
        ]
        other_operations = [
            ConsumeTaskOperation(),
            ConsumeLogOperation(),
            EnableDebugOperation(),
            ProduceTaskOperation(foreign_buckets=[], headers=[{}]),
            RegisterBindOperation(filters=[], persistent=False),
        ]
        for entitlement in simple_operations:
            for requested in other_operations:
                expected = type(requested) is type(entitlement)
                self.assertEqual(
                    entitlement.covers(requested),
                    expected,
                    f"{entitlement!r}.covers({requested!r})",
                )

    def test_produce_task_foreign_buckets_subset(self):
        entitlement = ProduceTaskOperation(
            foreign_buckets=["bucket1", "bucket2"], headers=[{}]
        )
        self.assertTrue(
            entitlement.covers(ProduceTaskOperation(foreign_buckets=[], headers=[{}]))
        )
        self.assertTrue(
            entitlement.covers(
                ProduceTaskOperation(foreign_buckets=["bucket1"], headers=[{}])
            )
        )
        self.assertFalse(
            entitlement.covers(
                ProduceTaskOperation(foreign_buckets=["bucket3"], headers=[{}])
            )
        )
        self.assertFalse(
            entitlement.covers(
                ProduceTaskOperation(
                    foreign_buckets=["bucket1", "bucket3"], headers=[{}]
                )
            )
        )

    def test_produce_task_doesnt_cover_other_types(self):
        entitlement = ProduceTaskOperation(headers=[{}])
        self.assertFalse(entitlement.covers(ConsumeTaskOperation()))

    def test_produce_task_egress_filters_partial_match(self):
        entitlement = ProduceTaskOperation(
            headers=[{"kind": "raw"}, {"kind": "file", "extension": "txt"}]
        )

        def requested(headers):
            return ProduceTaskOperation(headers=[headers])

        for headers in (
            {"kind": "raw"},
            {"kind": "raw", "foo": "bar"},
            {"kind": "file", "extension": "txt"},
            {"kind": "file", "extension": "txt", "xyz": "qwer"},
        ):
            self.assertTrue(entitlement.covers(requested(headers)), headers)

        for headers in (
            {},
            {"kind": "file"},
            {"kind": "file", "extension": "exe"},
            {"extension": "txt"},
            {"type": "sample"},
        ):
            self.assertFalse(entitlement.covers(requested(headers)), headers)

    def test_produce_task_egress_filters_catch_all(self):
        # [{}] is a catch-all that covers every requested headers
        entitlement = ProduceTaskOperation(headers=[{}])
        self.assertTrue(
            entitlement.covers(ProduceTaskOperation(headers=[{"kind": "raw"}]))
        )
        self.assertTrue(entitlement.covers(ProduceTaskOperation(headers=[{}])))
        self.assertTrue(entitlement.covers(ProduceTaskOperation(headers=[{"a": 1}])))

    def test_produce_task_egress_filters_empty(self):
        entitlement = ProduceTaskOperation(headers=[])
        self.assertFalse(
            entitlement.covers(ProduceTaskOperation(headers=[{"kind": "raw"}]))
        )
        self.assertFalse(entitlement.covers(ProduceTaskOperation(headers=[{}])))

    def test_register_bind_requires_same_persistence(self):
        bind = RegisterBindOperation(filters=[{"type": "a"}], persistent=True)
        self.assertTrue(
            bind.covers(RegisterBindOperation(filters=[{"type": "a"}], persistent=True))
        )
        self.assertFalse(
            bind.covers(
                RegisterBindOperation(filters=[{"type": "a"}], persistent=False)
            )
        )

    def test_register_bind_requires_equal_filters(self):
        bind = RegisterBindOperation(filters=[{"type": "a"}], persistent=True)
        self.assertTrue(
            bind.covers(RegisterBindOperation(filters=[{"type": "a"}], persistent=True))
        )
        # Not a superset relationship: filters must be equal
        self.assertFalse(
            bind.covers(
                RegisterBindOperation(
                    filters=[{"type": "a"}, {"type": "b"}], persistent=True
                )
            )
        )
        self.assertFalse(
            bind.covers(RegisterBindOperation(filters=[{"type": "b"}], persistent=True))
        )

    def test_register_bind_filter_order_insensitive(self):
        bind = RegisterBindOperation(
            filters=[{"type": "a"}, {"type": "b"}], persistent=True
        )
        self.assertTrue(
            bind.covers(
                RegisterBindOperation(
                    filters=[{"type": "b"}, {"type": "a"}], persistent=True
                )
            )
        )

    def test_register_bind_doesnt_cover_other_types(self):
        bind = RegisterBindOperation(filters=[], persistent=False)
        self.assertFalse(bind.covers(ConsumeLogOperation()))


if __name__ == "__main__":
    unittest.main()
