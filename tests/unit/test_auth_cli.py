import io
import json
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path

from cryptography.hazmat.primitives import serialization

from karton.core.auth.cli import (
    KARTON_TYPE_CHOICES,
    claims_from_args,
    cmd_validate,
    dedup_and_sort_operations,
    parse_filters_args,
    resolve_bind_persistence,
)
from karton.core.auth.models import (
    AuthClaims,
    ConsumeLogOperation,
    ConsumeTaskOperation,
    EnableDebugOperation,
    ProduceTaskOperation,
    RegisterBindOperation,
)
from karton.core.auth.signer import generate_new_keypair, issue_auth_token


def make_args(**kwargs):
    """Build an argparse.Namespace resembling parsed 'claims' arguments."""
    defaults = {
        "karton": None,
        "identity": None,
        "karton_type": None,
        "filters": None,
        "persistent": False,
        "non_persistent": False,
        "debug": False,
        "produce_task": False,
        "consume_task": False,
        "consume_log": False,
        "register_bind": None,
        "foreign_buckets": None,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


class TestParseFiltersArgs(unittest.TestCase):
    def test_none(self):
        self.assertEqual(parse_filters_args(None), [])

    def test_single_list(self):
        filters = parse_filters_args(['[{"type": "example.RawFeed"}]'])
        self.assertEqual(filters, [{"type": "example.RawFeed"}])

    def test_concatenation(self):
        filters = parse_filters_args(
            ['[{"type": "a"}]', '[{"type": "b"}, {"type": "c"}]']
        )
        self.assertEqual(filters, [{"type": "a"}, {"type": "b"}, {"type": "c"}])

    def test_multivalue_filter(self):
        filters = parse_filters_args(
            ['[{"type": "example.RawFeed", "platform": ["linux", "windows"]}]']
        )
        self.assertEqual(
            filters,
            [{"type": "example.RawFeed", "platform": ["linux", "windows"]}],
        )

    def test_invalid_json(self):
        with self.assertRaises(ValueError):
            parse_filters_args(["not json"])

    def test_not_a_list(self):
        with self.assertRaises(ValueError):
            parse_filters_args(['{"type": "a"}'])

    def test_list_of_non_objects(self):
        with self.assertRaises(ValueError):
            parse_filters_args(['["a", "b"]'])

    def test_non_json_value(self):
        with self.assertRaises(ValueError):
            parse_filters_args(['[{"type": 5j}]'])

    def test_accepts_non_string_values(self):
        # Non-string JSON values (numbers, nested lists) are valid
        # filter values as long as variants are objects
        filters = parse_filters_args(['[{"type": "t", "amount": 5}, {"x": [1, 2]}]'])
        self.assertEqual(filters, [{"type": "t", "amount": 5}, {"x": [1, 2]}])


class TestResolveBindPersistence(unittest.TestCase):
    def test_default(self):
        persistent, non_persistent = resolve_bind_persistence(make_args())
        self.assertTrue(persistent)
        self.assertFalse(non_persistent)

    def test_non_persistent(self):
        persistent, non_persistent = resolve_bind_persistence(
            make_args(non_persistent=True)
        )
        self.assertFalse(persistent)
        self.assertTrue(non_persistent)

    def test_debug_implies_non_persistent(self):
        persistent, non_persistent = resolve_bind_persistence(make_args(debug=True))
        self.assertFalse(persistent)
        self.assertTrue(non_persistent)

    def test_debug_with_persistent(self):
        persistent, non_persistent = resolve_bind_persistence(
            make_args(debug=True, persistent=True)
        )
        self.assertTrue(persistent)
        self.assertTrue(non_persistent)


class TestClaimsWithKartonType(unittest.TestCase):
    FILTERS = ['[{"type": "example.RawFeed"}]']

    def operations_of(self, **kwargs):
        claims = claims_from_args(make_args(**kwargs))
        return claims.allowed_operations

    def test_producer(self):
        operations = self.operations_of(identity="p", karton_type="producer")
        self.assertEqual(operations, [ProduceTaskOperation(foreign_buckets=[])])

    def test_consumer(self):
        operations = self.operations_of(
            identity="c", karton_type="consumer", filters=self.FILTERS
        )
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                RegisterBindOperation(
                    filters=[{"type": "example.RawFeed"}], persistent=True
                ),
            ],
        )

    def test_karton(self):
        operations = self.operations_of(
            identity="k", karton_type="karton", filters=self.FILTERS
        )
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                ProduceTaskOperation(foreign_buckets=[]),
                RegisterBindOperation(
                    filters=[{"type": "example.RawFeed"}], persistent=True
                ),
            ],
        )

    def test_karton_debug(self):
        operations = self.operations_of(
            identity="k",
            karton_type="karton",
            filters=self.FILTERS,
            debug=True,
        )
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                EnableDebugOperation(),
                ProduceTaskOperation(foreign_buckets=[]),
                RegisterBindOperation(
                    filters=[{"type": "example.RawFeed"}], persistent=False
                ),
            ],
        )

    def test_karton_debug_with_persistent(self):
        operations = self.operations_of(
            identity="k",
            karton_type="karton",
            filters=self.FILTERS,
            debug=True,
            persistent=True,
        )
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                EnableDebugOperation(),
                ProduceTaskOperation(foreign_buckets=[]),
                RegisterBindOperation(
                    filters=[{"type": "example.RawFeed"}], persistent=True
                ),
                RegisterBindOperation(
                    filters=[{"type": "example.RawFeed"}], persistent=False
                ),
            ],
        )

    def test_logconsumer(self):
        operations = self.operations_of(identity="l", karton_type="logconsumer")
        self.assertEqual(operations, [ConsumeLogOperation()])

    def test_consumer_requires_filters(self):
        with self.assertRaises(ValueError):
            claims_from_args(make_args(identity="c", karton_type="consumer"))

    def test_consumer_accepts_empty_filter_list(self):
        # Explicit empty filter list is a legitimate bind configuration
        operations = self.operations_of(
            identity="c", karton_type="consumer", filters=["[]"]
        )
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                RegisterBindOperation(filters=[], persistent=True),
            ],
        )

    def test_consumer_accepts_empty_variant(self):
        # [{}] is a match-everything bind
        operations = self.operations_of(
            identity="c", karton_type="consumer", filters=["[{}]"]
        )
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                RegisterBindOperation(filters=[{}], persistent=True),
            ],
        )

    def test_karton_requires_filters(self):
        with self.assertRaises(ValueError):
            claims_from_args(make_args(identity="k", karton_type="karton"))

    def test_producer_rejects_filters(self):
        with self.assertRaises(ValueError):
            claims_from_args(
                make_args(identity="p", karton_type="producer", filters=self.FILTERS)
            )

    def test_logconsumer_rejects_filters(self):
        with self.assertRaises(ValueError):
            claims_from_args(
                make_args(identity="l", karton_type="logconsumer", filters=self.FILTERS)
            )

    def test_identity_required(self):
        with self.assertRaises(ValueError):
            claims_from_args(make_args(karton_type="producer"))

    def test_karton_type_choices_content(self):
        self.assertEqual(
            set(KARTON_TYPE_CHOICES),
            {"producer", "consumer", "karton", "logconsumer"},
        )


class TestClaimsExplicitFlags(unittest.TestCase):
    def test_legacies_flags_only(self):
        operations = claims_from_args(
            make_args(identity="x", produce_task=True, consume_log=True)
        ).allowed_operations
        self.assertEqual(
            operations,
            [ConsumeLogOperation(), ProduceTaskOperation(foreign_buckets=[])],
        )

    def test_filters_with_explicit_flags(self):
        operations = claims_from_args(
            make_args(
                identity="x",
                consume_task=True,
                filters=['[{"type": "t"}]'],
            )
        ).allowed_operations
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                RegisterBindOperation(filters=[{"type": "t"}], persistent=True),
            ],
        )

    def test_non_persistent_replaces_persistent(self):
        operations = claims_from_args(
            make_args(
                identity="x",
                consume_task=True,
                filters=['[{"type": "t"}]'],
                non_persistent=True,
            )
        ).allowed_operations
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                RegisterBindOperation(filters=[{"type": "t"}], persistent=False),
            ],
        )

    def test_register_bind_json_coexists(self):
        operations = claims_from_args(
            make_args(
                identity="x",
                consume_task=True,
                filters=['[{"type": "t"}]'],
                register_bind=['{"filters": [{"type": "y"}], "persistent": false}'],
            )
        ).allowed_operations
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                RegisterBindOperation(filters=[{"type": "t"}], persistent=True),
                RegisterBindOperation(filters=[{"type": "y"}], persistent=False),
            ],
        )

    def test_foreign_buckets_deduplicated(self):
        operations = claims_from_args(
            make_args(
                identity="x",
                produce_task=True,
                foreign_buckets=["b2", "b1", "b2"],
            )
        ).allowed_operations
        self.assertEqual(
            operations, [ProduceTaskOperation(foreign_buckets=["b1", "b2"])]
        )

    def test_legacy_flags_merge_with_karton_type(self):
        operations = claims_from_args(
            make_args(
                identity="x",
                karton_type="producer",
                consume_log=True,
            )
        ).allowed_operations
        self.assertEqual(
            operations,
            [ConsumeLogOperation(), ProduceTaskOperation(foreign_buckets=[])],
        )

    def test_empty_filters_valid(self):
        # Explicit empty filter list is a legitimate bind configuration
        operations = claims_from_args(
            make_args(identity="x", consume_task=True, filters=["[]"])
        ).allowed_operations
        self.assertEqual(
            operations,
            [
                ConsumeTaskOperation(),
                RegisterBindOperation(filters=[], persistent=True),
            ],
        )

    def test_filters_without_identity_error(self):
        with self.assertRaises(ValueError):
            claims_from_args(make_args(filters=['[{"type": "t"}]'], consume_task=True))


class TestValidateOutput(unittest.TestCase):
    """cmd_validate must emit a single machine-readable JSON document."""

    def _validated_report(self):
        private_pem, jwks = generate_new_keypair("test-passphrase")
        private_key = serialization.load_pem_private_key(
            private_pem, password=b"test-passphrase"
        )
        claims = AuthClaims(
            identity="test-karton",
            allowed_operations=[
                ConsumeTaskOperation(),
                ProduceTaskOperation(foreign_buckets=[]),
            ],
        )
        token = issue_auth_token(
            private_key, claims, issuer="test@issuer", expire_after_seconds=3600
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            jwks_path = Path(tmp_dir) / "jwks.json"
            jwks_path.write_text(json.dumps(jwks))
            args = Namespace(jwks=str(jwks_path), token=token)
            captured = io.StringIO()
            with redirect_stdout(captured):
                cmd_validate(args)
        return json.loads(captured.getvalue()), claims

    def test_output_is_single_json_document(self):
        report, _claims = self._validated_report()
        self.assertEqual(set(report), {"header", "standard_claims", "claims"})

    def test_claims_section_round_trips(self):
        report, claims = self._validated_report()
        self.assertEqual(AuthClaims.model_validate(report["claims"]), claims)

    def test_standard_claims_are_numeric(self):
        report, _ = self._validated_report()
        self.assertEqual(report["standard_claims"]["sub"], "test-karton")
        self.assertEqual(report["standard_claims"]["iss"], "test@issuer")
        self.assertEqual(report["standard_claims"]["ver"], 1)
        self.assertIsInstance(report["standard_claims"]["iat"], int)
        self.assertIsInstance(report["standard_claims"]["exp"], int)

    def test_header_section(self):
        report, _ = self._validated_report()
        self.assertEqual(report["header"]["alg"], "ES256")


class TestDedupAndSort(unittest.TestCase):
    def test_dedup(self):
        operations = [
            ConsumeTaskOperation(),
            ProduceTaskOperation(foreign_buckets=[]),
            ConsumeTaskOperation(),
        ]
        self.assertEqual(
            dedup_and_sort_operations(operations),
            [ConsumeTaskOperation(), ProduceTaskOperation(foreign_buckets=[])],
        )

    def test_sorting_order(self):
        operations = [
            EnableDebugOperation(),
            ConsumeLogOperation(),
            RegisterBindOperation(filters=[{}], persistent=True),
            ConsumeTaskOperation(),
            ProduceTaskOperation(foreign_buckets=[]),
        ]
        self.assertEqual(
            dedup_and_sort_operations(operations),
            [
                ConsumeLogOperation(),
                ConsumeTaskOperation(),
                EnableDebugOperation(),
                ProduceTaskOperation(foreign_buckets=[]),
                RegisterBindOperation(filters=[{}], persistent=True),
            ],
        )


if __name__ == "__main__":
    unittest.main()
