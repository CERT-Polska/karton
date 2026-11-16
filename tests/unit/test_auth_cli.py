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
    parse_register_bind_args,
    pin_origin,
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
        "egress_filters": None,
        "inspect_karton": False,
        "restart_task": False,
        "cancel_task": False,
        "remove_bind": False,
        "get_metrics": False,
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


class TestParseRegisterBindArgs(unittest.TestCase):
    def test_none(self):
        self.assertEqual(parse_register_bind_args(None), [])

    def test_empty_list(self):
        self.assertEqual(parse_register_bind_args([]), [])

    def test_single_bind(self):
        operations = parse_register_bind_args(
            ['{"filters": [{"type": "t"}], "persistent": true}']
        )
        self.assertEqual(
            operations,
            [RegisterBindOperation(filters=[{"type": "t"}], persistent=True)],
        )

    def test_multiple_binds(self):
        operations = parse_register_bind_args(
            [
                '{"filters": [{"type": "a"}], "persistent": true}',
                '{"filters": [{"type": "b"}], "persistent": false}',
            ]
        )
        self.assertEqual(
            operations,
            [
                RegisterBindOperation(filters=[{"type": "a"}], persistent=True),
                RegisterBindOperation(filters=[{"type": "b"}], persistent=False),
            ],
        )

    def test_invalid_json(self):
        with self.assertRaises(ValueError):
            parse_register_bind_args(["not json"])

    def test_not_an_object(self):
        with self.assertRaises(ValueError):
            parse_register_bind_args(['["a"]'])

    def test_missing_fields(self):
        with self.assertRaises(ValueError):
            parse_register_bind_args(['{"filters": [{"type": "t"}]}'])

    def test_wrong_operation_type(self):
        with self.assertRaises(ValueError):
            parse_register_bind_args(
                ['{"operation": "consume_task", "filters": [], "persistent": true}']
            )


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


def pinned_origin_condition(identity):
    """Origin condition pinned to the identity, as built by the CLI."""

    from karton.core.auth.cli import pin_origin

    return pin_origin([{}], identity=identity)[0]["origin"]


def expected_produce_op(identity="x", foreign_buckets=None, egress_filters=None):
    """ProduceTaskOperation expected from claims_from_args for a given identity."""
    if egress_filters is None:
        egress_filters = [{}]
    return ProduceTaskOperation(
        foreign_buckets=foreign_buckets or [],
        headers=[
            {**variant, "origin": pinned_origin_condition(identity)}
            for variant in egress_filters
        ],
    )


class TestClaimsWithKartonType(unittest.TestCase):
    FILTERS = ['[{"type": "example.RawFeed"}]']

    def operations_of(self, **kwargs):
        claims = claims_from_args(make_args(**kwargs))
        return claims.allowed_operations

    def test_producer(self):
        operations = self.operations_of(identity="p", karton_type="producer")
        self.assertEqual(operations, [expected_produce_op("p")])

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
                expected_produce_op("k"),
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
                expected_produce_op("k"),
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
                expected_produce_op("k"),
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

    def test_producer_accepts_filters(self):
        # --filters is never rejected: it composes freely with every
        # --karton-type instead of adding special-case bans
        operations = self.operations_of(
            identity="p", karton_type="producer", filters=self.FILTERS
        )
        self.assertEqual(
            operations,
            [
                expected_produce_op("p"),
                RegisterBindOperation(
                    filters=[{"type": "example.RawFeed"}], persistent=True
                ),
            ],
        )

    def test_logconsumer_accepts_filters(self):
        operations = self.operations_of(
            identity="l", karton_type="logconsumer", filters=self.FILTERS
        )
        self.assertEqual(
            operations,
            [
                ConsumeLogOperation(),
                RegisterBindOperation(
                    filters=[{"type": "example.RawFeed"}], persistent=True
                ),
            ],
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
            [
                ConsumeLogOperation(),
                expected_produce_op("x"),
            ],
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
            operations,
            [expected_produce_op("x", foreign_buckets=["b1", "b2"])],
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
            [
                ConsumeLogOperation(),
                expected_produce_op("x"),
            ],
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

    def test_invalid_register_bind_json_error(self):
        # Malformed --register-bind JSON is reported as a friendly
        # ValueError instead of a raw JSONDecodeError traceback
        with self.assertRaises(ValueError):
            claims_from_args(make_args(identity="x", register_bind=["not json"]))


class TestClaimsWithEgressFilters(unittest.TestCase):
    EGRESS = ['[{"kind": "raw"}, {"kind": "file", "extension": "txt"}]']

    def operations_of(self, **kwargs):
        claims = claims_from_args(make_args(**kwargs))
        return claims.allowed_operations

    def test_egress_filters_require_produce_task(self):
        with self.assertRaises(ValueError):
            self.operations_of(identity="p", egress_filters=self.EGRESS)

    def test_egress_filters_with_produce_task(self):
        operations = self.operations_of(
            identity="p", produce_task=True, egress_filters=self.EGRESS
        )
        self.assertEqual(
            operations,
            [
                expected_produce_op(
                    "p",
                    egress_filters=[
                        {"kind": "raw"},
                        {"kind": "file", "extension": "txt"},
                    ],
                )
            ],
        )

    def test_egress_filters_concatenated(self):
        operations = self.operations_of(
            identity="p",
            produce_task=True,
            egress_filters=['[{"kind": "raw"}]', '[{"kind": "file"}]'],
        )
        self.assertEqual(
            operations,
            [
                expected_produce_op(
                    "p", egress_filters=[{"kind": "raw"}, {"kind": "file"}]
                )
            ],
        )

    def test_egress_filters_empty_list(self):
        # Explicit empty egress filter list allows to produce nothing.
        # Origin pinning doesn't change that.
        operations = self.operations_of(
            identity="p", produce_task=True, egress_filters=["[]"]
        )
        self.assertEqual(
            operations, [ProduceTaskOperation(foreign_buckets=[], headers=[])]
        )

    def test_egress_filters_with_karton_type_producer(self):
        operations = self.operations_of(
            identity="p", karton_type="producer", egress_filters=self.EGRESS
        )
        self.assertEqual(
            operations,
            [
                expected_produce_op(
                    "p",
                    egress_filters=[
                        {"kind": "raw"},
                        {"kind": "file", "extension": "txt"},
                    ],
                )
            ],
        )

    def test_egress_filters_with_foreign_buckets(self):
        operations = self.operations_of(
            identity="p",
            foreign_buckets=["b1"],
            egress_filters=self.EGRESS,
        )
        self.assertEqual(
            operations,
            [
                expected_produce_op(
                    "p",
                    foreign_buckets=["b1"],
                    egress_filters=[
                        {"kind": "raw"},
                        {"kind": "file", "extension": "txt"},
                    ],
                )
            ],
        )

    def test_invalid_json(self):
        with self.assertRaises(ValueError):
            claims_from_args(
                make_args(identity="p", produce_task=True, egress_filters=["not json"])
            )

    def test_not_a_list(self):
        with self.assertRaises(ValueError):
            claims_from_args(
                make_args(
                    identity="p",
                    produce_task=True,
                    egress_filters=['{"kind": "raw"}'],
                )
            )


class TestPinOriginInEgressFilters(unittest.TestCase):
    def pin(self, egress_filters, identity="karton.sample"):
        return pin_origin(egress_filters, identity=identity)

    def test_origin_condition_added_to_each_variant(self):
        pinned = self.pin([{"kind": "raw"}, {"kind": "file", "extension": "txt"}])
        origin = pinned[0]["origin"]
        self.assertEqual(set(pinned[0]), {"kind", "origin"})
        self.assertEqual(pinned[0]["kind"], "raw")
        self.assertEqual(pinned[1]["origin"], origin)
        self.assertEqual(pinned[1]["kind"], "file")

    def test_catch_all_variant_gets_origin_condition(self):
        pinned = self.pin([{}])
        self.assertEqual(set(pinned[0]), {"origin"})

    def test_empty_filters_stay_empty(self):
        self.assertEqual(self.pin([]), [])

    def test_pinned_claims_cover_own_identity_only(self):
        from karton.core import query

        pinned = self.pin([{"kind": "raw"}])
        match = query.convert(pinned).match

        # Own identity, also with extra headers
        self.assertTrue(match({"kind": "raw", "origin": "karton.sample"}))
        self.assertTrue(match({"kind": "raw", "origin": "karton.sample", "x": "y"}))
        # Debug mode instance identity
        self.assertTrue(
            match(
                {
                    "kind": "raw",
                    "origin": "karton.sample-550e8400-e29b-41d4-a716-446655440000-dev",
                }
            )
        )
        # Someone else's identity or lookalikes are not covered
        self.assertFalse(match({"kind": "raw", "origin": "karton.other"}))
        self.assertFalse(match({"kind": "raw", "origin": "karton.sample2"}))
        self.assertFalse(match({"kind": "raw", "origin": "karton.sample-impostor"}))
        # Missing origin is not covered
        self.assertFalse(match({"kind": "raw"}))

    def test_identity_with_regex_specials_is_escaped(self):
        pinned = self.pin([{}], identity="karton.foo+bar")
        from karton.core import query

        match = query.convert(pinned).match
        self.assertTrue(match({"origin": "karton.foo+bar"}))
        self.assertFalse(match({"origin": "karton.foobar"}))


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
                ProduceTaskOperation(foreign_buckets=[], headers=[{}]),
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
            ProduceTaskOperation(foreign_buckets=[], headers=[{}]),
            ConsumeTaskOperation(),
        ]
        self.assertEqual(
            dedup_and_sort_operations(operations),
            [
                ConsumeTaskOperation(),
                ProduceTaskOperation(foreign_buckets=[], headers=[{}]),
            ],
        )

    def test_sorting_order(self):
        operations = [
            EnableDebugOperation(),
            ConsumeLogOperation(),
            RegisterBindOperation(filters=[{}], persistent=True),
            ConsumeTaskOperation(),
            ProduceTaskOperation(foreign_buckets=[], headers=[{}]),
        ]
        self.assertEqual(
            dedup_and_sort_operations(operations),
            [
                ConsumeLogOperation(),
                ConsumeTaskOperation(),
                EnableDebugOperation(),
                ProduceTaskOperation(foreign_buckets=[], headers=[{}]),
                RegisterBindOperation(filters=[{}], persistent=True),
            ],
        )


if __name__ == "__main__":
    unittest.main()
