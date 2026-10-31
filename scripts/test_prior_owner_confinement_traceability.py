"""Confinement metadata checks do not execute or qualify Rust runtime behavior.

Private draft: adopt only this NEW file for a genuine metadata RED first. The
pending synthetic records below exercise checker wiring, not runtime promotion.
Root must bind final YAML to independently reviewed actual evidence separately.
"""
import contextlib
import copy
import io
import unittest
from unittest.mock import patch

import check_contracts


GROUP = "prior_owner_confinement_requirements"
SOURCE = "spec/runtime/prior-owner-confinement-design.md"
SCOPE = "explicit_bounded_native_prior_owner_confinement_only"
SCOPED_PASS = "passed_bounded_prior_owner_native_macos_linux_scope"
# One source-existing representative per agreed semantic requirement category.
# HTTP implementation expectations are pending; these are not independent-public
# qualification evidence and are never assigned a passed status by this fixture.
PENDING_REFERENCES = (
    ('crates/b4-runtime/tests/recovery_confinement.rs',
     'memory_target_guards_and_arc_reference_forwarding_preserve_custom_default_refusal'),
    ('crates/b4-kernel/tests/recovery_confinement.rs',
     'accepted_proposal_preserves_all_fields_for_each_reason_and_actual_owner_jump'),
    ('crates/b4-kernel/tests/recovery_confinement.rs',
     'terminal_and_old_recovery_blocked_inputs_refuse_without_rewriting_their_history'),
    ('crates/b4-runtime/tests/recovery_confinement.rs',
     'memory_actual_volatile_grant_profile_remains_current_owner_and_exactly_unchanged'),
    ('crates/b4-sqlite/tests/recovery_confinement.rs',
     'sqlite_pending_durable_grant_survives_confinement_and_actual_owner_reopen'),
    ('crates/b4-sqlite/tests/recovery_confinement.rs',
     'sqlite_legacy_direct_once_and_always_without_durable_rows_remain_unsupported'),
    ('crates/b4-sqlite/tests/recovery_confinement.rs',
     'sqlite_compact_raw_failure_drain_is_actually_confined_when_replacement_shrinks'),
    ('crates/b4-sqlite/tests/recovery_confinement.rs',
     'sqlite_thread_component_cap_counts_raw_metadata_origin_and_fixed_identity_scalars'),
    ('crates/b4-sqlite/tests/recovery_confinement.rs',
     'sqlite_real_grant_raw_128k_boundary_changes_spelling_only_and_is_inclusive'),
    ('crates/b4-sqlite/tests/recovery_confinement.rs',
     'sqlite_actual_required_durable_pending_grant_count_has_inclusive_64_record_limit'),
    ('crates/b4-sqlite/tests/recovery_confinement.rs',
     'sqlite_real_pending_grants_can_cross_8mib_aggregate_while_each_row_and_count_fits'),
    ('crates/b4-sqlite/tests/recovery_confinement.rs',
     'sqlite_atomic_boundary_rejects_changed_whole_original_view_with_matching_fences'),
    ('crates/b4-http/src/confinement/tests.rs',
     'confinement_unpolled_future_borrows_handles_without_owner_or_mutation'),
    ('crates/b4-http/src/confinement/tests.rs',
     'confinement_missing_exact_definition_calls_only_separate_authority_with_original'),
    ('crates/b4-http/src/confinement/tests.rs',
     'confinement_renewal_checks_whole_view_not_only_revision_and_incarnation'),
    ('crates/b4-http/src/confinement/tests.rs',
     'confinement_shutdown_cut_loses_before_lease_or_finishes_after_won_lease'),
    ('crates/b4-http/src/confinement/tests.rs',
     'confinement_owned_core_retains_verified_receipt_through_authority_replacement_payloads'),
)


def pending_group():
    return [{
        "id": f"CONFINE-{number:03d}",
        "classification": "NATIVE",
        "statement": "Pending synthetic confinement metadata-hook record.",
        "source": SOURCE,
        "component": "prior-owner-confinement",
        "acceptance_tests": [{"file": file, "name": name}],
        "implementation_status": "pending",
        "verification_status": "not_run",
        "release_scope": SCOPE,
    } for number, (file, name) in enumerate(PENDING_REFERENCES, 1)]


class PriorOwnerConfinementTraceabilityChecks(unittest.TestCase):
    def checked_requirements(self, mutate=None):
        original_load = check_contracts.yaml.safe_load
        retained = {}

        def with_confinement_group(source):
            value = original_load(source)
            if isinstance(value, dict) and "foundation_requirements" in value:
                value = copy.deepcopy(value)
                retained["before"] = copy.deepcopy(value)
                value[GROUP] = pending_group()
                if mutate:
                    mutate(value)
                retained["checked"] = value
            return value

        output = io.StringIO()
        with patch.object(check_contracts.yaml, "safe_load", with_confinement_group), \
                contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        self.assertTrue(retained, "Actual requirements YAML was not checked")
        self.assertEqual(
            {key: value for key, value in retained["checked"].items() if key != GROUP},
            {key: value for key, value in retained["before"].items() if key != GROUP},
            "Checking the new group must preserve every old group/status/requirement",
        )
        return output.getvalue()

    def test_new_group_is_separate_and_preserves_every_prior_scope(self):
        requirements_path = check_contracts.REPO / "spec/runtime/requirements.yaml"
        before = check_contracts.yaml.safe_load(requirements_path.read_text())
        self.assertTrue(GROUP in before, "Actual YAML must contain the separate confinement group")
        self.assertEqual(sorted(item["id"] for item in before[GROUP]),
                         [f"CONFINE-{number:03d}" for number in range(1, 18)])
        output_buffer = io.StringIO()
        with contextlib.redirect_stdout(output_buffer):
            check_contracts.check_contracts()
        output = output_buffer.getvalue()
        after = check_contracts.yaml.safe_load(requirements_path.read_text())
        self.assertEqual({key: value for key, value in after.items() if key != GROUP},
                         {key: value for key, value in before.items() if key != GROUP},
                         "Every existing group/status/requirement must remain exact")
        self.assertIn("Prior-owner confinement: 17 named local requirements; "
                      "explicit bounded native scope only; metadata is not runtime evidence.", output)
        for prior in (
            "Traceability: 338 named local requirements",
            "Unchanged client corpus: 8 named local requirements",
            "Native cooperative cancellation: 15 named local requirements",
            "Unfinished discovery: 19 named local requirements",
            "Startup recovery preparation: 28 named local requirements",
            "Historical native Definitions: 31 named local requirements",
            "eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending",
            "CP-010 unresolved; M0 open",
            "named-test existence do not assert fresh test execution",
        ):
            self.assertIn(prior, output)

    def test_new_group_requires_narrow_scope_and_recorded_pass_evidence(self):
        changes = (
            {"classification": "COMPAT"},
            {"source": "spec/runtime/0001-agent-protocol-core.md"},
            {"component": "automatic-restart-readiness"},
            {"release_scope": "full_rfc11_4_or_core_profile"},
            {"implementation_status": "complete"},
            {"verification_status": "passed"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS},
        )
        for change in changes:
            with self.subTest(change=change):
                def mutate(value):
                    value[GROUP][0].update(change)
                with self.assertRaisesRegex(ValueError, "Prior-owner confinement"):
                    self.checked_requirements(mutate)

    def test_new_group_refuses_missing_required_cases_names_and_cross_group_duplicates(self):
        # Remove two independently required layers, not merely the last record.
        for identity in ("CONFINE-010", "CONFINE-017"):
            with self.subTest(missing_case=identity):
                def mutate(value):
                    value[GROUP] = [item for item in value[GROUP] if item["id"] != identity]
                with self.assertRaisesRegex(ValueError, "Prior-owner confinement scoped case inventory"):
                    self.checked_requirements(mutate)

        def empty_case(value):
            value[GROUP][9]["acceptance_tests"] = []
        with self.assertRaisesRegex(ValueError, "Prior-owner confinement.*acceptance"):
            self.checked_requirements(empty_case)

        def missing_name(value):
            value[GROUP][9]["acceptance_tests"][0]["name"] = "missing_confinement_journal_qualifier"
        with self.assertRaisesRegex(ValueError, "Missing named local test"):
            self.checked_requirements(missing_name)

        for previous in (
            "foundation_requirements", "unchanged_ap_client_requirements",
            "native_cooperative_cancellation_requirements", "unfinished_discovery_requirements",
            "startup_recovery_preparation_requirements", "historical_native_definitions_requirements",
        ):
            with self.subTest(previous=previous):
                def mutate(value):
                    value[GROUP][0]["id"] = value[previous][0]["id"]
                with self.assertRaisesRegex(ValueError, "Duplicate named local requirement IDs"):
                    self.checked_requirements(mutate)


if __name__ == "__main__":
    unittest.main()
