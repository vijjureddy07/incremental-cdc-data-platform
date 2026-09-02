"""Integration tests for the end-to-end source snapshot and CDC simulator."""

import tempfile
from pathlib import Path

from src.cdc.generator import CDCScenarioGenerator
from src.cdc.serialization import write_cdc_batch_jsonl
from src.cdc.validator import CDCValidator
from src.source.generator import SnapshotConfig, SourceGenerator
from src.source.mutation_engine import SourceMutationEngine


def test_full_simulation_lifecycle():
    """Verify complete end-to-end simulation lifecycle:

    1. Generate and persist initial source snapshot (Parquet).
    2. Initialize state reconciliation engine.
    3. Generate and land deterministic CDC batches 1 & 2 (JSONL).
    4. Validate all landing events and quarantine invalid fixtures in batch 3.
    5. Replay change batches in sequence-aware order.
    6. Verify final source state reconciliation and entity counts.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        snapshot_dir = base_dir / "data" / "source_snapshot"
        cdc_landing_dir = base_dir / "data" / "cdc_landing"

        # Step 1: Initial Snapshot Generation & Parquet persistence
        source_gen = SourceGenerator(SnapshotConfig(seed=42))
        parquet_paths = source_gen.persist_snapshot_parquet(base_dir=snapshot_dir)
        initial_dicts = source_gen.generate_snapshot_dicts()

        assert len(parquet_paths) == 4
        assert len(initial_dicts["accounts"]) == 40
        assert len(initial_dicts["subscriptions"]) == 60
        assert len(initial_dicts["invoices"]) == 120
        assert len(initial_dicts["payments"]) == 90

        # Step 2: Initialize Mutation Engine
        engine = SourceMutationEngine(initial_state=initial_dicts)
        assert engine.get_table_row_count("accounts") == 40

        # Step 3: Generate CDC Batches
        cdc_gen = CDCScenarioGenerator(source_generator=source_gen)
        batch_1 = cdc_gen.generate_batch_1_inserts_and_updates("batch_001")
        batch_2 = cdc_gen.generate_batch_2_advanced_cdc_scenarios("batch_002")
        batch_3_invalid = cdc_gen.generate_batch_3_quarantine_fixtures("batch_003_quarantine")

        # Step 4: Write & Read JSON Lines from Landing Area
        all_valid_events = batch_1 + batch_2
        written_files = write_cdc_batch_jsonl(all_valid_events, output_base_dir=cdc_landing_dir)
        assert len(written_files) > 0

        # Step 5: Validate and Quarantine Testing
        for raw_invalid in batch_3_invalid:
            val_res = CDCValidator.validate(raw_invalid)
            assert not val_res.is_valid, f"Expected invalid event to fail: {raw_invalid}"

        # Step 6: Apply Valid Changes to Mutation Engine
        # Apply Batch 1
        res_b1 = engine.apply_batch(batch_1, sort_by_sequence=True)
        assert res_b1.applied_count == 8
        assert res_b1.invalid_count == 0

        # Apply Batch 2
        res_b2 = engine.apply_batch(batch_2, sort_by_sequence=True)
        assert res_b2.applied_count == 4  # 4 applied (del, late, ooo 101, ooo 102), 1 duplicate ignored
        assert res_b2.duplicate_count == 1

        # Step 7: Final Reconciliation Assertions
        # Final Row Counts:
        # accounts: 40 + 1 (ACC-0041) = 41
        # subscriptions: 60 + 1 (SUB-0061) = 61
        # invoices: 120 + 1 (INV-0121) = 121
        # payments: 90 + 1 (PAY-0091) - 1 (PAY-0002 deleted) = 90
        assert engine.get_table_row_count("accounts") == 41
        assert engine.get_table_row_count("subscriptions") == 61
        assert engine.get_table_row_count("invoices") == 121
        assert engine.get_table_row_count("payments") == 90

        # Verify mutated values:
        # 1. ACC-0041 inserted
        acc_41 = engine.get_record("accounts", "ACC-0041")
        assert acc_41 is not None
        assert acc_41["account_name"] == "Apex Cloud Analytics"

        # 2. ACC-0001 status changed to SUSPENDED
        acc_1 = engine.get_record("accounts", "ACC-0001")
        assert acc_1 is not None
        assert acc_1["status"] == "SUSPENDED"

        # 3. SUB-0001 plan changed to ENTERPRISE with $1299.00
        sub_1 = engine.get_record("subscriptions", "SUB-0001")
        assert sub_1 is not None
        assert sub_1["plan_name"] == "ENTERPRISE"
        assert sub_1["monthly_amount"] == "1299.00"

        # 4. INV-0001 status changed to PAID
        inv_1 = engine.get_record("invoices", "INV-0001")
        assert inv_1 is not None
        assert inv_1["invoice_status"] == "PAID"

        # 5. PAY-0001 status changed to REFUNDED
        pay_1 = engine.get_record("payments", "PAY-0001")
        assert pay_1 is not None
        assert pay_1["payment_status"] == "REFUNDED"

        # 6. PAY-0002 deleted
        assert engine.get_record("payments", "PAY-0002") is None

        # 7. Out-of-order resolved state for ACC-0002 (seq 102 state)
        acc_2 = engine.get_record("accounts", "ACC-0002")
        assert acc_2 is not None
        assert acc_2["account_name"] == "Healthcare Solutions 2 Inc"
        assert acc_2["status"] == "ACTIVE"
        assert acc_2["country"] == "CA"

        # 8. Late arrival state for SUB-0002
        sub_2 = engine.get_record("subscriptions", "SUB-0002")
        assert sub_2 is not None
        assert sub_2["billing_cycle"] == "ANNUAL"
