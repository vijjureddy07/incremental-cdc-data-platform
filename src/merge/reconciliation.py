"""Reconciliation engine comparing Delta current-state business state against Module 1 mutation oracle."""

from datetime import UTC
from decimal import Decimal
from typing import Any

from src.merge.target_store import DeltaTargetStore
from src.source.mutation_engine import SourceMutationEngine
from src.source.schemas import TABLE_PRIMARY_KEYS, TABLE_SCHEMAS_MAP
from src.utils.helpers import format_decimal, format_iso_date


def reconcile_delta_against_mutation_oracle(
    target_store: DeltaTargetStore,
    mutation_engine: SourceMutationEngine,
) -> dict[str, Any]:
    """Reconcile all 4 Delta Lake current-state tables against the Module 1 mutation oracle.

    Args:
        target_store: The DeltaTargetStore containing current-state tables.
        mutation_engine: The SourceMutationEngine tracking expected business state.

    Returns:
        Reconciliation summary dictionary with exact counts and any detected mismatches.
    """
    oracle_state = mutation_engine.get_snapshot_state()
    report: dict[str, Any] = {
        "is_reconciled": True,
        "counts": {},
        "mismatches": {},
    }

    for table_name in TABLE_SCHEMAS_MAP:
        pk = TABLE_PRIMARY_KEYS[table_name]
        oracle_records = oracle_state.get(table_name, [])
        oracle_by_pk: dict[str, dict[str, Any]] = {str(r[pk]): dict(r) for r in oracle_records}

        # Read Delta table excluding CDC operational metadata columns
        delta_df = target_store.read_current_table(
            table_name=table_name,
            include_metadata=False,
            include_deleted=False,
        )
        delta_rows = delta_df.collect()

        delta_by_pk: dict[str, dict[str, Any]] = {}
        for row in delta_rows:
            row_dict = row.asDict()
            # Normalize types to match oracle dict format
            norm_dict: dict[str, Any] = {}
            for k, v in row_dict.items():
                if v is None:
                    norm_dict[k] = None
                elif isinstance(v, Decimal):
                    norm_dict[k] = format_decimal(v, 2)
                elif hasattr(v, "isoformat"):
                    if hasattr(v, "hour"):
                        # PySpark collect() produces naive local datetimes; convert to UTC
                        v_utc = v.astimezone(UTC)
                        norm_dict[k] = v_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                    else:
                        norm_dict[k] = format_iso_date(v)
                else:
                    norm_dict[k] = str(v) if not isinstance(v, (int, float, bool)) else v

            pk_val = str(norm_dict[pk])
            delta_by_pk[pk_val] = norm_dict

        table_mismatches: list[dict[str, Any]] = []

        # Count check
        report["counts"][table_name] = {
            "delta_count": len(delta_by_pk),
            "oracle_count": len(oracle_by_pk),
        }

        # Check all keys present in oracle
        for pk_val, oracle_row in oracle_by_pk.items():
            if pk_val not in delta_by_pk:
                table_mismatches.append(
                    {
                        "primary_key": pk_val,
                        "error": "Missing in Delta target",
                        "oracle": oracle_row,
                        "delta": None,
                    }
                )
            else:
                delta_row = delta_by_pk[pk_val]
                # Compare each field
                diffs = {}
                for col, o_val in oracle_row.items():
                    d_val = delta_row.get(col)
                    if str(o_val) != str(d_val):
                        diffs[col] = {"oracle": o_val, "delta": d_val}
                if diffs:
                    table_mismatches.append(
                        {
                            "primary_key": pk_val,
                            "error": "Column value mismatch",
                            "diffs": diffs,
                        }
                    )

        # Check for unexpected rows in Delta
        for pk_val in delta_by_pk:
            if pk_val not in oracle_by_pk:
                table_mismatches.append(
                    {
                        "primary_key": pk_val,
                        "error": "Unexpected row in Delta target (not in oracle)",
                        "oracle": None,
                        "delta": delta_by_pk[pk_val],
                    }
                )

        if table_mismatches or len(delta_by_pk) != len(oracle_by_pk):
            report["is_reconciled"] = False
            report["mismatches"][table_name] = table_mismatches

    return report
