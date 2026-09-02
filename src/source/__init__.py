"""Source domain package: schemas, generator, and mutation engine."""

from src.source.generator import SnapshotConfig, SourceGenerator
from src.source.mutation_engine import MutationResult, SourceMutationEngine
from src.source.schemas import (
    ACCOUNTS_SCHEMA,
    INVOICES_SCHEMA,
    PAYMENTS_SCHEMA,
    SUBSCRIPTIONS_SCHEMA,
    TABLE_PRIMARY_KEYS,
    TABLE_SCHEMAS_MAP,
    Account,
    Invoice,
    Payment,
    Subscription,
)

__all__ = [
    "ACCOUNTS_SCHEMA",
    "Account",
    "INVOICES_SCHEMA",
    "Invoice",
    "MutationResult",
    "PAYMENTS_SCHEMA",
    "Payment",
    "SUBSCRIPTIONS_SCHEMA",
    "SnapshotConfig",
    "SourceGenerator",
    "SourceMutationEngine",
    "Subscription",
    "TABLE_PRIMARY_KEYS",
    "TABLE_SCHEMAS_MAP",
]
