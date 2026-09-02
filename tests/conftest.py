"""Pytest shared fixtures for Module 1 test suites."""

import pytest

from src.cdc.generator import CDCScenarioGenerator
from src.source.generator import SnapshotConfig, SourceGenerator
from src.source.mutation_engine import SourceMutationEngine


@pytest.fixture
def snapshot_config() -> SnapshotConfig:
    """Fixture returning default deterministic snapshot configuration."""
    return SnapshotConfig(
        num_accounts=40,
        num_subscriptions=60,
        num_invoices=120,
        num_payments=90,
        seed=42,
    )


@pytest.fixture
def source_gen(snapshot_config: SnapshotConfig) -> SourceGenerator:
    """Fixture returning configured deterministic SourceGenerator."""
    return SourceGenerator(config=snapshot_config)


@pytest.fixture
def initial_snapshot(source_gen: SourceGenerator) -> dict[str, list[dict]]:
    """Fixture generating initial snapshot records as dictionary."""
    return source_gen.generate_snapshot_dicts()


@pytest.fixture
def mutation_engine(initial_snapshot: dict[str, list[dict]]) -> SourceMutationEngine:
    """Fixture returning mutation engine initialized with standard snapshot."""
    return SourceMutationEngine(initial_state=initial_snapshot)


@pytest.fixture
def cdc_gen(source_gen: SourceGenerator) -> CDCScenarioGenerator:
    """Fixture returning CDC scenario generator."""
    return CDCScenarioGenerator(source_generator=source_gen)
