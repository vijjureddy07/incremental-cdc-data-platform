"""Pytest shared fixtures for Module 1, 2, and 3 test suites."""

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.cdc.generator import CDCScenarioGenerator
from src.source.generator import SnapshotConfig, SourceGenerator
from src.source.mutation_engine import SourceMutationEngine


def _ensure_java_home() -> None:
    """Ensure JAVA_HOME is configured to a valid JVM runtime on macOS / Linux."""
    if "JAVA_HOME" not in os.environ or not (Path(os.environ["JAVA_HOME"]) / "bin" / "java").exists():
        candidate_paths = [
            Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
            Path("/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"),
            Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
            Path("/usr/lib/jvm/java-17-openjdk"),
            Path("/usr/lib/jvm/default-java"),
        ]
        for candidate in candidate_paths:
            if (candidate / "bin" / "java").exists():
                os.environ["JAVA_HOME"] = str(candidate)
                os.environ["PATH"] = f"{candidate}/bin:{os.environ.get('PATH', '')}"
                break


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Fixture providing a session-scoped PySpark local test session."""
    _ensure_java_home()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("CDC_Normalization_UnitTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


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
