"""Contract tests for Declarative Automation Bundle and GitHub Actions CI/CD workflows."""

from pathlib import Path

import yaml


def test_databricks_bundle_yaml_contract():
    """Verify databricks.yml Declarative Automation Bundle configuration."""
    bundle_path = Path("databricks.yml")
    assert bundle_path.exists(), "databricks.yml missing from root"

    content = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))

    # Bundle metadata
    assert "bundle" in content
    assert content["bundle"]["name"] == "incremental-cdc-data-platform"

    # Targets: dev and prod
    assert "targets" in content
    assert "dev" in content["targets"]
    assert "prod" in content["targets"]
    assert content["targets"]["dev"]["mode"] == "development"
    assert content["targets"]["prod"]["mode"] == "production"

    # Variables
    assert "variables" in content
    assert "catalog" in content["variables"]
    assert "schema" in content["variables"]

    # Artifact configuration
    assert "artifacts" in content
    assert "default" in content["artifacts"]
    assert content["artifacts"]["default"]["type"] == "whl"

    # Security check: no hardcoded tokens or URLs
    raw_text = bundle_path.read_text(encoding="utf-8")
    assert "token" not in raw_text.lower()
    assert "dapi" not in raw_text.lower()
    assert "adb-" not in raw_text.lower()


def test_lakeflow_pipeline_resource_contract():
    """Verify resources/lakeflow.pipeline.yml configuration, relative paths, and single-source entry."""
    resource_path = Path("resources/lakeflow.pipeline.yml")
    assert resource_path.exists(), "resources/lakeflow.pipeline.yml missing"

    content = yaml.safe_load(resource_path.read_text(encoding="utf-8"))
    assert "resources" in content
    assert "pipelines" in content["resources"]

    pipeline = content["resources"]["pipelines"]["lakeflow_auto_cdc_pipeline"]
    assert pipeline["serverless"] is True

    # Uses modern 'schema' instead of deprecated 'target'
    assert "schema" in pipeline
    assert "target" not in pipeline

    # Relative root_path resolution relative to resources/ directory
    resource_file = resource_path.resolve()
    resource_dir = resource_file.parent
    repo_root = Path(".").resolve()

    assert pipeline.get("root_path") == "..", "root_path must be '..' relative to resources/"
    assert (resource_dir / pipeline["root_path"]).resolve() == repo_root

    # Libraries: exactly one source entry pointing to pipeline.py
    libraries = pipeline.get("libraries", [])
    assert len(libraries) == 1, f"Expected exactly one library entry, found {len(libraries)}"
    assert "glob" in libraries[0] and "include" in libraries[0]["glob"]

    include_path_str = libraries[0]["glob"]["include"]
    resolved_pipeline_py = (resource_dir / include_path_str).resolve()
    expected_pipeline_py = (repo_root / "databricks/lakeflow/pipeline.py").resolve()

    assert resolved_pipeline_py.exists(), f"Resolved pipeline source {resolved_pipeline_py} does not exist"
    assert resolved_pipeline_py == expected_pipeline_py, (
        f"Pipeline source resolved to {resolved_pipeline_py}, expected {expected_pipeline_py}"
    )

    # Reject broad directory globs and ensure SQL reference is not an executable source
    raw_text = resource_path.read_text(encoding="utf-8")
    assert "databricks/lakeflow/**" not in raw_text, "Broad glob 'databricks/lakeflow/**' is prohibited"
    assert "../databricks/lakeflow/**" not in raw_text, "Broad glob '../databricks/lakeflow/**' is prohibited"
    assert "auto_cdc_reference.sql" not in raw_text, "SQL reference must not be included as pipeline source"

    # Configuration assertions
    assert "configuration" in pipeline
    assert pipeline["configuration"].get("pipelines.cdc.tombstoneGCThresholdInSeconds") == "604800"


def test_ci_workflow_contract():
    """Verify .github/workflows/ci.yml triggers, build steps, and true wheel-import isolation."""
    ci_path = Path(".github/workflows/ci.yml")
    assert ci_path.exists(), "ci.yml missing"

    content = yaml.safe_load(ci_path.read_text(encoding="utf-8"))

    # Triggers (handle YAML 1.1 boolean parsing of 'on')
    on_triggers = content.get("on") or content.get(True)
    assert on_triggers is not None
    assert "push" in on_triggers
    assert "pull_request" in on_triggers

    # Job steps
    raw_text = ci_path.read_text(encoding="utf-8")
    assert "actions/setup-python" in raw_text
    assert "3.11" in raw_text
    assert "actions/setup-java" in raw_text
    assert "temurin" in raw_text
    assert "17" in raw_text
    assert "pytest -v" in raw_text
    assert "ruff check ." in raw_text
    assert "python -m build --wheel" in raw_text
    assert "dist/*.whl" in raw_text

    # Real isolation assertions
    assert "mktemp -d" in raw_text, "Smoke test must create temporary directory outside repo"
    assert 'cd "$SMOKE_DIR"' in raw_text, "Smoke test must cd into temporary directory"
    assert "-P" in raw_text, "Smoke test must invoke python with -P safe-path option"
    assert "repo not in module_path.parents" in raw_text, "Smoke test must verify modules are not from repo checkout"
    assert "venv in module_path.parents" in raw_text, "Smoke test must verify modules are loaded from smoke venv"

    # No secrets or cloud credentials required
    assert "secrets." not in raw_text
    assert "DATABRICKS_TOKEN" not in raw_text


def test_databricks_deploy_workflow_oidc_contract():
    """Verify .github/workflows/databricks-deploy.yml uses secretless OIDC and manual dispatch."""
    deploy_path = Path(".github/workflows/databricks-deploy.yml")
    assert deploy_path.exists(), "databricks-deploy.yml missing"

    content = yaml.safe_load(deploy_path.read_text(encoding="utf-8"))

    # Manual dispatch only (handle YAML 1.1 boolean parsing of 'on')
    on_triggers = content.get("on") or content.get(True)
    assert on_triggers is not None
    assert "workflow_dispatch" in on_triggers
    assert "push" not in on_triggers

    # Permissions for OIDC
    assert "permissions" in content
    assert content["permissions"]["id-token"] == "write"
    assert content["permissions"]["contents"] == "read"

    # Environment variables for OIDC
    raw_text = deploy_path.read_text(encoding="utf-8")
    assert "DATABRICKS_AUTH_TYPE: github-oidc" in raw_text
    assert "vars.DATABRICKS_HOST" in raw_text
    assert "vars.DATABRICKS_CLIENT_ID" in raw_text
    assert "secrets." not in raw_text
    assert "DATABRICKS_TOKEN" not in raw_text

    # CLI steps: validate and deploy
    assert "databricks bundle validate" in raw_text
    assert "databricks bundle deploy" in raw_text
    assert "databricks bundle run" not in raw_text  # Deployment must not trigger execution
