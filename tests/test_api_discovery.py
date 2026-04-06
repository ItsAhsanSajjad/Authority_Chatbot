"""Tests for API source discovery — detect new, changed, removed configs."""
import os
import tempfile
import pytest

SAMPLE_YAML = """
source_id: {source_id}
source_type: api
display_name: "{display_name}"
enabled: true

fetch:
  method: GET
  url: https://example.com/api/{source_id}
"""


def _write_config(tmpdir, source_id, display_name="Test"):
    path = os.path.join(tmpdir, f"{source_id}.yaml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(SAMPLE_YAML.format(source_id=source_id, display_name=display_name))
    return path


@pytest.fixture
def discovery_setup():
    """Fresh DB, registry, and discovery for testing."""
    from api_db import ApiDatabase
    from api_registry import ApiSourceRegistry
    from api_discovery import ApiSourceDiscovery

    db_path = os.path.join(tempfile.gettempdir(), "pera_test_discovery.db")
    for f in [db_path, db_path + "-wal", db_path + "-shm"]:
        if os.path.exists(f):
            try:
                os.unlink(f)
            except OSError:
                pass

    db = ApiDatabase(db_path)
    db.migrate()
    registry = ApiSourceRegistry(db)

    tmpdir = tempfile.mkdtemp()

    discovery = ApiSourceDiscovery(registry, source_dir=tmpdir)

    yield discovery, registry, db, tmpdir, db_path

    # Best-effort cleanup
    import shutil
    try:
        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        pass
    for f in [db_path, db_path + "-wal", db_path + "-shm"]:
        try:
            if os.path.exists(f):
                os.unlink(f)
        except OSError:
            pass


def test_discover_new_configs(discovery_setup):
    """New YAML files should be detected and registered."""
    discovery, registry, db, tmpdir, _ = discovery_setup

    _write_config(tmpdir, "new_source_a", "Source A")
    _write_config(tmpdir, "new_source_b", "Source B")

    result = discovery.reconcile_sources()

    assert "new_source_a" in result.new_sources
    assert "new_source_b" in result.new_sources
    assert len(result.updated_sources) == 0
    assert result.has_changes is True

    # Verify in DB
    source_a = db.get_source("new_source_a")
    assert source_a is not None
    assert source_a["status"] == "active"


def test_discover_unchanged_configs(discovery_setup):
    """Re-running discovery on same configs should show unchanged."""
    discovery, registry, db, tmpdir, _ = discovery_setup

    _write_config(tmpdir, "stable_source")
    result1 = discovery.reconcile_sources()
    assert "stable_source" in result1.new_sources

    result2 = discovery.reconcile_sources()
    assert "stable_source" in result2.unchanged_sources
    assert len(result2.new_sources) == 0


def test_discover_changed_configs(discovery_setup):
    """Modified config files should be detected as updated."""
    discovery, registry, db, tmpdir, _ = discovery_setup

    _write_config(tmpdir, "changing_source", "Original Name")
    result1 = discovery.reconcile_sources()
    assert "changing_source" in result1.new_sources

    # Modify the config
    _write_config(tmpdir, "changing_source", "New Name")
    result2 = discovery.reconcile_sources()
    assert "changing_source" in result2.updated_sources


def test_discover_removed_configs(discovery_setup):
    """Deleted config files should mark sources as pending_removal."""
    discovery, registry, db, tmpdir, _ = discovery_setup

    path = _write_config(tmpdir, "will_be_removed")
    result1 = discovery.reconcile_sources()
    assert "will_be_removed" in result1.new_sources

    # Remove the config file
    os.unlink(path)

    result2 = discovery.reconcile_sources()
    assert "will_be_removed" in result2.removed_sources

    source = db.get_source("will_be_removed")
    assert source["status"] == "pending_removal"


def test_discovery_result_summary(discovery_setup):
    """DiscoveryResult.summary should return readable string."""
    discovery, _, _, tmpdir, _ = discovery_setup

    _write_config(tmpdir, "alpha")
    _write_config(tmpdir, "beta")

    result = discovery.reconcile_sources()
    summary = result.summary()
    assert "New: 2" in summary
    assert "Scanned:" in summary


def test_discover_empty_directory(discovery_setup):
    """Empty source dir should produce no changes."""
    discovery, _, _, tmpdir, _ = discovery_setup
    # tmpdir is empty
    result = discovery.reconcile_sources()
    assert result.total_scanned == 0
    assert result.has_changes is False


def test_invalid_yaml_skipped(discovery_setup):
    """Invalid YAML files should be skipped with errors recorded."""
    discovery, _, _, tmpdir, _ = discovery_setup

    # Write invalid YAML
    bad_path = os.path.join(tmpdir, "broken.yaml")
    with open(bad_path, "w") as f:
        f.write("source_type: api\nfetch:\n  url: https://example.com\n")
        # Missing source_id → validation error

    result = discovery.reconcile_sources()
    assert len(result.error_sources) > 0
