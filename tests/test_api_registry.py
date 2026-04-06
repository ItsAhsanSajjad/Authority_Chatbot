"""Tests for API source registry — register, update, mark missing."""
import os
import tempfile
import pytest

SAMPLE_YAML = """
source_id: {source_id}
source_type: api
display_name: "{display_name}"
enabled: {enabled}

fetch:
  method: GET
  url: https://example.com/api/{source_id}

sync:
  interval_minutes: 30
"""


def _write_config(tmpdir, source_id, display_name="Test", enabled="true"):
    path = os.path.join(tmpdir, f"{source_id}.yaml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(SAMPLE_YAML.format(
            source_id=source_id,
            display_name=display_name,
            enabled=enabled,
        ))
    return path


@pytest.fixture
def registry_setup():
    """Set up a fresh DB and registry for testing."""
    from api_db import ApiDatabase
    from api_registry import ApiSourceRegistry

    db_path = os.path.join(tempfile.gettempdir(), "pera_test_registry.db")
    # Clean up from prior run
    for f in [db_path, db_path + "-wal", db_path + "-shm"]:
        if os.path.exists(f):
            try:
                os.unlink(f)
            except OSError:
                pass

    db = ApiDatabase(db_path)
    db.migrate()
    registry = ApiSourceRegistry(db)

    yield registry, db, db_path

    # Best-effort cleanup
    for f in [db_path, db_path + "-wal", db_path + "-shm"]:
        try:
            if os.path.exists(f):
                os.unlink(f)
        except OSError:
            pass


def test_register_new_source(registry_setup):
    """Registering a new source should return 'new' and persist it."""
    registry, db, _ = registry_setup

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _write_config(tmpdir, "test_source", "Test Source")
        from api_config_models import load_api_source_config
        config = load_api_source_config(path)

        result = registry.register_or_update_source(config)
        assert result == "new"

        # Verify persisted
        source = db.get_source("test_source")
        assert source is not None
        assert source["display_name"] == "Test Source"
        assert source["status"] == "active"


def test_register_unchanged_source(registry_setup):
    """Re-registering same config should return 'unchanged'."""
    registry, db, _ = registry_setup

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _write_config(tmpdir, "stable_source")
        from api_config_models import load_api_source_config
        config = load_api_source_config(path)

        result1 = registry.register_or_update_source(config)
        assert result1 == "new"

        result2 = registry.register_or_update_source(config)
        assert result2 == "unchanged"


def test_register_updated_source(registry_setup):
    """Modifying a config file should return 'updated'."""
    registry, db, _ = registry_setup

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _write_config(tmpdir, "updating_source", "Original")
        from api_config_models import load_api_source_config

        config1 = load_api_source_config(path)
        result1 = registry.register_or_update_source(config1)
        assert result1 == "new"

        # Modify the file
        _write_config(tmpdir, "updating_source", "Updated Name")
        config2 = load_api_source_config(path)
        result2 = registry.register_or_update_source(config2)
        assert result2 == "updated"

        source = db.get_source("updating_source")
        assert source["display_name"] == "Updated Name"


def test_mark_missing_source(registry_setup):
    """Missing source configs should be marked pending_removal."""
    registry, db, _ = registry_setup

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _write_config(tmpdir, "will_disappear")
        from api_config_models import load_api_source_config
        config = load_api_source_config(path)
        registry.register_or_update_source(config)

    # Now source file is gone
    marked = registry.mark_missing_sources(
        current_source_ids=set(),  # nothing found on disk
        enable_grace=True,
    )
    assert "will_disappear" in marked

    source = db.get_source("will_disappear")
    assert source["status"] == "pending_removal"


def test_mark_missing_no_grace(registry_setup):
    """Without grace, missing sources should go directly to removed."""
    registry, db, _ = registry_setup

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _write_config(tmpdir, "instant_remove")
        from api_config_models import load_api_source_config
        config = load_api_source_config(path)
        registry.register_or_update_source(config)

    marked = registry.mark_missing_sources(
        current_source_ids=set(),
        enable_grace=False,
    )
    assert "instant_remove" in marked
    source = db.get_source("instant_remove")
    assert source["status"] == "removed"


def test_get_active_sources(registry_setup):
    """get_active_sources should return only active sources."""
    registry, db, _ = registry_setup

    with tempfile.TemporaryDirectory() as tmpdir:
        _write_config(tmpdir, "active_one")
        _write_config(tmpdir, "active_two")
        _write_config(tmpdir, "disabled_one", enabled="false")

        from api_config_models import load_api_source_config
        for fname in os.listdir(tmpdir):
            path = os.path.join(tmpdir, fname)
            config = load_api_source_config(path)
            registry.register_or_update_source(config)

    actives = registry.get_active_sources()
    active_ids = {s.source_id for s in actives}
    assert "active_one" in active_ids
    assert "active_two" in active_ids
    assert "disabled_one" not in active_ids


def test_set_source_status(registry_setup):
    """set_source_status should update status and message."""
    registry, db, _ = registry_setup

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _write_config(tmpdir, "status_test")
        from api_config_models import load_api_source_config
        config = load_api_source_config(path)
        registry.register_or_update_source(config)

    registry.set_source_status("status_test", "error", "Config parse failed")
    source = db.get_source("status_test")
    assert source["status"] == "error"
    assert "parse failed" in source["status_message"]


def test_set_invalid_status(registry_setup):
    """set_source_status should reject invalid statuses."""
    registry, _, _ = registry_setup
    with pytest.raises(ValueError, match="Invalid status"):
        registry.set_source_status("any_source", "flying", "Invalid")
