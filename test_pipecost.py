"""PipeCost test suite — 6 test cases covering all detection engines."""
import pytest
import json
import os
import tempfile
from datetime import datetime, timedelta
from pipecost import (
    Model, QueryRecord, detect_zombies, detect_over_scheduling,
    detect_redundant, analyze, parse_manifest,
)


@pytest.fixture
def pipeline():
    models = [
        Model("stg_orders", "table", "aaa", upstream=[], downstream=["fct_orders"]),
        Model("fct_orders", "table", "bbb", upstream=["stg_orders"], downstream=["rpt_daily"]),
        Model("rpt_daily", "table", "ccc", upstream=["fct_orders"], downstream=[]),
        Model("zombie_report", "table", "zzz", upstream=[], downstream=[]),
    ]
    base = datetime(2024, 1, 15)
    queries = [QueryRecord("stg_orders", 0.5, base), QueryRecord("rpt_daily", 1.0, base)]
    queries += [QueryRecord("fct_orders", 2.0, base + timedelta(hours=i * 8)) for i in range(3)]
    queries += [QueryRecord("zombie_report", 5.0, base + timedelta(hours=i)) for i in range(24)]
    return models, queries


def test_detect_zombies(pipeline):
    models, queries = pipeline
    findings = detect_zombies(models, queries)
    assert len(findings) >= 1
    zombie = next(f for f in findings if f.model == "zombie_report")
    assert zombie.category == "zombie"
    assert zombie.severity == "critical"
    assert zombie.estimated_savings_pct > 50
    non_zombie_names = [f.model for f in findings]
    assert "fct_orders" not in non_zombie_names


def test_detect_over_scheduling(pipeline):
    models, queries = pipeline
    findings = detect_over_scheduling(models, queries)
    assert len(findings) >= 1
    over = findings[0]
    assert over.category == "over_schedule"
    assert over.model == "zombie_report"
    assert over.severity == "critical"
    assert "1.0h" in over.detail


def test_detect_redundant_computation():
    models = [
        Model("revenue_v1", "table", "same_hash_abc"),
        Model("revenue_v2", "table", "same_hash_abc"),
        Model("revenue_v3", "table", "same_hash_abc"),
        Model("costs", "table", "unique_hash"),
    ]
    queries = [
        QueryRecord("revenue_v1", 10.0, datetime(2024, 1, 1)),
        QueryRecord("revenue_v2", 10.0, datetime(2024, 1, 1)),
        QueryRecord("revenue_v3", 10.0, datetime(2024, 1, 1)),
        QueryRecord("costs", 5.0, datetime(2024, 1, 1)),
    ]
    findings = detect_redundant(models, queries)
    assert len(findings) == 1
    f = findings[0]
    assert f.category == "redundant"
    assert "revenue_v1" in f.model and "revenue_v2" in f.model and "revenue_v3" in f.model
    assert f.estimated_savings_pct > 40


def test_analyze_full_pipeline(pipeline):
    models, queries = pipeline
    result = analyze(models, queries)
    assert result["total_credits"] == pytest.approx(127.5, rel=0.01)
    assert result["savings_pct"] <= 75.0
    assert result["savings_pct"] > 0
    assert result["summary"]["zombies"] >= 1
    assert result["summary"]["over_scheduled"] >= 1
    assert len(result["findings"]) >= 2


def test_parse_dbt_manifest():
    manifest = {"nodes": {
        "model.proj.stg_users": {
            "resource_type": "model", "name": "stg_users",
            "config": {"materialized": "view"}, "raw_sql": "SELECT * FROM raw.users",
            "depends_on": {"nodes": []},
        },
        "model.proj.dim_users": {
            "resource_type": "model", "name": "dim_users",
            "config": {"materialized": "table"},
            "raw_sql": "SELECT * FROM {{ ref('stg_users') }}",
            "depends_on": {"nodes": ["model.proj.stg_users"]},
        },
        "test.proj.not_null": {"resource_type": "test", "name": "not_null"},
    }}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(manifest, f)
        path = f.name
    try:
        models = parse_manifest(path)
        assert len(models) == 2
        stg = next(m for m in models if m.name == "stg_users")
        assert "dim_users" in stg.downstream
        assert stg.materialization == "view"
        dim = next(m for m in models if m.name == "dim_users")
        assert "stg_users" in dim.upstream
        assert dim.materialization == "table"
    finally:
        os.unlink(path)


def test_healthy_pipeline_no_false_positives():
    models = [
        Model("src", "incremental", "h1", downstream=["mid"]),
        Model("mid", "table", "h2", upstream=["src"], downstream=["out"]),
        Model("out", "view", "h3", upstream=["mid"], downstream=["dashboard"]),
    ]
    base = datetime(2024, 1, 1)
    queries = [QueryRecord(m.name, 2.0, base + timedelta(days=i)) for i, m in enumerate(models)]
    result = analyze(models, queries)
    assert result["summary"]["zombies"] == 0
    assert result["summary"]["redundant"] == 0
    assert result["summary"]["over_scheduled"] == 0
    assert result["savings_pct"] == 0.0
    assert len(result["findings"]) == 0
