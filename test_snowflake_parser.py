"""Tests for Snowflake QUERY_HISTORY parser and cost attribution engine."""
import pytest
import json
import os
import tempfile
from snowflake_parser import (
    CostAttribution,
    parse_query_history,
    attribute_cost_to_model,
    calculate_monthly_breakdown,
    _extract_model_names_from_manifest,
    _match_query_to_models,
)

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture
def manifest():
    with open(os.path.join(FIXTURE_DIR, "sample_manifest.json")) as f:
        return json.load(f)


@pytest.fixture
def queries_uppercase():
    return parse_query_history(os.path.join(FIXTURE_DIR, "sample_query_history_1.json"))


@pytest.fixture
def queries_lowercase():
    return parse_query_history(os.path.join(FIXTURE_DIR, "sample_query_history_2.json"))


# ---------------------------------------------------------------------------
# Test 1: parse_query_history normalizes uppercase Snowflake keys
# ---------------------------------------------------------------------------
class TestParseQueryHistory:
    def test_uppercase_keys_normalized(self, queries_uppercase):
        assert len(queries_uppercase) == 5
        q0 = queries_uppercase[0]
        assert q0["query_id"] == "01abc-0001"
        assert "stg_orders" in q0["query_text"]
        assert q0["warehouse_name"] == "TRANSFORM_WH"
        assert q0["bytes_scanned"] == 524288000
        assert q0["credits_used"] == 1.5
        assert q0["execution_time"] == 12000
        assert q0["start_time"] == "2024-01-15T08:00:00"

    def test_lowercase_keys_normalized(self, queries_lowercase):
        assert len(queries_lowercase) == 3
        q0 = queries_lowercase[0]
        assert q0["query_id"] == "sf-lc-001"
        assert q0["credits_used"] == 2.0
        assert q0["warehouse_name"] == "ETL_WH"

    def test_rejects_non_array(self, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text('{"not": "an array"}')
        with pytest.raises(ValueError, match="JSON array"):
            parse_query_history(str(bad_file))


# ---------------------------------------------------------------------------
# Test 2: attribute_cost_to_model matches queries to correct models
# ---------------------------------------------------------------------------
class TestAttributeCost:
    def test_single_model_attribution(self, queries_uppercase, manifest):
        attributions = attribute_cost_to_model(queries_uppercase, manifest)
        # Query 01abc-0004 only references dim_customers
        assert "dim_customers" in attributions
        dim_attr = attributions["dim_customers"]
        assert dim_attr.query_count >= 1
        assert dim_attr.total_credits > 0

    def test_multi_model_query_splits_credits(self, manifest):
        """A query referencing two models should split credits evenly."""
        queries = [{
            "query_id": "split-001",
            "query_text": "SELECT * FROM analytics.stg_orders JOIN analytics.dim_customers ON 1=1",
            "warehouse_name": "WH",
            "bytes_scanned": 1000,
            "credits_used": 10.0,
            "execution_time": 5000,
            "start_time": "2024-01-20T00:00:00",
        }]
        attributions = attribute_cost_to_model(queries, manifest)
        # Both models should each get 5.0 credits (half of 10)
        assert "stg_orders" in attributions
        assert "dim_customers" in attributions
        assert abs(attributions["stg_orders"].total_credits - 5.0) < 0.001
        assert abs(attributions["dim_customers"].total_credits - 5.0) < 0.001

    def test_unmatched_query_goes_to_unattributed(self, manifest):
        queries = [{
            "query_id": "nomatch-001",
            "query_text": "SELECT 1 FROM completely_unknown_table",
            "warehouse_name": "WH",
            "bytes_scanned": 100,
            "credits_used": 0.01,
            "execution_time": 100,
            "start_time": "2024-01-20T00:00:00",
        }]
        attributions = attribute_cost_to_model(queries, manifest)
        assert "__unattributed__" in attributions
        assert attributions["__unattributed__"].total_credits == pytest.approx(0.01)

    def test_no_false_match_on_partial_name(self, manifest):
        """stg_orders should NOT match stg_orders_v2."""
        queries = [{
            "query_id": "partial-001",
            "query_text": "SELECT * FROM analytics.stg_orders_v2",
            "warehouse_name": "WH",
            "bytes_scanned": 100,
            "credits_used": 1.0,
            "execution_time": 100,
            "start_time": "2024-01-20T00:00:00",
        }]
        attributions = attribute_cost_to_model(queries, manifest)
        assert "stg_orders" not in attributions
        assert "__unattributed__" in attributions

    def test_warehouses_tracked(self, queries_uppercase, manifest):
        attributions = attribute_cost_to_model(queries_uppercase, manifest)
        stg = attributions.get("stg_orders")
        assert stg is not None
        assert "TRANSFORM_WH" in stg.warehouses


# ---------------------------------------------------------------------------
# Test 3: calculate_monthly_breakdown aggregates correctly
# ---------------------------------------------------------------------------
class TestMonthlyBreakdown:
    def test_single_month(self, manifest):
        queries = [
            {
                "query_id": "m-001",
                "query_text": "SELECT * FROM analytics.stg_orders",
                "warehouse_name": "WH",
                "bytes_scanned": 100,
                "credits_used": 3.0,
                "execution_time": 1000,
                "start_time": "2024-01-10T00:00:00",
            },
            {
                "query_id": "m-002",
                "query_text": "SELECT * FROM analytics.dim_customers",
                "warehouse_name": "WH",
                "bytes_scanned": 100,
                "credits_used": 1.0,
                "execution_time": 500,
                "start_time": "2024-01-12T00:00:00",
            },
        ]
        attributions = attribute_cost_to_model(queries, manifest)
        breakdown = calculate_monthly_breakdown(attributions)

        assert breakdown["total_credits"] == pytest.approx(4.0, abs=0.01)
        assert "2024-01" in breakdown["months"]
        jan = breakdown["months"]["2024-01"]
        assert jan["total_credits"] == pytest.approx(4.0, abs=0.01)
        assert len(jan["top_models"]) == 2
        # stg_orders (3.0) should rank first
        assert jan["top_models"][0]["model"] == "stg_orders"
        assert jan["top_models"][0]["pct"] == pytest.approx(75.0, abs=0.1)

    def test_multi_month(self, queries_uppercase, manifest):
        attributions = attribute_cost_to_model(queries_uppercase, manifest)
        breakdown = calculate_monthly_breakdown(attributions)
        # Fixture 1 has queries in 2024-01 and 2024-02
        assert "2024-01" in breakdown["months"]
        assert "2024-02" in breakdown["months"]
        assert breakdown["total_credits"] > 0

    def test_top_n_limits_output(self, manifest):
        queries = [
            {
                "query_id": f"tn-{i}",
                "query_text": f"SELECT * FROM analytics.stg_orders",
                "warehouse_name": "WH",
                "bytes_scanned": 100,
                "credits_used": 1.0,
                "execution_time": 100,
                "start_time": "2024-06-01T00:00:00",
            }
            for i in range(5)
        ]
        attributions = attribute_cost_to_model(queries, manifest)
        breakdown = calculate_monthly_breakdown(attributions, top_n=1)
        jun = breakdown["months"]["2024-06"]
        assert len(jun["top_models"]) == 1


# ---------------------------------------------------------------------------
# Test 4: CostAttribution.to_dict serialization
# ---------------------------------------------------------------------------
def test_cost_attribution_to_dict():
    attr = CostAttribution(
        model_name="test_model",
        total_credits=12.345678,
        query_count=3,
        total_bytes_scanned=999,
        avg_execution_time=1234.5,
        warehouses=["WH_A", "WH_B", "WH_A"],
    )
    d = attr.to_dict()
    assert d["model_name"] == "test_model"
    assert d["total_credits"] == pytest.approx(12.345678)
    assert d["query_count"] == 3
    # Warehouses should be deduplicated and sorted
    assert d["warehouses"] == ["WH_A", "WH_B"]


# ---------------------------------------------------------------------------
# Test 5: End-to-end with fixture files
# ---------------------------------------------------------------------------
def test_end_to_end_fixture_1(manifest):
    queries = parse_query_history(os.path.join(FIXTURE_DIR, "sample_query_history_1.json"))
    assert len(queries) == 5
    attributions = attribute_cost_to_model(queries, manifest)
    assert len(attributions) >= 2  # at least stg_orders and fct_orders
    breakdown = calculate_monthly_breakdown(attributions)
    assert breakdown["total_credits"] > 0
    assert len(breakdown["months"]) >= 1


def test_end_to_end_fixture_2(manifest):
    queries = parse_query_history(os.path.join(FIXTURE_DIR, "sample_query_history_2.json"))
    assert len(queries) == 3
    attributions = attribute_cost_to_model(queries, manifest)
    # Query sf-lc-002 references unknown table -> __unattributed__
    assert "__unattributed__" in attributions
    breakdown = calculate_monthly_breakdown(attributions)
    assert "2024-03" in breakdown["months"]
