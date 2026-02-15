"""Snowflake QUERY_HISTORY parser and cost attribution engine for PipeCost.

Parses exported Snowflake query history (JSON), attributes credits to dbt models
by matching SQL text against model/table names from a dbt manifest, and produces
monthly cost breakdowns ranked by spend.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict
import json
import re


@dataclass
class CostAttribution:
    """Aggregated cost attribution for a single dbt model."""
    model_name: str
    total_credits: float = 0.0
    query_count: int = 0
    total_bytes_scanned: int = 0
    avg_execution_time: float = 0.0
    warehouses: List[str] = field(default_factory=list)
    queries: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "model_name": self.model_name,
            "total_credits": round(self.total_credits, 6),
            "query_count": self.query_count,
            "total_bytes_scanned": self.total_bytes_scanned,
            "avg_execution_time": round(self.avg_execution_time, 2),
            "warehouses": sorted(set(self.warehouses)),
        }


def parse_query_history(file_path: str) -> List[dict]:
    """Read exported Snowflake QUERY_HISTORY JSON and normalize to standard keys.

    Handles both uppercase Snowflake-native column names (QUERY_ID, QUERY_TEXT, ...)
    and lowercase variants from third-party export tools.

    Returns a list of dicts with keys:
        query_id, query_text, warehouse_name, bytes_scanned,
        credits_used, execution_time, start_time
    """
    with open(file_path) as f:
        raw = json.load(f)

    if not isinstance(raw, list):
        raise ValueError("Expected a JSON array of query records")

    results: List[dict] = []
    for row in raw:
        results.append({
            "query_id": str(row.get("QUERY_ID", row.get("query_id", ""))),
            "query_text": str(row.get("QUERY_TEXT", row.get("query_text", ""))),
            "warehouse_name": str(row.get("WAREHOUSE_NAME", row.get("warehouse_name", ""))),
            "bytes_scanned": int(row.get("BYTES_SCANNED", row.get("bytes_scanned", 0))),
            "credits_used": float(row.get("CREDITS_USED", row.get("credits_used", 0.0))),
            "execution_time": int(row.get("EXECUTION_TIME", row.get("execution_time", 0))),
            "start_time": str(row.get("START_TIME", row.get("start_time", ""))),
        })
    return results


def _extract_model_names_from_manifest(dbt_manifest: dict) -> Dict[str, str]:
    """Build a lookup from lowercased table/alias patterns to canonical model names."""
    model_map: Dict[str, str] = {}
    for key, node in dbt_manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        name = node["name"]
        model_map[name.lower()] = name
        alias = node.get("alias")
        if alias and alias.lower() != name.lower():
            model_map[alias.lower()] = name
        schema = node.get("schema", "")
        if schema:
            model_map[f"{schema.lower()}.{name.lower()}"] = name
            if alias and alias.lower() != name.lower():
                model_map[f"{schema.lower()}.{alias.lower()}"] = name
    return model_map


def _match_query_to_models(
    query_text: str, model_map: Dict[str, str]
) -> List[str]:
    """Return canonical model names referenced in *query_text*.

    Matches are word-boundary-aware so ``stg_orders`` does not false-match
    ``stg_orders_v2``.
    """
    matched: set = set()
    query_lower = query_text.lower()
    # Check longest patterns first so schema.table wins over bare table
    for pattern, model_name in sorted(model_map.items(), key=lambda x: -len(x[0])):
        escaped = re.escape(pattern)
        if re.search(r"\b" + escaped + r"\b", query_lower):
            matched.add(model_name)
    return sorted(matched)


def attribute_cost_to_model(
    queries: List[dict], dbt_manifest: dict
) -> Dict[str, CostAttribution]:
    """Attribute each query's credits to the dbt models it references.

    * When a query references multiple models the credits are split evenly.
    * Queries that match no model are attributed to ``__unattributed__``.
    """
    model_map = _extract_model_names_from_manifest(dbt_manifest)
    attributions: Dict[str, CostAttribution] = {}
    # Collect execution times per model for averaging
    exec_times: Dict[str, List[int]] = defaultdict(list)

    for q in queries:
        matched_models = _match_query_to_models(
            q.get("query_text", ""), model_map
        )
        if not matched_models:
            matched_models = ["__unattributed__"]

        share = 1.0 / len(matched_models)
        credits = q.get("credits_used", 0.0)
        bytes_scanned = q.get("bytes_scanned", 0)
        exec_time = q.get("execution_time", 0)
        warehouse = q.get("warehouse_name", "")

        for model_name in matched_models:
            if model_name not in attributions:
                attributions[model_name] = CostAttribution(model_name=model_name)

            attr = attributions[model_name]
            attr.total_credits += credits * share
            attr.query_count += 1
            attr.total_bytes_scanned += int(bytes_scanned * share)
            attr.queries.append({
                "query_id": q.get("query_id", ""),
                "credits_share": round(credits * share, 6),
                "start_time": q.get("start_time", ""),
            })
            if warehouse:
                attr.warehouses.append(warehouse)
            exec_times[model_name].append(exec_time)

    for model_name, times in exec_times.items():
        if model_name in attributions and times:
            attributions[model_name].avg_execution_time = sum(times) / len(times)

    return attributions


def calculate_monthly_breakdown(
    attributions: Dict[str, CostAttribution], top_n: int = 10
) -> dict:
    """Aggregate attributed costs by calendar month.

    Returns::

        {
          "total_credits": 42.5,
          "months": {
            "2024-01": {
              "total_credits": 42.5,
              "top_models": [
                {"model": "fct_orders", "credits": 20.0, "pct": 47.06},
                ...
              ]
            }
          }
        }
    """
    monthly: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    total_credits = 0.0

    for model_name, attr in attributions.items():
        for q in attr.queries:
            start = q.get("start_time", "")
            if not start:
                month_key = "unknown"
            else:
                try:
                    dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                    month_key = dt.strftime("%Y-%m")
                except (ValueError, TypeError):
                    month_key = "unknown"
            monthly[month_key][model_name] += q.get("credits_share", 0.0)
        total_credits += attr.total_credits

    result: dict = {
        "total_credits": round(total_credits, 4),
        "months": {},
    }

    for month_key in sorted(monthly.keys()):
        models_in_month = monthly[month_key]
        month_total = sum(models_in_month.values())
        ranked = sorted(models_in_month.items(), key=lambda x: -x[1])[:top_n]
        result["months"][month_key] = {
            "total_credits": round(month_total, 4),
            "top_models": [
                {
                    "model": name,
                    "credits": round(cred, 4),
                    "pct": round((cred / month_total * 100) if month_total > 0 else 0, 2),
                }
                for name, cred in ranked
            ],
        }

    return result
