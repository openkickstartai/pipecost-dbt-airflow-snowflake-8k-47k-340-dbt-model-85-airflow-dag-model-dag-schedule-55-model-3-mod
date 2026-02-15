"""PipeCost — Core analysis engine for dbt/warehouse cost attribution."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict
from collections import defaultdict
import json
import hashlib


@dataclass
class Model:
    name: str
    materialization: str = "view"
    sql_hash: str = ""
    upstream: list = field(default_factory=list)
    downstream: list = field(default_factory=list)


@dataclass
class QueryRecord:
    model_name: str
    credits_used: float
    start_time: datetime
    warehouse: str = "default"


@dataclass
class Finding:
    category: str
    severity: str
    model: str
    detail: str
    estimated_savings_pct: float
    recommendation: str


def parse_manifest(path: str) -> List[Model]:
    with open(path) as f:
        data = json.load(f)
    models = []
    for key, node in data.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        raw = node.get("raw_sql", node.get("raw_code", ""))
        models.append(Model(
            name=node["name"],
            materialization=node.get("config", {}).get("materialized", "view"),
            sql_hash=hashlib.md5(raw.encode()).hexdigest(),
            upstream=[d.split(".")[-1] for d in node.get("depends_on", {}).get("nodes", [])],
        ))
    name_map = {m.name: m for m in models}
    for m in models:
        for up in m.upstream:
            if up in name_map:
                name_map[up].downstream.append(m.name)
    return models


def _cost_map(queries):
    cm = defaultdict(float)
    for q in queries:
        cm[q.model_name] += q.credits_used
    return cm, sum(cm.values()) or 1.0


def detect_zombies(models, queries):
    cm, total = _cost_map(queries)
    findings = []
    for m in models:
        if m.downstream or cm.get(m.name, 0) <= 0:
            continue
        pct = (cm[m.name] / total) * 100
        if pct >= 1.0:
            sev = "critical" if pct >= 5 else "warning"
            findings.append(Finding("zombie", sev, m.name,
                f"Costs {pct:.1f}% ({cm[m.name]:.1f} credits), zero downstream",
                round(pct, 1), f"Archive '{m.name}' to save ~{pct:.1f}%"))
    return sorted(findings, key=lambda f: -f.estimated_savings_pct)


def detect_over_scheduling(models, queries):
    groups = defaultdict(list)
    for q in queries:
        groups[q.model_name].append(q.start_time)
    cm, total = _cost_map(queries)
    findings = []
    for name, times in groups.items():
        if len(times) < 3:
            continue
        ts = sorted(times)
        intervals_h = [(ts[i + 1] - ts[i]).total_seconds() / 3600 for i in range(len(ts) - 1)]
        avg_h = sum(intervals_h) / len(intervals_h)
        if avg_h <= 4:
            runs_day = 24 / max(avg_h, 0.1)
            sav = cm.get(name, 0) * 0.75 / total * 100
            sev = "critical" if avg_h <= 1 else "warning"
            findings.append(Finding("over_schedule", sev, name,
                f"Every {avg_h:.1f}h ({runs_day:.0f}x/day), {cm.get(name, 0):.1f} credits",
                round(sav, 1), f"Reduce to every {max(avg_h * 4, 6):.0f}h + incremental"))
    return sorted(findings, key=lambda f: -f.estimated_savings_pct)


def detect_redundant(models, queries):
    groups = defaultdict(list)
    for m in models:
        if m.sql_hash:
            groups[m.sql_hash].append(m.name)
    cm, total = _cost_map(queries)
    findings = []
    for h, names in groups.items():
        if len(names) < 2:
            continue
        cost = sum(cm.get(n, 0) for n in names)
        sav = cost * (len(names) - 1) / len(names) / total * 100
        findings.append(Finding("redundant", "critical" if sav > 5 else "warning",
            ", ".join(names), f"{len(names)} models, identical SQL, {cost:.1f} credits",
            round(sav, 1), f"Consolidate into '{names[0]}'"))
    return sorted(findings, key=lambda f: -f.estimated_savings_pct)


def analyze(models, queries):
    z = detect_zombies(models, queries)
    o = detect_over_scheduling(models, queries)
    r = detect_redundant(models, queries)
    all_f = z + o + r
    total_sav = min(sum(f.estimated_savings_pct for f in all_f), 75.0)
    return {"total_credits": sum(q.credits_used for q in queries), "findings": all_f,
        "savings_pct": round(total_sav, 1),
        "summary": {"zombies": len(z), "over_scheduled": len(o), "redundant": len(r)}}
