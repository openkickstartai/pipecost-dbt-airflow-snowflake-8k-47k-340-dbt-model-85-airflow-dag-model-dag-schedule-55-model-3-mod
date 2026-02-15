"""PipeCost CLI — Rich terminal interface for warehouse cost analysis."""
import click
import json
from datetime import datetime
from rich.console import Console
from rich.table import Table
from pipecost import parse_manifest, analyze, QueryRecord

console = Console()
FREE_MODEL_LIMIT = 50


def load_queries(path):
    with open(path) as f:
        data = json.load(f)
    return [QueryRecord(
        model_name=r["model_name"], credits_used=r["credits_used"],
        start_time=datetime.fromisoformat(r["start_time"]),
        warehouse=r.get("warehouse", "default"),
    ) for r in data]


@click.group()
@click.version_option("0.1.0")
def cli():
    """PipeCost — Data warehouse cost attribution & optimization."""


@cli.command()
@click.argument("manifest", type=click.Path(exists=True))
@click.argument("queries", type=click.Path(exists=True))
@click.option("--format", "fmt", type=click.Choice(["table", "json"]), default="table")
@click.option("--pro-key", envvar="PIPECOST_PRO_KEY", default=None)
def scan(manifest, queries, fmt, pro_key):
    """Analyze dbt manifest + query history for cost savings."""
    models = parse_manifest(manifest)
    if not pro_key and len(models) > FREE_MODEL_LIMIT:
        console.print(f"[yellow]⚠ Free tier: first {FREE_MODEL_LIMIT}/{len(models)} models."
                      f" Set PIPECOST_PRO_KEY for unlimited.[/yellow]")
        models = models[:FREE_MODEL_LIMIT]
    query_records = load_queries(queries)
    result = analyze(models, query_records)
    if fmt == "json":
        out = {**result, "findings": [{"category": f.category, "severity": f.severity,
            "model": f.model, "detail": f.detail, "savings_pct": f.estimated_savings_pct,
            "recommendation": f.recommendation if pro_key else "Upgrade to Pro"}
            for f in result["findings"]]}
        click.echo(json.dumps(out, indent=2, default=str))
        return
    console.print("\n[bold cyan]━━━ PipeCost Report ━━━[/bold cyan]")
    console.print(f"Total credits: [bold]{result['total_credits']:.1f}[/bold]")
    console.print(f"Potential savings: [bold green]{result['savings_pct']:.1f}%[/bold green]\n")
    s = result["summary"]
    console.print(f"🧟 Zombies: {s['zombies']}  ⏰ Over-scheduled: "
                  f"{s['over_scheduled']}  🔁 Redundant: {s['redundant']}\n")
    if not result["findings"]:
        console.print("[green]✅ No significant waste detected. Your pipelines are efficient![/green]")
        return
    table = Table(title="Findings", show_lines=True)
    for col, w in [("Sev", 3), ("Type", 13), ("Model", 24), ("Detail", 50), ("Save%", 6)]:
        table.add_column(col, width=w)
    for f in result["findings"]:
        sev = "🔴" if f.severity == "critical" else "🟡"
        table.add_row(sev, f.category, f.model, f.detail, f"{f.estimated_savings_pct:.1f}%")
    console.print(table)
    if pro_key:
        console.print("\n[bold]Recommendations:[/bold]")
        for f in result["findings"]:
            console.print(f"  → [green]{f.recommendation}[/green]")
    else:
        console.print("\n[dim]💡 Set PIPECOST_PRO_KEY to unlock actionable recommendations.[/dim]")


if __name__ == "__main__":
    cli()
