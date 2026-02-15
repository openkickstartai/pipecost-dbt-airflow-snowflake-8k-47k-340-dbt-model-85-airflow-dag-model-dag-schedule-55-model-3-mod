# PipeCost — Data Warehouse Cost Attribution & Optimization Engine

Your Snowflake bill went from $8K to $47K. Which of your 340 dbt models is burning money?

PipeCost scans your dbt manifest + query history to **attribute every dollar** to specific models, detect waste, and generate actionable savings.

## 🚀 Quick Start

```bash
pip install -r requirements.txt

# Analyze your dbt project
python cli.py scan manifest.json query_history.json

# JSON output for CI/CD integration
python cli.py scan manifest.json query_history.json --format json

# Pro mode (unlimited models + recommendations)
PIPECOST_PRO_KEY=your-key python cli.py scan manifest.json query_history.json
```

### Input: `query_history.json`
```json
[
  {"model_name": "fct_orders", "credits_used": 5.2, "start_time": "2024-01-15T08:00:00", "warehouse": "TRANSFORM_WH"}
]
```

## 🔍 What It Detects

| Issue | Example | Typical Savings |
|-------|---------|----------------|
| 🧟 **Zombie Models** | 55% compute, zero downstream consumers | 20-40% |
| ⏰ **Over-Scheduling** | Hourly refresh, source updates daily | 10-25% |
| 🔁 **Redundant Compute** | 3 models compute same JOIN independently | 5-15% |

## 💰 Pricing

| Feature | Free | Pro $99/mo | Enterprise $599/mo |
|---------|------|-----------|-------------------|
| Models analyzed | ≤ 50 | Unlimited | Unlimited |
| Zombie detection | ✅ | ✅ | ✅ |
| Over-schedule detection | ✅ | ✅ | ✅ |
| Redundant compute detection | ✅ | ✅ | ✅ |
| Actionable recommendations | ❌ | ✅ | ✅ |
| JSON/CSV export | ✅ | ✅ | ✅ |
| Slack alerts | ❌ | ✅ | ✅ |
| PDF cost reports | ❌ | ✅ | ✅ |
| Multi-warehouse support | ❌ | ❌ | ✅ |
| SSO/SAML | ❌ | ❌ | ✅ |
| Snowflake live connector | ❌ | ✅ | ✅ |
| Trend analysis & forecasting | ❌ | ✅ | ✅ |
| GitHub Action PR comments | ❌ | ✅ | ✅ |
| Audit trail & SOC2 | ❌ | ❌ | ✅ |

## 📊 Why Pay?

A $47K/month Snowflake bill with **30% waste** = **$14K/month wasted**. PipeCost Pro at $99/month delivers **141x ROI** on day one. Enterprise teams with $200K+ monthly bills save $60K+ per month — $599 is a rounding error.

## 🧪 Testing

```bash
pytest test_pipecost.py -v
```

## License

BSL 1.1 — Free for teams ≤ 50 models. Commercial license required for larger deployments.
