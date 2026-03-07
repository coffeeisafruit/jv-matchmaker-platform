# Prefect Operations Runbook

## Architecture

- **Prefect Cloud** hosts schedules, run history, and monitoring
- **Railway worker** executes flows (Procfile `worker:` process)
- **Railway web** serves Django app (Procfile `web:` process)
- Both services share the same env vars and codebase

## Quick Reference

### Check system health
```bash
# Verify Prefect Cloud connection
prefect cloud workspace ls

# List deployed flows and next run times
prefect deployment ls

# Check worker status
prefect worker ls
```

### Manually trigger a flow
```bash
# Via Prefect CLI
prefect deployment run 'monthly-orchestrator/monthly-orchestrator' --param phase=delivery --param dry_run=true

# Via Python
python3 -m matching.enrichment.flows.monthly_orchestrator --phase delivery --dry-run

# Via Prefect Cloud UI
# Navigate to Deployments > select flow > Run > set parameters
```

### Monthly cycle calendar
| When | Flow | Purpose |
|------|------|---------|
| 1st, 6 AM ET | monthly-orchestrator | Top-level coordinator |
| 1st, 8 AM ET | report-delivery | Deliver reports with new access codes |
| Week 1 Mon, 2 AM ET | change-detection | Profile freshness check |
| Week 3 Mon, 9 AM ET | client-verification | Verification emails |
| Week 4 Mon, 2 AM ET | monthly-processing | Full processing pipeline |
| Week 4 Tue, 9 AM ET | admin-notification | Admin report with AI suggestions |

### Deploy flows after code changes
```bash
# Validate entrypoints first
python3 manage.py deploy_flows --dry-run

# Deploy all flows to Prefect Cloud
prefect deploy --all

# Or deploy a single flow
prefect deploy -n monthly-orchestrator
```

## Common Issues

### Zombie runs (stuck in RUNNING)
Flows stuck in RUNNING for hours typically mean the worker process crashed.
```bash
# Check for zombies
prefect flow-run ls --state RUNNING

# Cancel a specific run
prefect flow-run cancel <run-id>

# Bulk cleanup (local SQLite only)
python3 scripts/cleanup_zombie_runs.py
```

### Worker not picking up runs
1. Check Railway worker service logs
2. Verify `PREFECT_API_URL` and `PREFECT_API_KEY` env vars are set
3. Verify work pool exists: `prefect work-pool ls`
4. Restart the worker service in Railway dashboard

### Email delivery failures
1. Check Resend dashboard for delivery status
2. Verify `EMAIL_HOST_PASSWORD` (Resend API key) is set in Railway
3. Test locally:
```bash
python3 -c "
import os, django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()
from django.core.mail import send_mail
send_mail('Test', 'Body', None, ['your@email.com'])
"
```

### Flow timeout
Flows with `timeout_seconds` will be cancelled automatically. Check:
- `monthly-orchestrator`: 4h (14400s)
- `enrichment-pipeline`: 1h (3600s)
- `enrichment-cascade`: 2h (7200s)
- `candidate-acquisition`: 30m (1800s)
- `client-verification`: 1h (3600s)
- `admin-notification`: 30m (1800s)
- `report-delivery`: 30m (1800s)

## Environment Variables (Prefect-specific)

| Variable | Required | Purpose |
|----------|----------|---------|
| `PREFECT_API_URL` | Yes | Prefect Cloud workspace URL |
| `PREFECT_API_KEY` | Yes | Prefect Cloud API key |
| `EMAIL_BACKEND` | Yes | `django.core.mail.backends.smtp.EmailBackend` |
| `EMAIL_HOST` | Yes | `smtp.resend.com` |
| `EMAIL_PORT` | Yes | `587` |
| `EMAIL_HOST_USER` | Yes | `resend` |
| `EMAIL_HOST_PASSWORD` | Yes | Resend API key |
| `ADMIN_EMAIL` | Yes | Admin notification recipient |
| `ALERT_EMAIL` | Optional | Critical alert recipient |
| `SLACK_WEBHOOK_URL` | Optional | Slack alerting webhook |

## Adding a New Flow

1. Create the flow file in `matching/enrichment/flows/`
2. Add `@flow(name="...", timeout_seconds=...)` decorator
3. Add deployment to `prefect.yaml` with schedule and `work_pool: name: railway-pool`
4. If flow uses Django models/email: add Django bootstrap at top of file
5. Run `python3 manage.py deploy_flows --dry-run` to validate
6. Commit, push, then run `prefect deploy --all`
