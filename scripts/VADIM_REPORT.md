# Vadim Voss — Partner Report

Vadim has **two hand-curated partners** that must always appear at the top of his outreach. They are defined in `scripts/vadim_pinned_partners.json`:

1. **T Harv Eker** (Harv Eker International) — replaced David Riklan  
2. **Steve Essa** (Reliable Trading) — contact: Lisa Gering, Affiliate / JV Manager, lisa@reliabletrading.com

If you regenerate Vadim's report **without** `--pinned-partners`, the DB report (and static outreach export) will lose these two.

## Regenerate Vadim's report (with pinned partners, 10 total matches)

Use **--top 8** so that with 2 pinned partners the report has exactly **10 matches** (2 pinned + 8 algorithmic).

```bash
python manage.py generate_member_report --client-name "Vadim Voss" --month YYYY-MM --pinned-partners scripts/vadim_pinned_partners.json --top 8
```

To update an existing report instead of creating a new one:

```bash
python manage.py generate_member_report --client-name "Vadim Voss" --month YYYY-MM --pinned-partners scripts/vadim_pinned_partners.json --top 8 --update
```

## Re-export static pages after regenerating

```bash
python scripts/export_static_outreach.py --name "Vadim Voss" --output /path/to/vadim-voss-profile/outreach.html
```

Also export profile and hub as needed (see `export_vadim_profile.py` and `export_vadim_hub.py`).
