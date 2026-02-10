# How to Run OWL - Complete Guide

## Prerequisites

**Activate your virtual environment first:**
```bash
cd /Users/josephtepe/Projects/jv-matchmaker-platform

# Activate venv (choose one)
source venv/bin/activate  # or
source .venv/bin/activate
```

## Quick Start: Test a Single Profile

### Option 1: Simple Standalone Script (Easiest - Recommended)

```bash
# Make sure you're in the project directory
cd /Users/josephtepe/Projects/jv-matchmaker-platform

# Activate venv
source venv/bin/activate  # or source .venv/bin/activate

# Run the simple test script
python test_owl_simple.py
```

This will test with "Janet Bray Attwood" as an example. You can edit `test_owl_simple.py` to change the profile.

### Option 2: Run the Built-in Test

```bash
# Activate venv first
source venv/bin/activate

# Run the built-in test
python matching/enrichment/owl_research/agents/owl_agent.py
```

This will test with "Janet Bray Attwood" from "The Passion Test" as an example.

### Option 3: Python Script (Custom Profile)

Create a test file `test_owl.py`:

```python
import json
import os
import sys
import django

# Setup Django
sys.path.insert(0, '/Users/josephtepe/Projects/jv-matchmaker-platform')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from matching.enrichment.owl_research.agents.owl_agent import OWLEnrichmentAgent

# Create agent
agent = OWLEnrichmentAgent(timeout=120)

# Research a profile
result = agent.enrich_profile(
    name="Your Person's Name",
    company="Their Company",
    website="https://example.com",
    linkedin="https://linkedin.com/in/example"  # Optional
)

print(json.dumps(result, indent=2))
```

Run it:
```bash
python test_owl.py
```

### Option 4: Django Shell (Interactive)

```bash
cd /Users/josephtepe/Projects/jv-matchmaker-platform
python manage.py shell
```

Then in the shell:
```python
import json
from matching.enrichment.owl_research.agents.owl_agent import OWLEnrichmentAgent

agent = OWLEnrichmentAgent(timeout=120)
result = agent.enrich_profile(
    name="Test Person",
    company="Test Company",
    website="https://example.com"
)

print(json.dumps(result, indent=2))
```

---

## Batch Processing: Multiple Profiles

**Remember to activate your virtual environment first:**
```bash
source venv/bin/activate  # or source .venv/bin/activate
```

### Option 1: From CSV File

Create a CSV file `profiles.csv`:
```csv
name,company,website,linkedin
John Doe,Acme Corp,https://acme.com,https://linkedin.com/in/johndoe
Jane Smith,Tech Inc,https://techinc.com,
```

Then run:
```bash
python manage.py owl_enrich_batch --input profiles.csv --max 10
```

**Options:**
- `--max 10` - Process only first 10 profiles (for testing)
- `--output custom_output` - Custom output directory
- `--delay 2.0` - Seconds between profiles (rate limiting)
- `--workers 3` - Parallel processing (faster, but uses more resources)
- `--no-resume` - Start fresh (default: resumes from checkpoint)

**Example:**
```bash
# Process 5 profiles with 2 second delays
python manage.py owl_enrich_batch --input profiles.csv --max 5 --delay 2.0

# Process all profiles with parallel workers
python manage.py owl_enrich_batch --input profiles.csv --workers 3
```

### Option 2: From Supabase Database (Recommended)

```bash
# Process profiles from Supabase (only sparse ones)
python manage.py owl_enrich_batch --from-supabase --max 10

# Process and save back to Supabase
python manage.py owl_enrich_batch --from-supabase --save-to-supabase --max 50

# Process only profiles with websites (better research quality)
python manage.py owl_enrich_batch --from-supabase --require-website --max 20

# Parallel processing (faster)
python manage.py owl_enrich_batch --from-supabase --workers 3 --max 100
```

**Options:**
- `--from-supabase` - Load from Supabase instead of CSV
- `--filter-sparse` - Only process profiles missing key fields (default: True)
- `--no-filter` - Process all profiles
- `--require-website` - Only profiles with websites
- `--save-to-supabase` - Write enriched data back to database
- `--max N` - Limit number of profiles
- `--workers N` - Parallel workers

---

## Advanced Usage

### Using the Enrichment Service (More Features)

```python
import asyncio
from matching.enrichment.owl_research.agents.owl_enrichment_service import enrich_profile_with_owl_sync

# Synchronous wrapper
result = enrich_profile_with_owl_sync(
    name="John Doe",
    email="john@example.com",
    company="Acme Corp",
    website="https://acme.com",
    linkedin="https://linkedin.com/in/johndoe",
    existing_data={}  # Optional: existing profile data
)

print(result)
```

### Using Async Service

```python
import asyncio
from matching.enrichment.owl_research.agents.owl_enrichment_service import enrich_profile_with_owl

async def main():
    result = await enrich_profile_with_owl(
        name="John Doe",
        company="Acme Corp",
        website="https://acme.com"
    )
    print(result)

asyncio.run(main())
```

---

## Output Format

OWL returns a JSON object with this structure:

```json
{
  "name": "Person Name",
  "enriched_data": {
    "full_name": "Full Name",
    "title": "Their Title",
    "company_name": "Company Name",
    "company_description": "What they do...",
    "offerings": ["Product 1", "Product 2"],
    "signature_programs": ["Program Name"],
    "who_they_serve": "Target audience description",
    "seeking": "Partnership goals",
    "linkedin_url": "https://linkedin.com/...",
    "sources": ["https://source1.com", "https://source2.com"]
  },
  "success": true,
  "error": null
}
```

---

## Troubleshooting

### Error: "No API key found"
- Check that `OPENROUTER_API_KEY` is in your `.env` file
- Verify: `cat .env | grep OPENROUTER`

### Error: "OWL_VENV not found"
- The OWL framework needs its own virtual environment
- Check: `ls owl_framework/.venv/bin/python`
- If missing, you may need to set up the OWL framework first

### Error: "Timeout"
- Increase timeout: `OWLEnrichmentAgent(timeout=300)`  # 5 minutes
- Some profiles take longer to research

### Slow Processing
- Use parallel workers: `--workers 3`
- Reduce delay: `--delay 1.0` (but watch rate limits)

---

## Examples

### Example 1: Quick Test
```bash
python matching/enrichment/owl_research/agents/owl_agent.py
```

### Example 2: Process 10 Profiles from CSV
```bash
python manage.py owl_enrich_batch --input contacts.csv --max 10
```

### Example 3: Process All Sparse Profiles from Supabase
```bash
python manage.py owl_enrich_batch --from-supabase --save-to-supabase
```

### Example 4: Fast Parallel Processing
```bash
python manage.py owl_enrich_batch --from-supabase --workers 5 --max 100 --delay 1.0
```

---

## Next Steps

1. ✅ Test with a single profile
2. ✅ Process a small batch (5-10 profiles)
3. ✅ Scale up to larger batches
4. ✅ Save results back to Supabase

Happy researching! 🦉
