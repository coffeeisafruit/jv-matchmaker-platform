#!/bin/bash
# Quick test of Apollo.io enrichment (dry run, no credits used)

echo "=================================================="
echo "APOLLO.IO ENRICHMENT DRY RUN TEST"
echo "=================================================="
echo ""
echo "This will show you what would be enriched"
echo "NO API calls will be made, NO credits used"
echo ""
echo "Testing with top 20 from Batch 3..."
echo ""

python3 scripts/enrich_with_apollo.py \
  --api-key "test-key-will-not-be-used-in-dry-run" \
  --batch enrichment_batches/batch3_has_company.csv \
  --limit 20 \
  --dry-run

echo ""
echo "=================================================="
echo "To run for real, get your API key from:"
echo "https://app.apollo.io/#/settings/integrations/api"
echo ""
echo "Then run:"
echo "python3 scripts/enrich_with_apollo.py \\"
echo "  --api-key YOUR_KEY \\"
echo "  --batch enrichment_batches/batch3_has_company.csv \\"
echo "  --limit 20 \\"
echo "  --output enriched_top20.csv"
echo "=================================================="
