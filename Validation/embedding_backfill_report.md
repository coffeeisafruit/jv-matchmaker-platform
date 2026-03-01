# Embedding Backfill Report

## Summary

Backfilled **4 profiles** that had text content but were missing embedding vectors.

- **Model:** BAAI/bge-large-en-v1.5 (1024-dim)
- **Method:** Local sentence-transformers (HF API 403 → fallback)
- **Total fields embedded:** 11
- **Time elapsed:** 73.2s

## Profiles Updated

| Profile | ID | Missing Embeddings | Previous State |
|---------|----|--------------------|----------------|
| Linkedin Market | `ba97f7a6-00a6-45c4-990b-0c410b80c33b` | seeking | Partial (embeddings_updated_at was set) |
| Stuart Croll | `f7d307c2-1110-442d-bf28-212084690de9` | seeking, offering, who_you_serve, what_you_do | Never embedded |
| Erin Orekar | `1ba31cae-c588-4326-9e8d-ac518891183d` | who_you_serve, what_you_do | Never embedded |
| Susan Sinclair | `fb61d265-1c25-404f-93ae-d58a4e54c8fc` | who_you_serve, what_you_do | Never embedded |

## Result

All 4 profiles now have complete embedding coverage for their populated text fields.
Succeeded: 4, Failed: 0, Skipped: 0.

## Command Used

```bash
python manage.py backfill_embeddings \
  --profile-ids ba97f7a6-00a6-45c4-990b-0c410b80c33b \
                f7d307c2-1110-442d-bf28-212084690de9 \
                1ba31cae-c588-4326-9e8d-ac518891183d \
                fb61d265-1c25-404f-93ae-d58a4e54c8fc \
  --force
```
