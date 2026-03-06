# Vast.ai Morning Handoff — 2026-03-07

## What's Ready

### Batch files (local, in `/tmp/vast_jobs.tar.gz` — 11MB)
| Job | Batches | Profiles | Dir |
|-----|---------|----------|-----|
| Seeking inference | 5,377 | 53,763 | `tmp/seeking_batches/` |
| Tier C enrichment | 2,554 | 12,769 | `tmp/tier_c_batches/` |
| Tier D enrichment | 6,411 | 32,053 | `tmp/tier_d_batches/` |
| JSONL remainder | 370 | 1,849 | `tmp/jsonl_batches/` |
| **Total** | **14,712** | **~100K** | |

### Scripts (in `scripts/`)
- `vllm_seeking_inferrer.py` — Seeking inference (reads batch files, calls vLLM, writes results)
- `vllm_batch_enricher.py` — Tier C/D enrichment (already proven on 18K profiles)
- `vast_run_all_jobs.sh` — Master runner (runs all 4 jobs sequentially on the instance)
- `vast_auto_shutdown.sh` — Local monitor (polls for sentinel, auto-destroys instance)

## GPU Requirements

**Qwen3-30B-A3B needs 80GB+ VRAM** in BF16. Tested and failed on:
- RTX 4090 (24GB) — OOM
- A40 (48GB) — OOM (model weights = 43.58GB, no room for KV cache)
- RTX PRO 6000 Blackwell — sm_120 not supported by PyTorch/vLLM v0.8.5

**What works:**
- H100 80GB SXM (~$1.30/hr) — proven in prior session
- A100 80GB SXM — should work but untested (stuck loading last night)

**Vast.ai search filter:**
```
gpu_ram>=80 disk_space>=80 inet_down>=500 dph_total<1.50 cuda_vers>=12.0
```

## Deployment Steps

```bash
# 1. Rent instance (H100 80GB preferred)
vastai search offers 'gpu_ram>=80 disk_space>=80 inet_down>=500 dph_total<1.50 cuda_vers>=12.0' | head -5
vastai create instance <OFFER_ID> --image vllm/vllm-openai:v0.8.5 \
  --env '-p 8000:8000' \
  --args '--model Qwen/Qwen3-30B-A3B --max-model-len 16384 --dtype bfloat16 --trust-remote-code' \
  --disk 80

# 2. Wait for running status
vastai show instances

# 3. Get SSH info
SSH_URL=$(vastai ssh-url <INSTANCE_ID>)
# Parse host and port from output

# 4. Upload job files
scp -i ~/.ssh/vastai_key -P <PORT> /tmp/vast_jobs.tar.gz root@<HOST>:/root/
ssh -i ~/.ssh/vastai_key -p <PORT> root@<HOST> 'cd /root && tar xzf vast_jobs.tar.gz'

# 5. Start jobs (in tmux on instance)
ssh -i ~/.ssh/vastai_key -p <PORT> root@<HOST>
tmux new -s jobs
pip install requests
bash /root/jobs/vast_run_all_jobs.sh

# 6. Start auto-shutdown locally
bash scripts/vast_auto_shutdown.sh <INSTANCE_ID> <HOST> <PORT> &
```

## After Jobs Complete

```bash
# Download results
scp -i ~/.ssh/vastai_key -P <PORT> -r root@<HOST>:/root/results/ ./vast_results/

# Push seeking results to DB
python3 scripts/infer_seeking_field.py push --results-dir vast_results/seeking_results/

# Push Tier C/D enrichment results to DB
for f in vast_results/tier_c_results/*.json; do cat "$f" | python3 scripts/enrich_tier_b.py update; done
for f in vast_results/tier_d_results/*.json; do cat "$f" | python3 scripts/enrich_tier_b.py update; done

# Re-score everything
python3 manage.py score_new_enrichments --tier B --since 2026-03-05
python3 manage.py score_new_enrichments --tier C
python3 manage.py score_new_enrichments --tier D
```

## Other Pending Work (not Vast.ai)

1. **Import scraper CSVs to DB** — 3,345 new contacts in `tmp/`:
   - clickbank_run.csv (887), gumroad_run.csv (948), substack_run.csv (924)
   - warriorplus_run.csv (536), jvzoo_run.csv (50), clarity_fm_direct.csv (25)
   - tedx_direct.csv (24), indie_hackers_direct.csv (100)
   - Blocked last night by Supabase connection pool saturation (subscribe_contacts)

2. **Full affiliate page discovery** — Only scanned 2K of 50K enriched profiles
   - `python3 scripts/discover_affiliate_pages.py` (needs DB access)

3. **Full re-score** — Only 5K of 90K+ enriched profiles scored
   - `python3 manage.py score_new_enrichments --tier B --since 2026-03-05`

4. **Existing H200 instance** (contract 32434729, $2.65/hr) — running enrichment from another context
   - Check if finished: `vastai show instance 32434729`
   - If done, destroy it to stop billing

## Estimated Costs
- H100 80GB for ~100K profiles: ~$5-8 (4-6 hours at $1.30/hr)
- Total batch: seeking (5.4K batches × 15 workers) + enrichment (9.3K batches × 15 workers)
