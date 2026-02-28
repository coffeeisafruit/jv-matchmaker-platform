"""
OWL Batch Profile Enrichment Processor

Processes profiles in batches using OWL for deep research.
Features:
- Resume capability (saves progress to checkpoint file)
- Rich data extraction (programs, seeking, who they serve)
- Source verification for all data
- Rate limiting to avoid search blocks
"""

import asyncio
import csv
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from asgiref.sync import sync_to_async

from matching.enrichment.owl_research.agents.owl_enrichment_service import OWLEnrichmentService
from matching.enrichment.owl_research.schemas.profile_schema import BatchProgress

logger = logging.getLogger(__name__)


@dataclass
class BatchCheckpoint:
    """Checkpoint for resumable batch processing."""
    last_processed_index: int
    total_profiles: int
    completed: int
    failed: int
    skipped: int
    started_at: str
    last_updated: str
    failed_profiles: List[Dict]
    avg_verified_fields: float = 0.0
    total_sources_found: int = 0


class OWLBatchProcessor:
    """
    Processes large batches of profiles using OWL enrichment.

    Features:
    - Checkpointing: Saves progress every N profiles
    - Resume: Can restart from last checkpoint
    - Rate limiting: Configurable delays between profiles
    - Rich output: CSV with all enriched fields + sources
    """

    def __init__(
        self,
        output_dir: str = "output",
        checkpoint_file: str = "owl_checkpoint.json",
        delay_between_profiles: float = 2.0,  # Seconds between profiles
        save_interval: int = 5,  # Save checkpoint every N profiles
        save_to_supabase: bool = False,  # Write enriched data back to Supabase
        workers: int = 1,  # Number of concurrent workers
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.checkpoint_file = self.output_dir / checkpoint_file
        self.results_file = self.output_dir / "owl_enriched_profiles.csv"
        self.errors_file = self.output_dir / "owl_failed_profiles.json"

        self.delay_between_profiles = delay_between_profiles
        self.save_interval = save_interval
        self.save_to_supabase = save_to_supabase
        self.workers = max(1, workers)

        self.service = OWLEnrichmentService()
        self.checkpoint = self._load_checkpoint()

        # Thread-safe locks for parallel processing
        self._checkpoint_lock = asyncio.Lock()
        self._csv_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()

        # Session tracking
        self.session_stats = {
            "processed": 0,
            "enriched": 0,
            "failed": 0,
            "skipped": 0,
            "saved_to_supabase": 0,
            "total_verified_fields": 0,
        }

    def _load_checkpoint(self) -> Optional[BatchCheckpoint]:
        """Load checkpoint from file if exists."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    data = json.load(f)
                    return BatchCheckpoint(**data)
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        return None

    def _save_checkpoint(self, checkpoint: BatchCheckpoint):
        """Save checkpoint to file."""
        checkpoint.last_updated = datetime.now().isoformat()
        with open(self.checkpoint_file, "w") as f:
            json.dump(asdict(checkpoint), f, indent=2)

    def _append_result(self, profile: Dict, enriched_data: Dict):
        """Append enriched result to CSV."""
        file_exists = self.results_file.exists()

        # Merge original profile with enriched data
        result = {**profile}

        # Add enriched fields (skip internal fields)
        for key, value in enriched_data.items():
            if not key.startswith("_"):
                result[f"owl_{key}"] = value

        # Add metadata
        result["owl_confidence"] = enriched_data.get("_confidence", 0)
        result["owl_verified_fields"] = enriched_data.get("_verified_fields", 0)
        result["owl_sources"] = json.dumps(enriched_data.get("_all_sources", []))

        with open(self.results_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=result.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)

    def _save_failed(self, failed_profiles: List[Dict]):
        """Save failed profiles for retry."""
        with open(self.errors_file, "w") as f:
            json.dump(failed_profiles, f, indent=2)

    async def _process_single_profile(
        self,
        index: int,
        profile: Dict,
        total_profiles: int,
        semaphore: asyncio.Semaphore,
    ) -> Dict:
        """
        Process a single profile with semaphore-controlled concurrency.

        Returns dict with result info for aggregation.
        """
        async with semaphore:
            name = profile.get("name", profile.get("Name", ""))
            company = profile.get("company", profile.get("Company", ""))
            email = profile.get("email", profile.get("Email", ""))
            website = profile.get("website", profile.get("Website", ""))
            linkedin = profile.get("linkedin", profile.get("LinkedIn", ""))

            result_info = {
                "index": index,
                "name": name,
                "success": False,
                "verified_fields": 0,
                "error": None,
                "skipped": False,
            }

            if not name:
                logger.warning(f"Profile {index}: Missing name, skipping")
                result_info["skipped"] = True
                return result_info

            # Check if already has rich data (skip if complete)
            if self._has_sufficient_data(profile):
                logger.info(f"Profile {index}: {name} - Already enriched, skipping")
                result_info["skipped"] = True
                return result_info

            print(f"[{index+1}/{total_profiles}] Enriching: {name}...")

            try:
                result = await self.service.enrich_profile(
                    name=name,
                    company=company,
                    website=website,
                    linkedin=linkedin,
                    email=email,
                    existing_data=profile,
                )

                if result.enriched:
                    jv_data = result.to_jv_matcher_format()

                    # Thread-safe CSV write
                    async with self._csv_lock:
                        self._append_result(profile, jv_data)

                    verified = jv_data.get("_verified_fields", 0)
                    confidence = jv_data.get("_confidence", 0)

                    result_info["success"] = True
                    result_info["verified_fields"] = verified
                    result_info["jv_data"] = jv_data

                    print(f"  ✓ [{name}] {verified}/12 verified fields ({confidence:.0%})")

                    # Show contact info first (critical for outreach)
                    if jv_data.get("email"):
                        print(f"    📧 Email: {jv_data['email']}")
                    if jv_data.get("booking_link"):
                        print(f"    📅 Booking: {jv_data['booking_link'][:60]}...")

                    # Save to Supabase if enabled (use sync_to_async for Django ORM)
                    if self.save_to_supabase and profile.get("id"):
                        saved = await sync_to_async(save_enriched_to_supabase, thread_sensitive=True)(
                            profile["id"], jv_data
                        )
                        if saved:
                            async with self._stats_lock:
                                self.session_stats["saved_to_supabase"] += 1
                            print(f"    💾 Saved to Supabase")
                else:
                    error = result.error or "No enriched data returned"
                    result_info["error"] = error
                    print(f"  ✗ [{name}] {error[:60]}...")

            except Exception as e:
                result_info["error"] = str(e)
                print(f"  ✗ [{name}] ERROR - {str(e)[:60]}...")

            # Rate limiting delay between profiles
            if self.delay_between_profiles > 0:
                await asyncio.sleep(self.delay_between_profiles)

            return result_info

    async def process_batch(
        self,
        profiles: List[Dict],
        resume: bool = True,
        max_profiles: Optional[int] = None,
    ) -> BatchProgress:
        """
        Process a batch of profiles with OWL enrichment.

        Args:
            profiles: List of profile dicts with name, email, company, etc.
            resume: Whether to resume from last checkpoint
            max_profiles: Maximum profiles to process (for testing)

        Returns:
            BatchProgress with statistics
        """
        total_profiles = len(profiles)
        if max_profiles:
            total_profiles = min(total_profiles, max_profiles)

        # Determine starting point
        start_index = 0
        if resume and self.checkpoint:
            start_index = self.checkpoint.last_processed_index + 1
            logger.info(f"Resuming from index {start_index}")

        # Initialize checkpoint if new run
        if not self.checkpoint or not resume:
            self.checkpoint = BatchCheckpoint(
                last_processed_index=-1,
                total_profiles=total_profiles,
                completed=0,
                failed=0,
                skipped=0,
                started_at=datetime.now().isoformat(),
                last_updated=datetime.now().isoformat(),
                failed_profiles=[],
            )

        failed_profiles = self.checkpoint.failed_profiles.copy()
        total_verified = 0

        logger.info(f"Processing {total_profiles - start_index} profiles (starting at {start_index})")
        print(f"\n{'='*60}")
        print(f"OWL BATCH ENRICHMENT")
        print(f"{'='*60}")
        print(f"Total profiles: {total_profiles}")
        print(f"Starting at: {start_index}")
        print(f"Workers: {self.workers}")
        print(f"Output: {self.results_file}")
        print(f"{'='*60}\n")

        # Use parallel processing if workers > 1
        if self.workers > 1:
            progress = await self._process_batch_parallel(
                profiles, start_index, total_profiles, failed_profiles
            )
            return progress

        # Sequential processing (original behavior)
        for i in range(start_index, total_profiles):
            profile = profiles[i]
            name = profile.get("name", profile.get("Name", ""))
            company = profile.get("company", profile.get("Company", ""))
            email = profile.get("email", profile.get("Email", ""))
            website = profile.get("website", profile.get("Website", ""))
            linkedin = profile.get("linkedin", profile.get("LinkedIn", ""))

            if not name:
                logger.warning(f"Profile {i}: Missing name, skipping")
                self.checkpoint.skipped += 1
                continue

            # Check if already has rich data (skip if complete)
            if self._has_sufficient_data(profile):
                logger.info(f"Profile {i}: {name} - Already enriched, skipping")
                self.checkpoint.skipped += 1
                self.checkpoint.last_processed_index = i
                continue

            print(f"[{i+1}/{total_profiles}] Enriching: {name}...")

            try:
                result = await self.service.enrich_profile(
                    name=name,
                    company=company,
                    website=website,
                    linkedin=linkedin,
                    email=email,
                    existing_data=profile,
                )

                if result.enriched:
                    jv_data = result.to_jv_matcher_format()
                    self._append_result(profile, jv_data)
                    self.checkpoint.completed += 1
                    self.session_stats["enriched"] += 1

                    verified = jv_data.get("_verified_fields", 0)
                    total_verified += verified
                    confidence = jv_data.get("_confidence", 0)

                    print(f"  ✓ SUCCESS - {verified}/12 verified fields ({confidence:.0%} confidence)")

                    # Show contact info first (critical for outreach)
                    if jv_data.get("email"):
                        print(f"    📧 Email: {jv_data['email']}")
                    if jv_data.get("booking_link"):
                        print(f"    📅 Booking: {jv_data['booking_link'][:60]}...")
                    # Show key findings
                    if jv_data.get("signature_programs"):
                        print(f"    Programs: {jv_data['signature_programs'][:80]}...")
                    if jv_data.get("seeking"):
                        print(f"    Seeking: {jv_data['seeking'][:80]}...")

                    # Save to Supabase if enabled (use sync_to_async for Django ORM)
                    if self.save_to_supabase and profile.get("id"):
                        saved = await sync_to_async(save_enriched_to_supabase, thread_sensitive=True)(
                            profile["id"], jv_data
                        )
                        if saved:
                            self.session_stats["saved_to_supabase"] += 1
                            print(f"    💾 Saved to Supabase")
                else:
                    error = result.error or "No enriched data returned"
                    print(f"  ✗ FAILED - {error[:60]}...")
                    failed_profiles.append({
                        "index": i,
                        "profile": profile,
                        "error": error,
                        "timestamp": datetime.now().isoformat(),
                    })
                    self.checkpoint.failed += 1
                    self.session_stats["failed"] += 1

            except Exception as e:
                print(f"  ✗ ERROR - {str(e)[:60]}...")
                failed_profiles.append({
                    "index": i,
                    "profile": profile,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                })
                self.checkpoint.failed += 1
                self.session_stats["failed"] += 1

            self.checkpoint.last_processed_index = i
            self.checkpoint.failed_profiles = failed_profiles
            self.session_stats["processed"] += 1

            # Update average verified fields
            if self.checkpoint.completed > 0:
                self.checkpoint.avg_verified_fields = total_verified / self.checkpoint.completed

            # Save checkpoint periodically
            if (i + 1) % self.save_interval == 0:
                self._save_checkpoint(self.checkpoint)
                self._save_failed(failed_profiles)
                print(f"  [Checkpoint saved at {i+1}]")

            # Rate limiting delay
            if self.delay_between_profiles > 0 and i < total_profiles - 1:
                await asyncio.sleep(self.delay_between_profiles)

        # Final save
        self._save_checkpoint(self.checkpoint)
        self._save_failed(failed_profiles)

        # Build progress report
        progress = BatchProgress(
            total_profiles=total_profiles,
            completed=self.checkpoint.completed,
            failed=self.checkpoint.failed,
            skipped=self.checkpoint.skipped,
            avg_verified_fields=self.checkpoint.avg_verified_fields,
            last_processed_index=self.checkpoint.last_processed_index,
            started_at=datetime.fromisoformat(self.checkpoint.started_at),
            last_updated=datetime.now(),
        )

        return progress

    async def _process_batch_parallel(
        self,
        profiles: List[Dict],
        start_index: int,
        total_profiles: int,
        failed_profiles: List[Dict],
    ) -> BatchProgress:
        """
        Process profiles in parallel using asyncio.gather with semaphore.
        """
        semaphore = asyncio.Semaphore(self.workers)
        total_verified = 0

        print(f"🚀 Starting parallel processing with {self.workers} workers...\n")

        # Create tasks for all remaining profiles
        tasks = []
        for i in range(start_index, total_profiles):
            profile = profiles[i]
            task = self._process_single_profile(
                index=i,
                profile=profile,
                total_profiles=total_profiles,
                semaphore=semaphore,
            )
            tasks.append(task)

        # Process in batches to allow checkpointing
        batch_size = self.save_interval * self.workers
        for batch_start in range(0, len(tasks), batch_size):
            batch_end = min(batch_start + batch_size, len(tasks))
            batch_tasks = tasks[batch_start:batch_end]

            # Run batch concurrently
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Aggregate results
            for result in results:
                if isinstance(result, Exception):
                    self.checkpoint.failed += 1
                    self.session_stats["failed"] += 1
                    continue

                if result.get("skipped"):
                    self.checkpoint.skipped += 1
                elif result.get("success"):
                    self.checkpoint.completed += 1
                    self.session_stats["enriched"] += 1
                    total_verified += result.get("verified_fields", 0)
                elif result.get("error"):
                    failed_profiles.append({
                        "index": result["index"],
                        "name": result["name"],
                        "error": result["error"],
                        "timestamp": datetime.now().isoformat(),
                    })
                    self.checkpoint.failed += 1
                    self.session_stats["failed"] += 1

                self.session_stats["processed"] += 1

            # Update checkpoint
            actual_index = start_index + batch_end - 1
            self.checkpoint.last_processed_index = actual_index
            self.checkpoint.failed_profiles = failed_profiles

            if self.checkpoint.completed > 0:
                self.checkpoint.avg_verified_fields = total_verified / self.checkpoint.completed

            # Save checkpoint after each batch
            async with self._checkpoint_lock:
                self._save_checkpoint(self.checkpoint)
                self._save_failed(failed_profiles)
            print(f"\n  [Checkpoint saved: {actual_index + 1}/{total_profiles} processed]\n")

        # Build progress report
        progress = BatchProgress(
            total_profiles=total_profiles,
            completed=self.checkpoint.completed,
            failed=self.checkpoint.failed,
            skipped=self.checkpoint.skipped,
            avg_verified_fields=self.checkpoint.avg_verified_fields,
            last_processed_index=self.checkpoint.last_processed_index,
            started_at=datetime.fromisoformat(self.checkpoint.started_at),
            last_updated=datetime.now(),
        )

        return progress

    def _has_sufficient_data(self, profile: Dict) -> bool:
        """Check if profile already has sufficient enrichment data."""
        # Check for OWL-enriched fields
        if profile.get("owl_seeking") or profile.get("owl_signature_programs"):
            return True

        # Check standard fields
        required_fields = ["seeking", "who_you_serve", "what_you_do", "offering"]
        filled = sum(1 for f in required_fields if profile.get(f) and len(str(profile.get(f))) > 20)
        return filled >= 3

    def get_progress_report(self) -> str:
        """Generate a human-readable progress report."""
        if not self.checkpoint:
            return "No processing started yet."

        lines = [
            "",
            "=" * 60,
            "OWL BATCH ENRICHMENT PROGRESS REPORT",
            "=" * 60,
            f"Started: {self.checkpoint.started_at}",
            f"Last Update: {self.checkpoint.last_updated}",
            "",
            f"Total Profiles: {self.checkpoint.total_profiles}",
            f"Processed: {self.checkpoint.last_processed_index + 1}",
            f"  - Enriched: {self.checkpoint.completed}",
            f"  - Failed: {self.checkpoint.failed}",
            f"  - Skipped: {self.checkpoint.skipped}",
            "",
            f"Avg Verified Fields: {self.checkpoint.avg_verified_fields:.1f}/12",
            f"Remaining: {self.checkpoint.total_profiles - self.checkpoint.last_processed_index - 1}",
            "",
            f"Output File: {self.results_file}",
            "=" * 60,
        ]
        return "\n".join(lines)


def load_profiles_from_csv(csv_path: str) -> List[Dict]:
    """Load profiles from a CSV file."""
    profiles = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            profiles.append(dict(row))
    return profiles


def load_profiles_from_supabase(
    filter_sparse: bool = True,
    min_missing_fields: int = 2,
    require_website: bool = False,
) -> List[Dict]:
    """
    Load profiles from Supabase database.

    Args:
        filter_sparse: Only return profiles missing key JV fields
        min_missing_fields: Minimum number of missing fields to include (if filter_sparse)
        require_website: Only include profiles with a website (needed for research)

    Returns:
        List of profile dicts ready for OWL enrichment
    """
    from matching.models import SupabaseProfile
    from django.db.models import Q

    # Start with all profiles
    queryset = SupabaseProfile.objects.all()

    # Optionally require website (makes research more effective)
    if require_website:
        queryset = queryset.exclude(Q(website__isnull=True) | Q(website=''))

    profiles = []
    key_fields = ['seeking', 'who_you_serve', 'what_you_do', 'offering']

    for p in queryset:
        # Count missing fields
        missing = []
        if not p.seeking:
            missing.append('seeking')
        if not p.who_you_serve:
            missing.append('who_you_serve')
        if not p.what_you_do:
            missing.append('what_you_do')
        if not p.offering:
            missing.append('offering')

        # Skip if filtering and has enough data
        if filter_sparse and len(missing) < min_missing_fields:
            continue

        # Convert to dict format expected by batch processor
        profile = {
            'id': str(p.id),  # Keep UUID for write-back
            'name': p.name or '',
            'email': p.email or '',
            'company': p.company or '',
            'website': p.website or '',
            'linkedin': p.linkedin or '',
            'phone': p.phone or '',
            'niche': p.niche or '',
            'list_size': p.list_size or 0,
            'social_reach': p.social_reach or 0,
            # Existing JV fields (may be empty)
            'seeking': p.seeking or '',
            'who_you_serve': p.who_you_serve or '',
            'what_you_do': p.what_you_do or '',
            'offering': p.offering or '',
            'bio': p.bio or '',
            'notes': p.notes or '',
            # Metadata
            '_missing_fields': missing,
        }
        profiles.append(profile)

    logger.info(f"Loaded {len(profiles)} profiles from Supabase (filter_sparse={filter_sparse})")
    return profiles


def save_enriched_to_supabase(
    profile_id: str,
    enriched_data: Dict,
    overwrite_existing: bool = False,
) -> bool:
    """
    Save OWL-enriched data back to Supabase.

    Args:
        profile_id: UUID of the profile to update
        enriched_data: Dict with enriched fields from OWL
        overwrite_existing: If True, overwrite existing non-empty fields

    Returns:
        True if saved successfully
    """
    from matching.models import SupabaseProfile

    try:
        profile = SupabaseProfile.objects.get(id=profile_id)

        # Fields to potentially update
        # JV partnership fields
        field_mapping = {
            'seeking': 'seeking',
            'who_you_serve': 'who_you_serve',
            'who_they_serve': 'who_you_serve',  # Handle both naming conventions
            'what_you_do': 'what_you_do',
            'offering': 'offering',
            # Contact info (critical for outreach)
            'email': 'email',
            'phone': 'phone',
            'linkedin': 'linkedin',
            'website': 'website',
            'bio': 'bio',
            # Signature programs and booking links (columns added to Supabase)
            'signature_programs': 'signature_programs',
            'booking_link': 'booking_link',
        }

        updated_fields = []
        for owl_field, db_field in field_mapping.items():
            new_value = enriched_data.get(owl_field)
            if not new_value:
                continue

            current_value = getattr(profile, db_field, None)

            # Only update if empty or overwrite is enabled
            if not current_value or overwrite_existing:
                setattr(profile, db_field, new_value)
                updated_fields.append(db_field)

        if updated_fields:
            profile.save(update_fields=updated_fields + ['updated_at'])
            logger.info(f"Updated {profile.name}: {', '.join(updated_fields)}")
            return True

        return False

    except SupabaseProfile.DoesNotExist:
        logger.warning(f"Profile not found: {profile_id}")
        return False
    except Exception as e:
        logger.error(f"Error saving to Supabase: {e}")
        return False


async def run_owl_batch_enrichment(
    input_csv: Optional[str] = None,
    output_dir: str = "output",
    resume: bool = True,
    max_profiles: Optional[int] = None,
    delay: float = 2.0,
    workers: int = 1,
    # Supabase options
    from_supabase: bool = False,
    filter_sparse: bool = True,
    save_to_supabase: bool = False,
    require_website: bool = False,
) -> BatchProgress:
    """
    Main entry point for OWL batch enrichment.

    Args:
        input_csv: Path to CSV with profiles (ignored if from_supabase=True)
        output_dir: Directory for output files
        resume: Whether to resume from checkpoint
        max_profiles: Max profiles to process (for testing)
        delay: Seconds between profiles (rate limiting)
        workers: Number of concurrent workers (default 1 = sequential)
        from_supabase: Load profiles from Supabase instead of CSV
        filter_sparse: Only process profiles missing key JV fields
        save_to_supabase: Write enriched data back to Supabase
        require_website: Only include profiles with a website

    Returns:
        BatchProgress with final statistics
    """
    # Load profiles from Supabase or CSV
    if from_supabase:
        # Use sync_to_async for Django ORM calls from async context
        profiles = await sync_to_async(load_profiles_from_supabase, thread_sensitive=True)(
            filter_sparse=filter_sparse,
            require_website=require_website,
        )
        logger.info(f"Loaded {len(profiles)} profiles from Supabase")
        print(f"\n📊 Loaded {len(profiles)} profiles from Supabase")
        if filter_sparse:
            print(f"   (filtered to profiles missing key JV fields)")
    else:
        if not input_csv:
            raise ValueError("Must provide input_csv or set from_supabase=True")
        profiles = load_profiles_from_csv(input_csv)
        logger.info(f"Loaded {len(profiles)} profiles from {input_csv}")

    # Create processor
    processor = OWLBatchProcessor(
        output_dir=output_dir,
        delay_between_profiles=delay,
        save_to_supabase=save_to_supabase,
        workers=workers,
    )

    if save_to_supabase:
        print(f"   💾 Will save enriched data back to Supabase")
    if workers > 1:
        print(f"   🚀 Parallel processing with {workers} workers")

    # Process
    progress = await processor.process_batch(
        profiles=profiles,
        resume=resume,
        max_profiles=max_profiles,
    )

    # Print report
    print(processor.get_progress_report())

    return progress


def run_owl_batch_enrichment_sync(
    input_csv: Optional[str] = None,
    output_dir: str = "output",
    resume: bool = True,
    max_profiles: Optional[int] = None,
    delay: float = 2.0,
    workers: int = 1,
    # Supabase options
    from_supabase: bool = False,
    filter_sparse: bool = True,
    save_to_supabase: bool = False,
    require_website: bool = False,
) -> BatchProgress:
    """Synchronous wrapper for run_owl_batch_enrichment."""
    return asyncio.run(
        run_owl_batch_enrichment(
            input_csv=input_csv,
            output_dir=output_dir,
            resume=resume,
            max_profiles=max_profiles,
            delay=delay,
            workers=workers,
            from_supabase=from_supabase,
            filter_sparse=filter_sparse,
            save_to_supabase=save_to_supabase,
            require_website=require_website,
        )
    )


# Test / CLI
if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        print("Usage: python owl_batch_processor.py <input_csv> [max_profiles]")
        print("Example: python owl_batch_processor.py contacts.csv 5")
        sys.exit(1)

    input_csv = sys.argv[1]
    max_profiles = int(sys.argv[2]) if len(sys.argv) > 2 else None

    progress = run_owl_batch_enrichment_sync(
        input_csv=input_csv,
        output_dir="owl_output",
        max_profiles=max_profiles,
    )

    print(f"\nCompleted: {progress.completed}")
    print(f"Failed: {progress.failed}")
    print(f"Avg Verified Fields: {progress.avg_verified_fields:.1f}/9")
