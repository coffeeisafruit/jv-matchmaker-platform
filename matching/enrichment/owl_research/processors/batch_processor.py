"""
Batch Profile Enrichment Processor

Processes profiles in batches with:
- Resume capability (saves progress to checkpoint file)
- Cost tracking and budget limits
- Parallel processing with rate limiting
- Error handling with retries

Uses Claude Code Max subscription via Agent SDK for extraction.
"""

import asyncio
import csv
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from matching.enrichment.owl_research.agents.enrichment_agent import ProfileEnrichmentAgent
from matching.enrichment.owl_research.config.settings import EnrichmentConfig
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
    estimated_cost_usd: float
    started_at: str
    last_updated: str
    failed_profiles: List[Dict]  # Store failed profiles for retry


class BatchEnrichmentProcessor:
    """
    Processes large batches of profiles with resume capability.

    Features:
    - Checkpointing: Saves progress every N profiles
    - Resume: Can restart from last checkpoint
    - Cost tracking: Estimates and tracks costs
    - Error handling: Retries failed profiles, logs errors
    - Rate limiting: Configurable delays between profiles
    """

    def __init__(
        self,
        config: Optional[EnrichmentConfig] = None,
        output_dir: str = "output",
        checkpoint_file: str = "checkpoint.json",
    ):
        self.config = config or EnrichmentConfig()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.checkpoint_file = self.output_dir / checkpoint_file
        self.results_file = self.output_dir / "enriched_profiles.csv"
        self.errors_file = self.output_dir / "failed_profiles.json"

        self.agent = ProfileEnrichmentAgent(self.config)

        # Tracking
        self.checkpoint = self._load_checkpoint()
        self.session_stats = {
            "processed": 0,
            "enriched": 0,
            "failed": 0,
            "skipped": 0,
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
        result = {**profile, **enriched_data}

        # Remove internal fields starting with _
        result = {k: v for k, v in result.items() if not k.startswith("_")}

        with open(self.results_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=result.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)

    def _save_failed(self, failed_profiles: List[Dict]):
        """Save failed profiles for retry."""
        with open(self.errors_file, "w") as f:
            json.dump(failed_profiles, f, indent=2)

    async def process_batch(
        self,
        profiles: List[Dict],
        resume: bool = True,
        max_profiles: Optional[int] = None,
    ) -> BatchProgress:
        """
        Process a batch of profiles with enrichment.

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
                estimated_cost_usd=0.0,
                started_at=datetime.now().isoformat(),
                last_updated=datetime.now().isoformat(),
                failed_profiles=[],
            )

        failed_profiles = self.checkpoint.failed_profiles.copy()

        logger.info(f"Processing {total_profiles - start_index} profiles (starting at {start_index})")

        for i in range(start_index, total_profiles):
            profile = profiles[i]
            name = profile.get("name", profile.get("Name", ""))
            company = profile.get("company", profile.get("Company", ""))
            email = profile.get("email", profile.get("Email", ""))
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

            logger.info(f"Profile {i}/{total_profiles}: Enriching {name}...")

            try:
                result = await self.agent.enrich_profile(
                    name=name,
                    company=company,
                    email=email,
                    linkedin_url=linkedin,
                    existing_data=profile,
                )

                if result.enriched:
                    jv_data = result.to_jv_matcher_format()
                    self._append_result(profile, jv_data)
                    self.checkpoint.completed += 1
                    self.session_stats["enriched"] += 1

                    confidence = jv_data.get("_confidence", 0)
                    logger.info(f"  SUCCESS - Confidence: {confidence:.2f}")
                else:
                    error = result.error or "No enriched data returned"
                    logger.warning(f"  FAILED - {error}")
                    failed_profiles.append({
                        "index": i,
                        "profile": profile,
                        "error": error,
                        "timestamp": datetime.now().isoformat(),
                    })
                    self.checkpoint.failed += 1
                    self.session_stats["failed"] += 1

            except Exception as e:
                logger.error(f"  ERROR - {e}")
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

            # Save checkpoint periodically
            if (i + 1) % self.config.save_interval == 0:
                self._save_checkpoint(self.checkpoint)
                self._save_failed(failed_profiles)
                logger.info(f"  Checkpoint saved at index {i}")

            # Estimate cost
            searches = self.agent.total_searches
            self.checkpoint.estimated_cost_usd = searches * 0.004  # Tavily cost estimate

            # Check budget
            if self.checkpoint.estimated_cost_usd >= self.config.max_total_budget:
                logger.warning(f"Budget limit reached: ${self.checkpoint.estimated_cost_usd:.2f}")
                break

            # Rate limiting delay
            if self.config.delay_between_profiles > 0:
                await asyncio.sleep(self.config.delay_between_profiles)

        # Final save
        self._save_checkpoint(self.checkpoint)
        self._save_failed(failed_profiles)

        # Build progress report
        progress = BatchProgress(
            total_profiles=total_profiles,
            completed=self.checkpoint.completed,
            failed=self.checkpoint.failed,
            skipped=self.checkpoint.skipped,
            total_cost_usd=self.checkpoint.estimated_cost_usd,
            last_processed_index=self.checkpoint.last_processed_index,
            started_at=datetime.fromisoformat(self.checkpoint.started_at),
            last_updated=datetime.now(),
        )

        return progress

    def _has_sufficient_data(self, profile: Dict) -> bool:
        """Check if profile already has sufficient enrichment data."""
        required_fields = ["seeking", "who_you_serve", "what_you_do", "offering"]
        filled = sum(1 for f in required_fields if profile.get(f) and len(str(profile.get(f))) > 10)
        return filled >= 3  # At least 3 of 4 fields filled

    def get_progress_report(self) -> str:
        """Generate a human-readable progress report."""
        if not self.checkpoint:
            return "No processing started yet."

        lines = [
            "=" * 60,
            "BATCH ENRICHMENT PROGRESS REPORT",
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
            f"Remaining: {self.checkpoint.total_profiles - self.checkpoint.last_processed_index - 1}",
            f"Estimated Cost: ${self.checkpoint.estimated_cost_usd:.2f}",
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


async def run_batch_enrichment(
    input_csv: str,
    output_dir: str = "output",
    resume: bool = True,
    max_profiles: Optional[int] = None,
) -> BatchProgress:
    """
    Main entry point for batch enrichment.

    Args:
        input_csv: Path to CSV with profiles
        output_dir: Directory for output files
        resume: Whether to resume from checkpoint
        max_profiles: Max profiles to process (for testing)

    Returns:
        BatchProgress with final statistics
    """
    # Load profiles
    profiles = load_profiles_from_csv(input_csv)
    logger.info(f"Loaded {len(profiles)} profiles from {input_csv}")

    # Create processor
    processor = BatchEnrichmentProcessor(
        output_dir=output_dir,
    )

    # Process
    progress = await processor.process_batch(
        profiles=profiles,
        resume=resume,
        max_profiles=max_profiles,
    )

    # Print report
    print(processor.get_progress_report())

    return progress


def run_batch_enrichment_sync(
    input_csv: str,
    output_dir: str = "output",
    resume: bool = True,
    max_profiles: Optional[int] = None,
) -> BatchProgress:
    """Synchronous wrapper for run_batch_enrichment."""
    return asyncio.run(
        run_batch_enrichment(input_csv, output_dir, resume, max_profiles)
    )
