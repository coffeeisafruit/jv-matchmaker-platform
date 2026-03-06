"""
Substack newsletter creator scraper.

Uses Substack's public category leaderboard API to discover newsletter
publications across JV-relevant categories (business, health & wellness,
education, faith & spirituality, finance).

API endpoint: /api/v1/category/public/{category_id}/{filter}?page={n}
  - Returns 25 publications per page, ranked by subscriber count
  - Each publication includes author name, bio, subscriber estimates,
    custom domain, hero text, and payment status
  - Pagination stops when `more` is False (~21 pages per category)

JV relevance: Substack creators are Tier B audience owners -- they have
built-in subscriber bases, many offer paid products, and they actively
seek partnerships for growth. The API exposes subscriber magnitude
(e.g. "Thousands of subscribers") which maps to revenue_indicator.

Estimated yield: 2,000-5,000 unique creators across all categories.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


logger = logging.getLogger(__name__)


# JV-relevant Substack categories: (category_id, slug, display_name)
# Category IDs sourced from /api/v1/categories endpoint
CATEGORIES: list[tuple[int, str, str]] = [
    (62, "business", "Business"),
    (355, "health", "Health & Wellness"),
    (34, "education", "Education"),
    (153, "finance", "Finance"),
    (223, "faith", "Faith & Spirituality"),
    (4, "technology", "Technology"),
    (96, "culture", "Culture"),
    (114, "philosophy", "Philosophy"),
    (1796, "parenting", "Parenting"),
]

# Leaderboard filters: "paid" surfaces monetized pubs (higher JV value),
# "all" catches free-tier creators with large audiences too.
LEADERBOARD_FILTERS = ["paid", "all"]

PUBLICATIONS_PER_PAGE = 25


class Scraper(BaseScraper):
    """Substack category leaderboard scraper via JSON API."""

    SOURCE_NAME = "substack"
    BASE_URL = "https://substack.com"
    REQUESTS_PER_MINUTE = 15  # Conservative -- public API, be polite

    TYPICAL_ROLES = [
        "newsletter_creator", "writer", "thought_leader",
        "coach", "educator", "podcaster",
    ]
    TYPICAL_NICHES = [
        "business", "coaching", "self-improvement", "finance",
        "health", "education", "spirituality", "leadership",
    ]
    TYPICAL_OFFERINGS = [
        "newsletter", "paid subscription", "community",
        "course", "podcast", "consulting",
    ]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_pub_ids: set[int] = set()

    # ------------------------------------------------------------------
    # Abstract method implementations (not used -- run() handles everything)
    # ------------------------------------------------------------------

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- run() handles pagination directly."""
        return iter([])

    def scrape_page(self, url: str, html_text: str) -> list[ScrapedContact]:
        """Not used -- run() handles extraction directly."""
        return []

    # ------------------------------------------------------------------
    # Main run loop (overrides BaseScraper.run)
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Iterate category leaderboards, paginate, extract unique creators.

        Args:
            max_pages: Max API pages to fetch across all categories (0 = unlimited).
            max_contacts: Max contacts to yield (0 = unlimited).
            checkpoint: Optional dict with 'category_id', 'filter', and 'page'
                        to resume from a previous run.

        Yields:
            ScrapedContact for each unique Substack publication creator.
        """
        pages_fetched = 0
        contacts_yielded = 0

        # Resume support
        resume_cat_id = (checkpoint or {}).get("category_id")
        resume_filter = (checkpoint or {}).get("filter")
        resume_page = (checkpoint or {}).get("page", 0)
        past_checkpoint = resume_cat_id is None

        self.logger.info(
            "Starting Substack scraper (max_pages=%s, max_contacts=%s, checkpoint=%s)",
            max_pages or "unlimited",
            max_contacts or "unlimited",
            checkpoint or "none",
        )

        for cat_id, cat_slug, cat_name in CATEGORIES:
            for lb_filter in LEADERBOARD_FILTERS:
                # Handle checkpoint resume
                if not past_checkpoint:
                    if cat_id == resume_cat_id and lb_filter == resume_filter:
                        past_checkpoint = True
                    else:
                        continue

                start_page = resume_page if (
                    cat_id == resume_cat_id and lb_filter == resume_filter
                ) else 0
                resume_page = 0  # Only apply to the first after resume

                self.logger.info(
                    "Scraping category: %s (%s) filter=%s starting page=%d",
                    cat_name, cat_id, lb_filter, start_page,
                )

                page = start_page
                while True:
                    url = (
                        f"{self.BASE_URL}/api/v1/category/public"
                        f"/{cat_id}/{lb_filter}?page={page}"
                    )
                    data = self.fetch_json(url)
                    if data is None:
                        self.logger.warning("Failed to fetch %s, skipping", url)
                        break

                    pages_fetched += 1
                    publications = data.get("publications", [])
                    has_more = data.get("more", False)

                    if not publications:
                        self.logger.debug(
                            "No publications on %s page %d, moving on",
                            cat_name, page,
                        )
                        break

                    for pub in publications:
                        pub_id = pub.get("id")
                        if not pub_id or pub_id in self._seen_pub_ids:
                            continue
                        self._seen_pub_ids.add(pub_id)

                        contact = self._pub_to_contact(pub, cat_slug, cat_name)
                        if not contact or not contact.is_valid():
                            continue

                        contact.source_platform = self.SOURCE_NAME
                        contact.source_url = url
                        contact.scraped_at = datetime.now().isoformat()
                        contact.email = contact.clean_email()

                        self.stats["contacts_found"] += 1
                        self.stats["contacts_valid"] += 1
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info(
                                "Reached max_contacts=%d", max_contacts
                            )
                            return

                    # Log progress periodically
                    if pages_fetched % 10 == 0:
                        self.logger.info(
                            "Progress: %d pages, %d unique pubs yielded, "
                            "%d total pubs seen",
                            pages_fetched,
                            contacts_yielded,
                            len(self._seen_pub_ids),
                        )

                    if max_pages and pages_fetched >= max_pages:
                        self.logger.info("Reached max_pages=%d", max_pages)
                        return

                    if not has_more:
                        self.logger.info(
                            "Finished %s/%s: %d pages",
                            cat_name, lb_filter, page + 1,
                        )
                        break

                    page += 1

        self.logger.info("Substack scraper complete: %s", self.stats)

    # ------------------------------------------------------------------
    # Publication -> ScrapedContact mapping
    # ------------------------------------------------------------------

    def _pub_to_contact(
        self,
        pub: dict,
        cat_slug: str,
        cat_name: str,
    ) -> Optional[ScrapedContact]:
        """Convert a Substack publication record to a ScrapedContact.

        Args:
            pub: The publication dict from the category leaderboard API.
            cat_slug: The category slug (e.g. "business").
            cat_name: Human-readable category name (e.g. "Business").

        Returns:
            ScrapedContact or None if author name is missing/invalid.
        """
        author_name = (pub.get("author_name") or "").strip()
        if not author_name or len(author_name) < 2:
            return None

        pub_name = (pub.get("name") or "").strip()
        subdomain = (pub.get("subdomain") or "").strip()

        # Build the publication URL
        custom_domain = (pub.get("custom_domain") or "").strip()
        if custom_domain:
            website = f"https://{custom_domain}"
        elif subdomain:
            website = f"https://{subdomain}.substack.com"
        else:
            website = ""

        # Build a rich bio from available fields
        bio_parts: list[str] = []
        hero_text = (pub.get("hero_text") or "").strip()
        if hero_text:
            bio_parts.append(hero_text)

        author_bio = (pub.get("author_bio") or "").strip()
        if author_bio:
            bio_parts.append(f"Author bio: {author_bio}")

        if pub_name and pub_name != author_name:
            bio_parts.append(f"Newsletter: {pub_name}")

        # Subscriber info
        free_sub_count = (
            pub.get("rankingDetailFreeSubscriberCount") or ""
        ).strip()
        if free_sub_count:
            bio_parts.append(f"Subscribers: {free_sub_count}")

        # Paid subscription benefits (signals premium offerings)
        paid_benefits = pub.get("paid_subscription_benefits") or []
        if paid_benefits and isinstance(paid_benefits, list):
            benefits_str = "; ".join(
                str(b).strip() for b in paid_benefits[:3] if b
            )
            if benefits_str:
                bio_parts.append(f"Paid benefits: {benefits_str}")

        # Podcast indicator
        if pub.get("podcast_enabled"):
            podcast_title = (pub.get("podcast_title") or "").strip()
            if podcast_title:
                bio_parts.append(f"Podcast: {podcast_title}")
            else:
                bio_parts.append("Has podcast")

        bio = " | ".join(bio_parts)

        # Revenue / audience indicator from ranking detail
        ranking_detail = (pub.get("rankingDetail") or "").strip()
        free_sub_magnitude = str(
            pub.get("freeSubscriberCountOrderOfMagnitude") or ""
        ).strip()
        revenue_indicator = ""
        if ranking_detail:
            revenue_indicator = f"Paid: {ranking_detail}"
        if free_sub_magnitude:
            revenue_indicator += (
                f" (Total: {free_sub_magnitude} subscribers)"
                if revenue_indicator
                else f"{free_sub_magnitude} subscribers"
            )

        # Payments state as pricing signal
        payments_state = (pub.get("payments_state") or "").strip()
        pricing = ""
        if payments_state == "enabled":
            pricing = "Paid subscription available"
        elif pub.get("has_subscriber_only_podcast"):
            pricing = "Premium podcast"

        # Use free subscriber count as a magnitude indicator
        review_count = str(
            pub.get("freeSubscriberCount") or ""
        ).replace(",", "")

        return ScrapedContact(
            name=author_name,
            website=website,
            company=pub_name or "",
            bio=bio[:2000],
            categories=cat_name,
            source_category="newsletter_creators",
            pricing=pricing,
            revenue_indicator=revenue_indicator,
            review_count=review_count,
            product_focus=str(pub.get("type") or "newsletter"),
        )
