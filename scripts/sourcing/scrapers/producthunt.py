"""
Product Hunt Startup/Maker Profile Scraper

Scrapes product and maker data from https://www.producthunt.com.

Product Hunt provides a public GraphQL API at:
  https://api.producthunt.com/v2/api/graphql

However it requires a developer token (free to obtain from
https://www.producthunt.com/v2/oauth/applications). If the
PRODUCTHUNT_API_TOKEN env var is set, we use the GraphQL API for
efficient bulk queries. Otherwise, we fall back to scraping the
leaderboard archive pages which are server-rendered HTML.

Strategy:
  1. (Primary) GraphQL API: query posts with pagination cursors
  2. (Fallback) Leaderboard archive: /leaderboard/daily/YYYY/M/D
     These pages are server-rendered and contain product cards with
     name, tagline, website, vote count, and maker info.

Product Hunt posts include:
  - Product name, tagline, description
  - Website URL
  - Maker name(s)
  - Topic tags
  - Vote count

Estimated yield: 5,000-20,000 products (API) or 2,000-5,000 (HTML)
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# GraphQL query for fetching posts
POSTS_QUERY = """
query GetPosts($cursor: String, $postedAfter: DateTime, $postedBefore: DateTime) {
  posts(
    first: 20,
    after: $cursor,
    postedAfter: $postedAfter,
    postedBefore: $postedBefore
  ) {
    edges {
      node {
        id
        name
        tagline
        description
        slug
        website
        votesCount
        createdAt
        makers {
          id
          name
          username
          headline
          websiteUrl
        }
        topics {
          edges {
            node {
              name
              slug
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# How many months back to scrape leaderboard pages
LEADERBOARD_MONTHS_BACK = 6

# GraphQL API rate limit: max 450 requests per 15 minutes
GRAPHQL_REQUESTS_PER_MINUTE = 25


class Scraper(BaseScraper):
    SOURCE_NAME = "producthunt"
    BASE_URL = "https://www.producthunt.com"
    GRAPHQL_URL = "https://api.producthunt.com/v2/api/graphql"
    REQUESTS_PER_MINUTE = 6
    TYPICAL_ROLES = ["Product Creator"]
    TYPICAL_NICHES = ["saas_software", "ecommerce", "ai_machine_learning"]
    TYPICAL_OFFERINGS = ["software", "tools", "platform"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()
        self._seen_names: set[str] = set()
        self._api_token = os.environ.get("PRODUCTHUNT_API_TOKEN", "")
        if self._api_token:
            self.logger.info("Product Hunt API token found, using GraphQL API")
        else:
            self.logger.info("No PRODUCTHUNT_API_TOKEN set, falling back to HTML scraping")

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Product Hunt URLs to scrape."""
        if self._api_token:
            # GraphQL mode: yield sentinel URL, run() override handles the rest
            yield "graphql://posts"
        else:
            # HTML fallback: yield leaderboard archive URLs
            today = datetime.now()
            for days_back in range(LEADERBOARD_MONTHS_BACK * 30):
                date = today - timedelta(days=days_back)
                yield (
                    f"{self.BASE_URL}/leaderboard/daily"
                    f"/{date.year}/{date.month}/{date.day}"
                )

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Product Hunt HTML leaderboard page."""
        if url.startswith("graphql://"):
            # Should not reach here in GraphQL mode, but handle gracefully
            return []

        return self._parse_leaderboard_html(url, html)

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Override run() to support GraphQL API mode."""
        if self._api_token:
            yield from self._run_graphql(max_pages, max_contacts, checkpoint)
        else:
            # Use default HTML scraping via parent class
            yield from super().run(max_pages, max_contacts, checkpoint)

    def _run_graphql(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch products via GraphQL API with cursor pagination."""
        self.logger.info("Starting Product Hunt GraphQL scraper")

        cursor = (checkpoint or {}).get("cursor")
        pages_done = 0
        contacts_yielded = 0

        # Query in monthly chunks going back
        today = datetime.now()
        for month_offset in range(LEADERBOARD_MONTHS_BACK):
            start_date = today - timedelta(days=(month_offset + 1) * 30)
            end_date = today - timedelta(days=month_offset * 30)

            current_cursor = cursor if month_offset == 0 else None

            while True:
                variables = {
                    "postedAfter": start_date.strftime("%Y-%m-%dT00:00:00Z"),
                    "postedBefore": end_date.strftime("%Y-%m-%dT23:59:59Z"),
                }
                if current_cursor:
                    variables["cursor"] = current_cursor

                payload = {
                    "query": POSTS_QUERY,
                    "variables": variables,
                }

                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, GRAPHQL_REQUESTS_PER_MINUTE)

                try:
                    resp = self.session.post(
                        self.GRAPHQL_URL,
                        json=payload,
                        headers={
                            "Authorization": f"Bearer {self._api_token}",
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                        },
                        timeout=30,
                    )
                    resp.raise_for_status()
                    result = resp.json()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.logger.warning("GraphQL request failed: %s", e)
                    self.stats["errors"] += 1
                    break

                # Check for errors
                if "errors" in result:
                    self.logger.warning("GraphQL errors: %s", result["errors"])
                    break

                posts_data = result.get("data", {}).get("posts", {})
                edges = posts_data.get("edges", [])
                page_info = posts_data.get("pageInfo", {})

                if not edges:
                    break

                for edge in edges:
                    node = edge.get("node", {})
                    contact = self._node_to_contact(node)
                    if contact:
                        contact.source_platform = self.SOURCE_NAME
                        contact.scraped_at = datetime.now().isoformat()
                        contact.email = contact.clean_email()

                        if contact.is_valid():
                            self.stats["contacts_valid"] += 1
                            contacts_yielded += 1
                            yield contact

                            if max_contacts and contacts_yielded >= max_contacts:
                                self.logger.info("Reached max_contacts=%d", max_contacts)
                                return

                        self.stats["contacts_found"] += 1

                pages_done += 1
                if pages_done % 10 == 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                # Pagination
                if page_info.get("hasNextPage") and page_info.get("endCursor"):
                    current_cursor = page_info["endCursor"]
                else:
                    break

        self.logger.info("GraphQL scraper complete: %s", self.stats)

    def _node_to_contact(self, node: dict) -> ScrapedContact | None:
        """Convert a GraphQL post node to a ScrapedContact."""
        post_id = str(node.get("id", "")).strip()
        if post_id in self._seen_ids:
            return None
        self._seen_ids.add(post_id)

        name = (node.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        tagline = (node.get("tagline") or "").strip()
        description = (node.get("description") or "").strip()
        website = (node.get("website") or "").strip()
        slug = (node.get("slug") or "").strip()
        votes = node.get("votesCount", 0)
        created_at = (node.get("createdAt") or "").strip()

        # Maker info
        makers = node.get("makers", [])
        maker_name = ""
        maker_website = ""
        maker_headline = ""
        if makers and isinstance(makers, list):
            first_maker = makers[0]
            maker_name = (first_maker.get("name") or "").strip()
            maker_website = (first_maker.get("websiteUrl") or "").strip()
            maker_headline = (first_maker.get("headline") or "").strip()

        # Topics
        topics = []
        topics_data = node.get("topics", {})
        if isinstance(topics_data, dict):
            for edge in topics_data.get("edges", []):
                topic_node = edge.get("node", {})
                topic_name = (topic_node.get("name") or "").strip()
                if topic_name:
                    topics.append(topic_name)

        # If no external website, use Product Hunt page
        if not website:
            website = f"{self.BASE_URL}/posts/{slug}" if slug else ""

        # Build bio
        bio_parts = []
        if tagline:
            bio_parts.append(tagline)
        if description and description != tagline:
            bio_parts.append(description[:500])
        if votes:
            bio_parts.append(f"Upvotes: {votes}")
        if maker_name:
            bio_parts.append(f"Maker: {maker_name}")
        if maker_headline:
            bio_parts.append(f"({maker_headline})")
        if topics:
            bio_parts.append(f"Topics: {', '.join(topics[:5])}")
        if created_at:
            bio_parts.append(f"Launched: {created_at[:10]}")
        bio = " | ".join(bio_parts) if bio_parts else f"Product on Product Hunt"

        # Use maker name as contact name if available
        contact_name = maker_name if maker_name else name

        return ScrapedContact(
            name=contact_name,
            company=name,
            website=maker_website if maker_website else website,
            bio=bio,
            source_url=f"{self.BASE_URL}/posts/{slug}" if slug else "",
            source_category="startups",
            raw_data={
                "product_hunt_id": post_id,
                "slug": slug,
                "votes": votes,
                "maker": maker_name,
                "topics": topics,
            },
        )

    def _parse_leaderboard_html(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Product Hunt leaderboard HTML page."""
        soup = self.parse_html(html)
        contacts = []

        # Product Hunt uses data-test attributes for product cards
        # Look for links to /posts/{slug}
        post_links = set()
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            match = re.match(r"^/posts/([a-zA-Z0-9_\-]+)$", href)
            if match:
                slug = match.group(1)
                if slug not in self._seen_ids:
                    self._seen_ids.add(slug)
                    post_links.add(slug)

        # Try to extract data from the cards directly
        # PH uses various div structures - try multiple selectors
        for slug in post_links:
            # Find the link element for this slug
            link = soup.find("a", href=f"/posts/{slug}")
            if not link:
                continue

            name = ""
            tagline = ""

            # The product name is typically in the card near the link
            card = link.find_parent("div")
            if card:
                # Look for text elements within the card
                texts = []
                for el in card.find_all(["h2", "h3", "p", "span", "a"]):
                    text = el.get_text(strip=True)
                    if text and len(text) > 1:
                        texts.append(text)

                if texts:
                    name = texts[0]
                    if len(texts) > 1:
                        tagline = texts[1]
            else:
                name = link.get_text(strip=True)

            if not name or len(name) < 2:
                continue

            name_key = name.lower()
            if name_key in self._seen_names:
                continue
            self._seen_names.add(name_key)

            bio = tagline if tagline else f"Product on Product Hunt"

            contacts.append(ScrapedContact(
                name=name,
                company=name,
                website=f"{self.BASE_URL}/posts/{slug}",
                bio=bio,
                source_url=url,
                source_category="startups",
                raw_data={"slug": slug},
            ))

        # Also try to extract from embedded JSON/script data
        for script in soup.find_all("script", type="application/json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    self._extract_from_json(data, contacts)
            except (json.JSONDecodeError, ValueError):
                continue

        # Check for Next.js __NEXT_DATA__
        for script in soup.find_all("script", id="__NEXT_DATA__"):
            try:
                data = json.loads(script.string or "")
                props = data.get("props", {}).get("pageProps", {})
                posts = props.get("posts", [])
                if isinstance(posts, list):
                    for post in posts:
                        contact = self._node_to_contact(post)
                        if contact:
                            contacts.append(contact)
            except (json.JSONDecodeError, ValueError):
                continue

        return contacts

    def _extract_from_json(self, data: dict, contacts: list, depth: int = 0):
        """Recursively extract product data from embedded JSON."""
        if depth > 8:
            return

        if isinstance(data, dict):
            # Check if this looks like a product
            if (
                data.get("name")
                and (data.get("tagline") or data.get("slug"))
                and isinstance(data.get("name"), str)
            ):
                name = data["name"].strip()
                if name and len(name) > 1 and name.lower() not in self._seen_names:
                    self._seen_names.add(name.lower())
                    tagline = (data.get("tagline") or "").strip()
                    slug = (data.get("slug") or "").strip()
                    website = (data.get("website") or "").strip()

                    if not website and slug:
                        website = f"{self.BASE_URL}/posts/{slug}"

                    contacts.append(ScrapedContact(
                        name=name,
                        company=name,
                        website=website,
                        bio=tagline or f"Product on Product Hunt",
                        source_category="startups",
                        raw_data={"slug": slug},
                    ))
            for value in data.values():
                if isinstance(value, (dict, list)):
                    self._extract_from_json(value, contacts, depth + 1)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self._extract_from_json(item, contacts, depth + 1)
