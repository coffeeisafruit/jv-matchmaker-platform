"""
Wikidata SPARQL scraper — notable speakers, authors, coaches, influencers.

Wikidata is the structured data backbone of Wikipedia with 100M+ items.
We use SPARQL queries to extract humans with JV-relevant occupations
who have websites, social media, or other contact signals.

No auth required. JSON responses via SPARQL endpoint.

Estimated yield: 10,000-30,000+ unique individuals
"""

from __future__ import annotations

import json
from typing import Iterator
from urllib.parse import urlencode, quote

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Wikidata occupation QIDs and their labels
# Each maps to P106 (occupation) values
# Query ONE at a time to avoid SPARQL timeouts on broad categories
OCCUPATIONS = [
    # Speakers / influencers (small, targeted)
    ("Q18939491", "motivational speaker"),
    ("Q16533", "public speaker"),  # orator
    # Authors (targeted subcategories)
    ("Q5482740", "self-help author"),
    ("Q11774202", "blogger"),
    ("Q15077007", "podcaster"),
    # Coaches
    ("Q806798", "life coach"),
    ("Q3400985", "coach"),
    # Consultants / business
    ("Q845392", "consultant"),
    ("Q131524", "entrepreneur"),
    # Wellness / health (smaller categories)
    ("Q2640827", "nutritionist"),
    ("Q774306", "psychotherapist"),
    ("Q203234", "naturopath"),
    ("Q162606", "chiropractor"),
    # Media / content
    ("Q245068", "television presenter"),
    ("Q947873", "television producer"),
]

# Query one occupation at a time to avoid SPARQL timeouts
BATCH_SIZE = 1


class Scraper(BaseScraper):
    SOURCE_NAME = "wikidata"
    BASE_URL = "https://query.wikidata.org"
    REQUESTS_PER_MINUTE = 5  # Be conservative with SPARQL service

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()
        # Wikidata requires proper User-Agent
        self.session.headers.update({
            "User-Agent": "JVMatchmaker/1.0 (joe@jvmatches.com)",
            "Accept": "application/sparql-results+json",
        })

    def _build_sparql_query(self, occupation_qids: list[str]) -> str:
        """Build SPARQL query for a batch of occupation QIDs.
        Requires website (P856) to ensure useful contact signals."""
        values = " ".join(f"wd:{qid}" for qid in occupation_qids)
        return f"""
SELECT DISTINCT ?person ?personLabel ?website ?description WHERE {{
  VALUES ?occupation {{ {values} }}
  ?person wdt:P31 wd:Q5 ;
          wdt:P106 ?occupation ;
          wdt:P856 ?website .
  OPTIONAL {{ ?person schema:description ?description .
              FILTER(LANG(?description) = "en") }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT 10000
"""

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield SPARQL query URLs for batches of occupations."""
        for i in range(0, len(OCCUPATIONS), BATCH_SIZE):
            batch = OCCUPATIONS[i:i + BATCH_SIZE]
            qids = [qid for qid, _ in batch]
            labels = [label for _, label in batch]
            sparql = self._build_sparql_query(qids)
            params = urlencode({"query": sparql, "format": "json"})
            yield f"{self.BASE_URL}/sparql?{params}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse SPARQL JSON results."""
        try:
            data = json.loads(html)
        except (json.JSONDecodeError, TypeError):
            return []

        contacts = []
        results = data.get("results", {}).get("bindings", [])

        for result in results:
            # Extract Wikidata entity ID
            person_uri = result.get("person", {}).get("value", "")
            if not person_uri:
                continue
            entity_id = person_uri.split("/")[-1]  # e.g., Q12345

            if entity_id in self._seen_ids:
                continue
            self._seen_ids.add(entity_id)

            # Name
            name = result.get("personLabel", {}).get("value", "")
            if not name or len(name) < 2 or len(name) > 150:
                continue

            # Skip if name looks like a QID (unresolved label)
            if name.startswith("Q") and name[1:].isdigit():
                continue

            # Website
            website = result.get("website", {}).get("value", "")

            # Description/bio
            description = result.get("description", {}).get("value", "")
            bio = description[:500] if description else ""

            # Wikidata URL as fallback website
            wikidata_url = f"https://www.wikidata.org/wiki/{entity_id}"

            contacts.append(ScrapedContact(
                name=name,
                website=website or wikidata_url,
                bio=bio,
                source_category="notable_person",
                raw_data={
                    "wikidata_id": entity_id,
                },
            ))

        return contacts
