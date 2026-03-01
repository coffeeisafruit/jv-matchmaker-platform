"""
US Chambers of Commerce scraper.

Multi-source scraper targeting state and local chambers of commerce
across all 50 states + DC. Chambers are high-value prospects for
B2B partnerships, event sponsorships, and professional networking.

Primary source: regionaldirectory.us (state-level chamber directories)
Estimated yield: 3,000-8,000 chambers nationwide

CSV output: Filling Database/chambers/{state}.csv
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


# State abbreviation -> URL slug mapping
US_STATES = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas",
    "CA": "california", "CO": "colorado", "CT": "connecticut", "DE": "delaware",
    "FL": "florida", "GA": "georgia", "HI": "hawaii", "ID": "idaho",
    "IL": "illinois", "IN": "indiana", "IA": "iowa", "KS": "kansas",
    "KY": "kentucky", "LA": "louisiana", "ME": "maine", "MD": "maryland",
    "MA": "massachusetts", "MI": "michigan", "MN": "minnesota", "MS": "mississippi",
    "MO": "missouri", "MT": "montana", "NE": "nebraska", "NV": "nevada",
    "NH": "new-hampshire", "NJ": "new-jersey", "NM": "new-mexico", "NY": "new-york",
    "NC": "north-carolina", "ND": "north-dakota", "OH": "ohio", "OK": "oklahoma",
    "OR": "oregon", "PA": "pennsylvania", "RI": "rhode-island", "SC": "south-carolina",
    "SD": "south-dakota", "TN": "tennessee", "TX": "texas", "UT": "utah",
    "VT": "vermont", "VA": "virginia", "WA": "washington", "WV": "west-virginia",
    "WI": "wisconsin", "WY": "wyoming", "DC": "district-of-columbia",
}

# Reverse mapping for lookups
STATE_SLUG_TO_CODE = {slug: code for code, slug in US_STATES.items()}

# Regex for phone number extraction
PHONE_RE = re.compile(
    r"(?:1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}"
)


class Scraper(BaseScraper):
    SOURCE_NAME = "chambers"
    BASE_URL = "https://chambers-of-commerce.regionaldirectory.us"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_chambers: set[str] = set()
        # CSV output tracking
        self._csv_dir = Path(__file__).parent.parent.parent.parent / "Filling Database" / "chambers"
        self._csv_dir.mkdir(parents=True, exist_ok=True)
        self._csv_files: dict[str, tuple] = {}  # state_code -> (file_handle, csv_writer)

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield state page URLs from regionaldirectory.us."""
        for state_code, state_slug in US_STATES.items():
            yield f"{self.BASE_URL}/{state_slug}.htm"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse chamber listings from state directory page."""
        soup = self.parse_html(html)
        contacts = []

        # Extract state code from URL
        state_slug = url.rstrip("/").split("/")[-1].replace(".htm", "")
        state_code = STATE_SLUG_TO_CODE.get(state_slug, "XX")
        state_name = state_slug.replace("-", " ").title()

        # Initialize CSV file for this state
        if state_code not in self._csv_files:
            csv_path = self._csv_dir / f"{state_code.lower()}.csv"
            file_exists = csv_path.exists() and csv_path.stat().st_size > 0
            csv_file = open(csv_path, "a", newline="", encoding="utf-8")
            csv_writer = csv.DictWriter(
                csv_file,
                fieldnames=["name", "company", "website", "phone", "city", "state", "bio", "source_url"],
            )
            if not file_exists:
                csv_writer.writeheader()
            self._csv_files[state_code] = (csv_file, csv_writer)

        # Strategy 1: Find table rows with chamber entries
        # Pattern: <tr> with <td> containing <a href="...">Chamber Name</a>
        #          followed by <td> or <br> with address/phone
        for row in soup.find_all("tr"):
            chamber_data = self._parse_table_row(row, state_code, state_name)
            if chamber_data:
                contacts.append(chamber_data)

        # Strategy 2: If no table structure, look for any <a> tags with chamber-like text
        if not contacts:
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if not href.startswith("http"):
                    continue
                # Skip internal navigation links
                if "regionaldirectory.us" in href.lower():
                    continue

                text = link.get_text(strip=True)
                # Look for "chamber" in the link text
                if "chamber" in text.lower() and len(text) > 5:
                    chamber_data = self._parse_link_based(link, href, state_code, state_name)
                    if chamber_data:
                        contacts.append(chamber_data)

        self.logger.info(f"Found {len(contacts)} chambers in {state_name}")
        return contacts

    def _parse_table_row(self, row, state_code: str, state_name: str) -> ScrapedContact | None:
        """Parse a table row looking for chamber data."""
        cells = row.find_all("td")
        if len(cells) < 1:
            return None

        # Find the first cell with an external link (chamber website)
        chamber_link = None
        chamber_name = ""
        chamber_website = ""

        for cell in cells:
            link = cell.find("a", href=True)
            if link:
                href = link.get("href", "")
                if href.startswith("http") and "regionaldirectory.us" not in href.lower():
                    chamber_link = link
                    chamber_website = href
                    chamber_name = link.get_text(strip=True)
                    break

        if not chamber_name:
            return None

        # Normalize name for deduplication
        normalized_name = self._normalize_chamber_name(chamber_name)
        if normalized_name in self._seen_chambers:
            return None
        self._seen_chambers.add(normalized_name)

        # Extract city, state, zip from the same cell or next cells
        city = ""
        phone = ""

        # Get all text from the row
        row_text = row.get_text(separator=" | ", strip=True)

        # Look for phone number
        phone_match = PHONE_RE.search(row_text)
        if phone_match:
            phone = phone_match.group(0)

        # Extract city from location text (e.g., "Bay Minette, Alabama 36507")
        # Typically follows the chamber name link
        if chamber_link:
            # Get text after the link in the same cell
            cell_text = chamber_link.parent.get_text(strip=True)
            # Remove the chamber name to isolate location
            location_text = cell_text.replace(chamber_name, "", 1).strip()

            # Split by <br> tags to get structured lines
            for br in chamber_link.parent.find_all("br"):
                br.replace_with("\n")
            lines = chamber_link.parent.get_text().split("\n")

            # Look for city line (typically second line after name)
            for line in lines[1:4]:
                line = line.strip()
                # Match "City, State ZIP" pattern
                city_match = re.match(r"^([^,]+),\s*([A-Z]{2}|[A-Za-z\s]+)\s*\d{5}(-\d{4})?$", line)
                if city_match:
                    city = city_match.group(1).strip()
                    break
                # Fallback: just city name before comma
                elif "," in line and len(line) < 50:
                    city = line.split(",")[0].strip()

        # Build bio
        bio_parts = [chamber_name]
        if city:
            bio_parts.append(f"{city}, {state_code}")
        else:
            bio_parts.append(state_name)
        bio_parts.append("Chamber of Commerce")
        bio = " | ".join(bio_parts)

        contact = ScrapedContact(
            name=chamber_name,
            company=chamber_name,
            website=chamber_website,
            phone=phone,
            bio=bio,
            source_category="chambers_of_commerce",
            raw_data={
                "city": city,
                "state": state_code,
                "state_name": state_name,
            },
        )

        # Write to state CSV
        if state_code in self._csv_files:
            _, csv_writer = self._csv_files[state_code]
            csv_writer.writerow({
                "name": chamber_name,
                "company": chamber_name,
                "website": chamber_website,
                "phone": phone,
                "city": city,
                "state": state_code,
                "bio": bio,
                "source_url": chamber_website,
            })

        return contact

    def _parse_link_based(self, link, href: str, state_code: str, state_name: str) -> ScrapedContact | None:
        """Parse chamber data from a link element (fallback strategy)."""
        chamber_name = link.get_text(strip=True)
        chamber_website = href

        # Normalize and deduplicate
        normalized_name = self._normalize_chamber_name(chamber_name)
        if normalized_name in self._seen_chambers:
            return None
        self._seen_chambers.add(normalized_name)

        # Look for phone/location in nearby text
        parent = link.parent
        if parent:
            parent_text = parent.get_text(strip=True)
            phone_match = PHONE_RE.search(parent_text)
            phone = phone_match.group(0) if phone_match else ""
        else:
            phone = ""

        bio = f"{chamber_name} | {state_name} | Chamber of Commerce"

        contact = ScrapedContact(
            name=chamber_name,
            company=chamber_name,
            website=chamber_website,
            phone=phone,
            bio=bio,
            source_category="chambers_of_commerce",
            raw_data={
                "city": "",
                "state": state_code,
                "state_name": state_name,
            },
        )

        # Write to state CSV
        if state_code in self._csv_files:
            _, csv_writer = self._csv_files[state_code]
            csv_writer.writerow({
                "name": chamber_name,
                "company": chamber_name,
                "website": chamber_website,
                "phone": phone,
                "city": "",
                "state": state_code,
                "bio": bio,
                "source_url": chamber_website,
            })

        return contact

    def _normalize_chamber_name(self, name: str) -> str:
        """Normalize chamber name for deduplication."""
        # Lowercase, remove punctuation, collapse whitespace
        normalized = re.sub(r"[^\w\s]", "", name.lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        # Remove common suffixes
        normalized = re.sub(r"\s*(chamber of commerce|chamber|coc)\s*$", "", normalized)
        return normalized

    def __del__(self):
        """Close all open CSV files."""
        for csv_file, _ in self._csv_files.values():
            try:
                csv_file.close()
            except Exception:
                pass
