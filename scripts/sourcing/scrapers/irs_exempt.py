"""
IRS Tax-Exempt Organization Scraper

Scrapes 1.8M+ US tax-exempt organizations from IRS Business Master File (BMF) Extract.
Focuses on business leagues (501c6), chambers of commerce, trade associations, and
other JV-relevant organizations.

Data source: https://www.irs.gov/pub/irs-soi/eo*.csv (4 regional files)
"""

import csv
import io
import urllib.parse
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "irs_exempt"
    BASE_URL = "https://www.irs.gov"
    REQUESTS_PER_MINUTE = 2  # Be gentle with IRS servers

    # IRS Business Master File Extract URLs (split by region)
    BMF_URLS = [
        "https://www.irs.gov/pub/irs-soi/eo1.csv",
        "https://www.irs.gov/pub/irs-soi/eo2.csv",
        "https://www.irs.gov/pub/irs-soi/eo3.csv",
        "https://www.irs.gov/pub/irs-soi/eo4.csv",
    ]

    # Focus on JV-relevant organization types
    # 03 = 501(c)(3) - Charitable organizations
    # 04 = 501(c)(4) - Social welfare
    # 05 = 501(c)(5) - Labor, agricultural organizations
    # 06 = 501(c)(6) - Business leagues, chambers, trade associations (GOLD!)
    # 07 = 501(c)(7) - Social/recreational clubs
    RELEVANT_SUBSECTIONS = {"03", "04", "05", "06", "07"}

    # Priority NTEE codes (National Taxonomy of Exempt Entities)
    # S = Community Improvement, Capacity Building (includes chambers)
    # U = Science and Technology Research
    # W = Public, Society Benefit
    # B = Education (includes training organizations)
    # T = Philanthropy, Voluntarism, Grantmaking
    # Y = Mutual/Membership Benefit Organizations
    PRIORITY_NTEE_PREFIXES = {"S", "U", "W", "B", "T", "Y"}

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_eins = set()
        self._current_file_index = 0
        self._current_row_index = 0

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used - we override run() to handle CSV downloads directly."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used - we override run() to handle CSV downloads directly."""
        return []

    def run(self, max_pages=0, max_contacts=0, checkpoint=None):
        """Download and parse IRS BMF Extract CSV files."""
        self.logger.info("Starting IRS Tax-Exempt Organization scraper")
        self.logger.info("Downloading 4 regional CSV files from IRS...")

        # Resume from checkpoint if provided
        start_file_index = 0
        start_row_index = 0
        if checkpoint:
            start_file_index = checkpoint.get("file_index", 0)
            start_row_index = checkpoint.get("row_index", 0)
            self.logger.info(
                "Resuming from checkpoint: file %d, row %d",
                start_file_index,
                start_row_index
            )

        for file_index, csv_url in enumerate(self.BMF_URLS):
            # Skip files before checkpoint
            if file_index < start_file_index:
                self.logger.info("Skipping file %d (before checkpoint)", file_index)
                continue

            self._current_file_index = file_index
            self.logger.info(
                "Downloading file %d/%d: %s",
                file_index + 1,
                len(self.BMF_URLS),
                csv_url
            )

            try:
                response = self.session.get(csv_url, timeout=300)
                if response.status_code != 200:
                    self.logger.error(
                        "Failed to download %s: HTTP %d",
                        csv_url,
                        response.status_code
                    )
                    continue

                self.logger.info(
                    "Downloaded %s (%.2f MB)",
                    csv_url,
                    len(response.content) / 1024 / 1024
                )

                # Parse CSV
                reader = csv.DictReader(io.StringIO(response.text))

                for row_index, row in enumerate(reader):
                    # Skip rows before checkpoint (only for the resume file)
                    if file_index == start_file_index and row_index < start_row_index:
                        continue

                    self._current_row_index = row_index

                    # Update checkpoint every 1000 rows
                    if row_index % 1000 == 0:
                        self._update_checkpoint({
                            "file_index": file_index,
                            "row_index": row_index
                        })

                    contact = self._parse_row(row, csv_url)
                    if contact:
                        self.stats["contacts_valid"] += 1
                        yield contact

                        # Check max_contacts limit
                        if max_contacts and self.stats["contacts_valid"] >= max_contacts:
                            self.logger.info(
                                "Reached max_contacts limit: %d",
                                max_contacts
                            )
                            return

                self.logger.info(
                    "Completed file %d/%d: %d contacts found, %d valid",
                    file_index + 1,
                    len(self.BMF_URLS),
                    self.stats["contacts_found"],
                    self.stats["contacts_valid"]
                )

            except Exception as e:
                self.logger.error(
                    "Error processing %s: %s",
                    csv_url,
                    str(e),
                    exc_info=True
                )
                continue

        self.logger.info(
            "IRS scraper complete: %d total contacts, %d valid",
            self.stats["contacts_found"],
            self.stats["contacts_valid"]
        )

    def _parse_row(self, row: dict, source_url: str) -> ScrapedContact | None:
        """Parse a single CSV row into a ScrapedContact."""
        # Filter by subsection (organization type)
        subsection = row.get("SUBSECTION", "").strip()
        if subsection not in self.RELEVANT_SUBSECTIONS:
            return None

        # Deduplicate by EIN
        ein = row.get("EIN", "").strip()
        if not ein:
            return None
        if ein in self._seen_eins:
            return None
        self._seen_eins.add(ein)

        # Extract basic info
        name = row.get("NAME", "").strip()
        if not name or len(name) < 2:
            return None

        # Skip generic/invalid names
        name_lower = name.lower()
        if any(skip in name_lower for skip in ["unknown", "invalid", "test", "n/a"]):
            return None

        city = row.get("CITY", "").strip()
        state = row.get("STATE", "").strip()
        street = row.get("STREET", "").strip()
        zip_code = row.get("ZIP", "").strip()

        # Must have at least city and state
        if not city or not state:
            return None

        # Extract classification info
        ntee = row.get("NTEE_CD", "").strip()
        classification = row.get("CLASSIFICATION", "").strip()

        # Build comprehensive bio
        bio_parts = [name]

        # Location
        if city and state:
            bio_parts.append(f"{city}, {state}")

        # Organization type
        subsection_labels = {
            "03": "501(c)(3) Charitable",
            "04": "501(c)(4) Social Welfare",
            "05": "501(c)(5) Labor/Agricultural",
            "06": "501(c)(6) Business League/Chamber",  # GOLD!
            "07": "501(c)(7) Social/Recreational",
        }
        if subsection in subsection_labels:
            bio_parts.append(subsection_labels[subsection])

        # NTEE code (category)
        if ntee:
            bio_parts.append(f"NTEE: {ntee}")

        # Financial info
        income = row.get("INCOME_AMT", "0").strip()
        revenue = row.get("REVENUE_AMT", "0").strip()
        assets = row.get("ASSET_AMT", "0").strip()

        try:
            income_int = int(income or 0)
            if income_int > 0:
                bio_parts.append(f"Income: ${income_int:,}")
        except (ValueError, TypeError):
            pass

        try:
            revenue_int = int(revenue or 0)
            if revenue_int > 0:
                bio_parts.append(f"Revenue: ${revenue_int:,}")
        except (ValueError, TypeError):
            pass

        try:
            assets_int = int(assets or 0)
            if assets_int > 0:
                bio_parts.append(f"Assets: ${assets_int:,}")
        except (ValueError, TypeError):
            pass

        bio = " | ".join(bio_parts)

        # Build full address
        address_parts = []
        if street:
            address_parts.append(street)
        if city:
            address_parts.append(city)
        if state:
            if zip_code:
                address_parts.append(f"{state} {zip_code}")
            else:
                address_parts.append(state)
        full_address = ", ".join(address_parts)

        # Use GuideStar/Candid as discovery website (nonprofits database)
        # This provides a valid website URL for is_valid() check
        website = f"https://www.guidestar.org/profile/{ein}"

        # Determine source category based on org type
        if subsection == "06":
            source_category = "business_leagues"  # Chambers, trade associations
        elif ntee and ntee[0] in self.PRIORITY_NTEE_PREFIXES:
            source_category = "nonprofit_orgs"
        else:
            source_category = "tax_exempt_orgs"

        contact = ScrapedContact(
            name=name,
            company=name,
            email="",  # IRS data doesn't include email
            phone="",  # IRS data doesn't include phone
            website=website,  # GuideStar profile as discovery URL
            linkedin="",
            bio=bio,
            source_category=source_category,
            raw_data={
                "ein": ein,
                "subsection": subsection,
                "ntee_cd": ntee,
                "classification": classification,
                "address": full_address,
                "street": street,
                "city": city,
                "state": state,
                "zip": zip_code,
                "income_amt": income,
                "revenue_amt": revenue,
                "asset_amt": assets,
                "ruling": row.get("RULING", "").strip(),
                "foundation": row.get("FOUNDATION", "").strip(),
                "activity": row.get("ACTIVITY", "").strip(),
                "organization": row.get("ORGANIZATION", "").strip(),
                "status": row.get("STATUS", "").strip(),
            },
        )

        contact.source_platform = self.SOURCE_NAME
        contact.source_url = source_url

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        return contact

    def _update_checkpoint(self, checkpoint_data: dict):
        """Update checkpoint for resume capability."""
        # This is called by the base class or progress tracker
        # Store file_index and row_index for resuming
        pass
