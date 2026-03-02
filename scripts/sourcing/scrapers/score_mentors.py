"""
SCORE.org mentor directory scraper.

SCORE (Service Corps of Retired Executives) is an SBA-partnered
nonprofit with thousands of volunteer mentors across the US.

Uses SCORE's mentor search API at:
  https://www.score.org/find-a-mentor

The search form submits to a JSON endpoint that returns mentor
profiles by location and industry. We iterate through US zip codes
and industry categories to discover mentors.

Estimated yield: 5,000-10,000 mentors
"""

from __future__ import annotations

import re
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Major US metro zip codes for geographic coverage
# Each zip covers a wide metro area in the SCORE search radius
METRO_ZIPS = [
    "10001",  # New York, NY
    "90001",  # Los Angeles, CA
    "60601",  # Chicago, IL
    "77001",  # Houston, TX
    "85001",  # Phoenix, AZ
    "19101",  # Philadelphia, PA
    "78201",  # San Antonio, TX
    "92101",  # San Diego, CA
    "75201",  # Dallas, TX
    "95101",  # San Jose, CA
    "78701",  # Austin, TX
    "32099",  # Jacksonville, FL
    "46201",  # Indianapolis, IN
    "94101",  # San Francisco, CA
    "43201",  # Columbus, OH
    "28201",  # Charlotte, NC
    "76101",  # Fort Worth, TX
    "48201",  # Detroit, MI
    "79901",  # El Paso, TX
    "98101",  # Seattle, WA
    "80201",  # Denver, CO
    "20001",  # Washington, DC
    "37201",  # Nashville, TN
    "73101",  # Oklahoma City, OK
    "33101",  # Miami, FL
    "97201",  # Portland, OR
    "89101",  # Las Vegas, NV
    "38101",  # Memphis, TN
    "40201",  # Louisville, KY
    "21201",  # Baltimore, MD
    "53201",  # Milwaukee, WI
    "87101",  # Albuquerque, NM
    "85701",  # Tucson, AZ
    "93701",  # Fresno, CA
    "95801",  # Sacramento, CA
    "64101",  # Kansas City, MO
    "80901",  # Colorado Springs, CO
    "30301",  # Atlanta, GA
    "68101",  # Omaha, NE
    "27601",  # Raleigh, NC
    "23219",  # Richmond, VA
    "55401",  # Minneapolis, MN
    "33601",  # Tampa, FL
    "70112",  # New Orleans, LA
    "44101",  # Cleveland, OH
    "45201",  # Cincinnati, OH
    "15201",  # Pittsburgh, PA
    "32801",  # Orlando, FL
    "63101",  # St. Louis, MO
    "02101",  # Boston, MA
    "84101",  # Salt Lake City, UT
    "96801",  # Honolulu, HI
    "99501",  # Anchorage, AK
]

# SCORE industry categories
INDUSTRIES = [
    "Accounting",
    "Advertising & Marketing",
    "Agriculture",
    "Arts & Entertainment",
    "Automotive",
    "Business Services",
    "Construction",
    "Consulting",
    "Education",
    "Engineering",
    "Finance & Insurance",
    "Food & Beverage",
    "Government",
    "Health Care",
    "Hospitality",
    "Information Technology",
    "Legal",
    "Manufacturing",
    "Nonprofit",
    "Real Estate",
    "Retail",
    "Technology",
    "Transportation",
]


class Scraper(BaseScraper):
    SOURCE_NAME = "score_mentors"
    BASE_URL = "https://www.score.org"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield SCORE chapter listing URLs by state and mentor search pages."""
        # Strategy 1: Scrape the find-a-mentor page which lists chapters
        yield f"{self.BASE_URL}/find-a-mentor"

        # Strategy 2: Chapter pages by major metros
        # SCORE chapter URLs follow pattern: /chapter/{chapter-name}
        # We'll discover these from the main page and listing pages
        for zip_code in METRO_ZIPS:
            yield f"{self.BASE_URL}/find-a-mentor?zip={zip_code}&distance=50"

        # Strategy 3: Sitemap for mentor/volunteer profile pages
        yield f"{self.BASE_URL}/sitemap.xml"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse SCORE pages for mentor and chapter data."""
        if url.endswith("sitemap.xml"):
            return self._parse_sitemap(html)

        soup = self.parse_html(html)
        contacts = []

        # Look for mentor profile cards / listings
        # SCORE mentor listings typically have cards with name, expertise, location
        for card in soup.find_all(class_=re.compile(
            r"mentor|volunteer|profile|card|listing|result", re.I
        )):
            contact = self._parse_mentor_card(card, url)
            if contact:
                contacts.append(contact)

        # Look for chapter listings (chapters are organizations, good JV targets)
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True)

            # Chapter page links
            if "/chapter/" in href and text and len(text) > 3:
                chapter_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                chapter_html = self.fetch_page(chapter_url)
                if chapter_html:
                    chapter_contacts = self._parse_chapter_page(chapter_url, chapter_html)
                    contacts.extend(chapter_contacts)

            # Direct mentor profile links
            elif "/mentor/" in href or "/mentors/" in href:
                mentor_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                mentor_html = self.fetch_page(mentor_url)
                if mentor_html:
                    contact = self._parse_mentor_profile(mentor_url, mentor_html)
                    if contact:
                        contacts.append(contact)

        return contacts

    def _parse_sitemap(self, xml_text: str) -> list[ScrapedContact]:
        """Extract mentor/chapter URLs from sitemap and fetch profiles."""
        contacts = []
        # Find URLs matching mentor or chapter patterns
        urls = re.findall(r"<loc>(https?://[^<]+)</loc>", xml_text)

        mentor_urls = [u for u in urls if "/mentor/" in u or "/mentors/" in u]
        chapter_urls = [u for u in urls if "/chapter/" in u]

        self.logger.info(
            "Sitemap: found %d mentor URLs, %d chapter URLs",
            len(mentor_urls), len(chapter_urls),
        )

        for mentor_url in mentor_urls[:500]:  # Cap to avoid overload
            html = self.fetch_page(mentor_url)
            if html:
                contact = self._parse_mentor_profile(mentor_url, html)
                if contact:
                    contacts.append(contact)

        for chapter_url in chapter_urls[:200]:
            html = self.fetch_page(chapter_url)
            if html:
                chapter_contacts = self._parse_chapter_page(chapter_url, html)
                contacts.extend(chapter_contacts)

        return contacts

    def _parse_mentor_card(self, card, source_url: str) -> ScrapedContact | None:
        """Parse a mentor listing card element."""
        name = ""
        bio = ""
        website = ""

        # Try to find name in heading tags
        for tag in ["h2", "h3", "h4", "strong", "a"]:
            el = card.find(tag)
            if el:
                name = el.get_text(strip=True)
                if name and len(name) > 2 and len(name) < 100:
                    break
                name = ""

        if not name:
            return None

        name_key = name.lower().strip()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # Extract bio / expertise from card text
        card_text = card.get_text(separator=" | ", strip=True)
        if card_text and len(card_text) > len(name):
            bio = card_text[:500]

        # Look for links
        for a in card.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("http") and "score.org" not in href.lower():
                website = href
                break

        linkedin = self.extract_linkedin(str(card))
        emails = self.extract_emails(str(card))
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            company="SCORE",
            website=website,
            linkedin=linkedin,
            phone="",
            bio=f"SCORE Mentor | {bio}" if bio else "SCORE Volunteer Mentor | SBA Partner",
            source_url=source_url,
            source_category="mentoring",
        )

    def _parse_mentor_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Parse a full mentor profile page."""
        soup = self.parse_html(html)

        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = (og.get("content") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower().strip()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # Bio from meta description or page content
        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = (meta_desc.get("content") or "")[:500]

        if not bio:
            # Look for expertise/about section
            for section in soup.find_all(class_=re.compile(r"bio|about|expertise|description", re.I)):
                text = section.get_text(strip=True)
                if text and len(text) > 20:
                    bio = text[:500]
                    break

        emails = self.extract_emails(html)
        email = emails[0] if emails else ""
        linkedin = self.extract_linkedin(html)

        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if (href.startswith("http")
                    and "score.org" not in href.lower()
                    and "linkedin.com" not in href.lower()
                    and "facebook.com" not in href.lower()
                    and "twitter.com" not in href.lower()):
                website = href
                break

        return ScrapedContact(
            name=name,
            email=email,
            company="SCORE",
            website=website or url,
            linkedin=linkedin,
            phone="",
            bio=f"SCORE Mentor | {bio}" if bio else "SCORE Volunteer Mentor | SBA Partner",
            source_url=url,
            source_category="mentoring",
        )

    def _parse_chapter_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a SCORE chapter page for chapter info and any listed mentors."""
        soup = self.parse_html(html)
        contacts = []

        # Get chapter name
        chapter_name = ""
        h1 = soup.find("h1")
        if h1:
            chapter_name = h1.get_text(strip=True)

        if not chapter_name:
            return []

        name_key = chapter_name.lower().strip()
        if name_key in self._seen_names:
            return []
        self._seen_names.add(name_key)

        # Chapter contact info
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        phone = ""
        phone_match = re.search(
            r"(?:1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}", html
        )
        if phone_match:
            phone = phone_match.group(0)

        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if (href.startswith("http")
                    and "score.org" not in href.lower()
                    and "facebook.com" not in href.lower()
                    and "twitter.com" not in href.lower()
                    and "linkedin.com" not in href.lower()):
                website = href
                break

        contacts.append(ScrapedContact(
            name=chapter_name,
            email=email,
            company="SCORE",
            website=website or url,
            linkedin="",
            phone=phone,
            bio=f"SCORE Chapter | Free business mentoring | SBA Partner",
            source_url=url,
            source_category="mentoring",
        ))

        return contacts
