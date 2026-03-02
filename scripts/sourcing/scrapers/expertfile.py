"""
ExpertFile expert directory scraper.

ExpertFile (expertfile.com) connects journalists and organizations with
subject-matter experts across thousands of topics. Experts have rich
profile pages with contact info, credentials, speaking details, and
media presence.

Strategy:
  1. Browse /find-an-expert to get topic category URLs
  2. For each topic, paginate through listing pages using page_number param
  3. On listing pages, collect expert profile URLs
  4. Fetch individual expert profile pages for full data
  5. Extract name, email, phone, company, website, LinkedIn, bio, topics

URL patterns:
  - Topic listing: https://expertfile.com/find-an-expert/{Topic}
  - Pagination: https://expertfile.com/find-an-expert/{Topic}?page_number={N}
    (page_number=0 and 1 both show page 1; page_number=2 is page 2, etc.)
  - Expert profile: https://expertfile.com/experts/{username}/{name-slug}

Estimated yield: 10,000-30,000 experts (50,000+ topics, many overlapping)
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin, quote

from scripts.sourcing.base import BaseScraper, ScrapedContact


# High-value topic categories for JV partner discovery.
# ExpertFile has 1,295+ categories; we focus on business, leadership,
# technology, and professional expertise categories.
CATEGORIES = [
    "Business",
    "Leadership",
    "Entrepreneurship",
    "Marketing",
    "Management",
    "Innovation",
    "Technology",
    "Finance",
    "Strategy",
    "Economics",
    "Consulting",
    "Sales",
    "Branding",
    "Digital Marketing",
    "Social Media",
    "Public Relations",
    "Venture Capital",
    "Private Equity",
    "Investment",
    "Banking",
    "Accounting",
    "Real Estate",
    "Healthcare",
    "Education",
    "Law",
    "Sustainability",
    "Energy",
    "AI",
    "Artificial Intelligence",
    "Machine Learning",
    "Data Science",
    "Cybersecurity",
    "Cloud Computing",
    "Blockchain",
    "Biotechnology",
    "Psychology",
    "Communication",
    "Negotiation",
    "Human Resources",
    "Organizational Behavior",
    "Supply Chain",
    "Operations",
    "Project Management",
    "Risk Management",
    "Corporate Governance",
    "Ethics",
    "Diversity",
    "Women in Business",
    "Startup",
    "E-Commerce",
    "Retail",
    "Manufacturing",
    "Agriculture",
    "International Business",
    "Trade",
    "Globalization",
    "Public Policy",
    "Government",
    "Nonprofit",
    "Philanthropy",
    "Media",
    "Journalism",
    "Publishing",
    "Coaching",
    "Personal Development",
    "Career",
    "Wellness",
    "Mental Health",
    "Nutrition",
    "Fitness",
    "Environmental Science",
    "Climate Change",
    "Renewable Energy",
    "Transportation",
    "Aviation",
    "Logistics",
    "Insurance",
    "Tax",
    "Mergers and Acquisitions",
    "Intellectual Property",
    "Patent",
    "Copyright",
    "Franchising",
    "Small Business",
    "Family Business",
    "Networking",
    "Public Speaking",
    "Writing",
    "Content Marketing",
    "SEO",
    "Analytics",
    "Big Data",
    "Internet of Things",
    "Robotics",
    "Nanotechnology",
    "Genetics",
    "Pharmaceutical",
    "Medical Devices",
    "Telemedicine",
    "Nursing",
    "Aging",
]

# Maximum pages to scan per category (10 experts per page)
MAX_PAGES_PER_CATEGORY = 50


class Scraper(BaseScraper):
    SOURCE_NAME = "expertfile"
    BASE_URL = "https://expertfile.com"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_usernames: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield paginated topic listing URLs.

        ExpertFile uses page_number param for pagination:
          - page_number=0 or 1 -> page 1
          - page_number=2 -> page 2
          - page_number=N -> page N
        Each page shows 10 experts.
        """
        for category in CATEGORIES:
            # URL-encode category name (handles spaces in names)
            encoded = quote(category, safe="")
            # Page 1 (no page_number param needed)
            yield f"{self.BASE_URL}/find-an-expert/{encoded}"
            # Pages 2+
            for page_num in range(2, MAX_PAGES_PER_CATEGORY + 1):
                yield f"{self.BASE_URL}/find-an-expert/{encoded}?page_number={page_num}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse expert listing page and fetch individual profile pages.

        Listing pages contain expert cards with links to /experts/{username}/{slug}.
        Each card shows name, title, company, and a brief tagline. We follow
        the profile link to get full data including email, phone, and bio.
        """
        soup = self.parse_html(html)
        contacts = []

        # Find expert profile links
        profile_links: dict[str, str] = {}  # url -> username (for dedup)
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            match = re.search(r"/experts/([^/]+)/([^/\"?#]+)", href)
            if match:
                username = match.group(1)
                if username not in self._seen_usernames:
                    self._seen_usernames.add(username)
                    full_url = urljoin(self.BASE_URL, href)
                    profile_links[full_url] = username

        if not profile_links:
            # No experts found on this page - likely past last page
            return []

        # Fetch each expert's profile page
        for profile_url in profile_links:
            profile_html = self.fetch_page(profile_url)
            if not profile_html:
                continue

            contact = self._parse_profile(profile_url, profile_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Extract expert data from a profile page.

        ExpertFile profiles are rich and include:
          - Name, job title, company, location
          - Email and phone (often publicly listed)
          - Detailed bio / professional summary
          - Areas of expertise (topics)
          - Social links (LinkedIn, Twitter)
          - Speaking engagement details and fee range
          - Education and awards
          - Media appearances and publications
        """
        soup = self.parse_html(html)

        # Name -- try multiple locations
        name = ""

        # Look for JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string or ""
            if '"@type"' in script_text and '"Person"' in script_text:
                name_match = re.search(r'"name"\s*:\s*"([^"]+)"', script_text)
                if name_match:
                    name = name_match.group(1)
                break

        # Fallback to h1
        if not name:
            h1 = soup.find("h1")
            if h1:
                name = h1.get_text(strip=True)

        # Fallback to og:title
        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = (og_title.get("content") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        # Job title and company
        job_title = ""
        company = ""

        # Look for structured data with job info
        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string or ""
            if '"jobTitle"' in script_text:
                jt_match = re.search(r'"jobTitle"\s*:\s*"([^"]+)"', script_text)
                if jt_match:
                    job_title = jt_match.group(1)
            if '"worksFor"' in script_text:
                wf_match = re.search(r'"worksFor"[^}]*"name"\s*:\s*"([^"]+)"', script_text)
                if wf_match:
                    company = wf_match.group(1)

        # Fallback: look for job title + company in the profile header
        # ExpertFile puts these in a <p data-testid="user-job-org"> after <h1>,
        # with company in a <span data-testid="user-org">
        if not company or not job_title:
            h1 = soup.find("h1")
            if h1:
                # Try data-testid first (most reliable)
                title_p = h1.find_next("p", attrs={"data-testid": "user-job-org"})
                if not title_p:
                    # Fallback: first <p> sibling with font-bold class
                    title_p = h1.find_next("p")

                if title_p:
                    # Company is in <span data-testid="user-org">
                    org_span = title_p.find("span", attrs={"data-testid": "user-org"})
                    if org_span and not company:
                        company = org_span.get_text(strip=True)[:100]

                    # If no data-testid, try any non-empty span
                    if not company:
                        for span in title_p.find_all("span"):
                            text = span.get_text(strip=True)
                            if text and len(text) > 2:
                                company = text[:100]
                                break

                    if not job_title:
                        # Job title is the text content before the company span
                        full_text = title_p.get_text(strip=True)
                        if company and company in full_text:
                            job_title = full_text.replace(company, "").strip()[:100]
                        elif full_text:
                            job_title = full_text[:100]

        # Fallback: look for company-related class elements
        if not company:
            company_el = soup.find(
                class_=re.compile(r"company|organization|employer|institution", re.I)
            )
            if company_el:
                company = company_el.get_text(strip=True)[:100]

        # Email
        email = ""
        emails = self.extract_emails(html)
        for e in emails:
            e_lower = e.lower()
            if (
                "expertfile.com" not in e_lower
                and "sentry.io" not in e_lower
                and "example.com" not in e_lower
            ):
                email = e
                break

        # Also check for mailto: links
        if not email:
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                if href.startswith("mailto:"):
                    candidate = href.replace("mailto:", "").split("?")[0].strip()
                    if candidate and "expertfile.com" not in candidate.lower():
                        email = candidate
                        break

        # Phone
        phone = ""
        phone_match = re.search(
            r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}(?:\s*(?:x|ext\.?)\s*\d+)?",
            html,
        )
        if phone_match:
            phone = phone_match.group(0).strip()

        # Also check for tel: links
        if not phone:
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                if href.startswith("tel:"):
                    phone = href.replace("tel:", "").strip()
                    break

        # Website
        website = ""
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            href_lower = href.lower()
            # Look for personal/company website links
            if (
                href.startswith("http")
                and "expertfile.com" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "linkedin.com" not in href_lower
                and "instagram.com" not in href_lower
                and "youtube.com" not in href_lower
                and "google.com" not in href_lower
                and "mailto:" not in href_lower
                and "apple.com" not in href_lower
                and "play.google.com" not in href_lower
                and "schema.org" not in href_lower
                and not website
            ):
                website = href

        # LinkedIn
        linkedin = ""
        linkedin_match = re.search(
            r"https?://(?:www\.)?linkedin\.com/(?:in|profile)/[a-zA-Z0-9_\-]+/?",
            html,
        )
        if linkedin_match:
            linkedin = linkedin_match.group(0)

        # Bio -- look for professional summary
        bio = ""

        # Try og:description first
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            bio = (og_desc.get("content") or "").strip()

        # Try meta description
        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = (meta_desc.get("content") or "").strip()

        # Try to get more detailed bio from the page content
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if len(text) > 150 and (
                name.split()[0] in text
                or "expertise" in text.lower()
                or "experience" in text.lower()
                or "professor" in text.lower()
                or "director" in text.lower()
            ):
                bio = text[:1000]
                break

        # Location
        location = ""
        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string or ""
            if '"addressLocality"' in script_text:
                loc_parts = []
                city_match = re.search(r'"addressLocality"\s*:\s*"([^"]+)"', script_text)
                state_match = re.search(r'"addressRegion"\s*:\s*"([^"]+)"', script_text)
                if city_match:
                    loc_parts.append(city_match.group(1))
                if state_match:
                    loc_parts.append(state_match.group(1))
                if loc_parts:
                    location = ", ".join(loc_parts)
                break

        # Topics / areas of expertise
        topics = []
        topic_els = soup.find_all(
            class_=re.compile(r"topic|expertise|tag|skill|specialty", re.I)
        )
        for el in topic_els:
            text = el.get_text(strip=True)
            if text and len(text) < 60 and text not in topics:
                topics.append(text)

        # Build enriched bio
        bio_parts = []
        if job_title:
            bio_parts.append(f"{job_title}")
        if company:
            bio_parts.append(f"at {company}")
        if bio_parts:
            bio_header = " ".join(bio_parts)
            if bio:
                bio = f"{bio_header}. {bio}"
            else:
                bio = bio_header
        if location:
            bio = f"{bio} | Location: {location}" if bio else f"Location: {location}"
        if topics:
            topic_str = ", ".join(topics[:10])
            bio = f"{bio} | Expertise: {topic_str}" if bio else f"Expertise: {topic_str}"

        return ScrapedContact(
            name=name,
            email=email,
            company=company,
            website=website,
            linkedin=linkedin,
            phone=phone,
            bio=bio[:2000],
            source_url=url,
            source_category="experts",
            raw_data={
                "job_title": job_title,
                "location": location,
                "topics": topics[:15],
            },
        )
