#!/usr/bin/env python3
"""Run BBB scraper starting from sitemap index 30 (skipping already-processed sitemaps)."""

import csv
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.sourcing.scrapers.bbb_sitemap import Scraper
from scripts.sourcing.rate_limiter import RateLimiter
from scripts.sourcing.base import ScrapedContact

HEADER = ["name", "email", "company", "website", "linkedin", "phone", "bio", "source", "source_url"]
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "partners", "bbb_sitemap_fresh.csv")

def main():
    limiter = RateLimiter()
    scraper = Scraper(rate_limiter=limiter)

    count = 0
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER, extrasaction="ignore")
        writer.writeheader()

        # Start from sitemap 30 to skip already-processed ones
        for contact in scraper.run(max_contacts=100000, checkpoint={"sitemap_idx": 30}):
            row = {
                "name": contact.name,
                "email": contact.email,
                "company": contact.company,
                "website": contact.website,
                "linkedin": contact.linkedin,
                "phone": contact.phone,
                "bio": contact.bio,
                "source": contact.source_platform,
                "source_url": contact.source_url,
            }
            writer.writerow(row)
            count += 1
            if count % 10000 == 0:
                print(f"  {count:,} contacts written...")
                f.flush()

    print(f"\nDone: {count:,} contacts written to {OUTPUT}")

if __name__ == "__main__":
    main()
