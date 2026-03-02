#!/usr/bin/env python3
"""Run BBB scraper from a given sitemap checkpoint. Usage: python3 run_bbb_batch.py START_IDX MAX_CONTACTS OUTPUT_SUFFIX"""

import csv
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.sourcing.scrapers.bbb_sitemap import Scraper
from scripts.sourcing.rate_limiter import RateLimiter

HEADER = ["name", "email", "company", "website", "linkedin", "phone", "bio", "source", "source_url"]

def main():
    start_idx = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    max_contacts = int(sys.argv[2]) if len(sys.argv) > 2 else 100000
    suffix = sys.argv[3] if len(sys.argv) > 3 else f"s{start_idx}"

    output = os.path.join(os.path.dirname(os.path.abspath(__file__)), "partners", f"bbb_sitemap_{suffix}.csv")
    print(f"BBB batch: sitemap_idx={start_idx}, max={max_contacts}, output={output}")

    limiter = RateLimiter()
    scraper = Scraper(rate_limiter=limiter)

    count = 0
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER, extrasaction="ignore")
        writer.writeheader()

        for contact in scraper.run(max_contacts=max_contacts, checkpoint={"sitemap_idx": start_idx}):
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

    print(f"\nDone: {count:,} contacts written to {output}")

if __name__ == "__main__":
    main()
