"""
Django management command to scrape emails from JV Directory member profiles.

This script reads the member profile URLs from the jvdirectory.csv export
and visits each profile page to extract the "Best Way to Contact" field.

Usage:
    1. First, log into jvdirectory.com in your browser
    2. Get your session cookies (see instructions below)
    3. Run: python manage.py scrape_jv_emails --cookies "your_cookie_string"

To get cookies from Chrome:
    1. Log into jvdirectory.com
    2. Open DevTools (F12) -> Application -> Cookies
    3. Copy all cookie values as: "cookie1=value1; cookie2=value2"

Or use --cookie-file to load from a file.
"""

import csv
import re
import time
import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Scrape emails from JV Directory member profile pages'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input',
            type=str,
            default='/Users/josephtepe/Downloads/jvdirectory.csv',
            help='Input CSV file with member URLs'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='jv_directory_with_emails.csv',
            help='Output CSV file'
        )
        parser.add_argument(
            '--cookies',
            type=str,
            help='Session cookies string (e.g., "session=abc123; token=xyz")'
        )
        parser.add_argument(
            '--cookie-file',
            type=str,
            help='File containing cookies (one per line: name=value)'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Limit number of profiles to scrape (0 = all)'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=1.0,
            help='Delay between requests in seconds (default: 1.0)'
        )
        parser.add_argument(
            '--resume',
            type=int,
            default=0,
            help='Resume from row number (skip first N rows)'
        )

    def handle(self, *args, **options):
        input_file = options['input']
        output_file = options['output']
        cookies_str = options['cookies']
        cookie_file = options['cookie_file']
        limit = options['limit']
        delay = options['delay']
        resume_from = options['resume']

        # Parse cookies
        cookies = {}
        if cookie_file:
            with open(cookie_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line:
                        name, value = line.split('=', 1)
                        cookies[name.strip()] = value.strip()
        elif cookies_str:
            for part in cookies_str.split(';'):
                if '=' in part:
                    name, value = part.split('=', 1)
                    cookies[name.strip()] = value.strip()

        if not cookies:
            self.stdout.write(self.style.WARNING(
                '\nNo cookies provided. You need to be logged in to access contact info.\n'
                'Get cookies from your browser after logging into jvdirectory.com\n'
                'Usage: python manage.py scrape_jv_emails --cookies "cookie1=val1; cookie2=val2"\n'
            ))
            return

        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('JV DIRECTORY EMAIL SCRAPER'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}\n'))

        # Read input CSV
        self.stdout.write(f'Reading {input_file}...')
        members = self._read_input_csv(input_file)
        self.stdout.write(f'Found {len(members)} members')

        if resume_from > 0:
            members = members[resume_from:]
            self.stdout.write(f'Resuming from row {resume_from}, {len(members)} remaining')

        if limit > 0:
            members = members[:limit]
            self.stdout.write(f'Limited to {limit} members')

        # Set up session
        session = requests.Session()
        session.cookies.update(cookies)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })

        # Scrape each profile
        results = []
        success_count = 0
        error_count = 0

        for i, member in enumerate(members):
            try:
                self.stdout.write(f'\n[{i+1}/{len(members)}] Scraping: {member["name"]}')

                profile_data = self._scrape_profile(session, member['url'])

                if profile_data:
                    member.update(profile_data)
                    if profile_data.get('email'):
                        success_count += 1
                        self.stdout.write(self.style.SUCCESS(f'  ✓ Found email: {profile_data["email"]}'))
                    else:
                        self.stdout.write(self.style.WARNING(f'  - No email found'))
                else:
                    self.stdout.write(self.style.WARNING(f'  - Could not load profile'))
                    error_count += 1

                results.append(member)

                # Rate limiting
                if i < len(members) - 1:
                    time.sleep(delay)

            except KeyboardInterrupt:
                self.stdout.write(self.style.WARNING(f'\n\nInterrupted! Saving {len(results)} results...'))
                break
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'  Error: {e}'))
                error_count += 1
                results.append(member)

        # Write output
        self._write_output_csv(results, output_file)

        # Summary
        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('COMPLETE'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}'))
        self.stdout.write(f'\nTotal processed: {len(results)}')
        self.stdout.write(f'Emails found: {success_count}')
        self.stdout.write(f'Errors: {error_count}')
        self.stdout.write(f'\nOutput saved to: {output_file}')

    def _read_input_csv(self, filepath):
        """Read the jvdirectory export CSV and extract member info."""
        members = []

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)

            # Find the column indices
            name_idx = 0  # "Name"
            company_idx = 3  # "Company"
            business_focus_idx = 4  # "Business Focus"
            status_idx = 5  # "Status"
            url_idx = 2  # "Link for Mor info"

            for row in reader:
                if len(row) > url_idx:
                    url = row[url_idx] if url_idx < len(row) else ''

                    # Only include rows with valid profile URLs
                    if url and 'jvdirectory.com/member-more-info' in url:
                        members.append({
                            'name': row[name_idx] if name_idx < len(row) else '',
                            'company': row[company_idx] if company_idx < len(row) else '',
                            'business_focus': row[business_focus_idx] if business_focus_idx < len(row) else '',
                            'status': row[status_idx] if status_idx < len(row) else '',
                            'url': url,
                            'email': '',
                            'phone': '',
                            'website': '',
                            'calendar_link': '',
                            'best_way_to_contact': '',
                        })

        return members

    def _scrape_profile(self, session, url):
        """Scrape a single member profile page."""
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            data = {
                'email': '',
                'phone': '',
                'website': '',
                'calendar_link': '',
                'best_way_to_contact': '',
                'list_size': '',
                'social_reach': '',
            }

            # Look for "Best Way to Contact" field
            # The field appears to be a textarea or div with label
            page_text = soup.get_text()

            # Method 1: Find by label text
            for label in soup.find_all(['label', 'span', 'div', 'td']):
                label_text = label.get_text(strip=True).lower()

                if 'best way to contact' in label_text:
                    # Get the next sibling or parent's next element
                    contact_elem = label.find_next(['textarea', 'div', 'td', 'input'])
                    if contact_elem:
                        data['best_way_to_contact'] = contact_elem.get_text(strip=True)
                        break

            # Method 2: Look for the contact box structure from screenshot
            # It appears to be in a form field or text area
            contact_boxes = soup.find_all(['textarea', 'div'], class_=lambda x: x and 'contact' in str(x).lower())
            for box in contact_boxes:
                text = box.get_text(strip=True)
                if '@' in text or 'http' in text:
                    data['best_way_to_contact'] = text
                    break

            # Method 3: Search for email pattern in best_way_to_contact area
            # Look for the specific structure shown in the screenshot
            all_textareas = soup.find_all('textarea')
            for ta in all_textareas:
                text = ta.get_text(strip=True)
                if '@' in text:
                    data['best_way_to_contact'] = text
                    break

            # Method 4: Find any div/span that contains email after "Best Way to Contact" text
            if not data['best_way_to_contact']:
                html_str = str(soup)
                match = re.search(r'Best Way to Contact.*?<(?:textarea|div|td)[^>]*>([^<]+)', html_str, re.IGNORECASE | re.DOTALL)
                if match:
                    data['best_way_to_contact'] = match.group(1).strip()

            # Extract email from best_way_to_contact
            contact_text = data['best_way_to_contact']
            if contact_text:
                # Find email addresses
                email_matches = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', contact_text)
                if email_matches:
                    data['email'] = email_matches[0]

                # Find URLs (calendar links, etc.)
                url_matches = re.findall(r'https?://[^\s<>"\']+', contact_text)
                for url in url_matches:
                    if 'calendar' in url.lower() or 'schedule' in url.lower() or 'calendly' in url.lower():
                        data['calendar_link'] = url
                    elif not data['website']:
                        data['website'] = url

            # Also look for Primary Website field
            for label in soup.find_all(['label', 'span', 'td']):
                if 'primary website' in label.get_text(strip=True).lower():
                    website_elem = label.find_next(['input', 'a', 'td'])
                    if website_elem:
                        if website_elem.name == 'a':
                            data['website'] = website_elem.get('href', '')
                        else:
                            data['website'] = website_elem.get_text(strip=True)
                        break

            # Look for list size / audience reach
            for label in soup.find_all(['label', 'span', 'td']):
                text = label.get_text(strip=True).lower()
                if 'list size' in text or 'email list' in text:
                    size_elem = label.find_next(['input', 'td', 'span'])
                    if size_elem:
                        data['list_size'] = size_elem.get_text(strip=True)
                elif 'social reach' in text:
                    reach_elem = label.find_next(['input', 'td', 'span'])
                    if reach_elem:
                        data['social_reach'] = reach_elem.get_text(strip=True)

            return data

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'    Scrape error: {e}'))
            return None

    def _write_output_csv(self, results, filepath):
        """Write results to CSV."""
        if not results:
            return

        fieldnames = [
            'name', 'email', 'company', 'business_focus', 'status',
            'website', 'phone', 'calendar_link', 'list_size', 'social_reach',
            'best_way_to_contact', 'url'
        ]

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)
