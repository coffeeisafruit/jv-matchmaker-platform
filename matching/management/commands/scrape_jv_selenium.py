"""
Django management command to scrape emails from JV Directory using Selenium.

This uses your actual Chrome browser to bypass Cloudflare protection.

Usage:
    1. Make sure Chrome is installed
    2. Run: python manage.py scrape_jv_selenium --limit 5

The script will open a Chrome window. You'll need to log in manually the first time.
After that, your session will be saved for future runs.
"""

import csv
import re
import time
import os
from pathlib import Path
from django.core.management.base import BaseCommand

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False


class Command(BaseCommand):
    help = 'Scrape emails from JV Directory using Selenium (browser automation)'

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
            '--limit',
            type=int,
            default=0,
            help='Limit number of profiles to scrape (0 = all)'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=2.0,
            help='Delay between requests in seconds (default: 2.0)'
        )
        parser.add_argument(
            '--resume',
            type=int,
            default=0,
            help='Resume from row number (skip first N rows)'
        )
        parser.add_argument(
            '--headless',
            action='store_true',
            help='Run in headless mode (no browser window)'
        )

    def handle(self, *args, **options):
        if not SELENIUM_AVAILABLE:
            self.stdout.write(self.style.ERROR(
                '\nSelenium not installed. Run:\n'
                '  pip install selenium webdriver-manager\n'
            ))
            return

        input_file = options['input']
        output_file = options['output']
        limit = options['limit']
        delay = options['delay']
        resume_from = options['resume']
        headless = options['headless']

        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('JV DIRECTORY EMAIL SCRAPER (Selenium)'))
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

        # Set up Chrome
        self.stdout.write('\nStarting Chrome browser...')
        driver = self._setup_driver(headless)

        if not driver:
            return

        try:
            # Go to login page
            self.stdout.write('\nNavigating to JV Directory login...')
            driver.get('https://jvdirectory.com/login-page')

            # Wait for Cloudflare challenge to pass
            self.stdout.write('Waiting for page to fully load...')
            time.sleep(8)

            # Auto-login with robust element finding
            self.stdout.write('Attempting login...')
            login_success = self._perform_login(driver, 'ken@thepreparedgroup.com', 'NNm2F4xaz!aJhEZ')

            if login_success:
                self.stdout.write(self.style.SUCCESS('Login successful!'))
            else:
                self.stdout.write(self.style.WARNING('Login may have failed, continuing anyway...'))

            # Navigate to member search to verify login
            self.stdout.write('Navigating to member search...')
            driver.get('https://jvdirectory.com/member-search')
            time.sleep(5)

            # Scrape each profile
            results = []
            success_count = 0
            error_count = 0

            for i, member in enumerate(members):
                try:
                    self.stdout.write(f'\n[{i+1}/{len(members)}] Scraping: {member["name"]}')

                    profile_data = self._scrape_profile(driver, member['url'])

                    if profile_data:
                        member.update(profile_data)
                        if profile_data.get('email'):
                            success_count += 1
                            self.stdout.write(self.style.SUCCESS(f'  ✓ Found: {profile_data["email"]}'))
                        else:
                            self.stdout.write(self.style.WARNING(f'  - No email in profile'))
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

        finally:
            driver.quit()

        # Summary
        self.stdout.write(self.style.SUCCESS(f'\n{"="*60}'))
        self.stdout.write(self.style.SUCCESS('COMPLETE'))
        self.stdout.write(self.style.SUCCESS(f'{"="*60}'))
        self.stdout.write(f'\nTotal processed: {len(results)}')
        self.stdout.write(f'Emails found: {success_count}')
        self.stdout.write(f'Errors: {error_count}')
        self.stdout.write(f'\nOutput saved to: {output_file}')

    def _perform_login(self, driver, email, password):
        """Perform login with robust element detection."""
        try:
            # Wait for any input field to be present (page loaded)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "input"))
            )

            # Try multiple strategies to find email field
            email_field = None
            email_selectors = [
                (By.NAME, "email"),
                (By.ID, "email"),
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.CSS_SELECTOR, "input[placeholder*='mail']"),
                (By.CSS_SELECTOR, "input[name*='mail']"),
                (By.CSS_SELECTOR, "input[id*='mail']"),
                (By.XPATH, "//input[@type='text'][1]"),  # First text input
            ]

            for selector_type, selector in email_selectors:
                try:
                    email_field = driver.find_element(selector_type, selector)
                    if email_field.is_displayed():
                        self.stdout.write(f'  Found email field with: {selector}')
                        break
                except NoSuchElementException:
                    continue

            if not email_field:
                self.stdout.write(self.style.ERROR('  Could not find email field'))
                return False

            # Find password field
            password_field = None
            password_selectors = [
                (By.NAME, "password"),
                (By.ID, "password"),
                (By.CSS_SELECTOR, "input[type='password']"),
                (By.CSS_SELECTOR, "input[name*='pass']"),
                (By.CSS_SELECTOR, "input[id*='pass']"),
            ]

            for selector_type, selector in password_selectors:
                try:
                    password_field = driver.find_element(selector_type, selector)
                    if password_field.is_displayed():
                        self.stdout.write(f'  Found password field with: {selector}')
                        break
                except NoSuchElementException:
                    continue

            if not password_field:
                self.stdout.write(self.style.ERROR('  Could not find password field'))
                return False

            # Clear and fill email
            email_field.clear()
            time.sleep(0.3)
            email_field.send_keys(email)
            self.stdout.write(f'  Entered email: {email}')

            # Clear and fill password
            password_field.clear()
            time.sleep(0.3)
            password_field.send_keys(password)
            self.stdout.write('  Entered password: ********')

            # Find and click login button
            time.sleep(0.5)
            login_btn = None
            button_selectors = [
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.CSS_SELECTOR, "input[type='submit']"),
                (By.XPATH, "//button[contains(text(), 'Login')]"),
                (By.XPATH, "//button[contains(text(), 'Log in')]"),
                (By.XPATH, "//button[contains(text(), 'Sign in')]"),
                (By.XPATH, "//input[@value='Login']"),
                (By.XPATH, "//input[@value='Log in']"),
                (By.CSS_SELECTOR, "button.login"),
                (By.CSS_SELECTOR, "button.submit"),
            ]

            for selector_type, selector in button_selectors:
                try:
                    login_btn = driver.find_element(selector_type, selector)
                    if login_btn.is_displayed():
                        self.stdout.write(f'  Found login button with: {selector}')
                        break
                except NoSuchElementException:
                    continue

            if not login_btn:
                # Try submitting the form via Enter key
                self.stdout.write('  No button found, pressing Enter...')
                password_field.send_keys('\n')
            else:
                login_btn.click()
                self.stdout.write('  Clicked login button')

            # Wait for login to complete
            self.stdout.write('  Waiting for login to complete...')
            time.sleep(5)

            # Check if we're still on login page (login failed) or moved on
            current_url = driver.current_url.lower()
            if 'login' not in current_url and 'sign' not in current_url:
                return True

            return False

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  Login error: {e}'))
            return False

    def _setup_driver(self, headless=False):
        """Set up Chrome WebDriver."""
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            from selenium.webdriver.chrome.service import Service as ChromeService

            options = Options()

            # Use a persistent profile to keep login session
            user_data_dir = os.path.expanduser('~/.jv_scraper_chrome_profile')
            options.add_argument(f'--user-data-dir={user_data_dir}')

            if headless:
                options.add_argument('--headless=new')

            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')

            # Use webdriver-manager to auto-download correct ChromeDriver
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)

            return driver

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Failed to start Chrome: {e}'))
            self.stdout.write(self.style.WARNING(
                '\nMake sure you have Chrome installed and run:\n'
                '  pip install selenium webdriver-manager\n'
            ))
            return None

    def _read_input_csv(self, filepath):
        """Read the jvdirectory export CSV and extract member info."""
        members = []

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)

            # Find the column indices
            name_idx = 0
            company_idx = 3
            business_focus_idx = 4
            status_idx = 5
            url_idx = 2

            for row in reader:
                if len(row) > url_idx:
                    url = row[url_idx] if url_idx < len(row) else ''

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
                            'list_size': '',
                            'social_reach': '',
                        })

        return members

    def _scrape_profile(self, driver, url):
        """Scrape ALL fields from a member profile page using Selenium."""
        try:
            driver.get(url)

            # Wait for page to load
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "form"))
                )
            except TimeoutException:
                pass

            time.sleep(1.5)  # Extra wait for dynamic content

            # Initialize all possible fields
            data = {
                'email': '',
                'phone': '',
                'website': '',
                'calendar_link': '',
                'linkedin': '',
                'best_way_to_contact': '',
                'business_summary': '',
                'list_size': '',
                'social_reach': '',
                'business_focus': '',
                'keywords': '',
                'service_providers': '',
                'video_intro': '',
                'annual_revenue': '',
                'jv_offer': '',
                'ideal_jv_partner': '',
                'who_they_serve': '',
                'what_they_do': '',
                'seeking': '',
                'offering': '',
            }

            page_source = driver.page_source

            # Field mapping: label text -> data key
            field_mappings = {
                'best way to contact': 'best_way_to_contact',
                'primary website': 'website',
                'business summary': 'business_summary',
                'business focus': 'business_focus',
                'keywords': 'keywords',
                'service providers': 'service_providers',
                'video introduction': 'video_intro',
                'estimated annual revenue': 'annual_revenue',
                'list size': 'list_size',
                'social reach': 'social_reach',
                'what do they offer': 'jv_offer',
                'joint venture partner': 'jv_offer',
                'ideal joint venture': 'ideal_jv_partner',
                'who is their ideal': 'ideal_jv_partner',
                'who they serve': 'who_they_serve',
                'what they do': 'what_they_do',
                'seeking': 'seeking',
                'offering': 'offering',
            }

            # Method 1: Find all labeled fields by looking for text patterns
            all_text_elements = driver.find_elements(By.XPATH, "//*[string-length(text()) > 0]")

            for field_label, data_key in field_mappings.items():
                if data[data_key]:  # Already found
                    continue
                try:
                    # Find elements containing the label
                    elements = driver.find_elements(By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{field_label}')]")
                    for elem in elements:
                        # Look for adjacent textarea or input
                        parent = elem
                        for _ in range(3):  # Check up to 3 levels up
                            try:
                                parent = parent.find_element(By.XPATH, "./..")
                                # Check for textarea
                                textareas = parent.find_elements(By.TAG_NAME, "textarea")
                                for ta in textareas:
                                    val = ta.get_attribute('value') or ta.text
                                    if val and val.strip():
                                        data[data_key] = val.strip()
                                        break
                                if data[data_key]:
                                    break
                                # Check for input
                                inputs = parent.find_elements(By.TAG_NAME, "input")
                                for inp in inputs:
                                    val = inp.get_attribute('value')
                                    if val and val.strip():
                                        data[data_key] = val.strip()
                                        break
                                if data[data_key]:
                                    break
                            except:
                                break
                        if data[data_key]:
                            break
                except Exception:
                    pass

            # Method 2: Find all textareas and inputs and map by proximity to labels
            if not data['best_way_to_contact']:
                try:
                    textareas = driver.find_elements(By.TAG_NAME, "textarea")
                    for ta in textareas:
                        text = ta.get_attribute('value') or ta.text
                        if text and '@' in text:
                            data['best_way_to_contact'] = text
                            break
                except Exception:
                    pass

            # Method 3: Regex search in page source for contact info
            if not data['best_way_to_contact']:
                match = re.search(
                    r'Best Way to Contact.*?(?:<textarea[^>]*>|<div[^>]*>)([^<]+)',
                    page_source,
                    re.IGNORECASE | re.DOTALL
                )
                if match:
                    data['best_way_to_contact'] = match.group(1).strip()

            # Extract structured contact info from best_way_to_contact
            contact_text = data['best_way_to_contact']
            if contact_text:
                # Find email addresses
                email_matches = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', contact_text)
                if email_matches:
                    data['email'] = email_matches[0]

                # Find URLs
                url_matches = re.findall(r'https?://[^\s<>"\']+', contact_text)
                for u in url_matches:
                    if 'calendar' in u.lower() or 'schedule' in u.lower() or 'calendly' in u.lower():
                        data['calendar_link'] = u
                    elif not data['website']:
                        data['website'] = u

            # Look for Primary Website field
            try:
                website_labels = driver.find_elements(By.XPATH, "//*[contains(text(), 'Primary Website')]")
                for label in website_labels:
                    parent = label.find_element(By.XPATH, "./..")
                    inputs = parent.find_elements(By.TAG_NAME, "input")
                    for inp in inputs:
                        val = inp.get_attribute('value')
                        if val and 'http' in val:
                            data['website'] = val
                            break
                    links = parent.find_elements(By.TAG_NAME, "a")
                    for link in links:
                        href = link.get_attribute('href')
                        if href and 'http' in href:
                            data['website'] = href
                            break
            except Exception:
                pass

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
