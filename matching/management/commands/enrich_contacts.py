"""
Enrich contacts CSV with data from existing database records.
Matches by email, name, or LinkedIn URL and fills in missing fields.
"""
import csv
import re
from django.core.management.base import BaseCommand
from matching.models import SupabaseProfile, Profile
from django.db.models import Q


class Command(BaseCommand):
    help = 'Enrich contacts CSV with data from existing database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input',
            type=str,
            default='contacts_to_enrich.csv',
            help='Input CSV file path'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='contacts_enriched.csv',
            help='Output CSV file path'
        )

    def normalize_email(self, email):
        """Normalize email for matching."""
        if not email:
            return None
        return email.lower().strip()

    def normalize_name(self, name):
        """Normalize name for fuzzy matching."""
        if not name:
            return None
        # Remove extra whitespace, convert to lowercase
        name = ' '.join(name.lower().strip().split())
        # Remove common suffixes
        name = re.sub(r'\s+(jr\.?|sr\.?|ii|iii|iv|phd|md|lmft|mba)$', '', name, flags=re.IGNORECASE)
        return name

    def extract_linkedin_username(self, linkedin_url):
        """Extract username from LinkedIn URL."""
        if not linkedin_url:
            return None
        match = re.search(r'linkedin\.com/in/([^/]+)', linkedin_url)
        return match.group(1) if match else None

    def normalize_url(self, url):
        """Normalize URL for comparison."""
        if not url:
            return None
        url = url.lower().strip()
        # Remove protocol
        url = re.sub(r'^https?://(www\.)?', '', url)
        # Remove trailing slash
        url = url.rstrip('/')
        return url

    def find_matching_profile(self, contact):
        """Find matching profile from database."""
        email = self.normalize_email(contact.get('Email'))
        email2 = self.normalize_email(contact.get('Email 2'))
        name = self.normalize_name(contact.get('Name'))
        linkedin = contact.get('LinkedIn')
        linkedin_username = self.extract_linkedin_username(linkedin)

        # Try to find in SupabaseProfile first (3,143+ profiles)
        query = Q()

        # Match by email
        if email:
            query |= Q(email__iexact=email)
        if email2:
            query |= Q(email__iexact=email2)

        # Match by LinkedIn username
        if linkedin_username:
            query |= Q(linkedin__icontains=linkedin_username)

        if query:
            profile = SupabaseProfile.objects.filter(query).first()
            if profile:
                return profile, 'supabase'

        # Try to match by name (fuzzy)
        if name:
            # Try exact name match first
            profile = SupabaseProfile.objects.filter(name__iexact=contact.get('Name')).first()
            if profile:
                return profile, 'supabase'

            # Try partial name match
            name_parts = name.split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = name_parts[-1]
                profile = SupabaseProfile.objects.filter(
                    Q(name__icontains=first_name) & Q(name__icontains=last_name)
                ).first()
                if profile:
                    return profile, 'supabase'

        # Try Django Profile model as fallback
        if email or email2:
            profile = Profile.objects.filter(
                Q(email__iexact=email) | Q(email__iexact=email2) if email2 else Q(email__iexact=email)
            ).first()
            if profile:
                return profile, 'django'

        return None, None

    def enrich_contact(self, contact, profile, source):
        """Enrich contact with profile data."""
        enriched = contact.copy()

        if source == 'supabase':
            # Fill in missing fields from SupabaseProfile
            if not enriched.get('Email') and profile.email:
                enriched['Email'] = profile.email

            if not enriched.get('Phone') and profile.phone:
                enriched['Phone'] = profile.phone

            if not enriched.get('Website') and profile.website:
                enriched['Website'] = profile.website

            if not enriched.get('Calendar Link') and profile.booking_link:
                enriched['Calendar Link'] = profile.booking_link

            if not enriched.get('LinkedIn') and profile.linkedin:
                enriched['LinkedIn'] = profile.linkedin

            # Enrich with additional profile data
            enriched['Company'] = profile.company or ''
            enriched['Business Focus'] = profile.business_focus or ''
            enriched['List Size'] = str(profile.list_size) if profile.list_size else ''
            enriched['Social Reach'] = str(profile.social_reach) if profile.social_reach else ''
            enriched['Service Provided'] = profile.service_provided or ''
            enriched['Who You Serve'] = profile.who_you_serve or ''
            enriched['Seeking'] = profile.seeking or ''
            enriched['Offering'] = profile.offering or ''
            enriched['Signature Programs'] = profile.signature_programs or ''
            enriched['Niche'] = profile.niche or ''
            enriched['Status'] = profile.status or ''
            enriched['Bio'] = profile.bio or ''

        elif source == 'django':
            # Fill in missing fields from Profile
            if not enriched.get('Email') and profile.email:
                enriched['Email'] = profile.email

            if not enriched.get('Website') and profile.website_url:
                enriched['Website'] = profile.website_url

            if not enriched.get('LinkedIn') and profile.linkedin_url:
                enriched['LinkedIn'] = profile.linkedin_url

            enriched['Company'] = profile.company or ''
            enriched['Industry'] = profile.industry or ''
            enriched['Audience Size'] = str(profile.audience_size) if profile.audience_size else ''
            enriched['Audience Description'] = profile.audience_description or ''
            enriched['Content Style'] = profile.content_style or ''

        return enriched

    def handle(self, *args, **options):
        input_file = options['input']
        output_file = options['output']

        self.stdout.write(f'Reading contacts from: {input_file}')

        contacts = []
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            contacts = list(reader)

        self.stdout.write(f'Found {len(contacts)} contacts to enrich')

        enriched_contacts = []
        matched_count = 0

        for i, contact in enumerate(contacts, 1):
            self.stdout.write(f"Processing {i}/{len(contacts)}: {contact.get('Name', 'Unknown')}")

            profile, source = self.find_matching_profile(contact)

            if profile:
                enriched = self.enrich_contact(contact, profile, source)
                enriched['Match Source'] = source
                enriched['Match Status'] = 'Matched'
                matched_count += 1
                self.stdout.write(
                    self.style.SUCCESS(f"  ✓ Matched with {source} profile: {profile.name if hasattr(profile, 'name') else profile.company}")
                )
            else:
                enriched = contact.copy()
                enriched['Match Source'] = ''
                enriched['Match Status'] = 'Not Matched'
                # Add empty columns for consistency
                enriched['Company'] = enriched.get('Company', '')
                enriched['Business Focus'] = ''
                enriched['List Size'] = ''
                enriched['Social Reach'] = ''
                enriched['Service Provided'] = ''
                enriched['Who You Serve'] = ''
                enriched['Seeking'] = ''
                enriched['Offering'] = ''
                enriched['Signature Programs'] = ''
                enriched['Niche'] = ''
                enriched['Status'] = ''
                enriched['Bio'] = ''
                self.stdout.write(
                    self.style.WARNING(f"  ⚠ No match found")
                )

            enriched_contacts.append(enriched)

        # Write enriched contacts to output CSV
        self.stdout.write(f'\nWriting enriched data to: {output_file}')

        if enriched_contacts:
            fieldnames = list(enriched_contacts[0].keys())

            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(enriched_contacts)

        self.stdout.write(
            self.style.SUCCESS(
                f'\n✓ Enrichment complete!\n'
                f'  Total contacts: {len(contacts)}\n'
                f'  Matched: {matched_count} ({matched_count/len(contacts)*100:.1f}%)\n'
                f'  Not matched: {len(contacts) - matched_count}\n'
                f'  Output: {output_file}'
            )
        )
