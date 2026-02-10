"""
Django management command to export JV directory emails to CSV.

Usage:
    python manage.py export_jv_emails
    python manage.py export_jv_emails --output jv_emails.csv
"""

import csv
from django.core.management.base import BaseCommand
from django.db.models import Q
from matching.models import SupabaseProfile


class Command(BaseCommand):
    help = 'Export all JV directory emails to a CSV file'

    def add_arguments(self, parser):
        parser.add_argument(
            '--output',
            type=str,
            default='jv_directory_emails.csv',
            help='Output CSV filename (default: jv_directory_emails.csv)',
        )

    def handle(self, *args, **options):
        output_file = options['output']
        
        # Get all profiles with emails
        profiles = SupabaseProfile.objects.exclude(
            Q(email__isnull=True) | Q(email='')
        ).order_by('name')

        total_count = profiles.count()
        
        if total_count == 0:
            self.stdout.write(
                self.style.WARNING('No profiles with email addresses found.')
            )
            return

        self.stdout.write(f'Found {total_count} profiles with email addresses.')
        self.stdout.write(f'Exporting to {output_file}...')

        # Create CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header row
            writer.writerow([
                'First Name',
                'Last Name',
                'Full Name',
                'Email',
                'Company',
                'Phone',
                'Website',
                'LinkedIn',
                'Status',
                'Niche',
                'List Size',
                'Social Reach',
                'Business Focus',
                'What They Do',
                'Who They Serve',
                'Seeking',
                'Offering'
            ])

            # Write data rows
            for profile in profiles:
                # Split name into first and last
                full_name = (profile.name or '').strip()
                name_parts = full_name.split(' ', 1) if full_name else ['', '']
                first_name = name_parts[0] if len(name_parts) > 0 else ''
                last_name = name_parts[1] if len(name_parts) > 1 else ''
                
                writer.writerow([
                    first_name,
                    last_name,
                    full_name,
                    profile.email or '',
                    profile.company or '',
                    profile.phone or '',
                    profile.website or '',
                    profile.linkedin or '',
                    profile.status or '',
                    profile.niche or '',
                    profile.list_size or 0,
                    profile.social_reach or 0,
                    profile.business_focus or '',
                    profile.what_you_do or '',
                    profile.who_you_serve or '',
                    profile.seeking or '',
                    profile.offering or '',
                ])

        self.stdout.write(
            self.style.SUCCESS(
                f'Successfully exported {total_count} emails to {output_file}'
            )
        )
