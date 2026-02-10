#!/usr/bin/env python3
"""Find contacts missing email/phone and search Supabase for their information"""

import csv
import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from matching.models import SupabaseProfile

# Read CSV
with open('contacts_complete_v8.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    contacts = list(reader)
    fieldnames = reader.fieldnames

# Find contacts missing email or phone
missing_contacts = []
for contact in contacts:
    email = contact.get('Email', '').strip()
    phone = contact.get('Phone', '').strip()
    name = contact.get('Name', '').strip()

    if not email and not phone and name:
        missing_contacts.append(contact)
        print(f"\n❌ {name}")
        print(f"   Match Status: {contact.get('Match Status', 'Unknown')}")
        print(f"   Source: {contact.get('Source', 'Unknown')}")
        print(f"   LinkedIn: {contact.get('LinkedIn', 'N/A')}")

print(f"\n\n{'='*60}")
print(f"Found {len(missing_contacts)} contacts missing both email AND phone")
print(f"{'='*60}\n")

# Search Supabase for each missing contact
for contact in missing_contacts:
    name = contact.get('Name', '').strip()
    print(f"\n🔍 Searching Supabase for: {name}")

    # Try exact name match
    profiles = SupabaseProfile.objects.filter(name__iexact=name)

    if profiles.exists():
        profile = profiles.first()
        print(f"   ✅ FOUND in Supabase!")
        print(f"      Email: {profile.email}")
        print(f"      Phone: {profile.phone or 'N/A'}")
        print(f"      LinkedIn: {profile.linkedin or 'N/A'}")
        print(f"      Company: {profile.company or 'N/A'}")

        # Update contact
        if profile.email:
            contact['Email'] = profile.email
        if profile.phone:
            contact['Phone'] = profile.phone
        if profile.linkedin:
            contact['LinkedIn'] = profile.linkedin
        if profile.company and not contact.get('Company'):
            contact['Company'] = profile.company
    else:
        # Try partial name match
        first_name = name.split()[0] if name else ''
        if first_name:
            profiles = SupabaseProfile.objects.filter(name__icontains=first_name)
            if profiles.exists():
                print(f"   ⚠️  Found {profiles.count()} partial matches:")
                for p in profiles[:5]:
                    print(f"      - {p.name} ({p.email})")
            else:
                print(f"   ❌ NOT FOUND in Supabase")

# Write updated CSV
output_file = 'contacts_complete_v9.csv'
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(contacts)

print(f"\n\n{'='*60}")
print(f"✓ Updated CSV saved to: {output_file}")
print(f"{'='*60}")
