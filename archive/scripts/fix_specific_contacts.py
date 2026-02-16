#!/usr/bin/env python3
"""Fix specific contacts with known issues"""

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

print("Checking specific contacts with known issues...\n")

# 1. Joe Apfelbaum (misspelled as Applebaum)
print("1. Joe Apfelbaum (currently 'Joe Applebaum'):")
joe_profiles = SupabaseProfile.objects.filter(name__icontains='Apfelbaum')
if joe_profiles.exists():
    for profile in joe_profiles:
        print(f"   ✅ Found: {profile.name}")
        print(f"      Email: {profile.email}")
        print(f"      Phone: {profile.phone}")
        print(f"      LinkedIn: {profile.linkedin}")
        print(f"      Company: {profile.company}")

        # Update in contacts list
        for contact in contacts:
            if contact.get('Name') == 'Joe Applebaum':
                contact['Name'] = profile.name
                contact['Email'] = profile.email or contact.get('Email', '')
                contact['Phone'] = profile.phone or contact.get('Phone', '')
                contact['LinkedIn'] = profile.linkedin or contact.get('LinkedIn', '')
                contact['Company'] = profile.company or contact.get('Company', '')
                contact['Match Status'] = 'Matched'
                print(f"      → Updated contact record")
else:
    print("   ❌ Not found with 'Apfelbaum' spelling")

# 2. Darla LeDoux (currently listed as "Darla Ladoo")
print("\n2. Darla LeDoux (currently 'Darla Ladoo'):")
darla_profiles = SupabaseProfile.objects.filter(name__icontains='LeDoux')
if darla_profiles.exists():
    for profile in darla_profiles:
        print(f"   ✅ Found: {profile.name}")
        print(f"      Email: {profile.email}")
        print(f"      Phone: {profile.phone}")
        print(f"      LinkedIn: {profile.linkedin}")
        print(f"      Niche: {profile.niche}")

        # Update in contacts list
        for contact in contacts:
            if contact.get('Name') == 'Darla Ladoo':
                contact['Name'] = profile.name
                contact['Email'] = profile.email or contact.get('Email', '')
                contact['Phone'] = profile.phone or contact.get('Phone', '')
                contact['LinkedIn'] = profile.linkedin or contact.get('LinkedIn', '')
                contact['Match Status'] = 'Matched'
                if profile.niche:
                    contact['Niche'] = profile.niche
                print(f"      → Updated contact record")
else:
    print("   ❌ Not found with 'LeDoux' spelling")

# 3. Renee Loketi - check if she's in Supabase
print("\n3. Renee Loketi:")
renee_profiles = SupabaseProfile.objects.filter(name__icontains='Renee')
for profile in renee_profiles:
    if 'Loketi' in profile.name or (profile.email and 'renee' in profile.email.lower()):
        print(f"   ✅ Found potential match: {profile.name}")
        print(f"      Email: {profile.email}")
        print(f"      LinkedIn: {profile.linkedin}")

# 4. Alessio Pieroni - mentioned in notes
print("\n4. Alessio Pieroni:")
alessio_profiles = SupabaseProfile.objects.filter(name__icontains='Alessio')
if alessio_profiles.exists():
    for profile in alessio_profiles:
        print(f"   ✅ Found: {profile.name}")
        print(f"      Email: {profile.email}")
        print(f"      List Size: {profile.list_size}")

        # Update in contacts list
        for contact in contacts:
            if contact.get('Name') == 'Alessio Pieroni':
                contact['Email'] = profile.email or contact.get('Email', '')
                contact['Phone'] = profile.phone or contact.get('Phone', '')
                contact['LinkedIn'] = profile.linkedin or contact.get('LinkedIn', '')
                contact['Match Status'] = 'Matched'
                print(f"      → Updated contact record")
else:
    print("   ❌ Not found")

# 5. Jessica Jobes
print("\n5. Jessica Jobes:")
jessica_profiles = SupabaseProfile.objects.filter(name__icontains='Jessica')
for profile in jessica_profiles:
    if 'Jobes' in profile.name or (profile.email and 'jobe' in profile.email.lower()):
        print(f"   ✅ Found potential match: {profile.name}")
        print(f"      Email: {profile.email}")
        print(f"      LinkedIn: {profile.linkedin}")

# 6. Michael Neely (Infinite Lists)
print("\n6. Michael Neely (Infinite Lists):")
michael_profiles = SupabaseProfile.objects.filter(
    name__icontains='Michael'
).filter(company__icontains='Infinite')
if not michael_profiles.exists():
    michael_profiles = SupabaseProfile.objects.filter(name__icontains='Neely')
if michael_profiles.exists():
    for profile in michael_profiles:
        print(f"   ✅ Found: {profile.name}")
        print(f"      Email: {profile.email}")
        print(f"      Company: {profile.company}")

# Write updated CSV
output_file = 'contacts_complete_v9.csv'
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(contacts)

print(f"\n{'='*60}")
print(f"✓ Updated CSV saved to: {output_file}")
print(f"{'='*60}")
