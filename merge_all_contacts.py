#!/usr/bin/env python3
"""Merge all contacts into one complete file"""

import csv

# Read the 26 MATCHED contacts from contacts_enriched.csv
matched_contacts = []
with open('contacts_enriched.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get('Match Status') == 'Matched':
            matched_contacts.append(dict(row))

print(f'Found {len(matched_contacts)} matched contacts from Supabase')

# Read the 32 contacts from v7 (27 enriched + 5 new)
with open('contacts_web_enriched_v7.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    enriched_contacts = list(reader)
    fieldnames = reader.fieldnames

print(f'Found {len(enriched_contacts)} enriched contacts from v7')

# Combine all contacts
all_contacts = matched_contacts + enriched_contacts

# Get all unique fieldnames
all_fieldnames = set()
for contact in all_contacts:
    all_fieldnames.update(contact.keys())
all_fieldnames = sorted(list(all_fieldnames))

# Write complete CSV
output_file = 'contacts_complete_v8.csv'
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=all_fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(all_contacts)

print(f'\n✓ Complete CSV saved to: {output_file}')
print(f'  Total contacts: {len(all_contacts)}')
print(f'  - From Supabase (matched): {len(matched_contacts)}')
print(f'  - Enriched (not matched): {len(enriched_contacts) - 5}')
print(f'  - New additions: 5')
print(f'\nBreakdown:')
print(f'  26 matched + 27 enriched + 5 new = {len(all_contacts)} total')
