#!/usr/bin/env python3
"""Merge duplicate contacts keeping the best information from each"""

import csv

# Read CSV
with open('contacts_complete_final.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    contacts = list(reader)
    fieldnames = reader.fieldnames

print("Merging duplicate contacts...\n")

def merge_contact_data(contact1, contact2):
    """Merge two contact dictionaries, keeping non-empty values"""
    merged = {}
    for field in fieldnames:
        val1 = (contact1.get(field) or '').strip()
        val2 = (contact2.get(field) or '').strip()

        # Keep the longer/more complete value
        if val1 and val2:
            merged[field] = val1 if len(val1) >= len(val2) else val2
        else:
            merged[field] = val1 or val2
    return merged

# Track indices to remove
to_remove = []

# 1. Merge Danny Bermant entries
danny_indices = []
for i, c in enumerate(contacts):
    if c.get('Name', '').strip() == 'Danny Bermant':
        danny_indices.append(i)

if len(danny_indices) == 2:
    print("1. Merging Danny Bermant entries...")
    merged_danny = merge_contact_data(contacts[danny_indices[0]], contacts[danny_indices[1]])
    merged_danny['Match Status'] = 'Matched'  # Keep Matched status
    merged_danny['Source'] = 'Both, Retreat'
    contacts[danny_indices[0]] = merged_danny
    to_remove.append(danny_indices[1])
    print("   ✓ Merged 2 entries into 1")

# 2. Merge Michelle Abraham entries (NOTE: Different emails!)
michelle_indices = []
for i, c in enumerate(contacts):
    if c.get('Name', '').strip() == 'Michelle Abraham':
        michelle_indices.append(i)

if len(michelle_indices) == 2:
    print("\n2. Merging Michelle Abraham entries...")
    merged_michelle = merge_contact_data(contacts[michelle_indices[0]], contacts[michelle_indices[1]])
    # Keep BOTH emails
    email1 = contacts[michelle_indices[0]].get('Email', '').strip()
    email2 = contacts[michelle_indices[1]].get('Email', '').strip()
    if email1 != email2:
        merged_michelle['Email'] = email1  # Primary
        merged_michelle['Email 2'] = email2  # Secondary
        print(f"   📧 Kept both emails: {email1}, {email2}")
    merged_michelle['Match Status'] = 'Matched'
    merged_michelle['Source'] = 'Retreat, has an idea'
    contacts[michelle_indices[0]] = merged_michelle
    to_remove.append(michelle_indices[1])
    print("   ✓ Merged 2 entries into 1")

# 3. Merge Susan Crossman entries
susan_indices = []
for i, c in enumerate(contacts):
    if c.get('Name', '').strip() == 'Susan Crossman':
        susan_indices.append(i)

if len(susan_indices) == 2:
    print("\n3. Merging Susan Crossman entries...")
    merged_susan = merge_contact_data(contacts[susan_indices[0]], contacts[susan_indices[1]])
    merged_susan['Match Status'] = 'Matched'
    merged_susan['Source'] = 'Team, Retreat'
    contacts[susan_indices[0]] = merged_susan
    to_remove.append(susan_indices[1])
    print("   ✓ Merged 2 entries into 1")

# Remove duplicates (in reverse order to maintain indices)
for idx in sorted(to_remove, reverse=True):
    del contacts[idx]

# Write final deduplicated CSV
output_file = 'contacts_complete_final.csv'
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(contacts)

print(f"\n{'='*60}")
print(f"✓ Removed {len(to_remove)} duplicate entries")
print(f"✓ Saved to: {output_file}")
print(f"{'='*60}\n")

# Final statistics
total_contacts = len([c for c in contacts if c.get('Name', '').strip()])
with_email = len([c for c in contacts if c.get('Name', '').strip() and c.get('Email', '').strip()])
without_email = total_contacts - with_email

print(f"{'='*60}")
print(f"FINAL DEDUPLICATED DATABASE")
print(f"{'='*60}")
print(f"Total Contacts: {total_contacts}")
print(f"With Email: {with_email}")
print(f"Without Email: {without_email}")
print(f"Completion Rate: {with_email/total_contacts*100:.1f}%")
print(f"{'='*60}")
