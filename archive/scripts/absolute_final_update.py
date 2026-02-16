#!/usr/bin/env python3
"""Absolute final update with last found contacts"""

import csv

# Read CSV
with open('contacts_complete_v11.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    contacts = list(reader)
    fieldnames = reader.fieldnames

print("Applying final contact updates...\n")

updates = 0

for contact in contacts:
    name = contact.get('Name', '').strip()

    # Sheri Rosenthal
    if 'Sheri Rosenthal' in name:
        contact['Email'] = 'awesomeness@wanderlustentrepreneur.com'
        contact['Website'] = 'https://www.wanderlustentrepreneur.com/'
        contact['LinkedIn'] = 'https://www.linkedin.com/in/sherirosenthal/'
        contact['Company'] = 'Wanderlust Entrepreneur'
        print(f"✓ Updated Sheri Rosenthal")
        print(f"  Email: awesomeness@wanderlustentrepreneur.com")
        updates += 1

    # Michelle Hummel
    elif 'Michelle Hummel' in name:
        contact['Email'] = 'shelly@travelwithmichelle.com'
        contact['Phone'] = '405-360-4482'
        contact['Website'] = 'https://www.travelwithmichelle.com/'
        print(f"\n✓ Updated Michelle Hummel")
        print(f"  Email: shelly@travelwithmichelle.com")
        print(f"  Phone: 405-360-4482")
        updates += 1

    # Chuck Anderson
    elif 'Chuck Anderson' in name:
        contact['Email'] = 'chuck@chuckandersoncoaching.com'
        contact['Website'] = 'https://www.chuckandersoncoaching.com/'
        contact['LinkedIn'] = 'https://www.linkedin.com/in/chuck-anderson-15596712/'
        print(f"\n✓ Updated Chuck Anderson")
        print(f"  Email: chuck@chuckandersoncoaching.com")
        updates += 1

# Write final CSV
output_file = 'contacts_complete_final.csv'
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(contacts)

print(f"\n{'='*60}")
print(f"✓ Updated {updates} contacts")
print(f"✓ Saved to: {output_file}")
print(f"{'='*60}")

# Final statistics
total_contacts = len([c for c in contacts if c.get('Name', '').strip()])
with_email = len([c for c in contacts if c.get('Name', '').strip() and c.get('Email', '').strip()])
without_email = total_contacts - with_email

print(f"\n{'='*60}")
print(f"FINAL CONTACT DATABASE STATISTICS")
print(f"{'='*60}")
print(f"Total Contacts: {total_contacts}")
print(f"With Email: {with_email}")
print(f"Without Email: {without_email}")
print(f"Completion Rate: {with_email/total_contacts*100:.1f}%")
print(f"{'='*60}\n")

# Show remaining contacts without email
if without_email > 0:
    print("📧 Contacts still missing email:\n")
    for contact in contacts:
        name = contact.get('Name', '').strip()
        email = contact.get('Email', '').strip()
        if name and not email:
            notes = contact.get('Notes', '')[:80]
            print(f"  ❌ {name}")
            if notes:
                print(f"     → {notes}...")
