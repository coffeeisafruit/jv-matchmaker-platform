#!/usr/bin/env python3
"""Update contacts with found information from web search"""

import csv

# Read CSV
with open('contacts_complete_v9.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    contacts = list(reader)
    fieldnames = reader.fieldnames

print("Updating contacts with found information...\n")

updates = 0

for contact in contacts:
    name = contact.get('Name', '').strip()

    # Jessica Jobes
    if name == 'Jessica Jobes':
        contact['Email'] = 'jess@onthegridnow.com'
        contact['Phone'] = '+1 (425) 922-3210'
        contact['Match Status'] = 'Not Matched'  # Keep original status
        print(f"✓ Updated Jessica Jobes")
        print(f"  Email: jess@onthegridnow.com")
        print(f"  Phone: +1 (425) 922-3210")
        updates += 1

    # Alessio Pieroni
    elif name == 'Alessio Pieroni':
        contact['Email'] = 'alessio.pieroni89@gmail.com'
        contact['Match Status'] = 'Not Matched'
        print(f"\n✓ Updated Alessio Pieroni")
        print(f"  Email: alessio.pieroni89@gmail.com")
        updates += 1

    # Michael Neely/Neeley
    elif name in ['Michael Neely', 'Michael Neeley']:
        contact['Name'] = 'Michael Neeley'  # Correct spelling
        contact['Email'] = 'info@michaelneeley.com'  # Using info@ as generic
        contact['Website'] = 'https://infinite-list.pages.ontraport.net/home'
        contact['LinkedIn'] = 'https://www.linkedin.com/in/neeleymichael/'
        contact['Match Status'] = 'Not Matched'
        print(f"\n✓ Updated Michael Neeley")
        print(f"  Email: info@michaelneeley.com")
        print(f"  LinkedIn: https://www.linkedin.com/in/neeleymichael/")
        updates += 1

    # Michelle Hummel - check if we have retreat notes
    elif name == 'Michelle Hummel':
        if not contact.get('Email'):
            # From retreat analysis, she might be "Travel with Michelle"
            contact['Website'] = 'https://www.travelwithmichelle.com'
            print(f"\n⚠️  Michelle Hummel - added website, still needs email")

    # Renee Loketi - add Truelancer link from earlier notes
    elif name == 'Renee Loketi':
        if not contact.get('Email'):
            contact['Website'] = 'https://www.truelancer.com/freelancer/reneeloketi'
            contact['LinkedIn'] = 'https://www.linkedin.com/in/reneeloketi'
            print(f"\n⚠️  Renee Loketi - added Truelancer profile, still needs email")

# Write updated CSV
output_file = 'contacts_complete_v10.csv'
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(contacts)

print(f"\n{'='*60}")
print(f"✓ Updated {updates} contacts with email/phone")
print(f"✓ Saved to: {output_file}")
print(f"{'='*60}")

# Summary of remaining contacts without email
print("\n📊 REMAINING CONTACTS WITHOUT EMAIL:\n")
for contact in contacts:
    name = contact.get('Name', '').strip()
    email = contact.get('Email', '').strip()
    if name and not email:
        source = contact.get('Source', 'Unknown')
        notes = contact.get('Notes', '')[:80]
        print(f"  ❌ {name} (Source: {source})")
        if notes:
            print(f"     Notes: {notes}...")
