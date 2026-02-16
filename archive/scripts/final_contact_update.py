#!/usr/bin/env python3
"""Final update with all found contact information"""

import csv

# Read CSV
with open('contacts_complete_v10.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    contacts = list(reader)
    fieldnames = reader.fieldnames

print("Final contact information update...\n")

updates = 0

for contact in contacts:
    name = contact.get('Name', '').strip()

    # Whitney Gee
    if 'Whitney Gee' in name:
        contact['Email'] = 'whitney@thewholeexperience.org'
        contact['LinkedIn'] = 'https://www.linkedin.com/in/whitney-gee-a35a4543/'
        contact['Website'] = 'https://innergee.me'
        print(f"✓ Updated Whitney Gee")
        print(f"  Email: whitney@thewholeexperience.org")
        print(f"  Website: https://innergee.me")
        updates += 1

    # Stepheni/Stephanie Kwong
    elif 'Kwong' in name:
        contact['Name'] = 'Stephanie Kwong'  # Correct spelling
        contact['Email'] = 'info@rapidrewiremethod.com'  # Generic, but valid
        contact['LinkedIn'] = 'https://www.linkedin.com/in/stephaniekaikwong/'
        contact['Website'] = 'https://www.stephaniekwong.com/'
        print(f"\n✓ Updated Stephanie Kwong")
        print(f"  Email: info@rapidrewiremethod.com")
        print(f"  LinkedIn: https://www.linkedin.com/in/stephaniekaikwong/")
        updates += 1

    # Andrew Golden
    elif 'Andrew Golden' in name:
        contact['Email'] = 'info@atlanticrecruiters.com'  # Generic company email
        contact['LinkedIn'] = 'https://www.linkedin.com/in/andrew-golden-901a5a5/'
        contact['Website'] = 'https://atlanticrecruiters.com/'
        contact['Company'] = 'Atlantic Group'
        print(f"\n✓ Updated Andrew Golden")
        print(f"  Email: info@atlanticrecruiters.com")
        print(f"  Company: Atlantic Group")
        updates += 1

    # Darla LeDoux
    elif 'Darla' in name and 'LeDoux' in name:
        contact['Email'] = 'info@alignedentrepreneurs.com'  # Generic company email
        contact['LinkedIn'] = 'https://www.linkedin.com/in/darlaledoux/'
        contact['Website'] = 'https://sourcedexperience.com/'
        contact['Company'] = 'Aligned Entrepreneurs / Sourced'
        print(f"\n✓ Updated Darla LeDoux")
        print(f"  Email: info@alignedentrepreneurs.com")
        print(f"  Website: https://sourcedexperience.com/")
        updates += 1

    # Fix Joe Apfelbaum name corruption
    elif 'ApfelbaumevyAI' in name or name == 'Joe Applebaum':
        # Search for the real Joe Apfelbaum in contacts
        contact['Name'] = 'Joe Apfelbaum'
        contact['Email'] = 'joe@ajaxunion.com'  # From earlier notes
        contact['Phone'] = '917-865-7631'  # From WhatsApp number in notes
        contact['Company'] = 'Ajax Union'
        contact['Website'] = 'https://www.linkedin.com/in/joeapfelbaum/'
        contact['Match Status'] = 'Matched'
        print(f"\n✓ Fixed and updated Joe Apfelbaum")
        print(f"  Email: joe@ajaxunion.com")
        print(f"  Phone: 917-865-7631 (WhatsApp)")
        updates += 1

# Write updated CSV
output_file = 'contacts_complete_v11.csv'
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(contacts)

print(f"\n{'='*60}")
print(f"✓ Updated {updates} contacts")
print(f"✓ Saved to: {output_file}")
print(f"{'='*60}")

# Final summary
print("\n📊 FINAL SUMMARY - Contacts still missing email:\n")
missing_count = 0
for contact in contacts:
    name = contact.get('Name', '').strip()
    email = contact.get('Email', '').strip()
    if name and not email:
        missing_count += 1
        print(f"  {missing_count}. {name}")

total_contacts = len([c for c in contacts if c.get('Name', '').strip()])
enriched_contacts = len([c for c in contacts if c.get('Name', '').strip() and c.get('Email', '').strip()])

print(f"\n{'='*60}")
print(f"TOTAL CONTACTS: {total_contacts}")
print(f"WITH EMAIL: {enriched_contacts}")
print(f"WITHOUT EMAIL: {missing_count}")
print(f"COMPLETION RATE: {enriched_contacts/total_contacts*100:.1f}%")
print(f"{'='*60}")
