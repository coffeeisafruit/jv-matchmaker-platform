#!/usr/bin/env python3
"""Add new contacts and enrich Bobby Cardwell"""

import csv
from pathlib import Path

# Read existing CSV
input_file = 'contacts_web_enriched_v6.csv'
output_file = 'contacts_web_enriched_v7.csv'

with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    contacts = list(reader)
    fieldnames = reader.fieldnames

# Update Bobby Cauldwell/Cardwell
for contact in contacts:
    if 'Bobby' in contact.get('Name', ''):
        # Fix spelling and enrich
        contact['Name'] = 'Bobby Cardwell'  # Fix spelling
        contact['Company'] = 'HealthMeans'
        contact['Notes'] = contact.get('Notes', '') + " | HealthMeans = largest summit platform + 'Netflix for health'. Health Talks Online = B2B summit production. Founded Jan 2015, closed 2025. Based in St. Augustine, FL. Soul Affiliate Alliance member."
        contact['offering'] = 'Turn-key summit production, Health Talks Online (B2B), HealthMeans consumer platform, summit marketing, masterclass/docuseries production'
        contact['what_you_do'] = 'Founder of HealthMeans (largest summit platform) and Health Talks Online. Provides turn-key solutions for digital summit production and marketing. Netflix for health content. Hosted iconic summits: Thyroid Summit, Diabetes Summit, Autoimmune Summit, Healthy Gut Summit, Essential Oils Revolution.'
        contact['who_you_serve'] = 'Health experts, wellness coaches, doctors seeking to create summits, masterclasses, docuseries, or documentaries. B2B clients needing summit production services.'
        contact['signature_programs'] = 'HealthMeans platform, Health Talks Online summit production, various health summits (Thyroid, Diabetes, Autoimmune, Healthy Gut, Essential Oils)'
        contact['social_proof'] = 'Founded and grew HealthMeans into leading summit platform. Hosted dozens of iconic health summits. Based in St. Augustine, FL.'
        break

# New contacts to add
new_contacts = [
    {
        '#': '54',
        'Name': 'Danny Bermant',
        'Email': 'danny@captainjv.co',
        'Company': 'CaptainJV',
        'Website': 'https://calendly.com/captainjv/catch-up-call',
        'Calendar Link': 'https://calendly.com/captainjv/catch-up-call',
        'Match Status': 'Not Matched',
        'Source': 'Retreat',
        'Notes': 'Captain JV - Joint Venture specialist. Christina Hills\' advisor. Attending Soulful Leadership Retreat Feb 4-8, looking for 1 villa roommate.',
        'offering': 'Joint venture strategy, JV consulting, partnership advisory',
        'what_you_do': 'Captain JV - Joint Venture strategist and consultant helping transformation leaders build strategic partnerships.',
        'who_you_serve': 'Transformation leaders, coaches, speakers, entrepreneurs seeking JV partnerships and strategic alliances.',
        'signature_programs': 'Captain JV consulting, JV strategy services',
        'social_proof': 'Advises Christina Hills and other transformation leaders on JV strategy. Active in Soulful Leadership Retreat community.',
        'seeking': 'JV partnership opportunities, collaboration with transformation leaders, strategic alliance building.',
    },
    {
        '#': '55',
        'Name': 'David Riklan',
        'Email': 'david@selfgrowth.com',
        'Company': 'SelfGrowth.com',
        'Website': 'https://www.linkedin.com/in/davidriklan/',
        'LinkedIn': 'https://www.linkedin.com/in/davidriklan/',
        'Match Status': 'Not Matched',
        'Source': 'Soul Affiliate Alliance',
        'Notes': 'Co-founder Joint Venture Directory (with Mark Porteous). Past speaker at Soulful Leadership Retreat 2022. 275,000+ weekly newsletter subscribers.',
        'Niche': 'Business Skills, Fitness, Health (Traditional), Lifestyle, Mental Health, Natural Health, Personal Finances, Relationships, Self Improvement, Service Provider, Spirituality, Success',
        'list_size': '295000',
        'List Size': '295,000',
        'offering': 'Article Submission Sites, Audio Books, Business Coaching, Email List Building, LinkedIn Marketing, Marketing Coach, Online Magazine, Podcast Host',
        'what_you_do': 'Founder of SelfGrowth.com (top self-improvement website with millions of visitors). Co-founder Joint Venture Directory. Promotes JV networking, invites to join JV Directory, shares events, offers training about approaching JV partners.',
        'who_you_serve': 'Entrepreneurs seeking JV opportunities, self-improvement seekers, personal development professionals',
        'seeking': 'Partnerships with other JV-minded professionals; cross-promotion via email and social media; opportunities for interviews, speaking engagements, and affiliate relationships. Partnerships and connections to help members grow their businesses through networking and referrals.',
        'signature_programs': 'SelfGrowth.com platform, Joint Venture Directory, JV training programs',
        'social_proof': '275,000+ weekly newsletter subscribers, millions of annual website visitors, co-founded Joint Venture Directory, past Soulful Leadership Retreat speaker',
    },
    {
        '#': '56',
        'Name': 'Mark Porteous',
        'Email': 'mark@markporteous.com',
        'Company': 'Max Your Life, LLC',
        'Match Status': 'Not Matched',
        'Source': 'Soul Affiliate Alliance',
        'Notes': 'Co-founder Soul Affiliate Alliance and Joint Venture Directory (with David Riklan). Organizer of Soulful Leadership Retreat (150+ attendees annually). "The Soul Connector".',
        'Niche': 'Business Skills, Self Improvement, Spirituality, Success',
        'list_size': '12800',
        'List Size': '12,800',
        'offering': 'Affiliate Managers, Business Consulting, Joint Venture Resources, Launches for Online Programs',
        'what_you_do': 'Co-founder Soul Affiliate Alliance and Joint Venture Directory. Known as "The Soul Connector". Organizes annual Soulful Leadership Retreat. Helps transformational leaders (authors, speakers, coaches, healers) thrive in their Divine purpose through audience growth, client enrollment, and business scaling.',
        'who_you_serve': 'Transformational leaders, authors, speakers, coaches, healers seeking to thrive in their Divine purpose',
        'seeking': 'Strategic partnerships, JV opportunities, connections for Soul Affiliate Alliance members',
        'signature_programs': 'Soul Affiliate Alliance, Joint Venture Directory, Soulful Leadership Retreat (annual), "Soulful Leadership" book (author)',
        'social_proof': 'Co-founded Joint Venture Directory and Soul Affiliate Alliance, organizes 150+ person annual retreat, author of "Soulful Leadership" book, 12,800+ list',
    },
    {
        '#': '57',
        'Name': 'Michelle Abraham',
        'Email': 'michelle@amplifyou.ca',
        'Company': 'Amplifyou',
        'Match Status': 'Not Matched',
        'Source': 'Retreat',
        'Notes': 'Attending Soulful Leadership Retreat Feb 4-8 (sharing villa with Elisa Boogarets). Amplify You - likely podcast/media amplification services.',
        'Niche': 'Health (Traditional), Lifestyle, Mental Health, Natural Health, Personal Finances, Relationships, Self Improvement, Service Provider, Spirituality, Success',
        'list_size': '14872',
        'List Size': '14,872',
        'offering': 'Business Coaching, podcast amplification, media services',
        'what_you_do': 'Founder of Amplify You helping transformation leaders amplify their reach through media and podcast strategies.',
        'who_you_serve': 'Transformation leaders, coaches, speakers seeking to amplify their reach through podcasts and media',
        'seeking': 'JV partnerships, podcast collaboration opportunities, media amplification clients',
        'signature_programs': 'Amplify You programs',
        'social_proof': '14,872+ list, active in Soulful Leadership Retreat community',
    },
    {
        '#': '58',
        'Name': 'Susan Crossman',
        'Email': 'susan@crossmancommunications.com',
        'Company': 'Crossman Communications',
        'Match Status': 'Not Matched',
        'Source': 'Retreat',
        'Notes': 'Attending Soulful Leadership Retreat Feb 8-9 (staying extra night with Rachel Claret). Communications/PR expert.',
        'list_size': '3000',
        'List Size': '3,000',
        'offering': 'Communications services, PR, messaging strategy, visibility campaigns',
        'what_you_do': 'Founder of Crossman Communications providing communications, PR, and messaging strategy for transformation leaders and their partnerships.',
        'who_you_serve': 'Transformation leaders, coaches, speakers needing communications support for JV partnerships and visibility',
        'seeking': 'Partnership opportunities, communications clients, visibility campaign collaborations',
        'signature_programs': 'Crossman Communications services',
        'social_proof': '3,000+ list, active in Soulful Leadership Retreat community',
    },
]

# Add new contacts
contacts.extend(new_contacts)

# Write updated CSV
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(contacts)

print(f'✓ Updated CSV saved to: {output_file}')
print(f'  - Updated Bobby Cardwell enrichment')
print(f'  - Added 5 new contacts:')
print(f'    1. Danny Bermant (Captain JV)')
print(f'    2. David Riklan (SelfGrowth.com, 295K list)')
print(f'    3. Mark Porteous (Soul Affiliate Alliance organizer)')
print(f'    4. Michelle Abraham (Amplify You)')
print(f'    5. Susan Crossman (Crossman Communications)')
print(f'\\nTotal contacts: {len(contacts)}')
