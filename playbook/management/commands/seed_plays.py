"""
Management command to seed the 54 Launch Plays from the MP3 Framework.

Based on The Launch Content Playbook by Rob Lennon and Erica Schneider.
See: docs/research/54_PLAY_LIBRARY.md
"""

from django.core.management.base import BaseCommand
from django.db import transaction

from playbook.models import LaunchPlay


class Command(BaseCommand):
    help = 'Seeds the database with the 54 Launch Plays from the MP3 Framework'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing plays before seeding',
        )

    def handle(self, *args, **options):
        if options['clear']:
            self.stdout.write('Clearing existing plays...')
            LaunchPlay.objects.all().delete()

        self.stdout.write('Seeding Launch Plays...')

        with transaction.atomic():
            plays_data = self.get_plays_data()

            for play_data in plays_data:
                play, created = LaunchPlay.objects.update_or_create(
                    play_number=play_data['play_number'],
                    defaults=play_data
                )

                status = 'Created' if created else 'Updated'
                self.stdout.write(
                    self.style.SUCCESS(
                        f'{status} Play {play.play_number}: {play.name}'
                    )
                )

        total = LaunchPlay.objects.count()
        self.stdout.write(
            self.style.SUCCESS(f'\nSuccessfully seeded {total} plays!')
        )

    def get_plays_data(self):
        """Return the complete 54-play library data."""

        plays = [
            # ========================================
            # PRE-LAUNCH PHASE (23 plays)
            # ========================================
            {
                'play_number': 1,
                'name': "Something's Coming",
                'phase': 'pre_launch',
                'purpose': 'Build mystery/curiosity',
                'psychology': 'Curiosity Gap + FOMO',
                'content_concept': "Send an email/post indicating something exciting is on the way. Share just a hint of what's coming.",
                'aha_moment': "It's okay to talk about what you're building before it's done. I don't have to keep things a secret.",
                'vibe_authority': "Show the audience you're actively working on something valuable for them.",
                'soft_cta': 'Stay tuned for more details coming soon.',
                'hook_inspirations': [
                    "I shouldn't be telling you this yet...",
                    "Something I've been working on...",
                    "Can't keep a secret anymore...",
                ],
                'included_in_small': True,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 2,
                'name': 'The Problem Spotlight',
                'phase': 'pre_launch',
                'purpose': 'Make audience feel seen',
                'psychology': 'Empathy + Pain Amplification',
                'content_concept': 'Dive deep into the pain your audience faces. Paint a vivid picture of their struggles.',
                'aha_moment': 'This is really what I\'m going through, I feel understood.',
                'vibe_authority': 'Show you deeply understand your audience\'s pain points. Position yourself as someone who gets it.',
                'soft_cta': 'If this resonates, reply and let me know.',
                'hook_inspirations': [
                    'You spend X hours doing...',
                    'Sound familiar?',
                    'I know that feeling when...',
                ],
                'included_in_small': True,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 3,
                'name': 'The Sneak Peek',
                'phase': 'pre_launch',
                'purpose': 'Show effort behind solution',
                'psychology': 'Effort Justification + Open Loop',
                'content_concept': 'Give a sneak peek of the solution. Show behind-the-scenes effort/progress.',
                'aha_moment': 'Wow, this looks really thoughtful. A lot of care is going into this.',
                'vibe_authority': 'Highlight your expertise by showing the depth of work you\'re putting in.',
                'soft_cta': 'Want to be the first to know when it\'s ready?',
                'hook_inspirations': [
                    'Here\'s what I\'ve been building...',
                    'A look behind the curtain...',
                    'Watch me work on...',
                ],
                'included_in_small': False,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 4,
                'name': 'Belief Breakthrough',
                'phase': 'pre_launch',
                'purpose': 'Address limiting beliefs',
                'psychology': 'Cognitive Dissonance Relief',
                'content_concept': 'Address the limiting belief your audience holds. Offer a breakthrough insight that shifts their thinking.',
                'aha_moment': "This is why I've been stuck - it's not that I can't do it, it's that I've been thinking about it wrong.",
                'vibe_authority': 'Position yourself as someone who can shift thinking and see beyond surface-level problems.',
                'soft_cta': 'This is just the beginning. Stay tuned for more insights.',
                'hook_inspirations': [
                    'What if everything you believed about X was wrong?',
                    "The real reason you're stuck isn't...",
                ],
                'included_in_small': True,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 5,
                'name': 'The Big Myth',
                'phase': 'pre_launch',
                'purpose': 'Expose external false belief',
                'psychology': 'Breaking Misconceptions',
                'content_concept': 'Debunk a popular myth or misconception in your industry.',
                'aha_moment': "I've been told this my whole career, but maybe it's not true.",
                'vibe_authority': 'Position yourself as a myth-buster, someone who goes against the grain and questions the norm.',
                'soft_cta': 'If you\'ve been following this myth, reply and let me know.',
                'hook_inspirations': [
                    'Everyone says X, but here\'s the truth...',
                    'The biggest lie in [industry]...',
                    'Stop believing...',
                ],
                'included_in_small': False,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 6,
                'name': 'The Untold Story',
                'phase': 'pre_launch',
                'purpose': 'Reveal hidden truth',
                'psychology': 'Transparency + Emotional Engagement',
                'content_concept': 'Share a personal or untold story about your journey, the problem you\'re solving, or the industry.',
                'aha_moment': "They've been through what I've been through. I can trust them.",
                'vibe_authority': 'Build trust by being vulnerable and sharing your journey authentically.',
                'soft_cta': 'Let me know if my story resonates with yours.',
                'hook_inspirations': [
                    'I\'ve never told anyone this before...',
                    'The day I realized...',
                    'What I learned when everything fell apart...',
                ],
                'included_in_small': False,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 7,
                'name': 'The Domino Effect',
                'phase': 'pre_launch',
                'purpose': 'Show ripple benefits',
                'psychology': 'Future Pacing + Perceived Value',
                'content_concept': 'Show how solving one problem creates a ripple effect of positive outcomes.',
                'aha_moment': 'Once I fix this one thing, everything else will fall into place.',
                'vibe_authority': 'Position your solution as the key to unlocking multiple benefits.',
                'soft_cta': 'Imagine what happens when this one thing clicks.',
                'hook_inspirations': [
                    'Fix this one thing and watch everything change...',
                    'The domino that changes everything...',
                    'Why solving X also fixes Y, Z, and...',
                ],
                'included_in_small': False,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 8,
                'name': 'The Big Mistake',
                'phase': 'pre_launch',
                'purpose': 'Identify common error',
                'psychology': 'Common Enemy + Problem Recognition',
                'content_concept': 'Identify the most common mistake your audience makes and why it\'s holding them back.',
                'aha_moment': 'Oh no, I\'ve been making this mistake! No wonder I\'m not seeing results.',
                'vibe_authority': 'Showcase expertise by pinpointing mistakes others miss.',
                'soft_cta': 'Are you making this mistake? Let me know.',
                'hook_inspirations': [
                    'The #1 mistake I see...',
                    'Stop doing this immediately...',
                    'If you\'re doing X, you\'re losing Y...',
                ],
                'included_in_small': True,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 9,
                'name': 'The Real Reason',
                'phase': 'pre_launch',
                'purpose': 'Reveal root cause',
                'psychology': 'Clarity Brings Relief',
                'content_concept': 'Explain the real reason your audience is struggling—something they may not have realized.',
                'aha_moment': 'I knew something felt off, but I couldn\'t put my finger on it. This is it.',
                'vibe_authority': 'Be the expert who sees beyond symptoms to root causes.',
                'soft_cta': 'Does this resonate? Reply and share your thoughts.',
                'hook_inspirations': [
                    'It\'s not what you think...',
                    'The real reason you\'re stuck...',
                    'What nobody tells you about...',
                ],
                'included_in_small': False,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 10,
                'name': 'Behind the Scenes',
                'phase': 'pre_launch',
                'purpose': 'Show quality/effort',
                'psychology': 'Transparency Builds Trust',
                'content_concept': 'Take the audience behind the scenes of what you\'re building. Show the work, thought, and effort going into your solution.',
                'aha_moment': 'So much thought is going into this—it\'s going to be so valuable!',
                'vibe_authority': 'Reinforce the quality and depth of your offering by showing the work.',
                'soft_cta': 'Want to see more updates? Stay tuned.',
                'hook_inspirations': [
                    'Here\'s a look at my process...',
                    'What goes into creating...',
                    'The work nobody sees...',
                ],
                'included_in_small': False,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 11,
                'name': 'Success Story',
                'phase': 'pre_launch',
                'purpose': 'Share transformation',
                'psychology': 'Social Proof + Relatability',
                'content_concept': 'Share a success story (yours or a customer\'s) that shows the transformation your solution enables.',
                'aha_moment': 'If they could do it, maybe I can too.',
                'vibe_authority': 'Use social proof to build credibility and trust.',
                'soft_cta': 'Reply if you want to be the next success story.',
                'hook_inspirations': [
                    'How [Name] went from X to Y...',
                    'From struggling to thriving...',
                    'The transformation that changed everything...',
                ],
                'included_in_small': False,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 12,
                'name': 'The Invisible Bridge',
                'phase': 'pre_launch',
                'purpose': 'Connect problem to solution',
                'psychology': 'Gap Awareness + Logical Flow',
                'content_concept': 'Explain the gap between where your audience is and where they want to be. Position your solution as the bridge.',
                'aha_moment': 'I can finally see the path forward. I didn\'t know what was missing until now.',
                'vibe_authority': 'Position yourself as a guide who can lead them across the gap.',
                'soft_cta': 'Want help crossing that bridge? Keep an eye out for what\'s coming.',
                'hook_inspirations': [
                    'The missing piece between where you are and where you want to be...',
                    'Why you can\'t get from A to B...',
                ],
                'included_in_small': False,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 13,
                'name': 'The Story Behind',
                'phase': 'pre_launch',
                'purpose': 'Share origin/inspiration',
                'psychology': 'Emotional Connection',
                'content_concept': 'Share the story behind why you created this solution. What inspired you? What moment made you realize it was needed?',
                'aha_moment': 'This is more than just a product—there\'s real heart and purpose behind it.',
                'vibe_authority': 'Build connection by showing the "why" behind your work.',
                'soft_cta': 'If this story resonates with you, let me know.',
                'hook_inspirations': [
                    'Why I decided to create this...',
                    'The moment I knew I had to build...',
                    'This isn\'t just a product, it\'s...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 14,
                'name': 'Quick Win Quick Start',
                'phase': 'pre_launch',
                'purpose': 'Offer immediate action',
                'psychology': 'Progress Builds Momentum',
                'content_concept': 'Offer a quick win or actionable tip the audience can use immediately.',
                'aha_moment': 'I can actually start making progress today!',
                'vibe_authority': 'Position yourself as someone who gives value without holding back.',
                'soft_cta': 'Try this today and let me know how it goes.',
                'hook_inspirations': [
                    'Do this in the next 10 minutes...',
                    'A quick win you can implement today...',
                    'Start here...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 15,
                'name': "Why I'm Solving This",
                'phase': 'pre_launch',
                'purpose': 'Personal connection',
                'psychology': 'Authenticity + Shared Values',
                'content_concept': 'Share a personal connection to the problem. Explain why solving this matters to you beyond just business.',
                'aha_moment': 'They\'re not just selling something—they truly care about helping people like me.',
                'vibe_authority': 'Build trust by showing your genuine passion and connection to the problem.',
                'soft_cta': 'If this resonates, I\'d love to hear your story.',
                'hook_inspirations': [
                    'Why this is personal for me...',
                    'The real reason I do this work...',
                    'It\'s not about the money, it\'s about...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 16,
                'name': 'Overwhelmed? Not Alone',
                'phase': 'pre_launch',
                'purpose': 'Acknowledge struggle',
                'psychology': 'Normalization + Empathy',
                'content_concept': 'Acknowledge that feeling overwhelmed is normal. Normalize the struggle and show that your audience isn\'t alone.',
                'aha_moment': 'I\'m not the only one feeling this way. It\'s okay to struggle.',
                'vibe_authority': 'Be the empathetic guide who understands the journey.',
                'soft_cta': 'You\'re not alone. Reply if you want to share what\'s overwhelming you.',
                'hook_inspirations': [
                    'If you\'re feeling overwhelmed, read this...',
                    'You\'re not alone in this...',
                    'Everyone struggles with...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 17,
                'name': 'Why a Plan Is Key',
                'phase': 'pre_launch',
                'purpose': 'Emphasize structure',
                'psychology': 'Clarity Builds Confidence',
                'content_concept': 'Explain why having a plan or framework is crucial for success in this area.',
                'aha_moment': 'I\'ve been winging it, and that\'s why I\'m not seeing results.',
                'vibe_authority': 'Position yourself as someone who offers structure and clarity.',
                'soft_cta': 'Stay tuned—I\'m going to share a plan that can help.',
                'hook_inspirations': [
                    'Why winging it doesn\'t work...',
                    'The power of having a plan...',
                    'Without a framework, you\'re...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 18,
                'name': 'Secret Sauce Reveal',
                'phase': 'pre_launch',
                'purpose': 'Show differentiator',
                'psychology': 'Unique Value Proposition',
                'content_concept': 'Reveal a unique element of your approach or solution—your "secret sauce."',
                'aha_moment': 'This is different from everything else I\'ve seen. It might actually work for me.',
                'vibe_authority': 'Differentiate yourself by showcasing what makes your approach unique.',
                'soft_cta': 'Want to learn more about how this works? Stay tuned.',
                'hook_inspirations': [
                    'What makes this different...',
                    'My secret approach to...',
                    'Why my method works when others don\'t...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 19,
                'name': 'What Are You Hoping?',
                'phase': 'pre_launch',
                'purpose': 'Engage audience input',
                'psychology': 'Interactive Engagement',
                'content_concept': 'Ask your audience to share their hopes, desires, or what they\'re looking forward to. Get them engaged by inviting responses.',
                'aha_moment': 'They actually want to hear from me. My input matters.',
                'vibe_authority': 'Build community and show that you value your audience\'s voice.',
                'soft_cta': 'Reply with what you\'re hoping to achieve—I\'d love to hear from you.',
                'hook_inspirations': [
                    'What are you hoping to accomplish?',
                    'What would change if you solved this?',
                    'Tell me your biggest goal...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 20,
                'name': 'Early Access/Pre-Sale',
                'phase': 'pre_launch',
                'purpose': 'Offer exclusivity',
                'psychology': 'Exclusivity + Ownership',
                'content_concept': 'Offer early access or a pre-sale opportunity for your most engaged followers.',
                'aha_moment': 'I can get in before everyone else! This feels special.',
                'vibe_authority': 'Reward your engaged audience and build urgency.',
                'soft_cta': 'Claim your early access spot before it\'s gone.',
                'hook_inspirations': [
                    'Early access is open...',
                    'Be the first to...',
                    'Exclusive pre-sale for...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 21,
                'name': 'Live Event Announcement',
                'phase': 'pre_launch',
                'purpose': 'Announce value event',
                'psychology': 'Live Connection + Value First',
                'content_concept': 'Announce a live event (webinar, Q&A, workshop) to give value and interact with your audience.',
                'aha_moment': 'I can learn directly from them, live! This is going to be valuable.',
                'vibe_authority': 'Show commitment to engaging with your audience in real time.',
                'soft_cta': 'Save your spot for the live event—seats are limited.',
                'hook_inspirations': [
                    'Join me live for...',
                    'You\'re invited to...',
                    'Free workshop on...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 22,
                'name': 'My Solution Story',
                'phase': 'pre_launch',
                'purpose': 'Share emotional journey',
                'psychology': 'Emotional Engagement',
                'content_concept': 'Share the emotional story behind how your solution came to be. Include the struggles, doubts, and victories.',
                'aha_moment': 'This wasn\'t just whipped up—it was born out of real struggle and hard work.',
                'vibe_authority': 'Deepen connection by sharing the human side of building your solution.',
                'soft_cta': 'If my journey resonates, I\'d love to hear from you.',
                'hook_inspirations': [
                    'How this solution was born...',
                    'The struggles that led to...',
                    'From rock bottom to...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },
            {
                'play_number': 23,
                'name': 'Development Updates',
                'phase': 'pre_launch',
                'purpose': 'Regular progress sharing',
                'psychology': 'Transparency + Anticipation',
                'content_concept': 'Share regular development updates, teasing new features, content, or improvements.',
                'aha_moment': 'It just keeps getting better! I can\'t wait until it\'s ready.',
                'vibe_authority': 'Keep the audience engaged by showing ongoing progress.',
                'soft_cta': 'Stay tuned for the next update—it\'s going to be good.',
                'hook_inspirations': [
                    'New update on...',
                    'Just added...',
                    'Here\'s what\'s new...',
                ],
                'included_in_small': False,
                'included_in_medium': False,
                'included_in_large': True,
            },

            # ========================================
            # LAUNCH PHASE - ANNOUNCEMENT (2 plays)
            # ========================================
            {
                'play_number': 24,
                'name': 'Launch Announcement',
                'phase': 'launch_announcement',
                'purpose': 'Doors are open',
                'psychology': 'Urgency + Clear CTA',
                'content_concept': 'Announce the official launch! The doors are open—your product is live and ready.',
                'aha_moment': 'It\'s finally here! I can take action now.',
                'vibe_authority': 'Celebrate the launch and make it feel like a big event.',
                'soft_cta': 'Get yours now—doors are officially open!',
                'hook_inspirations': [
                    'It\'s here!',
                    'The wait is over...',
                    'Doors are officially open...',
                ],
                'included_in_small': True,
                'included_in_medium': True,
                'included_in_large': True,
            },
            {
                'play_number': 25,
                'name': 'Introducing [Product] + Urgency Bonus',
                'phase': 'launch_announcement',
                'purpose': 'Full reveal + bonus',
                'psychology': 'Problem-Solution Fit + Bonus Urgency',
                'content_concept': 'Introduce the full product with all its features and benefits. Include an urgency bonus for fast action.',
                'aha_moment': 'This is exactly what I need, and there\'s a bonus if I act fast!',
                'vibe_authority': 'Position your product as the comprehensive solution to their problem.',
                'soft_cta': 'Claim your bonus before time runs out!',
                'hook_inspirations': [
                    'Introducing...',
                    'Everything you get with...',
                    'Plus, if you act now...',
                ],
                'included_in_small': True,
                'included_in_medium': True,
                'included_in_large': True,
            },

            # TODO: Add remaining plays 26-54
            # This seed includes plays 1-25 to get started.
            # Full implementation should include all 54 plays.
        ]

        return plays
