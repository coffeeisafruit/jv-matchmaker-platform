"""
Management command: find_promotion_matches

Finds profiles who have promoted products in the same niche as a given client.
Uses PostgreSQL JSON containment on promotion_network field.

Usage:
    python3 manage.py find_promotion_matches --client-name "Amy Porterfield"
    python3 manage.py find_promotion_matches --niche "online business" --limit 20
    python3 manage.py find_promotion_matches --product-type course --min-promotions 3
"""

import json
import logging

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Find profiles who promote products similar to a given client'

    def add_arguments(self, parser):
        parser.add_argument('--client-name', type=str, default='',
                            help='Client name to find promotion matches for')
        parser.add_argument('--niche', type=str, default='',
                            help='Niche keyword to match in promoted_partners')
        parser.add_argument('--product-type', type=str, default='',
                            help='Product type (course, webinar, coaching, etc.)')
        parser.add_argument('--min-promotions', type=int, default=1,
                            help='Minimum total partner promotions (default: 1)')
        parser.add_argument('--limit', type=int, default=20,
                            help='Max results (default: 20)')

    def handle(self, *args, **options):
        client_name = options['client_name']
        niche = options['niche'].lower()
        product_type = options['product_type'].lower()
        min_promotions = options['min_promotions']
        limit = options['limit']

        # Derive niche from client if provided
        if client_name and not niche:
            niche = self._get_client_niche(client_name)
            if niche:
                self.stdout.write(f'Client niche: {niche}')

        if not niche and not product_type:
            self.stdout.write(self.style.ERROR(
                'Provide --client-name, --niche, or --product-type'
            ))
            return

        results = self._find_promoters(niche, product_type, min_promotions, limit)

        self.stdout.write(f'\nFound {len(results)} promotion matches:\n')
        for i, r in enumerate(results, 1):
            network = r.get('promotion_network') or {}
            total = network.get('total_partner_promotions', 0)
            unique = network.get('unique_partners', 0)
            self.stdout.write(
                f'{i:2}. {r["name"]} ({r.get("jv_tier", "?")}) — '
                f'{total} promotions, {unique} unique partners | '
                f'promo_score={r.get("promotion_willingness_score") or 0:.2f}'
            )
            # Show matching promoted partners
            partners = network.get('promoted_partners', [])
            for p in partners[:3]:
                if (niche and niche in (p.get('niche', '') or '').lower()) or \
                   (product_type and product_type in (p.get('product_type', '') or '').lower()):
                    self.stdout.write(
                        f'     → {p["name"]} ({p.get("product_type", "")}) ×{p.get("count", 1)}'
                    )

    def _get_client_niche(self, client_name: str) -> str:
        from matching.models import SupabaseProfile
        profile = SupabaseProfile.objects.filter(name__icontains=client_name).first()
        return (profile.niche or '') if profile else ''

    def _find_promoters(
        self, niche: str, product_type: str, min_promotions: int, limit: int
    ) -> list[dict]:
        """
        Query profiles where promotion_network promoted_partners match niche/product_type.
        Uses PostgreSQL GIN index on promotion_network JSONB.
        """
        conditions = ['promotion_network IS NOT NULL',
                      f"(promotion_network->>'total_partner_promotions')::int >= %s"]
        params: list = [min_promotions]

        # Filter by niche or product type via JSON contains
        if niche:
            conditions.append(
                "EXISTS (SELECT 1 FROM jsonb_array_elements(promotion_network->'promoted_partners') elem "
                "WHERE elem->>'niche' ILIKE %s)"
            )
            params.append(f'%{niche}%')

        if product_type:
            conditions.append(
                "EXISTS (SELECT 1 FROM jsonb_array_elements(promotion_network->'promoted_partners') elem "
                "WHERE elem->>'product_type' ILIKE %s)"
            )
            params.append(f'%{product_type}%')

        where = ' AND '.join(conditions)
        params.append(limit)

        sql = f"""
            SELECT id, name, jv_tier, promotion_willingness_score,
                   email_list_activity_score, promotion_network
            FROM profiles
            WHERE {where}
            ORDER BY promotion_willingness_score DESC NULLS LAST,
                     (promotion_network->>'total_partner_promotions')::int DESC
            LIMIT %s
        """

        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()

        results = []
        for row in rows:
            d = dict(zip(cols, row))
            if isinstance(d.get('promotion_network'), str):
                try:
                    d['promotion_network'] = json.loads(d['promotion_network'])
                except Exception:
                    pass
            results.append(d)
        return results
