"""
Django management command to compute network centrality metrics for all partners.

Uses NetworkX to build a graph from SupabaseMatch data and compute:
- PageRank: Overall importance in the network
- Degree Centrality: Number of direct connections (normalized)
- Betweenness Centrality: How often a partner bridges other partners

Usage:
    python manage.py compute_network_centrality
    python manage.py compute_network_centrality --dry-run
"""

import networkx as nx
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db.models import Q
from matching.models import SupabaseProfile, SupabaseMatch


class Command(BaseCommand):
    help = 'Compute network centrality metrics for all partners based on match data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Print metrics without saving to database',
        )
        parser.add_argument(
            '--min-score',
            type=float,
            default=50.0,
            help='Minimum harmonic_mean score to consider as an edge (default: 50.0)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        min_score = options['min_score']

        self.stdout.write('Building network graph from SupabaseMatch data...')

        # Get all matches with sufficient score
        matches = SupabaseMatch.objects.filter(
            harmonic_mean__gte=min_score
        ).values_list('profile_id', 'suggested_profile_id', 'harmonic_mean')

        if not matches:
            self.stdout.write(
                self.style.WARNING('No matches found. Cannot compute centrality metrics.')
            )
            return

        # Build directed graph
        G = nx.DiGraph()

        # Get all profile IDs for reference
        all_profile_ids = set(
            SupabaseProfile.objects.values_list('id', flat=True)
        )

        # Add edges with weights (normalized score as weight)
        edge_count = 0
        for profile_id, suggested_id, score in matches:
            if profile_id and suggested_id:
                # Weight is normalized score (0-1 range)
                # Convert Decimal to float if necessary
                score_float = float(score) if score else 50.0
                weight = score_float / 100.0
                G.add_edge(str(profile_id), str(suggested_id), weight=weight)
                edge_count += 1

        self.stdout.write(f'Graph built with {G.number_of_nodes()} nodes and {edge_count} edges')

        if G.number_of_nodes() == 0:
            self.stdout.write(
                self.style.WARNING('Graph has no nodes. Cannot compute metrics.')
            )
            return

        # Compute centrality metrics
        self.stdout.write('Computing PageRank...')
        try:
            pagerank = nx.pagerank(G, weight='weight', max_iter=100)
        except nx.PowerIterationFailedConvergence:
            self.stdout.write(self.style.WARNING('PageRank did not converge, using default values'))
            pagerank = {node: 1.0 / G.number_of_nodes() for node in G.nodes()}

        self.stdout.write('Computing degree centrality...')
        # Use in-degree + out-degree for directed graph
        in_degree = dict(G.in_degree())
        out_degree = dict(G.out_degree())
        max_degree = max(max(in_degree.values(), default=1), max(out_degree.values(), default=1))
        degree_centrality = {
            node: (in_degree.get(node, 0) + out_degree.get(node, 0)) / (2 * max_degree)
            for node in G.nodes()
        }

        self.stdout.write('Computing betweenness centrality...')
        # For large graphs, use approximation
        if G.number_of_nodes() > 1000:
            self.stdout.write('  (Using approximation for large graph...)')
            betweenness = nx.betweenness_centrality(G, k=min(500, G.number_of_nodes()))
        else:
            betweenness = nx.betweenness_centrality(G)

        # Compute percentiles for role classification
        pagerank_values = sorted(pagerank.values(), reverse=True)
        degree_values = sorted(degree_centrality.values(), reverse=True)
        betweenness_values = sorted(betweenness.values(), reverse=True)

        def percentile_threshold(values, pct):
            """Get value at given percentile (0-100)."""
            if not values:
                return 0
            idx = int(len(values) * (100 - pct) / 100)
            return values[min(idx, len(values) - 1)]

        # Top 10% thresholds
        pagerank_90 = percentile_threshold(pagerank_values, 90)
        degree_90 = percentile_threshold(degree_values, 90)
        betweenness_90 = percentile_threshold(betweenness_values, 90)

        # Bottom 25% thresholds (for newcomers)
        pagerank_25 = percentile_threshold(pagerank_values, 25)
        degree_25 = percentile_threshold(degree_values, 25)

        def classify_role(node_id):
            """Classify network role based on centrality metrics."""
            pr = pagerank.get(node_id, 0)
            deg = degree_centrality.get(node_id, 0)
            btw = betweenness.get(node_id, 0)

            # Hub: High degree centrality (many connections)
            if deg >= degree_90:
                return 'hub'

            # Bridge: High betweenness (connects different clusters)
            if btw >= betweenness_90:
                return 'bridge'

            # Specialist: Low degree but relatively high PageRank
            # (connected to important nodes but not many)
            if deg < degree_25 and pr >= pagerank_90:
                return 'specialist'

            # Newcomer: Low metrics across the board
            if deg <= degree_25 and pr <= pagerank_25:
                return 'newcomer'

            # Default: no special role
            return None

        # Update profiles
        self.stdout.write('Updating profile records...')
        updated_count = 0
        now = timezone.now()

        # Get all profiles that are in the graph
        # Use only() to avoid selecting columns that might not exist yet
        node_ids_in_db = [uuid for uuid in G.nodes() if uuid in [str(pid) for pid in all_profile_ids]]
        profiles_to_update = SupabaseProfile.objects.filter(
            id__in=node_ids_in_db
        ).only('id', 'name')  # Only fetch minimal fields for iteration

        for profile in profiles_to_update:
            node_id = str(profile.id)
            if node_id in G.nodes():
                pr_score = pagerank.get(node_id, 0)
                deg_score = degree_centrality.get(node_id, 0)
                btw_score = betweenness.get(node_id, 0)
                role = classify_role(node_id)

                if dry_run:
                    self.stdout.write(
                        f'  {profile.name}: PR={pr_score:.4f}, Deg={deg_score:.4f}, '
                        f'Btw={btw_score:.4f}, Role={role or "none"}'
                    )
                else:
                    profile.pagerank_score = pr_score
                    profile.degree_centrality = deg_score
                    profile.betweenness_centrality = btw_score
                    profile.network_role = role
                    profile.centrality_updated_at = now
                    profile.save(update_fields=[
                        'pagerank_score', 'degree_centrality', 'betweenness_centrality',
                        'network_role', 'centrality_updated_at'
                    ])
                updated_count += 1

        # Summary statistics
        role_counts = {'hub': 0, 'bridge': 0, 'specialist': 0, 'newcomer': 0, 'none': 0}
        for node_id in G.nodes():
            role = classify_role(node_id)
            role_counts[role or 'none'] += 1

        self.stdout.write('\nNetwork Role Distribution:')
        self.stdout.write(f'  Hubs (Well Connected): {role_counts["hub"]}')
        self.stdout.write(f'  Bridges (Niche Connectors): {role_counts["bridge"]}')
        self.stdout.write(f'  Specialists (Focused Experts): {role_counts["specialist"]}')
        self.stdout.write(f'  Newcomers: {role_counts["newcomer"]}')
        self.stdout.write(f'  No special role: {role_counts["none"]}')

        if dry_run:
            self.stdout.write(
                self.style.WARNING(f'\nDry run complete. Would update {updated_count} profiles.')
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f'\nSuccessfully updated {updated_count} profiles with centrality metrics.')
            )
