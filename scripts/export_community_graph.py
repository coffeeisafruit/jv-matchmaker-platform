#!/usr/bin/env python3
"""
Export Community Graph Data for Visualization

Builds a NetworkX graph from SupabaseMatch data, computes centrality metrics,
and exports JSON for the Obsidian-style force graph in architecture_diagram.html.

Reuses graph-building and metric logic from:
  matching/management/commands/compute_network_centrality.py

Usage:
    python scripts/export_community_graph.py
    python scripts/export_community_graph.py --min-score 60 --output graph.json
"""

import os
import sys
import json
import argparse
from datetime import datetime
from collections import defaultdict

# Django setup (follows scripts/export_top_matches.py pattern)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

import networkx as nx
from matching.models import SupabaseProfile, SupabaseMatch


# Top-level niche categories with distinct colors
# Raw niches are comma-separated tags like "Business Skills, Self Improvement, Success"
# We categorize by the first meaningful keyword for visual clarity
NICHE_CATEGORIES = {
    'Business':        '#5b8af5',
    'Health & Fitness': '#4ecb8d',
    'Mental Health':   '#e87bb5',
    'Self Improvement': '#f0a04b',
    'Relationships':   '#f06b6b',
    'Spirituality':    '#a87bf5',
    'Lifestyle':       '#4ec8cb',
    'Finance':         '#f5d54e',
    'Service Provider': '#8dd65c',
    'Success':         '#cb6bf0',
    'Unknown':         '#555555',
}

# Priority order: first match wins (most specific first)
_CATEGORY_KEYWORDS = [
    ('Fitness',             'Health & Fitness'),
    ('Health (Traditional)', 'Health & Fitness'),
    ('Natural Health',      'Health & Fitness'),
    ('Mental Health',       'Mental Health'),
    ('Business Skills',     'Business'),
    ('Personal Finances',   'Finance'),
    ('Relationships',       'Relationships'),
    ('Spirituality',        'Spirituality'),
    ('Service Provider',    'Service Provider'),
    ('Self Improvement',    'Self Improvement'),
    ('Lifestyle',           'Lifestyle'),
    ('Success',             'Success'),
]


def categorize_niche(raw_niche):
    """Map a raw comma-separated niche string to a top-level category."""
    if not raw_niche or raw_niche.strip() == 'Unknown':
        return 'Unknown'
    for keyword, category in _CATEGORY_KEYWORDS:
        if keyword in raw_niche:
            return category
    return 'Unknown'


def build_graph(min_score=50.0):
    """Build NetworkX graph from SupabaseMatch data.
    Reuses logic from compute_network_centrality.py lines 44-71."""
    matches = SupabaseMatch.objects.filter(
        harmonic_mean__gte=min_score
    ).values_list('profile_id', 'suggested_profile_id', 'harmonic_mean', 'trust_level')

    G = nx.DiGraph()
    edge_metadata = {}

    for profile_id, suggested_id, score, trust in matches:
        if profile_id and suggested_id:
            score_float = float(score) if score else 50.0
            weight = score_float / 100.0
            src, tgt = str(profile_id), str(suggested_id)
            G.add_edge(src, tgt, weight=weight)
            edge_metadata[(src, tgt)] = {
                'score': round(score_float, 1),
                'trust': trust or 'legacy',
            }

    return G, edge_metadata


def compute_metrics(G):
    """Compute centrality metrics. Reuses logic from compute_network_centrality.py lines 82-152."""
    if G.number_of_nodes() == 0:
        return {}, {}, {}, {}

    # PageRank
    try:
        pagerank = nx.pagerank(G, weight='weight', max_iter=100)
    except nx.PowerIterationFailedConvergence:
        pagerank = {n: 1.0 / G.number_of_nodes() for n in G.nodes()}

    # Degree centrality
    in_deg = dict(G.in_degree())
    out_deg = dict(G.out_degree())
    max_deg = max(max(in_deg.values(), default=1), max(out_deg.values(), default=1))
    degree = {
        n: (in_deg.get(n, 0) + out_deg.get(n, 0)) / (2 * max_deg)
        for n in G.nodes()
    }

    # Betweenness centrality (approximation for large graphs)
    if G.number_of_nodes() > 1000:
        betweenness = nx.betweenness_centrality(G, k=min(500, G.number_of_nodes()))
    else:
        betweenness = nx.betweenness_centrality(G)

    # Role classification
    def percentile_threshold(values, pct):
        if not values:
            return 0
        sorted_vals = sorted(values, reverse=True)
        idx = int(len(sorted_vals) * (100 - pct) / 100)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]

    pr_90 = percentile_threshold(list(pagerank.values()), 90)
    deg_90 = percentile_threshold(list(degree.values()), 90)
    btw_90 = percentile_threshold(list(betweenness.values()), 90)
    pr_25 = percentile_threshold(list(pagerank.values()), 25)
    deg_25 = percentile_threshold(list(degree.values()), 25)

    roles = {}
    for node in G.nodes():
        pr = pagerank.get(node, 0)
        deg = degree.get(node, 0)
        btw = betweenness.get(node, 0)

        if deg >= deg_90:
            roles[node] = 'hub'
        elif btw >= btw_90:
            roles[node] = 'bridge'
        elif deg < deg_25 and pr >= pr_90:
            roles[node] = 'specialist'
        elif deg <= deg_25 and pr <= pr_25:
            roles[node] = 'newcomer'
        else:
            roles[node] = None

    return pagerank, degree, betweenness, roles


def fetch_profiles(node_ids):
    """Fetch profile metadata for graph nodes."""
    profiles = SupabaseProfile.objects.filter(
        id__in=node_ids
    ).values(
        'id', 'name', 'company', 'niche', 'list_size', 'social_reach',
        'what_you_do', 'who_you_serve', 'seeking', 'offering', 'signature_programs',
        'profile_confidence', 'last_enriched_at', 'recommendation_pressure_30d',
    )

    return {str(p['id']): p for p in profiles}


def assign_niche_colors(profiles):
    """Assign colors based on top-level niche categories."""
    category_counts = defaultdict(int)
    for p in profiles.values():
        raw = (p.get('niche') or 'Unknown').strip()
        cat = categorize_niche(raw)
        category_counts[cat] += 1

    # Colors come from the fixed NICHE_CATEGORIES map
    niche_colors = dict(NICHE_CATEGORIES)
    return niche_colors, dict(category_counts)


def build_node(node_id, profiles, pagerank, degree, betweenness, roles, pos):
    """Build a single node dict for JSON output."""
    p = profiles.get(node_id, {})
    x, y = pos.get(node_id, (0.5, 0.5))
    raw_niche = (p.get('niche') or 'Unknown').strip()
    return {
        'id': node_id,
        'name': p.get('name', 'Unknown'),
        'company': p.get('company') or '',
        'niche': raw_niche,
        'category': categorize_niche(raw_niche),
        'list_size': p.get('list_size') or 0,
        'what_you_do': p.get('what_you_do') or '',
        'who_you_serve': p.get('who_you_serve') or '',
        'seeking': p.get('seeking') or '',
        'offering': p.get('offering') or '',
        'signature_programs': p.get('signature_programs') or '',
        'confidence': round(float(p.get('profile_confidence') or 0), 2),
        'enriched': bool(p.get('last_enriched_at')),
        'pressure': p.get('recommendation_pressure_30d') or 0,
        'role': roles.get(node_id),
        'pagerank': round(pagerank.get(node_id, 0), 6),
        'degree': round(degree.get(node_id, 0), 4),
        'betweenness': round(betweenness.get(node_id, 0), 4),
        'x': round(float(x), 4),
        'y': round(float(y), 4),
    }


def build_view(node_ids, G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos):
    """Build nodes + edges for a view subset."""
    node_set = set(node_ids)
    nodes = [
        build_node(nid, profiles, pagerank, degree, betweenness, roles, pos)
        for nid in node_set if nid in profiles
    ]
    edges = []
    for src, tgt in G.edges():
        if src in node_set and tgt in node_set:
            meta = edge_metadata.get((src, tgt), {})
            edges.append({
                'source': src,
                'target': tgt,
                'score': meta.get('score', 50),
                'trust': meta.get('trust', 'legacy'),
            })
    return {'nodes': nodes, 'edges': edges}


def build_full_view(G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos, all_profiles=None):
    """Full community: every profile, including unconnected ones."""
    connected_nodes = set(G.nodes())
    view = build_view(connected_nodes, G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos)

    # Add unconnected profiles as isolated nodes around the periphery
    if all_profiles:
        connected_ids = set(n['id'] for n in view['nodes'])
        import math
        unconnected = []
        idx = 0
        for pid, p in all_profiles.items():
            if pid not in connected_ids:
                # Place in a ring around the outside
                angle = (idx / max(len(all_profiles) - len(connected_ids), 1)) * 2 * math.pi
                radius = 0.95
                raw_niche = (p.get('niche') or 'Unknown').strip()
                unconnected.append({
                    'id': pid,
                    'name': p.get('name', 'Unknown'),
                    'company': p.get('company') or '',
                    'niche': raw_niche,
                    'category': categorize_niche(raw_niche),
                    'list_size': p.get('list_size') or 0,
                    'what_you_do': p.get('what_you_do') or '',
                    'who_you_serve': p.get('who_you_serve') or '',
                    'seeking': p.get('seeking') or '',
                    'offering': p.get('offering') or '',
                    'signature_programs': p.get('signature_programs') or '',
                    'confidence': round(float(p.get('profile_confidence') or 0), 2),
                    'enriched': bool(p.get('last_enriched_at')),
                    'pressure': p.get('recommendation_pressure_30d') or 0,
                    'role': None,
                    'pagerank': 0,
                    'degree': 0,
                    'betweenness': 0,
                    'x': round(0.5 + radius * math.cos(angle), 4),
                    'y': round(0.5 + radius * math.sin(angle), 4),
                })
                idx += 1
        view['nodes'].extend(unconnected)

    return view


def build_connectors_view(G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos, top_n=50):
    """Super connectors: top N by degree centrality + their connections."""
    sorted_nodes = sorted(G.nodes(), key=lambda n: degree.get(n, 0), reverse=True)
    core = set(sorted_nodes[:top_n])

    expanded = set(core)
    for n in core:
        for neighbor in list(G.successors(n)) + list(G.predecessors(n)):
            expanded.add(neighbor)

    return build_view(expanded, G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos)


def build_clusters_view(G, profiles, niche_counts, niche_colors, pagerank, pos):
    """Niche clusters: one meta-node per niche, edges = cross-niche match count."""
    # Build niche membership
    node_to_niche = {}
    niche_members = defaultdict(list)
    for nid in G.nodes():
        p = profiles.get(nid, {})
        niche = (p.get('niche') or 'Unknown').strip()
        node_to_niche[nid] = niche
        niche_members[niche].append(nid)

    # Compute niche positions (centroid of member positions)
    niche_positions = {}
    for niche, members in niche_members.items():
        xs = [pos.get(m, (0.5, 0.5))[0] for m in members]
        ys = [pos.get(m, (0.5, 0.5))[1] for m in members]
        niche_positions[niche] = (
            round(float(sum(xs) / len(xs)), 4),
            round(float(sum(ys) / len(ys)), 4),
        )

    # Top members per niche (by PageRank)
    niche_top = {}
    for niche, members in niche_members.items():
        sorted_m = sorted(members, key=lambda m: pagerank.get(m, 0), reverse=True)[:5]
        niche_top[niche] = [
            profiles.get(m, {}).get('name', 'Unknown') for m in sorted_m
        ]

    # Build cluster nodes
    nodes = []
    for niche, count in niche_counts.items():
        if niche not in niche_members:
            continue
        x, y = niche_positions.get(niche, (0.5, 0.5))
        nodes.append({
            'id': f'niche:{niche}',
            'niche': niche,
            'count': count,
            'top_members': niche_top.get(niche, []),
            'x': x,
            'y': y,
        })

    # Build cross-niche edges
    cross_niche = defaultdict(int)
    for src, tgt in G.edges():
        src_niche = node_to_niche.get(src, 'Unknown')
        tgt_niche = node_to_niche.get(tgt, 'Unknown')
        if src_niche != tgt_niche:
            key = tuple(sorted([src_niche, tgt_niche]))
            cross_niche[key] += 1

    edges = []
    for (n1, n2), count in cross_niche.items():
        if count >= 2:  # Only show meaningful connections
            edges.append({
                'source': f'niche:{n1}',
                'target': f'niche:{n2}',
                'count': count,
            })

    return {'nodes': nodes, 'edges': edges}


def build_aggregators_view(G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos, top_n=50):
    """Aggregators: top N by betweenness centrality (bridges between niches)."""
    sorted_nodes = sorted(G.nodes(), key=lambda n: betweenness.get(n, 0), reverse=True)
    core = set(sorted_nodes[:top_n])

    expanded = set(core)
    for n in core:
        for neighbor in list(G.successors(n)) + list(G.predecessors(n)):
            expanded.add(neighbor)

    return build_view(expanded, G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos)


def main():
    parser = argparse.ArgumentParser(description='Export community graph data for visualization')
    parser.add_argument('--min-score', type=float, default=50.0, help='Minimum harmonic_mean score (default: 50.0)')
    parser.add_argument('--output', default='community_graph_data.js', help='Output JS file (loadable via script tag)')
    parser.add_argument('--max-full-nodes', type=int, default=300, help='Max nodes in full community view')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("COMMUNITY GRAPH EXPORT")
    print(f"{'='*60}\n")

    # Step 1: Build graph
    print("Building network graph from SupabaseMatch data...")
    G, edge_metadata = build_graph(min_score=args.min_score)
    print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    if G.number_of_nodes() == 0:
        print("No graph data found. Exiting.")
        sys.exit(1)

    # Step 2: Compute metrics
    print("Computing centrality metrics...")
    pagerank, degree, betweenness, roles = compute_metrics(G)

    role_counts = defaultdict(int)
    for r in roles.values():
        role_counts[r or 'none'] += 1
    print(f"  Roles: {dict(role_counts)}")

    # Step 3: Fetch profile metadata (connected + all)
    print("Fetching profile metadata...")
    all_node_ids = list(G.nodes())
    profiles = fetch_profiles(all_node_ids)
    print(f"  Connected profiles loaded: {len(profiles)}")

    # Fetch ALL profiles for full community view
    all_profile_ids = list(SupabaseProfile.objects.values_list('id', flat=True))
    all_profiles = fetch_profiles(all_profile_ids)
    print(f"  Total profiles loaded: {len(all_profiles)}")

    # Step 4: Assign niche colors
    niche_colors, niche_counts = assign_niche_colors(all_profiles)
    print(f"  Niches found: {len(niche_colors)}")

    # Step 5: Pre-compute layout
    print("Computing spring layout (this may take a moment)...")
    pos = nx.spring_layout(G, k=2.0, iterations=50, seed=42)

    # Step 6: Build views
    print("Building views...")

    print("  Full community...")
    full_view = build_full_view(G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos, all_profiles=all_profiles)
    print(f"    {len(full_view['nodes'])} nodes, {len(full_view['edges'])} edges")

    print("  Super connectors...")
    connectors_view = build_connectors_view(G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos)
    print(f"    {len(connectors_view['nodes'])} nodes, {len(connectors_view['edges'])} edges")

    print("  Niche clusters...")
    clusters_view = build_clusters_view(G, profiles, niche_counts, niche_colors, pagerank, pos)
    print(f"    {len(clusters_view['nodes'])} clusters, {len(clusters_view['edges'])} cross-niche edges")

    print("  Aggregators...")
    aggregators_view = build_aggregators_view(G, edge_metadata, profiles, pagerank, degree, betweenness, roles, pos)
    print(f"    {len(aggregators_view['nodes'])} nodes, {len(aggregators_view['edges'])} edges")

    # Step 7: Build leaderboard
    print("Building leaderboards...")
    top_hubs = sorted(G.nodes(), key=lambda n: degree.get(n, 0), reverse=True)[:10]
    top_bridges = sorted(G.nodes(), key=lambda n: betweenness.get(n, 0), reverse=True)[:10]

    leaderboard = {
        'hubs': [
            {
                'id': nid,
                'name': profiles.get(nid, {}).get('name', 'Unknown'),
                'company': profiles.get(nid, {}).get('company') or '',
                'degree': round(degree.get(nid, 0), 4),
                'list_size': profiles.get(nid, {}).get('list_size') or 0,
            }
            for nid in top_hubs
        ],
        'bridges': [
            {
                'id': nid,
                'name': profiles.get(nid, {}).get('name', 'Unknown'),
                'company': profiles.get(nid, {}).get('company') or '',
                'betweenness': round(betweenness.get(nid, 0), 4),
                'niche': (profiles.get(nid, {}).get('niche') or 'Unknown').strip(),
            }
            for nid in top_bridges
        ],
    }

    # Step 8: Unconnected breakdown
    print("Computing unconnected profile stats...")
    connected_ids = set(str(nid) for nid in G.nodes())
    unconnected_profiles = {pid: p for pid, p in all_profiles.items() if pid not in connected_ids}
    unconnected_count = len(unconnected_profiles)

    # Breakdown: missing fields that would help them get matched
    missing_niche = sum(1 for p in unconnected_profiles.values() if not (p.get('niche') or '').strip())
    missing_what = sum(1 for p in unconnected_profiles.values() if not (p.get('what_you_do') or '').strip())
    missing_who = sum(1 for p in unconnected_profiles.values() if not (p.get('who_you_serve') or '').strip())
    missing_seeking = sum(1 for p in unconnected_profiles.values() if not (p.get('seeking') or '').strip())

    # Enrichment stats
    enriched_unconnected = sum(1 for p in unconnected_profiles.values() if p.get('last_enriched_at'))
    has_confidence = [float(p.get('profile_confidence') or 0) for p in unconnected_profiles.values() if p.get('profile_confidence')]
    avg_confidence_uc = round(sum(has_confidence) / len(has_confidence), 2) if has_confidence else 0

    # Also compute for connected profiles
    connected_profiles = {pid: p for pid, p in all_profiles.items() if pid in connected_ids}
    enriched_connected = sum(1 for p in connected_profiles.values() if p.get('last_enriched_at'))
    conf_connected = [float(p.get('profile_confidence') or 0) for p in connected_profiles.values() if p.get('profile_confidence')]
    avg_confidence_conn = round(sum(conf_connected) / len(conf_connected), 2) if conf_connected else 0

    # Niche distribution of unconnected
    unconnected_niches = defaultdict(int)
    for p in unconnected_profiles.values():
        n = (p.get('niche') or 'Unknown').strip()
        unconnected_niches[n] += 1
    top_unconnected_niches = sorted(unconnected_niches.items(), key=lambda x: -x[1])[:10]

    unconnected_stats = {
        'count': unconnected_count,
        'missing_niche': missing_niche,
        'missing_what_you_do': missing_what,
        'missing_who_you_serve': missing_who,
        'missing_seeking': missing_seeking,
        'enriched': enriched_unconnected,
        'avg_confidence': avg_confidence_uc,
        'top_niches': [{'niche': n, 'count': c} for n, c in top_unconnected_niches],
    }

    # Step 9: Assemble output
    total_profiles = SupabaseProfile.objects.count()
    output = {
        'generated_at': datetime.now().isoformat(),
        'stats': {
            'total_profiles': total_profiles,
            'connected_profiles': G.number_of_nodes(),
            'total_matches': G.number_of_edges(),
            'niches': len(niche_colors),
            'hubs': role_counts.get('hub', 0),
            'bridges': role_counts.get('bridge', 0),
            'enriched_connected': enriched_connected,
            'enriched_unconnected': enriched_unconnected,
            'avg_confidence_connected': avg_confidence_conn,
            'avg_confidence_unconnected': avg_confidence_uc,
        },
        'niche_colors': niche_colors,
        'leaderboard': leaderboard,
        'unconnected': unconnected_stats,
        'views': {
            'full': full_view,
            'connectors': connectors_view,
            'clusters': clusters_view,
            'aggregators': aggregators_view,
        },
    }

    # Write as JS file (works with file:// protocol, unlike fetch + JSON)
    with open(args.output, 'w') as f:
        f.write('window.COMMUNITY_GRAPH_DATA = ')
        json.dump(output, f)
        f.write(';\n')

    print(f"\n{'='*60}")
    print(f"Exported to {args.output}")
    print(f"  File size: {os.path.getsize(args.output) / 1024:.0f} KB")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
