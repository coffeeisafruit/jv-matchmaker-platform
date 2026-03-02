#!/usr/bin/env python3
"""
07_network_analysis.py — Match Network Graph Structure Validation

Analyzes the match network graph using NetworkX to validate community
coherence, centrality properties, and structural characteristics of the
JV partner matching network.

Analyses performed:
  1. Graph construction and basic topology (nodes, edges, density, components)
  2. Graph density at multiple harmonic_mean thresholds (50, 55, 60, 67)
  3. Community detection via Louvain modularity optimization
  4. Community-niche coherence (NMI between communities and niche/role labels)
  5. HITS analysis (hubs and authorities)
  6. Centrality-score correlation (PageRank, degree, betweenness vs mean harmonic_mean)
  7. Degree distribution analysis (power-law vs uniform)

Outputs:
  - validation_results/network_analysis_report.txt
  - validation_results/network_communities.csv
  - validation_results/plots/graph_density_thresholds.png
  - validation_results/plots/community_sizes.png
  - validation_results/plots/degree_distribution.png
  - validation_results/plots/centrality_score_scatter.png

Usage:
  python scripts/validation/07_network_analysis.py          # Live database
  python scripts/validation/07_network_analysis.py --test   # Synthetic random graph
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import random
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django  # noqa: E402

django.setup()

from matching.models import SupabaseMatch, SupabaseProfile  # noqa: E402

# ---------------------------------------------------------------------------
# NetworkX
# ---------------------------------------------------------------------------
import networkx as nx  # noqa: E402
from networkx.algorithms.community import louvain_communities  # noqa: E402

# ---------------------------------------------------------------------------
# Scipy / sklearn
# ---------------------------------------------------------------------------
from scipy import stats as scipy_stats  # noqa: E402

try:
    from sklearn.metrics import normalized_mutual_info_score  # noqa: E402
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

# ---------------------------------------------------------------------------
# Matplotlib / Seaborn
# ---------------------------------------------------------------------------
import matplotlib  # noqa: E402

matplotlib.use('Agg')

import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402

try:
    import seaborn as sns  # noqa: E402
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

try:
    plt.style.use('seaborn-v0_8-whitegrid')
except OSError:
    pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'validation_results')
PLOTS_DIR = os.path.join(RESULTS_DIR, 'plots')

DENSITY_THRESHOLDS = [50, 55, 60, 67]
DEFAULT_MIN_SCORE = 50.0

NICHES_SYNTHETIC = [
    'Health & Wellness', 'Business Coaching', 'Mindset & Spirituality',
    'Marketing & Sales', 'Finance & Investing', 'Relationships',
    'Leadership', 'Real Estate', 'Parenting', 'Productivity',
]
ROLES_SYNTHETIC = ['hub', 'bridge', 'specialist', 'newcomer', None]

random.seed(42)
np.random.seed(42)


# ===========================================================================
# Data loading
# ===========================================================================

def load_live_data() -> Tuple[
    Dict[str, Dict[str, Any]],
    List[Tuple[str, str, float]],
]:
    """
    Load profiles and match edges from the live Django database.

    Returns:
        profile_map: node_id -> {name, niche, audience_type, network_role,
                                  pagerank_score, degree_centrality,
                                  betweenness_centrality}
        edges: list of (profile_id, suggested_profile_id, harmonic_mean)
    """
    print("  Loading profiles from database...")
    profiles = SupabaseProfile.objects.all().values(
        'id', 'name', 'niche', 'audience_type', 'network_role',
        'pagerank_score', 'degree_centrality', 'betweenness_centrality',
    )

    profile_map: Dict[str, Dict[str, Any]] = {}
    for p in profiles:
        pid = str(p['id'])
        profile_map[pid] = {
            'name': p['name'] or '',
            'niche': p['niche'] or '',
            'audience_type': p['audience_type'] or '',
            'network_role': p['network_role'] or '',
            'pagerank_score': float(p['pagerank_score']) if p['pagerank_score'] is not None else None,
            'degree_centrality': float(p['degree_centrality']) if p['degree_centrality'] is not None else None,
            'betweenness_centrality': float(p['betweenness_centrality']) if p['betweenness_centrality'] is not None else None,
        }
    print(f"  Loaded {len(profile_map):,d} profiles")

    print("  Loading matches from database...")
    match_qs = SupabaseMatch.objects.filter(
        harmonic_mean__isnull=False,
    ).values_list('profile_id', 'suggested_profile_id', 'harmonic_mean')

    edges: List[Tuple[str, str, float]] = []
    for pid, sid, hm in match_qs:
        if pid and sid and hm is not None:
            edges.append((str(pid), str(sid), float(hm)))

    print(f"  Loaded {len(edges):,d} scored matches")
    return profile_map, edges


def generate_test_data() -> Tuple[
    Dict[str, Dict[str, Any]],
    List[Tuple[str, str, float]],
]:
    """
    Generate synthetic graph data with community structure for --test mode.

    Creates a stochastic block model: nodes assigned to niche-correlated
    communities with higher intra-community edge weights.

    Returns same structure as load_live_data.
    """
    random.seed(42)
    np.random.seed(42)

    n_nodes = 300
    n_communities = 8

    # Assign nodes to communities
    node_ids: List[str] = []
    profile_map: Dict[str, Dict[str, Any]] = {}
    node_comm: Dict[str, int] = {}

    comm_sizes = []
    remaining = n_nodes
    for i in range(n_communities - 1):
        size = max(10, int(remaining / (n_communities - i) + random.gauss(0, 5)))
        size = min(size, remaining - (n_communities - i - 1) * 5)
        comm_sizes.append(size)
        remaining -= size
    comm_sizes.append(remaining)

    idx = 0
    for comm_id, size in enumerate(comm_sizes):
        primary_niche = NICHES_SYNTHETIC[comm_id % len(NICHES_SYNTHETIC)]
        for _ in range(size):
            nid = f"synth-{idx:04d}"
            node_ids.append(nid)
            node_comm[nid] = comm_id

            # Niche correlated with community (80% primary, 20% random)
            niche = primary_niche if random.random() < 0.80 else random.choice(NICHES_SYNTHETIC)
            role = random.choices(
                ['hub', 'bridge', 'specialist', 'newcomer', ''],
                weights=[0.10, 0.10, 0.20, 0.30, 0.30],
                k=1,
            )[0]

            profile_map[nid] = {
                'name': f'Partner {idx}',
                'niche': niche,
                'audience_type': random.choice(['B2B', 'B2C', 'Both', '']),
                'network_role': role,
                'pagerank_score': None,
                'degree_centrality': None,
                'betweenness_centrality': None,
            }
            idx += 1

    # Generate edges with community structure
    edges: List[Tuple[str, str, float]] = []
    edge_set: set = set()
    p_intra = 0.15
    p_inter = 0.01

    for i in range(len(node_ids)):
        for j in range(i + 1, len(node_ids)):
            ni, nj = node_ids[i], node_ids[j]
            same_comm = node_comm[ni] == node_comm[nj]
            p = p_intra if same_comm else p_inter
            if random.random() < p:
                if same_comm:
                    weight = round(random.uniform(55.0, 85.0), 2)
                else:
                    weight = round(random.uniform(50.0, 65.0), 2)
                # Add both directions (mimic SupabaseMatch bidirectional)
                edges.append((ni, nj, weight))
                edges.append((nj, ni, round(weight + random.gauss(0, 3), 2)))

    print(f"  Generated {n_nodes} nodes, {len(edges)} raw edges "
          f"({n_communities} communities)")
    return profile_map, edges


# ===========================================================================
# Graph construction
# ===========================================================================

def build_undirected_graph(
    edges: List[Tuple[str, str, float]],
    min_score: float = DEFAULT_MIN_SCORE,
) -> nx.Graph:
    """
    Build an undirected weighted graph from match edges.

    Filters edges where harmonic_mean >= min_score.
    Edge weight = harmonic_mean value.
    If both (A, B) and (B, A) exist, takes the maximum weight.
    """
    G = nx.Graph()
    for src, tgt, hm in edges:
        if hm < min_score:
            continue
        if G.has_edge(src, tgt):
            if hm > G[src][tgt]['weight']:
                G[src][tgt]['weight'] = hm
        else:
            G.add_edge(src, tgt, weight=hm)
    return G


def build_directed_graph(
    edges: List[Tuple[str, str, float]],
    min_score: float = DEFAULT_MIN_SCORE,
) -> nx.DiGraph:
    """
    Build a directed weighted graph from match edges.

    profile_id -> suggested_profile_id with harmonic_mean as weight.
    """
    G = nx.DiGraph()
    for src, tgt, hm in edges:
        if hm >= min_score:
            G.add_edge(src, tgt, weight=hm)
    return G


# ===========================================================================
# Analysis 1: Graph construction report
# ===========================================================================

def analyze_graph_topology(G: nx.Graph) -> Dict[str, Any]:
    """Compute basic graph topology metrics."""
    n = G.number_of_nodes()
    m = G.number_of_edges()
    density = nx.density(G) if n > 1 else 0.0
    n_components = nx.number_connected_components(G)

    result: Dict[str, Any] = {
        'nodes': n,
        'edges': m,
        'density': density,
        'n_components': n_components,
        'largest_cc_size': 0,
        'largest_cc_frac': 0.0,
        'avg_degree': 0.0,
        'max_degree': 0,
        'min_degree': 0,
    }

    if n > 0:
        largest_cc = max(nx.connected_components(G), key=len)
        result['largest_cc_size'] = len(largest_cc)
        result['largest_cc_frac'] = len(largest_cc) / n

        degrees = [d for _, d in G.degree()]
        result['avg_degree'] = statistics.mean(degrees)
        result['max_degree'] = max(degrees)
        result['min_degree'] = min(degrees)

    return result


# ===========================================================================
# Analysis 2: Graph density at multiple thresholds
# ===========================================================================

def analyze_density_thresholds(
    edges: List[Tuple[str, str, float]],
    thresholds: List[int] = None,
) -> List[Dict[str, Any]]:
    """Compute graph metrics at each harmonic_mean threshold."""
    if thresholds is None:
        thresholds = DENSITY_THRESHOLDS

    results = []
    for thresh in thresholds:
        G = build_undirected_graph(edges, min_score=thresh)
        n = G.number_of_nodes()
        m = G.number_of_edges()
        density = nx.density(G) if n > 1 else 0.0

        if n > 0:
            components = list(nx.connected_components(G))
            n_components = len(components)
            largest = max(len(c) for c in components)
            avg_degree = 2 * m / n if n > 0 else 0.0
        else:
            n_components = 0
            largest = 0
            avg_degree = 0.0

        results.append({
            'threshold': thresh,
            'nodes': n,
            'edges': m,
            'density': density,
            'n_components': n_components,
            'largest_cc_size': largest,
            'avg_degree': avg_degree,
        })

    return results


# ===========================================================================
# Analysis 3: Louvain community detection
# ===========================================================================

def detect_communities(
    G: nx.Graph,
) -> Tuple[List[set], Dict[str, int], float]:
    """
    Run Louvain community detection.

    Returns:
        communities: list of node sets (sorted by size, descending)
        node_community: node_id -> community_id mapping
        modularity: modularity score of the partition
    """
    if G.number_of_nodes() == 0:
        return [], {}, float('nan')

    communities = louvain_communities(G, weight='weight', seed=42)
    communities = sorted(communities, key=len, reverse=True)

    node_community: Dict[str, int] = {}
    for cid, comm in enumerate(communities):
        for node in comm:
            node_community[node] = cid

    try:
        modularity = nx.community.modularity(G, communities, weight='weight')
    except Exception:
        modularity = float('nan')

    return communities, node_community, modularity


# ===========================================================================
# Analysis 4: Community-niche coherence (NMI)
# ===========================================================================

def compute_nmi(
    communities: List[set],
    node_community: Dict[str, int],
    profile_map: Dict[str, Dict[str, Any]],
) -> Tuple[float, float, List[Dict[str, Any]]]:
    """
    Compute NMI between community labels and niche/role labels.

    Returns:
        nmi_niche: NMI between community and niche
        nmi_role: NMI between community and network_role
        per_community: list of dicts with dominant niche/role per community
    """
    nmi_niche = float('nan')
    nmi_role = float('nan')
    per_community: List[Dict[str, Any]] = []

    if not HAS_SKLEARN or not communities:
        return nmi_niche, nmi_role, per_community

    # Build aligned label vectors for nodes with niche data
    comm_labels_niche: List[int] = []
    niche_labels: List[str] = []
    comm_labels_role: List[int] = []
    role_labels: List[str] = []

    for node, cid in node_community.items():
        meta = profile_map.get(node, {})
        niche = meta.get('niche', '')
        role = meta.get('network_role', '')

        if niche:
            comm_labels_niche.append(cid)
            niche_labels.append(niche)
        if role:
            comm_labels_role.append(cid)
            role_labels.append(role)

    if len(comm_labels_niche) >= 10:
        nmi_niche = normalized_mutual_info_score(comm_labels_niche, niche_labels)

    if len(comm_labels_role) >= 10:
        nmi_role = normalized_mutual_info_score(comm_labels_role, role_labels)

    # Per-community dominant niche/role
    for cid, comm in enumerate(communities):
        niche_counter: Counter = Counter()
        role_counter: Counter = Counter()
        for node in comm:
            meta = profile_map.get(node, {})
            if meta.get('niche'):
                niche_counter[meta['niche']] += 1
            if meta.get('network_role'):
                role_counter[meta['network_role']] += 1

        top_niche = niche_counter.most_common(1)[0] if niche_counter else ('(none)', 0)
        top_role = role_counter.most_common(1)[0] if role_counter else ('(none)', 0)
        top_niches_3 = niche_counter.most_common(3)

        per_community.append({
            'community_id': cid,
            'size': len(comm),
            'top_niche': top_niche[0],
            'top_niche_count': top_niche[1],
            'top_niche_pct': top_niche[1] / max(len(comm), 1) * 100,
            'top_role': top_role[0],
            'top_role_count': top_role[1],
            'top_role_pct': top_role[1] / max(len(comm), 1) * 100,
            'top_niches_3': top_niches_3,
        })

    return nmi_niche, nmi_role, per_community


# ===========================================================================
# Analysis 5: HITS analysis
# ===========================================================================

def analyze_hits(
    G_dir: nx.DiGraph,
    profile_map: Dict[str, Dict[str, Any]],
    top_k: int = 20,
) -> Tuple[
    Dict[str, float],
    Dict[str, float],
    List[Tuple[str, str, str, float]],
    List[Tuple[str, str, str, float]],
    int,
]:
    """
    Run HITS algorithm to identify hubs and authorities.

    Returns:
        hub_scores, auth_scores,
        top_hubs [(id, name, niche, score)],
        top_auths [(id, name, niche, score)],
        overlap_count
    """
    if G_dir.number_of_nodes() == 0:
        return {}, {}, [], [], 0

    try:
        hub_scores, auth_scores = nx.hits(G_dir, max_iter=500, tol=1e-08)
    except nx.PowerIterationFailedConvergence:
        n = G_dir.number_of_nodes()
        hub_scores = {node: 1.0 / n for node in G_dir.nodes()}
        auth_scores = {node: 1.0 / n for node in G_dir.nodes()}

    def top_entries(scores: Dict[str, float]) -> List[Tuple[str, str, str, float]]:
        sorted_items = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        result = []
        for nid, score in sorted_items:
            meta = profile_map.get(nid, {})
            result.append((nid, meta.get('name', '')[:35], meta.get('niche', '')[:25], score))
        return result

    top_hubs = top_entries(hub_scores)
    top_auths = top_entries(auth_scores)

    hub_ids = set(nid for nid, _, _, _ in top_hubs)
    auth_ids = set(nid for nid, _, _, _ in top_auths)
    overlap = len(hub_ids & auth_ids)

    return hub_scores, auth_scores, top_hubs, top_auths, overlap


# ===========================================================================
# Analysis 6: Centrality-score correlation
# ===========================================================================

def analyze_centrality_correlation(
    G: nx.Graph,
    edges: List[Tuple[str, str, float]],
    profile_map: Dict[str, Dict[str, Any]],
    min_score: float = DEFAULT_MIN_SCORE,
) -> Tuple[
    Dict[str, Tuple[float, float]],
    Dict[str, float],
    Dict[str, float],
    Dict[str, float],
    Dict[str, float],
]:
    """
    Correlate centrality metrics with mean harmonic_mean per profile.

    Uses pre-computed centrality from SupabaseProfile if available,
    otherwise computes from the graph.

    Returns:
        correlations: metric_name -> (spearman_rho, p_value)
        pagerank, degree_cent, betweenness: computed dicts
        profile_mean_hm: node_id -> mean harmonic_mean
    """
    if G.number_of_nodes() == 0:
        return {}, {}, {}, {}, {}

    # Compute centrality from graph
    try:
        pagerank = nx.pagerank(G, weight='weight', max_iter=200)
    except nx.PowerIterationFailedConvergence:
        pagerank = {n: 1.0 / G.number_of_nodes() for n in G.nodes()}

    degree_cent = nx.degree_centrality(G)

    if G.number_of_nodes() > 1000:
        betweenness = nx.betweenness_centrality(
            G, k=min(500, G.number_of_nodes()), weight='weight',
        )
    else:
        betweenness = nx.betweenness_centrality(G, weight='weight')

    # Compute mean harmonic_mean per profile
    profile_scores: Dict[str, List[float]] = defaultdict(list)
    for src, tgt, hm in edges:
        if hm >= min_score:
            profile_scores[src].append(hm)
            profile_scores[tgt].append(hm)

    profile_mean_hm: Dict[str, float] = {
        nid: float(np.mean(scores))
        for nid, scores in profile_scores.items()
        if scores
    }

    # Check if DB-stored centrality is available
    db_count = sum(
        1 for nid in G.nodes()
        if profile_map.get(nid, {}).get('pagerank_score') is not None
    )
    use_db = db_count > G.number_of_nodes() * 0.5

    # Correlations
    correlations: Dict[str, Tuple[float, float]] = {}

    for metric_name, graph_vals, db_field in [
        ('PageRank', pagerank, 'pagerank_score'),
        ('Degree Centrality', degree_cent, 'degree_centrality'),
        ('Betweenness Centrality', betweenness, 'betweenness_centrality'),
    ]:
        x_vals: List[float] = []
        y_vals: List[float] = []

        for nid in G.nodes():
            mean_hm = profile_mean_hm.get(nid)
            if mean_hm is None or mean_hm <= 0:
                continue

            if use_db and profile_map.get(nid, {}).get(db_field) is not None:
                metric_val = profile_map[nid][db_field]
            else:
                metric_val = graph_vals.get(nid, 0.0)

            if metric_val is not None:
                x_vals.append(metric_val)
                y_vals.append(mean_hm)

        if len(x_vals) < 10:
            correlations[metric_name] = (float('nan'), float('nan'))
        else:
            rho, pval = scipy_stats.spearmanr(x_vals, y_vals)
            correlations[metric_name] = (float(rho), float(pval))

    return correlations, pagerank, degree_cent, betweenness, profile_mean_hm


# ===========================================================================
# Analysis 7: Degree distribution
# ===========================================================================

def analyze_degree_distribution(G: nx.Graph) -> Dict[str, Any]:
    """Analyze the degree distribution of the graph."""
    if G.number_of_nodes() == 0:
        return {'degrees': [], 'is_power_law': False}

    degrees = [d for _, d in G.degree()]

    result: Dict[str, Any] = {
        'degrees': degrees,
        'n': len(degrees),
        'mean': statistics.mean(degrees),
        'median': statistics.median(degrees),
        'stdev': statistics.stdev(degrees) if len(degrees) > 1 else 0.0,
        'max': max(degrees),
        'min': min(degrees),
        'is_power_law': False,
    }

    if max(degrees) > 0 and statistics.mean(degrees) > 0:
        ratio = max(degrees) / statistics.mean(degrees)
        skewness = float(scipy_stats.skew(degrees))
        result['max_mean_ratio'] = ratio
        result['skewness'] = skewness

        # Heuristic: heavy-tailed if max/mean > 5 and highly right-skewed
        if ratio > 5 and skewness > 2:
            result['is_power_law'] = True
            result['distribution_type'] = 'HEAVY-TAILED (power-law-like)'
        elif ratio > 3 and skewness > 1:
            result['distribution_type'] = 'MODERATELY SKEWED'
        else:
            result['distribution_type'] = 'RELATIVELY UNIFORM'
    else:
        result['max_mean_ratio'] = 0.0
        result['skewness'] = 0.0
        result['distribution_type'] = 'DEGENERATE'

    # Percentiles
    deg_arr = np.array(degrees)
    result['percentiles'] = {
        pct: float(np.percentile(deg_arr, pct))
        for pct in [10, 25, 50, 75, 90, 95, 99]
    }

    return result


# ===========================================================================
# Report generation
# ===========================================================================

def generate_report(
    is_test: bool,
    topo: Dict[str, Any],
    threshold_results: List[Dict[str, Any]],
    communities: List[set],
    modularity: float,
    nmi_niche: float,
    nmi_role: float,
    per_community: List[Dict[str, Any]],
    top_hubs: List[Tuple[str, str, str, float]],
    top_auths: List[Tuple[str, str, str, float]],
    hits_overlap: int,
    correlations: Dict[str, Tuple[float, float]],
    degree_info: Dict[str, Any],
) -> str:
    """Build the full text report."""
    buf = StringIO()
    w = buf.write
    sep = '=' * 80
    subsep = '-' * 80

    w(sep + '\n')
    w('  NETWORK ANALYSIS REPORT\n')
    w('  Match Network Graph Structure Validation\n')
    w(sep + '\n\n')
    w(f'  Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
    w(f'  Data source: {"SYNTHETIC (--test mode)" if is_test else "Production database"}\n')
    w(f'  Graph: {topo["nodes"]:,d} nodes, {topo["edges"]:,d} edges\n')
    w('\n')

    # ── 1. Graph Construction ──────────────────────────────────────
    w('\n' + sep + '\n')
    w('  1. GRAPH CONSTRUCTION AND BASIC TOPOLOGY\n')
    w(sep + '\n\n')
    w(f'  Nodes:                {topo["nodes"]:>8,d}\n')
    w(f'  Edges:                {topo["edges"]:>8,d}\n')
    w(f'  Density:              {topo["density"]:>12.6f}\n')
    w(f'  Connected components: {topo["n_components"]:>8,d}\n')
    w(f'  Largest component:    {topo["largest_cc_size"]:>8,d} '
      f'({topo["largest_cc_frac"] * 100:.1f}% of nodes)\n')
    w(f'  Average degree:       {topo["avg_degree"]:>12.2f}\n')
    w(f'  Max degree:           {topo["max_degree"]:>8,d}\n')
    w(f'  Min degree:           {topo["min_degree"]:>8,d}\n')
    w('\n')

    # ── 2. Graph Density at Multiple Thresholds ───────────────────
    w('\n' + sep + '\n')
    w('  2. GRAPH DENSITY AT MULTIPLE THRESHOLDS\n')
    w(sep + '\n\n')
    w(f'  {"Threshold":>10}  {"Nodes":>8}  {"Edges":>8}  {"Density":>10}  '
      f'{"Components":>11}  {"Largest CC":>11}  {"Avg Degree":>11}\n')
    w(f'  {"-" * 10}  {"-" * 8}  {"-" * 8}  {"-" * 10}  '
      f'{"-" * 11}  {"-" * 11}  {"-" * 11}\n')
    for d in threshold_results:
        w(f'  {d["threshold"]:>10}  {d["nodes"]:>8,d}  {d["edges"]:>8,d}  '
          f'{d["density"]:>10.6f}  {d["n_components"]:>11,d}  '
          f'{d["largest_cc_size"]:>11,d}  {d["avg_degree"]:>11.2f}\n')
    w('\n')
    w('  Observation: Increasing threshold reduces density and fragments\n')
    w('  the graph into more components, revealing which matches are robust.\n')
    w('\n')

    # ── 3. Community Detection ────────────────────────────────────
    w('\n' + sep + '\n')
    w('  3. COMMUNITY DETECTION (LOUVAIN)\n')
    w(sep + '\n\n')
    w(f'  Number of communities detected: {len(communities)}\n')
    mod_str = f'{modularity:.4f}' if not math.isnan(modularity) else 'N/A'
    w(f'  Modularity score:               {mod_str}\n')
    w('\n')

    if communities:
        sizes = [len(c) for c in communities]
        w('  Community size distribution:\n')
        w(f'    Mean:   {statistics.mean(sizes):.1f}\n')
        w(f'    Median: {statistics.median(sizes):.1f}\n')
        w(f'    Max:    {max(sizes)}\n')
        w(f'    Min:    {min(sizes)}\n')
        stdev = statistics.stdev(sizes) if len(sizes) > 1 else 0.0
        w(f'    Stdev:  {stdev:.1f}\n')
        w('\n')

    if per_community:
        w('  Top 5 communities:\n')
        w(f'    {"Comm":>4}  {"Size":>6}  {"Top Niches (up to 3)"}\n')
        w(f'    {"----":>4}  {"------":>6}  {"-" * 55}\n')
        for pc in per_community[:5]:
            niche_str = ', '.join(
                f'{n} ({c})' for n, c in pc['top_niches_3']
            ) if pc['top_niches_3'] else '(no niche data)'
            w(f'    {pc["community_id"]:>4}  {pc["size"]:>6,d}  {niche_str}\n')
        w('\n')

    # ── 4. Community-Niche Coherence ──────────────────────────────
    w('\n' + sep + '\n')
    w('  4. COMMUNITY-NICHE COHERENCE\n')
    w(sep + '\n\n')

    if per_community:
        w('  Per-community dominant niche and role:\n')
        w(f'    {"Comm":>4}  {"Size":>6}  {"Dominant Niche":<30}  {"Dominant Role":<15}\n')
        w(f'    {"----":>4}  {"------":>6}  {"-" * 30}  {"-" * 15}\n')
        for pc in per_community[:10]:
            niche_str = f'{pc["top_niche"]} ({pc["top_niche_pct"]:.1f}%)'
            role_str = f'{pc["top_role"]} ({pc["top_role_pct"]:.1f}%)'
            w(f'    {pc["community_id"]:>4}  {pc["size"]:>6,d}  {niche_str:<30}  {role_str:<15}\n')
        w('\n')

    nmi_n_str = f'{nmi_niche:.4f}' if not math.isnan(nmi_niche) else 'N/A'
    nmi_r_str = f'{nmi_role:.4f}' if not math.isnan(nmi_role) else 'N/A'

    w(f'  NMI (community vs niche):        {nmi_n_str}\n')
    if not math.isnan(nmi_niche):
        if nmi_niche > 0.20:
            w(f'    PASS: NMI {nmi_niche:.4f} > 0.20 target\n')
        else:
            w(f'    BELOW TARGET: NMI {nmi_niche:.4f} < 0.20 target\n')
    w('\n')

    w(f'  NMI (community vs network_role): {nmi_r_str}\n')
    w('\n')

    if not math.isnan(nmi_niche) and nmi_niche > 0.20:
        w('  Interpretation: Communities correlate with niche labels. The matching\n')
        w('  algorithm groups similar niches together, suggesting structural validity.\n')
    elif not math.isnan(nmi_niche):
        w('  Interpretation: Weak community-niche correlation. Communities may be\n')
        w('  driven more by other factors than pure niche alignment.\n')
    w('\n')

    # ── 5. HITS Analysis ──────────────────────────────────────────
    w('\n' + sep + '\n')
    w('  5. HITS ANALYSIS (HUBS AND AUTHORITIES)\n')
    w(sep + '\n\n')

    w('  Top 20 HUBS (profiles that match well with many others):\n')
    w(f'    {"Rank":>4}  {"Profile ID":>38}  {"Hub Score":>10}  '
      f'{"Name":<35}  {"Niche"}\n')
    w(f'    {"----":>4}  {"-" * 38}  {"-" * 10}  {"-" * 35}  {"-" * 25}\n')
    for i, (nid, name, niche, score) in enumerate(top_hubs, 1):
        w(f'    {i:>4}  {nid:>38}  {score:>10.6f}  {name:<35}  {niche}\n')
    w('\n')

    w('  Top 20 AUTHORITIES (profiles that many others want to match with):\n')
    w(f'    {"Rank":>4}  {"Profile ID":>38}  {"Auth Score":>10}  '
      f'{"Name":<35}  {"Niche"}\n')
    w(f'    {"----":>4}  {"-" * 38}  {"-" * 10}  {"-" * 35}  {"-" * 25}\n')
    for i, (nid, name, niche, score) in enumerate(top_auths, 1):
        w(f'    {i:>4}  {nid:>38}  {score:>10.6f}  {name:<35}  {niche}\n')
    w('\n')

    w(f'  Hub-Authority overlap (top 20):\n')
    w(f'    Profiles in both top-20 hubs and top-20 authorities: {hits_overlap}\n')
    w(f'    Overlap fraction: {hits_overlap / 20 * 100:.1f}%\n')
    w('\n')
    if hits_overlap > 15:
        w('    Note: High overlap is common for undirected matching networks.\n')
    elif hits_overlap < 5:
        w('    Note: Low overlap suggests distinct connector vs. sought-after profiles.\n')
    w('\n')

    # ── 6. Centrality-Score Correlation ───────────────────────────
    w('\n' + sep + '\n')
    w('  6. CENTRALITY-SCORE CORRELATION\n')
    w(sep + '\n\n')
    w('  Spearman rho between centrality metrics and mean harmonic_mean:\n\n')

    w(f'  {"Metric":<25}  {"N":>6}  {"Spearman rho":>13}  {"p-value":>12}\n')
    w(f'  {"-" * 25}  {"-" * 6}  {"-" * 13}  {"-" * 12}\n')
    for metric_name, (rho, pval) in correlations.items():
        if math.isnan(rho):
            w(f'  {metric_name:<25}  {"N/A":>6}  {"N/A":>13}  {"N/A":>12}\n')
        else:
            # Count by looking at how many nodes contributed
            w(f'  {metric_name:<25}  {"":>6}  {rho:>+13.4f}  {pval:>12.2e}\n')
    w('\n')

    pr_rho = correlations.get('PageRank', (float('nan'), float('nan')))[0]
    if not math.isnan(pr_rho):
        if abs(pr_rho) > 0.5:
            w('  Strong correlation between centrality and match quality.\n')
        elif abs(pr_rho) > 0.2:
            w('  Moderate correlation between centrality and match quality.\n')
        else:
            w('  Weak correlation: match quality is relatively independent of\n')
            w('  network position. This suggests diverse matching patterns.\n')
    w('\n')

    # ── 7. Degree Distribution ────────────────────────────────────
    w('\n' + sep + '\n')
    w('  7. DEGREE DISTRIBUTION\n')
    w(sep + '\n\n')

    if degree_info['degrees']:
        w(f'  Total nodes:    {degree_info["n"]:>8,d}\n')
        w(f'  Mean degree:    {degree_info["mean"]:>12.2f}\n')
        w(f'  Median degree:  {degree_info["median"]:>12.1f}\n')
        w(f'  Stdev:          {degree_info["stdev"]:>12.2f}\n')
        w(f'  Max degree:     {degree_info["max"]:>8,d}\n')
        w(f'  Min degree:     {degree_info["min"]:>8,d}\n')
        w('\n')

        w('  Percentiles:\n')
        for pct, val in degree_info.get('percentiles', {}).items():
            w(f'    P{pct:>2}: {val:>8.1f}\n')
        w('\n')

        w(f'  Max/Mean ratio:    {degree_info.get("max_mean_ratio", 0):.2f}\n')
        w(f'  Skewness:          {degree_info.get("skewness", 0):.2f}\n')
        w('\n')

        dist_type = degree_info.get('distribution_type', 'UNKNOWN')
        w(f'  Distribution characteristics: {dist_type}\n')

        if degree_info.get('is_power_law'):
            w('  Interpretation: A few "super connectors" have many more matches\n')
            w('  than typical profiles. This is common in social networks.\n')
        elif dist_type == 'MODERATELY SKEWED':
            w('  Interpretation: Some variation in connectivity, but not extreme.\n')
        elif dist_type == 'RELATIVELY UNIFORM':
            w('  Interpretation: Most profiles have a similar number of matches.\n')
    else:
        w('  No degree data available.\n')
    w('\n')

    w('\n' + sep + '\n')
    w('  END OF REPORT\n')
    w(sep + '\n')

    return buf.getvalue()


# ===========================================================================
# CSV export
# ===========================================================================

def export_community_csv(
    communities: List[set],
    node_community: Dict[str, int],
    profile_map: Dict[str, Dict[str, Any]],
    csv_path: str,
) -> None:
    """Export community assignments to CSV: profile_id, community_id, niche, role."""
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['profile_id', 'community_id', 'niche', 'role'])
        for node, cid in sorted(node_community.items(), key=lambda x: (x[1], x[0])):
            meta = profile_map.get(node, {})
            writer.writerow([
                node,
                cid,
                meta.get('niche', ''),
                meta.get('network_role', ''),
            ])
    print(f"  Saved: {csv_path}")


# ===========================================================================
# Visualizations
# ===========================================================================

def plot_graph_density_thresholds(
    threshold_results: List[Dict[str, Any]],
    out_path: str,
) -> None:
    """Line chart: density, edge count, and component count at each threshold."""
    if not threshold_results:
        return

    thresholds = [d['threshold'] for d in threshold_results]
    densities = [d['density'] for d in threshold_results]
    edges = [d['edges'] for d in threshold_results]
    components = [d['n_components'] for d in threshold_results]

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    # Density
    ax = axes[0]
    ax.plot(thresholds, densities, 'o-', color='#3498db', linewidth=2, markersize=8)
    ax.set_xlabel('Harmonic Mean Threshold', fontsize=11)
    ax.set_ylabel('Graph Density', fontsize=11)
    ax.set_title('Density vs Threshold', fontsize=13)
    ax.set_xticks(thresholds)
    for t, d in zip(thresholds, densities):
        ax.annotate(f'{d:.4f}', (t, d), textcoords='offset points',
                    xytext=(0, 10), ha='center', fontsize=9)

    # Edge count
    ax = axes[1]
    ax.plot(thresholds, edges, 'o-', color='#e74c3c', linewidth=2, markersize=8)
    ax.set_xlabel('Harmonic Mean Threshold', fontsize=11)
    ax.set_ylabel('Edge Count', fontsize=11)
    ax.set_title('Edges vs Threshold', fontsize=13)
    ax.set_xticks(thresholds)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{int(x):,}'))
    for t, e in zip(thresholds, edges):
        ax.annotate(f'{e:,}', (t, e), textcoords='offset points',
                    xytext=(0, 10), ha='center', fontsize=9)

    # Component count
    ax = axes[2]
    ax.plot(thresholds, components, 'o-', color='#2ecc71', linewidth=2, markersize=8)
    ax.set_xlabel('Harmonic Mean Threshold', fontsize=11)
    ax.set_ylabel('Connected Components', fontsize=11)
    ax.set_title('Components vs Threshold', fontsize=13)
    ax.set_xticks(thresholds)
    for t, c in zip(thresholds, components):
        ax.annotate(str(c), (t, c), textcoords='offset points',
                    xytext=(0, 10), ha='center', fontsize=9)

    fig.suptitle('Graph Properties at Different Harmonic Mean Thresholds',
                 fontsize=14, y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_community_sizes(
    communities: List[set],
    out_path: str,
) -> None:
    """Bar chart of community sizes (top 20 communities)."""
    if not communities:
        return

    sizes = [len(c) for c in communities]
    top_sizes = sizes[:20]

    fig, ax = plt.subplots(figsize=(12, 6))

    x = range(len(top_sizes))
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(top_sizes)))
    bars = ax.bar(x, top_sizes, color=colors, edgecolor='white', width=0.8)

    for bar, size in zip(bars, top_sizes):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                str(size), ha='center', va='bottom', fontsize=9)

    ax.set_xlabel('Community Index (sorted by size)', fontsize=11)
    ax.set_ylabel('Number of Members', fontsize=11)
    ax.set_title(f'Community Sizes (Top {len(top_sizes)} of {len(sizes)} total)',
                 fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels([str(i) for i in range(len(top_sizes))], fontsize=9)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_degree_distribution(
    degree_info: Dict[str, Any],
    out_path: str,
) -> None:
    """Histogram of node degrees with log-log plot for power-law detection."""
    degrees = degree_info.get('degrees', [])
    if not degrees:
        return

    is_power_law = degree_info.get('is_power_law', False)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Left: linear histogram
    ax = axes[0]
    max_deg = max(degrees)
    bins = min(50, max(10, max_deg // 2))
    ax.hist(degrees, bins=bins, color='#4C72B0', edgecolor='white', alpha=0.85)
    ax.axvline(statistics.mean(degrees), color='#C44E52', linestyle='--',
               linewidth=2, label=f'Mean = {statistics.mean(degrees):.1f}')
    ax.axvline(statistics.median(degrees), color='#55A868', linestyle=':',
               linewidth=2, label=f'Median = {statistics.median(degrees):.1f}')
    ax.set_xlabel('Degree (number of connections)', fontsize=11)
    ax.set_ylabel('Count', fontsize=11)
    ax.set_title('Degree Distribution (Linear Scale)', fontsize=13)
    ax.legend(frameon=True)

    # Right: log-log scatter
    ax2 = axes[1]
    degree_counts = Counter(degrees)
    d_vals = sorted(degree_counts.keys())
    d_counts = [degree_counts[d] for d in d_vals]

    d_vals_pos = [d for d in d_vals if d > 0]
    d_counts_pos = [degree_counts[d] for d in d_vals_pos]

    if d_vals_pos and d_counts_pos:
        ax2.scatter(d_vals_pos, d_counts_pos, color='#4C72B0', s=30, alpha=0.7,
                    edgecolors='none')
        ax2.set_xscale('log')
        ax2.set_yscale('log')
        ax2.set_xlabel('Degree (log scale)', fontsize=11)
        ax2.set_ylabel('Count (log scale)', fontsize=11)
        suffix = ' [Heavy-tailed]' if is_power_law else ' [Not heavy-tailed]'
        ax2.set_title(f'Degree Distribution (Log-Log Scale){suffix}', fontsize=13)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_centrality_score_scatter(
    pagerank: Dict[str, float],
    profile_mean_hm: Dict[str, float],
    correlations: Dict[str, Tuple[float, float]],
    out_path: str,
) -> None:
    """Scatter plot of PageRank vs mean harmonic_mean with Spearman rho annotation."""
    nodes_both = [n for n in pagerank if n in profile_mean_hm]
    if len(nodes_both) < 10:
        print("  Skipping centrality scatter plot: insufficient data")
        return

    x = [pagerank[n] for n in nodes_both]
    y = [profile_mean_hm[n] for n in nodes_both]

    rho, pval = correlations.get('PageRank', (float('nan'), float('nan')))

    fig, ax = plt.subplots(figsize=(10, 8))

    ax.scatter(x, y, alpha=0.4, s=20, c='#3498db', edgecolors='none')

    ax.set_xlabel('PageRank Score', fontsize=12)
    ax.set_ylabel('Mean Harmonic Mean (match quality)', fontsize=12)
    ax.set_title('PageRank vs Mean Match Quality', fontsize=14)

    # Spearman annotation
    if not math.isnan(rho):
        annot = f'Spearman $\\rho$ = {rho:+.4f}\np = {pval:.2e}\nn = {len(nodes_both):,d}'
        ax.annotate(
            annot,
            xy=(0.05, 0.95), xycoords='axes fraction',
            ha='left', va='top', fontsize=12,
            bbox=dict(boxstyle='round,pad=0.5', facecolor='lightyellow',
                      edgecolor='gray', alpha=0.9),
        )

    # Linear trend line
    if len(x) >= 3:
        z = np.polyfit(x, y, 1)
        p_fn = np.poly1d(z)
        x_sorted = sorted(x)
        ax.plot(x_sorted, p_fn(x_sorted), '--', color='#e74c3c', alpha=0.6,
                linewidth=2, label='Linear trend')
        ax.legend(loc='lower right', frameon=True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ===========================================================================
# Main
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Network graph structure validation for the JV match network.',
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Use synthetic random graph instead of production database.',
    )
    args = parser.parse_args()

    random.seed(42)
    np.random.seed(42)

    # Output directories
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(PLOTS_DIR, exist_ok=True)

    report_path = os.path.join(RESULTS_DIR, 'network_analysis_report.txt')
    community_csv_path = os.path.join(RESULTS_DIR, 'network_communities.csv')

    print()
    print('=' * 60)
    print('  Network Analysis -- Match Graph Validation')
    print('=' * 60)
    print()

    # ── Load data ─────────────────────────────────────────────────
    if args.test:
        print('  RUNNING IN TEST MODE (synthetic data)\n')
        profile_map, edges = generate_test_data()
    else:
        print('  RUNNING IN LIVE MODE (database)\n')
        profile_map, edges = load_live_data()

    if not edges:
        print('ERROR: No match edges found. Cannot proceed.')
        print('  Try running with --test for synthetic data.')
        sys.exit(1)

    # ── [1/7] Graph construction ──────────────────────────────────
    print('\n[1/7] Building graph (threshold=50)...')
    G_und = build_undirected_graph(edges, min_score=DEFAULT_MIN_SCORE)
    G_dir = build_directed_graph(edges, min_score=DEFAULT_MIN_SCORE)
    topo = analyze_graph_topology(G_und)
    print(f'  Undirected: {topo["nodes"]:,d} nodes, {topo["edges"]:,d} edges, '
          f'density={topo["density"]:.6f}')
    print(f'  Directed:   {G_dir.number_of_nodes():,d} nodes, '
          f'{G_dir.number_of_edges():,d} edges')
    print(f'  Components: {topo["n_components"]}, '
          f'largest CC: {topo["largest_cc_size"]:,d} '
          f'({topo["largest_cc_frac"] * 100:.1f}%)')

    # ── [2/7] Density at multiple thresholds ──────────────────────
    print('\n[2/7] Graph density at multiple thresholds...')
    threshold_results = analyze_density_thresholds(edges, DENSITY_THRESHOLDS)
    for d in threshold_results:
        print(f'  Threshold {d["threshold"]}: {d["nodes"]:,d} nodes, '
              f'{d["edges"]:,d} edges, density={d["density"]:.6f}, '
              f'{d["n_components"]} components')

    # ── [3/7] Community detection ─────────────────────────────────
    print('\n[3/7] Louvain community detection...')
    communities, node_community, modularity = detect_communities(G_und)
    mod_str = f'{modularity:.4f}' if not math.isnan(modularity) else 'N/A'
    print(f'  Found {len(communities)} communities, modularity={mod_str}')
    if communities:
        sizes = [len(c) for c in communities]
        print(f'  Sizes (top 5): {sizes[:5]}')

    # ── [4/7] Community-niche coherence ───────────────────────────
    print('\n[4/7] Community-niche coherence (NMI)...')
    nmi_niche, nmi_role, per_community = compute_nmi(
        communities, node_community, profile_map,
    )
    if not math.isnan(nmi_niche):
        target = 0.20
        status = 'PASS' if nmi_niche > target else 'BELOW TARGET'
        print(f'  NMI vs niche:        {nmi_niche:.4f}  ({status}, target > {target})')
    else:
        print('  NMI vs niche:        N/A')
    if not math.isnan(nmi_role):
        print(f'  NMI vs network_role: {nmi_role:.4f}')
    else:
        print('  NMI vs network_role: N/A')

    # ── [5/7] HITS analysis ───────────────────────────────────────
    print('\n[5/7] HITS analysis...')
    hub_scores, auth_scores, top_hubs, top_auths, hits_overlap = analyze_hits(
        G_dir, profile_map, top_k=20,
    )
    print(f'  Top hub:       {top_hubs[0][1] if top_hubs else "N/A"}')
    print(f'  Top authority: {top_auths[0][1] if top_auths else "N/A"}')
    print(f'  Hub-Authority overlap (top 20): {hits_overlap}')

    # ── [6/7] Centrality-score correlation ────────────────────────
    print('\n[6/7] Centrality-score correlation...')
    corr_result = analyze_centrality_correlation(
        G_und, edges, profile_map, min_score=DEFAULT_MIN_SCORE,
    )
    correlations, pagerank, degree_cent, betweenness, profile_mean_hm = corr_result

    for metric_name, (rho, pval) in correlations.items():
        if math.isnan(rho):
            print(f'  {metric_name}: N/A')
        else:
            print(f'  {metric_name}: Spearman rho = {rho:+.4f}, p = {pval:.2e}')

    # ── [7/7] Degree distribution ─────────────────────────────────
    print('\n[7/7] Degree distribution analysis...')
    degree_info = analyze_degree_distribution(G_und)
    if degree_info['degrees']:
        print(f'  Mean degree: {degree_info["mean"]:.2f}, '
              f'max: {degree_info["max"]}, '
              f'distribution: {degree_info.get("distribution_type", "UNKNOWN")}')

    # ── Generate report ───────────────────────────────────────────
    print('\nGenerating text report...')
    report_text = generate_report(
        is_test=args.test,
        topo=topo,
        threshold_results=threshold_results,
        communities=communities,
        modularity=modularity,
        nmi_niche=nmi_niche,
        nmi_role=nmi_role,
        per_community=per_community,
        top_hubs=top_hubs,
        top_auths=top_auths,
        hits_overlap=hits_overlap,
        correlations=correlations,
        degree_info=degree_info,
    )
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f'  Saved: {report_path}')

    # ── Export community CSV ──────────────────────────────────────
    print('\nExporting community CSV...')
    export_community_csv(communities, node_community, profile_map, community_csv_path)

    # ── Generate plots ────────────────────────────────────────────
    print('\nGenerating visualizations...')
    plot_graph_density_thresholds(
        threshold_results,
        os.path.join(PLOTS_DIR, 'graph_density_thresholds.png'),
    )
    plot_community_sizes(
        communities,
        os.path.join(PLOTS_DIR, 'community_sizes.png'),
    )
    plot_degree_distribution(
        degree_info,
        os.path.join(PLOTS_DIR, 'degree_distribution.png'),
    )
    plot_centrality_score_scatter(
        pagerank, profile_mean_hm, correlations,
        os.path.join(PLOTS_DIR, 'centrality_score_scatter.png'),
    )

    # ── Print report + summary ────────────────────────────────────
    print('\n' + report_text)

    print('\n' + '=' * 60)
    print('  NETWORK ANALYSIS COMPLETE')
    print('=' * 60)
    print(f'\n  Report:       {report_path}')
    print(f'  Communities:  {community_csv_path}')
    print(f'  Plots:        {PLOTS_DIR}/')
    print()

    # Key metrics to stdout
    print('  Key Metrics:')
    print(f'    Graph: {topo["nodes"]:,d} nodes, {topo["edges"]:,d} edges')
    print(f'    Communities: {len(communities)}')
    if not math.isnan(nmi_niche):
        status = 'PASS' if nmi_niche > 0.20 else 'BELOW TARGET'
        print(f'    NMI vs Niche: {nmi_niche:.4f} ({status})')
    if not math.isnan(nmi_role):
        print(f'    NMI vs Role:  {nmi_role:.4f}')
    pr_rho = correlations.get('PageRank', (float('nan'), float('nan')))[0]
    if not math.isnan(pr_rho):
        print(f'    PageRank-Score Spearman: {pr_rho:+.4f}')
    if degree_info['degrees']:
        print(f'    Degree distribution: {degree_info.get("distribution_type", "UNKNOWN")}')
    print()


if __name__ == '__main__':
    main()
