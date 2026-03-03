"""
Prefect @task: Prospect discovery using Exa Websets + Apollo People Search.

Discovers new potential JV partners for a client by building semantic
search queries from the client's profile and gap analysis, then querying
Exa's search/find-similar APIs and Apollo's structured People Search.

Discovery strategy (layered by cost):
  1. Exa Websets for bulk semantic discovery (primary, cheapest per result)
  2. Exa find-similar for "find people like top matches"
  3. Exa niche-specific supplementary keyword queries
  4. Apollo People Search for structured discovery (titles, seniority, industry)

Budget target: ~$0.50-1.25 per acquisition run.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

from prefect import task, get_run_logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum per-query budget as a fraction of total budget
_QUERY_BUDGET_FRACTION = 0.40

# Domains to skip when discovering prospect websites
_SKIP_DOMAINS = [
    "amazon.", "wikipedia.", "linkedin.com/company",
    "youtube.com", "facebook.com", "instagram.com",
    "twitter.com", "x.com", "reddit.", "tiktok.",
    "spotify.", "apple.com", "goodreads.", "imdb.",
    "crunchbase.", "glassdoor.", "yelp.",
]


# ---------------------------------------------------------------------------
# Network expansion helper
# ---------------------------------------------------------------------------

def _extract_partner_names(top_match_ids: list[str], max_names: int = 5) -> list[str]:
    """Read jv_history from top matches and extract partner names for discovery.

    Queries the database for profiles with jv_history, then extracts
    unique partner names that can be used as Exa search queries.
    """
    if not top_match_ids:
        return []

    import os
    import psycopg2
    from psycopg2.extras import RealDictCursor

    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        return []

    try:
        conn = psycopg2.connect(dsn)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # Fetch jv_history for top matches (limit to 10 for efficiency)
        cursor.execute(
            "SELECT jv_history FROM profiles "
            "WHERE id = ANY(%s::uuid[]) AND jv_history IS NOT NULL",
            ([str(uid) for uid in top_match_ids[:10]],),
        )
        rows = cursor.fetchall()
        conn.close()
    except Exception:
        return []

    names: list[str] = []
    seen: set[str] = set()
    for row in rows:
        jv_history = row.get("jv_history") or []
        if isinstance(jv_history, str):
            import json
            try:
                jv_history = json.loads(jv_history)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(jv_history, list):
            continue
        for entry in jv_history:
            if isinstance(entry, dict):
                name = (entry.get("partner_name") or "").strip()
            elif isinstance(entry, str):
                name = entry.strip()
            else:
                continue
            name_lower = name.lower()
            if name and len(name) > 3 and name_lower not in seen:
                seen.add(name_lower)
                names.append(name)
                if len(names) >= max_names:
                    return names

    return names


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------

def _build_primary_query(client_profile: dict, gap_analysis: dict) -> str:
    """Build the primary Exa search query from client profile data.

    Combines niche, seeking, who_you_serve, and audience_type fields
    into a semantically rich search query.
    """
    parts: list[str] = []

    niche = (client_profile.get("niche") or "").strip()
    if niche:
        parts.append(niche)

    seeking = (client_profile.get("seeking") or "").strip()
    if seeking:
        # Extract key phrases from seeking
        parts.append(seeking[:120])

    who_you_serve = (client_profile.get("who_you_serve") or "").strip()
    if who_you_serve:
        parts.append(f"serves {who_you_serve[:80]}")

    audience_type = (client_profile.get("audience_type") or "").strip()
    if audience_type and audience_type not in " ".join(parts).lower():
        parts.append(audience_type)

    # Add niche gaps from gap analysis to diversify results
    niche_gaps = gap_analysis.get("niche_gaps", [])
    if niche_gaps:
        parts.append(" ".join(niche_gaps[:3]))

    # Ensure we have at least something to search for
    if not parts:
        name = client_profile.get("name", "")
        what_you_do = client_profile.get("what_you_do", "")
        parts.append(f"{name} {what_you_do}".strip() or "business coach")

    # Add JV-oriented qualifiers
    parts.append("coach author speaker entrepreneur")

    query = " ".join(parts)
    return query[:500]  # Exa query length limit


def _build_niche_queries(client_profile: dict, gap_analysis: dict) -> list[str]:
    """Build supplementary niche-specific queries from gap analysis.

    Returns 1-3 focused queries targeting underrepresented niches.
    """
    queries: list[str] = []

    niche_gaps = gap_analysis.get("niche_gaps", [])
    who_you_serve = (client_profile.get("who_you_serve") or "").strip()

    for niche in niche_gaps[:3]:
        query = f"{niche} expert coach speaker"
        if who_you_serve:
            query += f" {who_you_serve[:50]}"
        queries.append(query)

    return queries


# ---------------------------------------------------------------------------
# Exa API interaction helpers
# ---------------------------------------------------------------------------

def _exa_search(
    service: Any,
    query: str,
    num_results: int = 25,
    logger: Any = None,
) -> tuple[list[dict], float]:
    """Execute an Exa search and return (prospects, cost).

    Uses Exa's search API with contents=False for cheap discovery,
    then extracts basic metadata from results.
    """
    try:
        result = service.client.search(
            query=query,
            type="auto",
            num_results=num_results,
            contents=False,
        )
    except Exception as exc:
        if logger:
            logger.warning("Exa search failed for query '%s': %s", query[:80], exc)
        return [], 0.0

    cost = result.cost_dollars.total if result.cost_dollars else 0.0

    prospects: list[dict] = []
    for r in result.results:
        url = r.url or ""

        # Skip non-personal-website URLs
        if any(d in url.lower() for d in _SKIP_DOMAINS):
            continue

        # Determine if this is a LinkedIn profile
        linkedin = ""
        website = ""
        if "linkedin.com/in/" in url:
            linkedin = url
        else:
            website = url

        prospect = {
            "name": (r.title or "").strip(),
            "website": website,
            "linkedin": linkedin,
            "company": "",
            "niche": "",
            "what_you_do": "",
            "source": "exa_search",
            "source_query": query[:200],
            "discovery_cost": cost / max(len(result.results), 1),
            "raw_data": {
                "url": url,
                "title": r.title or "",
                "published_date": str(r.published_date) if r.published_date else "",
            },
        }
        prospects.append(prospect)

    return prospects, cost


def _exa_find_similar(
    service: Any,
    url: str,
    num_results: int = 15,
    logger: Any = None,
) -> tuple[list[dict], float]:
    """Use Exa find-similar to discover prospects like an existing match.

    Finds websites similar to the given URL, returning basic metadata.
    """
    try:
        result = service.client.find_similar(
            url=url,
            num_results=num_results,
        )
    except Exception as exc:
        if logger:
            logger.warning("Exa find-similar failed for %s: %s", url, exc)
        return [], 0.0

    cost = result.cost_dollars.total if result.cost_dollars else 0.0

    prospects: list[dict] = []
    for r in result.results:
        found_url = r.url or ""
        if any(d in found_url.lower() for d in _SKIP_DOMAINS):
            continue

        linkedin = ""
        website = ""
        if "linkedin.com/in/" in found_url:
            linkedin = found_url
        else:
            website = found_url

        prospect = {
            "name": (r.title or "").strip(),
            "website": website,
            "linkedin": linkedin,
            "company": "",
            "niche": "",
            "what_you_do": "",
            "source": "exa_find_similar",
            "source_query": f"similar_to:{url}",
            "discovery_cost": cost / max(len(result.results), 1),
            "raw_data": {
                "url": found_url,
                "title": r.title or "",
                "similar_to": url,
            },
        }
        prospects.append(prospect)

    return prospects, cost


# ---------------------------------------------------------------------------
# Main discovery task
# ---------------------------------------------------------------------------

@task(name="discover-prospects", retries=2, retry_delay_seconds=30)
def discover_prospects(
    client_profile: dict,
    gap_analysis: dict,
    max_results: int = 100,
    budget: float = 0.50,
) -> list[dict]:
    """Discover new prospect candidates using Exa Websets + supplementary tools.

    Strategy (layered by cost):
      1. Exa search for bulk semantic discovery (primary)
      2. Exa find-similar for "find people like top matches"
      3. Supplementary niche-specific queries from gap analysis

    Each prospect dict contains: name, website, linkedin, company, niche,
    what_you_do, source, discovery_cost, raw_data

    Parameters
    ----------
    client_profile:
        Client profile dict with niche, seeking, who_you_serve, etc.
    gap_analysis:
        Output from detect_match_gaps with niche_gaps, gap count, etc.
    max_results:
        Maximum number of prospects to return.
    budget:
        Maximum dollar budget for this discovery run.

    Returns
    -------
    list[dict]
        Discovered prospects, deduplicated, with cost tracking.
    """
    logger = get_run_logger()
    client_name = client_profile.get("name", "Unknown")

    # Initialize Exa service
    try:
        from matching.enrichment.exa_research import ExaResearchService
        exa_service = ExaResearchService()
        if not exa_service.available:
            logger.error("Exa API key not configured -- cannot discover prospects")
            return []
    except ImportError:
        logger.error("ExaResearchService not available")
        return []

    all_prospects: list[dict] = []
    total_cost = 0.0
    seen_urls: set[str] = set()

    def _dedup_and_add(prospects: list[dict]) -> int:
        """Add prospects to the master list, deduplicating by URL."""
        added = 0
        for p in prospects:
            # Use website or linkedin as dedup key
            key = (p.get("website") or p.get("linkedin") or "").lower().strip()
            if not key or key in seen_urls:
                continue
            seen_urls.add(key)
            all_prospects.append(p)
            added += 1
        return added

    # ------------------------------------------------------------------
    # Layer 0: Network expansion (partners of existing good matches)
    # ------------------------------------------------------------------
    top_match_ids = gap_analysis.get("top_match_ids", [])
    if top_match_ids:
        partner_names = _extract_partner_names(top_match_ids)
        if partner_names:
            logger.info(
                "Discovery Layer 0: network expansion — %d partner names from jv_history",
                len(partner_names),
            )
            for i, name in enumerate(partner_names):
                if total_cost >= budget * 0.3:
                    logger.info("Layer 0: budget fraction reached, moving to Layer 1")
                    break
                if len(all_prospects) >= max_results:
                    break

                network_query = f"{name} joint venture partner coach speaker"
                network_prospects, cost = _exa_search(
                    exa_service, network_query,
                    num_results=min(10, max_results - len(all_prospects)),
                    logger=logger,
                )
                total_cost += cost
                added = _dedup_and_add(network_prospects)
                logger.info(
                    "Layer 0.%d: '%s' → %d results, %d unique, $%.4f",
                    i, name[:30], len(network_prospects), added, cost,
                )

    # ------------------------------------------------------------------
    # Layer 1: Primary semantic search (largest allocation)
    # ------------------------------------------------------------------
    primary_query = _build_primary_query(client_profile, gap_analysis)
    logger.info(
        "Discovery Layer 1: primary search for %s -- query: '%s'",
        client_name, primary_query[:100],
    )

    primary_results = min(50, max_results)
    prospects, cost = _exa_search(
        exa_service, primary_query,
        num_results=primary_results, logger=logger,
    )
    total_cost += cost
    added = _dedup_and_add(prospects)
    logger.info(
        "Layer 1: %d results, %d unique, $%.4f cost",
        len(prospects), added, cost,
    )

    # ------------------------------------------------------------------
    # Layer 2: Find-similar (if we have top match websites)
    # ------------------------------------------------------------------
    if total_cost < budget * 0.7 and len(all_prospects) < max_results:
        top_niches = gap_analysis.get("top_niches", [])
        # Use the client's own website for find-similar if available
        client_website = (client_profile.get("website") or "").strip()
        if client_website:
            logger.info(
                "Discovery Layer 2: find-similar for %s", client_website
            )
            similar_prospects, cost = _exa_find_similar(
                exa_service, client_website,
                num_results=min(20, max_results - len(all_prospects)),
                logger=logger,
            )
            total_cost += cost
            added = _dedup_and_add(similar_prospects)
            logger.info(
                "Layer 2: %d results, %d unique, $%.4f cost",
                len(similar_prospects), added, cost,
            )

    # ------------------------------------------------------------------
    # Layer 3: Niche-specific supplementary queries
    # ------------------------------------------------------------------
    if total_cost < budget * 0.9 and len(all_prospects) < max_results:
        niche_queries = _build_niche_queries(client_profile, gap_analysis)
        for i, niche_query in enumerate(niche_queries):
            if total_cost >= budget * 0.95:
                logger.info("Budget limit approaching -- stopping niche queries")
                break
            if len(all_prospects) >= max_results:
                break

            logger.info(
                "Discovery Layer 3.%d: niche query '%s'", i, niche_query[:80]
            )
            niche_prospects, cost = _exa_search(
                exa_service, niche_query,
                num_results=min(15, max_results - len(all_prospects)),
                logger=logger,
            )
            total_cost += cost
            added = _dedup_and_add(niche_prospects)
            logger.info(
                "Layer 3.%d: %d results, %d unique, $%.4f cost",
                i, len(niche_prospects), added, cost,
            )

    # ------------------------------------------------------------------
    # Layer 4: Apollo People Search (structured discovery)
    # ------------------------------------------------------------------
    if len(all_prospects) < max_results:
        try:
            from matching.enrichment.apollo_query_builder import build_apollo_queries
            from matching.enrichment.apollo_enrichment import ApolloEnrichmentService
            from matching.enrichment.cost_guard import get_cost_guard, BudgetExceededError

            apollo_svc = ApolloEnrichmentService()
            if apollo_svc.api_key:
                apollo_queries = build_apollo_queries(client_profile, gap_analysis)
                for i, query_params in enumerate(apollo_queries):
                    if len(all_prospects) >= max_results:
                        break

                    # Budget guard check before each Apollo query
                    try:
                        get_cost_guard().check_budget(
                            "apollo_discovery", estimated_cost=0.75,
                        )
                    except BudgetExceededError:
                        logger.warning(
                            "Budget exceeded — stopping Apollo discovery"
                        )
                        break

                    apollo_results = apollo_svc.search_people(
                        **query_params,
                        max_results=min(25, max_results - len(all_prospects)),
                    )

                    # Track cost per query
                    query_cost = len(apollo_results) * 0.03
                    total_cost += query_cost
                    added = _dedup_and_add(apollo_results)

                    # Log to JSONL cost tracker
                    try:
                        import json
                        from matching.enrichment.flows.cost_tracking import (
                            log_search_cost, CostEntry,
                        )
                        log_search_cost(CostEntry(
                            tool="apollo_discovery",
                            query=json.dumps(query_params)[:200],
                            cost=query_cost,
                            results_returned=len(apollo_results),
                            results_useful=added,
                            context=f"acquisition_for_{client_profile.get('id', '')}",
                            profile_id=client_profile.get("id", ""),
                        ))
                    except Exception:
                        pass  # cost logging should never block discovery

                    logger.info(
                        "Layer 4.%d (Apollo): %d results, %d unique, $%.3f cost",
                        i, len(apollo_results), added, query_cost,
                    )
            else:
                logger.info("Layer 4 (Apollo): skipped — API key not configured")
        except ImportError as exc:
            logger.warning("Layer 4 (Apollo): skipped — %s", exc)

    # ------------------------------------------------------------------
    # Finalize: trim to max_results and attach total cost
    # ------------------------------------------------------------------
    all_prospects = all_prospects[:max_results]

    # Distribute total cost evenly for reporting
    per_prospect_cost = total_cost / len(all_prospects) if all_prospects else 0.0
    for p in all_prospects:
        p["discovery_cost"] = round(per_prospect_cost, 6)

    logger.info(
        "Discovery complete for %s: %d prospects, $%.4f total cost",
        client_name, len(all_prospects), total_cost,
    )

    return all_prospects
