"""
OWL Enrichment Service - Full Integration

Combines OWL's deep research with verified data schemas.
This is the main entry point for profile enrichment.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from matching.enrichment.owl_research.schemas.profile_schema import (
    CompanyInfo,
    EnrichedProfile,
    IdealCustomer,
    PartnershipSeeking,
    ProfileEnrichmentResult,
    VerifiedField,
    VerifiedList,
)
from matching.enrichment.owl_research.agents.owl_sdk_agent import OWLProfileEnricher

logger = logging.getLogger(__name__)


class OWLEnrichmentService:
    """
    Main enrichment service using OWL for research.

    Converts OWL's research output into verified profile schemas
    that integrate with the JV Matcher system.
    """

    def __init__(self):
        self.enricher = OWLProfileEnricher()
        self.profiles_processed = 0

    async def enrich_profile(
        self,
        name: str,
        email: str = "",
        company: str = "",
        website: str = "",
        linkedin: str = "",
        existing_data: Optional[Dict] = None,
    ) -> ProfileEnrichmentResult:
        """
        Research and enrich a profile using OWL.

        Returns ProfileEnrichmentResult with verified fields.
        """
        result = ProfileEnrichmentResult(
            input_name=name,
            input_email=email,
            input_company=company,
            input_linkedin=linkedin,
        )

        try:
            # Run OWL research
            owl_result = await self.enricher.enrich_profile(
                name=name,
                company=company,
                website=website,
                linkedin=linkedin,
                existing_data=existing_data,
            )

            if owl_result.get("enriched") and not owl_result["enriched"].get("fallback"):
                # Convert OWL output to verified schema
                enriched = self._convert_to_verified_profile(
                    owl_result["enriched"],
                    owl_result.get("sources", []),
                )
                result.enriched = enriched
                self.profiles_processed += 1

                logger.info(f"Enriched {name}: {enriched.get_verified_field_count()}/9 verified fields")
            else:
                result.error = owl_result.get("enriched", {}).get("verification_summary", "No data found")

        except Exception as e:
            logger.error(f"Error enriching {name}: {e}")
            result.error = str(e)

        return result

    def _convert_to_verified_profile(
        self,
        owl_data: Dict,
        sources: List[str],
    ) -> EnrichedProfile:
        """Convert OWL's output to EnrichedProfile with verified fields."""

        def make_verified_field(data: Dict) -> VerifiedField:
            """Convert OWL field to VerifiedField."""
            if not isinstance(data, dict):
                return VerifiedField()

            # Handle both "value" (string) and "values" (list) formats
            value = data.get("value", "")
            if not value and data.get("values"):
                # Convert list to comma-separated string
                values = data.get("values", [])
                if isinstance(values, list):
                    value = ", ".join(str(v) for v in values)
                else:
                    value = str(values)

            source_quote = data.get("source_quote", "")
            source_url = data.get("source_url", "")

            # Calculate confidence based on verification
            confidence = 0.0
            if value and source_quote:
                confidence = 0.85
            elif value and source_url:
                confidence = 0.7
            elif value:
                confidence = 0.5

            return VerifiedField(
                value=str(value) if value else "",
                source_quote=str(source_quote) if source_quote else "",
                source_url=str(source_url) if source_url else "",
                confidence=confidence,
            )

        def make_verified_list(data: Dict) -> VerifiedList:
            """Convert OWL list to VerifiedList."""
            if not isinstance(data, dict):
                return VerifiedList()

            values = data.get("values", [])
            if isinstance(values, str):
                values = [values]

            source_quote = data.get("source_quote", "")
            source_url = data.get("source_url", "")

            confidence = 0.0
            if values and source_quote:
                confidence = 0.85
            elif values and source_url:
                confidence = 0.7
            elif values:
                confidence = 0.5

            return VerifiedList(
                values=values if isinstance(values, list) else [],
                source_quote=str(source_quote) if source_quote else "",
                source_url=str(source_url) if source_url else "",
                confidence=confidence,
            )

        # Extract company info
        company_data = owl_data.get("company", {})
        if isinstance(company_data, str):
            company_data = {"name": {"value": company_data}}

        company_info = CompanyInfo(
            name=make_verified_field(company_data.get("name", {})),
            website=make_verified_field(company_data.get("website", {})),
            industry=make_verified_field(company_data.get("industry", {})),
            description=make_verified_field(company_data.get("description", {})),
        )

        # Extract ideal customer
        icp_data = owl_data.get("ideal_customer", {})
        ideal_customer = IdealCustomer(
            description=make_verified_field(
                owl_data.get("who_they_serve", icp_data.get("description", {}))
            ),
            industries=make_verified_list(icp_data.get("industries", {})),
        )

        # Extract partnership seeking
        seeking_data = owl_data.get("seeking", {})
        if isinstance(seeking_data, dict) and "values" in seeking_data:
            partnership_seeking = PartnershipSeeking(
                partnership_types=make_verified_list(seeking_data),
                goals=VerifiedList(),
            )
        else:
            partnership_seeking = PartnershipSeeking(
                partnership_types=make_verified_list(seeking_data.get("partnership_types", {})),
                goals=make_verified_list(seeking_data.get("goals", {})),
            )

        # Build the enriched profile
        profile = EnrichedProfile(
            full_name=make_verified_field(owl_data.get("full_name", {})),
            title=make_verified_field(owl_data.get("title", {})),
            company=company_info,
            # CONTACT INFO - Critical for outreach
            email=make_verified_field(owl_data.get("email", {})),
            phone=make_verified_field(owl_data.get("phone", {})),
            booking_link=make_verified_field(owl_data.get("booking_link", {})),
            offerings=make_verified_list(owl_data.get("offerings", {})),
            signature_programs=make_verified_list(owl_data.get("signature_programs", {})),
            ideal_customer=ideal_customer,
            seeking=partnership_seeking,
            linkedin_url=make_verified_field(owl_data.get("linkedin_url", {})),
            matching_keywords=owl_data.get("matching_keywords", []),
            all_sources=sources[:10],
            verification_summary=owl_data.get("verification_summary", ""),
        )

        # Calculate overall confidence (now 12 possible verified fields)
        profile.overall_confidence = profile.get_verified_field_count() / 12.0

        return profile

    def get_stats(self) -> Dict:
        """Get processing statistics."""
        return {
            "profiles_processed": self.profiles_processed,
            "owl_stats": self.enricher.get_stats(),
        }


async def enrich_profile_with_owl(
    name: str,
    email: str = "",
    company: str = "",
    website: str = "",
    linkedin: str = "",
    existing_data: Optional[Dict] = None,
) -> Tuple[Dict, bool]:
    """
    Convenience function to enrich a single profile with OWL.

    Returns (jv_matcher_format_dict, success_bool)
    """
    service = OWLEnrichmentService()
    result = await service.enrich_profile(
        name=name,
        email=email,
        company=company,
        website=website,
        linkedin=linkedin,
        existing_data=existing_data,
    )

    if result.enriched:
        jv_data = result.to_jv_matcher_format()
        logger.info(result.enriched.get_verification_report())
        return jv_data, True

    return existing_data or {}, False


def enrich_profile_with_owl_sync(
    name: str,
    email: str = "",
    company: str = "",
    website: str = "",
    linkedin: str = "",
    existing_data: Optional[Dict] = None,
) -> Tuple[Dict, bool]:
    """Synchronous wrapper for enrich_profile_with_owl."""
    return asyncio.run(
        enrich_profile_with_owl(name, email, company, website, linkedin, existing_data)
    )


# Test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    jv_data, success = enrich_profile_with_owl_sync(
        name="David Riklan",
        company="SelfGrowth.com",
        website="https://selfgrowth.com",
    )

    print(f"\nSuccess: {success}")
    print(f"Verified fields: {jv_data.get('_verified_fields', 0)}/9")
    print(f"Confidence: {jv_data.get('_confidence', 0):.2%}")
    print(f"\nJV Matcher Format:")
    for key, value in jv_data.items():
        if not key.startswith("_") and value:
            print(f"  {key}: {str(value)[:100]}...")
