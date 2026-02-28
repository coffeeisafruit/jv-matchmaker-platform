"""
Pydantic schemas for JV Matchmaker profile enrichment.

CRITICAL: Every extracted field MUST have source verification.
Data without source citations is rejected.
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class VerifiedField(BaseModel):
    """
    A field value with required source verification.

    CRITICAL: Data without source_quote is considered unverified and will be rejected.
    """
    value: str = ""
    source_quote: str = ""  # Direct quote from source that proves this data
    source_url: str = ""    # URL where data was found
    confidence: float = 0.0  # 0.0-1.0 confidence for this specific field

    def is_verified(self) -> bool:
        """A field is only verified if it has both value AND source quote."""
        return bool(self.value.strip() and self.source_quote.strip())

    def __str__(self) -> str:
        return self.value


class VerifiedList(BaseModel):
    """A list of values with source verification."""
    values: List[str] = Field(default_factory=list)
    source_quote: str = ""
    source_url: str = ""
    confidence: float = 0.0

    def is_verified(self) -> bool:
        return bool(self.values and self.source_quote.strip())


class CompanyInfo(BaseModel):
    """Company/business information with verification."""
    name: VerifiedField = Field(default_factory=VerifiedField)
    website: VerifiedField = Field(default_factory=VerifiedField)
    industry: VerifiedField = Field(default_factory=VerifiedField)
    size: VerifiedField = Field(default_factory=VerifiedField)
    description: VerifiedField = Field(default_factory=VerifiedField)


class IdealCustomer(BaseModel):
    """Ideal Customer Profile (ICP) with verification."""
    description: VerifiedField = Field(default_factory=VerifiedField)
    industries: VerifiedList = Field(default_factory=VerifiedList)
    company_size: VerifiedField = Field(default_factory=VerifiedField)
    pain_points_solved: VerifiedList = Field(default_factory=VerifiedList)


class PartnershipSeeking(BaseModel):
    """Partnership goals with verification."""
    partnership_types: VerifiedList = Field(default_factory=VerifiedList)
    ideal_partner_profile: VerifiedField = Field(default_factory=VerifiedField)
    goals: VerifiedList = Field(default_factory=VerifiedList)


class EnrichedProfile(BaseModel):
    """
    Fully enriched profile with VERIFIED data only.

    Every field includes source citations.
    Unverified fields should be left empty.
    """
    full_name: VerifiedField = Field(default_factory=VerifiedField)
    title: VerifiedField = Field(default_factory=VerifiedField)
    company: CompanyInfo = Field(default_factory=CompanyInfo)
    # CONTACT INFO - Critical for outreach
    email: VerifiedField = Field(default_factory=VerifiedField)
    phone: VerifiedField = Field(default_factory=VerifiedField)
    booking_link: VerifiedField = Field(default_factory=VerifiedField)
    offerings: VerifiedList = Field(default_factory=VerifiedList)
    # IMPORTANT: Capture SPECIFIC named programs, courses, books, certifications
    signature_programs: VerifiedList = Field(default_factory=VerifiedList)
    ideal_customer: IdealCustomer = Field(default_factory=IdealCustomer)
    seeking: PartnershipSeeking = Field(default_factory=PartnershipSeeking)
    matching_keywords: List[str] = Field(default_factory=list)
    linkedin_url: VerifiedField = Field(default_factory=VerifiedField)

    # Overall metrics
    overall_confidence: float = 0.0
    all_sources: List[str] = Field(default_factory=list)
    verification_summary: str = ""

    def get_verified_field_count(self) -> int:
        """Count how many fields have verified data."""
        count = 0
        if self.full_name.is_verified(): count += 1
        if self.title.is_verified(): count += 1
        if self.company.name.is_verified(): count += 1
        if self.company.website.is_verified(): count += 1
        if self.company.description.is_verified(): count += 1
        # Contact info (critical for outreach)
        if self.email.is_verified(): count += 1
        if self.phone.is_verified(): count += 1
        if self.booking_link.is_verified(): count += 1
        if self.offerings.is_verified(): count += 1
        if self.ideal_customer.description.is_verified(): count += 1
        if self.seeking.partnership_types.is_verified(): count += 1
        if self.linkedin_url.is_verified(): count += 1
        return count

    def get_verification_report(self) -> str:
        """Generate a verification report showing what was verified."""
        lines = ["VERIFICATION REPORT", "=" * 40]

        def check(name: str, field) -> str:
            if hasattr(field, 'is_verified'):
                verified = field.is_verified()
                status = "✓ VERIFIED" if verified else "✗ unverified"
                if verified:
                    return f"{status}: {name}\n  Source: \"{field.source_quote[:60]}...\""
                return f"{status}: {name}"
            return f"?: {name}"

        lines.append(check("Full Name", self.full_name))
        lines.append(check("Title", self.title))
        lines.append(check("Company Name", self.company.name))
        lines.append(check("Company Website", self.company.website))
        lines.append(check("Company Description", self.company.description))
        # Contact info (critical)
        lines.append(check("Email", self.email))
        lines.append(check("Phone", self.phone))
        lines.append(check("Booking Link", self.booking_link))
        lines.append(check("Offerings", self.offerings))
        lines.append(check("Ideal Customer", self.ideal_customer.description))
        lines.append(check("Partnership Types", self.seeking.partnership_types))
        lines.append(check("LinkedIn URL", self.linkedin_url))

        lines.append("")
        lines.append(f"Verified Fields: {self.get_verified_field_count()}/12")
        lines.append(f"Overall Confidence: {self.overall_confidence:.2f}")

        return "\n".join(lines)


class ProfileEnrichmentResult(BaseModel):
    """Complete result for a profile enrichment job."""
    input_name: str
    input_email: str = ""
    input_company: str = ""
    input_linkedin: str = ""
    enriched: Optional[EnrichedProfile] = None
    research_timestamp: datetime = Field(default_factory=datetime.now)
    error: Optional[str] = None

    def to_jv_matcher_format(self) -> dict:
        """
        Convert to JV Matcher format, INCLUDING source citations.

        Only includes VERIFIED data with sources.
        """
        if not self.enriched:
            return {}

        e = self.enriched
        result = {}

        # seeking -> partnership goals and types (only if verified)
        if e.seeking.partnership_types.is_verified():
            seeking_parts = [", ".join(e.seeking.partnership_types.values)]
            if e.seeking.goals.is_verified():
                seeking_parts.append("; ".join(e.seeking.goals.values))
            result['seeking'] = ". ".join(seeking_parts)
            result['seeking_source'] = e.seeking.partnership_types.source_quote
            result['seeking_source_url'] = e.seeking.partnership_types.source_url

        # who_you_serve -> ideal customer description (only if verified)
        if e.ideal_customer.description.is_verified():
            result['who_you_serve'] = e.ideal_customer.description.value
            result['who_you_serve_source'] = e.ideal_customer.description.source_quote
            result['who_you_serve_source_url'] = e.ideal_customer.description.source_url

        # what_you_do -> company description (only if verified)
        if e.company.description.is_verified():
            what_parts = [e.company.description.value]
            if e.offerings.is_verified():
                what_parts.append("Offers: " + ", ".join(e.offerings.values[:3]))
            result['what_you_do'] = " ".join(what_parts)
            result['what_you_do_source'] = e.company.description.source_quote
            result['what_you_do_source_url'] = e.company.description.source_url

        # offering -> what they can offer to partners (only if verified)
        if e.offerings.is_verified():
            result['offering'] = ", ".join(e.offerings.values)
            result['offering_source'] = e.offerings.source_quote
            result['offering_source_url'] = e.offerings.source_url

        # signature_programs -> SPECIFIC named programs, courses, books (high value for matching)
        if e.signature_programs.is_verified():
            result['signature_programs'] = ", ".join(e.signature_programs.values)
            result['signature_programs_source'] = e.signature_programs.source_quote
            result['signature_programs_source_url'] = e.signature_programs.source_url

        # Additional fields (only if verified)
        if e.linkedin_url.is_verified():
            result['linkedin'] = e.linkedin_url.value
            result['linkedin_source'] = e.linkedin_url.source_url

        if e.company.website.is_verified():
            result['website'] = e.company.website.value

        if e.title.is_verified() and e.full_name.is_verified():
            result['bio'] = f"{e.full_name.value}, {e.title.value}"
            result['bio_source'] = e.title.source_quote

        # CONTACT INFO - Critical for outreach
        if e.email.is_verified():
            result['email'] = e.email.value
            result['email_source'] = e.email.source_url

        if e.phone.is_verified():
            result['phone'] = e.phone.value
            result['phone_source'] = e.phone.source_url

        if e.booking_link.is_verified():
            result['booking_link'] = e.booking_link.value
            result['booking_link_source'] = e.booking_link.source_url

        # Verification metadata
        result['_confidence'] = e.overall_confidence
        result['_verified_fields'] = e.get_verified_field_count()
        result['_all_sources'] = e.all_sources
        result['_verification_summary'] = e.verification_summary
        result['_keywords'] = e.matching_keywords

        return result


class BatchProgress(BaseModel):
    """Track progress of batch enrichment job."""
    total_profiles: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    total_cost_usd: float = 0.0
    avg_confidence: float = 0.0
    avg_verified_fields: float = 0.0  # Average verified fields per profile
    last_processed_index: int = -1
    started_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
