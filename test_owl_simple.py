#!/usr/bin/env python3
"""
Simple OWL Test Script

Run this to test OWL with a single profile.
No Django setup required - uses the OWL agent directly.
"""

import json
import sys
from pathlib import Path

# Add project to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import the OWL agent
from matching.enrichment.owl_research.agents.owl_agent import OWLEnrichmentAgent

def main():
    print("=" * 70)
    print("OWL Profile Enrichment Test")
    print("=" * 70)
    print()
    
    # Create agent (120 second timeout)
    agent = OWLEnrichmentAgent(timeout=120)
    
    # Test profile - modify these values
    name = "Janet Bray Attwood"
    company = "The Passion Test"
    website = "https://thepassiontest.com"
    linkedin = ""  # Optional
    
    print(f"Researching: {name}")
    print(f"Company: {company}")
    print(f"Website: {website}")
    print()
    print("Running OWL research (this may take 1-2 minutes)...")
    print()
    
    try:
        # Run enrichment
        result = agent.enrich_profile(
            name=name,
            company=company,
            website=website,
            linkedin=linkedin,
        )
        
        # Print results
        print("=" * 70)
        print("RESULTS")
        print("=" * 70)
        print()
        
        if result.get("success"):
            print("✅ Research completed successfully!")
            print()
            print(json.dumps(result, indent=2))
        else:
            print("❌ Research failed")
            print()
            print(f"Error: {result.get('error', 'Unknown error')}")
            if result.get("enriched_data"):
                print("\nPartial results:")
                print(json.dumps(result.get("enriched_data"), indent=2))
        
    except Exception as e:
        print("=" * 70)
        print("ERROR")
        print("=" * 70)
        print(f"An error occurred: {e}")
        print()
        print("Troubleshooting:")
        print("1. Check that OPENROUTER_API_KEY is set in .env")
        print("2. Verify OWL framework is set up: owl_framework/.venv")
        print("3. Check network connection")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
