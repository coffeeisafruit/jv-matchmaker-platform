"""
OWL-Powered Profile Enrichment Agent

Uses CAMEL-AI's OWL framework for deep multi-source research.
This agent can:
- Browse websites directly (not just search snippets)
- Parse PDFs, documents
- Search multiple engines
- Cross-reference sources for verification
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# OWL runs in its own venv (Python 3.12)
OWL_VENV = Path(__file__).parent.parent.parent.parent.parent / "owl_framework" / ".venv"
OWL_PYTHON = OWL_VENV / "bin" / "python"


def create_owl_research_script(
    name: str,
    company: str = "",
    website: str = "",
    linkedin: str = "",
) -> str:
    """Generate the OWL research script for a profile."""

    task_prompt = f"""Research and compile a comprehensive profile for {name}{f' from {company}' if company else ''}.

RESEARCH GOALS:
1. Find their current role and title
2. What does their business/company do? What products/services/programs do they offer?
3. Who is their target audience? Who do they serve?
4. What partnerships are they seeking? What collaborations interest them?
5. Find SPECIFIC named programs, courses, books, or certifications they offer
6. Find their LinkedIn profile URL if not provided

SOURCES TO CHECK:
{f'- Website: {website}' if website else ''}
{f'- LinkedIn: {linkedin}' if linkedin else ''}
- Search engines for recent interviews, podcasts, articles
- Their social media presence

OUTPUT FORMAT:
Return a JSON object with these exact fields (include source URLs for each):
{{
    "full_name": "{name}",
    "title": "",
    "company_name": "",
    "company_description": "",
    "offerings": [],
    "signature_programs": [],  // SPECIFIC named programs, courses, books
    "who_they_serve": "",
    "seeking": "",  // partnership goals
    "linkedin_url": "{linkedin if linkedin else ''}",
    "sources": []  // list of URLs used
}}

IMPORTANT: Only include information you can verify from actual sources. Include the source URL for each piece of data."""

    script = f'''
import sys
import pathlib
import json
import os
from dotenv import load_dotenv
from camel.models import ModelFactory
from camel.toolkits import (
    SearchToolkit,
    BrowserToolkit,
)
from camel.types import ModelPlatformType, ModelType
from camel.societies import RolePlaying

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from owl.utils import run_society, DocumentProcessingToolkit

# Load environment
# The temp script runs from owl_framework directory
# So we need to find the owl/.env file relative to owl_framework
script_dir = pathlib.Path(__file__).parent
if script_dir.name == "owl_framework":
    # Temp file is directly in owl_framework
    owl_env_path = script_dir / "owl" / ".env"
    parent_env_path = script_dir.parent / ".env"
else:
    # Fallback: try relative to script location
    owl_env_path = script_dir / "owl" / ".env"
    parent_env_path = script_dir.parent / ".env"

# Load OWL .env file
if owl_env_path.exists():
    load_dotenv(dotenv_path=str(owl_env_path), override=False)

# Also load from parent .env if it exists (don't override)
if parent_env_path.exists():
    load_dotenv(dotenv_path=str(parent_env_path), override=False)

# Debug: Check if API keys are loaded (for troubleshooting)
if not os.environ.get("OPENROUTER_API_KEY") and not os.environ.get("ANTHROPIC_API_KEY"):
    print("Warning: No API keys found in environment. Check .env files.", file=sys.stderr)


def construct_research_society(question: str) -> RolePlaying:
    """Create a research-focused agent society."""
    
    # Check for API keys - prefer OpenRouter (free models), then Anthropic Claude
    openrouter_key = os.environ.get("OPENROUTER_API_KEY")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    
    # Determine which model to use
    # Use OpenRouter free models first (free, but may have JSON parsing issues)
    # Fall back to Anthropic Claude if OpenRouter fails (requires credits)
    if openrouter_key:
        # Use Anthropic Claude (more reliable, better JSON parsing)
        models = {{
            "user": ModelFactory.create(
                model_platform=ModelPlatformType.ANTHROPIC,
                model_type=ModelType.CLAUDE_3_5_SONNET,
                model_config_dict={{"temperature": 0}},
            ),
            "assistant": ModelFactory.create(
                model_platform=ModelPlatformType.ANTHROPIC,
                model_type=ModelType.CLAUDE_3_5_SONNET,
                model_config_dict={{"temperature": 0}},
            ),
            "browsing": ModelFactory.create(
                model_platform=ModelPlatformType.ANTHROPIC,
                model_type=ModelType.CLAUDE_3_5_SONNET,
                model_config_dict={{"temperature": 0}},
            ),
            "planning": ModelFactory.create(
                model_platform=ModelPlatformType.ANTHROPIC,
                model_type=ModelType.CLAUDE_3_5_SONNET,
                model_config_dict={{"temperature": 0}},
            ),
        }}
    elif openrouter_key:
        # Use OpenRouter with free model (best free option)
        # Default to openrouter/free which auto-selects best available free model
        model_type_str = os.environ.get("OPENROUTER_MODEL", "openrouter/free")
        
        model_config = {{
            "temperature": 0,
            "max_tokens": 4000,
        }}
        
        models = {{
            "user": ModelFactory.create(
                model_platform=ModelPlatformType.OPENAI_COMPATIBLE_MODEL,
                api_key=openrouter_key,
                model_type=model_type_str,
                url="https://openrouter.ai/api/v1",
                model_config_dict=model_config,
            ),
            "assistant": ModelFactory.create(
                model_platform=ModelPlatformType.OPENAI_COMPATIBLE_MODEL,
                api_key=openrouter_key,
                model_type=model_type_str,
                url="https://openrouter.ai/api/v1",
                model_config_dict=model_config,
            ),
            "browsing": ModelFactory.create(
                model_platform=ModelPlatformType.OPENAI_COMPATIBLE_MODEL,
                api_key=openrouter_key,
                model_type=model_type_str,
                url="https://openrouter.ai/api/v1",
                model_config_dict=model_config,
            ),
            "planning": ModelFactory.create(
                model_platform=ModelPlatformType.OPENAI_COMPATIBLE_MODEL,
                api_key=openrouter_key,
                model_type=model_type_str,
                url="https://openrouter.ai/api/v1",
                model_config_dict=model_config,
            ),
        }}
        # Fallback to Anthropic Claude (requires paid API key)
        models = {{
            "user": ModelFactory.create(
                model_platform=ModelPlatformType.ANTHROPIC,
                model_type=ModelType.CLAUDE_3_5_SONNET,
                model_config_dict={{"temperature": 0}},
            ),
            "assistant": ModelFactory.create(
                model_platform=ModelPlatformType.ANTHROPIC,
                model_type=ModelType.CLAUDE_3_5_SONNET,
                model_config_dict={{"temperature": 0}},
            ),
            "browsing": ModelFactory.create(
                model_platform=ModelPlatformType.ANTHROPIC,
                model_type=ModelType.CLAUDE_3_5_SONNET,
                model_config_dict={{"temperature": 0}},
            ),
            "planning": ModelFactory.create(
                model_platform=ModelPlatformType.ANTHROPIC,
                model_type=ModelType.CLAUDE_3_5_SONNET,
                model_config_dict={{"temperature": 0}},
            ),
        }}
    else:
        raise ValueError(
            "No API key found! Please set either OPENROUTER_API_KEY (recommended for free) "
            "or ANTHROPIC_API_KEY in your .env file. "
            "Get a free OpenRouter key at: https://openrouter.ai/keys"
        )

    # Research-focused tools
    tools = [
        *BrowserToolkit(
            headless=True,  # Run headless for batch processing
            web_agent_model=models["browsing"],
            planning_agent_model=models["planning"],
        ).get_tools(),
        SearchToolkit().search_duckduckgo,
        SearchToolkit().search_wiki,
        SearchToolkit().search_google,
    ]

    user_agent_kwargs = {{"model": models["user"]}}
    assistant_agent_kwargs = {{"model": models["assistant"], "tools": tools}}

    task_kwargs = {{
        "task_prompt": question,
        "with_task_specify": False,
    }}

    society = RolePlaying(
        **task_kwargs,
        user_role_name="researcher",
        user_agent_kwargs=user_agent_kwargs,
        assistant_role_name="research_assistant",
        assistant_agent_kwargs=assistant_agent_kwargs,
    )

    return society


def main():
    task = """{task_prompt.replace('"', '\\"').replace("'", "\\'")}"""

    society = construct_research_society(task)
    answer, chat_history, token_count = run_society(society)

    # Try to extract JSON from the answer
    try:
        # Find JSON in the response
        if "{{" in answer and "}}" in answer:
            start = answer.find("{{")
            end = answer.rfind("}}") + 1
            json_str = answer[start:end]
            result = json.loads(json_str)
            print(json.dumps(result, indent=2))
        else:
            print(json.dumps({{"raw_answer": answer, "error": "No JSON found"}}))
    except json.JSONDecodeError as e:
        print(json.dumps({{"raw_answer": answer, "error": str(e)}}))


if __name__ == "__main__":
    main()
'''
    return script


def run_owl_research(
    name: str,
    company: str = "",
    website: str = "",
    linkedin: str = "",
    timeout: int = 300,
) -> Tuple[Dict, bool]:
    """
    Run OWL research for a profile.

    Args:
        name: Person's name
        company: Company name
        website: Website URL
        linkedin: LinkedIn URL
        timeout: Max seconds to run

    Returns:
        Tuple of (result_dict, success_bool)
    """
    import tempfile

    # Generate the research script
    script = create_owl_research_script(name, company, website, linkedin)

    # Write to temp file in OWL directory
    owl_dir = Path(__file__).parent.parent.parent.parent.parent / "owl_framework"

    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.py',
        dir=str(owl_dir),
        delete=False
    ) as f:
        f.write(script)
        script_path = f.name

    try:
        # Prepare environment variables to pass to subprocess
        import os
        from dotenv import load_dotenv
        
        env = os.environ.copy()
        
        # Load API keys from .env files directly (don't rely on Django settings)
        # Try OWL .env first
        owl_env_file = owl_dir / "owl" / ".env"
        if owl_env_file.exists():
            load_dotenv(dotenv_path=str(owl_env_file), override=False)
        
        # Try parent .env
        parent_env_file = owl_dir.parent / ".env"
        if parent_env_file.exists():
            load_dotenv(dotenv_path=str(parent_env_file), override=False)
        
        # Ensure API keys are in the environment for subprocess
        openrouter_key = os.environ.get("OPENROUTER_API_KEY")
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
        
        if openrouter_key:
            env['OPENROUTER_API_KEY'] = openrouter_key
        if anthropic_key:
            env['ANTHROPIC_API_KEY'] = anthropic_key
        
        # Run in OWL's venv
        result = subprocess.run(
            [str(OWL_PYTHON), script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(owl_dir),
            env=env,  # Pass environment variables
        )

        if result.returncode == 0:
            try:
                data = json.loads(result.stdout)
                return data, True
            except json.JSONDecodeError:
                return {"raw_output": result.stdout, "error": "Failed to parse JSON"}, False
        else:
            return {"error": result.stderr, "stdout": result.stdout}, False

    except subprocess.TimeoutExpired:
        return {"error": f"Research timed out after {timeout}s"}, False
    except Exception as e:
        return {"error": str(e)}, False
    finally:
        # Clean up temp script
        Path(script_path).unlink(missing_ok=True)


class OWLEnrichmentAgent:
    """
    Profile enrichment agent powered by OWL.

    Uses multi-agent collaboration for deep research:
    - Browser automation to visit websites
    - Multi-engine search (Google, DuckDuckGo, Wikipedia)
    - Document parsing
    - Source verification
    """

    def __init__(self, timeout: int = 300):
        self.timeout = timeout
        self.profiles_processed = 0

    def enrich_profile(
        self,
        name: str,
        email: str = "",
        company: str = "",
        website: str = "",
        linkedin: str = "",
    ) -> Dict:
        """
        Research and enrich a single profile using OWL.

        Returns dict with verified profile data and sources.
        """
        result, success = run_owl_research(
            name=name,
            company=company,
            website=website,
            linkedin=linkedin,
            timeout=self.timeout,
        )

        if success:
            self.profiles_processed += 1

        return {
            "name": name,
            "enriched_data": result if success else {},
            "success": success,
            "error": result.get("error") if not success else None,
        }


# Simple test
if __name__ == "__main__":
    agent = OWLEnrichmentAgent(timeout=120)
    result = agent.enrich_profile(
        name="Janet Bray Attwood",
        company="The Passion Test",
        website="https://thepassiontest.com",
    )
    print(json.dumps(result, indent=2))
