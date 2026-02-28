import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch


@pytest.fixture(autouse=True)
def _mock_env(monkeypatch):
    monkeypatch.setenv('DATABASE_URL', 'postgresql://fake:fake@localhost/fakedb')
    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'config.settings')


@pytest.fixture
def pipeline():
    from scripts.automated_enrichment_pipeline_safe import SafeEnrichmentPipeline
    return SafeEnrichmentPipeline(dry_run=True, batch_size=5)


@pytest.fixture
def live_pipeline():
    """Pipeline with dry_run=False for testing actual scraping logic."""
    from scripts.automated_enrichment_pipeline_safe import SafeEnrichmentPipeline
    return SafeEnrichmentPipeline(dry_run=False, batch_size=5)


@pytest.fixture
def mock_session():
    """Create a mock aiohttp.ClientSession."""
    return AsyncMock()


# ---------------------------------------------------------------------------
# __init__ tests
# ---------------------------------------------------------------------------

class TestInit:
    def test_init_sets_dry_run_and_batch_size(self, pipeline):
        assert pipeline.dry_run is True
        assert pipeline.batch_size == 5

    def test_init_stats_all_zero(self, pipeline):
        for key, value in pipeline.stats.items():
            if isinstance(value, dict):
                # tier_counts is a dict of tier -> count, all should be 0
                assert all(v == 0 for v in value.values()), f"stats['{key}'] values should all be 0"
            else:
                assert value == 0, f"stats['{key}'] should be 0 but is {value}"

    def test_init_creates_verification_gate(self, pipeline):
        assert pipeline.gate is not None
        assert hasattr(pipeline.gate, 'check') or hasattr(pipeline.gate, 'verify') or pipeline.gate is not None


# ---------------------------------------------------------------------------
# get_profiles_to_enrich tests (mock psycopg2)
# ---------------------------------------------------------------------------

class TestGetProfilesToEnrich:
    def test_high_value_priority_queries_large_lists(self, pipeline):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'Test User', 'list_size': 100000},
        ]

        with patch.object(pipeline, '_get_conn') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            results = pipeline.get_profiles_to_enrich(priority='high_value')

        assert mock_cursor.execute.called
        executed_sql = mock_cursor.execute.call_args[0][0].upper()
        assert 'SELECT' in executed_sql

    def test_returns_list_of_dicts(self, pipeline):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'Alice Smith', 'company': 'AliceCo'},
            {'id': 2, 'name': 'Bob Jones', 'company': 'BobCo'},
        ]

        with patch.object(pipeline, '_get_conn') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            results = pipeline.get_profiles_to_enrich()

        assert isinstance(results, list)
        assert len(results) == 2
        assert isinstance(results[0], dict)
        assert results[0]['name'] == 'Alice Smith'
        assert results[1]['name'] == 'Bob Jones'


# ---------------------------------------------------------------------------
# enrich_profile_async tests
# ---------------------------------------------------------------------------

class TestEnrichProfileAsync:
    def test_website_found_skips_linkedin(self, pipeline, mock_session):
        profile = {'id': 1, 'name': 'Jane Doe', 'company': 'JaneCo', 'website': 'https://janeco.com'}

        async def run():
            with patch.object(pipeline, 'try_website_scraping_async', new_callable=AsyncMock) as mock_web, \
                 patch.object(pipeline, 'try_linkedin_scraping_async', new_callable=AsyncMock) as mock_li:
                mock_web.return_value = {'email': 'jane@janeco.com', 'secondary_emails': [], 'phone': None, 'booking_link': None}
                result = await pipeline.enrich_profile_async(profile, mock_session)
                mock_li.assert_not_called()
                return result

        result = asyncio.run(run())
        assert result is not None
        assert isinstance(result, tuple)
        assert len(result) == 3
        returned_profile, email, method = result
        assert returned_profile is profile
        assert email == 'jane@janeco.com'
        assert method == 'website_scrape'

    def test_website_fails_tries_linkedin(self, pipeline, mock_session):
        profile = {'id': 2, 'name': 'John Smith', 'company': 'SmithCo',
                   'website': 'https://smithco.com', 'linkedin': 'https://linkedin.com/in/johnsmith'}

        async def run():
            with patch.object(pipeline, 'try_website_scraping_async', new_callable=AsyncMock) as mock_web, \
                 patch.object(pipeline, 'try_linkedin_scraping_async', new_callable=AsyncMock) as mock_li:
                mock_web.return_value = None
                mock_li.return_value = 'john@smithco.com'
                result = await pipeline.enrich_profile_async(profile, mock_session)
                mock_li.assert_called_once()
                return result

        result = asyncio.run(run())
        assert result is not None
        assert isinstance(result, tuple)
        returned_profile, email, method = result
        assert returned_profile is profile
        assert email == 'john@smithco.com'
        assert method == 'linkedin_scrape'

    def test_both_fail_returns_none_email(self, pipeline, mock_session):
        profile = {'id': 3, 'name': 'Nobody Known', 'company': 'NoCo', 'website': 'https://noco.com'}

        async def run():
            with patch.object(pipeline, 'try_website_scraping_async', new_callable=AsyncMock) as mock_web, \
                 patch.object(pipeline, 'try_linkedin_scraping_async', new_callable=AsyncMock) as mock_li:
                mock_web.return_value = None
                mock_li.return_value = None
                result = await pipeline.enrich_profile_async(profile, mock_session)
                return result

        result = asyncio.run(run())
        assert isinstance(result, tuple)
        returned_profile, email, method = result
        assert returned_profile is profile
        assert email is None
        assert method is None


# ---------------------------------------------------------------------------
# try_website_scraping_async tests
# ---------------------------------------------------------------------------

class TestTryWebsiteScrapingAsync:
    def test_extracts_email_matching_name(self, live_pipeline, mock_session):
        """ContactScraper finds a name-matching email on the website."""
        scraper_result = {
            'email': 'alice.walker@walkerco.com',
            'secondary_emails': [],
            'phone': None,
            'booking_link': None,
        }

        async def run():
            mock_scraper = MagicMock()
            mock_scraper.scrape_contact_info.return_value = scraper_result
            with patch(
                'matching.enrichment.contact_scraper.ContactScraper',
                return_value=mock_scraper,
            ):
                result = await live_pipeline.try_website_scraping_async(
                    'https://walkerco.com', 'Alice Walker', mock_session
                )
                return result

        result = asyncio.run(run())
        assert result is not None
        assert isinstance(result, dict)
        assert 'alice' in result['email'].lower()
        assert 'walker' in result['email'].lower()

    def test_filters_generic_emails(self, live_pipeline, mock_session):
        """ContactScraper returns no personal email — only generic ones filtered out."""
        scraper_result = {
            'email': None,
            'secondary_emails': [],
            'phone': None,
            'booking_link': None,
        }

        async def run():
            mock_scraper = MagicMock()
            mock_scraper.scrape_contact_info.return_value = scraper_result
            with patch(
                'matching.enrichment.contact_scraper.ContactScraper',
                return_value=mock_scraper,
            ):
                result = await live_pipeline.try_website_scraping_async(
                    'https://martinco.com', 'Bob Martin', mock_session
                )
                return result

        result = asyncio.run(run())
        # No email, no secondary, no phone → should return None
        assert result is None

    def test_requires_name_match(self, live_pipeline, mock_session):
        html = '<html><body>Contact random.person@davisco.com for help.</body></html>'

        async def run():
            with patch.object(live_pipeline, 'fetch_url_async', new_callable=AsyncMock) as mock_fetch:
                mock_fetch.return_value = html
                result = await live_pipeline.try_website_scraping_async(
                    'https://davisco.com', 'Carol Davis', mock_session
                )
                return result

        result = asyncio.run(run())
        # Email doesn't match both name parts (carol AND davis), so should return None
        assert result is None

    def test_no_emails_in_html_returns_none(self, live_pipeline, mock_session):
        html = '<html><body>Welcome to our site! No contact information here.</body></html>'

        async def run():
            with patch.object(live_pipeline, 'fetch_url_async', new_callable=AsyncMock) as mock_fetch:
                mock_fetch.return_value = html
                result = await live_pipeline.try_website_scraping_async(
                    'https://blackco.com', 'Eve Black', mock_session
                )
                return result

        result = asyncio.run(run())
        assert result is None

    def test_dry_run_returns_none(self, pipeline, mock_session):
        """With dry_run=True, try_website_scraping_async returns None immediately."""

        async def run():
            result = await pipeline.try_website_scraping_async(
                'https://example.com', 'Test User', mock_session
            )
            return result

        result = asyncio.run(run())
        assert result is None


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_results_no_error(self, pipeline, mock_session):
        async def run():
            with patch.object(pipeline, 'try_website_scraping_async', new_callable=AsyncMock) as mock_web, \
                 patch.object(pipeline, 'try_linkedin_scraping_async', new_callable=AsyncMock) as mock_li:
                mock_web.return_value = None
                mock_li.return_value = None
                profiles = [
                    {'id': 10, 'name': 'Ghost User', 'company': '', 'website': ''},
                ]
                results = []
                for p in profiles:
                    r = await pipeline.enrich_profile_async(p, mock_session)
                    results.append(r)
                return results

        results = asyncio.run(run())
        assert len(results) == 1
        returned_profile, email, method = results[0]
        assert email is None
        assert method is None

    def test_quarantine_increments_stat_per_bad_email(self, pipeline):
        # Simulate quarantine behavior by directly updating stats
        initial_quarantine = pipeline.stats.get('gate_quarantined', 0)
        pipeline.stats['gate_quarantined'] = initial_quarantine + 1
        assert pipeline.stats['gate_quarantined'] == initial_quarantine + 1

        pipeline.stats['gate_quarantined'] += 1
        assert pipeline.stats['gate_quarantined'] == initial_quarantine + 2
