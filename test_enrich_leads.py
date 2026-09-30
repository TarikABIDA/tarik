#!/usr/bin/env python3
"""Unit tests for enrich_leads helpers (no live Apollo / Google calls)."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

import enrich_leads as el


SAMPLE_CSV = Path(__file__).resolve().parent / "sample_prospects.csv"


class ExtractDomainTests(unittest.TestCase):
    def test_full_url(self) -> None:
        self.assertEqual(el.extract_domain("https://www.acme.com/about"), "acme.com")

    def test_bare_host(self) -> None:
        self.assertEqual(el.extract_domain("acme.com"), "acme.com")

    def test_empty(self) -> None:
        self.assertIsNone(el.extract_domain(""))
        self.assertIsNone(el.extract_domain(None))  # type: ignore[arg-type]


class CsvPipelineTests(unittest.TestCase):
    def test_load_and_dedupe_sample(self) -> None:
        df = el.load_prospects(SAMPLE_CSV)
        self.assertEqual(len(df), 6)
        deduped, removed = el.remove_duplicate_companies(df)
        self.assertEqual(removed, 1)
        self.assertEqual(len(deduped), 5)
        names = deduped["company_name"].str.casefold().tolist()
        self.assertEqual(len(names), len(set(names)))

    def test_missing_columns_raise(self) -> None:
        bad = Path("/tmp/bad_prospects.csv")
        bad.write_text("name,url\nAcme,https://acme.com\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            el.load_prospects(bad)

    def test_filter_qualified_leads(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "company_name": "Keep",
                    "website": "https://keep.com",
                    "linkedin_url": "",
                    "estimated_revenue": 250_000,
                    "monthly_traffic": 50_000,
                    "company_size": 120,
                },
                {
                    "company_name": "LowTraffic",
                    "website": "https://low.com",
                    "linkedin_url": "",
                    "estimated_revenue": 500_000,
                    "monthly_traffic": 500,
                    "company_size": 80,
                },
                {
                    "company_name": "LowRevenue",
                    "website": "https://lr.com",
                    "linkedin_url": "",
                    "estimated_revenue": 10_000,
                    "monthly_traffic": 80_000,
                    "company_size": 40,
                },
                {
                    "company_name": "Missing",
                    "website": "https://missing.com",
                    "linkedin_url": "",
                    "estimated_revenue": None,
                    "monthly_traffic": 80_000,
                    "company_size": 40,
                },
            ]
        )
        qualified, filtered_out = el.filter_qualified_leads(df)
        self.assertEqual(len(qualified), 1)
        self.assertEqual(qualified.iloc[0]["company_name"], "Keep")
        self.assertEqual(filtered_out, 3)


class ApolloClientTests(unittest.TestCase):
    def test_enrich_parses_organization_fields(self) -> None:
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "organization": {
                "annual_revenue": 1_500_000,
                "estimated_monthly_visits": 42_000,
                "estimated_num_employees": 250,
            }
        }
        session.get.return_value = response

        client = el.ApolloClient(api_key="test-key", session=session, request_delay=0)
        result = client.enrich_organization(
            company_name="Acme",
            website="https://www.acme.com",
            linkedin_url="https://www.linkedin.com/company/acme",
        )

        self.assertEqual(result.estimated_revenue, 1_500_000)
        self.assertEqual(result.monthly_traffic, 42_000)
        self.assertEqual(result.company_size, 250)
        session.get.assert_called_once()
        _, kwargs = session.get.call_args
        self.assertEqual(kwargs["params"]["domain"], "acme.com")
        self.assertEqual(kwargs["headers"]["x-api-key"], "test-key")

    def test_enrich_handles_404(self) -> None:
        session = MagicMock()
        response = MagicMock()
        response.status_code = 404
        session.get.return_value = response

        client = el.ApolloClient(api_key="test-key", session=session, request_delay=0)
        result = client.enrich_organization(company_name="Unknown Co", website="unknown.example")
        self.assertIsNone(result.estimated_revenue)
        self.assertIsNone(result.monthly_traffic)

    def test_enrich_auth_error(self) -> None:
        session = MagicMock()
        response = MagicMock()
        response.status_code = 401
        session.get.return_value = response

        client = el.ApolloClient(api_key="bad-key", session=session, request_delay=0)
        with self.assertRaises(RuntimeError):
            client.enrich_organization(company_name="Acme", website="acme.com")


class SheetsWriteTests(unittest.TestCase):
    def test_write_creates_worksheet_when_missing(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "company_name": "Keep",
                    "website": "https://keep.com",
                    "linkedin_url": "",
                    "estimated_revenue": 250_000,
                    "monthly_traffic": 50_000,
                    "company_size": 120,
                }
            ]
        )

        class WorksheetNotFound(Exception):
            pass

        fake_gspread = MagicMock()
        fake_gspread.WorksheetNotFound = WorksheetNotFound

        worksheet = MagicMock()
        spreadsheet = MagicMock()
        spreadsheet.worksheet.side_effect = WorksheetNotFound()
        spreadsheet.add_worksheet.return_value = worksheet

        client = MagicMock()
        client.open_by_key.return_value = spreadsheet
        fake_gspread.authorize.return_value = client

        fake_creds_mod = MagicMock()
        fake_creds_mod.Credentials.from_service_account_file.return_value = MagicMock()

        creds_file = Path("/tmp/fake_google_creds.json")
        creds_file.write_text("{}", encoding="utf-8")

        with patch.dict(
            "sys.modules",
            {
                "gspread": fake_gspread,
                "google": MagicMock(),
                "google.oauth2": MagicMock(),
                "google.oauth2.service_account": fake_creds_mod,
            },
        ):
            written = el.write_to_google_sheets(
                df,
                spreadsheet_id="sheet123",
                credentials_path=str(creds_file),
                sheet_name="Qualified Leads",
            )

        self.assertEqual(written, 1)
        spreadsheet.add_worksheet.assert_called_once()
        worksheet.clear.assert_called_once()
        worksheet.update.assert_called_once()
        payload = worksheet.update.call_args.args[0]
        self.assertEqual(payload[0], el.OUTPUT_COLUMNS)
        self.assertEqual(payload[1][0], "Keep")


class StatsTests(unittest.TestCase):
    def test_summary_text(self) -> None:
        stats = el.PipelineStats(
            input_rows=10,
            duplicates_removed=2,
            enrichment_failures=1,
            filtered_out=4,
            qualified=3,
            sheets_rows_written=3,
        )
        text = stats.summary()
        self.assertIn("duplicates_removed=2", text)
        self.assertIn("filtered_out=4", text)
        self.assertIn("added_to_sheet=3", text)


if __name__ == "__main__":
    unittest.main()
