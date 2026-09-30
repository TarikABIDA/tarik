#!/usr/bin/env python3
"""
Prospect Lead Enrichment Automation
===================================

Reads prospect companies from a CSV, deduplicates them, enriches each company
via the Apollo.io Organization Enrichment API, filters by traffic and revenue
thresholds, and writes qualified leads to a Google Sheet named "Qualified Leads".

Usage
-----
    python enrich_leads.py --input sample_prospects.csv
    python enrich_leads.py --input prospects.csv --sheet-name "Qualified Leads"

Environment (see .env.example)
------------------------------
    APOLLO_API_KEY              Required. Apollo.io API key.
    GOOGLE_SHEETS_ID            Required. Target spreadsheet ID.
    GOOGLE_CREDENTIALS_PATH     Path to Google service-account JSON.
    MIN_MONTHLY_TRAFFIC         Default: 10000
    MIN_ESTIMATED_REVENUE       Default: 100000
    APOLLO_REQUEST_DELAY        Seconds between Apollo requests (default: 0.5)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APOLLO_ENRICH_URL = "https://api.apollo.io/api/v1/organizations/enrich"
DEFAULT_SHEET_NAME = "Qualified Leads"
REQUIRED_CSV_COLUMNS = ("company_name", "website", "linkedin_url")
OUTPUT_COLUMNS = [
    "company_name",
    "website",
    "linkedin_url",
    "estimated_revenue",
    "monthly_traffic",
    "company_size",
]

# Apollo response keys checked (in order) for each enrichment field.
REVENUE_KEYS = (
    "annual_revenue",
    "organization_revenue",
    "estimated_annual_revenue",
    "revenue",
)
TRAFFIC_KEYS = (
    "monthly_traffic",
    "estimated_monthly_visits",
    "estimated_monthly_traffic",
    "website_monthly_traffic",
    "monthly_visitors",
)
SIZE_KEYS = (
    "estimated_num_employees",
    "organization_headcount",
    "employee_count",
    "num_employees",
)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure and return the module logger."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    return logging.getLogger("enrich_leads")


log = logging.getLogger("enrich_leads")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class EnrichmentResult:
    """Enriched fields returned for a single prospect company."""

    estimated_revenue: Optional[float] = None
    monthly_traffic: Optional[float] = None
    company_size: Optional[float] = None
    raw_organization: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass
class PipelineStats:
    """Counters summarizing a pipeline run."""

    input_rows: int = 0
    duplicates_removed: int = 0
    enrichment_failures: int = 0
    filtered_out: int = 0
    qualified: int = 0
    sheets_rows_written: int = 0

    def summary(self) -> str:
        """Return a human-readable summary of pipeline results."""
        return (
            f"Pipeline complete — "
            f"input={self.input_rows}, "
            f"duplicates_removed={self.duplicates_removed}, "
            f"enrichment_failures={self.enrichment_failures}, "
            f"filtered_out={self.filtered_out}, "
            f"qualified={self.qualified}, "
            f"added_to_sheet={self.sheets_rows_written}"
        )


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def build_session(
    total_retries: int = 3,
    backoff_factor: float = 1.0,
) -> requests.Session:
    """
    Build a requests Session with retry logic for transient failures.

    Retries on connection errors, timeouts, and HTTP 429 / 5xx responses.
    """
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def extract_domain(website: str) -> Optional[str]:
    """
    Extract a bare domain from a website URL or hostname string.

    Examples
    --------
    >>> extract_domain("https://www.acme.com/about")
    'acme.com'
    >>> extract_domain("acme.com")
    'acme.com'
    """
    if not website or not isinstance(website, str):
        return None
    value = website.strip()
    if not value:
        return None
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _first_numeric(data: dict[str, Any], keys: tuple[str, ...]) -> Optional[float]:
    """Return the first numeric value found for any of the given keys."""
    for key in keys:
        if key not in data or data[key] is None:
            continue
        try:
            return float(data[key])
        except (TypeError, ValueError):
            continue
    return None


# ---------------------------------------------------------------------------
# CSV loading & cleaning
# ---------------------------------------------------------------------------

def load_prospects(csv_path: str | Path) -> pd.DataFrame:
    """
    Load prospect rows from a CSV file.

    Parameters
    ----------
    csv_path:
        Path to a CSV with columns company_name, website, linkedin_url.

    Returns
    -------
    pd.DataFrame
        Normalized prospect DataFrame.

    Raises
    ------
    FileNotFoundError
        If the CSV path does not exist.
    ValueError
        If required columns are missing.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(path, dtype=str).fillna("")
    missing = [col for col in REQUIRED_CSV_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"CSV is missing required column(s): {missing}. "
            f"Expected columns: {list(REQUIRED_CSV_COLUMNS)}"
        )

    df = df[list(REQUIRED_CSV_COLUMNS)].copy()
    df["company_name"] = df["company_name"].str.strip()
    df["website"] = df["website"].str.strip()
    df["linkedin_url"] = df["linkedin_url"].str.strip()

    # Drop completely empty company names — they cannot be deduped usefully.
    empty_mask = df["company_name"] == ""
    if empty_mask.any():
        log.warning("Dropping %d row(s) with empty company_name", int(empty_mask.sum()))
        df = df.loc[~empty_mask].reset_index(drop=True)

    return df


def remove_duplicate_companies(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove duplicate companies by case-insensitive company_name.

    Keeps the first occurrence of each company.

    Returns
    -------
    tuple[pd.DataFrame, int]
        Deduplicated DataFrame and count of removed duplicates.
    """
    before = len(df)
    deduped = (
        df.assign(_name_key=df["company_name"].str.casefold())
        .drop_duplicates(subset=["_name_key"], keep="first")
        .drop(columns=["_name_key"])
        .reset_index(drop=True)
    )
    removed = before - len(deduped)
    return deduped, removed


# ---------------------------------------------------------------------------
# Apollo.io enrichment
# ---------------------------------------------------------------------------

class ApolloClient:
    """
    Thin client for Apollo.io Organization Enrichment.

    Handles authentication, retries, rate-limit delays, and field extraction.
    """

    def __init__(
        self,
        api_key: str,
        session: Optional[requests.Session] = None,
        request_delay: float = 0.5,
        timeout: float = 30.0,
    ) -> None:
        if not api_key:
            raise ValueError("Apollo API key is required")
        self.api_key = api_key
        self.session = session or build_session()
        self.request_delay = max(0.0, request_delay)
        self.timeout = timeout
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        """Sleep if needed to respect the configured request delay."""
        if self.request_delay <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.request_delay - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def enrich_organization(
        self,
        *,
        company_name: str,
        website: str = "",
        linkedin_url: str = "",
    ) -> EnrichmentResult:
        """
        Enrich a single organization via Apollo.io.

        Parameters
        ----------
        company_name:
            Company display name (used as a match signal).
        website:
            Company website URL or domain.
        linkedin_url:
            Company LinkedIn profile URL.

        Returns
        -------
        EnrichmentResult
            Parsed enrichment fields. Numeric fields may be None when Apollo
            does not return them for the matched organization.

        Raises
        ------
        requests.RequestException
            On network failures after retries are exhausted.
        RuntimeError
            On non-retryable Apollo API errors (auth, bad request, etc.).
        """
        params: dict[str, str] = {}
        domain = extract_domain(website)
        if domain:
            params["domain"] = domain
        if website:
            params["website"] = website if "://" in website else f"https://{website}"
        if linkedin_url:
            params["linkedin_url"] = linkedin_url
        if company_name:
            params["name"] = company_name

        if not params:
            raise ValueError("At least one of domain, website, linkedin_url, or name is required")

        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "x-api-key": self.api_key,
        }

        self._throttle()
        try:
            response = self.session.get(
                APOLLO_ENRICH_URL,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
        finally:
            self._last_request_at = time.monotonic()

        if response.status_code == 401:
            raise RuntimeError("Apollo API authentication failed — check APOLLO_API_KEY")
        if response.status_code == 403:
            raise RuntimeError("Apollo API access forbidden — check API key scopes / plan")
        if response.status_code == 404:
            log.info("No Apollo match for company=%s domain=%s", company_name, domain)
            return EnrichmentResult()
        if response.status_code == 429:
            raise RuntimeError("Apollo API rate limit exceeded after retries")
        if response.status_code >= 400:
            raise RuntimeError(
                f"Apollo API error {response.status_code}: {response.text[:300]}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("Apollo API returned invalid JSON") from exc

        organization = payload.get("organization") or {}
        if not organization:
            log.info("Empty organization payload for company=%s", company_name)
            return EnrichmentResult()

        return EnrichmentResult(
            estimated_revenue=_first_numeric(organization, REVENUE_KEYS),
            monthly_traffic=_first_numeric(organization, TRAFFIC_KEYS),
            company_size=_first_numeric(organization, SIZE_KEYS),
            raw_organization=organization,
        )


def enrich_prospects(
    df: pd.DataFrame,
    client: ApolloClient,
) -> tuple[pd.DataFrame, int]:
    """
    Enrich each prospect row with Apollo.io organization data.

    Parameters
    ----------
    df:
        Deduplicated prospects DataFrame.
    client:
        Configured ApolloClient instance.

    Returns
    -------
    tuple[pd.DataFrame, int]
        Enriched DataFrame and count of enrichment failures.
    """
    enriched_rows: list[dict[str, Any]] = []
    failures = 0

    total = len(df)
    for position, (_, row) in enumerate(df.iterrows(), start=1):
        company = row["company_name"]
        log.info("Enriching [%d/%d] %s", position, total, company)

        record: dict[str, Any] = {
            "company_name": company,
            "website": row["website"],
            "linkedin_url": row["linkedin_url"],
            "estimated_revenue": None,
            "monthly_traffic": None,
            "company_size": None,
        }

        try:
            result = client.enrich_organization(
                company_name=company,
                website=row["website"],
                linkedin_url=row["linkedin_url"],
            )
            record["estimated_revenue"] = result.estimated_revenue
            record["monthly_traffic"] = result.monthly_traffic
            record["company_size"] = result.company_size
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            failures += 1
            log.error("Enrichment failed for '%s': %s", company, exc)

        enriched_rows.append(record)

    return pd.DataFrame(enriched_rows, columns=OUTPUT_COLUMNS), failures


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def filter_qualified_leads(
    df: pd.DataFrame,
    min_monthly_traffic: float = 10_000,
    min_estimated_revenue: float = 100_000,
) -> tuple[pd.DataFrame, int]:
    """
    Keep leads that meet traffic AND revenue thresholds.

    Rows with missing numeric values for either metric are treated as
    not meeting the threshold and are filtered out.

    Parameters
    ----------
    df:
        Enriched prospects DataFrame.
    min_monthly_traffic:
        Minimum monthly website traffic required.
    min_estimated_revenue:
        Minimum estimated annual revenue required.

    Returns
    -------
    tuple[pd.DataFrame, int]
        Qualified leads DataFrame and count of filtered-out rows.
    """
    traffic = pd.to_numeric(df["monthly_traffic"], errors="coerce")
    revenue = pd.to_numeric(df["estimated_revenue"], errors="coerce")

    mask = (traffic > min_monthly_traffic) & (revenue > min_estimated_revenue)
    qualified = df.loc[mask].copy().reset_index(drop=True)
    filtered_out = int((~mask).sum())

    log.info(
        "Filter thresholds: monthly_traffic > %s AND estimated_revenue > %s",
        f"{min_monthly_traffic:,.0f}",
        f"{min_estimated_revenue:,.0f}",
    )
    return qualified, filtered_out


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------

def write_to_google_sheets(
    df: pd.DataFrame,
    spreadsheet_id: str,
    credentials_path: str | Path,
    sheet_name: str = DEFAULT_SHEET_NAME,
) -> int:
    """
    Write the qualified leads DataFrame to a Google Sheet worksheet.

    Creates the worksheet if it does not exist. Existing content in the
    target worksheet is cleared and replaced.

    Parameters
    ----------
    df:
        Qualified leads to write.
    spreadsheet_id:
        Google Sheets spreadsheet ID.
    credentials_path:
        Path to a Google Cloud service-account JSON key file. The service
        account must have edit access to the spreadsheet.
    sheet_name:
        Target worksheet title (default: "Qualified Leads").

    Returns
    -------
    int
        Number of data rows written (excluding the header).

    Raises
    ------
    FileNotFoundError
        If credentials_path does not exist.
    ImportError
        If gspread / google-auth are not installed.
    RuntimeError
        On Google API / authorization failures.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as exc:
        raise ImportError(
            "gspread and google-auth are required. Install with: "
            "pip install gspread google-auth"
        ) from exc

    creds_path = Path(credentials_path)
    if not creds_path.exists():
        raise FileNotFoundError(
            f"Google credentials file not found: {creds_path}. "
            "Share the spreadsheet with the service-account email and "
            "set GOOGLE_CREDENTIALS_PATH."
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    try:
        credentials = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(spreadsheet_id)
    except Exception as exc:  # noqa: BLE001 — surface as a clear pipeline error
        raise RuntimeError(f"Failed to authorize / open Google Sheet: {exc}") from exc

    try:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            log.info("Creating worksheet '%s'", sheet_name)
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=max(len(df) + 10, 100),
                cols=max(len(OUTPUT_COLUMNS) + 2, 10),
            )

        # Replace sheet contents with header + data rows.
        worksheet.clear()
        header = list(OUTPUT_COLUMNS)
        values: list[list[Any]] = [header]

        if not df.empty:
            # Ensure column order and JSON/Sheets-safe scalar values.
            ordered = df.reindex(columns=OUTPUT_COLUMNS)
            for row in ordered.itertuples(index=False, name=None):
                values.append(["" if pd.isna(cell) else cell for cell in row])

        worksheet.update(values, value_input_option="USER_ENTERED")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed writing to Google Sheet '{sheet_name}': {exc}") from exc

    rows_written = max(len(values) - 1, 0)
    log.info("Wrote %d qualified lead(s) to sheet '%s'", rows_written, sheet_name)
    return rows_written


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config(args: argparse.Namespace) -> dict[str, Any]:
    """
    Merge CLI arguments with environment variables into a config dict.

    Raises
    ------
    ValueError
        If required configuration values are missing.
    """
    load_dotenv()

    apollo_api_key = os.getenv("APOLLO_API_KEY", "").strip()
    spreadsheet_id = (args.spreadsheet_id or os.getenv("GOOGLE_SHEETS_ID", "")).strip()
    credentials_path = (
        args.credentials
        or os.getenv("GOOGLE_CREDENTIALS_PATH", "./credentials/google_service_account.json")
    ).strip()

    min_traffic = float(
        args.min_traffic
        if args.min_traffic is not None
        else os.getenv("MIN_MONTHLY_TRAFFIC", "10000")
    )
    min_revenue = float(
        args.min_revenue
        if args.min_revenue is not None
        else os.getenv("MIN_ESTIMATED_REVENUE", "100000")
    )
    request_delay = float(os.getenv("APOLLO_REQUEST_DELAY", "0.5"))

    missing = []
    if not apollo_api_key:
        missing.append("APOLLO_API_KEY")
    if not spreadsheet_id and not args.dry_run:
        missing.append("GOOGLE_SHEETS_ID")
    if missing:
        raise ValueError(
            "Missing required configuration: "
            + ", ".join(missing)
            + ". Set them in the environment or a .env file (see .env.example)."
        )

    return {
        "input_csv": args.input,
        "sheet_name": args.sheet_name,
        "apollo_api_key": apollo_api_key,
        "spreadsheet_id": spreadsheet_id,
        "credentials_path": credentials_path,
        "min_monthly_traffic": min_traffic,
        "min_estimated_revenue": min_revenue,
        "request_delay": request_delay,
        "dry_run": args.dry_run,
        "output_csv": args.output_csv,
    }


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------

def run_pipeline(config: dict[str, Any]) -> PipelineStats:
    """
    Execute the full lead enrichment pipeline.

    Steps
    -----
    1. Load CSV prospects
    2. Remove duplicate companies
    3. Enrich via Apollo.io
    4. Filter by traffic and revenue
    5. Write qualified leads to Google Sheets (unless dry-run)
    6. Log summary statistics
    """
    stats = PipelineStats()

    log.info("Loading prospects from %s", config["input_csv"])
    prospects = load_prospects(config["input_csv"])
    stats.input_rows = len(prospects)
    log.info("Loaded %d prospect row(s)", stats.input_rows)

    prospects, stats.duplicates_removed = remove_duplicate_companies(prospects)
    log.info(
        "Removed %d duplicate(s); %d unique compan(ies) remain",
        stats.duplicates_removed,
        len(prospects),
    )

    client = ApolloClient(
        api_key=config["apollo_api_key"],
        request_delay=config["request_delay"],
    )
    enriched, stats.enrichment_failures = enrich_prospects(prospects, client)
    log.info(
        "Enrichment finished with %d failure(s)",
        stats.enrichment_failures,
    )

    qualified, stats.filtered_out = filter_qualified_leads(
        enriched,
        min_monthly_traffic=config["min_monthly_traffic"],
        min_estimated_revenue=config["min_estimated_revenue"],
    )
    stats.qualified = len(qualified)
    log.info(
        "Qualified %d lead(s); filtered out %d",
        stats.qualified,
        stats.filtered_out,
    )

    if config.get("output_csv"):
        out_path = Path(config["output_csv"])
        qualified.to_csv(out_path, index=False)
        log.info("Also wrote qualified leads CSV to %s", out_path)

    if config["dry_run"]:
        log.info("Dry-run enabled — skipping Google Sheets write")
        stats.sheets_rows_written = 0
    else:
        stats.sheets_rows_written = write_to_google_sheets(
            qualified,
            spreadsheet_id=config["spreadsheet_id"],
            credentials_path=config["credentials_path"],
            sheet_name=config["sheet_name"],
        )

    log.info(stats.summary())
    return stats


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Enrich prospect leads with Apollo.io and write qualified leads to Google Sheets.",
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to input CSV (columns: company_name, website, linkedin_url)",
    )
    parser.add_argument(
        "--sheet-name",
        default=DEFAULT_SHEET_NAME,
        help=f'Google Sheets worksheet name (default: "{DEFAULT_SHEET_NAME}")',
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=None,
        help="Google Sheets spreadsheet ID (overrides GOOGLE_SHEETS_ID)",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help="Path to Google service-account JSON (overrides GOOGLE_CREDENTIALS_PATH)",
    )
    parser.add_argument(
        "--min-traffic",
        type=float,
        default=None,
        help="Minimum monthly_traffic to keep (default: 10000)",
    )
    parser.add_argument(
        "--min-revenue",
        type=float,
        default=None,
        help="Minimum estimated_revenue to keep (default: 100000)",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Optional path to also save qualified leads as a local CSV",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run enrichment + filtering without writing to Google Sheets",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point. Returns a process exit code."""
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)

    try:
        config = load_config(args)
        run_pipeline(config)
    except FileNotFoundError as exc:
        log.error("%s", exc)
        return 1
    except ValueError as exc:
        log.error("Configuration / validation error: %s", exc)
        return 1
    except requests.RequestException as exc:
        log.error("Network error talking to Apollo.io: %s", exc)
        return 1
    except RuntimeError as exc:
        log.error("Pipeline failed: %s", exc)
        return 1
    except KeyboardInterrupt:
        log.warning("Interrupted by user")
        return 130
    except Exception as exc:  # noqa: BLE001
        log.exception("Unexpected error: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
