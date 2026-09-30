# Prospect Lead Enrichment

Production-ready Python automation that cleans prospect CSVs, enriches companies via **Apollo.io**, filters by traffic and revenue, and writes qualified leads to **Google Sheets**.

## Pipeline

1. Read CSV (`company_name`, `website`, `linkedin_url`)
2. Remove duplicate companies (by `company_name`, case-insensitive)
3. Enrich each company with Apollo.io Organization Enrichment:
   - `estimated_revenue`
   - `monthly_traffic`
   - `company_size`
4. Keep leads where `monthly_traffic > 10000` **and** `estimated_revenue > 100000`
5. Write results to the Google Sheet worksheet **Qualified Leads**
6. Log how many were added, duplicates removed, and filtered out

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill in `.env`:

| Variable | Description |
|---|---|
| `APOLLO_API_KEY` | Apollo.io API key |
| `GOOGLE_SHEETS_ID` | Spreadsheet ID from the sheet URL |
| `GOOGLE_CREDENTIALS_PATH` | Path to Google service-account JSON |

### Google Sheets access

1. Create a Google Cloud service account and download its JSON key
2. Place the key at `credentials/google_service_account.json` (or update the path)
3. Share the target spreadsheet with the service-account email (**Editor**)

## Usage

```bash
# Full run → Apollo enrichment + Google Sheets write
python enrich_leads.py --input sample_prospects.csv

# Dry run (skip Sheets write); optionally save a local CSV of qualified leads
python enrich_leads.py --input sample_prospects.csv --dry-run --output-csv qualified.csv

# Custom thresholds / worksheet name
python enrich_leads.py \
  --input prospects.csv \
  --sheet-name "Qualified Leads" \
  --min-traffic 10000 \
  --min-revenue 100000
```

## Apollo field mapping

Apollo organization payloads vary by plan/match quality. The script reads the first available value from:

| Output column | Apollo keys checked |
|---|---|
| `estimated_revenue` | `annual_revenue`, `organization_revenue`, `estimated_annual_revenue`, `revenue` |
| `monthly_traffic` | `monthly_traffic`, `estimated_monthly_visits`, `estimated_monthly_traffic`, `website_monthly_traffic`, `monthly_visitors` |
| `company_size` | `estimated_num_employees`, `organization_headcount`, `employee_count`, `num_employees` |

Rows missing traffic or revenue after enrichment fail the filter and are counted under **filtered out**.

## Error handling

- HTTP retries with backoff for network issues and Apollo `429` / `5xx`
- Clear failures for auth / forbidden / invalid responses
- Per-company enrichment errors are logged; the pipeline continues
- Missing CSV columns, credentials, or config values fail fast with actionable messages

## Project layout

```
enrich_leads.py          # main automation script
requirements.txt         # Python dependencies
.env.example             # environment template
sample_prospects.csv     # example input
credentials/             # place Google service-account JSON here
```
