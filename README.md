# GBP Maps Scraper (Chrome Extension)

Chrome extension (Manifest V3) to scrape Google Maps business profile rows from the currently open search results page, apply filters, and export CSV.

## Features
- Scrape from active Google Maps search results page
- Built-in filters:
  - minimum rating
  - maximum rating
  - minimum reviews
  - maximum reviews
  - name keyword
  - category include/exclude
  - has website
  - has phone
- Optional `Infinite scroll` mode (ignores max rows and keeps scraping until end-of-results or Stop)
- Optional website enrichment mode (category-agnostic):
  - crawls internal website pages (configurable pages/site) instead of scanning only landing page
  - scans public pages for owner/founder/president/CEO-style names and emails
  - tracks discovered social links and can scan Facebook pages for fallback email when website email is missing
  - optional visible-tab mode to watch enrichment navigation live
  - skips blocked/unavailable websites and keeps core GBP data
- Persistent run/session snapshots in `chrome.storage.local` so progress/results survive popup close/reopen
- Email precedence output fields:
  - `primary_email` (personal email first; falls back to company/contact)
  - `primary_email_type` (`personal` or `company`)
  - `primary_email_source` (`website`, `facebook`, etc.)
- Fast pre-filtering from list cards for rating/review/name/category constraints to avoid opening every listing
- Live run analytics in popup: processing speed (entries/sec), seen listings, avg rating, avg reviews
- Live enrichment analytics: pages visited, pages discovered, social pages scanned
- Column picker to choose which fields are included in CSV export
- Deduplication by `place_id` (fallback to normalized Maps URL) with duplicate counter in run status
- CSV export with metadata columns:
  - `source_query`, `source_url`, `scraped_at`

## Files
- `manifest.json`
- `popup.html`, `popup.css`, `popup.js`
- `content.js`
- `background.js`
- `shared.js`

## Install (Unpacked)
1. Open Chrome and go to `chrome://extensions`.
2. Enable Developer mode.
3. Click **Load unpacked**.
4. Select this folder:
   - `/Users/ahmadkhattak/Downloads/14-days-100-dollars-challenge/scrapify`

## Usage
1. Open [Google Maps](https://www.google.com/maps) and run a search (example: `dentists in chicago`).
2. Open the extension popup.
3. Configure max rows and optional filters.
4. Optional: enable **Infinite scroll** to ignore max rows.
5. Choose export columns (All/None or individual checkboxes).
6. Click **Start Scrape**.
7. Optional: enable **Enrich websites (owner/email, best effort)** before export.
   - set **Website pages per site** (crawl depth)
   - optional: enable **Show tabs while enriching**
   - optional: enable **Hunt email on social links**
   - enrichment auto-starts after scrape completion when enabled
8. Click **Export CSV** after completion (or after stop).

## Notes / Limitations
- English UI-first selector strategy; non-English UI may partially work.
- Google Maps DOM changes can break selectors and require updates.
- Website enrichment is best-effort and limited to publicly visible data.
- Some websites block automation (CAPTCHA/anti-bot); blocked scans are marked and skipped.
- One active query per run.

## Compliance
You are responsible for complying with Google terms, local law, and privacy/data-use obligations.
