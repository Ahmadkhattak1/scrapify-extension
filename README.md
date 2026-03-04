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
  - skips common blog/news/article paths to avoid low-value crawl pages
  - scans public pages for owner/founder/president/CEO-style names and emails
  - tracks discovered social links and can scan Facebook pages for fallback email when website email is missing
  - optional external lead discovery mode:
    - trigger: when website is missing or website scan returns no email
    - order: Website -> Google Search
    - discovery uses Google Search only and evaluates the top 3 results per query
    - budget defaults: Google (2 queries/3 results)
  - optional visible-tab mode opens scan tabs without stealing focus from your active tab
  - skips blocked/unavailable websites and keeps core GBP data
- Persistent run/session snapshots in `chrome.storage.local` so progress/results survive popup close/reopen
- Email precedence output fields:
  - `email` (single unified export email based on popup precedence preference)
  - `primary_email` (personal email first; falls back to company/contact)
  - `primary_email_type` (`personal` or `company`)
  - `primary_email_source` (`website`, `facebook`, etc.)
- Fast pre-filtering from list cards for rating/review/name/category constraints to avoid opening every listing
- Live run analytics in popup: processing speed (entries/sec), seen listings, avg rating, avg reviews
- Live enrichment analytics: pages visited, pages discovered, social pages scanned
- Email KPI stats: total emails found and discovery-recovered emails
- Column picker to choose which fields are included in CSV export
- Popup remembers user settings (filters, max rows, toggles, and export columns) across reopen/reload
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
2. Click the extension icon to open the scraper control panel popup window (it stays open while you switch tabs).
3. Configure max rows and optional filters.
4. Optional: enable **Infinite scroll** to ignore max rows.
5. Choose export columns (All/None or individual checkboxes).
6. Click **Start Scrape**.
7. Optional: enable **Enrich websites (owner/email, best effort)** before export.
   - set **Website pages per site** (crawl depth)
   - optional: enable **Show tabs while enriching**
   - optional: enable **Hunt email on social links**
   - optional: enable **Enable external lead discovery**
   - optional: toggle **Google Search (top 3 results)**
   - enrichment auto-starts after scrape completion when enabled
8. Click **Export CSV** after completion (or after stop).

## Notes / Limitations
- English UI-first selector strategy; non-English UI may partially work.
- Google Maps DOM changes can break selectors and require updates.
- Website enrichment is best-effort and limited to publicly visible data.
- External discovery may increase runtime due extra Google lookups.
- Existing Maps website is preserved when present; alternate discovery domains are stored in discovery metadata fields.
- Some websites block automation (CAPTCHA/anti-bot); blocked scans are marked and skipped.
- One active query per run.

## Compliance
You are responsible for complying with Google terms, local law, and privacy/data-use obligations.
