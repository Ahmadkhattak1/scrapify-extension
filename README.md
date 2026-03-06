# Scrapify (Chrome Extension)

Chrome extension (Manifest V3) to scrape Google Maps business profile rows from the currently open search results page, apply filters, and export CSV.

## Features
- Scrape from active Google Maps search results page
- Incognito-aware run isolation (`incognito: split`) so incognito runs stay separate from regular browsing session
- Built-in filters:
  - minimum rating
  - maximum rating
  - minimum reviews
  - maximum reviews
  - name keyword
  - category include/exclude
  - has website
  - has phone
  - keep only leads with email
- Optional `Infinite scroll` mode (ignores max rows and keeps scraping until end-of-results or Stop)
- Optional website enrichment mode (category-agnostic):
  - primary contact goals are explicit: `email`, `phone`, or both
  - scans focused routes (`/contact`, `/about`, `/team`, `/careers`) with common aliases plus homepage/footer signals
  - skips low-value pages like blog/news/product/catalog routes
  - always includes Facebook/social fallback when selected contact goals are still missing
  - contact flow is deterministic: `GBP website -> homepage/contact/about/team/careers -> Facebook (GBP link or site social link) -> unavailable`
  - optional visible-tab mode opens scan tabs without stealing focus from your active tab
  - skips blocked/unavailable websites and keeps core GBP data
- Persistent run/session snapshots in `chrome.storage.local` so progress/results survive popup close/reopen
- Email precedence output fields:
  - `email` (single unified export email)
  - `primary_email` (best discovered email from website/Facebook flow)
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
5. Optional (for private runs): in extension details, enable **Allow in Incognito**.

## Usage
1. Open [Google Maps](https://www.google.com/maps) and run a search (example: `dentists in chicago`).
2. Click the extension icon to open the scraper control panel popup window (it stays open while you switch tabs).
3. Configure max rows and optional filters.
4. Optional: enable **Infinite scroll** to ignore max rows.
5. Choose export columns (All/None or individual checkboxes).
6. Click **Start Scrape**.
7. Optional: enable **Enrich websites** before export.
   - set primary contact goals: **Collect emails**, **Collect phone numbers**, or both
   - focused crawl checks homepage + contact/about/team/careers routes (and common aliases)
   - optional: enable **Show tabs while enriching**
   - Facebook/social fallback runs automatically during enrichment
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
- In incognito windows, extension state/storage is isolated from the regular profile.

## Compliance
You are responsible for complying with Google terms, local law, and privacy/data-use obligations.
