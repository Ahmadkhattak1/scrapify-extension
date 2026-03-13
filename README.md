# Scrapify

Scrapify is a Manifest V3 Chrome extension for collecting business listing data from Google Maps search results, optionally enriching those listings by scanning public business websites, and exporting the final dataset to CSV.

It is built around a simple workflow:

1. Open a Google Maps search results page.
2. Launch the Scrapify control panel.
3. Set filters and export columns.
4. Run the scrape.
5. Optionally enrich websites for emails and phone numbers.
6. Review everything in the built-in results viewer and export CSV.

## What Scrapify Does

- Scrapes listing data from the active `https://www.google.com/maps/*` results page
- Filters results by rating, review count, website, phone, and email availability
- Optionally ignores the row limit with infinite scrolling
- Deduplicates rows by `place_id` with URL fallback
- Optionally enriches listings by visiting public websites and Facebook pages
- Generates unified "best" email and phone fields for cleaner exports
- Lets you choose exactly which CSV columns to include
- Opens a separate results viewer for review, CSV import, and CSV export
- Persists sessions, results, and UI settings in `chrome.storage.local`
- Keeps regular and incognito sessions isolated with `incognito: split`

## Key Data It Can Capture

From Google Maps:

- place ID
- business name
- rating
- review count
- category
- address
- hours
- Maps URL
- website
- listing phone
- listing Facebook link when available

From website enrichment:

- best email (`email`)
- owner email
- company/contact email
- best email source and confidence metadata
- best phone (`phone`)
- website phone
- website phone source
- crawl status and crawl metrics

## Installation

There is no build step. Load the extension directly as an unpacked Chrome extension.

1. Open Chrome and go to `chrome://extensions/`.
2. Turn on **Developer mode**.
3. Click **Load unpacked**.
4. Select this repository folder.
5. Optional: open the extension details page and enable **Allow in Incognito** if you want to run it in incognito windows.

## Quick Start

1. Open [Google Maps](https://www.google.com/maps) and search for something like `dentists in chicago`.
2. Click the Scrapify extension icon.
3. A dedicated control panel window opens.
4. Set your row limit, filters, enrichment settings, and export columns.
5. Click **Start Scrape**.
6. Wait for the run to finish or click **Stop** if you want to end early.
7. Open the **Viewer** if it is not already open automatically.
8. Review the rows and click **Export CSV** from the results viewer.

## How the UI Works

### Control Panel

The control panel is the main command center. It opens in a separate popup window so you can keep it visible while browsing Maps.

It has four main parts:

- **Status dashboard**: live counters, progress, averages, and enrichment stats
- **Core Filters**: row limit and basic lead filters
- **Enrichment Engine**: website scanning and contact extraction settings
- **Export Columns**: choose which CSV fields should be included

### Footer Buttons

- **Viewer**: opens the results viewer tab
- **Stop**: requests the current scrape or enrichment run to stop
- **Start Scrape**: starts scraping the active Google Maps results page

## Option Reference

### Core Filters

| Option | What it does |
| --- | --- |
| `Max rows` | Stops after this many matched rows are collected. Default is `200`. |
| `Infinite (ignore max)` | Keeps scraping until Google Maps runs out of results or you stop the run manually. |
| `Min rating` | Keeps only listings at or above the given rating. |
| `Max rating` | Keeps only listings at or below the given rating. |
| `Min reviews` | Keeps only listings with at least this many reviews. |
| `Max reviews` | Keeps only listings with at most this many reviews. |
| `Has website` | Keeps only listings that already have a website on Google Maps. |
| `Has phone` | Keeps only listings that already have a phone number on Google Maps. |
| `Keep only leads with email` | Final output only keeps rows that have an email. This is most useful when website enrichment is enabled. |

### Enrichment Engine

When **Enrich websites** is enabled, Scrapify will continue after the Maps scrape and scan public business websites for contact details.

The enrichment flow is focused and conservative:

- checks the homepage first
- prioritizes contact/about/team/careers style pages
- scans discovered internal links selectively
- falls back to Facebook when useful contact details are still missing
- skips blocked or low-value paths where possible

Available options:

| Option | What it does |
| --- | --- |
| `Enrich websites (Deep Crawl)` | Turns website scanning on or off. |
| `Collect emails` | Tells the enrichment step to look for email addresses. |
| `Collect phone numbers` | Tells the enrichment step to look for phone numbers. |
| `Email columns` | Controls how email fields appear in the export. |
| `Phone columns` | Controls how phone fields appear in the export. |
| `Show tabs while enriching` | Opens visible crawl tabs during enrichment instead of keeping everything hidden in the background. |

#### Email Column Modes

| Mode | Result |
| --- | --- |
| `Best only (rec.)` | Exports one unified `email` column only. |
| `Best + raw` | Exports the unified `email` column plus detailed email fields such as `owner_email` and `contact_email`. |
| `Raw only` | Exports detailed email fields and removes the unified `email` column. |

#### Phone Column Modes

| Mode | Result |
| --- | --- |
| `Best only (rec.)` | Exports one unified `phone` column only. |
| `Best + raw` | Exports the unified `phone` column plus raw phone detail fields. |
| `Raw only` | Exports raw phone detail fields and removes the unified `phone` column. |

### Export Columns

The column picker controls the CSV schema.

Available controls:

- **Basic / Advanced**: shows or hides metadata-heavy fields
- **All**: selects every currently visible column
- **None**: clears every currently visible column
- Individual checkboxes: choose columns one by one

Column groups in the UI:

- **Core Listing Data**: main listing fields such as name, rating, category, address, website, and Maps URL
- **Phone Output**: unified and raw phone-related fields
- **Email Output**: unified and raw email-related fields
- **Enrichment Crawl Meta**: crawl diagnostics such as scan status and pages visited
- **Discovery Meta**: diagnostic fields for website discovery and recovery logic

The default export selection is focused on the most useful fields:

- core listing data
- phone output
- email output

## Results Viewer

The results viewer is a separate extension page used for review and export.

It can:

- show the latest live run
- open a specific run view
- import an existing CSV file
- export the current table back to CSV
- refresh back to live extension data after viewing an imported file
- resize columns manually
- auto-fit columns by double-clicking a column header

### Viewer Tracking Column

The viewer automatically appends one extra checkbox column at the end of the table.

You can use it to:

- mark rows as reviewed
- rename the column header to something like `Contacted`, `Qualified`, or `Followed Up`
- bulk-check or bulk-clear all rows
- export that tracking column along with the CSV

If you import a CSV whose last column looks like a tracking column (`TRUE`, `FALSE`, `yes`, `no`, `x`, and similar values), Scrapify will restore it as the viewer tracking column.

### Large Runs

For performance, the viewer renders the first `1000` rows in the browser table. CSV export still uses the full loaded dataset, not just the visible preview.

## Recommended Workflow

### Basic scrape only

Use this when you only need Google Maps listing data.

1. Search on Google Maps.
2. Set `Max rows`.
3. Optionally apply rating/review/website/phone filters.
4. Leave enrichment disabled.
5. Run the scrape.
6. Review results in the viewer.
7. Export CSV.

### Lead generation with website contacts

Use this when you want emails and cleaner phone output.

1. Search on Google Maps.
2. Enable **Enrich websites**.
3. Keep **Collect emails** enabled.
4. Optionally keep **Collect phone numbers** enabled.
5. Set email and phone output modes based on how much detail you want in the CSV.
6. Enable **Keep only leads with email** if you only want rows that end with an email.
7. Run the scrape and wait for enrichment to finish.
8. Export from the viewer.

## CSV Columns

Scrapify supports these export columns:

`place_id`, `name`, `rating`, `review_count`, `category`, `address`, `phone`, `listing_phone`, `website_phone`, `website_phone_source`, `website`, `listing_facebook`, `facebook_could_be`, `email`, `owner_name`, `owner_title`, `owner_email`, `contact_email`, `primary_email`, `primary_email_type`, `primary_email_source`, `owner_confidence`, `email_confidence`, `email_source_url`, `no_email_reason`, `website_scan_status`, `site_pages_visited`, `site_pages_discovered`, `social_pages_scanned`, `social_links`, `discovery_status`, `discovery_source`, `discovery_query`, `discovered_website`, `hours`, `maps_url`, `source_query`, `source_url`, `scraped_at`

### Important Column Notes

- `email` is the unified "best" email output
- `phone` is the unified "best" phone output
- `listing_phone` comes from Google Maps
- `website_phone` comes from website scanning
- `primary_email` is Scrapify's best discovered email candidate with supporting metadata
- `source_query`, `source_url`, and `scraped_at` are useful for auditability
- discovery-related fields are diagnostic and may be blank in many runs

## Live Metrics

During a run, the control panel can show:

- processed rows
- matched rows
- duplicates
- errors
- scrape speed
- seen listings
- average rating
- average reviews
- site pages visited
- site pages discovered
- social pages scanned
- emails found
- discovery emails found

## Permissions

Scrapify requests these permissions because they are required for the workflow above:

| Permission | Why it is needed |
| --- | --- |
| `activeTab` | Starts a scrape from the current Google Maps tab. |
| `tabs` | Opens and manages the control panel, results viewer, and enrichment tabs. |
| `storage` | Saves settings, sessions, rows, and selected export columns. |
| `downloads` | Saves exported CSV files. |
| `scripting` | Runs extraction and enrichment logic in tabs. |
| host access to `https://www.google.com/maps/*` | Reads Google Maps results. |
| host access to `http://*/*` and `https://*/*` | Visits public websites during enrichment. |

## Limitations

- Works on `https://www.google.com/maps/*` only in the current build.
- You need to start from a Google Maps search results page, not an arbitrary website tab.
- Google Maps DOM changes can break selectors and require code updates.
- Website enrichment is best-effort and depends on publicly visible contact data.
- Some sites block automation, loading, or script access; those rows are skipped or marked accordingly.
- The current selector strategy is English-leaning, so some localized Maps UIs may be less reliable.
- Only one active scrape/enrichment run should be treated as authoritative at a time.
- Regular and incognito sessions are intentionally isolated from each other.

## Project Structure

- `manifest.json` - extension manifest
- `background.js` - service worker, export handling, enrichment orchestration, viewer/control-panel opening
- `content.js` - Google Maps scraping logic
- `shared.js` - shared helpers, filter logic, CSV utilities, and column definitions
- `popup.html`, `popup.css`, `popup.js` - control panel UI
- `results.html`, `results.css`, `results.js` - results viewer UI
- `assets/brand/` - icons and logo assets

## Development Notes

- No bundler or build pipeline is required
- Load the repo directly as an unpacked extension
- Most user-facing behavior is driven by `popup.js`, `content.js`, `background.js`, and `shared.js`

## Compliance

You are responsible for using this extension in a way that complies with Google Maps terms, the target sites' terms, local law, privacy obligations, and your own data-handling requirements.
