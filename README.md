# Scrapify

<p align="center">
  <img src="assets/brand/scrapify-logo.svg" alt="Scrapify logo" width="240" />
</p>

<p align="center">
  Google Maps lead scraping plus focused website enrichment in a Manifest V3 Chrome extension.
</p>

<p align="center">
  <a href="https://github.com/Ahmadkhattak1/scrapify-extension/stargazers">
    <img alt="GitHub stars" src="https://img.shields.io/github/stars/Ahmadkhattak1/scrapify-extension?style=for-the-badge" />
  </a>
  <a href="https://github.com/Ahmadkhattak1/scrapify-extension/forks">
    <img alt="GitHub forks" src="https://img.shields.io/github/forks/Ahmadkhattak1/scrapify-extension?style=for-the-badge" />
  </a>
  <a href="https://github.com/Ahmadkhattak1/scrapify-extension/issues">
    <img alt="GitHub issues" src="https://img.shields.io/github/issues/Ahmadkhattak1/scrapify-extension?style=for-the-badge" />
  </a>
  <img alt="Chrome Manifest V3" src="https://img.shields.io/badge/Chrome-Manifest%20V3-4285F4?style=for-the-badge&logo=googlechrome&logoColor=white" />
  <img alt="No build step" src="https://img.shields.io/badge/Setup-No%20build%20step-111827?style=for-the-badge" />
</p>

<p align="center">
  <a href="https://github.com/Ahmadkhattak1/scrapify-extension/stargazers"><strong>Star the repo</strong></a>
  -
  <a href="#installation"><strong>Install locally</strong></a>
  -
  <a href="#quick-start"><strong>Quick start</strong></a>
  -
  <a href="https://github.com/Ahmadkhattak1/scrapify-extension/issues"><strong>Report an issue</strong></a>
</p>

## TL;DR

Scrapify lets you scrape Google Maps search results, filter weak leads, enrich public business websites for emails and phone numbers, review everything in a built-in viewer, and export clean CSVs without any build step.

- Load the repo as an unpacked Chrome extension.
- Start from a Google Maps results page.
- Run a basic scrape or enable deep website enrichment.
- Use parallel workers to speed up enrichment.
- Let Scrapify surface challenge tabs when manual CAPTCHA solving is worth the extra yield.
- If the project saves you time, [star the repository](https://github.com/Ahmadkhattak1/scrapify-extension/stargazers).

## Why People Use Scrapify

- No build pipeline. Load the repository directly into Chrome and start using it.
- Focused Google Maps scraping with filters for rating, review count, website, phone, and email availability.
- Deep website enrichment that checks the homepage first, prioritizes contact-style pages, and selectively explores internal links.
- Parallel enrichment workers, with support for up to `6` concurrent workers.
- Challenge-aware enrichment that can wait for manual CAPTCHA resolution or skip challenged tabs for unattended runs.
- Built-in results viewer for live review, CSV import/export, and manual follow-up tracking.
- Session and UI persistence in `chrome.storage.local`, with regular and incognito sessions kept separate.

## What Scrapify Captures

- From Google Maps: place ID, business name, rating, review count, category, address, hours, Maps URL, website, listing phone, and Facebook link when available.
- From website enrichment: unified best email and phone fields, raw owner/contact details, source URLs, confidence metadata, and crawl status details.

## Installation

There is no build step.

1. Open Chrome and go to `chrome://extensions/`.
2. Turn on `Developer mode`.
3. Click `Load unpacked`.
4. Select this repository folder.
5. Optional: enable `Allow in Incognito` if you want isolated incognito runs too.

## Quick Start

1. Open Google Maps and run a search such as `dentists in chicago`.
2. Click the Scrapify extension icon to open the control panel.
3. Set `Max rows` and any lead filters you want.
4. Optional: enable `Enrich websites (Deep Crawl)` to collect emails and phone numbers from public sites.
5. Choose your preferred email and phone output mode.
6. Click `Start Scrape`.
7. Open `Viewer` to review the run and export CSV.

## Enrichment, Workers, and CAPTCHA

### Website enrichment

When `Enrich websites` is enabled, Scrapify continues after the Maps scrape and scans public business websites for contact data. The crawl is intentionally focused:

- homepage first
- contact, about, team, and careers routes first
- selective internal link exploration after that
- Facebook fallback when useful contact details are still missing

Use `Collect emails` and `Collect phone numbers` to narrow the goal, and switch `Email columns` / `Phone columns` between best-only exports and raw detail exports.

### Parallel workers

Scrapify now supports concurrent enrichment workers.

- `Parallel workers` controls how many enrichment tabs run at once.
- The default is `3`, and the current max is `6`.
- More workers can speed up large runs, but they also increase the chance of anti-bot checks and CAPTCHAs.

### CAPTCHA and challenge handling

Challenges are normal during enrichment. Depending on the site or provider, a CAPTCHA or verification page can appear after only a small number of rows or tabs.

For the best data yield:

- Keep `Challenge handling` on `Try once, then wait (rec.)`.
- Leave `Keep going while waiting` on `1 more worker (rec.)` or raise it if you want limited parallel progress while one tab is blocked.
- When Scrapify shows `CAPTCHA needs your attention`, use the `Focus` action in the control panel, solve the challenge in that tab, and Scrapify will resume automatically when the page clears.

If you do not want manual intervention, change the challenge setting before you start the run:

- `Try once, then skip` keeps the run mostly automatic and skips challenged tabs after one automatic checkbox attempt.
- `Skip immediately` keeps the run fully automatic and never waits on a challenge.

## Results Viewer

The built-in viewer is a separate extension page for review and export.

It can:

- show live run data
- open a saved run view
- import an existing CSV
- export the current dataset back to CSV
- resize and auto-fit columns
- add a final tracking checkbox column that you can rename to something like `Contacted` or `Qualified`

For large runs, the browser table previews the first `1000` rows for performance, but CSV export uses the full loaded dataset.

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

## Support the Project

If Scrapify helps you find leads faster, please [star the repo](https://github.com/Ahmadkhattak1/scrapify-extension/stargazers). That makes the project more discoverable and helps prioritize future improvements.

If Google Maps or a target site changes and breaks a flow, [open an issue](https://github.com/Ahmadkhattak1/scrapify-extension/issues) with a clear description, sample query, and screenshots where possible.

## Contributing

Contributions are welcome. If you want to improve scraping stability, enrichment accuracy, challenge handling, UI polish, or documentation, open an issue first or send a pull request directly.

Small fixes are useful too, not just major features. If you build something valuable here, please include clear reproduction steps, before/after behavior, and screenshots when the change affects the UI.

## License

This repository is licensed under `PolyForm Noncommercial 1.0.0`.

- Personal use and other noncommercial use are allowed.
- Selling this extension, bundling it into a commercial product, or using it for commercial purposes is not allowed under this repository license.
- If you need commercial rights, do not assume this license grants them.

## Compliance

You are responsible for using this extension in a way that complies with Google Maps terms, the target sites' terms, local law, privacy obligations, and your own data-handling requirements.
