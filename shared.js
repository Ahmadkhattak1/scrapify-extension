(function (global) {
  const MSG = {
    START_SCRAPE: "START_SCRAPE",
    STOP_SCRAPE: "STOP_SCRAPE",
    GET_SCRAPE_STATE: "GET_SCRAPE_STATE",
    SCRAPE_STATE: "SCRAPE_STATE",
    SCRAPE_PROGRESS: "SCRAPE_PROGRESS",
    SCRAPE_DONE: "SCRAPE_DONE",
    SCRAPE_ERROR: "SCRAPE_ERROR",
    ENRICH_ROWS: "ENRICH_ROWS",
    STOP_ENRICH: "STOP_ENRICH",
    GET_ENRICH_STATE: "GET_ENRICH_STATE",
    ENRICH_PROGRESS: "ENRICH_PROGRESS",
    ENRICH_DONE: "ENRICH_DONE",
    ENRICH_ERROR: "ENRICH_ERROR",
    EXPORT_CSV: "EXPORT_CSV",
    EXPORT_DONE: "EXPORT_DONE",
    EXPORT_ERROR: "EXPORT_ERROR"
  };

  const DEFAULT_MAX_ROWS = 200;

  const CSV_COLUMNS = [
    "place_id",
    "name",
    "rating",
    "review_count",
    "category",
    "address",
    "phone",
    "listing_phone",
    "website_phone",
    "website_phone_source",
    "website",
    "email",
    "owner_name",
    "owner_title",
    "owner_email",
    "contact_email",
    "primary_email",
    "primary_email_type",
    "primary_email_source",
    "owner_confidence",
    "email_confidence",
    "email_source_url",
    "no_email_reason",
    "website_scan_status",
    "site_pages_visited",
    "site_pages_discovered",
    "social_pages_scanned",
    "social_links",
    "hours",
    "maps_url",
    "source_query",
    "source_url",
    "scraped_at"
  ];

  const COLUMN_LABELS = {
    place_id: "Place ID",
    name: "Name",
    rating: "Rating",
    review_count: "Review Count",
    category: "Category",
    address: "Address",
    phone: "Phone",
    listing_phone: "Listing Phone",
    website_phone: "Website Phone (Scanned)",
    website_phone_source: "Website Phone Source",
    website: "Website",
    email: "Email",
    owner_name: "Owner Name",
    owner_title: "Owner Title",
    owner_email: "Owner Email (Personal)",
    contact_email: "Contact Email (Company)",
    primary_email: "Primary Email (Auto)",
    primary_email_type: "Primary Email Type",
    primary_email_source: "Primary Email Source",
    owner_confidence: "Owner Confidence",
    email_confidence: "Email Confidence",
    email_source_url: "Email Source URL",
    no_email_reason: "No Email Reason",
    website_scan_status: "Website Scan Status",
    site_pages_visited: "Site Pages Visited",
    site_pages_discovered: "Site Pages Discovered",
    social_pages_scanned: "Social Pages Scanned",
    social_links: "Social Links",
    hours: "Hours",
    maps_url: "Maps URL",
    source_query: "Source Query",
    source_url: "Source URL",
    scraped_at: "Scraped At"
  };

  function normalizeText(value) {
    if (value == null) return "";
    return String(value)
      .replace(/[\u0000-\u001F\u007F-\u009F]/g, " ")
      .replace(/[\u200B-\u200F\u202A-\u202E\u2060-\u2069\uFEFF\uFFFC\uFFFD]/g, "")
      .replace(/\s+/g, " ")
      .trim();
  }

  function parseRating(value) {
    if (value == null) return "";
    const text = normalizeText(value).replace(",", ".");
    const match = text.match(/\d+(?:\.\d+)?/);
    if (!match) return "";
    const parsed = Number.parseFloat(match[0]);
    return Number.isFinite(parsed) ? parsed : "";
  }

  function parseReviewCount(value) {
    if (value == null) return "";
    const text = normalizeText(value);
    if (!text) return "";

    const reviewWord = text.match(/(\d[\d,]*(?:\.\d+)?\s*[kmb]?)\s+reviews?\b/i);
    if (reviewWord && reviewWord[1]) {
      const parsed = parseAbbreviatedCount(reviewWord[1]);
      if (Number.isFinite(parsed)) return parsed;
    }

    const parenPattern = text.match(/\((\d[\d,]*(?:\.\d+)?\s*[kmb]?)\)/i);
    if (parenPattern && parenPattern[1]) {
      const parsed = parseAbbreviatedCount(parenPattern[1]);
      if (Number.isFinite(parsed)) return parsed;
    }

    const bulletPattern = text.match(/\b([0-5](?:\.\d)?)\s*[·•]\s*(\d[\d,]*(?:\.\d+)?\s*[kmb]?)\b/i);
    if (bulletPattern && bulletPattern[2]) {
      const parsed = parseAbbreviatedCount(bulletPattern[2]);
      if (Number.isFinite(parsed)) return parsed;
    }

    const fallbackPattern = text.match(/\b(\d[\d,]*(?:\.\d+)?\s*[kmb]?)\b/i);
    if (fallbackPattern && fallbackPattern[1]) {
      const parsed = parseAbbreviatedCount(fallbackPattern[1]);
      if (Number.isFinite(parsed)) return parsed;
    }

    return "";
  }

  function parseAbbreviatedCount(value) {
    const raw = normalizeText(value).toLowerCase().replace(/\s+/g, "");
    if (!raw) return "";
    const match = raw.match(/^(\d[\d,]*)(?:\.(\d+))?([kmb])?$/i);
    if (!match) return "";

    const whole = Number((match[1] || "").replace(/,/g, ""));
    if (!Number.isFinite(whole)) return "";
    const fraction = match[2] ? Number(`0.${match[2]}`) : 0;
    if (!Number.isFinite(fraction)) return "";
    const suffix = (match[3] || "").toLowerCase();
    if (!suffix && match[2]) return "";
    const multiplier = suffix === "k" ? 1000 : suffix === "m" ? 1000000 : suffix === "b" ? 1000000000 : 1;
    const scaled = Math.round((whole + fraction) * multiplier);
    return Number.isFinite(scaled) ? scaled : "";
  }

  function normalizeMapsUrl(url) {
    const raw = normalizeText(url);
    if (!raw) return "";

    try {
      const parsed = new URL(raw, "https://www.google.com");
      if (!parsed.hostname.includes("google.")) return raw;
      parsed.searchParams.delete("hl");
      parsed.searchParams.delete("entry");
      parsed.searchParams.delete("g_ep");
      parsed.hash = "";
      return parsed.toString();
    } catch (_e) {
      return raw;
    }
  }

  function normalizeWebsiteUrl(url) {
    const raw = normalizeText(url);
    if (!raw) return "";

    let candidate = raw;
    if (!/^https?:\/\//i.test(candidate)) {
      if (/^[/?#]/.test(candidate)) return "";
      const hostLike = candidate.split(/[/?#]/)[0];
      if (!hostLike || !hostLike.includes(".")) return "";
      candidate = `https://${candidate}`;
    }

    try {
      const parsed = new URL(candidate);
      if (!/^https?:$/i.test(parsed.protocol)) return "";
      parsed.hash = "";
      return parsed.toString();
    } catch (_e) {
      return "";
    }
  }

  function decodeUrlComponentSafe(value) {
    const input = normalizeText(value).replace(/&amp;/gi, "&");
    if (!input) return "";

    let out = input;
    for (let i = 0; i < 2; i += 1) {
      try {
        const decoded = decodeURIComponent(out);
        if (!decoded || decoded === out) break;
        out = decoded;
      } catch (_error) {
        break;
      }
    }
    return normalizeText(out);
  }

  function extractUrlCandidate(value) {
    const raw = decodeUrlComponentSafe(value);
    if (!raw) return "";

    if (/^\/\//.test(raw)) {
      return normalizeWebsiteUrl(`https:${raw}`);
    }

    if (/^https?:\/\//i.test(raw)) {
      return normalizeWebsiteUrl(raw);
    }

    if (/^[a-z0-9][a-z0-9.-]+\.[a-z]{2,}(?:\/.*)?$/i.test(raw)) {
      return normalizeWebsiteUrl(raw);
    }

    // Handle nested wrappers like "/url?q=https%3A%2F%2Fexample.com"
    if (/^\/?(?:url|aclk|local_url)\?/i.test(raw)) {
      try {
        const normalizedPath = raw.startsWith("/") ? raw : `/${raw}`;
        const parsed = new URL(`https://www.google.com${normalizedPath}`);
        const nested = parsed.searchParams.get("q") || parsed.searchParams.get("url") || parsed.searchParams.get("adurl");
        return extractUrlCandidate(nested);
      } catch (_error) {
        return "";
      }
    }

    return "";
  }

  function unwrapGoogleRedirect(url) {
    const raw = decodeUrlComponentSafe(url);
    let candidate = normalizeWebsiteUrl(raw);
    if (!candidate && /^\/?(?:url|aclk|local_url)\?/i.test(raw)) {
      const normalizedPath = raw.startsWith("/") ? raw : `/${raw}`;
      candidate = normalizeWebsiteUrl(`https://www.google.com${normalizedPath}`);
    }
    if (!candidate && /^www\.google\./i.test(raw)) {
      candidate = normalizeWebsiteUrl(`https://${raw}`);
    }
    if (!candidate && /^\/\/www\.google\./i.test(raw)) {
      candidate = normalizeWebsiteUrl(`https:${raw}`);
    }
    if (!candidate) return "";

    const redirectKeys = ["adurl", "url", "q", "redirect", "dest", "target", "continue", "u"];

    for (let depth = 0; depth < 4; depth += 1) {
      let parsed = null;
      try {
        parsed = new URL(candidate);
      } catch (_error) {
        return "";
      }

      const host = normalizeText(parsed.hostname).toLowerCase();
      const isGoogleHost =
        /(^|\.)google\./i.test(host) ||
        host.includes("googleadservices.com") ||
        host.includes("g.doubleclick.net");
      if (!isGoogleHost) break;

      let next = "";
      for (const key of redirectKeys) {
        const value = parsed.searchParams.get(key);
        const extracted = extractUrlCandidate(value);
        if (extracted) {
          next = extracted;
          break;
        }
      }

      if (!next && parsed.hash) {
        const hash = parsed.hash.startsWith("#") ? parsed.hash.slice(1) : parsed.hash;
        try {
          const hashParams = new URLSearchParams(hash);
          for (const key of redirectKeys) {
            const value = hashParams.get(key);
            const extracted = extractUrlCandidate(value);
            if (extracted) {
              next = extracted;
              break;
            }
          }
        } catch (_error) {
          // Ignore malformed hash params.
        }
      }

      if (!next || next === candidate) {
        return "";
      }
      candidate = next;
    }

    return candidate;
  }

  function normalizeBusinessWebsiteUrl(url) {
    const direct = normalizeWebsiteUrl(url);
    const unwrapped = unwrapGoogleRedirect(url);
    const normalized = unwrapped || direct;
    if (!normalized) return "";

    try {
      const parsed = new URL(normalized);
      const host = normalizeText(parsed.hostname).toLowerCase();
      if (!host) return "";

      if (
        /(^|\.)google\./i.test(host) ||
        host.includes("googleadservices.com") ||
        host.includes("g.doubleclick.net") ||
        host.includes("gstatic.com")
      ) {
        return "";
      }

      const noisyParams = [
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "gclid",
        "fbclid",
        "msclkid",
        "mc_cid",
        "mc_eid",
        "ref",
        "source"
      ];
      for (const param of noisyParams) {
        parsed.searchParams.delete(param);
      }
      parsed.hash = "";
      return parsed.toString();
    } catch (_error) {
      return "";
    }
  }

  function dedupeKey(row) {
    if (row && row.place_id) return `place:${String(row.place_id).trim().toLowerCase()}`;
    if (row && row.maps_url) return `url:${normalizeMapsUrl(row.maps_url).toLowerCase()}`;
    return "";
  }

  function hasValue(value) {
    return normalizeText(value) !== "";
  }

  function safeLower(value) {
    return normalizeText(value).toLowerCase();
  }

  function includesNeedle(haystack, needle) {
    return safeLower(haystack).includes(safeLower(needle));
  }

  function applyFilters(row, filters) {
    const f = filters || {};

    if (f.minRating !== "" && f.minRating != null) {
      const minRating = Number(f.minRating);
      const rating = Number(row.rating);
      if (!Number.isFinite(rating) || rating < minRating) return false;
    }

    if (f.maxRating !== "" && f.maxRating != null) {
      const maxRating = Number(f.maxRating);
      const rating = Number(row.rating);
      if (!Number.isFinite(rating) || rating > maxRating) return false;
    }

    if (f.minReviews !== "" && f.minReviews != null) {
      const minReviews = Number(f.minReviews);
      const reviews = Number(row.review_count);
      if (!Number.isFinite(reviews) || reviews < minReviews) return false;
    }

    if (f.maxReviews !== "" && f.maxReviews != null) {
      const maxReviews = Number(f.maxReviews);
      const reviews = Number(row.review_count);
      if (!Number.isFinite(reviews) || reviews > maxReviews) return false;
    }

    if (hasValue(f.nameKeyword) && !includesNeedle(row.name, f.nameKeyword)) {
      return false;
    }

    if (hasValue(f.categoryInclude) && !includesNeedle(row.category, f.categoryInclude)) {
      return false;
    }

    if (hasValue(f.categoryExclude) && includesNeedle(row.category, f.categoryExclude)) {
      return false;
    }

    if (f.hasWebsite === true && !hasValue(row.website)) {
      return false;
    }

    if (f.hasPhone === true && !hasValue(row.phone)) {
      return false;
    }

    return true;
  }

  function csvEscape(value) {
    if (value == null) return "";
    const text = String(value);
    const escaped = text.replace(/"/g, '""');
    if (/[",\n]/.test(escaped)) {
      return `"${escaped}"`;
    }
    return escaped;
  }

  function sanitizeColumns(columns) {
    if (!Array.isArray(columns)) return [...CSV_COLUMNS];
    const valid = [];
    const seen = new Set();
    for (const column of columns) {
      if (!CSV_COLUMNS.includes(column) || seen.has(column)) continue;
      seen.add(column);
      valid.push(column);
    }
    return valid.length > 0 ? valid : [...CSV_COLUMNS];
  }

  function rowsToCsv(rows, columns) {
    const selectedColumns = sanitizeColumns(columns);
    const header = selectedColumns.join(",");
    const body = (rows || []).map((row) => {
      return selectedColumns
        .map((key) => {
          const value = row ? row[key] : "";
          if (typeof value === "string") {
            return csvEscape(normalizeText(value));
          }
          return csvEscape(value);
        })
        .join(",");
    });
    return [header, ...body].join("\n");
  }

  function toNumberOrEmpty(value) {
    if (value === "" || value == null) return "";
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : "";
  }

  function readFilterConfig(formValues) {
    const values = formValues || {};
    return {
      minRating: toNumberOrEmpty(values.minRating),
      maxRating: toNumberOrEmpty(values.maxRating),
      minReviews: toNumberOrEmpty(values.minReviews),
      maxReviews: toNumberOrEmpty(values.maxReviews),
      nameKeyword: normalizeText(values.nameKeyword),
      categoryInclude: normalizeText(values.categoryInclude),
      categoryExclude: normalizeText(values.categoryExclude),
      hasWebsite: Boolean(values.hasWebsite),
      hasPhone: Boolean(values.hasPhone)
    };
  }

  const api = {
    MSG,
    DEFAULT_MAX_ROWS,
    CSV_COLUMNS,
    COLUMN_LABELS,
    normalizeText,
    parseRating,
    parseReviewCount,
    normalizeMapsUrl,
    normalizeWebsiteUrl,
    normalizeBusinessWebsiteUrl,
    dedupeKey,
    applyFilters,
    sanitizeColumns,
    csvEscape,
    rowsToCsv,
    readFilterConfig
  };

  global.GbpShared = api;
})(typeof window !== "undefined" ? window : self);
