(function (global) {
  const MSG = {
    START_SCRAPE: "START_SCRAPE",
    STOP_SCRAPE: "STOP_SCRAPE",
    SCRAPE_PROGRESS: "SCRAPE_PROGRESS",
    SCRAPE_DONE: "SCRAPE_DONE",
    SCRAPE_ERROR: "SCRAPE_ERROR",
    ENRICH_ROWS: "ENRICH_ROWS",
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
    "website",
    "owner_name",
    "owner_title",
    "owner_email",
    "contact_email",
    "website_scan_status",
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
    website: "Website",
    owner_name: "Owner Name",
    owner_title: "Owner Title",
    owner_email: "Owner Email",
    contact_email: "Contact Email",
    website_scan_status: "Website Scan Status",
    hours: "Hours",
    maps_url: "Maps URL",
    source_query: "Source Query",
    source_url: "Source URL",
    scraped_at: "Scraped At"
  };

  function normalizeText(value) {
    if (value == null) return "";
    return String(value).replace(/\s+/g, " ").trim();
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
    const text = normalizeText(value).replace(/,/g, "");
    const match = text.match(/\d+/g);
    if (!match || match.length === 0) return "";
    const joined = match.join("");
    const parsed = Number.parseInt(joined, 10);
    return Number.isFinite(parsed) ? parsed : "";
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
      return selectedColumns.map((key) => csvEscape(row[key])).join(",");
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
    dedupeKey,
    applyFilters,
    sanitizeColumns,
    csvEscape,
    rowsToCsv,
    readFilterConfig
  };

  global.GbpShared = api;
})(typeof window !== "undefined" ? window : self);
