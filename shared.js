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
    "discovery_status",
    "discovery_source",
    "discovery_query",
    "discovered_website",
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
    owner_email: "Owner Email",
    contact_email: "Company Email",
    primary_email: "Best Email (Auto)",
    primary_email_type: "Best Email Type",
    primary_email_source: "Best Email Source",
    owner_confidence: "Owner Match Score",
    email_confidence: "Email Match Score",
    email_source_url: "Email Source URL",
    no_email_reason: "No Email Reason",
    website_scan_status: "Website Scan Result",
    site_pages_visited: "Site Pages Visited",
    site_pages_discovered: "Site Pages Discovered",
    social_pages_scanned: "Social Pages Scanned",
    social_links: "Social Links",
    discovery_status: "Discovery Status",
    discovery_source: "Discovery Source",
    discovery_query: "Discovery Query",
    discovered_website: "Discovered Website",
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

  function isLikelyPhoneDigits(digits) {
    const compact = normalizeText(digits).replace(/\D/g, "");
    return compact.length >= 10 && compact.length <= 15;
  }

  function formatNorthAmericaPhone(digits) {
    const compact = normalizeText(digits).replace(/\D/g, "");
    if (compact.length === 10) {
      return `(${compact.slice(0, 3)}) ${compact.slice(3, 6)}-${compact.slice(6)}`;
    }
    if (compact.length === 11 && compact.startsWith("1")) {
      return `(${compact.slice(1, 4)}) ${compact.slice(4, 7)}-${compact.slice(7)}`;
    }
    return "";
  }

  function normalizePhoneText(value) {
    const raw = normalizeText(value);
    if (!raw) return "";

    const withoutExtension = raw.replace(/\b(?:ext\.?|extension|x)\s*[:.]?\s*\d{1,6}\b/gi, " ");
    const candidates = withoutExtension.match(/\+?\s*\(?\d[\d().\s-]{7,}\d/g) || [];

    let selectedDigits = "";
    let selectedHasPlus = false;
    let bestScore = -1;

    const tryCandidate = (candidate) => {
      const cleaned = normalizeText(candidate).replace(/[^\d+().\s-]/g, " ");
      const digits = cleaned.replace(/\D/g, "");
      if (!isLikelyPhoneDigits(digits)) return;

      const hasPlus = /^\s*\+/.test(cleaned);
      let score = digits.length;
      if (digits.length === 10 && !hasPlus) score += 30;
      if (digits.length === 11 && digits.startsWith("1")) score += 28;
      if (hasPlus) score += 6;
      if (/[()]/.test(cleaned)) score += 2;

      if (score > bestScore) {
        bestScore = score;
        selectedDigits = digits;
        selectedHasPlus = hasPlus;
      }
    };

    if (candidates.length > 0) {
      for (const candidate of candidates) {
        tryCandidate(candidate);
      }
    } else {
      tryCandidate(withoutExtension);
    }

    if (!selectedDigits) return "";

    const naFormatted = formatNorthAmericaPhone(selectedDigits);
    if (naFormatted) {
      if (selectedDigits.length === 10 && selectedHasPlus) {
        return `+${selectedDigits}`;
      }
      return naFormatted;
    }

    if (selectedHasPlus || selectedDigits.length > 10) {
      return `+${selectedDigits}`;
    }

    return selectedDigits;
  }

  function parseRating(value) {
    if (value == null) return "";
    const clean = normalizeText(value).replace(/,/g, ".");
    if (!clean) return "";

    const ratingPatterns = [
      /\b([0-5](?:\.\d)?)\s*(?:stars?|★|⭐)\b/i,
      /\brated\s*([0-5](?:\.\d)?)/i,
      /\b([0-5](?:\.\d)?)\s*out of\s*5\b/i,
      /\b([0-5](?:\.\d)?)\s*\/\s*5\b/i,
      /\b([0-5](?:\.\d)?)\s*[·•]\s*\d[\d,.\s]*[kmb]?\b/i,
      /\b([0-5](?:\.\d)?)\s*\(\s*\d[\d,.\s]*[kmb]?\s*\)/i,
      /\b([0-5](?:\.\d)?)\s+\d[\d,.\s]*[kmb]?\s+reviews?\b/i
    ];

    for (const pattern of ratingPatterns) {
      const match = clean.match(pattern);
      if (!match || !match[1]) continue;
      const parsed = Number(match[1]);
      if (Number.isFinite(parsed) && parsed > 0 && parsed <= 5) {
        return parsed;
      }
    }

    const strictStandalonePattern = clean.match(/^\s*([0-5](?:\.\d)?)\s*$/);
    if (strictStandalonePattern && strictStandalonePattern[1]) {
      const parsed = Number(strictStandalonePattern[1]);
      if (Number.isFinite(parsed) && parsed > 0 && parsed <= 5) {
        return parsed;
      }
    }

    return "";
  }

  function parseReviewCount(value) {
    if (value == null) return "";
    const text = normalizeText(value);
    if (!text) return "";

    const reviewWord = text.match(/(\d[\d,.'’\u00A0\u202F\s]*[kmb]?)\s+reviews?\b/i);
    if (reviewWord && reviewWord[1]) {
      const parsed = parseAbbreviatedCount(reviewWord[1]);
      if (Number.isFinite(parsed)) return parsed;
    }

    const bulletPattern = text.match(/\b([0-5](?:\.\d)?)\s*[·•]\s*(\d[\d,.'’\u00A0\u202F\s]*[kmb]?)\b/i);
    if (bulletPattern && bulletPattern[2]) {
      const parsed = parseAbbreviatedCount(bulletPattern[2]);
      if (Number.isFinite(parsed)) return parsed;
    }

    const compactPattern = text.match(/\b([0-5](?:\.\d)?)\s*\((\d[\d,.'’\u00A0\u202F\s]*[kmb]?)\)/i);
    if (compactPattern && compactPattern[2]) {
      const parsed = parseAbbreviatedCount(compactPattern[2]);
      if (Number.isFinite(parsed)) return parsed;
    }

    // Only allow standalone numeric parsing when the whole candidate is numeric-ish.
    const strictStandalonePattern = text.match(/^\(?\s*(\d[\d,.'’\u00A0\u202F\s]*[kmb]?)\s*\)?$/i);
    if (strictStandalonePattern && strictStandalonePattern[1]) {
      const rawStandalone = normalizeText(strictStandalonePattern[1]);
      const standaloneDigits = rawStandalone.replace(/\D/g, "");
      const standaloneHasSuffix = /[kmb]$/i.test(rawStandalone);
      if (!standaloneHasSuffix && standaloneDigits.length > 7) {
        return "";
      }
      const parsed = parseAbbreviatedCount(strictStandalonePattern[1]);
      if (Number.isFinite(parsed)) return parsed;
    }

    return "";
  }

  function parseAbbreviatedCount(value) {
    const raw = normalizeText(value).toLowerCase().replace(/\s+/g, "");
    if (!raw) return "";

    const suffixMatch = raw.match(/([kmb])$/i);
    const suffix = suffixMatch ? suffixMatch[1].toLowerCase() : "";
    const numeric = suffix ? raw.slice(0, -1) : raw;
    if (!numeric || !/^\d[\d.,'’]*$/.test(numeric)) return "";
    if (!suffix && /^\d+$/.test(numeric) && numeric.length > 7) return "";

    let normalized = "";
    const groupedThousandsPattern = /^\d{1,3}(?:[.,'’]\d{3})+$/;

    if (groupedThousandsPattern.test(numeric)) {
      normalized = numeric.replace(/[.,'’]/g, "");
    } else if (suffix && numeric.includes(".") && numeric.includes(",")) {
      const commaIndex = numeric.lastIndexOf(",");
      const dotIndex = numeric.lastIndexOf(".");
      const compact = numeric.replace(/['’]/g, "");
      normalized = commaIndex > dotIndex
        ? compact.replace(/\./g, "").replace(",", ".")
        : compact.replace(/,/g, "");
    } else if (suffix && /^\d+[.,]\d+$/.test(numeric)) {
      normalized = numeric.replace(/['’]/g, "").replace(",", ".");
    } else if (/^\d+$/.test(numeric)) {
      normalized = numeric;
    } else {
      return "";
    }

    if (!suffix && /^\d+$/.test(normalized) && normalized.length > 7) {
      return "";
    }

    const base = Number(normalized);
    if (!Number.isFinite(base)) return "";

    if (!suffix) {
      return Math.round(base);
    }

    const multiplier = suffix === "k" ? 1000 : suffix === "m" ? 1000000 : suffix === "b" ? 1000000000 : 1;
    const scaled = Math.round(base * multiplier);
    return Number.isFinite(scaled) ? scaled : "";
  }

  function parseFlexibleNumber(value) {
    if (value === "" || value == null) return "";
    if (typeof value === "number") return Number.isFinite(value) ? value : "";

    const raw = normalizeText(value).replace(/\s+/g, "");
    if (!raw) return "";

    let normalized = raw;
    const hasComma = normalized.includes(",");
    const hasDot = normalized.includes(".");

    if (hasComma && hasDot) {
      const decimalIsComma = normalized.lastIndexOf(",") > normalized.lastIndexOf(".");
      normalized = decimalIsComma
        ? normalized.replace(/\./g, "").replace(",", ".")
        : normalized.replace(/,/g, "");
    } else if (hasComma) {
      if (/^\d{1,3}(?:,\d{3})+$/.test(normalized)) {
        normalized = normalized.replace(/,/g, "");
      } else if (/^\d+,\d+$/.test(normalized)) {
        normalized = normalized.replace(",", ".");
      } else {
        normalized = normalized.replace(/,/g, "");
      }
    } else if (hasDot && /^\d{1,3}(?:\.\d{3})+$/.test(normalized)) {
      normalized = normalized.replace(/\./g, "");
    }

    const parsed = Number(normalized);
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
    const ratingValue = parseRating(row && row.rating);
    const rating = ratingValue === "" ? Number.NaN : Number(ratingValue);
    const reviewValue = parseReviewCount(row && row.review_count);
    const reviews = reviewValue === "" ? Number.NaN : Number(reviewValue);

    if (f.minRating !== "" && f.minRating != null) {
      const minRating = parseFlexibleNumber(f.minRating);
      if (minRating === "") return false;
      if (!Number.isFinite(rating) || rating < minRating) return false;
    }

    if (f.maxRating !== "" && f.maxRating != null) {
      const maxRating = parseFlexibleNumber(f.maxRating);
      if (maxRating === "") return false;
      if (!Number.isFinite(rating) || rating > maxRating) return false;
    }

    if (f.minReviews !== "" && f.minReviews != null) {
      const minReviews = parseFlexibleNumber(f.minReviews);
      if (minReviews === "") return false;
      if (!Number.isFinite(reviews) || reviews < minReviews) return false;
    }

    if (f.maxReviews !== "" && f.maxReviews != null) {
      const maxReviews = parseFlexibleNumber(f.maxReviews);
      if (maxReviews === "") return false;
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
    return parseFlexibleNumber(value);
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
    normalizePhoneText,
    parseFlexibleNumber,
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
