importScripts("shared.js");

const { MSG, rowsToCsv, normalizeText, normalizeWebsiteUrl } = self.GbpShared;

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (!message || !message.type) {
    return false;
  }

  if (message.type === MSG.EXPORT_CSV) {
    handleExportCsv(message, sendResponse);
    return true;
  }

  if (message.type === MSG.ENRICH_ROWS) {
    handleEnrichRows(message, sendResponse);
    return true;
  }

  return false;
});

function handleExportCsv(message, sendResponse) {
  try {
    const rows = Array.isArray(message.rows) ? message.rows : [];
    const csv = rowsToCsv(rows, message.columns);
    const url = `data:text/csv;charset=utf-8,${encodeURIComponent(csv)}`;
    const filename = message.filename || defaultFilename();

    chrome.downloads.download(
      {
        url,
        filename,
        saveAs: true
      },
      (downloadId) => {
        if (chrome.runtime.lastError) {
          sendResponse({
            type: MSG.EXPORT_ERROR,
            error: chrome.runtime.lastError.message || "Failed to download CSV"
          });
          return;
        }

        sendResponse({
          type: MSG.EXPORT_DONE,
          downloadId
        });
      }
    );
  } catch (error) {
    sendResponse({
      type: MSG.EXPORT_ERROR,
      error: error && error.message ? error.message : "CSV export failed"
    });
  }
}

async function handleEnrichRows(message, sendResponse) {
  try {
    const rows = Array.isArray(message.rows) ? message.rows : [];
    const options = message.options || {};
    const result = await enrichRows(rows, options);

    sendResponse({
      type: MSG.ENRICH_DONE,
      rows: result.rows,
      summary: result.summary
    });
  } catch (error) {
    sendResponse({
      type: MSG.ENRICH_ERROR,
      error: error && error.message ? error.message : "Website enrichment failed"
    });
  }
}

async function enrichRows(rows, options) {
  const maxPagesPerSite = clampInt(options.maxPagesPerSite, 1, 5, 3);
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);

  const summary = {
    total: rows.length,
    processed: 0,
    enriched: 0,
    skipped: 0,
    blocked: 0,
    errors: 0
  };

  const outputRows = [];

  for (const row of rows) {
    const sourceRow = row || {};
    const enrichedRow = {
      ...sourceRow,
      owner_name: normalizeText(sourceRow.owner_name),
      owner_title: normalizeText(sourceRow.owner_title),
      owner_email: normalizeText(sourceRow.owner_email),
      contact_email: normalizeText(sourceRow.contact_email),
      website_scan_status: normalizeText(sourceRow.website_scan_status)
    };

    const website = normalizeWebsiteUrl(sourceRow.website);

    if (!website) {
      enrichedRow.website_scan_status = "no_website";
      summary.skipped += 1;
      summary.processed += 1;
      outputRows.push(enrichedRow);
      emitEnrichProgress(summary, sourceRow.name);
      continue;
    }

    try {
      const scan = await scanWebsite(website, {
        maxPagesPerSite,
        timeoutMs
      });

      if (scan.ownerName) enrichedRow.owner_name = scan.ownerName;
      if (scan.ownerTitle) enrichedRow.owner_title = scan.ownerTitle;
      if (scan.ownerEmail) enrichedRow.owner_email = scan.ownerEmail;
      if (scan.contactEmail) enrichedRow.contact_email = scan.contactEmail;
      enrichedRow.website_scan_status = scan.status;

      if (scan.blocked) {
        summary.blocked += 1;
      }

      if (scan.status === "enriched") {
        summary.enriched += 1;
      } else {
        summary.skipped += 1;
      }
    } catch (_rowError) {
      enrichedRow.website_scan_status = "scan_error";
      summary.errors += 1;
      summary.skipped += 1;
    }

    summary.processed += 1;
    outputRows.push(enrichedRow);
    emitEnrichProgress(summary, sourceRow.name);
  }

  return {
    rows: outputRows,
    summary
  };
}

function emitEnrichProgress(summary, currentName) {
  chrome.runtime.sendMessage({
    type: MSG.ENRICH_PROGRESS,
    total: summary.total,
    processed: summary.processed,
    enriched: summary.enriched,
    skipped: summary.skipped,
    blocked: summary.blocked,
    errors: summary.errors,
    current: normalizeText(currentName)
  });
}

async function scanWebsite(startUrl, options) {
  let tab = null;
  const emails = new Set();
  const ownerCandidates = [];
  let blocked = false;

  const queue = [startUrl];
  const visited = new Set();
  const queued = new Set([stripHash(startUrl)]);
  const baseOrigin = new URL(startUrl).origin;

  try {
    tab = await createHiddenTab(startUrl);

    while (queue.length > 0 && visited.size < options.maxPagesPerSite) {
      const nextUrl = queue.shift();
      const nextKey = stripHash(nextUrl);
      if (visited.has(nextKey)) continue;

      visited.add(nextKey);

      await updateTabUrl(tab.id, nextUrl);
      await waitForTabComplete(tab.id, options.timeoutMs);
      await sleep(800);

      const pageData = await executeExtraction(tab.id);
      if (!pageData) {
        continue;
      }

      if (pageData.blocked === true) {
        blocked = true;
      }

      for (const email of pageData.emails || []) {
        const normalized = normalizeEmail(email);
        if (normalized) emails.add(normalized);
      }

      for (const candidate of pageData.ownerCandidates || []) {
        if (!candidate || !candidate.name) continue;
        ownerCandidates.push({
          name: normalizeText(candidate.name),
          title: normalizeText(candidate.title),
          score: Number(candidate.score) || 0
        });
      }

      for (const link of pageData.relatedLinks || []) {
        const normalizedLink = normalizeWebsiteUrl(link);
        if (!normalizedLink) continue;

        try {
          const parsed = new URL(normalizedLink);
          if (parsed.origin !== baseOrigin) continue;
        } catch (_e) {
          continue;
        }

        const linkKey = stripHash(normalizedLink);
        if (visited.has(linkKey) || queued.has(linkKey)) continue;

        queued.add(linkKey);
        queue.push(normalizedLink);
      }
    }
  } finally {
    if (tab && tab.id != null) {
      await closeTab(tab.id).catch(() => {});
    }
  }

  const bestOwner = pickBestOwner(ownerCandidates);
  const emailList = Array.from(emails);
  const ownerEmail = chooseOwnerEmail(emailList, bestOwner ? bestOwner.name : "");
  const contactEmail = chooseContactEmail(emailList, ownerEmail);

  let status = "no_public_data";
  if (bestOwner || ownerEmail || contactEmail) {
    status = "enriched";
  } else if (blocked) {
    status = "blocked";
  }

  return {
    ownerName: bestOwner ? bestOwner.name : "",
    ownerTitle: bestOwner ? bestOwner.title : "",
    ownerEmail,
    contactEmail,
    status,
    blocked
  };
}

function pickBestOwner(candidates) {
  if (!Array.isArray(candidates) || candidates.length === 0) return null;

  let best = null;
  for (const candidate of candidates) {
    const name = normalizeText(candidate.name);
    if (!isLikelyPersonName(name)) continue;

    const title = normalizeText(candidate.title);
    const score = Number(candidate.score) || 0;
    const weightedScore = score + (title ? 1 : 0) + Math.min(2, name.split(/\s+/).length - 1);

    if (!best || weightedScore > best.weightedScore) {
      best = { name, title, weightedScore };
    }
  }

  if (!best) return null;
  return { name: best.name, title: best.title };
}

function chooseOwnerEmail(emails, ownerName) {
  const list = sanitizeEmailList(emails);
  if (list.length === 0) return "";

  const tokens = normalizeText(ownerName)
    .toLowerCase()
    .split(/\s+/)
    .filter((token) => token.length >= 3);

  for (const email of list) {
    const local = email.split("@")[0];
    if (tokens.some((token) => local.includes(token))) {
      return email;
    }
  }

  for (const email of list) {
    const local = email.split("@")[0];
    if (/(owner|founder|ceo|president|principal|director)/i.test(local)) {
      return email;
    }
  }

  return "";
}

function chooseContactEmail(emails, ownerEmail) {
  const list = sanitizeEmailList(emails).filter((email) => email !== ownerEmail);
  if (list.length === 0) return "";

  const priorityPatterns = [
    /^info@/i,
    /^contact@/i,
    /^hello@/i,
    /^office@/i,
    /^support@/i,
    /^admin@/i,
    /^sales@/i
  ];

  for (const pattern of priorityPatterns) {
    const hit = list.find((email) => pattern.test(email));
    if (hit) return hit;
  }

  return list[0] || "";
}

function sanitizeEmailList(emails) {
  if (!Array.isArray(emails)) return [];

  const out = [];
  const seen = new Set();

  for (const email of emails) {
    const normalized = normalizeEmail(email);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }

  out.sort((a, b) => a.length - b.length);
  return out;
}

function normalizeEmail(email) {
  const value = normalizeText(email).toLowerCase();
  if (!value.includes("@")) return "";
  if (value.length < 6 || value.length > 120) return "";
  if (/\.(png|jpg|jpeg|svg|gif|webp|js|css)$/i.test(value)) return "";
  if (/^(example|test)@/i.test(value)) return "";
  if (/(noreply|do-not-reply|donotreply)/i.test(value)) return "";
  return value;
}

function isLikelyPersonName(name) {
  const value = normalizeText(name);
  if (!value) return false;
  if (/\d/.test(value)) return false;

  const words = value.split(/\s+/);
  if (words.length < 2 || words.length > 4) return false;

  const blocked = [
    "contact us",
    "about us",
    "our team",
    "learn more",
    "privacy policy",
    "terms of service"
  ];
  const lower = value.toLowerCase();
  if (blocked.some((entry) => lower.includes(entry))) return false;

  return words.every((word) => /^[A-Za-z][A-Za-z'\-\.]+$/.test(word));
}

function stripHash(url) {
  const raw = normalizeWebsiteUrl(url);
  if (!raw) return "";

  try {
    const parsed = new URL(raw);
    parsed.hash = "";
    return parsed.toString();
  } catch (_e) {
    return raw;
  }
}

function createHiddenTab(url) {
  return new Promise((resolve, reject) => {
    chrome.tabs.create({ url, active: false }, (tab) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message || "Failed to open website tab"));
        return;
      }
      resolve(tab);
    });
  });
}

function updateTabUrl(tabId, url) {
  return new Promise((resolve, reject) => {
    chrome.tabs.update(tabId, { url }, (tab) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message || "Failed to navigate tab"));
        return;
      }
      resolve(tab);
    });
  });
}

function closeTab(tabId) {
  return new Promise((resolve, reject) => {
    chrome.tabs.remove(tabId, () => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message || "Failed to close tab"));
        return;
      }
      resolve();
    });
  });
}

function waitForTabComplete(tabId, timeoutMs) {
  return new Promise((resolve, reject) => {
    let settled = false;

    const timeoutHandle = setTimeout(() => {
      finish(() => reject(new Error("Timed out while loading website")));
    }, timeoutMs);

    const onUpdated = (updatedTabId, changeInfo, tab) => {
      if (updatedTabId !== tabId) return;
      if (changeInfo.status === "complete") {
        finish(() => resolve(tab));
      }
    };

    const onRemoved = (removedTabId) => {
      if (removedTabId !== tabId) return;
      finish(() => reject(new Error("Website tab closed unexpectedly")));
    };

    const finish = (callback) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeoutHandle);
      chrome.tabs.onUpdated.removeListener(onUpdated);
      chrome.tabs.onRemoved.removeListener(onRemoved);
      callback();
    };

    chrome.tabs.onUpdated.addListener(onUpdated);
    chrome.tabs.onRemoved.addListener(onRemoved);

    chrome.tabs.get(tabId, (tab) => {
      if (settled) return;
      if (chrome.runtime.lastError) return;
      if (tab && tab.status === "complete") {
        finish(() => resolve(tab));
      }
    });
  });
}

function executeExtraction(tabId) {
  return new Promise((resolve, reject) => {
    chrome.scripting.executeScript(
      {
        target: { tabId },
        func: extractPageDataScript
      },
      (results) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message || "Failed to scan website page"));
          return;
        }

        if (!Array.isArray(results) || !results[0]) {
          resolve(null);
          return;
        }

        resolve(results[0].result || null);
      }
    );
  });
}

function clampInt(value, min, max, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(num)));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function defaultFilename() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  const stamp = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
  return `gbp_export_${stamp}.csv`;
}

function extractPageDataScript() {
  const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim();

  const isLikelyEmail = (value) => {
    const email = normalize(value).toLowerCase();
    if (!email.includes("@")) return false;
    if (email.length < 6 || email.length > 120) return false;
    if (/\.(png|jpg|jpeg|svg|gif|webp|js|css)$/i.test(email)) return false;
    if (/^(example|test)@/i.test(email)) return false;
    return true;
  };

  const parseOwnerCandidate = (text) => {
    const normalized = normalize(text);
    if (!normalized) return null;

    const titlePattern = "(owner|founder|co-founder|president|ceo|chief executive(?: officer)?|managing director|principal|partner|director|manager)";
    const namePattern = "([A-Z][A-Za-z'\\-.]+(?:\\s+[A-Z][A-Za-z'\\-.]+){1,3})";

    const patternOne = new RegExp(`${titlePattern}\\s*[:\\-\\|,]?\\s*${namePattern}`, "i");
    const patternTwo = new RegExp(`${namePattern}\\s*(?:,|\\-|\\|)\\s*${titlePattern}`, "i");

    const one = normalized.match(patternOne);
    if (one) {
      return {
        name: normalize(one[2]),
        title: normalize(one[1])
      };
    }

    const two = normalized.match(patternTwo);
    if (two) {
      return {
        name: normalize(two[1]),
        title: normalize(two[2])
      };
    }

    return null;
  };

  const bodyText = normalize(document.body ? document.body.innerText || "" : "");
  const emails = new Set();

  const regexMatches = bodyText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
  for (const value of regexMatches) {
    const email = normalize(value).toLowerCase();
    if (isLikelyEmail(email)) emails.add(email);
  }

  const anchors = Array.from(document.querySelectorAll("a[href]"));
  const relatedLinkSet = new Set();
  const relatedKeywords = /(about|contact|team|leadership|management|staff|company|our-story|who-we-are|founder|owner)/i;

  for (const anchor of anchors) {
    const href = anchor.getAttribute("href") || "";
    const text = normalize(anchor.textContent || "");

    if (href.toLowerCase().startsWith("mailto:")) {
      const extracted = href.replace(/^mailto:/i, "").split("?")[0].trim().toLowerCase();
      if (isLikelyEmail(extracted)) emails.add(extracted);
      continue;
    }

    let absolute = "";
    try {
      absolute = new URL(href, window.location.href).toString();
    } catch (_e) {
      absolute = "";
    }

    if (!absolute) continue;

    const probe = `${text} ${absolute}`;
    if (!relatedKeywords.test(probe)) continue;

    relatedLinkSet.add(absolute);
    if (relatedLinkSet.size >= 10) break;
  }

  const ownerCandidates = [];
  const ownerKeyword = /(owner|founder|co-founder|president|ceo|chief executive|managing director|principal|partner|director|manager)/i;
  const nodes = Array.from(document.querySelectorAll("h1,h2,h3,h4,h5,p,li,strong,b,span,div")).slice(0, 2500);

  for (const node of nodes) {
    const text = normalize(node.textContent || "");
    if (!text || text.length < 6 || text.length > 220) continue;
    if (!ownerKeyword.test(text)) continue;

    const parsed = parseOwnerCandidate(text);
    if (!parsed || !parsed.name) continue;

    let score = 1;
    const tagName = (node.tagName || "").toLowerCase();
    if (/^h[1-5]$/.test(tagName)) score += 2;
    if (tagName === "strong" || tagName === "b") score += 1;

    ownerCandidates.push({
      name: parsed.name,
      title: parsed.title,
      score
    });

    if (ownerCandidates.length >= 30) break;
  }

  const antiBotSignal = `${document.title} ${bodyText.slice(0, 4000)}`.toLowerCase();
  const blocked = /(access denied|forbidden|verify you are human|captcha|attention required|cloudflare|blocked)/i.test(antiBotSignal);

  return {
    emails: Array.from(emails).slice(0, 50),
    ownerCandidates,
    relatedLinks: Array.from(relatedLinkSet),
    blocked
  };
}
