importScripts("shared.js");

const { MSG, rowsToCsv, normalizeText, normalizeWebsiteUrl } = self.GbpShared;
const ENRICH_SESSION_KEY = "enrichSession";
let lastEnrichPersistAtMs = 0;

function storageSet(items) {
  return new Promise((resolve, reject) => {
    chrome.storage.local.set(items, () => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message || "Storage write failed"));
        return;
      }
      resolve();
    });
  });
}

function saveEnrichSession(session, forceRows, rows) {
  const snapshot = {
    ...(session || {}),
    updated_at: new Date().toISOString()
  };
  const payload = {
    [ENRICH_SESSION_KEY]: snapshot
  };

  if (forceRows === true && Array.isArray(rows)) {
    payload.lastRows = rows;
  }

  return storageSet(payload).catch(() => {});
}

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
  const startedAt = new Date().toISOString();
  const runId = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const rows = Array.isArray(message.rows) ? message.rows : [];
  lastEnrichPersistAtMs = 0;
  await saveEnrichSession(
    {
      run_id: runId,
      status: "running",
      started_at: startedAt,
      total: rows.length,
      processed: 0,
      enriched: 0,
      skipped: 0,
      blocked: 0,
      errors: 0,
      social_scanned: 0,
      pages_visited: 0,
      pages_discovered: 0,
      personal_email_found: 0,
      company_email_found: 0,
      current: "",
      current_url: "",
      phase: "init",
      lead_signal_text: "",
      lead_signal_tone: "info"
    },
    false
  );

  try {
    const options = message.options || {};
    const result = await enrichRows(rows, options);

    await saveEnrichSession(
      {
        run_id: runId,
        status: "done",
        started_at: startedAt,
        completed_at: new Date().toISOString(),
        ...result.summary,
        phase: "done"
      },
      true,
      result.rows
    );

    sendResponse({
      type: MSG.ENRICH_DONE,
      rows: result.rows,
      summary: result.summary
    });
  } catch (error) {
    await saveEnrichSession(
      {
        run_id: runId,
        status: "error",
        started_at: startedAt,
        completed_at: new Date().toISOString(),
        error: error && error.message ? error.message : "Website enrichment failed",
        phase: "error"
      },
      false
    );
    sendResponse({
      type: MSG.ENRICH_ERROR,
      error: error && error.message ? error.message : "Website enrichment failed"
    });
  }
}

async function enrichRows(rows, options) {
  const maxPagesPerSite = clampInt(options.maxPagesPerSite, 1, 120, 40);
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;
  const scanSocialLinks = options.scanSocialLinks !== false;
  const maxSocialPages = clampInt(options.maxSocialPages, 0, 8, 4);
  const maxDiscoveredPages = clampInt(options.maxDiscoveredPages, maxPagesPerSite, 800, Math.max(200, maxPagesPerSite * 8));

  const summary = {
    total: rows.length,
    processed: 0,
    enriched: 0,
    skipped: 0,
    blocked: 0,
    errors: 0,
    social_scanned: 0,
    pages_visited: 0,
    pages_discovered: 0,
    personal_email_found: 0,
    company_email_found: 0
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
      primary_email: normalizeText(sourceRow.primary_email),
      primary_email_type: normalizeText(sourceRow.primary_email_type),
      primary_email_source: normalizeText(sourceRow.primary_email_source),
      website_scan_status: normalizeText(sourceRow.website_scan_status),
      site_pages_visited: Number(sourceRow.site_pages_visited || 0),
      site_pages_discovered: Number(sourceRow.site_pages_discovered || 0),
      social_pages_scanned: Number(sourceRow.social_pages_scanned || 0),
      social_links: normalizeText(sourceRow.social_links)
    };

    const website = normalizeWebsiteUrl(sourceRow.website);

    if (!website) {
      enrichedRow.website_scan_status = "no_website";
      enrichedRow.primary_email = "";
      enrichedRow.primary_email_type = "";
      enrichedRow.primary_email_source = "";
      enrichedRow.site_pages_visited = 0;
      enrichedRow.site_pages_discovered = 0;
      enrichedRow.social_pages_scanned = 0;
      enrichedRow.social_links = "";
      summary.skipped += 1;
      summary.processed += 1;
      outputRows.push(enrichedRow);
      emitEnrichProgress(summary, {
        currentName: sourceRow.name,
        currentUrl: website,
        phase: "skip",
        leadSignalText: "Skipped: no website",
        leadSignalTone: "warn",
        sitePagesVisited: summary.pages_visited,
        sitePagesDiscovered: summary.pages_discovered
      });
      continue;
    }

    try {
      emitEnrichProgress(summary, {
        currentName: sourceRow.name,
        currentUrl: website,
        phase: "site_init",
        sitePagesVisited: summary.pages_visited,
        sitePagesDiscovered: summary.pages_discovered
      });

      const scan = await scanWebsite(website, {
        maxPagesPerSite,
        maxDiscoveredPages,
        timeoutMs,
        visibleTabs,
        scanSocialLinks,
        maxSocialPages,
        onProgress: (scanProgress) => {
          const progress = scanProgress || {};
          emitEnrichProgress(summary, {
            currentName: sourceRow.name,
            currentUrl: progress.currentUrl || website,
            phase: progress.phase || "site_scan",
            sitePagesVisited: summary.pages_visited + Number(progress.pagesVisited || 0),
            sitePagesDiscovered: summary.pages_discovered + Number(progress.pagesDiscovered || 0),
            socialScanned: summary.social_scanned + Number(progress.socialScanned || 0)
          });
        }
      });

      if (scan.ownerName) enrichedRow.owner_name = scan.ownerName;
      if (scan.ownerTitle) enrichedRow.owner_title = scan.ownerTitle;
      if (scan.ownerEmail) enrichedRow.owner_email = scan.ownerEmail;
      if (scan.contactEmail) enrichedRow.contact_email = scan.contactEmail;
      enrichedRow.primary_email = normalizeText(scan.primaryEmail);
      enrichedRow.primary_email_type = normalizeText(scan.primaryEmailType);
      enrichedRow.primary_email_source = normalizeText(scan.primaryEmailSource);
      enrichedRow.website_scan_status = scan.status;
      enrichedRow.site_pages_visited = Number(scan.pagesVisited || 0);
      enrichedRow.site_pages_discovered = Number(scan.pagesDiscovered || 0);
      enrichedRow.social_pages_scanned = Number(scan.socialScanned || 0);
      enrichedRow.social_links = Array.isArray(scan.socialLinks) ? scan.socialLinks.join(" | ") : "";

      if (scan.blocked) {
        summary.blocked += 1;
      }
      if (scan.primaryEmailType === "personal") {
        summary.personal_email_found += 1;
      } else if (scan.primaryEmailType === "company") {
        summary.company_email_found += 1;
      }
      summary.social_scanned += Number(scan.socialScanned || 0);
      summary.pages_visited += Number(scan.pagesVisited || 0);
      summary.pages_discovered += Number(scan.pagesDiscovered || 0);

      if (scan.status === "enriched") {
        summary.enriched += 1;
      } else {
        summary.skipped += 1;
      }
    } catch (_rowError) {
      enrichedRow.website_scan_status = "scan_error";
      enrichedRow.primary_email = "";
      enrichedRow.primary_email_type = "";
      enrichedRow.primary_email_source = "";
      enrichedRow.site_pages_visited = 0;
      enrichedRow.site_pages_discovered = 0;
      enrichedRow.social_pages_scanned = 0;
      enrichedRow.social_links = "";
      summary.errors += 1;
      summary.skipped += 1;
    }

    summary.processed += 1;
    outputRows.push(enrichedRow);
    const leadSignal = buildLeadSignal(enrichedRow);
    emitEnrichProgress(summary, {
      currentName: sourceRow.name,
      currentUrl: website,
      phase: "done",
      leadSignalText: leadSignal.text,
      leadSignalTone: leadSignal.tone,
      sitePagesVisited: summary.pages_visited,
      sitePagesDiscovered: summary.pages_discovered
    });
  }

  return {
    rows: outputRows,
    summary
  };
}

function emitEnrichProgress(summary, context) {
  const ctx = context || {};
  const payload = {
    type: MSG.ENRICH_PROGRESS,
    total: summary.total,
    processed: summary.processed,
    enriched: summary.enriched,
    skipped: summary.skipped,
    blocked: summary.blocked,
    errors: summary.errors,
    social_scanned: Number(ctx.socialScanned != null ? ctx.socialScanned : summary.social_scanned),
    site_pages_visited: Number(ctx.sitePagesVisited != null ? ctx.sitePagesVisited : summary.pages_visited),
    site_pages_discovered: Number(ctx.sitePagesDiscovered != null ? ctx.sitePagesDiscovered : summary.pages_discovered),
    personal_email_found: Number(summary.personal_email_found || 0),
    company_email_found: Number(summary.company_email_found || 0),
    current: normalizeText(ctx.currentName),
    current_url: normalizeText(ctx.currentUrl),
    phase: normalizeText(ctx.phase),
    lead_signal_text: normalizeText(ctx.leadSignalText),
    lead_signal_tone: normalizeText(ctx.leadSignalTone)
  };

  chrome.runtime.sendMessage(payload);
  const now = Date.now();
  const forcePersist = /^(done|skip|error)$/.test(normalizeText(ctx.phase).toLowerCase());
  if (forcePersist || now - lastEnrichPersistAtMs >= 350) {
    lastEnrichPersistAtMs = now;
    saveEnrichSession(
      {
        status: "running",
        ...payload
      },
      false
    );
  }
}

function buildLeadSignal(row) {
  const primaryEmail = normalizeText(row && row.primary_email);
  const emailType = normalizeText(row && row.primary_email_type).toLowerCase();
  const emailSource = normalizeText(row && row.primary_email_source);
  const scanStatus = normalizeText(row && row.website_scan_status).toLowerCase();
  const ownerName = normalizeText(row && row.owner_name);

  if (primaryEmail) {
    if (emailType === "personal") {
      return {
        text: `Saved personal email (${sourceLabel(emailSource)})`,
        tone: "success"
      };
    }
    return {
      text: `Saved company email fallback (${sourceLabel(emailSource)})`,
      tone: "info"
    };
  }

  if (scanStatus === "blocked") {
    return { text: "Skipped: blocked by site protections", tone: "warn" };
  }
  if (scanStatus === "scan_error") {
    return { text: "Skipped: scan error", tone: "warn" };
  }
  if (scanStatus === "no_website") {
    return { text: "Skipped: no website", tone: "warn" };
  }

  if (scanStatus === "enriched" && ownerName) {
    return { text: "Saved owner details (no public email)", tone: "info" };
  }

  return { text: "Skipped: no public email found", tone: "warn" };
}

function sourceLabel(source) {
  const value = normalizeText(source);
  return value || "website";
}

async function scanWebsite(startUrl, options) {
  let tab = null;
  const emails = new Set();
  const emailSourceByAddress = new Map();
  const ownerCandidates = [];
  const socialCandidates = new Set();
  const socialDiscovered = new Set();
  let blocked = false;
  let socialScanned = 0;
  let pagesVisited = 0;

  const baseOrigin = new URL(startUrl).origin;
  const firstUrl = canonicalizeCrawlUrl(startUrl, baseOrigin) || stripHash(startUrl) || startUrl;
  const firstKey = stripHash(firstUrl) || firstUrl;
  const queue = [firstUrl];
  const visited = new Set();
  const queued = new Set([firstKey]);
  const discovered = new Set([firstKey]);

  const reportProgress = (phase, currentUrl) => {
    if (typeof options.onProgress !== "function") return;
    options.onProgress({
      phase,
      currentUrl: currentUrl || "",
      pagesVisited,
      pagesDiscovered: discovered.size,
      socialScanned
    });
  };

  const registerEmail = (email, sourceUrl) => {
    const normalized = normalizeEmail(email);
    if (!normalized) return;
    emails.add(normalized);

    if (!emailSourceByAddress.has(normalized)) {
      emailSourceByAddress.set(normalized, classifyEmailSource(sourceUrl));
    }
  };

  const registerSocialCandidate = (socialUrl) => {
    const normalizedSocial = normalizeWebsiteUrl(socialUrl);
    if (!normalizedSocial) return;
    socialCandidates.add(normalizedSocial);
    socialDiscovered.add(normalizedSocial);
  };

  try {
    tab = await createScanTab(firstUrl, options.visibleTabs === true);
    reportProgress("site_open", firstUrl);

    while (queue.length > 0 && visited.size < options.maxPagesPerSite) {
      const nextUrl = queue.shift();
      const nextKey = stripHash(nextUrl);
      if (!nextKey || visited.has(nextKey)) continue;

      visited.add(nextKey);
      pagesVisited = visited.size;
      reportProgress("site_page", nextUrl);

      let pageData = null;
      try {
        await updateTabUrl(tab.id, nextUrl);
        await waitForTabComplete(tab.id, options.timeoutMs);
        await sleep(800);
        pageData = await executeExtraction(tab.id);
      } catch (_sitePageError) {
        reportProgress("site_page_error", nextUrl);
        continue;
      }

      if (!pageData) {
        continue;
      }

      if (pageData.blocked === true) {
        blocked = true;
      }

      for (const email of pageData.emails || []) {
        registerEmail(email, nextUrl);
      }

      for (const candidate of pageData.ownerCandidates || []) {
        if (!candidate || !candidate.name) continue;
        ownerCandidates.push({
          name: normalizeText(candidate.name),
          title: normalizeText(candidate.title),
          score: Number(candidate.score) || 0
        });
      }

      const prioritizedLinks = prioritizeCrawlLinks([
        ...(Array.isArray(pageData.relatedLinks) ? pageData.relatedLinks : []),
        ...(Array.isArray(pageData.internalLinks) ? pageData.internalLinks : [])
      ]);

      for (const link of prioritizedLinks) {
        const normalizedLink = canonicalizeCrawlUrl(link, baseOrigin);
        if (!normalizedLink) continue;

        const linkKey = stripHash(normalizedLink);
        if (!linkKey) continue;
        if (visited.has(linkKey) || queued.has(linkKey)) continue;
        if (discovered.size >= options.maxDiscoveredPages) continue;

        discovered.add(linkKey);
        queued.add(linkKey);
        queue.push(normalizedLink);
      }

      for (const social of pageData.socialLinks || []) {
        registerSocialCandidate(social);
      }
      reportProgress("site_page_done", nextUrl);
    }

    if (options.scanSocialLinks === true && emails.size === 0 && socialCandidates.size > 0) {
      const socialQueue = prioritizeSocialLinks(Array.from(socialCandidates))
        .filter(shouldScanSocialUrl)
        .slice(0, options.maxSocialPages || 0);

      for (const socialUrl of socialQueue) {
        reportProgress("social_page", socialUrl);
        let socialData = null;
        try {
          await updateTabUrl(tab.id, socialUrl);
          await waitForTabComplete(tab.id, options.timeoutMs);
          await sleep(700);
          socialData = await executeExtraction(tab.id);
        } catch (_socialPageError) {
          socialScanned += 1;
          reportProgress("social_error", socialUrl);
          continue;
        }

        socialScanned += 1;
        reportProgress("social_done", socialUrl);
        if (!socialData) continue;

        if (socialData.blocked === true) {
          blocked = true;
        }

        for (const email of socialData.emails || []) {
          registerEmail(email, socialUrl);
        }

        for (const candidate of socialData.ownerCandidates || []) {
          if (!candidate || !candidate.name) continue;
          ownerCandidates.push({
            name: normalizeText(candidate.name),
            title: normalizeText(candidate.title),
            score: Number(candidate.score) || 0
          });
        }

        if (emails.size > 0) break;
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
  const primaryEmail = ownerEmail || contactEmail || "";
  const primaryEmailType = ownerEmail ? "personal" : contactEmail ? "company" : "";
  const primaryEmailSource = primaryEmail ? sourceForEmail(primaryEmail, emailSourceByAddress) : "";

  let status = "no_public_data";
  if (bestOwner || primaryEmail) {
    status = "enriched";
  } else if (blocked) {
    status = "blocked";
  }

  return {
    ownerName: bestOwner ? bestOwner.name : "",
    ownerTitle: bestOwner ? bestOwner.title : "",
    ownerEmail,
    contactEmail,
    primaryEmail,
    primaryEmailType,
    primaryEmailSource,
    status,
    blocked,
    socialScanned,
    pagesVisited,
    pagesDiscovered: discovered.size,
    socialLinks: Array.from(socialDiscovered).slice(0, 20)
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

  for (const email of list) {
    const local = email.split("@")[0];
    if (!isGenericMailboxLocalPart(local)) {
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

function isGenericMailboxLocalPart(localPart) {
  const local = normalizeText(localPart).toLowerCase();
  if (!local) return false;
  return /^(info|contact|hello|office|support|admin|sales|team|careers|jobs|hr|service|help|enquiries|inquiries)([._-]?[a-z0-9]*)?$/.test(local);
}

function sourceForEmail(email, sourceMap) {
  if (!email || !(sourceMap instanceof Map)) return "";
  const source = sourceMap.get(email);
  return normalizeText(source);
}

function prioritizeSocialLinks(links) {
  if (!Array.isArray(links) || links.length === 0) return [];
  const unique = new Map();
  for (const rawLink of links) {
    const normalized = normalizeWebsiteUrl(rawLink);
    if (!normalized || unique.has(normalized)) continue;
    unique.set(normalized, socialPriorityScore(normalized));
  }

  return Array.from(unique.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([link]) => link);
}

function socialPriorityScore(url) {
  const host = hostnameForUrl(url);
  if (!host) return 0;
  if (host.includes("facebook.com")) return 8;
  if (host.includes("instagram.com")) return 3;
  if (host.includes("linkedin.com")) return 2;
  if (host.includes("x.com") || host.includes("twitter.com")) return -3;
  if (host.includes("youtube.com")) return -4;
  return 1;
}

function shouldScanSocialUrl(url) {
  const host = hostnameForUrl(url);
  if (!host) return false;
  return host.includes("facebook.com");
}

function classifyEmailSource(url) {
  const host = hostnameForUrl(url);
  if (!host) return "website";
  if (host.includes("facebook.com")) return "facebook";
  if (host.includes("instagram.com")) return "instagram";
  if (host.includes("linkedin.com")) return "linkedin";
  if (host.includes("x.com") || host.includes("twitter.com")) return "x";
  if (host.includes("youtube.com")) return "youtube";
  return "website";
}

function hostnameForUrl(url) {
  const normalized = normalizeWebsiteUrl(url);
  if (!normalized) return "";
  try {
    const parsed = new URL(normalized);
    return normalizeText(parsed.hostname).toLowerCase();
  } catch (_e) {
    return "";
  }
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

function prioritizeCrawlLinks(links) {
  if (!Array.isArray(links) || links.length === 0) return [];

  const unique = new Map();
  for (const rawLink of links) {
    const link = normalizeWebsiteUrl(rawLink);
    if (!link || unique.has(link)) continue;
    unique.set(link, crawlPriorityScore(link));
  }

  return Array.from(unique.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([link]) => link);
}

function crawlPriorityScore(url) {
  const lower = normalizeText(url).toLowerCase();
  if (!lower) return 0;

  let score = 0;
  if (/(contact|about|team|leadership|management|staff|company|our-story|who-we-are|founder|owner|meet-the)/i.test(lower)) {
    score += 7;
  }
  if (/(email|support|help|faq|location|locations|office)/i.test(lower)) {
    score += 4;
  }
  if (/\/blog\//i.test(lower)) {
    score -= 2;
  }
  if (/[?&](replytocom|sort|filter|session|preview)=/i.test(lower)) {
    score -= 3;
  }
  if (/\?/.test(lower)) {
    score -= 1;
  }

  return score;
}

function canonicalizeCrawlUrl(rawUrl, baseOrigin) {
  const normalized = normalizeWebsiteUrl(rawUrl);
  if (!normalized) return "";

  try {
    const parsed = new URL(normalized);
    if (parsed.origin !== baseOrigin) return "";
    if (!/^https?:$/i.test(parsed.protocol)) return "";

    if (shouldSkipCrawlPath(parsed.pathname)) return "";

    const trackingParams = [
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
    for (const name of trackingParams) {
      parsed.searchParams.delete(name);
    }

    parsed.hash = "";
    parsed.pathname = parsed.pathname.replace(/\/{2,}/g, "/");
    if (parsed.pathname.length > 1) {
      parsed.pathname = parsed.pathname.replace(/\/+$/, "");
    }
    return parsed.toString();
  } catch (_e) {
    return "";
  }
}

function shouldSkipCrawlPath(pathname) {
  const lowerPath = normalizeText(pathname).toLowerCase();
  if (!lowerPath) return false;

  if (/\.(pdf|zip|rar|7z|gz|png|jpg|jpeg|gif|svg|webp|mp4|mp3|avi|mov|ico|css|js|xml|json)$/i.test(lowerPath)) {
    return true;
  }

  if (/(^|\/)(wp-admin|wp-login|login|signin|sign-in|signout|sign-out|logout|checkout|cart|basket|account)(\/|$)/i.test(lowerPath)) {
    return true;
  }

  return false;
}

function createScanTab(url, visible) {
  return new Promise((resolve, reject) => {
    chrome.tabs.create({ url, active: visible === true }, (tab) => {
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
  const ownerCandidates = [];
  const hostname = normalize(window.location.hostname || "").toLowerCase();

  const regexMatches = bodyText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
  for (const value of regexMatches) {
    const email = normalize(value).toLowerCase();
    if (isLikelyEmail(email)) emails.add(email);
  }

  if (hostname.includes("facebook.com")) {
    const fbMain = document.querySelector("[role='main']");
    const fbPrimaryText = normalize(
      (fbMain && fbMain.innerText) || document.title || ""
    );
    const fbMatches = fbPrimaryText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
    for (const value of fbMatches) {
      const email = normalize(value).toLowerCase();
      if (isLikelyEmail(email)) emails.add(email);
    }

    const fbContactMeta = [
      "meta[property='business:contact_data:email']",
      "meta[property='og:email']",
      "meta[name='email']"
    ];
    for (const selector of fbContactMeta) {
      const node = document.querySelector(selector);
      const email = normalize((node && node.getAttribute("content")) || "").toLowerCase();
      if (isLikelyEmail(email)) emails.add(email);
    }

    const ariaNodes = Array.from(document.querySelectorAll("a[aria-label], span[aria-label], div[aria-label]")).slice(0, 1800);
    for (const node of ariaNodes) {
      const aria = normalize(node.getAttribute("aria-label") || "");
      if (!aria) continue;
      const hits = aria.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
      for (const hit of hits) {
        const email = normalize(hit).toLowerCase();
        if (isLikelyEmail(email)) emails.add(email);
      }
    }
  }

  const tryCollectStructuredData = (obj) => {
    if (!obj || typeof obj !== "object") return;

    const typeValue = Array.isArray(obj["@type"]) ? obj["@type"].join(" ").toLowerCase() : String(obj["@type"] || "").toLowerCase();
    const nameValue = normalize(obj.name || "");
    const jobTitleValue = normalize(obj.jobTitle || "");
    const emailValue = normalize(obj.email || "").toLowerCase();

    if (emailValue && isLikelyEmail(emailValue)) {
      emails.add(emailValue);
    }

    if (/person|organization|localbusiness/.test(typeValue) && nameValue) {
      if (jobTitleValue) {
        ownerCandidates.push({ name: nameValue, title: jobTitleValue, score: 4 });
      } else if (/founder|ceo|president|owner|principal|director|manager/.test(typeValue)) {
        ownerCandidates.push({ name: nameValue, title: normalize(typeValue), score: 3 });
      }
    }

    const founder = obj.founder || obj.founders;
    if (founder) {
      const founders = Array.isArray(founder) ? founder : [founder];
      for (const item of founders) {
        if (!item) continue;
        const founderName = normalize(item.name || item.alternateName || "");
        const founderEmail = normalize(item.email || "").toLowerCase();
        if (founderName) {
          ownerCandidates.push({ name: founderName, title: "Founder", score: 5 });
        }
        if (founderEmail && isLikelyEmail(founderEmail)) {
          emails.add(founderEmail);
        }
      }
    }

    const employee = obj.employee || obj.employees;
    if (employee) {
      const employees = Array.isArray(employee) ? employee : [employee];
      for (const item of employees) {
        if (!item) continue;
        const employeeName = normalize(item.name || "");
        const employeeTitle = normalize(item.jobTitle || item.roleName || "");
        if (employeeName && /(owner|founder|ceo|president|principal|director|manager)/i.test(employeeTitle)) {
          ownerCandidates.push({ name: employeeName, title: employeeTitle, score: 4 });
        }
      }
    }
  };

  const jsonLdScripts = Array.from(document.querySelectorAll("script[type='application/ld+json']")).slice(0, 25);
  for (const script of jsonLdScripts) {
    const raw = normalize(script.textContent || "");
    if (!raw) continue;
    try {
      const parsed = JSON.parse(raw);
      const nodes = Array.isArray(parsed) ? parsed : [parsed];
      for (const node of nodes) {
        if (node && node["@graph"] && Array.isArray(node["@graph"])) {
          for (const graphNode of node["@graph"]) {
            tryCollectStructuredData(graphNode);
          }
        }
        tryCollectStructuredData(node);
      }
    } catch (_e) {
      // Ignore malformed JSON-LD blocks.
    }
  }

  const anchors = Array.from(document.querySelectorAll("a[href]"));
  const relatedLinkSet = new Set();
  const internalLinkSet = new Set();
  const socialLinkSet = new Set();
  const relatedKeywords = /(about|contact|team|leadership|management|staff|company|our-story|who-we-are|founder|owner)/i;
  const socialHostPattern = /(facebook\.com|instagram\.com|linkedin\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com|threads\.net)/i;
  const pageOrigin = window.location.origin;

  const isCrawlableInternal = (absoluteUrl) => {
    try {
      const parsed = new URL(absoluteUrl);
      if (parsed.origin !== pageOrigin) return false;
      if (!/^https?:$/i.test(parsed.protocol)) return false;
      const lowerPath = normalize(parsed.pathname || "").toLowerCase();
      if (!lowerPath) return true;
      if (/\.(pdf|zip|rar|7z|gz|png|jpg|jpeg|gif|svg|webp|mp4|mp3|avi|mov|ico|css|js|xml|json)$/i.test(lowerPath)) return false;
      if (/(^|\/)(wp-admin|wp-login|login|signin|sign-in|signout|sign-out|logout|checkout|cart|basket|account)(\/|$)/i.test(lowerPath)) return false;
      return true;
    } catch (_e) {
      return false;
    }
  };

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

    if (socialHostPattern.test(absolute)) {
      socialLinkSet.add(absolute);
      continue;
    }

    if (isCrawlableInternal(absolute)) {
      internalLinkSet.add(absolute);
    }

    const probe = `${text} ${absolute}`;
    if (!relatedKeywords.test(probe)) continue;

    if (relatedLinkSet.size < 30) {
      relatedLinkSet.add(absolute);
    }
  }

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
    relatedLinks: Array.from(relatedLinkSet).slice(0, 40),
    internalLinks: Array.from(internalLinkSet).slice(0, 250),
    socialLinks: Array.from(socialLinkSet).slice(0, 8),
    blocked
  };
}
