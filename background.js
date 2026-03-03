importScripts("shared.js");

const { MSG, rowsToCsv, normalizeText, normalizeWebsiteUrl, normalizeBusinessWebsiteUrl } = self.GbpShared;
const ENRICH_SESSION_KEY = "enrichSession";
const ENRICHMENT_SETTINGS_KEYS = [
  "enrichmentEnabled",
  "siteMaxPagesValue",
  "showEnrichmentTabsEnabled",
  "scanSocialLinksEnabled"
];
const RESULTS_PAGE_PATH = "results.html";
let lastEnrichPersistAtMs = 0;
let activeEnrichRun = null;
const autoOpenedResultsRunIds = new Set();
let lastAutoEnrichSourceRunId = "";

self.addEventListener("unhandledrejection", (event) => {
  const reason = event && event.reason;
  const message = normalizeText(reason && reason.message ? reason.message : reason);
  if (/could not establish connection|receiving end does not exist/i.test(message)) {
    event.preventDefault();
  }
});

self.addEventListener("error", (event) => {
  const message = normalizeText(event && event.message);
  if (/could not establish connection|receiving end does not exist/i.test(message)) {
    event.preventDefault();
  }
});

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

function storageGet(keys) {
  return new Promise((resolve, reject) => {
    chrome.storage.local.get(keys, (result) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message || "Storage read failed"));
        return;
      }
      resolve(result || {});
    });
  });
}

async function saveEnrichSession(session, forceRows, rows) {
  const previous = await storageGet([ENRICH_SESSION_KEY]).catch(() => ({}));
  const existing = previous && previous[ENRICH_SESSION_KEY] && typeof previous[ENRICH_SESSION_KEY] === "object"
    ? previous[ENRICH_SESSION_KEY]
    : {};

  const snapshot = {
    ...existing,
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

function safeSendResponse(sendResponse, payload) {
  try {
    sendResponse(payload);
  } catch (_error) {
    // Sender may be gone (popup closed) while async work is still running.
  }
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

  if (message.type === MSG.STOP_ENRICH) {
    handleStopEnrich(sendResponse);
    return true;
  }

  if (message.type === MSG.GET_ENRICH_STATE) {
    safeSendResponse(sendResponse, {
      ok: true,
      state: getEnrichRuntimeState()
    });
    return false;
  }

  if (message.type === MSG.SCRAPE_DONE) {
    handleScrapeDone(message);
    return false;
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
          safeSendResponse(sendResponse, {
            type: MSG.EXPORT_ERROR,
            error: chrome.runtime.lastError.message || "Failed to download CSV"
          });
          return;
        }

        safeSendResponse(sendResponse, {
          type: MSG.EXPORT_DONE,
          downloadId
        });
      }
    );
  } catch (error) {
    safeSendResponse(sendResponse, {
      type: MSG.EXPORT_ERROR,
      error: error && error.message ? error.message : "CSV export failed"
    });
  }
}

async function handleEnrichRows(message, sendResponse) {
  const rows = Array.isArray(message.rows) ? message.rows : [];
  if (activeEnrichRun) {
    safeSendResponse(sendResponse, {
      type: MSG.ENRICH_ERROR,
      error: "Enrichment already running"
    });
    return;
  }

  try {
    const result = await startEnrichRun(rows, message.options || {}, {
      sourceRunId: normalizeText(message.source_run_id),
      reason: "manual"
    });
    safeSendResponse(sendResponse, {
      type: MSG.ENRICH_DONE,
      rows: result.rows,
      summary: result.summary
    });
  } catch (error) {
    safeSendResponse(sendResponse, {
      type: MSG.ENRICH_ERROR,
      error: error && error.message ? error.message : "Website enrichment failed"
    });
  }
}

function getEnrichRuntimeState() {
  if (!activeEnrichRun) {
    return {
      is_running: false,
      run_id: "",
      status: "idle",
      stop_requested: false,
      scan_tab_id: null,
      source_run_id: ""
    };
  }

  return {
    is_running: true,
    run_id: normalizeText(activeEnrichRun.runId),
    status: activeEnrichRun.stopRequested === true ? "stopping" : "running",
    stop_requested: activeEnrichRun.stopRequested === true,
    scan_tab_id: Number.isFinite(Number(activeEnrichRun.scanTabId)) ? Number(activeEnrichRun.scanTabId) : null,
    source_run_id: normalizeText(activeEnrichRun.sourceRunId)
  };
}

async function startEnrichRun(rowsInput, optionsInput, metaInput) {
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const meta = metaInput && typeof metaInput === "object" ? metaInput : {};
  const rows = prepareRowsForEnrichment(rowsInput, "queued");
  const runId = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const startedAt = new Date().toISOString();
  const sourceRunId = normalizeText(meta.sourceRunId);
  const reason = normalizeText(meta.reason).toLowerCase();
  const shouldAutoOpenOnTerminal = reason === "auto_after_scrape";
  const runControl = {
    runId,
    sourceRunId,
    reason: normalizeText(meta.reason),
    startedAt,
    stopRequested: false,
    scanTabId: null
  };

  activeEnrichRun = runControl;
  lastEnrichPersistAtMs = 0;

  await saveEnrichSession(
    {
      run_id: runId,
      source_run_id: sourceRunId,
      reason: normalizeText(meta.reason),
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
      lead_signal_text: "Website enrichment started",
      lead_signal_tone: "info"
    },
    true,
    rows
  );

  try {
    const result = await enrichRows(rows, {
      ...options,
      shouldStop: () => runControl.stopRequested === true,
      onScanTabChange: (tabId) => {
        runControl.scanTabId = Number.isFinite(Number(tabId)) ? Number(tabId) : null;
      }
    });
    const stopped = result && result.summary && result.summary.stopped === true;

    await saveEnrichSession(
      {
        run_id: runId,
        source_run_id: sourceRunId,
        reason: normalizeText(meta.reason),
        status: stopped ? "stopped" : "done",
        started_at: startedAt,
        completed_at: new Date().toISOString(),
        ...result.summary,
        phase: stopped ? "stopped" : "done",
        lead_signal_text: stopped ? "Enrichment stopped by user" : "Website enrichment completed",
        lead_signal_tone: stopped ? "warn" : "success"
      },
      true,
      result.rows
    );

    if (shouldAutoOpenOnTerminal) {
      maybeAutoOpenResultsForRun(sourceRunId || runId);
    }

    return result;
  } catch (error) {
    await saveEnrichSession(
      {
        run_id: runId,
        source_run_id: sourceRunId,
        reason: normalizeText(meta.reason),
        status: "error",
        started_at: startedAt,
        completed_at: new Date().toISOString(),
        error: error && error.message ? error.message : "Website enrichment failed",
        phase: "error"
      },
      false
    );

    if (shouldAutoOpenOnTerminal) {
      maybeAutoOpenResultsForRun(sourceRunId || runId);
    }
    throw error;
  } finally {
    if (activeEnrichRun && activeEnrichRun.runId === runId) {
      activeEnrichRun = null;
    }
  }
}

function handleStopEnrich(sendResponse) {
  const run = activeEnrichRun;
  if (!run) {
    safeSendResponse(sendResponse, {
      ok: false,
      error: "No enrichment run is active"
    });
    return;
  }

  run.stopRequested = true;
  const runningTabId = Number(run.scanTabId);
  if (Number.isFinite(runningTabId)) {
    closeTab(runningTabId).catch(() => {});
  }

  saveEnrichSession(
    {
      run_id: run.runId,
      status: "stopping",
      phase: "stopping",
      lead_signal_text: "Stop requested",
      lead_signal_tone: "warn"
    },
    false
  ).catch(() => {});

  // User stop should surface partial results immediately.
  maybeAutoOpenResultsForRun(normalizeText(run.sourceRunId) || normalizeText(run.runId), { force: true });

  safeSendResponse(sendResponse, {
    ok: true
  });
}

function handleScrapeDone(message) {
  const runId = normalizeText(message && message.run_id);
  const rows = Array.isArray(message && message.rows) ? message.rows : [];
  const summary = message && typeof message.summary === "object" ? message.summary : {};
  const scrapeStopped = summary.stopped === true;

  if (scrapeStopped) {
    // When user stops the scrape, open partial results immediately.
    maybeAutoOpenResultsForRun(runId, { force: true });
  }

  void handlePostScrape(runId, rows, { scrapeStopped });
}

async function handlePostScrape(runId, rowsInput, metaInput) {
  const meta = metaInput && typeof metaInput === "object" ? metaInput : {};
  const scrapeStopped = meta.scrapeStopped === true;
  const rows = prepareRowsForEnrichment(rowsInput, "not_requested");
  const settings = await readEnrichmentSettings().catch(() => ({
    enrichmentEnabled: false,
    maxPagesPerSite: 40,
    visibleTabs: false,
    scanSocialLinks: true
  }));

  if (!settings.enrichmentEnabled || scrapeStopped) {
    await storageSet({ lastRows: rows }).catch(() => {});
    if (!scrapeStopped) {
      maybeAutoOpenResultsForRun(runId);
    }
    return;
  }

  const queuedRows = prepareRowsForEnrichment(rows, "queued");
  await storageSet({ lastRows: queuedRows }).catch(() => {});

  if (queuedRows.length === 0) {
    await saveEnrichSession(
      {
        source_run_id: normalizeText(runId),
        status: "done",
        started_at: new Date().toISOString(),
        completed_at: new Date().toISOString(),
        total: 0,
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
        phase: "done",
        lead_signal_text: "No rows to enrich",
        lead_signal_tone: "info"
      },
      true,
      queuedRows
    );
    maybeAutoOpenResultsForRun(runId);
    return;
  }

  if (activeEnrichRun) {
    return;
  }
  if (runId && runId === lastAutoEnrichSourceRunId) {
    return;
  }

  await saveEnrichSession(
    {
      source_run_id: normalizeText(runId),
      reason: "auto_after_scrape",
      status: "queued",
      started_at: new Date().toISOString(),
      total: queuedRows.length,
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
      phase: "queued",
      lead_signal_text: "Website enrichment queued",
      lead_signal_tone: "info"
    },
    true,
    queuedRows
  );

  if (runId) {
    lastAutoEnrichSourceRunId = runId;
  }

  startEnrichRun(
    queuedRows,
    {
      maxPagesPerSite: settings.maxPagesPerSite,
      timeoutMs: 10000,
      visibleTabs: settings.visibleTabs,
      scanSocialLinks: settings.scanSocialLinks,
      maxSocialPages: 4
    },
    {
      sourceRunId: runId,
      reason: "auto_after_scrape"
    }
  ).catch((error) => {
    console.warn("[enrich:auto] failed", error && error.message ? error.message : error);
  });
}

async function readEnrichmentSettings() {
  const data = await storageGet(ENRICHMENT_SETTINGS_KEYS);
  return {
    enrichmentEnabled: data.enrichmentEnabled === true,
    maxPagesPerSite: clampInt(data.siteMaxPagesValue, 1, 120, 40),
    visibleTabs: data.showEnrichmentTabsEnabled === true,
    scanSocialLinks: data.scanSocialLinksEnabled !== false
  };
}

function openOrFocusResultsPage(runId) {
  const baseUrl = chrome.runtime.getURL(RESULTS_PAGE_PATH);
  const url = runId ? `${baseUrl}?run_id=${encodeURIComponent(runId)}&t=${Date.now()}` : baseUrl;

  chrome.tabs.query({ url: `${baseUrl}*` }, (tabs) => {
    if (chrome.runtime.lastError) {
      chrome.tabs.create({ url, active: true }, () => {});
      return;
    }

    const existing = Array.isArray(tabs) && tabs.length > 0 ? tabs[0] : null;
    if (!existing || !existing.id) {
      chrome.tabs.create({ url, active: true }, () => {});
      return;
    }

    chrome.tabs.update(existing.id, { url, active: true }, () => {});
    if (Number.isFinite(Number(existing.windowId))) {
      chrome.windows.update(Number(existing.windowId), { focused: true }, () => {});
    }
  });
}

function maybeAutoOpenResultsForRun(runId, options) {
  const opts = options && typeof options === "object" ? options : {};
  const force = opts.force === true;
  const targetRunId = normalizeText(runId);

  if (!targetRunId) {
    openOrFocusResultsPage("");
    return;
  }

  if (!force && autoOpenedResultsRunIds.has(targetRunId)) {
    return;
  }

  autoOpenedResultsRunIds.add(targetRunId);
  openOrFocusResultsPage(targetRunId);
}

function prepareRowsForEnrichment(rows, statusForWebsite) {
  if (!Array.isArray(rows)) return [];
  const fallbackStatus = normalizeText(statusForWebsite).toLowerCase() || "not_requested";

  return rows.map((row) => {
    const sourceRow = row && typeof row === "object" ? row : {};
    const website = normalizeBusinessWebsiteUrl(sourceRow.website);
    const currentStatus = normalizeText(sourceRow.website_scan_status).toLowerCase();
    const websitePhone = sanitizePhoneText(sourceRow.website_phone);
    const fallbackPhone = sanitizePhoneText(sourceRow.phone);
    const listingPhone = sanitizePhoneText(
      sourceRow.listing_phone || (websitePhone && fallbackPhone === websitePhone ? "" : fallbackPhone)
    );
    const sitePagesVisited = Number(sourceRow.site_pages_visited || 0);
    const sitePagesDiscovered = Number(sourceRow.site_pages_discovered || 0);
    const socialPagesScanned = Number(sourceRow.social_pages_scanned || 0);

    let nextStatus = currentStatus;
    const shouldUpgradeToQueued =
      fallbackStatus === "queued" &&
      website &&
      (currentStatus === "" || currentStatus === "not_requested" || currentStatus === "no_website");
    const shouldRepairWebsiteStatus =
      fallbackStatus !== "queued" &&
      website &&
      (currentStatus === "" || currentStatus === "no_website");
    if (shouldUpgradeToQueued) {
      nextStatus = "queued";
    } else if (shouldRepairWebsiteStatus) {
      nextStatus = "not_requested";
    } else if (!nextStatus) {
      nextStatus = website ? fallbackStatus : "no_website";
    }
    if (
      !website &&
      (
        !nextStatus ||
        nextStatus === "not_requested" ||
        nextStatus === "queued" ||
        nextStatus === "running" ||
        nextStatus === "stopping" ||
        nextStatus === "init"
      )
    ) {
      nextStatus = "no_website";
    }

    return {
      ...sourceRow,
      website,
      phone: listingPhone || sanitizePhoneText(sourceRow.phone),
      listing_phone: listingPhone,
      website_phone: websitePhone,
      website_phone_source: normalizeText(sourceRow.website_phone_source),
      owner_name: normalizeText(sourceRow.owner_name),
      owner_title: normalizeText(sourceRow.owner_title),
      email: normalizeText(sourceRow.email),
      owner_email: normalizeText(sourceRow.owner_email),
      contact_email: normalizeText(sourceRow.contact_email),
      primary_email: normalizeText(sourceRow.primary_email),
      primary_email_type: normalizeText(sourceRow.primary_email_type),
      primary_email_source: normalizeText(sourceRow.primary_email_source),
      owner_confidence: normalizeText(sourceRow.owner_confidence),
      email_confidence: normalizeText(sourceRow.email_confidence),
      email_source_url: normalizeText(sourceRow.email_source_url),
      no_email_reason: normalizeText(sourceRow.no_email_reason),
      website_scan_status: normalizeText(nextStatus),
      site_pages_visited: Number.isFinite(sitePagesVisited) ? sitePagesVisited : 0,
      site_pages_discovered: Number.isFinite(sitePagesDiscovered) ? sitePagesDiscovered : 0,
      social_pages_scanned: Number.isFinite(socialPagesScanned) ? socialPagesScanned : 0,
      social_links: normalizeText(sourceRow.social_links)
    };
  });
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
    company_email_found: 0,
    stopped: false
  };

  const outputRows = [];
  let resumeIndex = rows.length;

  for (let index = 0; index < rows.length; index += 1) {
    if (isEnrichStopRequested(options)) {
      summary.stopped = true;
      resumeIndex = index;
      emitEnrichProgress(summary, {
        phase: "stopping",
        leadSignalText: "Stop requested",
        leadSignalTone: "warn",
        sitePagesVisited: summary.pages_visited,
        sitePagesDiscovered: summary.pages_discovered
      });
      break;
    }

    const row = rows[index];
    const sourceRow = row || {};
    const rawWebsitePhone = sanitizePhoneText(sourceRow.website_phone);
    const rawFallbackPhone = sanitizePhoneText(sourceRow.phone);
    const rawListingPhone = sanitizePhoneText(
      sourceRow.listing_phone || (rawWebsitePhone && rawFallbackPhone === rawWebsitePhone ? "" : rawFallbackPhone)
    );
    const enrichedRow = {
      ...sourceRow,
      phone: rawFallbackPhone || rawListingPhone,
      listing_phone: rawListingPhone,
      website_phone: rawWebsitePhone,
      website_phone_source: normalizeText(sourceRow.website_phone_source),
      owner_name: normalizeText(sourceRow.owner_name),
      owner_title: normalizeText(sourceRow.owner_title),
      email: normalizeText(sourceRow.email),
      owner_email: normalizeText(sourceRow.owner_email),
      contact_email: normalizeText(sourceRow.contact_email),
      primary_email: normalizeText(sourceRow.primary_email),
      primary_email_type: normalizeText(sourceRow.primary_email_type),
      primary_email_source: normalizeText(sourceRow.primary_email_source),
      owner_confidence: normalizeText(sourceRow.owner_confidence),
      email_confidence: normalizeText(sourceRow.email_confidence),
      email_source_url: normalizeText(sourceRow.email_source_url),
      no_email_reason: normalizeText(sourceRow.no_email_reason),
      website_scan_status: normalizeText(sourceRow.website_scan_status),
      site_pages_visited: Number(sourceRow.site_pages_visited || 0),
      site_pages_discovered: Number(sourceRow.site_pages_discovered || 0),
      social_pages_scanned: Number(sourceRow.social_pages_scanned || 0),
      social_links: normalizeText(sourceRow.social_links)
    };

    const website = normalizeBusinessWebsiteUrl(sourceRow.website);
    enrichedRow.website = website;

    if (!website) {
      enrichedRow.website_scan_status = "no_website";
      enrichedRow.no_email_reason = "no_website";
      enrichedRow.email_source_url = "";
      enrichedRow.email_confidence = "";
      enrichedRow.site_pages_visited = 0;
      enrichedRow.site_pages_discovered = 0;
      enrichedRow.social_pages_scanned = 0;
      enrichedRow.social_links = "";
      enrichedRow.website_phone = "";
      enrichedRow.website_phone_source = "";
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
        intent: deriveScanIntent(sourceRow),
        shouldStop: options.shouldStop,
        onTabChange: options.onScanTabChange,
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

      if (isEnrichStopRequested(options)) {
        summary.stopped = true;
        resumeIndex = index + 1;
        outputRows.push(sourceRow);
        emitEnrichProgress(summary, {
          currentName: sourceRow.name,
          currentUrl: website,
          phase: "stopping",
          leadSignalText: "Stop requested",
          leadSignalTone: "warn",
          sitePagesVisited: summary.pages_visited,
          sitePagesDiscovered: summary.pages_discovered
        });
        break;
      }

      if (scan.ownerName) enrichedRow.owner_name = scan.ownerName;
      if (scan.ownerTitle) enrichedRow.owner_title = scan.ownerTitle;
      if (scan.ownerConfidence) {
        enrichedRow.owner_confidence = scan.ownerConfidence;
      }
      const ownerEmailFound = normalizeText(scan.ownerEmail);
      const contactEmailFound = normalizeText(scan.contactEmail);
      const primaryEmailFound = normalizeText(scan.primaryEmail);
      const fallbackEmail =
        primaryEmailFound ||
        ownerEmailFound ||
        contactEmailFound ||
        normalizeText(enrichedRow.primary_email) ||
        normalizeText(enrichedRow.owner_email) ||
        normalizeText(enrichedRow.contact_email) ||
        normalizeText(enrichedRow.email);

      if (ownerEmailFound) {
        enrichedRow.owner_email = ownerEmailFound;
      }
      if (contactEmailFound) {
        enrichedRow.contact_email = contactEmailFound;
      }
      if (primaryEmailFound) {
        enrichedRow.primary_email = primaryEmailFound;
      }
      if (normalizeText(scan.primaryEmailType)) {
        enrichedRow.primary_email_type = normalizeText(scan.primaryEmailType);
      }
      if (normalizeText(scan.primaryEmailSource)) {
        enrichedRow.primary_email_source = normalizeText(scan.primaryEmailSource);
      }
      if (normalizeText(scan.emailSourceUrl)) {
        enrichedRow.email_source_url = normalizeText(scan.emailSourceUrl);
      }
      if (normalizeText(scan.emailConfidence)) {
        enrichedRow.email_confidence = normalizeText(scan.emailConfidence);
      } else {
        enrichedRow.email_confidence = "";
      }
      enrichedRow.no_email_reason = normalizeText(scan.noEmailReason);
      enrichedRow.email = fallbackEmail;
      const websitePhone = sanitizePhoneText(scan.primaryPhone);
      if (websitePhone) {
        enrichedRow.website_phone = websitePhone;
      }
      if (normalizeText(scan.primaryPhoneSource)) {
        enrichedRow.website_phone_source = normalizeText(scan.primaryPhoneSource);
      }
      if (!normalizeText(enrichedRow.phone) && websitePhone) {
        enrichedRow.phone = websitePhone;
      }
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
    } catch (rowError) {
      if (isEnrichStopError(rowError) || isEnrichStopRequested(options)) {
        summary.stopped = true;
        resumeIndex = index + 1;
        outputRows.push(sourceRow);
        emitEnrichProgress(summary, {
          currentName: sourceRow.name,
          currentUrl: website,
          phase: "stopped",
          leadSignalText: "Enrichment stopped by user",
          leadSignalTone: "warn",
          sitePagesVisited: summary.pages_visited,
          sitePagesDiscovered: summary.pages_discovered
        });
        break;
      }

      const message = rowError && rowError.message ? normalizeText(rowError.message) : "unknown_error";
      console.warn("[enrich] scan failed", normalizeText(sourceRow.website), message);
      enrichedRow.website_scan_status = "scan_error";
      enrichedRow.no_email_reason = "scan_error";
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

  if (summary.stopped && resumeIndex < rows.length) {
    for (const remaining of rows.slice(resumeIndex)) {
      outputRows.push(remaining || {});
    }
  }

  return {
    rows: outputRows,
    summary
  };
}

function isEnrichStopRequested(options) {
  return Boolean(options && typeof options.shouldStop === "function" && options.shouldStop() === true);
}

function createEnrichStopError() {
  const error = new Error("Enrichment stopped by user");
  error.code = "ENRICH_STOPPED";
  return error;
}

function isEnrichStopError(error) {
  if (!error || typeof error !== "object") return false;
  if (error.code === "ENRICH_STOPPED") return true;
  const message = normalizeText(error.message).toLowerCase();
  return message.includes("stopped by user");
}

function emitEnrichProgress(summary, context) {
  const ctx = context || {};
  const activeRun = activeEnrichRun;
  const payload = {
    type: MSG.ENRICH_PROGRESS,
    run_id: normalizeText(activeRun && activeRun.runId),
    source_run_id: normalizeText(activeRun && activeRun.sourceRunId),
    reason: normalizeText(activeRun && activeRun.reason),
    started_at: normalizeText(activeRun && activeRun.startedAt),
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

  const now = Date.now();
  const phase = normalizeText(ctx.phase).toLowerCase();
  const forcePersist = /^(done|skip|error|stopping|stopped)$/.test(phase);
  const progressStatus = phase === "stopping" || phase === "stopped" ? "stopping" : "running";
  if (forcePersist || now - lastEnrichPersistAtMs >= 350) {
    lastEnrichPersistAtMs = now;
    saveEnrichSession(
      {
        status: progressStatus,
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

function deriveScanIntent(row) {
  const source = row && typeof row === "object" ? row : {};
  const hasEmail =
    normalizeText(source.email) ||
    normalizeText(source.owner_email) ||
    normalizeText(source.contact_email) ||
    normalizeText(source.primary_email);
  const hasOwner = normalizeText(source.owner_name);
  const hasPhone = sanitizePhoneText(source.phone);

  return {
    needsEmail: !hasEmail,
    needsOwner: !hasOwner,
    needsPhone: !hasPhone
  };
}

function normalizeScanIntent(intent) {
  const raw = intent && typeof intent === "object" ? intent : {};
  const needsEmail = raw.needsEmail !== false;
  return {
    needsEmail,
    needsOwner: raw.needsOwner === true,
    needsPhone: raw.needsPhone === true
  };
}

function isHighIntentPath(url, intent) {
  const lower = normalizeText(url).toLowerCase();
  if (!lower) return false;
  const inx = normalizeScanIntent(intent);
  if (inx.needsEmail && /(contact|support|help|email|customer|service|about|faq|legal|privacy)/i.test(lower)) return true;
  if (inx.needsOwner && /(team|about|leadership|management|staff|founder|owner|who-we-are|our-story)/i.test(lower)) return true;
  if (inx.needsPhone && /(contact|locations?|office|call)/i.test(lower)) return true;
  return /(contact|about|team|leadership|staff)/i.test(lower);
}

function prioritizeCrawlLinkEntries(links, intent) {
  if (!Array.isArray(links) || links.length === 0) return [];

  const unique = new Map();
  for (const rawLink of links) {
    const link = normalizeBusinessWebsiteUrl(rawLink) || normalizeWebsiteUrl(rawLink);
    if (!link || unique.has(link)) continue;
    unique.set(link, crawlPriorityScore(link, intent));
  }

  return Array.from(unique.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([link, score]) => ({ link, score }));
}

async function scanWebsite(startUrl, options) {
  let tab = null;
  const emails = new Set();
  const phones = new Set();
  const emailSourceByAddress = new Map();
  const emailSourceUrlByAddress = new Map();
  const phoneSourceByNumber = new Map();
  const ownerCandidates = [];
  const socialCandidates = new Set();
  const socialDiscovered = new Set();
  let blocked = false;
  let socialScanned = 0;
  let pagesVisited = 0;
  let priorityPagesVisited = 0;
  const highIntentDiscovered = new Set();
  let sitemapAttempted = false;
  const intent = normalizeScanIntent(options.intent);

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

  const assertNotStopped = () => {
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }
  };

  const registerEmail = (email, sourceUrl) => {
    const normalized = normalizeEmail(email);
    if (!normalized) return;
    emails.add(normalized);

    if (!emailSourceByAddress.has(normalized)) {
      emailSourceByAddress.set(normalized, classifyEmailSource(sourceUrl));
    }
    if (!emailSourceUrlByAddress.has(normalized)) {
      const source = normalizeBusinessWebsiteUrl(sourceUrl) || normalizeWebsiteUrl(sourceUrl) || normalizeText(sourceUrl);
      emailSourceUrlByAddress.set(normalized, source);
    }
  };

  const registerSocialCandidate = (socialUrl) => {
    const normalizedSocial = normalizeBusinessWebsiteUrl(socialUrl);
    if (!normalizedSocial) return;
    socialCandidates.add(normalizedSocial);
    socialDiscovered.add(normalizedSocial);
  };

  const registerPhone = (phoneValue, sourceUrl) => {
    const normalizedPhone = sanitizePhoneText(phoneValue);
    if (!normalizedPhone) return;
    phones.add(normalizedPhone);
    if (!phoneSourceByNumber.has(normalizedPhone)) {
      phoneSourceByNumber.set(normalizedPhone, classifyEmailSource(sourceUrl));
    }
  };

  try {
    assertNotStopped();
    tab = await createScanTab(firstUrl, options.visibleTabs === true);
    if (typeof options.onTabChange === "function") {
      options.onTabChange(tab && tab.id != null ? tab.id : null);
    }
    reportProgress("site_open", firstUrl);

    while (visited.size < options.maxPagesPerSite) {
      if (queue.length === 0) {
        if (emails.size > 0 || sitemapAttempted) {
          break;
        }

        sitemapAttempted = true;
        reportProgress("sitemap_lookup", `${baseOrigin}/sitemap.xml`);
        const sitemapLinks = await discoverLinksFromSitemap(tab.id, baseOrigin, options.timeoutMs, intent).catch(() => []);
        if (!Array.isArray(sitemapLinks) || sitemapLinks.length === 0) {
          break;
        }

        const sitemapEntries = prioritizeCrawlLinkEntries(sitemapLinks, intent);
        for (const entry of sitemapEntries) {
          const normalizedLink = canonicalizeCrawlUrl(entry.link, baseOrigin);
          if (!normalizedLink) continue;
          const linkKey = stripHash(normalizedLink);
          if (!linkKey) continue;
          if (visited.has(linkKey) || queued.has(linkKey)) continue;
          if (discovered.size >= options.maxDiscoveredPages) continue;

          discovered.add(linkKey);
          queued.add(linkKey);
          queue.push(normalizedLink);
          if (entry.score >= 6) {
            highIntentDiscovered.add(linkKey);
          }
        }

        reportProgress("sitemap_queue", queue[0] || baseOrigin);
        if (queue.length === 0) {
          break;
        }
      }

      assertNotStopped();
      const nextUrl = queue.shift();
      const nextKey = stripHash(nextUrl);
      if (!nextKey || visited.has(nextKey)) continue;

      visited.add(nextKey);
      pagesVisited = visited.size;
      if (isHighIntentPath(nextUrl, intent)) {
        priorityPagesVisited += 1;
      }
      reportProgress("site_page", nextUrl);

      let pageData = null;
      try {
        assertNotStopped();
        await updateTabUrl(tab.id, nextUrl);
        await waitForTabComplete(tab.id, options.timeoutMs);
        assertNotStopped();
        await sleep(800);
        pageData = await executeExtraction(tab.id);
      } catch (_sitePageError) {
        assertNotStopped();
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
      for (const phoneValue of pageData.phones || []) {
        registerPhone(phoneValue, nextUrl);
      }

      for (const candidate of pageData.ownerCandidates || []) {
        if (!candidate || !candidate.name) continue;
        ownerCandidates.push({
          name: normalizeText(candidate.name),
          title: normalizeText(candidate.title),
          score: Number(candidate.score) || 0,
          source: normalizeText(candidate.source)
        });
      }

      const bestOwnerNow = pickBestOwner(ownerCandidates, Array.from(emails));
      const personalEmailNow = chooseOwnerEmail(Array.from(emails), bestOwnerNow ? bestOwnerNow.name : "");
      if (personalEmailNow) {
        reportProgress("site_personal_email_found", nextUrl);
        break;
      }

      const prioritizedLinks = prioritizeCrawlLinkEntries([
        ...(Array.isArray(pageData.relatedLinks) ? pageData.relatedLinks : []),
        ...(Array.isArray(pageData.internalLinks) ? pageData.internalLinks : [])
      ], intent);
      const hasHighIntentCandidate = prioritizedLinks.some((entry) => entry.score >= 6);

      for (const entry of prioritizedLinks) {
        if (
          entry.score <= -3 &&
          hasHighIntentCandidate &&
          queue.length >= Math.min(8, options.maxPagesPerSite)
        ) {
          continue;
        }

        const normalizedLink = canonicalizeCrawlUrl(entry.link, baseOrigin);
        if (!normalizedLink) continue;

        const linkKey = stripHash(normalizedLink);
        if (!linkKey) continue;
        if (visited.has(linkKey) || queued.has(linkKey)) continue;
        if (discovered.size >= options.maxDiscoveredPages) continue;

        discovered.add(linkKey);
        queued.add(linkKey);
        queue.push(normalizedLink);
        if (entry.score >= 6) {
          highIntentDiscovered.add(linkKey);
        }
      }

      for (const social of pageData.socialLinks || []) {
        registerSocialCandidate(social);
      }
      reportProgress("site_page_done", nextUrl);
    }

    const bestOwnerBeforeSocial = pickBestOwner(ownerCandidates, Array.from(emails));
    const personalBeforeSocial = chooseOwnerEmail(Array.from(emails), bestOwnerBeforeSocial ? bestOwnerBeforeSocial.name : "");
    if (options.scanSocialLinks === true && !personalBeforeSocial && socialCandidates.size > 0) {
      const socialQueue = prioritizeSocialLinks(Array.from(socialCandidates))
        .filter(shouldScanSocialUrl)
        .slice(0, options.maxSocialPages || 0);
      const scannedTargets = new Set();
      let socialBudget = Number(options.maxSocialPages || 0);

      for (const baseSocialUrl of socialQueue) {
        if (socialBudget <= 0) break;
        const probes = buildSocialProbeUrls(baseSocialUrl);
        for (const socialUrl of probes) {
          if (socialBudget <= 0) break;
          const normalizedTarget = normalizeBusinessWebsiteUrl(socialUrl) || normalizeWebsiteUrl(socialUrl);
          if (!normalizedTarget || scannedTargets.has(normalizedTarget)) continue;
          scannedTargets.add(normalizedTarget);
          socialBudget -= 1;

          assertNotStopped();
          reportProgress("social_page", normalizedTarget);
          let socialData = null;
          try {
            assertNotStopped();
            await updateTabUrl(tab.id, normalizedTarget);
            await waitForTabComplete(tab.id, options.timeoutMs);
            assertNotStopped();
            await sleep(700);
            socialData = await executeExtraction(tab.id);
          } catch (_socialPageError) {
            assertNotStopped();
            socialScanned += 1;
            reportProgress("social_error", normalizedTarget);
            continue;
          }

          socialScanned += 1;
          reportProgress("social_done", normalizedTarget);
          if (!socialData) continue;

          if (socialData.blocked === true) {
            blocked = true;
          }

          for (const email of socialData.emails || []) {
            registerEmail(email, normalizedTarget);
          }
          for (const phoneValue of socialData.phones || []) {
            registerPhone(phoneValue, normalizedTarget);
          }

          for (const candidate of socialData.ownerCandidates || []) {
            if (!candidate || !candidate.name) continue;
            ownerCandidates.push({
              name: normalizeText(candidate.name),
              title: normalizeText(candidate.title),
              score: Number(candidate.score) || 0,
              source: normalizeText(candidate.source || "social")
            });
          }
          const bestOwnerNow = pickBestOwner(ownerCandidates, Array.from(emails));
          const personalEmailNow = chooseOwnerEmail(Array.from(emails), bestOwnerNow ? bestOwnerNow.name : "");
          if (personalEmailNow) {
            reportProgress("social_personal_email_found", normalizedTarget);
            socialBudget = 0;
            break;
          }
        }
      }
    }
  } finally {
    if (typeof options.onTabChange === "function") {
      options.onTabChange(null);
    }
    if (tab && tab.id != null) {
      await closeTab(tab.id).catch(() => {});
    }
  }

  const emailList = Array.from(emails);
  const bestOwner = pickBestOwner(ownerCandidates, emailList);
  const ownerEmail = chooseOwnerEmail(emailList, bestOwner ? bestOwner.name : "");
  // Preference order: owner/personal first, company fallback only when owner is missing.
  const contactEmail = ownerEmail ? "" : chooseContactEmail(emailList, ownerEmail);
  const primaryEmail = ownerEmail || contactEmail || "";
  const primaryEmailType = ownerEmail ? "personal" : contactEmail ? "company" : "";
  const primaryEmailSource = primaryEmail ? sourceForEmail(primaryEmail, emailSourceByAddress) : "";
  const emailSourceUrl = primaryEmail ? sourceUrlForEmail(primaryEmail, emailSourceUrlByAddress) : "";
  const emailConfidence = primaryEmail
    ? formatConfidence(
      estimateEmailConfidence({
        primaryEmail,
        primaryEmailType,
        primaryEmailSource,
        ownerEmail,
        ownerName: bestOwner ? bestOwner.name : "",
        emailSourceUrl
      })
    )
    : "";
  const primaryPhone = choosePrimaryPhone(Array.from(phones));
  const primaryPhoneSource = primaryPhone ? sourceForPhone(primaryPhone, phoneSourceByNumber) : "";
  const ownerConfidence = bestOwner ? formatConfidence(bestOwner.confidence) : "";

  let status = "no_public_data";
  if (bestOwner || primaryEmail || primaryPhone) {
    status = "enriched";
  } else if (blocked) {
    status = "blocked";
  }
  let noEmailReason = "";
  if (!primaryEmail) {
    if (blocked) {
      noEmailReason = "blocked";
    } else if (priorityPagesVisited === 0 && highIntentDiscovered.size === 0) {
      noEmailReason = "no_contact_page";
    } else {
      noEmailReason = "no_public_email";
    }
  }

  return {
    ownerName: bestOwner ? bestOwner.name : "",
    ownerTitle: bestOwner ? bestOwner.title : "",
    ownerConfidence,
    ownerEmail,
    contactEmail,
    primaryEmail,
    primaryEmailType,
    primaryEmailSource,
    emailSourceUrl,
    emailConfidence,
    noEmailReason,
    primaryPhone,
    primaryPhoneSource,
    status,
    blocked,
    socialScanned,
    pagesVisited,
    pagesDiscovered: discovered.size,
    socialLinks: Array.from(socialDiscovered).slice(0, 20)
  };
}

function buildSocialProbeUrls(url) {
  const normalized = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url);
  if (!normalized) return [];

  const out = [normalized];
  const host = hostnameForUrl(normalized);
  if (!host || !host.includes("facebook.com")) {
    return out;
  }

  try {
    const parsed = new URL(normalized);
    const rootPath = parsed.pathname.replace(/\/+$/, "");
    const roots = [rootPath];
    if (/\/about(\/|$)/i.test(rootPath)) {
      roots.push(rootPath.replace(/\/about(\/.*)?$/i, ""));
    }
    if (/\/info(\/|$)/i.test(rootPath)) {
      roots.push(rootPath.replace(/\/info(\/.*)?$/i, ""));
    }

    for (const root of roots.filter(Boolean)) {
      const base = root.replace(/\/+$/, "");
      const candidates = [
        `${parsed.origin}${base}`,
        `${parsed.origin}${base}/about`,
        `${parsed.origin}${base}/about_contact_and_basic_info`,
        `${parsed.origin}${base}/info_contact`,
        `${parsed.origin}${base}/details`
      ];
      for (const candidate of candidates) {
        const normalizedCandidate = normalizeWebsiteUrl(candidate);
        if (normalizedCandidate && !out.includes(normalizedCandidate)) {
          out.push(normalizedCandidate);
        }
      }
    }
  } catch (_error) {
    return out;
  }

  return out;
}

async function discoverLinksFromSitemap(tabId, baseOrigin, timeoutMs, intent) {
  const sitemapPaths = ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"];
  const links = new Set();

  for (const path of sitemapPaths) {
    const sitemapUrl = `${baseOrigin}${path}`;
    try {
      await updateTabUrl(tabId, sitemapUrl);
      await waitForTabComplete(tabId, timeoutMs);
      await sleep(500);
      const extracted = await executeSitemapExtraction(tabId);
      for (const rawLink of extracted) {
        const normalized = normalizeWebsiteUrl(rawLink);
        if (!normalized || !normalized.startsWith(baseOrigin)) continue;
        links.add(normalized);
      }
      if (links.size >= 120) break;
    } catch (_error) {
      // Try next sitemap path.
    }
  }

  const prioritized = prioritizeCrawlLinkEntries(Array.from(links), intent);
  return prioritized.slice(0, 120).map((entry) => entry.link);
}

function executeSitemapExtraction(tabId) {
  return new Promise((resolve, reject) => {
    chrome.scripting.executeScript(
      {
        target: { tabId },
        func: extractSitemapLinksScript
      },
      (results) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message || "Failed to parse sitemap"));
          return;
        }
        if (!Array.isArray(results) || !results[0]) {
          resolve([]);
          return;
        }
        const value = results[0].result;
        resolve(Array.isArray(value) ? value : []);
      }
    );
  });
}

function extractSitemapLinksScript() {
  const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim();
  const out = new Set();

  const xmlNodes = Array.from(document.querySelectorAll("loc"));
  for (const node of xmlNodes) {
    const value = normalize(node.textContent || "");
    if (value) out.add(value);
  }

  const bodyText = normalize((document.body && (document.body.innerText || document.body.textContent)) || "");
  const regex = /https?:\/\/[a-z0-9.-]+\.[a-z]{2,}(?:\/[^\s<>"']*)?/gi;
  const matches = bodyText.match(regex) || [];
  for (const match of matches) {
    const value = normalize(match).replace(/[),.;]+$/, "");
    if (value) out.add(value);
  }

  return Array.from(out).slice(0, 300);
}

function pickBestOwner(candidates, emails) {
  if (!Array.isArray(candidates) || candidates.length === 0) return null;
  const emailList = sanitizeEmailList(emails);
  const emailLocals = emailList.map((email) => localPartForEmail(email));
  const aggregate = new Map();

  for (const candidate of candidates) {
    const name = normalizeText(candidate && candidate.name);
    if (!isLikelyPersonName(name)) continue;

    const title = normalizeText(candidate && candidate.title);
    const source = normalizeText(candidate && candidate.source);
    const baseScore = Number(candidate && candidate.score) || 0;
    const key = `${name.toLowerCase()}::${title.toLowerCase()}`;

    let weightedScore = baseScore + (title ? 1 : 0) + Math.min(2, name.split(/\s+/).length - 1);
    if (/(owner|founder|ceo|president|principal|director|partner|manager)/i.test(title)) {
      weightedScore += 1.5;
    }
    if (/jsonld|schema/i.test(source)) {
      weightedScore += 0.8;
    }
    if (/heading|h1|h2|h3/i.test(source)) {
      weightedScore += 0.7;
    }

    const tokens = name
      .toLowerCase()
      .split(/\s+/)
      .filter((token) => token.length >= 3);
    if (tokens.length > 0 && emailLocals.some((local) => tokens.some((token) => local.includes(token)))) {
      weightedScore += 1.2;
    }

    const existing = aggregate.get(key) || {
      name,
      title,
      scoreTotal: 0,
      count: 0
    };
    existing.scoreTotal += weightedScore;
    existing.count += 1;
    aggregate.set(key, existing);
  }

  let best = null;
  for (const entry of aggregate.values()) {
    const repetitionBonus = Math.min(2, (entry.count - 1) * 0.8);
    const avgScore = entry.scoreTotal / Math.max(1, entry.count);
    const finalScore = avgScore + repetitionBonus;
    const confidence = Math.min(0.99, Math.max(0.35, 0.42 + finalScore / 20));

    if (!best || finalScore > best.finalScore) {
      best = {
        name: entry.name,
        title: entry.title,
        finalScore,
        confidence
      };
    }
  }

  if (!best) return null;
  return {
    name: best.name,
    title: best.title,
    confidence: best.confidence
  };
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
    const [local = "", domain = ""] = email.split("@");
    if (isLikelyPersonalMailboxLocalPart(local, domain)) {
      return email;
    }
  }

  return "";
}

function chooseContactEmail(emails, ownerEmail) {
  const list = sanitizeEmailList(emails).filter((email) => email !== ownerEmail);
  if (list.length === 0) return "";

  const priorityPrefixes = [
    "owner",
    "founder",
    "ceo",
    "president",
    "principal",
    "director",
    "info",
    "contact",
    "hello",
    "office",
    "support",
    "customer",
    "customers",
    "client",
    "clients",
    "admin",
    "sales",
    "service",
    "help",
    "billing",
    "accounts",
    "accounting",
    "finance",
    "operations",
    "ops",
    "dispatch",
    "bookings",
    "reservations"
  ];

  for (const prefix of priorityPrefixes) {
    const hit = list.find((email) => hasMailboxPrefix(localPartForEmail(email), prefix));
    if (hit) return hit;
  }

  return list[0] || "";
}

function isGenericMailboxLocalPart(localPart) {
  const local = normalizeText(localPart).toLowerCase();
  if (!local) return false;
  const genericPrefixes = [
    "info",
    "contact",
    "hello",
    "office",
    "support",
    "customer",
    "customers",
    "client",
    "clients",
    "admin",
    "sales",
    "team",
    "careers",
    "career",
    "jobs",
    "job",
    "hr",
    "humanresources",
    "service",
    "help",
    "enquiries",
    "inquiries",
    "billing",
    "accounts",
    "accounting",
    "finance",
    "payments",
    "payroll",
    "bookings",
    "reservations",
    "reservation",
    "dispatch",
    "operations",
    "ops",
    "marketing",
    "media",
    "press",
    "pr",
    "partnerships",
    "partners",
    "legal",
    "privacy",
    "compliance",
    "webmaster",
    "postmaster",
    "hostmaster",
    "security",
    "abuse",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
    "newsletter",
    "news",
    "updates",
    "notifications",
    "alerts",
    "community",
    "members",
    "store",
    "orders",
    "returns",
    "reception",
    "frontdesk",
    "helpdesk",
    "servicedesk",
    "customercare",
    "customerservice",
    "clientservice",
    "mail"
  ];

  return genericPrefixes.some((prefix) => hasMailboxPrefix(local, prefix));
}

function isLikelyPersonalMailboxLocalPart(localPart, domainPart) {
  const local = normalizeText(localPart).toLowerCase();
  if (!local || isGenericMailboxLocalPart(local)) return false;

  if (/(support|customer|client|service|sales|billing|admin|hello|info|contact|office|team|hr|jobs|careers|marketing|media|press|accounts?|finance|booking|reservations?|dispatch|operations?|ops|legal|privacy|compliance|security|abuse|newsletter|notifications?|alerts?|orders?|returns?|store|community|member|partners?|partnerships)/i.test(local)) {
    return false;
  }

  if (/(plumbing|restoration|services?|hvac|electric|roofing|construction|clinic|dental|homes?|group|company|corp|inc|llc|ltd)/i.test(local)) {
    return false;
  }

  const domainRoot = normalizeText(domainPart).toLowerCase().split(".")[0].replace(/[^a-z0-9]/g, "");
  const localFlat = local.replace(/[^a-z0-9]/g, "");
  if (domainRoot && localFlat.includes(domainRoot)) {
    return false;
  }

  if (/^[a-z]{2,}[._-][a-z]{2,}$/i.test(local)) return true;
  if (/^[a-z]{1,2}[._-][a-z]{2,}$/i.test(local)) return true;
  if (/^[a-z]{4,14}$/i.test(local) && !/\d/.test(local)) return true;
  return false;
}

function localPartForEmail(email) {
  const value = normalizeText(email).toLowerCase();
  const at = value.indexOf("@");
  if (at <= 0) return "";
  return value.slice(0, at);
}

function hasMailboxPrefix(localPart, prefix) {
  const local = normalizeText(localPart).toLowerCase();
  const token = normalizeText(prefix).toLowerCase();
  if (!local || !token) return false;
  if (local === token) return true;
  if (!local.startsWith(token)) return false;
  const next = local.charAt(token.length);
  return next === "." || next === "_" || next === "-" || /\d/.test(next);
}

function sourceForEmail(email, sourceMap) {
  if (!email || !(sourceMap instanceof Map)) return "";
  const source = sourceMap.get(email);
  return normalizeText(source);
}

function sourceForPhone(phone, sourceMap) {
  if (!phone || !(sourceMap instanceof Map)) return "";
  const source = sourceMap.get(phone);
  return normalizeText(source);
}

function sourceUrlForEmail(email, sourceMap) {
  if (!email || !(sourceMap instanceof Map)) return "";
  const source = sourceMap.get(email);
  return normalizeText(source);
}

function estimateEmailConfidence(params) {
  const input = params && typeof params === "object" ? params : {};
  const type = normalizeText(input.primaryEmailType).toLowerCase();
  const source = normalizeText(input.primaryEmailSource).toLowerCase();
  const ownerEmail = normalizeText(input.ownerEmail).toLowerCase();
  const ownerName = normalizeText(input.ownerName).toLowerCase();
  const sourceUrl = normalizeText(input.emailSourceUrl).toLowerCase();
  const primaryEmail = normalizeText(input.primaryEmail).toLowerCase();
  if (!primaryEmail) return 0;

  let score = 0.6;
  if (type === "personal") score += 0.26;
  if (type === "company") score += 0.14;
  if (source === "website") score += 0.06;
  if (source === "facebook") score -= 0.07;
  if (ownerEmail && ownerEmail === primaryEmail) score += 0.05;

  const local = localPartForEmail(primaryEmail);
  const ownerTokens = ownerName.split(/\s+/).filter((token) => token.length >= 3);
  if (ownerTokens.some((token) => local.includes(token))) {
    score += 0.05;
  }
  if (sourceUrl && sourceUrl.includes("/about")) score += 0.02;
  if (sourceUrl && sourceUrl.includes("/contact")) score += 0.02;

  return Math.min(0.99, Math.max(0.35, score));
}

function formatConfidence(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "";
  return numeric.toFixed(2);
}

function prioritizeSocialLinks(links) {
  if (!Array.isArray(links) || links.length === 0) return [];
  const unique = new Map();
  for (const rawLink of links) {
    const normalized = normalizeBusinessWebsiteUrl(rawLink) || normalizeWebsiteUrl(rawLink);
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
  const normalized = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url);
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

function sanitizePhoneText(value) {
  const raw = normalizeText(value);
  if (!raw) return "";

  const withoutPrefixNoise = raw.replace(/^[^\d+()]+/, "");
  const matched = withoutPrefixNoise.match(/(?:\+?\d[\d().\s-]{7,}\d)/);
  const candidate = normalizeText(matched ? matched[0] : withoutPrefixNoise).replace(/[^\d+().\s-]/g, " ");
  const compact = normalizeText(candidate);
  const digits = compact.replace(/\D/g, "");
  if (!isLikelyPhoneDigits(digits)) return "";
  return compact;
}

function isLikelyPhoneDigits(digits) {
  const value = normalizeText(digits).replace(/\D/g, "");
  if (!value) return false;
  return value.length >= 10 && value.length <= 15;
}

function choosePrimaryPhone(phones) {
  if (!Array.isArray(phones)) return "";

  const seen = new Set();
  const list = [];
  for (const item of phones) {
    const phone = sanitizePhoneText(item);
    if (!phone) continue;
    const key = phone.replace(/\D/g, "");
    if (!key || seen.has(key)) continue;
    seen.add(key);
    list.push(phone);
  }

  if (list.length === 0) return "";
  if (list.length === 1) return list[0];

  const score = (phone) => {
    const digits = phone.replace(/\D/g, "");
    let points = 0;
    if (phone.startsWith("+")) points += 3;
    if (digits.length === 11 && digits.startsWith("1")) points += 2;
    if (digits.length === 10) points += 2;
    if (/[()]/.test(phone)) points += 1;
    return points;
  };

  list.sort((a, b) => score(b) - score(a));
  return list[0];
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

function prioritizeCrawlLinks(links, intent) {
  return prioritizeCrawlLinkEntries(links, intent).map((entry) => entry.link);
}

function crawlPriorityScore(url, intent) {
  const lower = normalizeText(url).toLowerCase();
  if (!lower) return 0;
  const inx = normalizeScanIntent(intent);

  let score = 0;
  if (/(contact|about|team|leadership|management|staff|company|our-story|who-we-are|founder|owner|meet-the|people|about-us)/i.test(lower)) {
    score += 6;
  }
  if (/(email|support|help|faq|location|locations|office)/i.test(lower)) {
    score += 4;
  }
  if (inx.needsEmail && /(contact|support|help|customer|service|email|privacy|terms|legal)/i.test(lower)) {
    score += 5;
  }
  if (inx.needsOwner && /(team|about|leadership|management|staff|founder|owner|who-we-are|our-story)/i.test(lower)) {
    score += 6;
  }
  if (inx.needsPhone && /(contact|location|locations|office|call)/i.test(lower)) {
    score += 4;
  }
  if (/\/blog(\/|$)|\/news(\/|$)|\/article(s)?(\/|$)|\/press(\/|$)/i.test(lower)) {
    score -= 7;
  }
  if (/\/product(s)?(\/|$)|\/shop(\/|$)|\/store(\/|$)|\/catalog(\/|$)|\/category(\/|$)/i.test(lower)) {
    score -= 6;
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

  if (/(^|\/)(blog|blogs|news|articles|posts|insights|press)(\/|$)/i.test(lowerPath)) {
    return true;
  }

  if (/(^|\/)(wp-admin|wp-login|login|signin|sign-in|signout|sign-out|logout|checkout|cart|basket|account)(\/|$)/i.test(lowerPath)) {
    return true;
  }

  return false;
}

function createScanTab(url, visible) {
  return new Promise((resolve, reject) => {
    // Keep browser control with the user: scan tabs should never force focus.
    if (visible === true) {
      chrome.tabs.create({ url, active: false }, (tab) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message || "Failed to open website tab"));
          return;
        }
        resolve(tab);
      });
      return;
    }

    createHiddenScanTab(url)
      .then(resolve)
      .catch((error) => {
        reject(error);
      });
  });
}

function createHiddenScanTab(url) {
  return new Promise((resolve, reject) => {
    const openMinimizedPopup = () => {
      chrome.windows.create(
        {
          url,
          focused: false,
          state: "minimized",
          type: "popup"
        },
        (windowRef) => {
          if (chrome.runtime.lastError) {
            openBackgroundWindow();
            return;
          }
          const tab = windowRef && Array.isArray(windowRef.tabs) ? windowRef.tabs[0] : null;
          if (!tab) {
            reject(new Error("Failed to open hidden website tab"));
            return;
          }
          resolve(tab);
        }
      );
    };

    const openBackgroundWindow = () => {
      chrome.windows.create(
        {
          url,
          focused: false,
          state: "minimized",
          type: "normal"
        },
        (windowRef) => {
          if (chrome.runtime.lastError) {
            reject(new Error(chrome.runtime.lastError.message || "Failed to open hidden website tab"));
            return;
          }

          const tab = windowRef && Array.isArray(windowRef.tabs) ? windowRef.tabs[0] : null;
          const windowId = Number(windowRef && windowRef.id);
          if (Number.isFinite(windowId)) {
            chrome.windows.update(windowId, { state: "minimized", focused: false }, () => {});
          }

          if (!tab) {
            reject(new Error("Failed to open hidden website tab"));
            return;
          }
          resolve(tab);
        }
      );
    };

    openMinimizedPopup();
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

  const isLikelyPhoneDigits = (digits) => {
    const compact = normalize(digits).replace(/\D/g, "");
    return compact.length >= 10 && compact.length <= 15;
  };

  const normalizePhone = (value) => {
    const raw = normalize(value);
    if (!raw) return "";
    const withoutPrefixNoise = raw.replace(/^[^\d+()]+/, "");
    const matched = withoutPrefixNoise.match(/(?:\+?\d[\d().\s-]{7,}\d)/);
    const candidate = normalize(matched ? matched[0] : withoutPrefixNoise).replace(/[^\d+().\s-]/g, " ");
    const compact = normalize(candidate);
    const digits = compact.replace(/\D/g, "");
    if (!isLikelyPhoneDigits(digits)) return "";
    return compact;
  };

  const decodeHtmlEntities = (value) => {
    const raw = normalize(value);
    if (!raw) return "";
    const textarea = document.createElement("textarea");
    textarea.innerHTML = raw;
    return normalize(textarea.value || "");
  };

  const decodeEscapedText = (value) => {
    let out = normalize(value);
    if (!out) return "";
    out = out
      .replace(/\\x40/gi, "@")
      .replace(/\\u0040/gi, "@")
      .replace(/\\x2e/gi, ".")
      .replace(/\\u002e/gi, ".");
    return normalize(out);
  };

  const deobfuscateEmailText = (value) => {
    let text = decodeHtmlEntities(decodeEscapedText(value));
    if (!text) return "";
    text = text
      .replace(/\s*(?:\(|\[|\{)?\s*(?:at|where)\s*(?:\)|\]|\})\s*/gi, "@")
      .replace(/\s*(?:\(|\[|\{)?\s*(?:dot|dt)\s*(?:\)|\]|\})\s*/gi, ".")
      .replace(/\s+at\s+/gi, "@")
      .replace(/\s+dot\s+/gi, ".");
    return normalize(text);
  };

  const collectEmailsFromText = (text, target) => {
    const value = deobfuscateEmailText(text);
    if (!value) return;
    const matches = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
    for (const match of matches) {
      const email = normalize(match).toLowerCase();
      if (isLikelyEmail(email)) target.add(email);
    }
  };

  const decodeCloudflareEmail = (encoded) => {
    const raw = normalize(encoded);
    if (!/^[0-9a-f]+$/i.test(raw) || raw.length < 4) return "";
    try {
      const key = parseInt(raw.slice(0, 2), 16);
      let out = "";
      for (let i = 2; i < raw.length; i += 2) {
        const value = parseInt(raw.slice(i, i + 2), 16) ^ key;
        out += String.fromCharCode(value);
      }
      return normalize(out).toLowerCase();
    } catch (_error) {
      return "";
    }
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

  const detectPlatform = () => {
    const html = normalize(document.documentElement ? document.documentElement.innerHTML.slice(0, 120000) : "").toLowerCase();
    const host = normalize(window.location.hostname || "").toLowerCase();
    if (/wp-content|wordpress|wp-includes/.test(html)) return "wordpress";
    if (/wixstatic|_wixcss|wix-code|wix-site/.test(html) || host.includes("wixsite.com")) return "wix";
    if (/squarespace|sqs-/.test(html) || host.includes("squarespace.com")) return "squarespace";
    if (/webflow|w-webflow/.test(html) || host.includes("webflow.io")) return "webflow";
    return "";
  };

  const collectPlatformNodes = (platform) => {
    const selectorsByPlatform = {
      wordpress: [".site-footer", "footer", ".elementor-widget", ".wp-block-group", ".wp-block-columns", "#colophon", ".contact", ".team"],
      wix: ["footer", "[data-testid*='footer']", "[data-testid*='contact']", "[id*='comp-']", "[class*='contact']"],
      squarespace: ["footer", ".sqs-block-form", "[data-section-type]", ".summary-item", "[class*='contact']"],
      webflow: ["footer", ".w-form", "[class*='contact']", "[class*='team']", "[class*='footer']"]
    };
    const selectors = selectorsByPlatform[platform] || ["footer", "[class*='contact']", "[class*='team']"];
    const out = [];
    for (const selector of selectors) {
      const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 30);
      for (const node of nodes) {
        out.push(node);
      }
    }
    return out;
  };

  const rawBodyText = normalize(document.body ? document.body.innerText || "" : "");
  const bodyText = deobfuscateEmailText(rawBodyText);
  const emails = new Set();
  const phones = new Set();
  const ownerCandidates = [];
  const hostname = normalize(window.location.hostname || "").toLowerCase();
  const platform = detectPlatform();

  collectEmailsFromText(rawBodyText, emails);
  collectEmailsFromText(bodyText, emails);

  const scriptBlob = Array.from(document.scripts || [])
    .slice(0, 80)
    .map((script) => normalize(script.textContent || ""))
    .join(" ");
  collectEmailsFromText(scriptBlob, emails);

  const cfNodes = Array.from(document.querySelectorAll("[data-cfemail]")).slice(0, 40);
  for (const node of cfNodes) {
    const encoded = normalize(node.getAttribute("data-cfemail") || "");
    const decoded = decodeCloudflareEmail(encoded);
    if (decoded && isLikelyEmail(decoded)) {
      emails.add(decoded);
    }
  }

  const phoneMatches = bodyText.match(/(?:\+?\d[\d().\s-]{7,}\d)/g) || [];
  for (const value of phoneMatches) {
    const phone = normalizePhone(value);
    if (phone) phones.add(phone);
  }

  const platformNodes = collectPlatformNodes(platform);
  for (const node of platformNodes) {
    const text = normalize(node && node.textContent ? node.textContent : "");
    if (!text) continue;
    collectEmailsFromText(text, emails);
    const localPhones = text.match(/(?:\+?\d[\d().\s-]{7,}\d)/g) || [];
    for (const phoneValue of localPhones) {
      const phone = normalizePhone(phoneValue);
      if (phone) phones.add(phone);
    }
    const ownerHit = parseOwnerCandidate(text);
    if (ownerHit) {
      ownerCandidates.push({
        name: ownerHit.name,
        title: ownerHit.title,
        score: 3,
        source: `${platform || "platform"}_section`
      });
    }
  }

  if (hostname.includes("facebook.com")) {
    const fbMain = document.querySelector("[role='main']");
    const fbPrimaryText = normalize((fbMain && fbMain.innerText) || document.title || "");
    collectEmailsFromText(fbPrimaryText, emails);

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
      collectEmailsFromText(aria, emails);
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
        ownerCandidates.push({ name: nameValue, title: jobTitleValue, score: 4, source: "jsonld" });
      } else if (/founder|ceo|president|owner|principal|director|manager/.test(typeValue)) {
        ownerCandidates.push({ name: nameValue, title: normalize(typeValue), score: 3, source: "jsonld" });
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
          ownerCandidates.push({ name: founderName, title: "Founder", score: 5, source: "jsonld" });
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
          ownerCandidates.push({ name: employeeName, title: employeeTitle, score: 4, source: "jsonld" });
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

    if (href.toLowerCase().startsWith("tel:")) {
      const extractedPhone = href.replace(/^tel:/i, "").split("?")[0].trim();
      const phone = normalizePhone(extractedPhone);
      if (phone) phones.add(phone);
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

  // Some sites expose socials as raw text/scripts instead of direct anchor tags.
  const socialTextMatches = bodyText.match(/https?:\/\/(?:www\.|m\.)?(?:facebook\.com|instagram\.com|linkedin\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com|threads\.net)\/[^\s"'<>]+/gi) || [];
  for (const rawSocial of socialTextMatches) {
    const candidate = normalize(rawSocial).replace(/[),.;]+$/, "");
    if (candidate) {
      socialLinkSet.add(candidate);
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
    const source = /^h[1-5]$/.test(tagName) ? `${tagName}_heading` : tagName;

    ownerCandidates.push({
      name: parsed.name,
      title: parsed.title,
      score,
      source
    });

    if (ownerCandidates.length >= 30) break;
  }

  const antiBotSignal = `${document.title} ${bodyText.slice(0, 4000)}`.toLowerCase();
  const blocked = /(access denied|forbidden|verify you are human|captcha|attention required|cloudflare|blocked)/i.test(antiBotSignal);
  const hasContactSignals = /(contact|about|team|leadership|owner|founder|email|call|get in touch)/i.test(
    `${document.title} ${window.location.pathname} ${bodyText.slice(0, 5000)}`
  );

  return {
    emails: Array.from(emails).slice(0, 60),
    phones: Array.from(phones).slice(0, 20),
    ownerCandidates,
    relatedLinks: Array.from(relatedLinkSet).slice(0, 50),
    internalLinks: Array.from(internalLinkSet).slice(0, 260),
    socialLinks: Array.from(socialLinkSet).slice(0, 12),
    blocked,
    platform,
    hasContactSignals
  };
}
