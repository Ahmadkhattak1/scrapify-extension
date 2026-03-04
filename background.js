importScripts("shared.js");

const {
  MSG,
  rowsToCsv,
  normalizeText,
  normalizePhoneText,
  normalizeWebsiteUrl,
  normalizeBusinessWebsiteUrl,
  normalizeMapsUrl,
  applyFilters,
  readFilterConfig
} = self.GbpShared;
const SCRAPE_SESSION_KEY = "scrapeSession";
const ENRICH_SESSION_KEY = "enrichSession";
const POPUP_UI_SETTINGS_KEY = "popupUiSettings";
const ACTIVE_SCRAPE_FILTERS_KEY = "activeScrapeFilters";
const ENRICHMENT_SETTINGS_KEYS = [
  "enrichmentEnabled",
  "siteMaxPagesValue",
  "showEnrichmentTabsEnabled",
  "scanSocialLinksEnabled",
  "leadDiscoveryEnabled",
  "discoveryGoogleEnabled"
];
const RESULTS_PAGE_PATH = "results.html";
const CONTROL_PANEL_PATH = "popup.html";
const CONTROL_PANEL_ANCHOR_WINDOW_KEY = "controlPanelAnchorWindowId";
const CONTROL_PANEL_WINDOW_WIDTH = 420;
const CONTROL_PANEL_WINDOW_HEIGHT = 760;
let lastEnrichPersistAtMs = 0;
let activeEnrichRun = null;
const autoOpenedResultsRunIds = new Set();
let lastAutoEnrichSourceRunId = "";
const ACTION_DEFAULT_TITLE = "GBP Maps Scraper";
const ACTION_RUNNING_COLOR = "#127a3e";
const ACTION_STOPPING_COLOR = "#b54708";

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

function normalizeSessionStatus(value) {
  const status = normalizeText(value).toLowerCase();
  if (!status) return "idle";
  return status;
}

function isCurrentContextIncognito() {
  try {
    return chrome.extension && chrome.extension.inIncognitoContext === true;
  } catch (_error) {
    return false;
  }
}

function tabMatchesCurrentContext(tab) {
  if (!tab || typeof tab !== "object") return false;
  if (typeof tab.incognito !== "boolean") return true;
  return tab.incognito === isCurrentContextIncognito();
}

function isActiveStatus(status) {
  const value = normalizeSessionStatus(status);
  return value === "running" || value === "stopping" || value === "queued";
}

function isStoppingStatus(status) {
  return normalizeSessionStatus(status) === "stopping";
}

function getBadgeSnapshot(scrapeSession, enrichSession) {
  const scrapeStatus = normalizeSessionStatus(scrapeSession && scrapeSession.status);
  const enrichStatus = normalizeSessionStatus(enrichSession && enrichSession.status);
  const enrichRuntimeStatus = activeEnrichRun
    ? activeEnrichRun.stopRequested === true ? "stopping" : "running"
    : "idle";

  const runningScrape = isActiveStatus(scrapeStatus);
  const runningEnrich = isActiveStatus(enrichStatus) || isActiveStatus(enrichRuntimeStatus);
  const anyRunning = runningScrape || runningEnrich;
  const anyStopping = isStoppingStatus(scrapeStatus) || isStoppingStatus(enrichStatus) || isStoppingStatus(enrichRuntimeStatus);

  let title = ACTION_DEFAULT_TITLE;
  if (anyRunning) {
    const parts = [];
    if (runningScrape) parts.push("scrape");
    if (runningEnrich) parts.push("enrichment");
    const phase = anyStopping ? "stopping" : "running";
    title = `${ACTION_DEFAULT_TITLE} (${parts.join(" + ")} ${phase})`;
  }

  return {
    anyRunning,
    anyStopping,
    title
  };
}

function applyActionBadgeState(state) {
  const snapshot = state && typeof state === "object" ? state : {};
  const anyRunning = snapshot.anyRunning === true;
  const anyStopping = snapshot.anyStopping === true;
  const title = normalizeText(snapshot.title) || ACTION_DEFAULT_TITLE;

  try {
    chrome.action.setTitle({ title });
    if (!anyRunning) {
      chrome.action.setBadgeText({ text: "" });
      return;
    }

    chrome.action.setBadgeText({ text: anyStopping ? "STP" : "RUN" });
    chrome.action.setBadgeBackgroundColor({ color: anyStopping ? ACTION_STOPPING_COLOR : ACTION_RUNNING_COLOR });
  } catch (_error) {
    // Badge updates are best-effort.
  }
}

async function refreshActionBadge() {
  const data = await storageGet([SCRAPE_SESSION_KEY, ENRICH_SESSION_KEY]).catch(() => ({}));
  const scrapeSession = data[SCRAPE_SESSION_KEY] && typeof data[SCRAPE_SESSION_KEY] === "object" ? data[SCRAPE_SESSION_KEY] : null;
  const enrichSession = data[ENRICH_SESSION_KEY] && typeof data[ENRICH_SESSION_KEY] === "object" ? data[ENRICH_SESSION_KEY] : null;
  applyActionBadgeState(getBadgeSnapshot(scrapeSession, enrichSession));
}

function setControlPanelAnchorWindowId(windowId) {
  const normalized = Number(windowId);
  if (!Number.isFinite(normalized) || normalized < 0) {
    return;
  }
  storageSet({
    [CONTROL_PANEL_ANCHOR_WINDOW_KEY]: normalized
  }).catch(() => {});
}

function openControlPanelWindow(url, anchorWindowId) {
  const createOptions = {
    url,
    type: "popup",
    focused: true,
    width: CONTROL_PANEL_WINDOW_WIDTH,
    height: CONTROL_PANEL_WINDOW_HEIGHT,
    incognito: isCurrentContextIncognito()
  };

  const normalizedAnchorId = Number(anchorWindowId);
  if (!Number.isFinite(normalizedAnchorId) || normalizedAnchorId < 0) {
    chrome.windows.create(createOptions, () => {});
    return;
  }

  chrome.windows.get(normalizedAnchorId, {}, (anchorWindow) => {
    if (!chrome.runtime.lastError && anchorWindow && anchorWindow.type === "normal") {
      if (typeof anchorWindow.incognito === "boolean") {
        createOptions.incognito = anchorWindow.incognito;
      }
      const anchorLeft = Number(anchorWindow.left);
      const anchorTop = Number(anchorWindow.top);
      const anchorWidth = Number(anchorWindow.width);
      if (Number.isFinite(anchorLeft) && Number.isFinite(anchorWidth)) {
        createOptions.left = Math.max(0, anchorLeft + anchorWidth - CONTROL_PANEL_WINDOW_WIDTH - 24);
      }
      if (Number.isFinite(anchorTop)) {
        createOptions.top = Math.max(0, anchorTop + 60);
      }
    }
    chrome.windows.create(createOptions, () => {});
  });
}

function openOrFocusControlPanel(anchorWindowId) {
  const controlPanelUrl = chrome.runtime.getURL(CONTROL_PANEL_PATH);
  const normalizedAnchorId = Number(anchorWindowId);
  if (Number.isFinite(normalizedAnchorId) && normalizedAnchorId >= 0) {
    setControlPanelAnchorWindowId(normalizedAnchorId);
  }

  chrome.tabs.query({ url: `${controlPanelUrl}*` }, (tabs) => {
    if (chrome.runtime.lastError) {
      openControlPanelWindow(controlPanelUrl, normalizedAnchorId);
      return;
    }

    const existing = Array.isArray(tabs)
      ? tabs.find((tab) => tab && tab.id && tabMatchesCurrentContext(tab))
      : null;
    if (!existing || !existing.id) {
      openControlPanelWindow(controlPanelUrl, normalizedAnchorId);
      return;
    }

    chrome.tabs.update(existing.id, { active: true }, () => {});
    const existingWindowId = Number(existing.windowId);
    if (Number.isFinite(existingWindowId) && existingWindowId >= 0) {
      chrome.windows.update(existingWindowId, { focused: true }, () => {});
    }
  });
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

chrome.storage.onChanged.addListener((changes, areaName) => {
  if (areaName !== "local" || !changes) return;
  if (changes[SCRAPE_SESSION_KEY] || changes[ENRICH_SESSION_KEY]) {
    void refreshActionBadge();
  }
});

chrome.runtime.onInstalled.addListener(() => {
  void refreshActionBadge();
});

chrome.runtime.onStartup.addListener(() => {
  void refreshActionBadge();
});

chrome.action.onClicked.addListener((tab) => {
  const anchorWindowId = Number(tab && tab.windowId);
  openOrFocusControlPanel(Number.isFinite(anchorWindowId) ? anchorWindowId : null);
});

chrome.windows.onFocusChanged.addListener((windowId) => {
  const focusedWindowId = Number(windowId);
  if (!Number.isFinite(focusedWindowId) || focusedWindowId < 0) {
    return;
  }

  chrome.windows.get(focusedWindowId, {}, (windowRef) => {
    if (chrome.runtime.lastError || !windowRef) {
      return;
    }
    if (windowRef.type === "normal") {
      setControlPanelAnchorWindowId(focusedWindowId);
    }
  });
});

void refreshActionBadge();

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
  applyActionBadgeState({
    anyRunning: true,
    anyStopping: false,
    title: `${ACTION_DEFAULT_TITLE} (enrichment running)`
  });

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
      discovery_attempted: 0,
      discovery_website_recovered: 0,
      discovery_email_recovered: 0,
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
    void refreshActionBadge();
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
  applyActionBadgeState({
    anyRunning: true,
    anyStopping: true,
    title: `${ACTION_DEFAULT_TITLE} (enrichment stopping)`
  });
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
  const filters = message && typeof message.filters === "object" ? message.filters : {};
  const scrapeStopped = summary.stopped === true;

  void handlePostScrape(runId, rows, { scrapeStopped, filters });
}

async function handlePostScrape(runId, rowsInput, metaInput) {
  const meta = metaInput && typeof metaInput === "object" ? metaInput : {};
  const scrapeStopped = meta.scrapeStopped === true;
  const rawRows = prepareRowsForEnrichment(rowsInput, "not_requested");
  const filters = await resolveScrapeFilters(runId, meta.filters);
  const rows = applyScrapeFilters(rawRows, filters);
  await syncScrapeSessionFiltersAndCounts(runId, filters, rows.length).catch(() => {});
  const settings = await readEnrichmentSettings().catch(() => ({
    enrichmentEnabled: false,
    maxPagesPerSite: 12,
    visibleTabs: false,
    scanSocialLinks: true,
    leadDiscoveryEnabled: false,
    discoverySources: {
      google: true
    }
  }));

  if (!settings.enrichmentEnabled || scrapeStopped) {
    await storageSet({ lastRows: rows }).catch(() => {});
    maybeAutoOpenResultsForRun(runId, scrapeStopped ? { force: true } : {});
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
        discovery_attempted: 0,
        discovery_website_recovered: 0,
        discovery_email_recovered: 0,
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
      discovery_attempted: 0,
      discovery_website_recovered: 0,
      discovery_email_recovered: 0,
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
      maxSocialPages: 2,
      leadDiscoveryEnabled: settings.leadDiscoveryEnabled === true,
      discoverySources: settings.discoverySources || { google: true, linkedin: false, yelp: false },
      discoveryTrigger: "missing_website_or_missing_email",
      discoveryBudget: {
        googleQueries: 2,
        googlePages: 3,
        linkedinPages: 0,
        yelpPages: 0
      }
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
    maxPagesPerSite: clampInt(data.siteMaxPagesValue, 1, 120, 12),
    visibleTabs: data.showEnrichmentTabsEnabled === true,
    scanSocialLinks: data.scanSocialLinksEnabled !== false,
    leadDiscoveryEnabled: data.leadDiscoveryEnabled === true,
    discoverySources: {
      google: data.discoveryGoogleEnabled !== false,
      linkedin: false,
      yelp: false
    }
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
    const discoveredWebsite = normalizeBusinessWebsiteUrl(sourceRow.discovered_website);
    const ownerName = normalizeText(sourceRow.owner_name);
    const ownerContext = {
      businessName: normalizeText(sourceRow.name),
      businessCategory: normalizeText(sourceRow.category)
    };
    const safeOwnerName = isLikelyPersonName(ownerName, ownerContext) ? ownerName : "";
    const safeOwnerTitle = safeOwnerName ? normalizeText(sourceRow.owner_title) : "";
    const safeOwnerConfidence = safeOwnerName ? normalizeText(sourceRow.owner_confidence) : "";
    const ownerEmail = normalizeEmail(sourceRow.owner_email);
    const contactEmail = normalizeEmail(sourceRow.contact_email);
    const primaryEmail = normalizeEmail(sourceRow.primary_email);
    const email = normalizeEmail(sourceRow.email) || primaryEmail || ownerEmail || contactEmail;

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
      owner_name: safeOwnerName,
      owner_title: safeOwnerTitle,
      email,
      owner_email: ownerEmail,
      contact_email: contactEmail,
      primary_email: primaryEmail,
      primary_email_type: primaryEmail ? normalizeText(sourceRow.primary_email_type) : "",
      primary_email_source: primaryEmail ? normalizeText(sourceRow.primary_email_source) : "",
      owner_confidence: safeOwnerConfidence,
      email_confidence: primaryEmail ? normalizeText(sourceRow.email_confidence) : "",
      email_source_url: primaryEmail ? normalizeText(sourceRow.email_source_url) : "",
      no_email_reason: normalizeText(sourceRow.no_email_reason),
      website_scan_status: normalizeText(nextStatus),
      site_pages_visited: Number.isFinite(sitePagesVisited) ? sitePagesVisited : 0,
      site_pages_discovered: Number.isFinite(sitePagesDiscovered) ? sitePagesDiscovered : 0,
      social_pages_scanned: Number.isFinite(socialPagesScanned) ? socialPagesScanned : 0,
      social_links: normalizeText(sourceRow.social_links),
      discovery_status: normalizeText(sourceRow.discovery_status) || "not_requested",
      discovery_source: normalizeText(sourceRow.discovery_source),
      discovery_query: normalizeText(sourceRow.discovery_query),
      discovered_website: discoveredWebsite
    };
  });
}

function createEnrichedRowFromSource(sourceRow) {
  const base = sourceRow && typeof sourceRow === "object" ? sourceRow : {};
  const rawWebsitePhone = sanitizePhoneText(base.website_phone);
  const rawFallbackPhone = sanitizePhoneText(base.phone);
  const rawListingPhone = sanitizePhoneText(
    base.listing_phone || (rawWebsitePhone && rawFallbackPhone === rawWebsitePhone ? "" : rawFallbackPhone)
  );
  const ownerEmail = normalizeEmail(base.owner_email);
  const contactEmail = normalizeEmail(base.contact_email);
  const primaryEmail = normalizeEmail(base.primary_email);
  const email = normalizeEmail(base.email) || primaryEmail || ownerEmail || contactEmail;
  const ownerName = normalizeText(base.owner_name);
  const ownerContext = {
    businessName: normalizeText(base.name),
    businessCategory: normalizeText(base.category)
  };
  const safeOwnerName = isLikelyPersonName(ownerName, ownerContext) ? ownerName : "";
  const safeOwnerTitle = safeOwnerName ? normalizeText(base.owner_title) : "";
  const safeOwnerConfidence = safeOwnerName ? normalizeText(base.owner_confidence) : "";

  return {
    ...base,
    phone: rawFallbackPhone || rawListingPhone,
    listing_phone: rawListingPhone,
    website_phone: rawWebsitePhone,
    website_phone_source: normalizeText(base.website_phone_source),
    owner_name: safeOwnerName,
    owner_title: safeOwnerTitle,
    email,
    owner_email: ownerEmail,
    contact_email: contactEmail,
    primary_email: primaryEmail,
    primary_email_type: primaryEmail ? normalizeText(base.primary_email_type) : "",
    primary_email_source: primaryEmail ? normalizeText(base.primary_email_source) : "",
    owner_confidence: safeOwnerConfidence,
    email_confidence: primaryEmail ? normalizeText(base.email_confidence) : "",
    email_source_url: primaryEmail ? normalizeText(base.email_source_url) : "",
    no_email_reason: normalizeText(base.no_email_reason),
    website_scan_status: normalizeText(base.website_scan_status),
    site_pages_visited: Number(base.site_pages_visited || 0),
    site_pages_discovered: Number(base.site_pages_discovered || 0),
    social_pages_scanned: Number(base.social_pages_scanned || 0),
    social_links: normalizeText(base.social_links),
    discovery_status: normalizeText(base.discovery_status) || "not_requested",
    discovery_source: normalizeText(base.discovery_source),
    discovery_query: normalizeText(base.discovery_query),
    discovered_website: normalizeBusinessWebsiteUrl(base.discovered_website)
  };
}

function rowHasAnyEmail(row) {
  const value = row && typeof row === "object" ? row : {};
  return Boolean(
    normalizeEmail(value.primary_email) ||
      normalizeEmail(value.owner_email) ||
      normalizeEmail(value.contact_email) ||
      normalizeEmail(value.email)
  );
}

function scanHasAnyEmail(scan) {
  const value = scan && typeof scan === "object" ? scan : {};
  return Boolean(normalizeEmail(value.primaryEmail) || normalizeEmail(value.ownerEmail) || normalizeEmail(value.contactEmail));
}

function applyScanResultToRow(row, scan, options) {
  const target = row && typeof row === "object" ? row : {};
  const result = scan && typeof scan === "object" ? scan : {};
  const opts = options && typeof options === "object" ? options : {};
  const overwrite = opts.overwrite !== false;
  const overwriteWithoutEmail = opts.overwriteWithoutEmail === true;
  const hasEmailInScan = scanHasAnyEmail(result);
  const allowOverwrite = overwrite && (hasEmailInScan || overwriteWithoutEmail);

  const assign = (key, value, force) => {
    const normalized = normalizeText(value);
    if (!normalized && !force) return;
    if (force || allowOverwrite || !normalizeText(target[key])) {
      target[key] = normalized;
    }
  };
  const assignEmail = (key, value, force) => {
    const normalized = normalizeEmail(value);
    if (!normalized && !force) return;
    if (force || allowOverwrite || !normalizeEmail(target[key])) {
      target[key] = normalized;
    }
  };

  assign("owner_name", result.ownerName, false);
  assign("owner_title", result.ownerTitle, false);
  assign("owner_confidence", result.ownerConfidence, false);
  assignEmail("owner_email", result.ownerEmail, false);
  assignEmail("contact_email", result.contactEmail, false);
  assignEmail("primary_email", result.primaryEmail, false);
  assign("primary_email_type", result.primaryEmailType, false);
  assign("primary_email_source", result.primaryEmailSource, false);
  assign("email_source_url", result.emailSourceUrl, false);
  assign("email_confidence", result.emailConfidence, allowOverwrite);
  assign("no_email_reason", result.noEmailReason, allowOverwrite);

  const fallbackEmail =
    normalizeEmail(result.primaryEmail) ||
    normalizeEmail(result.ownerEmail) ||
    normalizeEmail(result.contactEmail) ||
    normalizeEmail(target.primary_email) ||
    normalizeEmail(target.owner_email) ||
    normalizeEmail(target.contact_email) ||
    normalizeEmail(target.email);
  if (allowOverwrite || !normalizeEmail(target.email)) {
    target.email = fallbackEmail;
  }
  target.owner_email = normalizeEmail(target.owner_email);
  target.contact_email = normalizeEmail(target.contact_email);
  target.primary_email = normalizeEmail(target.primary_email);
  target.email = normalizeEmail(target.email) || target.primary_email || target.owner_email || target.contact_email || "";
  if (!target.primary_email) {
    target.primary_email_type = "";
    target.primary_email_source = "";
    target.email_source_url = "";
    target.email_confidence = "";
  }
  const ownerContext = {
    businessName: normalizeText(target.name),
    businessCategory: normalizeText(target.category)
  };
  if (!isLikelyPersonName(target.owner_name, ownerContext)) {
    target.owner_name = "";
    target.owner_title = "";
    target.owner_confidence = "";
    target.owner_email = "";
  }

  const websitePhone = sanitizePhoneText(result.primaryPhone);
  if (websitePhone && (allowOverwrite || !normalizeText(target.website_phone))) {
    target.website_phone = websitePhone;
  }
  if (normalizeText(result.primaryPhoneSource) && (allowOverwrite || !normalizeText(target.website_phone_source))) {
    target.website_phone_source = normalizeText(result.primaryPhoneSource);
  }
  if (!normalizeText(target.phone) && websitePhone) {
    target.phone = websitePhone;
  }

  if (allowOverwrite || !normalizeText(target.website_scan_status)) {
    target.website_scan_status = normalizeText(result.status);
  }
  if (allowOverwrite || !Number(target.site_pages_visited)) {
    target.site_pages_visited = Number(result.pagesVisited || 0);
  }
  if (allowOverwrite || !Number(target.site_pages_discovered)) {
    target.site_pages_discovered = Number(result.pagesDiscovered || 0);
  }
  if (allowOverwrite || !Number(target.social_pages_scanned)) {
    target.social_pages_scanned = Number(result.socialScanned || 0);
  }
  if (allowOverwrite || !normalizeText(target.social_links)) {
    target.social_links = Array.isArray(result.socialLinks) ? result.socialLinks.join(" | ") : normalizeText(target.social_links);
  }
}

function sameWebsiteHost(urlA, urlB) {
  const hostA = hostnameForUrl(urlA);
  const hostB = hostnameForUrl(urlB);
  if (!hostA || !hostB) return false;
  return hostA === hostB;
}

function normalizeWebsiteHostKey(url) {
  const host = normalizeText(hostnameForUrl(url)).toLowerCase();
  if (!host) return "";
  return host.replace(/^www\./, "");
}

function normalizeBusinessNameForWebsiteGuard(name) {
  const stop = new Set(["the", "and", "of", "llc", "inc", "ltd", "co", "company", "services", "service"]);
  return normalizeText(name)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((token) => token.length >= 2 && !stop.has(token))
    .join(" ");
}

function areLikelySameBusinessForWebsite(nameA, nameB) {
  const left = normalizeBusinessNameForWebsiteGuard(nameA);
  const right = normalizeBusinessNameForWebsiteGuard(nameB);
  if (!left || !right) return false;
  if (left === right) return true;

  const leftSet = new Set(left.split(/\s+/));
  const rightSet = new Set(right.split(/\s+/));
  let overlap = 0;
  for (const token of leftSet) {
    if (rightSet.has(token)) overlap += 1;
  }
  const union = new Set([...leftSet, ...rightSet]).size;
  if (union === 0) return false;
  return overlap / union >= 0.75;
}

function registerOrRejectWebsiteForRow(url, row, registryInput) {
  const registry = registryInput instanceof Map ? registryInput : new Map();
  const normalized = normalizeBusinessWebsiteUrl(url);
  if (!normalized) return "";

  const host = normalizeWebsiteHostKey(normalized);
  if (!host) return "";

  const source = row && typeof row === "object" ? row : {};
  const placeId = normalizeText(source.place_id);
  const mapsUrl = normalizeMapsUrl(source.maps_url || "");
  const businessName = normalizeText(source.name);
  const existing = registry.get(host);

  if (!existing) {
    registry.set(host, {
      placeIds: new Set(placeId ? [placeId] : []),
      mapsUrls: new Set(mapsUrl ? [mapsUrl] : []),
      primaryName: businessName
    });
    return normalized;
  }

  const sameIdentity =
    (placeId && existing.placeIds.has(placeId)) ||
    (mapsUrl && existing.mapsUrls.has(mapsUrl)) ||
    (businessName && existing.primaryName && areLikelySameBusinessForWebsite(businessName, existing.primaryName));

  if (!sameIdentity) {
    return "";
  }

  if (placeId) existing.placeIds.add(placeId);
  if (mapsUrl) existing.mapsUrls.add(mapsUrl);
  return normalized;
}

async function enrichRows(rows, options) {
  const config = options && typeof options === "object" ? options : {};
  const maxPagesPerSite = clampInt(config.maxPagesPerSite, 1, 120, 12);
  const timeoutMs = clampInt(config.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = config.visibleTabs === true;
  const scanSocialLinks = config.scanSocialLinks !== false;
  const maxSocialPages = clampInt(config.maxSocialPages, 0, 8, 2);
  const maxDiscoveredPages = clampInt(config.maxDiscoveredPages, maxPagesPerSite, 240, Math.max(80, maxPagesPerSite * 5));
  const discovery = normalizeDiscoveryOptions(config);

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
    discovery_attempted: 0,
    discovery_website_recovered: 0,
    discovery_email_recovered: 0,
    stopped: false
  };
  const websiteHostOwners = new Map();

  const outputRows = [];
  let resumeIndex = rows.length;

  for (let index = 0; index < rows.length; index += 1) {
    if (isEnrichStopRequested(config)) {
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

    const sourceRow = rows[index] || {};
    const enrichedRow = createEnrichedRowFromSource(sourceRow);
    let website = registerOrRejectWebsiteForRow(sourceRow.website, sourceRow, websiteHostOwners);
    enrichedRow.website = website;

    let rowPagesVisited = 0;
    let rowPagesDiscovered = 0;
    let rowSocialScanned = 0;
    let rowBlocked = false;
    let rowCurrentUrl = website;
    let rowDiscoveryEmailRecovered = false;
    let preDiscoveryRan = false;

    const runSiteScan = async (targetUrl, phasePrefix) => {
      const phaseInit = phasePrefix === "discovery" ? "discovery_site_init" : "site_init";
      emitEnrichProgress(summary, {
        currentName: sourceRow.name,
        currentUrl: targetUrl,
        phase: phaseInit,
        sitePagesVisited: summary.pages_visited + rowPagesVisited,
        sitePagesDiscovered: summary.pages_discovered + rowPagesDiscovered,
        socialScanned: summary.social_scanned + rowSocialScanned
      });

      const scan = await scanWebsite(targetUrl, {
        maxPagesPerSite,
        maxDiscoveredPages,
        timeoutMs,
        visibleTabs,
        scanSocialLinks,
        maxSocialPages,
        skipSitemapLookup: phasePrefix === "discovery",
        intent: deriveScanIntent(enrichedRow),
        businessName: sourceRow.name,
        businessCategory: sourceRow.category,
        businessAddress: sourceRow.address || sourceRow.source_query,
        sourceQuery: sourceRow.source_query,
        businessWebsite: targetUrl,
        discoveredWebsite: enrichedRow.discovered_website,
        shouldStop: config.shouldStop,
        onTabChange: config.onScanTabChange,
        onProgress: (scanProgress) => {
          const progress = scanProgress || {};
          const rawPhase = normalizeText(progress.phase || "site_scan");
          const nextPhase = phasePrefix === "discovery" ? `discovery_${rawPhase}` : rawPhase;
          emitEnrichProgress(summary, {
            currentName: sourceRow.name,
            currentUrl: progress.currentUrl || targetUrl,
            phase: nextPhase,
            sitePagesVisited: summary.pages_visited + rowPagesVisited + Number(progress.pagesVisited || 0),
            sitePagesDiscovered: summary.pages_discovered + rowPagesDiscovered + Number(progress.pagesDiscovered || 0),
            socialScanned: summary.social_scanned + rowSocialScanned + Number(progress.socialScanned || 0)
          });
        }
      });

      rowBlocked = rowBlocked || scan.blocked === true;
      rowPagesVisited += Number(scan.pagesVisited || 0);
      rowPagesDiscovered += Number(scan.pagesDiscovered || 0);
      rowSocialScanned += Number(scan.socialScanned || 0);
      rowCurrentUrl = targetUrl;
      return scan;
    };

    try {
      if (!website && discovery.enabled) {
        preDiscoveryRan = true;
        const discoveryResult = await runLeadDiscovery(enrichedRow, {
          ...discovery,
          timeoutMs,
          visibleTabs,
          shouldStop: config.shouldStop,
          onScanTabChange: config.onScanTabChange
        });
        if (discoveryResult.attempted) {
          summary.discovery_attempted += 1;
        }
        if (discoveryResult.discoveredWebsite) {
          summary.discovery_website_recovered += 1;
        }
        applyDiscoveryResultToRow(enrichedRow, discoveryResult);
        if (!website && discoveryResult.discoveredWebsite) {
          const recoveredWebsite = registerOrRejectWebsiteForRow(discoveryResult.discoveredWebsite, enrichedRow, websiteHostOwners);
          website = recoveredWebsite;
          enrichedRow.website = website;
          if (!recoveredWebsite) {
            enrichedRow.discovered_website = "";
            if (normalizeText(enrichedRow.discovery_status).toLowerCase() === "recovered_website") {
              enrichedRow.discovery_status = "no_match";
            }
          }
        }
      }

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
          currentUrl: rowCurrentUrl,
          phase: "skip",
          leadSignalText: "Skipped: no website",
          leadSignalTone: "warn",
          sitePagesVisited: summary.pages_visited,
          sitePagesDiscovered: summary.pages_discovered
        });
        continue;
      }

      const firstScan = await runSiteScan(website, "site");
      applyScanResultToRow(enrichedRow, firstScan, {
        overwrite: true,
        overwriteWithoutEmail: true
      });
      if (
        preDiscoveryRan &&
        rowHasAnyEmail(enrichedRow) &&
        normalizeText(enrichedRow.discovery_status).toLowerCase() === "recovered_website"
      ) {
        rowDiscoveryEmailRecovered = true;
        enrichedRow.discovery_status = "recovered_email";
      }

      if (discovery.enabled && !rowHasAnyEmail(enrichedRow) && !preDiscoveryRan) {
        const discoveryResult = await runLeadDiscovery(enrichedRow, {
          ...discovery,
          timeoutMs,
          visibleTabs,
          shouldStop: config.shouldStop,
          onScanTabChange: config.onScanTabChange,
          existingWebsite: website
        });
        if (discoveryResult.attempted) {
          summary.discovery_attempted += 1;
        }
        if (discoveryResult.discoveredWebsite) {
          summary.discovery_website_recovered += 1;
        }
        applyDiscoveryResultToRow(enrichedRow, discoveryResult);

        const discoveredCandidate = registerOrRejectWebsiteForRow(discoveryResult.discoveredWebsite, enrichedRow, websiteHostOwners);
        if (!discoveredCandidate && normalizeText(discoveryResult.discoveredWebsite)) {
          enrichedRow.discovered_website = "";
          if (normalizeText(enrichedRow.discovery_status).toLowerCase() === "recovered_website") {
            enrichedRow.discovery_status = "no_match";
          }
        }
        if (discoveredCandidate && !sameWebsiteHost(discoveredCandidate, website)) {
          const hadEmailBeforeDiscoveryScan = rowHasAnyEmail(enrichedRow);
          const discoveryScan = await runSiteScan(discoveredCandidate, "discovery");
          const discoveryScanHasEmail = scanHasAnyEmail(discoveryScan);

          applyScanResultToRow(enrichedRow, discoveryScan, {
            overwrite: discoveryScanHasEmail,
            overwriteWithoutEmail: discoveryScanHasEmail
          });

          if (!hadEmailBeforeDiscoveryScan && discoveryScanHasEmail) {
            rowDiscoveryEmailRecovered = true;
            enrichedRow.discovery_status = "recovered_email";
            if (!normalizeText(enrichedRow.discovery_source)) {
              enrichedRow.discovery_source = normalizeText(discoveryResult.source);
            }
          }
        }
      }

      if (!normalizeText(enrichedRow.owner_name) && normalizeText(enrichedRow.website_scan_status).toLowerCase() !== "blocked") {
        emitEnrichProgress(summary, {
          currentName: sourceRow.name,
          currentUrl: rowCurrentUrl,
          phase: "owner_lookup",
          sitePagesVisited: summary.pages_visited + rowPagesVisited,
          sitePagesDiscovered: summary.pages_discovered + rowPagesDiscovered,
          socialScanned: summary.social_scanned + rowSocialScanned
        });
        const ownerLookup = await recoverOwnerViaGoogle(
          {
            ...enrichedRow,
            name: normalizeText(sourceRow.name) || normalizeText(enrichedRow.name),
            address: normalizeText(sourceRow.address) || normalizeText(enrichedRow.address),
            source_query: normalizeText(sourceRow.source_query) || normalizeText(enrichedRow.source_query),
            category: normalizeText(sourceRow.category) || normalizeText(enrichedRow.category)
          },
          {
            timeoutMs,
            visibleTabs,
            shouldStop: config.shouldStop,
            onScanTabChange: config.onScanTabChange
          }
        );
        if (ownerLookup && ownerLookup.found) {
          const lookupName = normalizeText(ownerLookup.ownerName);
          const ownerContext = {
            businessName: normalizeText(enrichedRow.name),
            businessCategory: normalizeText(enrichedRow.category)
          };
          if (isLikelyPersonName(lookupName, ownerContext) && Number(ownerLookup.ownerConfidence) >= 0.86) {
            enrichedRow.owner_name = lookupName;
            enrichedRow.owner_title = normalizeText(ownerLookup.ownerTitle);
            enrichedRow.owner_confidence = formatConfidence(ownerLookup.ownerConfidence);
          }
        }
      }

      if (rowBlocked) {
        summary.blocked += 1;
      }
      summary.social_scanned += rowSocialScanned;
      summary.pages_visited += rowPagesVisited;
      summary.pages_discovered += rowPagesDiscovered;

      if (rowDiscoveryEmailRecovered) {
        summary.discovery_email_recovered += 1;
      }

      const emailType = normalizeText(enrichedRow.primary_email_type).toLowerCase();
      if (rowHasAnyEmail(enrichedRow)) {
        if (emailType === "personal") {
          summary.personal_email_found += 1;
        } else if (emailType === "company") {
          summary.company_email_found += 1;
        }
      }

      const finalStatus = normalizeText(enrichedRow.website_scan_status).toLowerCase();
      if (finalStatus === "enriched") {
        summary.enriched += 1;
      } else {
        summary.skipped += 1;
      }
    } catch (rowError) {
      if (isEnrichStopError(rowError) || isEnrichStopRequested(config)) {
        summary.stopped = true;
        resumeIndex = index + 1;
        outputRows.push(enrichedRow);
        emitEnrichProgress(summary, {
          currentName: sourceRow.name,
          currentUrl: rowCurrentUrl,
          phase: "stopped",
          leadSignalText: "Enrichment stopped by user",
          leadSignalTone: "warn",
          sitePagesVisited: summary.pages_visited + rowPagesVisited,
          sitePagesDiscovered: summary.pages_discovered + rowPagesDiscovered
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
      summary.social_scanned += rowSocialScanned;
      summary.pages_visited += rowPagesVisited;
      summary.pages_discovered += rowPagesDiscovered;
      if (rowBlocked) {
        summary.blocked += 1;
      }
    }

    summary.processed += 1;
    outputRows.push(enrichedRow);
    const leadSignal = buildLeadSignal(enrichedRow);
    emitEnrichProgress(summary, {
      currentName: sourceRow.name,
      currentUrl: rowCurrentUrl,
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
    discovery_attempted: Number(summary.discovery_attempted || 0),
    discovery_website_recovered: Number(summary.discovery_website_recovered || 0),
    discovery_email_recovered: Number(summary.discovery_email_recovered || 0),
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
  const discoveryStatus = normalizeText(row && row.discovery_status).toLowerCase();
  const discoverySource = normalizeText(row && row.discovery_source);

  if (primaryEmail) {
    if (discoveryStatus === "recovered_email") {
      return {
        text: `Discovery recovered email (${sourceLabel(discoverySource)})`,
        tone: "success"
      };
    }
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

  if (discoveryStatus === "recovered_website") {
    return { text: `Recovered website (${sourceLabel(discoverySource)})`, tone: "info" };
  }

  return { text: "Skipped: no public email found", tone: "warn" };
}

function sourceLabel(source) {
  const value = normalizeText(source).toLowerCase();
  if (!value) return "website";
  if (value === "google") return "Google Search";
  return value;
}

function normalizeDiscoveryOptions(options) {
  const raw = options && typeof options === "object" ? options : {};
  const sourcesRaw = raw.discoverySources && typeof raw.discoverySources === "object" ? raw.discoverySources : {};
  const budgetRaw = raw.discoveryBudget && typeof raw.discoveryBudget === "object" ? raw.discoveryBudget : {};

  return {
    enabled: raw.leadDiscoveryEnabled === true,
    sources: {
      google: sourcesRaw.google !== false,
      linkedin: false,
      yelp: false
    },
    trigger: normalizeText(raw.discoveryTrigger || "missing_website_or_missing_email"),
    budget: {
      googleQueries: clampInt(budgetRaw.googleQueries, 1, 6, 2),
      googlePages: clampInt(budgetRaw.googlePages, 1, 3, 3),
      linkedinPages: 0,
      yelpPages: 0
    }
  };
}

function applyDiscoveryResultToRow(row, result) {
  const target = row && typeof row === "object" ? row : {};
  const discovery = result && typeof result === "object" ? result : {};
  if (discovery.attempted !== true) {
    if (!normalizeText(target.discovery_status)) {
      target.discovery_status = "not_requested";
    }
    return;
  }

  target.discovery_status = normalizeText(discovery.status || "no_match");
  target.discovery_source = normalizeText(discovery.source);
  target.discovery_query = normalizeText(discovery.query);
  if (normalizeText(discovery.discoveredWebsite)) {
    target.discovered_website = normalizeBusinessWebsiteUrl(discovery.discoveredWebsite);
  }
}

async function runLeadDiscovery(row, optionsInput) {
  const rowData = row && typeof row === "object" ? row : {};
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const sources = options.sources && typeof options.sources === "object" ? options.sources : {};
  const budget = options.budget && typeof options.budget === "object" ? options.budget : {};
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;
  const existingWebsite = normalizeBusinessWebsiteUrl(options.existingWebsite || rowData.website);
  const existingHost = hostnameForUrl(existingWebsite);

  if (options.enabled !== true) {
    return {
      attempted: false,
      status: "not_requested",
      source: "",
      query: "",
      discoveredWebsite: ""
    };
  }

  const queries = buildDiscoveryQueries(rowData).slice(0, clampInt(budget.googleQueries, 1, 6, 2));
  if (queries.length === 0) {
    return {
      attempted: true,
      status: "no_match",
      source: "",
      query: "",
      discoveredWebsite: ""
    };
  }

  const searchOptions = {
    timeoutMs,
    visibleTabs,
    maxResults: clampInt(budget.googlePages, 1, 3, 3),
    shouldStop: options.shouldStop,
    onScanTabChange: options.onScanTabChange
  };

  try {
    for (const query of queries) {
      if (isEnrichStopRequested(options)) {
        throw createEnrichStopError();
      }

      if (sources.google !== false) {
        const candidates = await searchGoogleCandidates(query, {
          ...searchOptions,
          siteFilter: "",
          includeDirectoryHosts: false
        });
        const best = pickBestDiscoveryCandidate(candidates, rowData, {
          includeDirectoryHosts: false,
          excludedHost: existingHost
        });
        if (best) {
          return {
            attempted: true,
            status: "recovered_website",
            source: "google",
            query,
            discoveredWebsite: best.url
          };
        }
      }

      if (sources.linkedin !== false) {
        const linkedInWebsite = await discoverPointerWebsite("linkedin", query, rowData, {
          timeoutMs,
          visibleTabs,
          maxPages: clampInt(budget.linkedinPages, 0, 8, 2),
          shouldStop: options.shouldStop,
          onScanTabChange: options.onScanTabChange,
          excludedHost: existingHost
        });
        if (linkedInWebsite) {
          return {
            attempted: true,
            status: "recovered_website",
            source: "linkedin",
            query,
            discoveredWebsite: linkedInWebsite
          };
        }
      }

      if (sources.yelp !== false) {
        const yelpWebsite = await discoverPointerWebsite("yelp", query, rowData, {
          timeoutMs,
          visibleTabs,
          maxPages: clampInt(budget.yelpPages, 0, 8, 2),
          shouldStop: options.shouldStop,
          onScanTabChange: options.onScanTabChange,
          excludedHost: existingHost
        });
        if (yelpWebsite) {
          return {
            attempted: true,
            status: "recovered_website",
            source: "yelp",
            query,
            discoveredWebsite: yelpWebsite
          };
        }
      }
    }

    return {
      attempted: true,
      status: "no_match",
      source: "",
      query: queries[queries.length - 1] || "",
      discoveredWebsite: ""
    };
  } catch (error) {
    if (isEnrichStopError(error)) {
      throw error;
    }

    const message = normalizeText(error && error.message ? error.message : error).toLowerCase();
    const status = /(captcha|verify you are human|access denied|forbidden|blocked|cloudflare)/i.test(message)
      ? "blocked"
      : "error";
    return {
      attempted: true,
      status,
      source: "",
      query: queries[0] || "",
      discoveredWebsite: ""
    };
  }
}

function hasAnyActiveFilter(filters) {
  const f = filters && typeof filters === "object" ? filters : {};
  return (
    f.minRating !== "" ||
    f.maxRating !== "" ||
    f.minReviews !== "" ||
    f.maxReviews !== "" ||
    normalizeText(f.nameKeyword) !== "" ||
    normalizeText(f.categoryInclude) !== "" ||
    normalizeText(f.categoryExclude) !== "" ||
    f.hasWebsite === true ||
    f.hasPhone === true
  );
}

function normalizeScrapeFilters(filtersLike) {
  const source = filtersLike && typeof filtersLike === "object" ? filtersLike : {};
  return readFilterConfig({
    minRating: source.minRating,
    maxRating: source.maxRating,
    minReviews: source.minReviews,
    maxReviews: source.maxReviews,
    nameKeyword: source.nameKeyword,
    categoryInclude: source.categoryInclude,
    categoryExclude: source.categoryExclude,
    hasWebsite: source.hasWebsite === true,
    hasPhone: source.hasPhone === true
  });
}

function applyScrapeFilters(rows, filters) {
  if (!Array.isArray(rows)) return [];
  const normalizedFilters = normalizeScrapeFilters(filters);
  if (!hasAnyActiveFilter(normalizedFilters)) {
    return rows;
  }
  return rows.filter((row) => applyFilters(row, normalizedFilters));
}

async function resolveScrapeFilters(runId, candidateFilters) {
  const direct = normalizeScrapeFilters(candidateFilters);
  if (hasAnyActiveFilter(direct)) {
    return direct;
  }

  const data = await storageGet([SCRAPE_SESSION_KEY, ACTIVE_SCRAPE_FILTERS_KEY, POPUP_UI_SETTINGS_KEY]).catch(() => ({}));
  const session = data[SCRAPE_SESSION_KEY] && typeof data[SCRAPE_SESSION_KEY] === "object" ? data[SCRAPE_SESSION_KEY] : null;
  const active = normalizeScrapeFilters(data[ACTIVE_SCRAPE_FILTERS_KEY]);
  const uiSettings = normalizeScrapeFilters(data[POPUP_UI_SETTINGS_KEY]);
  const targetRunId = normalizeText(runId);

  if (session && normalizeText(session.run_id) === targetRunId) {
    const sessionFilters = normalizeScrapeFilters(session.filters);
    if (hasAnyActiveFilter(sessionFilters)) {
      return sessionFilters;
    }
  }
  if (hasAnyActiveFilter(active)) {
    return active;
  }
  return uiSettings;
}

async function syncScrapeSessionFiltersAndCounts(runId, filters, rowsCount) {
  const data = await storageGet([SCRAPE_SESSION_KEY]).catch(() => ({}));
  const session = data[SCRAPE_SESSION_KEY] && typeof data[SCRAPE_SESSION_KEY] === "object" ? data[SCRAPE_SESSION_KEY] : null;
  if (!session) return;

  const targetRunId = normalizeText(runId);
  if (!targetRunId || normalizeText(session.run_id) !== targetRunId) return;

  const normalizedFilters = normalizeScrapeFilters(filters);
  const next = {
    ...session,
    filters: normalizedFilters,
    rows_count: Number(rowsCount) > 0 ? Number(rowsCount) : 0,
    matched: Number(rowsCount) > 0 ? Number(rowsCount) : 0,
    updated_at: new Date().toISOString()
  };
  await storageSet({
    [SCRAPE_SESSION_KEY]: next
  }).catch(() => {});
}

function buildDiscoveryQueries(row) {
  const source = row && typeof row === "object" ? row : {};
  const name = normalizeText(source.name);
  const address = normalizeText(source.address);
  const sourceQuery = normalizeText(source.source_query);
  const owner = normalizeText(source.owner_name);
  const locationHint = address || sourceQuery;

  if (!name) return [];

  const candidates = [];
  if (locationHint) {
    candidates.push(`${name} ${locationHint} official website`);
    candidates.push(`${name} ${locationHint} contact email`);
  } else {
    candidates.push(`${name} official website`);
    candidates.push(`${name} contact email`);
  }
  if (owner) {
    candidates.push(`${owner} ${name}`);
  }

  const out = [];
  const seen = new Set();
  for (const candidate of candidates) {
    const normalized = normalizeText(candidate);
    if (!normalized || seen.has(normalized.toLowerCase())) continue;
    seen.add(normalized.toLowerCase());
    out.push(normalized);
  }
  return out;
}

function ownerLookupSourceForUrl(url) {
  const host = hostnameForUrl(url);
  if (!host) return "google";
  if (host.includes("linkedin.com")) return "linkedin";
  if (host.includes("yelp.com")) return "yelp";
  return "website";
}

function normalizePersonNameKey(name) {
  return normalizeText(name)
    .toLowerCase()
    .replace(/[^a-z\s]/g, " ")
    .split(/\s+/)
    .filter((token) => token.length >= 2)
    .join(" ");
}

function areLikelySamePersonName(nameA, nameB) {
  const left = normalizePersonNameKey(nameA);
  const right = normalizePersonNameKey(nameB);
  if (!left || !right) return false;
  if (left === right) return true;
  if (left.includes(right) || right.includes(left)) return true;

  const leftSet = new Set(left.split(/\s+/));
  const rightSet = new Set(right.split(/\s+/));
  let overlap = 0;
  for (const token of leftSet) {
    if (rightSet.has(token)) overlap += 1;
  }
  const union = new Set([...leftSet, ...rightSet]).size;
  if (union === 0) return false;
  return overlap / union >= 0.67;
}

const SEMANTIC_STOPWORDS = new Set([
  "a",
  "an",
  "and",
  "are",
  "as",
  "at",
  "be",
  "by",
  "for",
  "from",
  "in",
  "is",
  "it",
  "of",
  "on",
  "or",
  "our",
  "that",
  "the",
  "their",
  "this",
  "to",
  "we",
  "with",
  "you",
  "your",
  "llc",
  "inc",
  "ltd",
  "co",
  "company",
  "group",
  "services",
  "service",
  "official",
  "website",
  "com",
  "net",
  "org"
]);

const LOCATION_NOISE_TOKENS = new Set([
  "street",
  "st",
  "avenue",
  "ave",
  "road",
  "rd",
  "lane",
  "ln",
  "drive",
  "dr",
  "suite",
  "ste",
  "floor",
  "fl",
  "unit",
  "city",
  "county",
  "state",
  "united",
  "states"
]);

function normalizeSemanticToken(token) {
  let value = normalizeText(token).toLowerCase().replace(/[^a-z0-9]/g, "");
  if (!value) return "";
  if (value.length > 4 && value.endsWith("ies")) {
    value = `${value.slice(0, -3)}y`;
  } else if (value.length > 5 && value.endsWith("ing")) {
    value = value.slice(0, -3);
  } else if (value.length > 4 && value.endsWith("ed")) {
    value = value.slice(0, -2);
  } else if (value.length > 4 && value.endsWith("es")) {
    value = value.slice(0, -2);
  } else if (value.length > 3 && value.endsWith("s")) {
    value = value.slice(0, -1);
  }
  return value;
}

function tokenizeSemanticText(value, optionsInput) {
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const minLen = Number.isFinite(Number(options.minLen)) ? Number(options.minLen) : 3;
  const unique = options.unique !== false;
  const keepStopwords = options.keepStopwords === true;
  const text = normalizeText(value).toLowerCase().replace(/[^a-z0-9\s]/g, " ");
  if (!text) return [];

  const out = [];
  const seen = new Set();
  const parts = text.split(/\s+/);
  for (const part of parts) {
    const token = normalizeSemanticToken(part);
    if (!token || token.length < minLen) continue;
    if (!keepStopwords && SEMANTIC_STOPWORDS.has(token)) continue;
    if (unique) {
      if (seen.has(token)) continue;
      seen.add(token);
    }
    out.push(token);
  }
  return out;
}

function tokenOverlapRatio(leftTokens, rightTokens) {
  const leftSet = new Set(Array.isArray(leftTokens) ? leftTokens : []);
  const rightSet = new Set(Array.isArray(rightTokens) ? rightTokens : []);
  if (leftSet.size === 0 || rightSet.size === 0) return 0;
  let matched = 0;
  for (const token of leftSet) {
    if (rightSet.has(token)) matched += 1;
  }
  return matched / leftSet.size;
}

function tokenMatchCount(leftTokens, rightTokens) {
  const leftSet = new Set(Array.isArray(leftTokens) ? leftTokens : []);
  const rightSet = new Set(Array.isArray(rightTokens) ? rightTokens : []);
  if (leftSet.size === 0 || rightSet.size === 0) return 0;
  let matched = 0;
  for (const token of leftSet) {
    if (rightSet.has(token)) matched += 1;
  }
  return matched;
}

function businessNameSimilarityScore(nameA, nameB) {
  const left = normalizeBusinessNameForWebsiteGuard(nameA);
  const right = normalizeBusinessNameForWebsiteGuard(nameB);
  if (!left || !right) return 0;
  if (left === right) return 1;
  if (left.includes(right) || right.includes(left)) return 0.92;

  const leftSet = new Set(left.split(/\s+/));
  const rightSet = new Set(right.split(/\s+/));
  let overlap = 0;
  for (const token of leftSet) {
    if (rightSet.has(token)) overlap += 1;
  }
  const union = new Set([...leftSet, ...rightSet]).size;
  if (union === 0) return 0;
  return overlap / union;
}

function buildBusinessSemanticContext(contextInput) {
  const context = contextInput && typeof contextInput === "object" ? contextInput : {};
  const businessName = normalizeText(context.businessName || context.name);
  const businessCategory = normalizeText(context.businessCategory || context.category);
  const businessAddress = normalizeText(context.businessAddress || context.address || context.source_query);
  const website = normalizeBusinessWebsiteUrl(context.businessWebsite || context.website || "");
  const discoveredWebsite = normalizeBusinessWebsiteUrl(context.discoveredWebsite || context.discovered_website || "");
  const hostName = normalizeText(hostnameForUrl(website)).toLowerCase().replace(/^www\./, "");
  const discoveredHost = normalizeText(hostnameForUrl(discoveredWebsite)).toLowerCase().replace(/^www\./, "");

  const nameTokens = tokenizeSemanticText(businessName, { minLen: 2, unique: true });
  const categoryTokens = tokenizeSemanticText(businessCategory, { minLen: 3, unique: true });
  const locationTokens = tokenizeSemanticText(businessAddress, { minLen: 3, unique: true })
    .filter((token) => !LOCATION_NOISE_TOKENS.has(token))
    .slice(0, 8);

  return {
    businessName,
    businessCategory,
    businessAddress,
    websiteHost: hostName,
    discoveredHost,
    nameTokens,
    categoryTokens,
    locationTokens
  };
}

function combineSemanticEvidenceText(evidenceInput) {
  const evidence = evidenceInput && typeof evidenceInput === "object" ? evidenceInput : {};
  const pageData = evidence.pageData && typeof evidence.pageData === "object" ? evidence.pageData : {};
  const semanticProfile = pageData.semanticProfile && typeof pageData.semanticProfile === "object"
    ? pageData.semanticProfile
    : {};
  const orgNames = Array.isArray(semanticProfile.orgNames) ? semanticProfile.orgNames : [];
  const snippets = [
    evidence.title,
    evidence.snippet,
    semanticProfile.pageTitle,
    semanticProfile.metaDescription,
    semanticProfile.headingText,
    orgNames.join(" | "),
    semanticProfile.textSample
  ];
  return normalizeText(snippets.join(" "));
}

function scoreBusinessSemanticEvidence(evidenceInput, contextInput) {
  const context = buildBusinessSemanticContext(contextInput);
  const evidence = evidenceInput && typeof evidenceInput === "object" ? evidenceInput : {};
  const url = normalizeWebsiteUrl(evidence.url) || normalizeBusinessWebsiteUrl(evidence.url) || "";
  const host = normalizeText(hostnameForUrl(url)).toLowerCase().replace(/^www\./, "");
  const semanticText = combineSemanticEvidenceText(evidence);
  const semanticTokens = tokenizeSemanticText(semanticText, { minLen: 3, unique: true });
  const hostTokens = tokenizeSemanticText(host.replace(/\./g, " "), { minLen: 3, unique: true });

  const nameOverlap = tokenOverlapRatio(context.nameTokens, semanticTokens);
  const categoryOverlap = tokenOverlapRatio(context.categoryTokens, semanticTokens);
  const locationOverlap = tokenOverlapRatio(context.locationTokens, semanticTokens);
  const hostNameOverlap = tokenOverlapRatio(context.nameTokens, hostTokens);
  const titleSimilarity = businessNameSimilarityScore(
    context.businessName,
    [normalizeText(evidence.title), normalizeText(evidence.snippet)].join(" ")
  );

  let organizationSimilarity = 0;
  const pageData = evidence.pageData && typeof evidence.pageData === "object" ? evidence.pageData : {};
  const semanticProfile = pageData.semanticProfile && typeof pageData.semanticProfile === "object"
    ? pageData.semanticProfile
    : {};
  const orgNames = Array.isArray(semanticProfile.orgNames) ? semanticProfile.orgNames : [];
  for (const orgName of orgNames) {
    organizationSimilarity = Math.max(organizationSimilarity, businessNameSimilarityScore(context.businessName, orgName));
  }

  const lowerSemanticText = semanticText.toLowerCase();
  let score = 0.12;
  score += Math.min(0.34, nameOverlap * 0.34);
  score += Math.min(0.18, titleSimilarity * 0.18);
  score += Math.min(0.16, organizationSimilarity * 0.16);
  score += Math.min(0.12, categoryOverlap * 0.12);
  score += Math.min(0.08, locationOverlap * 0.08);
  score += Math.min(0.12, hostNameOverlap * 0.12);

  if (context.websiteHost && host && host === context.websiteHost) {
    score += 0.2;
  } else if (context.discoveredHost && host && host === context.discoveredHost) {
    score += 0.14;
  }

  if (host && !isDirectoryHost(host)) {
    score += 0.03;
  } else if (host && isDirectoryHost(host)) {
    score -= 0.03;
  }

  if (/(owner|founder|co-founder|ceo|president|principal|managing)/i.test(lowerSemanticText)) {
    score += 0.05;
  }
  if (/(directory|listing|reviews?|jobs?|careers?|top\s+\d+|best\s+\d+)/i.test(lowerSemanticText)) {
    score -= 0.07;
  }
  if (/\b(formerly|previously|ex[-\s])/i.test(lowerSemanticText)) {
    score -= 0.05;
  }

  const matchedNameTokens = tokenMatchCount(context.nameTokens, semanticTokens);
  if (matchedNameTokens === 0 && context.nameTokens.length >= 2 && !context.websiteHost) {
    score -= 0.14;
  }
  if (nameOverlap < 0.18 && titleSimilarity < 0.25 && organizationSimilarity < 0.25) {
    score -= 0.2;
  }

  const bounded = Math.min(0.99, Math.max(0, score));
  return {
    score: bounded,
    signals: {
      nameOverlap,
      categoryOverlap,
      locationOverlap,
      hostNameOverlap,
      titleSimilarity,
      organizationSimilarity
    }
  };
}

function isPotentialPersonalEmail(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return false;
  const [local = "", domain = ""] = normalized.split("@");
  if (!local || !domain) return false;
  if (isGenericMailboxLocalPart(local)) return false;
  return true;
}

function scoreOwnerLookupCandidate(url, contextInput) {
  const context = contextInput && typeof contextInput === "object" ? contextInput : {};
  const host = normalizeText(hostnameForUrl(url)).toLowerCase().replace(/^www\./, "");
  if (!host) return -100;

  let score = 0;
  if (normalizeText(context.websiteHost) && host === context.websiteHost) score += 5;
  if (normalizeText(context.discoveredHost) && host === context.discoveredHost) score += 4;
  if (host.includes("linkedin.com")) score += 3.5;
  if (host.includes("yelp.com")) score += 2.5;
  if (isDirectoryHost(host) && !host.includes("linkedin.com") && !host.includes("yelp.com")) score -= 3;
  if (/(contact|about|team|leadership|our-story|founder|owner)/i.test(normalizeText(url))) score += 1.2;
  if (/reviews?|photos?|directory|listing|jobs?|careers?/i.test(normalizeText(url))) score -= 0.8;
  return score;
}

async function recoverOwnerViaGoogle(row, optionsInput) {
  const rowData = row && typeof row === "object" ? row : {};
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;
  const businessName = normalizeText(rowData.name);
  const locationHint = normalizeText(rowData.address || rowData.source_query);
  if (!businessName) {
    return { attempted: false, found: false, query: "" };
  }

  const query = locationHint
    ? `${businessName} ${locationHint} owner founder`
    : `${businessName} owner founder`;

  const semanticContext = buildBusinessSemanticContext({
    businessName,
    businessCategory: normalizeText(rowData.category),
    businessAddress: normalizeText(rowData.address || rowData.source_query),
    businessWebsite: normalizeText(rowData.website),
    discoveredWebsite: normalizeText(rowData.discovered_website)
  });

  const googleCandidates = await searchGoogleCandidates(query, {
    timeoutMs,
    visibleTabs,
    maxResults: 3,
    includeDirectoryHosts: true,
    shouldStop: options.shouldStop,
    onScanTabChange: options.onScanTabChange
  });

  const rankedCandidates = googleCandidates
    .map((candidate) => {
      const url = normalizeWebsiteUrl(candidate && candidate.url);
      const snippetScore = scoreBusinessSemanticEvidence({
        url,
        title: normalizeText(candidate && candidate.title),
        snippet: normalizeText(candidate && candidate.snippet)
      }, semanticContext);
      return {
        url,
        title: normalizeText(candidate && candidate.title),
        snippet: normalizeText(candidate && candidate.snippet),
        semanticScore: snippetScore.score,
        score: scoreOwnerLookupCandidate(url, {
          websiteHost: semanticContext.websiteHost,
          discoveredHost: semanticContext.discoveredHost
        }) + snippetScore.score * 4.4
      };
    })
    .filter((item) => item.url && item.score > -2 && item.semanticScore >= 0.26)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  for (const candidate of rankedCandidates) {
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }
    const pageData = await openTabAndExtractData(candidate.url, {
      timeoutMs,
      visibleTabs,
      shouldStop: options.shouldStop,
      onScanTabChange: options.onScanTabChange
    }, executeExtraction).catch(() => null);
    if (!pageData || !Array.isArray(pageData.ownerCandidates)) continue;

    const pageSemanticScore = scoreBusinessSemanticEvidence({
      url: candidate.url,
      title: candidate.title,
      snippet: candidate.snippet,
      pageData
    }, semanticContext);
    if (pageSemanticScore.score < 0.52) continue;

    const owner = pickBestOwner(pageData.ownerCandidates, pageData.emails || [], {
      businessName,
      businessCategory: normalizeText(rowData.category),
      minConfidence: pageSemanticScore.score >= 0.74 ? 0.82 : 0.86,
      businessEvidenceScore: pageSemanticScore.score
    });
    if (!owner) continue;

    const combinedConfidence = Math.min(0.99, Math.max(0.35, owner.confidence * 0.72 + pageSemanticScore.score * 0.28));
    return {
      attempted: true,
      found: true,
      query,
      source: ownerLookupSourceForUrl(candidate.url),
      sourceUrl: candidate.url,
      ownerName: owner.name,
      ownerTitle: owner.title,
      ownerConfidence: combinedConfidence
    };
  }

  return {
    attempted: true,
    found: false,
    query
  };
}

async function verifyPersonalEmailViaGoogle(input, optionsInput) {
  const payload = input && typeof input === "object" ? input : {};
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const candidateEmail = normalizeEmail(payload.candidateEmail);
  const ownerName = normalizeText(payload.ownerName);
  const businessName = normalizeText(payload.businessName);
  const businessCategory = normalizeText(payload.businessCategory);
  const businessAddress = normalizeText(payload.businessAddress || payload.address || payload.source_query);
  const businessWebsite = normalizeText(payload.businessWebsite || payload.website);
  const discoveredWebsite = normalizeText(payload.discoveredWebsite || payload.discovered_website);
  const semanticContext = buildBusinessSemanticContext({
    businessName,
    businessCategory,
    businessAddress,
    businessWebsite,
    discoveredWebsite
  });
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;

  if (!candidateEmail) {
    return { verified: false, matchedUrl: "", query: "" };
  }

  const query = ownerName
    ? `${ownerName} ${businessName} ${businessCategory} "${candidateEmail}"`
    : `${businessName} ${businessCategory} owner "${candidateEmail}"`;

  const candidates = await searchGoogleCandidates(query, {
    timeoutMs,
    visibleTabs,
    maxResults: 3,
    includeDirectoryHosts: true,
    shouldStop: options.shouldStop,
    onScanTabChange: options.onScanTabChange
  });

  const topCandidates = candidates
    .map((item) => ({
      url: normalizeWebsiteUrl(item && item.url),
      title: normalizeText(item && item.title),
      snippet: normalizeText(item && item.snippet)
    }))
    .filter((item) => item.url)
    .slice(0, 3);

  for (const candidate of topCandidates) {
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }

    const pageData = await openTabAndExtractData(
      candidate.url,
      {
        timeoutMs,
        visibleTabs,
        shouldStop: options.shouldStop,
        onScanTabChange: options.onScanTabChange
      },
      executeExtraction
    ).catch(() => null);

    if (!pageData) continue;
    const emails = sanitizeEmailList(pageData.emails || []);
    const emailMatch = emails.includes(candidateEmail);
    let ownerMatch = false;
    if (ownerName && Array.isArray(pageData.ownerCandidates)) {
      ownerMatch = pageData.ownerCandidates.some((candidate) => areLikelySamePersonName(candidate && candidate.name, ownerName));
    }

    const semanticMatch = scoreBusinessSemanticEvidence({
      url: candidate.url,
      title: candidate.title,
      snippet: candidate.snippet,
      pageData
    }, semanticContext);

    const semanticThreshold = emailMatch ? 0.5 : 0.58;
    if ((emailMatch || ownerMatch) && semanticMatch.score >= semanticThreshold) {
      return {
        verified: true,
        matchedUrl: candidate.url,
        query
      };
    }
  }

  return {
    verified: false,
    matchedUrl: "",
    query
  };
}

async function searchGoogleCandidates(query, optionsInput) {
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const siteFilter = normalizeText(options.siteFilter);
  const siteFilterHost = siteFilter
    ? normalizeText(siteFilter.replace(/^site:/i, "").replace(/^https?:\/\//i, "").split("/")[0]).toLowerCase().replace(/^www\./, "")
    : "";
  const maxResults = clampInt(options.maxResults, 1, 3, 3);
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;
  const includeDirectoryHosts = options.includeDirectoryHosts === true;

  if (isEnrichStopRequested(options)) {
    throw createEnrichStopError();
  }

  const searchQuery = siteFilter ? `site:${siteFilter} ${query}` : query;
  const searchUrl = `https://www.google.com/search?q=${encodeURIComponent(searchQuery)}&num=10&hl=en`;
  const rawLinks = await openTabAndExtractLinks(searchUrl, {
    timeoutMs,
    visibleTabs,
    shouldStop: options.shouldStop,
    onScanTabChange: options.onScanTabChange
  }, (tabId) => executeGoogleResultsExtraction(tabId, maxResults));

  const out = [];
  const seen = new Set();
  for (const rawLink of rawLinks) {
    const rawItem = rawLink && typeof rawLink === "object" ? rawLink : { url: rawLink };
    const rawUrl = normalizeText(rawItem.url);
    let normalized = normalizeBusinessWebsiteUrl(rawUrl);
    if (!normalized) {
      normalized = normalizeWebsiteUrl(rawUrl);
    }
    if (!normalized) continue;
    const host = normalizeText(hostnameForUrl(normalized)).toLowerCase().replace(/^www\./, "");
    if (!host) continue;
    if (isSearchEngineHost(host)) continue;
    if (!includeDirectoryHosts && isDirectoryHost(host)) continue;
    if (siteFilterHost && !host.includes(siteFilterHost)) continue;
    const key = `${host}::${normalizeText(normalized).toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      url: normalized,
      host,
      title: normalizeText(rawItem.title),
      snippet: normalizeText(rawItem.snippet)
    });
    if (out.length >= maxResults * 3) break;
  }

  return out.slice(0, Math.max(maxResults, 1));
}

async function discoverPointerWebsite(provider, query, row, optionsInput) {
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;
  const maxPages = clampInt(options.maxPages, 0, 8, 2);
  const excludedHost = normalizeText(options.excludedHost).toLowerCase().replace(/^www\./, "");

  if (maxPages <= 0) return "";

  const siteQuery = provider === "linkedin" ? "linkedin.com/company" : "yelp.com";
  const directoryPages = await searchGoogleCandidates(query, {
    timeoutMs,
    visibleTabs,
    maxResults: maxPages,
    siteFilter: siteQuery,
    includeDirectoryHosts: true,
    shouldStop: options.shouldStop,
    onScanTabChange: options.onScanTabChange
  });

  for (const page of directoryPages) {
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }
    const pageUrl = normalizeWebsiteUrl(page && page.url);
    if (!pageUrl) continue;

    const inlineWebsite = normalizeDiscoveryWebsiteCandidate(pageUrl);
    if (inlineWebsite) {
      const inlineHost = normalizeText(hostnameForUrl(inlineWebsite)).toLowerCase().replace(/^www\./, "");
      if (inlineHost && inlineHost !== excludedHost && !isDirectoryHost(inlineHost)) {
        return inlineWebsite;
      }
    }

    const extractedLinks = await openTabAndExtractLinks(pageUrl, {
      timeoutMs,
      visibleTabs,
      shouldStop: options.shouldStop,
      onScanTabChange: options.onScanTabChange
    }, executeDirectoryResultsExtraction);

    const candidates = [];
    for (const rawLink of extractedLinks) {
      const normalized = normalizeDiscoveryWebsiteCandidate(rawLink);
      if (!normalized) continue;
      const host = normalizeText(hostnameForUrl(normalized)).toLowerCase().replace(/^www\./, "");
      if (!host || host === excludedHost || isDirectoryHost(host)) continue;
      candidates.push({ url: normalized, host });
    }

    const best = pickBestDiscoveryCandidate(candidates, row, {
      includeDirectoryHosts: false,
      excludedHost
    });
    if (best) {
      return best.url;
    }
  }

  return "";
}

function pickBestDiscoveryCandidate(candidates, row, optionsInput) {
  if (!Array.isArray(candidates) || candidates.length === 0) return null;
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const includeDirectoryHosts = options.includeDirectoryHosts === true;
  const excludedHost = normalizeText(options.excludedHost).toLowerCase().replace(/^www\./, "");
  const deduped = [];
  const seen = new Set();

  for (const item of candidates) {
    const url = normalizeBusinessWebsiteUrl(item && item.url) || normalizeWebsiteUrl(item && item.url);
    if (!url) continue;
    const host = normalizeText(hostnameForUrl(url)).toLowerCase().replace(/^www\./, "");
    if (!host) continue;
    if (excludedHost && host === excludedHost) continue;
    if (!includeDirectoryHosts && isDirectoryHost(host)) continue;
    const key = `${host}::${normalizeText(url).toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push({
      url,
      host,
      title: normalizeText(item && item.title),
      snippet: normalizeText(item && item.snippet),
      score: scoreDiscoveryCandidate(
        {
          url,
          title: normalizeText(item && item.title),
          snippet: normalizeText(item && item.snippet)
        },
        row
      )
    });
  }

  deduped.sort((a, b) => b.score - a.score);
  return deduped[0] || null;
}

function scoreDiscoveryCandidate(candidateInput, row) {
  const payload = candidateInput && typeof candidateInput === "object"
    ? candidateInput
    : { url: candidateInput, title: "", snippet: "" };
  const candidate = normalizeText(payload.url).toLowerCase();
  const host = normalizeText(hostnameForUrl(candidate)).toLowerCase().replace(/^www\./, "");
  if (!candidate || !host) return -100;

  const source = row && typeof row === "object" ? row : {};
  const nameTokens = normalizeText(source.name)
    .toLowerCase()
    .split(/\s+/)
    .filter((token) => token.length >= 3 && !/^(the|and|for|llc|inc|ltd|co|company|services?)$/.test(token));
  const locationTokens = normalizeText(source.address || source.source_query)
    .toLowerCase()
    .split(/\s+/)
    .filter((token) => token.length >= 3);

  let score = 0;
  if (!isDirectoryHost(host)) score += 5;
  for (const token of nameTokens) {
    if (host.includes(token)) score += 2;
    if (candidate.includes(`/${token}`)) score += 1;
  }
  for (const token of locationTokens.slice(0, 4)) {
    if (host.includes(token) || candidate.includes(`/${token}`)) score += 0.6;
  }
  if (/contact|about|team|leadership|company/i.test(candidate)) score += 0.8;
  if (/blog|news|article|press|directory|listing|profile/i.test(candidate)) score -= 1.2;
  if (host.includes("facebook.com") || host.includes("instagram.com") || host.includes("x.com") || host.includes("twitter.com")) {
    score -= 2.5;
  }

  const semanticEvidence = scoreBusinessSemanticEvidence({
    url: candidate,
    title: normalizeText(payload.title),
    snippet: normalizeText(payload.snippet)
  }, {
    businessName: normalizeText(source.name),
    businessCategory: normalizeText(source.category),
    businessAddress: normalizeText(source.address || source.source_query),
    businessWebsite: normalizeText(source.website),
    discoveredWebsite: normalizeText(source.discovered_website)
  });
  score += semanticEvidence.score * 3.1;
  if (semanticEvidence.score < 0.24) {
    score -= 1.1;
  }

  return score;
}

function isSearchEngineHost(hostname) {
  const host = normalizeText(hostname).toLowerCase();
  if (!host) return false;
  return (
    /(^|\.)google\./i.test(host) ||
    host.includes("bing.com") ||
    host.includes("yahoo.com") ||
    host.includes("duckduckgo.com") ||
    host.includes("search.brave.com") ||
    host.includes("ecosia.org")
  );
}

function isDirectoryHost(hostname) {
  const host = normalizeText(hostname).toLowerCase();
  if (!host) return false;
  return (
    host.includes("linkedin.com") ||
    host.includes("yelp.com") ||
    host.includes("zoominfo.com") ||
    host.includes("bbb.org") ||
    host.includes("bbb.com") ||
    host.includes("dnb.com") ||
    host.includes("manta.com") ||
    host.includes("bizapedia.com") ||
    host.includes("chamberofcommerce.com") ||
    host.includes("nextdoor.com") ||
    host.includes("yellowpages.com") ||
    host.includes("mapquest.com") ||
    host.includes("tripadvisor.") ||
    host.includes("thumbtack.com") ||
    host.includes("angi.com")
  );
}

function normalizeDiscoveryWebsiteCandidate(rawUrl) {
  const direct = normalizeBusinessWebsiteUrl(rawUrl);
  if (direct && !isDirectoryHost(hostnameForUrl(direct))) {
    return direct;
  }

  const normalized = normalizeWebsiteUrl(rawUrl);
  if (!normalized) return "";

  try {
    const parsed = new URL(normalized);
    const redirectKeys = ["url", "u", "target", "dest", "redirect", "q", "out"];
    for (const key of redirectKeys) {
      const nested = normalizeBusinessWebsiteUrl(parsed.searchParams.get(key)) || normalizeWebsiteUrl(parsed.searchParams.get(key));
      if (nested && !isDirectoryHost(hostnameForUrl(nested))) {
        return nested;
      }
    }
  } catch (_error) {
    return "";
  }

  return "";
}

async function openTabAndExtractLinks(url, optionsInput, extractFn) {
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;
  let tab = null;

  try {
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }
    tab = await createScanTab(url, visibleTabs);
    if (typeof options.onScanTabChange === "function") {
      options.onScanTabChange(tab && tab.id != null ? tab.id : null);
    }

    await waitForTabComplete(tab.id, timeoutMs);
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }
    await sleep(700);
    const links = await extractFn(tab.id);
    return Array.isArray(links) ? links : [];
  } finally {
    if (typeof options.onScanTabChange === "function") {
      options.onScanTabChange(null);
    }
    if (tab && tab.id != null) {
      await closeTab(tab.id).catch(() => {});
    }
  }
}

async function openTabAndExtractData(url, optionsInput, extractFn) {
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;
  let tab = null;

  try {
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }
    tab = await createScanTab(url, visibleTabs);
    if (typeof options.onScanTabChange === "function") {
      options.onScanTabChange(tab && tab.id != null ? tab.id : null);
    }
    await waitForTabComplete(tab.id, timeoutMs);
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }
    await sleep(700);
    return await extractFn(tab.id);
  } finally {
    if (typeof options.onScanTabChange === "function") {
      options.onScanTabChange(null);
    }
    if (tab && tab.id != null) {
      await closeTab(tab.id).catch(() => {});
    }
  }
}

function executeGoogleResultsExtraction(tabId, maxResults) {
  return new Promise((resolve, reject) => {
    chrome.scripting.executeScript(
      {
        target: { tabId },
        func: extractGoogleSearchResultLinksScript,
        args: [clampInt(maxResults, 1, 3, 3)]
      },
      (results) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message || "Failed to parse Google results"));
          return;
        }
        if (!Array.isArray(results) || !results[0]) {
          resolve([]);
          return;
        }
        resolve(Array.isArray(results[0].result) ? results[0].result : []);
      }
    );
  });
}

function executeDirectoryResultsExtraction(tabId) {
  return new Promise((resolve, reject) => {
    chrome.scripting.executeScript(
      {
        target: { tabId },
        func: extractDirectoryWebsiteLinksScript
      },
      (results) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message || "Failed to parse directory page"));
          return;
        }
        if (!Array.isArray(results) || !results[0]) {
          resolve([]);
          return;
        }
        resolve(Array.isArray(results[0].result) ? results[0].result : []);
      }
    );
  });
}

function extractGoogleSearchResultLinksScript(maxResultsInput) {
  const maxResults = Math.max(1, Math.min(3, Number(maxResultsInput) || 3));
  const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim();
  const out = [];
  const indexByUrl = new Map();

  const readSnippet = (anchor, heading) => {
    const baseNode = heading || anchor;
    if (!baseNode) return "";
    const resultCard = baseNode.closest("[data-sokoban-container], .MjjYud, .tF2Cxc, .g, div[data-hveid]");
    if (!resultCard) return "";
    const snippetNode =
      resultCard.querySelector("div.VwiC3b") ||
      resultCard.querySelector("div[data-sncf]") ||
      resultCard.querySelector("span.aCOpRe") ||
      resultCard.querySelector("div.IsZvec");
    return normalize((snippetNode && snippetNode.textContent) || "");
  };

  const push = (rawHref, metaInput) => {
    const href = normalize(rawHref);
    if (!href) return;
    if (/^javascript:/i.test(href)) return;
    if (/^mailto:/i.test(href)) return;
    if (/^tel:/i.test(href)) return;
    let absolute = "";
    try {
      absolute = new URL(href, window.location.href).toString();
    } catch (_error) {
      return;
    }
    if (!/^https?:/i.test(absolute)) return;
    const lower = absolute.toLowerCase();
    if (/^https?:\/\/(?:www\.)?google\./i.test(lower) && !/\/(?:url|aclk|local_url)\?/i.test(lower)) return;
    if (lower.includes("/preferences?") || lower.includes("/setprefs?") || lower.includes("/advanced_search")) return;
    const meta = metaInput && typeof metaInput === "object" ? metaInput : {};
    const title = normalize(meta.title);
    const snippet = normalize(meta.snippet);

    if (indexByUrl.has(absolute)) {
      const existing = out[indexByUrl.get(absolute)] || {};
      if (!existing.title && title) existing.title = title;
      if (!existing.snippet && snippet) existing.snippet = snippet;
      return;
    }

    indexByUrl.set(absolute, out.length);
    out.push({
      url: absolute,
      title,
      snippet
    });
  };

  const organicSelectors = [
    "div#search h3",
    "[data-sokoban-container] h3",
    "main h3"
  ];
  for (const selector of organicSelectors) {
    const headings = Array.from(document.querySelectorAll(selector)).slice(0, 80);
    for (const heading of headings) {
      const anchor = heading.closest("a[href]");
      if (!anchor) continue;
      push(anchor.getAttribute("href") || anchor.href || "", {
        title: normalize(heading.textContent || anchor.textContent || ""),
        snippet: readSnippet(anchor, heading)
      });
      if (out.length >= maxResults * 4) {
        return out.slice(0, maxResults * 4);
      }
    }
  }

  const fallbackAnchors = Array.from(document.querySelectorAll("div#search a[href]")).slice(0, 500);
  for (const anchor of fallbackAnchors) {
    const heading = anchor.querySelector("h3");
    if (!heading && !anchor.closest("[data-sokoban-container]")) continue;
    push(anchor.getAttribute("href") || anchor.href || "", {
      title: normalize((heading && heading.textContent) || anchor.textContent || ""),
      snippet: readSnippet(anchor, heading)
    });
    if (out.length >= maxResults * 4) break;
  }

  return out.slice(0, maxResults * 4);
}

function extractDirectoryWebsiteLinksScript() {
  const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim();
  const out = new Set();
  const nodes = Array.from(document.querySelectorAll("a[href]")).slice(0, 2000);

  for (const node of nodes) {
    const href = normalize(node.getAttribute("href") || node.href || "");
    if (!href) continue;
    if (/^javascript:/i.test(href)) continue;
    if (/^mailto:/i.test(href)) continue;
    if (/^tel:/i.test(href)) continue;
    try {
      const absolute = new URL(href, window.location.href).toString();
      out.add(absolute);
    } catch (_error) {
      // Ignore malformed URLs.
    }
  }

  return Array.from(out).slice(0, 400);
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

function isFocusedCrawlTarget(url, intent) {
  if (isHighIntentPath(url, intent)) return true;
  const lower = normalizeText(url).toLowerCase();
  if (!lower) return false;
  return /(contact|about|team|leadership|management|staff|founder|owner|who-we-are|our-story|careers?|jobs?|locations?|office|meet-the)/i.test(lower);
}

function shouldQueueFocusedCrawlLink(url, score, intent, visitedCount, queueCount) {
  const numericScore = Number(score) || 0;
  if (isFocusedCrawlTarget(url, intent)) return true;
  // Allow very small exploration from homepage before narrowing to high-intent pages.
  if (visitedCount === 0 && queueCount < 3 && numericScore >= 4) return true;
  if (visitedCount <= 1 && queueCount < 2 && numericScore >= 5) return true;
  return false;
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
  let noSignalStreak = 0;
  const intent = normalizeScanIntent(options.intent);
  const ownerContext = {
    businessName: normalizeText(options.businessName),
    businessCategory: normalizeText(options.businessCategory)
  };
  const normalizedStartUrl = normalizeBusinessWebsiteUrl(startUrl) || normalizeWebsiteUrl(startUrl) || startUrl;
  const baseOrigin = new URL(normalizedStartUrl).origin;
  const firstUrl = canonicalizeCrawlUrl(normalizedStartUrl, baseOrigin) || stripHash(normalizedStartUrl) || normalizedStartUrl;
  const startHost = hostnameForUrl(firstUrl) || hostnameForUrl(normalizedStartUrl);
  const socialRootScan = isSocialNetworkHost(startHost);
  const searchRootScan = isSearchEngineHost(startHost);
  const directoryRootScan = isDirectoryHost(startHost);
  const focusedSinglePageScan = options.focusedSinglePage === true || searchRootScan || directoryRootScan;
  const skipSitemapLookup = socialRootScan || options.skipSitemapLookup === true || focusedSinglePageScan;
  const maxPagesCap = socialRootScan ? 4 : focusedSinglePageScan ? 1 : 12;
  const effectiveMaxPages = Math.min(clampInt(options.maxPagesPerSite, 1, 120, maxPagesCap), maxPagesCap);
  const sitemapQueueBudget = Math.max(6, effectiveMaxPages * 2);
  const perPageLinkBudget = Math.max(24, effectiveMaxPages * 4);
  const noSignalExitThreshold = socialRootScan ? 2 : focusedSinglePageScan ? 1 : 4;

  const seedUrls = socialRootScan ? buildSocialProbeUrls(firstUrl) : [firstUrl];
  const queue = [];
  const visited = new Set();
  const queued = new Set();
  const discovered = new Set();

  for (const seedUrl of seedUrls) {
    const normalizedSeed = canonicalizeCrawlUrl(seedUrl, baseOrigin) || stripHash(seedUrl) || "";
    const seedKey = stripHash(normalizedSeed);
    if (!normalizedSeed || !seedKey || queued.has(seedKey)) continue;
    queued.add(seedKey);
    discovered.add(seedKey);
    queue.push(normalizedSeed);
  }
  if (queue.length === 0) {
    const firstKey = stripHash(firstUrl) || firstUrl;
    queue.push(firstUrl);
    queued.add(firstKey);
    discovered.add(firstKey);
  }

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

    while (visited.size < effectiveMaxPages) {
      if (queue.length === 0) {
        if (emails.size > 0 || sitemapAttempted || skipSitemapLookup) {
          break;
        }

        sitemapAttempted = true;
        reportProgress("sitemap_lookup", `${baseOrigin}/sitemap.xml`);
        const sitemapLinks = await discoverLinksFromSitemap(tab.id, baseOrigin, options.timeoutMs, intent).catch(() => []);
        if (!Array.isArray(sitemapLinks) || sitemapLinks.length === 0) {
          break;
        }

        const sitemapEntries = prioritizeCrawlLinkEntries(sitemapLinks, intent);
        for (const entry of sitemapEntries.slice(0, sitemapQueueBudget)) {
          const normalizedLink = canonicalizeCrawlUrl(entry.link, baseOrigin);
          if (!normalizedLink) continue;
          if (!shouldQueueFocusedCrawlLink(normalizedLink, entry.score, intent, visited.size, queue.length)) continue;
          const linkKey = stripHash(normalizedLink);
          if (!linkKey) continue;
          if (visited.has(linkKey) || queued.has(linkKey)) continue;
          if (discovered.size >= options.maxDiscoveredPages) continue;

          discovered.add(linkKey);
          queued.add(linkKey);
          queue.push(normalizedLink);
          if (queue.length >= sitemapQueueBudget) break;
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

      const hasSignalOnPage =
        (Array.isArray(pageData.emails) && pageData.emails.length > 0) ||
        (Array.isArray(pageData.ownerCandidates) && pageData.ownerCandidates.length > 0) ||
        (Array.isArray(pageData.phones) && pageData.phones.length > 0) ||
        pageData.hasContactSignals === true;
      noSignalStreak = hasSignalOnPage ? 0 : noSignalStreak + 1;

      const bestOwnerNow = pickBestOwner(ownerCandidates, Array.from(emails), ownerContext);
      const personalEmailNow = chooseOwnerEmail(Array.from(emails), bestOwnerNow ? bestOwnerNow.name : "");
      const companyEmailNow = chooseContactEmail(Array.from(emails), personalEmailNow);
      if (personalEmailNow) {
        reportProgress("site_personal_email_found", nextUrl);
        break;
      }
      if (
        intent.needsEmail &&
        companyEmailNow &&
        (
          !intent.needsOwner ||
          priorityPagesVisited >= 2 ||
          pagesVisited >= Math.min(effectiveMaxPages, 5)
        )
      ) {
        reportProgress("site_company_email_found", nextUrl);
        break;
      }

      if (!socialRootScan && !focusedSinglePageScan) {
        const prioritizedLinks = prioritizeCrawlLinkEntries([
          ...(Array.isArray(pageData.relatedLinks) ? pageData.relatedLinks : []),
          ...(Array.isArray(pageData.internalLinks) ? pageData.internalLinks : [])
        ], intent).slice(0, perPageLinkBudget);
        const hasHighIntentCandidate = prioritizedLinks.some((entry) => entry.score >= 6);

        for (const entry of prioritizedLinks) {
          if (
            entry.score <= -3 &&
            hasHighIntentCandidate &&
            queue.length >= Math.min(8, effectiveMaxPages)
          ) {
            continue;
          }

          const normalizedLink = canonicalizeCrawlUrl(entry.link, baseOrigin);
          if (!normalizedLink) continue;
          if (!shouldQueueFocusedCrawlLink(normalizedLink, entry.score, intent, visited.size, queue.length)) continue;

          const linkKey = stripHash(normalizedLink);
          if (!linkKey) continue;
          if (visited.has(linkKey) || queued.has(linkKey)) continue;
          if (discovered.size >= options.maxDiscoveredPages) continue;
          if (queue.length >= effectiveMaxPages * 2 && entry.score < 6) continue;

          discovered.add(linkKey);
          queued.add(linkKey);
          queue.push(normalizedLink);
          if (entry.score >= 6) {
            highIntentDiscovered.add(linkKey);
          }
        }
      }

      for (const social of pageData.socialLinks || []) {
        registerSocialCandidate(social);
      }
      reportProgress("site_page_done", nextUrl);

      const queueHasFocusedTarget = queue.some((queuedUrl) => isFocusedCrawlTarget(queuedUrl, intent));
      if (
        noSignalStreak >= noSignalExitThreshold &&
        emails.size === 0 &&
        ownerCandidates.length === 0 &&
        (priorityPagesVisited >= 1 || !queueHasFocusedTarget)
      ) {
        reportProgress("site_focus_exit", nextUrl);
        break;
      }
    }

    const bestOwnerBeforeSocial = pickBestOwner(ownerCandidates, Array.from(emails), ownerContext);
    const ownerEmailBeforeSocial = chooseOwnerEmail(Array.from(emails), bestOwnerBeforeSocial ? bestOwnerBeforeSocial.name : "");
    const companyEmailBeforeSocial = chooseContactEmail(Array.from(emails), ownerEmailBeforeSocial);
    const emailBeforeSocial = ownerEmailBeforeSocial || companyEmailBeforeSocial;
    if (options.scanSocialLinks === true && !emailBeforeSocial && socialCandidates.size > 0) {
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
          const bestOwnerNow = pickBestOwner(ownerCandidates, Array.from(emails), ownerContext);
          const ownerEmailNow = chooseOwnerEmail(Array.from(emails), bestOwnerNow ? bestOwnerNow.name : "");
          const companyEmailNow = chooseContactEmail(Array.from(emails), ownerEmailNow);
          if (ownerEmailNow || companyEmailNow) {
            reportProgress(ownerEmailNow ? "social_personal_email_found" : "social_email_found", normalizedTarget);
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
  const bestOwner = pickBestOwner(ownerCandidates, emailList, ownerContext);
  const ownerEmailCandidate = chooseOwnerEmail(emailList, bestOwner ? bestOwner.name : "");
  let ownerEmail = "";
  if (ownerEmailCandidate && isPotentialPersonalEmail(ownerEmailCandidate)) {
    reportProgress("owner_email_verify_lookup", ownerEmailCandidate);
    const verification = await verifyPersonalEmailViaGoogle(
      {
        candidateEmail: ownerEmailCandidate,
        ownerName: bestOwner ? bestOwner.name : "",
        businessName: normalizeText(options.businessName),
        businessCategory: normalizeText(options.businessCategory),
        businessAddress: normalizeText(options.businessAddress || options.sourceQuery),
        businessWebsite: normalizeText(options.businessWebsite || startUrl),
        discoveredWebsite: normalizeText(options.discoveredWebsite)
      },
      {
        timeoutMs: options.timeoutMs,
        visibleTabs: options.visibleTabs === true,
        shouldStop: options.shouldStop,
        onScanTabChange: options.onTabChange
      }
    ).catch(() => ({ verified: false, matchedUrl: "" }));
    if (verification.verified === true) {
      ownerEmail = ownerEmailCandidate;
      reportProgress("owner_email_verified", normalizeText(verification.matchedUrl));
    } else {
      reportProgress("owner_email_unverified", ownerEmailCandidate);
    }
  }
  // Personal email must be verified, otherwise keep only company/contact output.
  let contactEmail = "";
  if (ownerEmail) {
    contactEmail = "";
  } else {
    contactEmail = chooseContactEmail(emailList, "");
    if (!contactEmail && ownerEmailCandidate) {
      contactEmail = ownerEmailCandidate;
    }
  }
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

function pickBestOwner(candidates, emails, contextInput) {
  if (!Array.isArray(candidates) || candidates.length === 0) return null;
  const context = contextInput && typeof contextInput === "object" ? contextInput : {};
  const minConfidence = Number.isFinite(Number(context.minConfidence)) ? Number(context.minConfidence) : 0.82;
  const emailList = sanitizeEmailList(emails);
  const emailLocals = emailList.map((email) => localPartForEmail(email));
  const aggregate = new Map();

  for (const candidate of candidates) {
    const name = normalizeText(candidate && candidate.name);
    const nameAssessment = scorePersonNameCandidate(name, context);
    if (nameAssessment.score < 0.72) continue;

    const title = normalizeText(candidate && candidate.title);
    const source = normalizeText(candidate && candidate.source);
    const baseScore = Number(candidate && candidate.score) || 0;
    const key = `${name.toLowerCase()}::${title.toLowerCase()}`;
    const hasStrongTitle = isStrongOwnerTitle(title);
    const hasStructuredSource = /jsonld|schema/i.test(source);
    const hasHeadingSource = /heading|h1|h2|h3|h4|h5/i.test(source);
    const hasTrustedSource = hasStructuredSource || hasHeadingSource;
    if (!hasStrongTitle && !hasStructuredSource) continue;

    let weightedScore = baseScore + Math.min(1.6, name.split(/\s+/).length - 1) + nameAssessment.score * 1.5;
    if (title) weightedScore += 0.4;
    if (hasStrongTitle) {
      weightedScore += 2.2;
    } else if (title) {
      weightedScore -= 1.6;
    }
    if (hasStructuredSource) {
      weightedScore += 1.1;
    }
    if (hasHeadingSource) {
      weightedScore += 0.4;
    }
    if (Number.isFinite(Number(context.businessEvidenceScore))) {
      const businessEvidenceScore = Number(context.businessEvidenceScore);
      weightedScore += (businessEvidenceScore - 0.5) * 2.8;
      if (businessEvidenceScore < 0.45) {
        weightedScore -= 1.4;
      }
    }

    const tokens = name
      .toLowerCase()
      .split(/\s+/)
      .filter((token) => token.length >= 3);
    let emailTokenMatched = false;
    if (tokens.length > 0 && emailLocals.some((local) => tokens.some((token) => local.includes(token)))) {
      weightedScore += 1;
      emailTokenMatched = true;
    }

    const existing = aggregate.get(key) || {
      name,
      title,
      scoreTotal: 0,
      count: 0,
      hasStrongTitle: false,
      hasTrustedSource: false,
      hasStructuredSource: false,
      emailTokenMatched: false
    };
    existing.scoreTotal += weightedScore;
    existing.count += 1;
    existing.hasStrongTitle = existing.hasStrongTitle || hasStrongTitle;
    existing.hasTrustedSource = existing.hasTrustedSource || hasTrustedSource;
    existing.hasStructuredSource = existing.hasStructuredSource || hasStructuredSource;
    existing.emailTokenMatched = existing.emailTokenMatched || emailTokenMatched;
    aggregate.set(key, existing);
  }

  let best = null;
  for (const entry of aggregate.values()) {
    const repetitionBonus = Math.min(2, (entry.count - 1) * 0.7);
    const avgScore = entry.scoreTotal / Math.max(1, entry.count);
    const finalScore = avgScore + repetitionBonus;
    const confidence = Math.min(0.99, Math.max(0.35, 0.44 + finalScore / 18));
    const strongEvidence =
      entry.hasStrongTitle && (entry.hasTrustedSource || entry.emailTokenMatched || entry.count >= 2) ||
      (entry.hasStructuredSource && entry.emailTokenMatched && entry.count >= 2);
    if (confidence < minConfidence) continue;
    if (!strongEvidence) continue;

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
    "reservations",
    "online",
    "comment",
    "comments",
    "wecare"
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
    "mail",
    "online",
    "comment",
    "comments",
    "wecare"
  ];

  return genericPrefixes.some((prefix) => hasMailboxPrefix(local, prefix));
}

function isLikelyPersonalMailboxLocalPart(localPart, domainPart) {
  const local = normalizeText(localPart).toLowerCase();
  if (!local || isGenericMailboxLocalPart(local)) return false;

  if (/(support|customer|client|service|sales|billing|admin|hello|info|contact|office|team|hr|jobs|careers|marketing|media|press|accounts?|finance|booking|reservations?|dispatch|operations?|ops|legal|privacy|compliance|security|abuse|newsletter|notifications?|alerts?|orders?|returns?|store|community|member|partners?|partnerships|online|comments?|wecare)/i.test(local)) {
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

function isSocialNetworkHost(hostnameOrUrl) {
  const raw = normalizeText(hostnameOrUrl).toLowerCase();
  const host = raw.includes("/") ? hostnameForUrl(raw) : raw;
  if (!host) return false;
  return (
    host.includes("facebook.com") ||
    host.includes("instagram.com") ||
    host.includes("linkedin.com") ||
    host.includes("twitter.com") ||
    host.includes("x.com") ||
    host.includes("youtube.com") ||
    host.includes("tiktok.com") ||
    host.includes("threads.net")
  );
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

function isPlaceholderEmailParts(localPart, domainPart) {
  const local = normalizeText(localPart).toLowerCase();
  const domain = normalizeText(domainPart).toLowerCase().replace(/^www\./, "");
  if (!local || !domain) return true;

  const placeholderDomains = new Set([
    "example.com",
    "example.net",
    "example.org",
    "example.edu",
    "example.gov",
    "example.mil",
    "test.com",
    "test.net",
    "test.org",
    "domain.com",
    "domain.net",
    "domain.org",
    "yourdomain.com",
    "mydomain.com",
    "sample.com"
  ]);
  if (placeholderDomains.has(domain)) return true;
  if (/(^|\.)(example|invalid|localhost|test)$/.test(domain)) return true;
  if (/^(n\/?a|na|null|none|unknown)$/i.test(local)) return true;

  const localLooksGeneric = /^(user(name)?|your-?name|name|email|test|example|sample|demo|mail|contact)$/i.test(local);
  if (localLooksGeneric && /(example|domain|test|localhost|invalid|sample|demo)/i.test(domain)) {
    return true;
  }

  return false;
}

function normalizeEmail(email) {
  const value = normalizeText(email).toLowerCase();
  if (!value.includes("@")) return "";
  if (value.length < 6 || value.length > 120) return "";
  if (/\.(png|jpg|jpeg|svg|gif|webp|js|css)$/i.test(value)) return "";
  if (/\s/.test(value)) return "";
  const parts = value.split("@");
  if (parts.length !== 2) return "";
  const localPart = parts[0];
  const domainPart = parts[1];
  if (!localPart || !domainPart) return "";
  if (!/^[a-z0-9._%+-]+$/.test(localPart)) return "";
  if (!/^[a-z0-9.-]+$/.test(domainPart)) return "";
  if (!domainPart.includes(".") || domainPart.startsWith(".") || domainPart.endsWith(".") || domainPart.includes("..")) return "";
  const labels = domainPart.split(".");
  if (labels.some((label) => !label || label.startsWith("-") || label.endsWith("-") || !/^[a-z0-9-]+$/.test(label))) return "";
  const topLevel = labels[labels.length - 1] || "";
  if (!/^(?:[a-z]{2,24}|xn--[a-z0-9-]{2,59})$/.test(topLevel)) return "";
  if (isPlaceholderEmailParts(localPart, domainPart)) return "";
  if (/^(example|test)@/i.test(value)) return "";
  if (/(noreply|do-not-reply|donotreply)/i.test(value)) return "";
  return value;
}

function sanitizePhoneText(value) {
  return normalizePhoneText(value);
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

function scorePersonNameCandidate(name, contextInput) {
  const context = contextInput && typeof contextInput === "object" ? contextInput : {};
  const value = normalizeText(name)
    .replace(/[|,;:]/g, " ")
    .replace(/\s+/g, " ");
  if (!value) return { score: 0, words: [] };
  if (/\d|@|https?:\/\//i.test(value)) return { score: 0, words: [] };

  const words = value
    .split(/\s+/)
    .map((word) => word.replace(/^[^A-Za-z]+|[^A-Za-z'.-]+$/g, ""))
    .filter(Boolean);
  if (words.length < 2 || words.length > 4) return { score: 0.12, words };
  if (words.some((word) => word.length < 2 || word.length > 22)) return { score: 0.1, words };
  if (!words.every((word) => /^[A-Za-z][A-Za-z'\-\.]*$/.test(word))) return { score: 0.08, words };
  if (new Set(words.map((word) => word.toLowerCase())).size < 2) return { score: 0.1, words };

  const lowercaseConnectors = new Set(["de", "da", "del", "della", "di", "du", "van", "von", "bin", "al", "la", "le", "st"]);
  let capitalizedCount = 0;
  for (const word of words) {
    if (/^[A-Z]/.test(word)) {
      capitalizedCount += 1;
      continue;
    }
    if (!lowercaseConnectors.has(word.toLowerCase())) {
      return { score: 0.12, words };
    }
  }
  if (capitalizedCount < 2) return { score: 0.16, words };

  const blockedPhrases = [
    "contact us",
    "about us",
    "our team",
    "learn more",
    "privacy policy",
    "terms of service",
    "service finance company",
    "customer service",
    "only shared it with",
    "may not have",
    "for home comfort"
  ];
  const lower = words.join(" ").toLowerCase();
  if (blockedPhrases.some((entry) => lower.includes(entry))) return { score: 0.06, words };

  const blockedTokens = new Set([
    "llc",
    "inc",
    "corp",
    "co",
    "company",
    "group",
    "services",
    "service",
    "solutions",
    "plumbing",
    "heating",
    "cooling",
    "air",
    "conditioning",
    "electrical",
    "electric",
    "roofing",
    "construction",
    "contracting",
    "industries",
    "systems",
    "partners",
    "finance",
    "bank",
    "credit",
    "guide",
    "support",
    "team",
    "staff",
    "office",
    "home",
    "online",
    "comments",
    "comment",
    "wecare",
    "for",
    "with",
    "only",
    "shared",
    "call",
    "may",
    "not",
    "have",
    "customer"
  ]);
  const loweredWords = words.map((word) => word.toLowerCase().replace(/\.+$/g, ""));
  if (loweredWords.some((word) => blockedTokens.has(word))) return { score: 0.07, words };

  const businessTokens = normalizeText(context.businessName)
    .toLowerCase()
    .split(/\s+/)
    .map((word) => word.replace(/[^a-z]/g, ""))
    .filter((word) => word.length >= 3 && !blockedTokens.has(word));
  const categoryTokens = normalizeText(context.businessCategory)
    .toLowerCase()
    .split(/\s+/)
    .map((word) => word.replace(/[^a-z]/g, ""))
    .filter((word) => word.length >= 3 && !blockedTokens.has(word));
  const contextTokens = Array.from(new Set([...businessTokens, ...categoryTokens]));
  let contextPenalty = 0;
  if (contextTokens.length > 0) {
    const overlapCount = loweredWords.filter((word) => contextTokens.includes(word)).length;
    if (overlapCount >= Math.min(2, loweredWords.length)) {
      contextPenalty = 0.24;
    } else if (overlapCount > 0) {
      contextPenalty = 0.08;
    }
  }

  let score = 0.66;
  score += Math.min(0.14, (capitalizedCount - 1) * 0.07);
  score += Math.min(0.08, Math.max(0, words.length - 2) * 0.04);
  score -= contextPenalty;
  score = Math.min(0.99, Math.max(0, score));
  return { score, words };
}

function isLikelyPersonName(name, contextInput) {
  return scorePersonNameCandidate(name, contextInput).score >= 0.72;
}

function isStrongOwnerTitle(title) {
  const value = normalizeText(title).toLowerCase();
  if (!value) return false;
  return /\b(owner(?:\s*\/\s*operator)?|co-owner|founder|co-founder|ceo|chief executive(?: officer)?|president|principal|proprietor|managing director|managing member)\b/i.test(value);
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

    // Drop query strings entirely to avoid crawling pagination/filter loops.
    parsed.search = "";
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

  if (/(^|\/)(category|categories|tag|tags|archive|archives|events|event|calendar|search)(\/|$)/i.test(lowerPath)) {
    return true;
  }

  if (/\/page\/\d+\/?$/i.test(lowerPath)) {
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

  const isPlaceholderEmailParts = (localPart, domainPart) => {
    const local = normalize(localPart).toLowerCase();
    const domain = normalize(domainPart).toLowerCase().replace(/^www\./, "");
    if (!local || !domain) return true;

    const placeholderDomains = new Set([
      "example.com",
      "example.net",
      "example.org",
      "example.edu",
      "example.gov",
      "example.mil",
      "test.com",
      "test.net",
      "test.org",
      "domain.com",
      "domain.net",
      "domain.org",
      "yourdomain.com",
      "mydomain.com",
      "sample.com"
    ]);
    if (placeholderDomains.has(domain)) return true;
    if (/(^|\.)(example|invalid|localhost|test)$/.test(domain)) return true;
    if (/^(n\/?a|na|null|none|unknown)$/i.test(local)) return true;

    const localLooksGeneric = /^(user(name)?|your-?name|name|email|test|example|sample|demo|mail|contact)$/i.test(local);
    if (localLooksGeneric && /(example|domain|test|localhost|invalid|sample|demo)/i.test(domain)) {
      return true;
    }

    return false;
  };

  const isLikelyEmail = (value) => {
    const email = normalize(value).toLowerCase();
    if (!email.includes("@")) return false;
    if (email.length < 6 || email.length > 120) return false;
    if (/\.(png|jpg|jpeg|svg|gif|webp|js|css)$/i.test(email)) return false;
    if (/\s/.test(email)) return false;
    const parts = email.split("@");
    if (parts.length !== 2) return false;
    const localPart = parts[0];
    const domainPart = parts[1];
    if (!localPart || !domainPart) return false;
    if (!/^[a-z0-9._%+-]+$/.test(localPart)) return false;
    if (!/^[a-z0-9.-]+$/.test(domainPart)) return false;
    if (!domainPart.includes(".") || domainPart.startsWith(".") || domainPart.endsWith(".") || domainPart.includes("..")) return false;
    const labels = domainPart.split(".");
    if (labels.some((label) => !label || label.startsWith("-") || label.endsWith("-") || !/^[a-z0-9-]+$/.test(label))) return false;
    const topLevel = labels[labels.length - 1] || "";
    if (!/^(?:[a-z]{2,24}|xn--[a-z0-9-]{2,59})$/.test(topLevel)) return false;
    if (isPlaceholderEmailParts(localPart, domainPart)) return false;
    if (/^(example|test)@/i.test(email)) return false;
    if (/(noreply|do-not-reply|donotreply)/i.test(email)) return false;
    return true;
  };

  const isLikelyPhoneDigits = (digits) => {
    const compact = normalize(digits).replace(/\D/g, "");
    return compact.length >= 10 && compact.length <= 15;
  };

  const formatNorthAmericaPhone = (digits) => {
    const compact = normalize(digits).replace(/\D/g, "");
    if (compact.length === 10) {
      return `(${compact.slice(0, 3)}) ${compact.slice(3, 6)}-${compact.slice(6)}`;
    }
    if (compact.length === 11 && compact.startsWith("1")) {
      return `(${compact.slice(1, 4)}) ${compact.slice(4, 7)}-${compact.slice(7)}`;
    }
    return "";
  };

  const normalizePhone = (value) => {
    const raw = normalize(value);
    if (!raw) return "";

    const withoutExtension = raw.replace(/\b(?:ext\.?|extension|x)\s*[:.]?\s*\d{1,6}\b/gi, " ");
    const candidates = withoutExtension.match(/\+?\s*\(?\d[\d().\s-]{7,}\d/g) || [];

    let selectedDigits = "";
    let selectedHasPlus = false;
    let bestScore = -1;

    const tryCandidate = (candidate) => {
      const cleaned = normalize(candidate).replace(/[^\d+().\s-]/g, " ");
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

  const isLikelyPersonName = (value) => {
    const normalized = normalize(value)
      .replace(/[|,;:]/g, " ")
      .replace(/\s+/g, " ");
    if (!normalized) return false;
    if (/\d|@|https?:\/\//i.test(normalized)) return false;

    const words = normalized
      .split(/\s+/)
      .map((word) => word.replace(/^[^A-Za-z]+|[^A-Za-z'.-]+$/g, ""))
      .filter(Boolean);
    if (words.length < 2 || words.length > 4) return false;
    if (new Set(words.map((word) => word.toLowerCase())).size < 2) return false;
    if (!words.every((word) => /^[A-Za-z][A-Za-z'.-]*$/.test(word))) return false;

    const lowercaseConnectors = new Set(["de", "da", "del", "della", "di", "du", "van", "von", "bin", "al", "la", "le", "st"]);
    let capitalizedCount = 0;
    for (const word of words) {
      if (/^[A-Z]/.test(word)) {
        capitalizedCount += 1;
        continue;
      }
      if (!lowercaseConnectors.has(word.toLowerCase())) {
        return false;
      }
    }
    if (capitalizedCount < 2) return false;

    const blockedPhrases = [
      "contact us",
      "about us",
      "our team",
      "only shared it with",
      "may not have",
      "for home comfort"
    ];
    const lowerPhrase = words.join(" ").toLowerCase();
    if (blockedPhrases.some((entry) => lowerPhrase.includes(entry))) return false;

    const blockedTokens = new Set([
      "llc",
      "inc",
      "corp",
      "co",
      "company",
      "group",
      "service",
      "services",
      "solutions",
      "plumbing",
      "finance",
      "bank",
      "credit",
      "guide",
      "support",
      "team",
      "staff",
      "office",
      "home",
      "online",
      "comments",
      "comment",
      "wecare",
      "for",
      "with",
      "only",
      "shared",
      "call",
      "may",
      "not",
      "have",
      "customer"
    ]);
    const loweredWords = words.map((word) => word.toLowerCase().replace(/\.+$/g, ""));
    if (loweredWords.some((word) => blockedTokens.has(word))) return false;

    return true;
  };

  const ownerTitlePattern = /(owner(?:\s*\/\s*operator)?|co-owner|founder|co-founder|president|ceo|chief executive(?: officer)?|principal|proprietor|managing director|managing member)/i;

  const parseOwnerCandidate = (text) => {
    const normalized = normalize(text);
    if (!normalized) return null;

    const titlePattern = "(owner(?:\\s*\\/\\s*operator)?|co-owner|founder|co-founder|president|ceo|chief executive(?: officer)?|principal|proprietor|managing director|managing member)";
    const namePattern = "([A-Z][A-Za-z'\\-.]+(?:\\s+[A-Z][A-Za-z'\\-.]+){1,3})";

    const patternOne = new RegExp(`${titlePattern}\\s*[:\\-\\|,]?\\s*${namePattern}`, "i");
    const patternTwo = new RegExp(`${namePattern}\\s*(?:,|\\-|\\|)\\s*${titlePattern}`, "i");

    const one = normalized.match(patternOne);
    if (one) {
      const candidateName = normalize(one[2]);
      if (!isLikelyPersonName(candidateName)) return null;
      return {
        name: candidateName,
        title: normalize(one[1])
      };
    }

    const two = normalized.match(patternTwo);
    if (two) {
      const candidateName = normalize(two[1]);
      if (!isLikelyPersonName(candidateName)) return null;
      return {
        name: candidateName,
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
  const orgNameSet = new Set();
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

    if (nameValue && /(organization|localbusiness|business|store|professionalservice|corporation|restaurant|medical|dentist|legal|financial|realestate)/i.test(typeValue)) {
      orgNameSet.add(nameValue);
    }
    const legalName = normalize(obj.legalName || "");
    if (legalName) {
      orgNameSet.add(legalName);
    }
    const alternateName = normalize(obj.alternateName || "");
    if (alternateName && !isLikelyPersonName(alternateName)) {
      orgNameSet.add(alternateName);
    }

    if (nameValue && isLikelyPersonName(nameValue) && /person/.test(typeValue)) {
      if (jobTitleValue && ownerTitlePattern.test(jobTitleValue)) {
        ownerCandidates.push({ name: nameValue, title: jobTitleValue, score: 4, source: "jsonld" });
      } else if (ownerTitlePattern.test(typeValue)) {
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
        if (founderName && isLikelyPersonName(founderName)) {
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
        if (employeeName && isLikelyPersonName(employeeName) && ownerTitlePattern.test(employeeTitle)) {
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

  const ownerKeyword = /(owner(?:\s*\/\s*operator)?|co-owner|founder|co-founder|president|ceo|chief executive|managing director|principal|proprietor|managing member)/i;
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
  const metaDescriptionNode =
    document.querySelector("meta[name='description']") ||
    document.querySelector("meta[property='og:description']");
  const metaDescription = normalize((metaDescriptionNode && metaDescriptionNode.getAttribute("content")) || "");
  const headingText = normalize(
    Array.from(document.querySelectorAll("h1,h2,h3"))
      .slice(0, 14)
      .map((node) => normalize(node.textContent || ""))
      .filter(Boolean)
      .join(" | ")
  );
  const textSample = bodyText.slice(0, 2200);

  return {
    emails: Array.from(emails).slice(0, 60),
    phones: Array.from(phones).slice(0, 20),
    ownerCandidates,
    relatedLinks: Array.from(relatedLinkSet).slice(0, 50),
    internalLinks: Array.from(internalLinkSet).slice(0, 260),
    socialLinks: Array.from(socialLinkSet).slice(0, 12),
    blocked,
    platform,
    hasContactSignals,
    semanticProfile: {
      pageTitle: normalize(document.title || ""),
      metaDescription,
      headingText,
      textSample,
      orgNames: Array.from(orgNameSet).slice(0, 12)
    }
  };
}
