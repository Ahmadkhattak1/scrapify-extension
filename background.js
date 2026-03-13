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
  "showEnrichmentTabsEnabled",
  "leadDiscoveryEnabled",
  "contactGoalEmailEnabled",
  "contactGoalPhoneEnabled"
];
const RESULTS_PAGE_PATH = "results.html";
const CONTROL_PANEL_PATH = "popup.html";
const CONTROL_PANEL_ANCHOR_WINDOW_KEY = "controlPanelAnchorWindowId";
const CONTROL_PANEL_WINDOW_WIDTH = 420;
const CONTROL_PANEL_WINDOW_HEIGHT = 760;
const CONTROL_PANEL_WINDOW_RIGHT_OFFSET = 24;
const CONTROL_PANEL_WINDOW_TOP_OFFSET = 60;
let lastEnrichPersistAtMs = 0;
let activeEnrichRun = null;
const autoOpenedResultsRunIds = new Set();
let lastAutoEnrichSourceRunId = "";
const ACTION_DEFAULT_TITLE = "Scrapify";
const ACTION_RUNNING_COLOR = "#127a3e";
const ACTION_STOPPING_COLOR = "#b54708";
const FOCUSED_CRAWL_MAX_PAGES = 8;
const FOCUSED_CRAWL_MAX_PATHS_PER_TYPE = 2;
const FOCUSED_CRAWL_SEED_PATHS = [
  "/contact",
  "/about",
  "/team",
  "/careers",
  "/contact-us",
  "/about-us",
  "/our-team",
  "/jobs"
];
let hiddenScanWindowId = null;
const controlPanelRestoreInFlight = new Set();

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

async function getControlPanelAnchorWindowId() {
  const data = await storageGet([CONTROL_PANEL_ANCHOR_WINDOW_KEY]).catch(() => ({}));
  const normalized = Number(data[CONTROL_PANEL_ANCHOR_WINDOW_KEY]);
  return Number.isFinite(normalized) && normalized >= 0 ? normalized : null;
}

function getControlPanelWindowBounds(anchorWindow) {
  const bounds = {
    width: CONTROL_PANEL_WINDOW_WIDTH,
    height: CONTROL_PANEL_WINDOW_HEIGHT
  };

  if (!anchorWindow || anchorWindow.type !== "normal") {
    return bounds;
  }

  const anchorLeft = Number(anchorWindow.left);
  const anchorTop = Number(anchorWindow.top);
  const anchorWidth = Number(anchorWindow.width);
  if (Number.isFinite(anchorLeft) && Number.isFinite(anchorWidth)) {
    bounds.left = anchorLeft + anchorWidth - CONTROL_PANEL_WINDOW_WIDTH - CONTROL_PANEL_WINDOW_RIGHT_OFFSET;
  }
  if (Number.isFinite(anchorTop)) {
    bounds.top = anchorTop + CONTROL_PANEL_WINDOW_TOP_OFFSET;
  }
  return bounds;
}

function withControlPanelAnchorWindow(anchorWindowId, callback) {
  const normalizedAnchorId = Number(anchorWindowId);
  if (!Number.isFinite(normalizedAnchorId) || normalizedAnchorId < 0) {
    callback(null);
    return;
  }

  chrome.windows.get(normalizedAnchorId, {}, (anchorWindow) => {
    if (chrome.runtime.lastError || !anchorWindow || anchorWindow.type !== "normal") {
      callback(null);
      return;
    }
    callback(anchorWindow);
  });
}

function restoreControlPanelWindow(windowId, anchorWindowId, callback) {
  const normalizedWindowId = Number(windowId);
  if (!Number.isFinite(normalizedWindowId) || normalizedWindowId < 0) {
    if (typeof callback === "function") {
      callback();
    }
    return;
  }

  withControlPanelAnchorWindow(anchorWindowId, (anchorWindow) => {
    const updateOptions = {
      focused: true,
      ...getControlPanelWindowBounds(anchorWindow)
    };

    chrome.windows.update(normalizedWindowId, { state: "normal" }, () => {
      chrome.windows.update(normalizedWindowId, updateOptions, () => {
        if (typeof callback === "function") {
          callback();
        }
      });
    });
  });
}

function findControlPanelPopupTab(tabs, index, callback) {
  if (!Array.isArray(tabs) || index >= tabs.length) {
    callback(null);
    return;
  }

  const candidate = tabs[index];
  const candidateWindowId = Number(candidate && candidate.windowId);
  if (!candidate || !candidate.id || !Number.isFinite(candidateWindowId) || candidateWindowId < 0) {
    findControlPanelPopupTab(tabs, index + 1, callback);
    return;
  }

  chrome.windows.get(candidateWindowId, {}, (windowRef) => {
    if (!chrome.runtime.lastError && windowRef && windowRef.type === "popup") {
      callback(candidate);
      return;
    }
    findControlPanelPopupTab(tabs, index + 1, callback);
  });
}

function controlPanelWindowNeedsRestore(windowRef) {
  if (!windowRef || windowRef.type !== "popup") {
    return false;
  }

  const state = normalizeText(windowRef.state).toLowerCase();
  const width = Number(windowRef.width);
  const height = Number(windowRef.height);

  if (state && state !== "normal") {
    return true;
  }
  if (Number.isFinite(width) && width !== CONTROL_PANEL_WINDOW_WIDTH) {
    return true;
  }
  if (Number.isFinite(height) && height !== CONTROL_PANEL_WINDOW_HEIGHT) {
    return true;
  }
  return false;
}

function maybeRestoreLockedControlPanelWindow(windowRef) {
  const normalizedWindowId = Number(windowRef && windowRef.id);
  if (!Number.isFinite(normalizedWindowId) || normalizedWindowId < 0) {
    return;
  }
  if (!controlPanelWindowNeedsRestore(windowRef) || controlPanelRestoreInFlight.has(normalizedWindowId)) {
    return;
  }

  const controlPanelUrl = chrome.runtime.getURL(CONTROL_PANEL_PATH);
  chrome.tabs.query({ windowId: normalizedWindowId, url: `${controlPanelUrl}*` }, (tabs) => {
    if (chrome.runtime.lastError || !Array.isArray(tabs) || tabs.length === 0) {
      return;
    }

    controlPanelRestoreInFlight.add(normalizedWindowId);
    void getControlPanelAnchorWindowId()
      .catch(() => null)
      .then((anchorWindowId) => {
        restoreControlPanelWindow(normalizedWindowId, anchorWindowId, () => {
          controlPanelRestoreInFlight.delete(normalizedWindowId);
        });
      });
  });
}

function openControlPanelWindow(url, anchorWindowId) {
  withControlPanelAnchorWindow(anchorWindowId, (anchorWindow) => {
    const createOptions = {
      url,
      type: "popup",
      focused: true,
      state: "normal",
      incognito: anchorWindow && typeof anchorWindow.incognito === "boolean"
        ? anchorWindow.incognito
        : isCurrentContextIncognito(),
      ...getControlPanelWindowBounds(anchorWindow)
    };

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

    const candidates = Array.isArray(tabs)
      ? tabs.filter((tab) => tab && tab.id && tabMatchesCurrentContext(tab))
      : [];

    findControlPanelPopupTab(candidates, 0, (existing) => {
      if (!existing || !existing.id) {
        openControlPanelWindow(controlPanelUrl, normalizedAnchorId);
        return;
      }

      chrome.tabs.update(existing.id, { active: true }, () => {});
      restoreControlPanelWindow(existing.windowId, normalizedAnchorId);
    });
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

  if (message.type === MSG.OPEN_RESULTS_VIEWER) {
    openOrFocusResultsPage(normalizeText(message.runId));
    safeSendResponse(sendResponse, { ok: true });
    return false;
  }

  if (message.type === MSG.SCRAPE_DONE) {
    handleScrapeDone(message)
      .then(() => {
        safeSendResponse(sendResponse, { ok: true });
      })
      .catch((error) => {
        console.warn("[scrape:done] post-processing failed", error && error.message ? error.message : error);
        safeSendResponse(sendResponse, {
          ok: false,
          error: error && error.message ? error.message : "Post-scrape processing failed"
        });
      });
    return true;
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

chrome.windows.onBoundsChanged.addListener((windowRef) => {
  maybeRestoreLockedControlPanelWindow(windowRef);
});

chrome.windows.onRemoved.addListener((windowId) => {
  controlPanelRestoreInFlight.delete(Number(windowId));
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
  const optionFilters = options.filters && typeof options.filters === "object" ? options.filters : {};
  const hasEmailFilter = optionFilters.hasEmail != null ? optionFilters.hasEmail === true : options.requireEmail === true;
  const outputFilters = normalizeOutputFilters({
    ...optionFilters,
    hasEmail: hasEmailFilter
  });
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
    const contactGoals = normalizeContactGoals(options.contactGoals);
    const leadDiscoveryEnabled = options.leadDiscoveryEnabled === true;
    const discoverySources = {
      ...(options.discoverySources && typeof options.discoverySources === "object" ? options.discoverySources : {}),
      google: leadDiscoveryEnabled,
      linkedin: false,
      yelp: false
    };
    let result = await enrichRows(rows, {
      ...options,
      requireEmail: outputFilters.hasEmail === true,
      contactGoals,
      leadDiscoveryEnabled,
      discoverySources,
      shouldStop: () => runControl.stopRequested === true,
      onScanTabChange: (tabId) => {
        runControl.scanTabId = Number.isFinite(Number(tabId)) ? Number(tabId) : null;
      }
    });
    if (Array.isArray(result.rows)) {
      const filteredRows = applyOutputFilters(result.rows, outputFilters);
      const filteredOut = Math.max(0, result.rows.length - filteredRows.length);
      if (filteredOut > 0 || hasAnyActiveFilter(outputFilters)) {
        result = {
          rows: filteredRows,
          summary: {
            ...(result.summary || {}),
            filtered_no_email: outputFilters.hasEmail === true ? filteredOut : 0,
            filtered_output: filteredOut,
            output_rows: filteredRows.length
          }
        };
      }
    }
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

async function handleScrapeDone(message) {
  const runId = normalizeText(message && message.run_id);
  const rows = Array.isArray(message && message.rows) ? message.rows : [];
  const summary = message && typeof message.summary === "object" ? message.summary : {};
  const filters = message && typeof message.filters === "object" ? message.filters : {};
  const scrapeStopped = summary.stopped === true;

  await handlePostScrape(runId, rows, { scrapeStopped, filters });
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
    maxPagesPerSite: FOCUSED_CRAWL_MAX_PAGES,
    visibleTabs: false,
    leadDiscoveryEnabled: false,
    contactGoals: { email: true, phone: true },
    discoverySources: {
      google: false
    }
  }));

  if (!settings.enrichmentEnabled || scrapeStopped) {
    const outputRows = applyOutputFilters(rows, filters);
    if (outputRows.length !== rows.length) {
      await syncScrapeSessionFiltersAndCounts(runId, filters, outputRows.length).catch(() => {});
    }
    await storageSet({ lastRows: outputRows }).catch(() => {});
    maybeAutoOpenResultsForRun(runId, scrapeStopped ? { force: true } : {});
    return;
  }

  const queuedRows = prepareRowsForEnrichment(rows, "queued");
  await storageSet({ lastRows: queuedRows }).catch(() => {});
  if (queuedRows.length > 0) {
    maybeAutoOpenResultsForRun(runId);
  }

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
      filters,
      requireEmail: filters.hasEmail === true,
      contactGoals: settings.contactGoals,
      maxSocialPages: 4,
      leadDiscoveryEnabled: settings.leadDiscoveryEnabled === true,
      discoverySources: settings.discoverySources || { google: false, linkedin: false, yelp: false },
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
  const leadDiscoveryEnabled = data.leadDiscoveryEnabled === true;
  const contactGoalEmailEnabled = data.contactGoalEmailEnabled !== false;
  const contactGoalPhoneEnabled = data.contactGoalPhoneEnabled !== false;
  const contactGoals = normalizeContactGoals({
    email: contactGoalEmailEnabled,
    phone: contactGoalPhoneEnabled
  });
  return {
    enrichmentEnabled: data.enrichmentEnabled === true,
    maxPagesPerSite: FOCUSED_CRAWL_MAX_PAGES,
    visibleTabs: data.showEnrichmentTabsEnabled === true,
    leadDiscoveryEnabled,
    contactGoals,
    discoverySources: {
      google: leadDiscoveryEnabled,
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

async function shouldAutoOpenResultsTab() {
  // The results viewer is separate from visible enrichment crawl tabs and should
  // always open for completed scrape runs.
  return true;
}

function maybeAutoOpenResultsForRun(runId, optionsInput) {
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const force = options.force === true;
  const targetRunId = normalizeText(runId);
  const openViewer = () => {
    if (!targetRunId) {
      openOrFocusResultsPage("");
      return;
    }

    if (!force && autoOpenedResultsRunIds.has(targetRunId)) {
      return;
    }

    autoOpenedResultsRunIds.add(targetRunId);
    openOrFocusResultsPage(targetRunId);
  };

  if (force) {
    openViewer();
    return;
  }

  void shouldAutoOpenResultsTab().then((allowed) => {
    if (!allowed) {
      return;
    }

    openViewer();
  }).catch(() => {});
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
    const listingFacebook = pickRowFacebookFallback(sourceRow);
    const facebookCouldBe = normalizeFacebookProfileUrl(sourceRow.facebook_could_be);
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
      listing_facebook: listingFacebook,
      facebook_could_be: facebookCouldBe,
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
  const listingFacebook = pickRowFacebookFallback(base);
  const facebookCouldBe = normalizeFacebookProfileUrl(base.facebook_could_be);

  return {
    ...base,
    phone: rawFallbackPhone || rawListingPhone,
    listing_phone: rawListingPhone,
    website_phone: rawWebsitePhone,
    website_phone_source: normalizeText(base.website_phone_source),
    owner_name: safeOwnerName,
    owner_title: safeOwnerTitle,
    listing_facebook: listingFacebook,
    facebook_could_be: facebookCouldBe,
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

function normalizeFacebookProfileUrl(url) {
  const normalized = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url);
  if (!normalized) return "";
  return shouldScanSocialUrl(normalized) ? normalized : "";
}

function pickRowFacebookFallback(row) {
  const source = row && typeof row === "object" ? row : {};
  const candidates = [
    source.listing_facebook,
    source.facebook,
    source.facebook_url,
    source.social_facebook,
    ...parseStoredSocialLinks(source.social_links)
  ];
  for (const candidate of candidates) {
    const normalized = normalizeFacebookProfileUrl(candidate);
    if (normalized) {
      return normalized;
    }
  }
  return "";
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
  const hostA = normalizeWebsiteHostKey(urlA);
  const hostB = normalizeWebsiteHostKey(urlB);
  if (!hostA || !hostB) return false;
  return hostA === hostB;
}

function extractSocialProfileKeyFromUrl(url, hostInput) {
  const normalized = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url);
  if (!normalized) return "";
  let parsed = null;
  try {
    parsed = new URL(normalized);
  } catch (_error) {
    return "";
  }

  const host = normalizeText(hostInput || parsed.hostname).toLowerCase().replace(/^www\./, "");
  if (!host) return "";
  const segments = normalizeText(parsed.pathname || "")
    .toLowerCase()
    .replace(/\/+$/, "")
    .split("/")
    .filter(Boolean);
  if (segments.length === 0) return "";

  if (host.includes("facebook.com")) {
    const first = segments[0];
    if (!first) return "";
    if (first === "profile.php") {
      const profileId = normalizeText(parsed.searchParams.get("id")).toLowerCase();
      return profileId ? `id:${profileId}` : "";
    }
    if (first === "pg") {
      return normalizeText(segments[1]).toLowerCase();
    }
    if (first === "pages") {
      const numericId = segments.find((segment) => /^\d{5,}$/.test(segment));
      return numericId ? `id:${numericId}` : normalizeText(segments[1]).toLowerCase();
    }
    const reserved = new Set([
      "about",
      "sharer",
      "share.php",
      "dialog",
      "plugins",
      "privacy",
      "policies",
      "terms",
      "help",
      "legal",
      "settings",
      "login",
      "recover",
      "checkpoint",
      "watch",
      "reel",
      "story.php",
      "groups",
      "events",
      "marketplace"
    ]);
    if (reserved.has(first)) return "";
    return first;
  }

  if (
    host.includes("instagram.com") ||
    host.includes("linkedin.com") ||
    host.includes("x.com") ||
    host.includes("twitter.com") ||
    host.includes("youtube.com") ||
    host.includes("tiktok.com") ||
    host.includes("threads.net")
  ) {
    const first = normalizeText(segments[0]).toLowerCase();
    const second = normalizeText(segments[1]).toLowerCase();
    if (!first) return "";
    if (host.includes("linkedin.com")) {
      if ((first === "company" || first === "in" || first === "school" || first === "showcase") && second) {
        return `${first}/${second}`;
      }
      if (first === "feed" || first === "jobs" || first === "posts" || first === "events") return "";
      return first;
    }

    const reserved = new Set([
      "p",
      "reel",
      "explore",
      "accounts",
      "about",
      "legal",
      "privacy",
      "terms",
      "stories",
      "i",
      "share",
      "home",
      "watch",
      "shorts",
      "channel"
    ]);
    if (reserved.has(first)) return "";
    return first;
  }

  return "";
}

function normalizeWebsiteHostKey(url) {
  const normalized = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url);
  if (!normalized) return "";
  const host = normalizeText(hostnameForUrl(normalized)).toLowerCase();
  if (!host) return "";
  const normalizedHost = host.replace(/^www\./, "");
  const socialKey = extractSocialProfileKeyFromUrl(normalized, normalizedHost);
  if (socialKey) {
    return `${normalizedHost}/${socialKey}`;
  }
  return normalizedHost;
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

function isWebsiteLikelyForBusinessNameGuard(url, businessName) {
  if (isLikelySocialBusinessProfileUrl(url)) {
    return true;
  }
  const host = normalizeWebsiteHostKey(url);
  if (!host) return false;
  const hostRoot = host.split(".")[0].replace(/[^a-z0-9]/g, "");
  if (!hostRoot) return false;

  const tokens = normalizeBusinessNameForWebsiteGuard(businessName)
    .split(/\s+/)
    .filter((token) => token.length >= 3);
  if (tokens.length === 0) {
    return true;
  }
  return tokens.some((token) => hostRoot.includes(token) || token.includes(hostRoot));
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

function registerOrRejectWebsiteForRow(url, row, registryInput, optionsInput) {
  const registry = registryInput instanceof Map ? registryInput : new Map();
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const trustedSource = options.trustedSource === true;
  const normalized = normalizeBusinessWebsiteUrl(url);
  if (!normalized) return "";

  const host = normalizeWebsiteHostKey(normalized);
  if (!host) return "";

  const source = row && typeof row === "object" ? row : {};
  const placeId = normalizeText(source.place_id);
  const mapsUrl = normalizeMapsUrl(source.maps_url || "");
  const businessName = normalizeText(source.name);
  const isSocialProfile = isLikelySocialBusinessProfileUrl(normalized);
  if (!trustedSource && !isWebsiteLikelyForBusinessNameGuard(normalized, businessName)) {
    return "";
  }
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

  // Permit host reuse for normal business websites (many leads share one domain),
  // but keep strict identity guard for social profile URLs.
  if (!sameIdentity && isSocialProfile) {
    return "";
  }

  if (placeId) existing.placeIds.add(placeId);
  if (mapsUrl) existing.mapsUrls.add(mapsUrl);
  return normalized;
}

function parseStoredSocialLinks(value) {
  const raw = normalizeText(value);
  if (!raw) return [];
  const parts = raw
    .split(/\s*\|\s*|\s*,\s*|\n+/)
    .map((entry) => normalizeBusinessWebsiteUrl(entry) || normalizeWebsiteUrl(entry))
    .filter(Boolean);
  return Array.from(new Set(parts));
}

function mergeStoredSocialLinks(existingValue, additionsInput) {
  const merged = new Set(parseStoredSocialLinks(existingValue));
  const additions = Array.isArray(additionsInput) ? additionsInput : [additionsInput];
  for (const candidate of additions) {
    const normalized = normalizeFacebookProfileUrl(candidate) || normalizeBusinessWebsiteUrl(candidate) || normalizeWebsiteUrl(candidate);
    if (normalized) {
      merged.add(normalized);
    }
  }
  return Array.from(merged).join(" | ");
}

async function enrichRows(rows, options) {
  const config = options && typeof options === "object" ? options : {};
  const maxPagesPerSite = FOCUSED_CRAWL_MAX_PAGES;
  const timeoutMs = clampInt(config.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = config.visibleTabs === true;
  const contactGoals = normalizeContactGoals(config.contactGoals);
  const maxSocialPages = clampInt(config.maxSocialPages, 0, 8, 4);
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
    let website = registerOrRejectWebsiteForRow(sourceRow.website, sourceRow, websiteHostOwners, {
      trustedSource: true
    });
    const listingFacebook = pickRowFacebookFallback(sourceRow);
    const sourceSocialLinks = Array.from(new Set([listingFacebook, ...parseStoredSocialLinks(sourceRow.social_links)].filter(Boolean)));
    enrichedRow.website = website;
    enrichedRow.listing_facebook = listingFacebook;

    let rowPagesVisited = 0;
    let rowPagesDiscovered = 0;
    let rowSocialScanned = 0;
    let rowBlocked = false;
    let rowCurrentUrl = website || listingFacebook;
    let rowDiscoveryEmailRecovered = false;
    let preDiscoveryRan = false;
    const initialIntent = deriveGoalScanIntent(enrichedRow, contactGoals);
    const needsAnyGoalData = initialIntent.needsEmail || initialIntent.needsPhone;

    if (!needsAnyGoalData) {
      const currentStatus = normalizeText(enrichedRow.website_scan_status).toLowerCase();
      if (
        currentStatus === "" ||
        currentStatus === "queued" ||
        currentStatus === "not_requested" ||
        currentStatus === "init" ||
        currentStatus === "running"
      ) {
        enrichedRow.website_scan_status = "enriched";
      }
      summary.enriched += 1;
      summary.processed += 1;
      outputRows.push(enrichedRow);
      emitEnrichProgress(summary, {
        currentName: sourceRow.name,
        currentUrl: rowCurrentUrl,
        phase: "skip",
        leadSignalText: "Skipped: selected contact goal already met",
        leadSignalTone: "info",
        sitePagesVisited: summary.pages_visited,
        sitePagesDiscovered: summary.pages_discovered
      });
      continue;
    }

    const runSiteScan = async (targetUrl, phasePrefix) => {
      const rowIntent = deriveGoalScanIntent(enrichedRow, contactGoals);
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
        scanSocialLinks: true,
        maxSocialPages,
        skipSitemapLookup: phasePrefix === "discovery",
        intent: rowIntent,
        businessName: sourceRow.name,
        businessCategory: sourceRow.category,
        businessAddress: sourceRow.address || sourceRow.source_query,
        sourceQuery: sourceRow.source_query,
        businessWebsite: targetUrl,
        discoveredWebsite: enrichedRow.discovered_website,
        preferFacebookEmail: contactGoals.email === true,
        seedSocialLinks: sourceSocialLinks,
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
      if (!website && sourceSocialLinks.length > 0) {
        const socialFallbackQueue = prioritizeSocialLinks(sourceSocialLinks)
          .filter(shouldScanSocialUrl)
          .slice(0, Math.max(1, maxSocialPages));
        for (const socialCandidate of socialFallbackQueue) {
          const recoveredFromSocial = registerOrRejectWebsiteForRow(socialCandidate, enrichedRow, websiteHostOwners, {
            trustedSource: true
          });
          if (!recoveredFromSocial) continue;
          website = recoveredFromSocial;
          enrichedRow.website = recoveredFromSocial;
          rowCurrentUrl = recoveredFromSocial;
          const discoveryStatus = normalizeText(enrichedRow.discovery_status).toLowerCase();
          if (discoveryStatus === "" || discoveryStatus === "not_requested") {
            enrichedRow.discovery_status = "recovered_website";
            enrichedRow.discovery_source = "gbp_social";
          }
          break;
        }
      }

      const shouldRecoverMissingWebsite = !website && discovery.enabled === true && discovery.recoverMissingWebsite === true;
      if (shouldRecoverMissingWebsite) {
        preDiscoveryRan = true;
        const discoveryResult = await runLeadDiscovery(enrichedRow, {
          ...discovery,
          enabled: true,
          sources: discovery.sources,
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
          if (recoveredWebsite) {
            rowCurrentUrl = recoveredWebsite;
          } else {
            enrichedRow.discovered_website = "";
            if (normalizeText(enrichedRow.discovery_status).toLowerCase() === "recovered_website") {
              enrichedRow.discovery_status = "no_match";
            }
          }
        }
      }

      if (!website) {
        const noWebsiteReason = "no_website";
        const noWebsiteSignal = "Skipped: no website";
        enrichedRow.website_scan_status = "no_website";
        enrichedRow.no_email_reason = noWebsiteReason;
        enrichedRow.email_source_url = "";
        enrichedRow.email_confidence = "";
        enrichedRow.site_pages_visited = 0;
        enrichedRow.site_pages_discovered = 0;
        enrichedRow.social_pages_scanned = 0;
        enrichedRow.social_links = sourceSocialLinks.join(" | ");
        enrichedRow.website_phone = "";
        enrichedRow.website_phone_source = "";
        summary.skipped += 1;
        summary.processed += 1;
        outputRows.push(enrichedRow);
        emitEnrichProgress(summary, {
          currentName: sourceRow.name,
          currentUrl: rowCurrentUrl,
          phase: "skip",
          leadSignalText: noWebsiteSignal,
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

      if (
        website &&
        listingFacebook &&
        !sameWebsiteHost(listingFacebook, website)
      ) {
        const followupIntent = deriveGoalScanIntent(enrichedRow, contactGoals);
        const needsFacebookEmailAfterSite =
          contactGoals.email === true &&
          normalizeText(enrichedRow.primary_email_source).toLowerCase() !== "facebook";
        const needsPhoneAfterSite = followupIntent.needsPhone;
        if (needsFacebookEmailAfterSite || needsPhoneAfterSite) {
          const socialFallbackScan = await runSiteScan(listingFacebook, "listing_social");
          const hasEmailFromFallback = scanHasAnyEmail(socialFallbackScan);
          const hasPhoneFromFallback = Boolean(sanitizePhoneText(socialFallbackScan.primaryPhone));
          applyScanResultToRow(enrichedRow, socialFallbackScan, {
            overwrite: hasEmailFromFallback || hasPhoneFromFallback,
            overwriteWithoutEmail: hasEmailFromFallback || hasPhoneFromFallback
          });
        }
      }

      const confirmedFacebookAfterSite = pickRowFacebookFallback(enrichedRow);
      if (confirmedFacebookAfterSite) {
        enrichedRow.facebook_could_be = "";
      }
      const shouldLookupFacebookOutsideWebsite =
        !confirmedFacebookAfterSite &&
        contactGoals.email === true &&
        normalizeText(enrichedRow.primary_email_source).toLowerCase() !== "facebook";
      if (shouldLookupFacebookOutsideWebsite) {
        emitEnrichProgress(summary, {
          currentName: sourceRow.name,
          currentUrl: rowCurrentUrl,
          phase: "facebook_lookup",
          sitePagesVisited: summary.pages_visited + rowPagesVisited,
          sitePagesDiscovered: summary.pages_discovered + rowPagesDiscovered,
          socialScanned: summary.social_scanned + rowSocialScanned
        });

        const facebookLookup = await discoverFacebookViaGoogleSearch(enrichedRow, {
          timeoutMs,
          visibleTabs,
          shouldStop: config.shouldStop,
          onScanTabChange: config.onScanTabChange
        });
        const confirmedFacebook = normalizeFacebookProfileUrl(facebookLookup.confirmedUrl);
        const possibleFacebook = normalizeFacebookProfileUrl(facebookLookup.possibleUrl);

        if (confirmedFacebook) {
          const hadEmailBeforeFacebookLookup = rowHasAnyEmail(enrichedRow);
          enrichedRow.listing_facebook = confirmedFacebook;
          enrichedRow.facebook_could_be = "";
          enrichedRow.social_links = mergeStoredSocialLinks(enrichedRow.social_links, [confirmedFacebook]);
          rowCurrentUrl = confirmedFacebook;

          const facebookIntent = deriveGoalScanIntent(enrichedRow, contactGoals);
          const needsFacebookEmailAfterLookup =
            contactGoals.email === true &&
            normalizeText(enrichedRow.primary_email_source).toLowerCase() !== "facebook";
          if (needsFacebookEmailAfterLookup || facebookIntent.needsPhone) {
            const externalFacebookScan = await runSiteScan(confirmedFacebook, "listing_social");
            const facebookScanHasEmail = scanHasAnyEmail(externalFacebookScan);
            const facebookScanHasPhone = Boolean(sanitizePhoneText(externalFacebookScan.primaryPhone));
            applyScanResultToRow(enrichedRow, externalFacebookScan, {
              overwrite: facebookScanHasEmail || facebookScanHasPhone,
              overwriteWithoutEmail: facebookScanHasEmail || facebookScanHasPhone
            });

            if (!hadEmailBeforeFacebookLookup && facebookScanHasEmail) {
              rowDiscoveryEmailRecovered = true;
              enrichedRow.discovery_status = "recovered_email";
              enrichedRow.discovery_source = "google_facebook";
              enrichedRow.discovery_query = normalizeText(facebookLookup.query);
            }
          }
        } else if (possibleFacebook && !normalizeFacebookProfileUrl(enrichedRow.facebook_could_be)) {
          enrichedRow.facebook_could_be = possibleFacebook;
        }
      }

      const postSiteIntent = deriveGoalScanIntent(enrichedRow, contactGoals);
      const stillMissingAnyGoal = postSiteIntent.needsEmail || postSiteIntent.needsPhone;
      if (discovery.enabled && stillMissingAnyGoal && !preDiscoveryRan) {
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

      const shouldLookupOwner = discovery.enabled === true && !normalizeText(enrichedRow.owner_name);
      if (shouldLookupOwner) {
        emitEnrichProgress(summary, {
          currentName: sourceRow.name,
          currentUrl: rowCurrentUrl,
          phase: "owner_lookup",
          sitePagesVisited: summary.pages_visited + rowPagesVisited,
          sitePagesDiscovered: summary.pages_discovered + rowPagesDiscovered,
          socialScanned: summary.social_scanned + rowSocialScanned
        });
        try {
          const ownerRecovery = await recoverOwnerViaGoogle(enrichedRow, {
            timeoutMs,
            visibleTabs,
            shouldStop: config.shouldStop,
            onScanTabChange: config.onScanTabChange
          });
          if (ownerRecovery && ownerRecovery.found) {
            enrichedRow.owner_name = normalizeText(ownerRecovery.ownerName);
            enrichedRow.owner_title = normalizeText(ownerRecovery.ownerTitle);
            enrichedRow.owner_confidence = formatConfidence(ownerRecovery.ownerConfidence);
            if (!normalizeText(enrichedRow.email_source_url) && normalizeText(ownerRecovery.sourceUrl)) {
              enrichedRow.email_source_url =
                normalizeBusinessWebsiteUrl(ownerRecovery.sourceUrl) ||
                normalizeWebsiteUrl(ownerRecovery.sourceUrl) ||
                "";
            }
            rowCurrentUrl = normalizeBusinessWebsiteUrl(ownerRecovery.sourceUrl) || rowCurrentUrl;
          }
        } catch (ownerLookupError) {
          if (isEnrichStopError(ownerLookupError)) {
            throw ownerLookupError;
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
  const primaryPhone = sanitizePhoneText((row && row.phone) || (row && row.website_phone));
  const emailType = normalizeText(row && row.primary_email_type).toLowerCase();
  const emailSource = normalizeText(row && row.primary_email_source);
  const scanStatus = normalizeText(row && row.website_scan_status).toLowerCase();
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

  if (primaryPhone) {
    return {
      text: "Saved phone number",
      tone: "success"
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

  if (discoveryStatus === "recovered_website") {
    return { text: `Recovered website (${sourceLabel(discoverySource)})`, tone: "info" };
  }

  return { text: "Skipped: no public contact details found", tone: "warn" };
}

function sourceLabel(source) {
  const value = normalizeText(source).toLowerCase();
  if (!value) return "website";
  if (value === "google") return "Google Search";
  if (value === "google_facebook") return "Google Facebook";
  return value;
}

function normalizeDiscoveryOptions(options) {
  const raw = options && typeof options === "object" ? options : {};
  const budgetRaw = raw.discoveryBudget && typeof raw.discoveryBudget === "object" ? raw.discoveryBudget : {};
  const enabled = raw.leadDiscoveryEnabled === true;
  const recoverMissingWebsite = enabled && raw.recoverMissingWebsite !== false;

  return {
    enabled,
    recoverMissingWebsite,
    sources: {
      google: enabled,
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
    f.hasPhone === true ||
    f.hasEmail === true
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
    hasPhone: source.hasPhone === true,
    hasEmail: source.hasEmail === true || source.requireEmailForLeads === true || source.requireEmail === true
  });
}

function applyScrapeFilters(rows, filters) {
  if (!Array.isArray(rows)) return [];
  const scrapeStageFilters = toScrapeStageFilters(filters);
  if (!hasAnyActiveFilter(scrapeStageFilters)) {
    return rows;
  }
  return rows.filter((row) => applyFilters(row, scrapeStageFilters));
}

function applyOutputFilters(rows, filters) {
  if (!Array.isArray(rows)) return [];
  const outputFilters = normalizeOutputFilters(filters);
  if (!hasAnyActiveFilter(outputFilters)) {
    return rows;
  }
  return rows.filter((row) => applyFilters(row, outputFilters));
}

function toScrapeStageFilters(filtersLike) {
  const base = normalizeScrapeFilters(filtersLike);
  return {
    ...base,
    hasEmail: false
  };
}

function normalizeOutputFilters(filtersLike) {
  return normalizeScrapeFilters(filtersLike);
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

const ADDRESS_MATCH_NOISE_TOKENS = new Set([
  ...LOCATION_NOISE_TOKENS,
  "address",
  "building",
  "bldg",
  "suite",
  "ste",
  "unit",
  "floor",
  "fl"
]);

function normalizePhoneDigitsForMatch(value) {
  const digits = sanitizePhoneText(value).replace(/\D/g, "");
  if (!digits) return "";
  if (digits.length === 11 && digits.startsWith("1")) {
    return digits.slice(1);
  }
  return digits;
}

function collectPhoneMatchKeys(valuesInput) {
  const values = Array.isArray(valuesInput) ? valuesInput : [valuesInput];
  const out = [];
  const seen = new Set();
  for (const value of values) {
    const normalized = normalizePhoneDigitsForMatch(value);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
}

function tokenizeAddressMatchText(value) {
  const text = normalizeText(value).toLowerCase().replace(/[^a-z0-9\s]/g, " ");
  if (!text) return [];

  const out = [];
  const seen = new Set();
  for (const part of text.split(/\s+/)) {
    const token = /^[0-9]+[a-z]?$/i.test(part) ? part : normalizeSemanticToken(part);
    if (!token || token.length < 2) continue;
    if (ADDRESS_MATCH_NOISE_TOKENS.has(token)) continue;
    if (seen.has(token)) continue;
    seen.add(token);
    out.push(token);
  }
  return out;
}

function extractAddressNumberToken(value) {
  const match = normalizeText(value).toLowerCase().match(/\b\d{1,6}[a-z]?\b/);
  return match ? match[0] : "";
}

function scoreAddressEvidenceMatch(referenceInput, candidatesInput) {
  const reference = normalizeText(referenceInput);
  const candidates = Array.isArray(candidatesInput) ? candidatesInput : [candidatesInput];
  if (!reference) {
    return { matched: false, score: 0, candidate: "" };
  }

  const referenceTokens = tokenizeAddressMatchText(reference);
  if (referenceTokens.length === 0) {
    return { matched: false, score: 0, candidate: "" };
  }

  const referenceNumber = extractAddressNumberToken(reference);
  let best = { matched: false, score: 0, candidate: "" };

  for (const rawCandidate of candidates) {
    const candidate = normalizeText(rawCandidate);
    if (!candidate) continue;
    const candidateTokens = tokenizeAddressMatchText(candidate);
    if (candidateTokens.length === 0) continue;

    const overlap = tokenOverlapRatio(referenceTokens, candidateTokens);
    const matchCount = tokenMatchCount(referenceTokens, candidateTokens);
    const numberMatch = referenceNumber && extractAddressNumberToken(candidate) === referenceNumber;
    let score = overlap;
    if (numberMatch) score += 0.28;
    if (matchCount >= 3) score += 0.12;
    if (matchCount >= 4) score += 0.08;

    const matched =
      Boolean(numberMatch && matchCount >= 2 && overlap >= 0.34) ||
      Boolean(numberMatch && matchCount >= 3) ||
      Boolean(matchCount >= 4 && overlap >= 0.72);

    if (score > best.score || (score === best.score && matched && !best.matched)) {
      best = {
        matched,
        score,
        candidate
      };
    }
  }

  return best;
}

function buildFacebookSearchQueries(row) {
  const source = row && typeof row === "object" ? row : {};
  const name = normalizeText(source.name);
  if (!name) return [];

  const address = normalizeText(source.address);
  const locationHint = address || normalizeText(source.source_query);
  const phone = collectPhoneMatchKeys([
    source.phone,
    source.listing_phone,
    source.website_phone
  ])[0] || "";

  const candidates = [];
  if (address && phone) {
    candidates.push(`${name} ${address} ${phone} facebook`);
  }
  if (address) {
    candidates.push(`${name} ${address} facebook`);
  }
  if (locationHint && phone) {
    candidates.push(`${name} ${locationHint} ${phone} facebook`);
  }
  if (locationHint) {
    candidates.push(`${name} ${locationHint} facebook`);
  }
  if (phone) {
    candidates.push(`${name} ${phone} facebook`);
  }
  candidates.push(`${name} facebook`);

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

function normalizeFacebookDiscoveryCandidateUrl(url) {
  const normalized = normalizeFacebookProfileUrl(url);
  if (!normalized) return "";
  const probeUrls = buildSocialProbeUrls(normalized);
  return normalizeFacebookProfileUrl(probeUrls[0] || normalized);
}

function evaluateFacebookSearchCandidate(candidateInput, row, semanticContextInput) {
  const candidate = candidateInput && typeof candidateInput === "object" ? candidateInput : {};
  const rowData = row && typeof row === "object" ? row : {};
  const pageData = candidate.pageData && typeof candidate.pageData === "object" ? candidate.pageData : {};
  const semanticProfile = pageData.semanticProfile && typeof pageData.semanticProfile === "object"
    ? pageData.semanticProfile
    : {};
  const semanticContext = semanticContextInput && typeof semanticContextInput === "object"
    ? semanticContextInput
    : buildBusinessSemanticContext({
      businessName: normalizeText(rowData.name),
      businessCategory: normalizeText(rowData.category),
      businessAddress: normalizeText(rowData.address || rowData.source_query),
      businessWebsite: normalizeText(rowData.website),
      discoveredWebsite: normalizeText(rowData.discovered_website)
    });
  const normalizedUrl = normalizeFacebookDiscoveryCandidateUrl(candidate.url);
  if (!normalizedUrl) {
    return { status: "reject", url: "", score: 0, matchedOn: "" };
  }

  const semanticEvidence = scoreBusinessSemanticEvidence({
    url: normalizedUrl,
    title: normalizeText(candidate.title),
    snippet: normalizeText(candidate.snippet),
    pageData
  }, semanticContext);
  const titleSimilarity = Math.max(
    businessNameSimilarityScore(rowData.name, normalizeText(candidate.title)),
    businessNameSimilarityScore(rowData.name, normalizeText(semanticProfile.pageTitle)),
    businessNameSimilarityScore(rowData.name, Array.isArray(semanticProfile.orgNames) ? semanticProfile.orgNames.join(" | ") : "")
  );
  const rowPhoneKeys = collectPhoneMatchKeys([
    rowData.phone,
    rowData.listing_phone,
    rowData.website_phone
  ]);
  const pagePhoneKeys = collectPhoneMatchKeys(Array.isArray(pageData.phones) ? pageData.phones : []);
  const phoneMatched = rowPhoneKeys.some((phoneKey) => pagePhoneKeys.includes(phoneKey));
  const addressMatch = scoreAddressEvidenceMatch(rowData.address, [
    ...(Array.isArray(pageData.addresses) ? pageData.addresses : []),
    semanticProfile.metaDescription,
    semanticProfile.headingText,
    semanticProfile.textSample
  ]);

  let score = semanticEvidence.score;
  if (phoneMatched) score += 0.18;
  if (addressMatch.matched) score += 0.14;
  score += Math.min(0.08, titleSimilarity * 0.08);
  const boundedScore = Math.min(0.99, Math.max(0, score));

  if ((phoneMatched && (semanticEvidence.score >= 0.28 || titleSimilarity >= 0.45)) || addressMatch.matched) {
    return {
      status: "confirmed",
      url: normalizedUrl,
      score: boundedScore,
      matchedOn: phoneMatched ? "phone" : "address"
    };
  }

  if (semanticEvidence.score >= 0.78 && titleSimilarity >= 0.56) {
    return {
      status: "probable",
      url: normalizedUrl,
      score: boundedScore,
      matchedOn: ""
    };
  }

  return {
    status: "reject",
    url: normalizedUrl,
    score: boundedScore,
    matchedOn: ""
  };
}

async function discoverFacebookViaGoogleSearch(row, optionsInput) {
  const rowData = row && typeof row === "object" ? row : {};
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const timeoutMs = clampInt(options.timeoutMs, 5000, 30000, 12000);
  const visibleTabs = options.visibleTabs === true;
  const queries = buildFacebookSearchQueries(rowData).slice(0, 5);
  if (queries.length === 0) {
    return { attempted: false, confirmedUrl: "", possibleUrl: "", query: "", matchedOn: "", confidence: 0 };
  }

  const semanticContext = buildBusinessSemanticContext({
    businessName: normalizeText(rowData.name),
    businessCategory: normalizeText(rowData.category),
    businessAddress: normalizeText(rowData.address || rowData.source_query),
    businessWebsite: normalizeText(rowData.website),
    discoveredWebsite: normalizeText(rowData.discovered_website)
  });
  const seenUrls = new Set();
  let bestProbable = null;

  for (const query of queries) {
    if (isEnrichStopRequested(options)) {
      throw createEnrichStopError();
    }

    const googleCandidates = await searchGoogleCandidates(query, {
      timeoutMs,
      visibleTabs,
      maxResults: 2,
      siteFilter: "facebook.com",
      includeDirectoryHosts: true,
      shouldStop: options.shouldStop,
      onScanTabChange: options.onScanTabChange
    });

    for (const candidate of googleCandidates) {
      const normalizedUrl = normalizeFacebookDiscoveryCandidateUrl(candidate && candidate.url);
      if (!normalizedUrl || seenUrls.has(normalizedUrl)) continue;
      seenUrls.add(normalizedUrl);

      const pageData = await openTabAndExtractData(
        normalizedUrl,
        {
          timeoutMs,
          visibleTabs,
          shouldStop: options.shouldStop,
          onScanTabChange: options.onScanTabChange
        },
        (tabId, extractionTimeout) => executeExtraction(tabId, extractionTimeout, {
          parseSourceHtml: false
        })
      ).catch(() => null);
      if (!pageData) continue;

      const evaluation = evaluateFacebookSearchCandidate({
        url: normalizedUrl,
        title: normalizeText(candidate && candidate.title),
        snippet: normalizeText(candidate && candidate.snippet),
        pageData
      }, rowData, semanticContext);

      if (evaluation.status === "confirmed") {
        return {
          attempted: true,
          confirmedUrl: evaluation.url,
          possibleUrl: "",
          query,
          matchedOn: evaluation.matchedOn,
          confidence: evaluation.score
        };
      }

      if (evaluation.status === "probable" && (!bestProbable || evaluation.score > bestProbable.confidence)) {
        bestProbable = {
          url: evaluation.url,
          query,
          confidence: evaluation.score
        };
      }
    }
  }

  return {
    attempted: true,
    confirmedUrl: "",
    possibleUrl: bestProbable ? bestProbable.url : "",
    query: bestProbable ? bestProbable.query : (queries[queries.length - 1] || ""),
    matchedOn: "",
    confidence: bestProbable ? bestProbable.confidence : 0
  };
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
    const links = await promiseWithTimeout(
      Promise.resolve(extractFn(tab.id, timeoutMs)),
      timeoutMs,
      "Timed out while extracting links"
    );
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
    return await promiseWithTimeout(
      Promise.resolve(extractFn(tab.id, timeoutMs)),
      timeoutMs,
      "Timed out while extracting page data"
    );
  } finally {
    if (typeof options.onScanTabChange === "function") {
      options.onScanTabChange(null);
    }
    if (tab && tab.id != null) {
      await closeTab(tab.id).catch(() => {});
    }
  }
}

function executeGoogleResultsExtraction(tabId, maxResults, timeoutMs) {
  const task = new Promise((resolve, reject) => {
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
  if (timeoutMs == null) {
    return task;
  }
  return promiseWithTimeout(
    task,
    clampInt(timeoutMs, 1500, 120000, 12000),
    "Timed out while parsing Google results"
  );
}

function executeDirectoryResultsExtraction(tabId, timeoutMs) {
  const task = new Promise((resolve, reject) => {
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
  if (timeoutMs == null) {
    return task;
  }
  return promiseWithTimeout(
    task,
    clampInt(timeoutMs, 1500, 120000, 12000),
    "Timed out while parsing directory links"
  );
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

function normalizeContactGoals(input) {
  const raw = input && typeof input === "object" ? input : {};
  const wantsEmail = raw.email !== false;
  const wantsPhone = raw.phone !== false;
  return {
    email: wantsEmail,
    phone: wantsPhone
  };
}

function deriveGoalScanIntent(row, goalsInput) {
  const baseIntent = deriveScanIntent(row);
  const goals = normalizeContactGoals(goalsInput);
  return {
    needsEmail: goals.email === true && baseIntent.needsEmail === true,
    needsOwner: false,
    needsPhone: goals.phone === true && baseIntent.needsPhone === true
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

function focusedCrawlPageType(url) {
  const normalized = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url) || normalizeText(url);
  if (!normalized) return "";

  let lowerPath = "";
  try {
    const parsed = new URL(normalized);
    lowerPath = normalizeText(`${parsed.pathname} ${parsed.search}`).toLowerCase();
  } catch (_error) {
    lowerPath = normalizeText(normalized).toLowerCase();
  }
  if (!lowerPath) return "";

  if (/(^|[\/\s_-])(contact(?:-?us)?|get-in-touch|reach-us|call-us)([\/\s_-]|$)/i.test(lowerPath)) {
    return "contact";
  }
  if (/(^|[\/\s_-])(team|our-team|ourteam|leadership|management|staff)([\/\s_-]|$)/i.test(lowerPath)) {
    return "team";
  }
  if (/(^|[\/\s_-])(about(?:-?us)?|our-story|who-we-are|company|founder|owner)([\/\s_-]|$)/i.test(lowerPath)) {
    return "about";
  }
  if (/(^|[\/\s_-])(careers?|jobs?|join-us|work-with-us|vacanc(y|ies))([\/\s_-]|$)/i.test(lowerPath)) {
    return "careers";
  }
  return "";
}

function buildFocusedRouteSeedUrls(baseOrigin) {
  const origin = normalizeText(baseOrigin);
  if (!origin) return [];

  const out = [];
  for (const path of FOCUSED_CRAWL_SEED_PATHS) {
    const seed = canonicalizeCrawlUrl(`${origin}${path}`, origin);
    if (!seed) continue;
    out.push(seed);
  }
  return out;
}

function isHighIntentPath(url, _intent) {
  return Boolean(focusedCrawlPageType(url));
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

function isFocusedCrawlTarget(url, _intent) {
  return Boolean(focusedCrawlPageType(url));
}

function shouldQueueFocusedCrawlLink(url, _score, _intent, _visitedCount, _queueCount) {
  return Boolean(focusedCrawlPageType(url));
}

function isLikelyHomepageUrl(url) {
  const normalized = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url) || normalizeText(url);
  if (!normalized) return false;
  try {
    const parsed = new URL(normalized);
    const path = normalizeText(parsed.pathname || "").toLowerCase().replace(/\/+$/, "");
    return path === "" || path === "/" || /^\/index(?:\.[a-z0-9]{2,6})?$/i.test(path);
  } catch (_error) {
    return false;
  }
}

function normalizeFocusedHintType(value) {
  const lower = normalizeText(value).toLowerCase();
  if (!lower) return "";
  if (lower === "contact" || lower.startsWith("contact")) return "contact";
  if (lower === "about" || lower.startsWith("about")) return "about";
  if (lower === "team" || lower.startsWith("team")) return "team";
  if (lower === "careers" || lower === "career" || lower.startsWith("career")) return "careers";
  return "";
}

function normalizeFocusedHintEntries(entries, intent) {
  if (!Array.isArray(entries) || entries.length === 0) return [];

  const unique = new Map();
  for (const rawEntry of entries) {
    let link = "";
    let type = "";
    let weight = 0;

    if (typeof rawEntry === "string") {
      link = rawEntry;
    } else if (rawEntry && typeof rawEntry === "object") {
      link = normalizeText(rawEntry.url || rawEntry.link || rawEntry.href);
      type = normalizeFocusedHintType(rawEntry.type || rawEntry.pageType || rawEntry.kind);
      weight = Number(rawEntry.weight || rawEntry.score || 0);
    }

    const normalizedLink = normalizeBusinessWebsiteUrl(link) || normalizeWebsiteUrl(link);
    if (!normalizedLink) continue;

    let score = crawlPriorityScore(normalizedLink, intent);
    if (type === "contact") {
      score += 12;
    } else if (type === "about") {
      score += 8;
    } else if (type === "team") {
      score += 7;
    } else if (type === "careers") {
      score += 6;
    }
    if (Number.isFinite(weight)) {
      score += Math.max(0, Math.min(5, weight));
    }

    const existing = unique.get(normalizedLink);
    if (!existing || score > existing.score || (!existing.type && type)) {
      unique.set(normalizedLink, {
        link: normalizedLink,
        type,
        score
      });
    }
  }

  return Array.from(unique.values()).sort((a, b) => b.score - a.score);
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
  const preferFacebookEmail = options.preferFacebookEmail === true;
  const normalizedStartUrl = normalizeBusinessWebsiteUrl(startUrl) || normalizeWebsiteUrl(startUrl) || startUrl;
  const baseOrigin = new URL(normalizedStartUrl).origin;
  let firstUrl = canonicalizeCrawlUrl(normalizedStartUrl, baseOrigin) || stripHash(normalizedStartUrl) || normalizedStartUrl;
  firstUrl = normalizeSeedScanUrl(firstUrl, baseOrigin);
  const startHost = hostnameForUrl(firstUrl) || hostnameForUrl(normalizedStartUrl);
  const preferredEmailHosts = Array.from(new Set([
    normalizeHostForMatch(startHost),
    normalizeHostForMatch(hostnameForUrl(normalizedStartUrl)),
    normalizeHostForMatch(hostnameForUrl(options.businessWebsite)),
    normalizeHostForMatch(hostnameForUrl(options.discoveredWebsite))
  ].filter(Boolean)));
  const socialRootScan = isSocialNetworkHost(startHost);
  const searchRootScan = isSearchEngineHost(startHost);
  const directoryRootScan = isDirectoryHost(startHost);
  const focusedSinglePageScan = options.focusedSinglePage === true || searchRootScan || directoryRootScan;
  const strictFocusedPageFlow = !socialRootScan && !focusedSinglePageScan;
  if (strictFocusedPageFlow) {
    firstUrl = `${baseOrigin}/`;
  }
  const skipSitemapLookup = socialRootScan || options.skipSitemapLookup === true || focusedSinglePageScan;
  const maxPagesCap = socialRootScan ? 4 : focusedSinglePageScan ? 1 : FOCUSED_CRAWL_MAX_PAGES;
  const effectiveMaxPages = maxPagesCap;
  const sitemapQueueBudget = Math.max(6, effectiveMaxPages * 2);
  const perPageLinkBudget = Math.max(24, effectiveMaxPages * 4);
  const noSignalExitThreshold = socialRootScan ? 2 : focusedSinglePageScan ? 1 : 4;

  const seedUrls = socialRootScan
    ? buildSocialProbeUrls(firstUrl)
    : strictFocusedPageFlow
      ? [firstUrl, ...buildFocusedRouteSeedUrls(baseOrigin)]
      : [firstUrl];
  const queue = [];
  const visited = new Set();
  const queued = new Set();
  const discovered = new Set();
  const focusedPageTypeQueueCounts = new Map();
  const focusedTypeHintByKey = new Map();
  const forcedFocusedKeys = new Set();
  const maxForcedFocusedLinks = Math.max(3, Math.min(8, effectiveMaxPages));
  let forcedFocusedQueued = 0;
  let sourceFallbackEnabled = !socialRootScan && !focusedSinglePageScan;

  const normalizedQueueUrl = (url) => canonicalizeCrawlUrl(url, baseOrigin) || stripHash(url) || "";
  const normalizedQueueKey = (url) => {
    const normalized = normalizedQueueUrl(url);
    return stripHash(normalized);
  };
  const setFocusedTypeHint = (url, rawType) => {
    const type = normalizeFocusedHintType(rawType);
    const key = normalizedQueueKey(url);
    if (!type || !key) return;
    focusedTypeHintByKey.set(key, type);
  };
  const focusedTypeForUrl = (url) => {
    const key = normalizedQueueKey(url);
    const hintedType = key ? normalizeFocusedHintType(focusedTypeHintByKey.get(key)) : "";
    return hintedType || focusedCrawlPageType(url);
  };
  const ensureForcedFocused = (url) => {
    const key = normalizedQueueKey(url);
    if (!key) return false;
    if (forcedFocusedKeys.has(key)) return true;
    if (forcedFocusedQueued >= maxForcedFocusedLinks) return false;
    forcedFocusedKeys.add(key);
    forcedFocusedQueued += 1;
    return true;
  };
  const isForcedFocusedUrl = (url) => {
    const key = normalizedQueueKey(url);
    return Boolean(key && forcedFocusedKeys.has(key));
  };

  const focusedPageTypeQueueCount = (type) => Number(focusedPageTypeQueueCounts.get(type) || 0);
  const canQueueFocusedPageType = (type) => !type || focusedPageTypeQueueCount(type) < FOCUSED_CRAWL_MAX_PATHS_PER_TYPE;

  const markFocusedPageTypeQueued = (url) => {
    const type = focusedTypeForUrl(url);
    if (type) {
      focusedPageTypeQueueCounts.set(type, focusedPageTypeQueueCount(type) + 1);
    }
    return type;
  };

  for (const seedUrl of seedUrls) {
    const normalizedSeed = canonicalizeCrawlUrl(seedUrl, baseOrigin) || stripHash(seedUrl) || "";
    const seedKey = stripHash(normalizedSeed);
    if (!normalizedSeed || !seedKey || queued.has(seedKey)) continue;
    if (strictFocusedPageFlow) {
      const seedType = focusedTypeForUrl(normalizedSeed);
      if (seedType && !canQueueFocusedPageType(seedType)) {
        continue;
      }
    }
    queued.add(seedKey);
    discovered.add(seedKey);
    queue.push(normalizedSeed);
    markFocusedPageTypeQueued(normalizedSeed);
  }
  if (queue.length === 0) {
    const firstKey = stripHash(firstUrl) || firstUrl;
    queue.push(firstUrl);
    queued.add(firstKey);
    discovered.add(firstKey);
    markFocusedPageTypeQueued(firstUrl);
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

  const contactSelectionOptions = {
    preferredHosts: preferredEmailHosts,
    sourceMap: emailSourceByAddress
  };

  const seededSocialLinks = Array.isArray(options.seedSocialLinks) ? options.seedSocialLinks : [];
  for (const seededSocial of seededSocialLinks) {
    registerSocialCandidate(seededSocial);
  }

  const isTrustedCollectedEmail = (email) => {
    const normalized = normalizeEmail(email);
    if (!normalized) return false;
    const source = sourceForEmail(normalized, emailSourceByAddress).toLowerCase();
    if (source === "facebook") {
      return true;
    }
    if (!isEmailAlignedWithBusiness(normalized, preferredEmailHosts)) {
      return false;
    }
    return true;
  };

  const scannedSocialTargets = new Set();
  let homepageSocialAttempted = false;

  const scanQueuedSocialCandidates = async (optionsInput) => {
    const scanOptions = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
    const prioritizeEmailGoal = scanOptions.prioritizeEmailGoal === true && intent.needsEmail;
    const shouldPreferSocialEmail = scanOptions.preferFacebookEmail === true || preferFacebookEmail;
    let companyEmailNow = chooseContactEmail(Array.from(emails), "", contactSelectionOptions);
    let trustedCompanyEmailNow = Boolean(companyEmailNow && isTrustedCollectedEmail(companyEmailNow));
    let primaryPhoneNow = choosePrimaryPhone(Array.from(phones));
    let needsEmailNow = intent.needsEmail && !trustedCompanyEmailNow;
    let needsPhoneNow = intent.needsPhone && !primaryPhoneNow;

    const facebookQueue = prioritizeSocialLinks(Array.from(socialCandidates))
      .filter(shouldScanSocialUrl)
      .slice(0, options.maxSocialPages || 0);
    const shouldTrySocial =
      facebookQueue.length > 0 &&
      Number(options.maxSocialPages || 0) > 0 &&
      (needsEmailNow || needsPhoneNow || prioritizeEmailGoal || shouldPreferSocialEmail);

    if (!shouldTrySocial || !tab || tab.id == null) {
      return {
        attempted: false,
        emailFound: trustedCompanyEmailNow,
        phoneFound: Boolean(primaryPhoneNow),
        goalsMet: !needsEmailNow && !needsPhoneNow
      };
    }

    let attempted = false;
    let socialBudget = Number(options.maxSocialPages || 0);

    for (const baseSocialUrl of facebookQueue) {
      if (socialBudget <= 0) break;
      const probes = buildSocialProbeUrls(baseSocialUrl);
      for (const socialUrl of probes) {
        if (socialBudget <= 0) break;
        const normalizedTarget = normalizeBusinessWebsiteUrl(socialUrl) || normalizeWebsiteUrl(socialUrl);
        if (!normalizedTarget || scannedSocialTargets.has(normalizedTarget)) continue;

        attempted = true;
        scannedSocialTargets.add(normalizedTarget);
        socialBudget -= 1;

        assertNotStopped();
        reportProgress("social_page", normalizedTarget);
        let socialData = null;
        try {
          const socialHost = hostnameForUrl(normalizedTarget);
          const isFacebookTarget = socialHost.includes("facebook.com");
          assertNotStopped();
          await updateTabUrl(tab.id, normalizedTarget);
          await waitForTabComplete(tab.id, options.timeoutMs);
          assertNotStopped();
          await sleep(isFacebookTarget ? 1100 : 700);

          const aggregateEmails = new Set();
          const aggregatePhones = new Set();
          const aggregateOwnerCandidates = [];
          let aggregateBlocked = false;
          let hasExtractionPayload = false;
          let lastExtractionError = null;
          const maxExtractAttempts = isFacebookTarget
            ? options.visibleTabs === true ? 1 : 2
            : 1;

          for (let attempt = 0; attempt < maxExtractAttempts; attempt += 1) {
            assertNotStopped();
            let extracted = null;
            try {
              extracted = await executeExtraction(tab.id, options.timeoutMs, {
                parseSourceHtml: false
              });
              lastExtractionError = null;
            } catch (extractionError) {
              lastExtractionError = extractionError;
            }

            if (extracted && typeof extracted === "object") {
              hasExtractionPayload = true;
              if (extracted.blocked === true) {
                aggregateBlocked = true;
              }

              for (const email of extracted.emails || []) {
                const normalizedEmail = normalizeEmail(email);
                if (normalizedEmail) {
                  aggregateEmails.add(normalizedEmail);
                }
              }

              for (const phoneValue of extracted.phones || []) {
                const normalizedPhone = sanitizePhoneText(phoneValue);
                if (normalizedPhone) {
                  aggregatePhones.add(normalizedPhone);
                }
              }

              for (const candidate of extracted.ownerCandidates || []) {
                if (!candidate || !candidate.name) continue;
                aggregateOwnerCandidates.push(candidate);
              }

              if (aggregateEmails.size > 0 || aggregatePhones.size > 0) {
                break;
              }
            }

            if (attempt < maxExtractAttempts - 1) {
              reportProgress("social_retry", normalizedTarget);
              await sleep(650 + attempt * 250);
            }
          }

          if (!hasExtractionPayload && lastExtractionError) {
            throw lastExtractionError;
          }

          if (hasExtractionPayload) {
            socialData = {
              emails: Array.from(aggregateEmails),
              phones: Array.from(aggregatePhones),
              ownerCandidates: aggregateOwnerCandidates,
              blocked: aggregateBlocked
            };
          }
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

        companyEmailNow = chooseContactEmail(Array.from(emails), "", contactSelectionOptions);
        trustedCompanyEmailNow = Boolean(companyEmailNow && isTrustedCollectedEmail(companyEmailNow));
        primaryPhoneNow = choosePrimaryPhone(Array.from(phones));
        needsEmailNow = intent.needsEmail && !trustedCompanyEmailNow;
        needsPhoneNow = intent.needsPhone && !primaryPhoneNow;
        if (!needsEmailNow && !needsPhoneNow) {
          if (trustedCompanyEmailNow) {
            reportProgress("social_email_found", normalizedTarget);
          } else if (primaryPhoneNow) {
            reportProgress("social_phone_found", normalizedTarget);
          }
          socialBudget = 0;
          break;
        }
      }
    }

    return {
      attempted,
      emailFound: trustedCompanyEmailNow,
      phoneFound: Boolean(primaryPhoneNow),
      goalsMet: !needsEmailNow && !needsPhoneNow
    };
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
        let queuedFromSitemap = 0;
        for (const entry of sitemapEntries.slice(0, sitemapQueueBudget)) {
          const normalizedLink = canonicalizeCrawlUrl(entry.link, baseOrigin);
          if (!normalizedLink) continue;
          const pageType = focusedTypeForUrl(normalizedLink);
          if (!shouldQueueFocusedCrawlLink(normalizedLink, entry.score, intent, visited.size, queue.length) && !pageType) continue;
          if (strictFocusedPageFlow && !canQueueFocusedPageType(pageType)) continue;
          const linkKey = stripHash(normalizedLink);
          if (!linkKey) continue;
          if (visited.has(linkKey) || queued.has(linkKey)) continue;
          if (discovered.size >= options.maxDiscoveredPages) continue;

          discovered.add(linkKey);
          queued.add(linkKey);
          queue.push(normalizedLink);
          markFocusedPageTypeQueued(normalizedLink);
          queuedFromSitemap += 1;
          if (queue.length >= sitemapQueueBudget) break;
          if (entry.score >= 6) {
            highIntentDiscovered.add(linkKey);
          }
        }

        if (queuedFromSitemap > 0) {
          // Sitemap produced focused crawl candidates, so skip expensive HTML source fallback parsing.
          sourceFallbackEnabled = false;
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
      if (strictFocusedPageFlow && visited.size > 0 && !focusedTypeForUrl(nextUrl) && !isForcedFocusedUrl(nextUrl)) {
        continue;
      }

      visited.add(nextKey);
      pagesVisited = visited.size;
      if (focusedTypeForUrl(nextUrl) || isForcedFocusedUrl(nextUrl)) {
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
        const shouldParseSourceHtml = !socialRootScan && !searchRootScan && !directoryRootScan;
        pageData = await executeExtraction(tab.id, options.timeoutMs, {
          parseSourceHtml: shouldParseSourceHtml
        });
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
      for (const social of pageData.socialLinks || []) {
        registerSocialCandidate(social);
      }
      if (pageData.hasFooterFacebookLink === true) {
        // Direct footer Facebook link is already available; no need to parse raw HTML source fallback.
        sourceFallbackEnabled = false;
      }

      const hasSignalOnPage =
        (Array.isArray(pageData.emails) && pageData.emails.length > 0) ||
        (Array.isArray(pageData.ownerCandidates) && pageData.ownerCandidates.length > 0) ||
        (Array.isArray(pageData.phones) && pageData.phones.length > 0) ||
        pageData.hasContactSignals === true;
      noSignalStreak = hasSignalOnPage ? 0 : noSignalStreak + 1;

      const companyEmailNow = chooseContactEmail(Array.from(emails), "", contactSelectionOptions);
      const trustedCompanyEmailNow = companyEmailNow && isTrustedCollectedEmail(companyEmailNow);
      const primaryPhoneNow = choosePrimaryPhone(Array.from(phones));
      const needsPhoneStill = intent.needsPhone && !primaryPhoneNow;
      const shouldTryHomepageSocialFirst =
        !socialRootScan &&
        !focusedSinglePageScan &&
        pagesVisited === 1 &&
        homepageSocialAttempted !== true &&
        socialCandidates.size > 0 &&
        intent.needsEmail;
      if (
        intent.needsEmail &&
        trustedCompanyEmailNow &&
        !needsPhoneStill &&
        !shouldTryHomepageSocialFirst
      ) {
        reportProgress("site_email_found", nextUrl);
        break;
      }

      if (!socialRootScan && !focusedSinglePageScan) {
        const focusedHintEntries = normalizeFocusedHintEntries(pageData.focusedLinks, intent).slice(0, perPageLinkBudget);
        for (const entry of focusedHintEntries) {
          const normalizedLink = canonicalizeCrawlUrl(entry.link, baseOrigin);
          if (!normalizedLink) continue;
          const hintType = normalizeFocusedHintType(entry.type);
          const linkKey = stripHash(normalizedLink);
          if (!linkKey) continue;
          if (visited.has(linkKey) || queued.has(linkKey)) continue;
          if (discovered.size >= options.maxDiscoveredPages) continue;

          if (hintType) {
            if (!canQueueFocusedPageType(hintType)) continue;
            setFocusedTypeHint(normalizedLink, hintType);
          } else if (!ensureForcedFocused(normalizedLink)) {
            continue;
          }

          discovered.add(linkKey);
          queued.add(linkKey);
          queue.push(normalizedLink);
          markFocusedPageTypeQueued(normalizedLink);
          if (entry.score >= 6 || hintType === "contact" || isForcedFocusedUrl(normalizedLink)) {
            highIntentDiscovered.add(linkKey);
          }
        }

        const prioritizedRelatedLinks = prioritizeCrawlLinkEntries(
          Array.isArray(pageData.relatedLinks) ? pageData.relatedLinks : [],
          intent
        ).slice(0, perPageLinkBudget);

        for (const entry of prioritizedRelatedLinks) {
          const normalizedLink = canonicalizeCrawlUrl(entry.link, baseOrigin);
          if (!normalizedLink) continue;
          const linkKey = stripHash(normalizedLink);
          if (!linkKey) continue;
          if (visited.has(linkKey) || queued.has(linkKey)) continue;
          if (discovered.size >= options.maxDiscoveredPages) continue;

          const pageType = focusedTypeForUrl(normalizedLink);
          const forcedFocused = !pageType;
          if (forcedFocused) {
            if (!ensureForcedFocused(normalizedLink)) continue;
          } else if (!canQueueFocusedPageType(pageType)) {
            continue;
          }

          discovered.add(linkKey);
          queued.add(linkKey);
          queue.push(normalizedLink);
          markFocusedPageTypeQueued(normalizedLink);
          if (entry.score >= 6 || forcedFocused) {
            highIntentDiscovered.add(linkKey);
          }
        }

        const prioritizedInternalLinks = prioritizeCrawlLinkEntries(
          Array.isArray(pageData.internalLinks) ? pageData.internalLinks : [],
          intent
        ).slice(0, perPageLinkBudget);

        for (const entry of prioritizedInternalLinks) {
          const normalizedLink = canonicalizeCrawlUrl(entry.link, baseOrigin);
          if (!normalizedLink) continue;
          if (!shouldQueueFocusedCrawlLink(normalizedLink, entry.score, intent, visited.size, queue.length)) continue;
          const pageType = focusedTypeForUrl(normalizedLink);
          if (!pageType) continue;
          if (!canQueueFocusedPageType(pageType)) continue;

          const linkKey = stripHash(normalizedLink);
          if (!linkKey) continue;
          if (visited.has(linkKey) || queued.has(linkKey)) continue;
          if (discovered.size >= options.maxDiscoveredPages) continue;

          discovered.add(linkKey);
          queued.add(linkKey);
          queue.push(normalizedLink);
          markFocusedPageTypeQueued(normalizedLink);
          if (entry.score >= 6) {
            highIntentDiscovered.add(linkKey);
          }
        }
      }

      reportProgress("site_page_done", nextUrl);

      if (shouldTryHomepageSocialFirst) {
        homepageSocialAttempted = true;
        const homepageSocialResult = await scanQueuedSocialCandidates({
          prioritizeEmailGoal: true,
          preferFacebookEmail
        });
        if (homepageSocialResult.goalsMet) {
          break;
        }
      }

      const queueHasFocusedTarget = queue.some(
        (queuedUrl) => focusedTypeForUrl(queuedUrl) || isForcedFocusedUrl(queuedUrl)
      );
      if (
        noSignalStreak >= noSignalExitThreshold &&
        emails.size === 0 &&
        (priorityPagesVisited >= 1 || !queueHasFocusedTarget)
      ) {
        reportProgress("site_focus_exit", nextUrl);
        break;
      }
    }

    await scanQueuedSocialCandidates({
      prioritizeEmailGoal: false,
      preferFacebookEmail
    });

  } finally {
    if (typeof options.onTabChange === "function") {
      options.onTabChange(null);
    }
    if (tab && tab.id != null) {
      await closeTab(tab.id).catch(() => {});
    }
  }

  const emailList = Array.from(emails);
  const bestOwner = pickBestOwner(ownerCandidates, emailList, {
    businessName: normalizeText(options.businessName),
    businessCategory: normalizeText(options.businessCategory)
  });
  const ownerName = bestOwner ? normalizeText(bestOwner.name) : "";
  const ownerTitle = bestOwner ? normalizeText(bestOwner.title) : "";
  const ownerConfidence = bestOwner ? formatConfidence(bestOwner.confidence) : "";
  const ownerEmail = ownerName ? chooseOwnerEmail(emailList, ownerName) : "";
  let contactEmail = chooseContactEmail(emailList, ownerEmail, contactSelectionOptions);
  if (contactEmail && !isTrustedCollectedEmail(contactEmail)) {
    contactEmail = "";
  }
  const primaryEmail = contactEmail || "";
  const primaryEmailType = primaryEmail ? (isPotentialPersonalEmail(primaryEmail) ? "personal" : "company") : "";
  const primaryEmailSource = primaryEmail ? sourceForEmail(primaryEmail, emailSourceByAddress) : "";
  const emailSourceUrl = primaryEmail ? sourceUrlForEmail(primaryEmail, emailSourceUrlByAddress) : "";
  const emailConfidence = primaryEmail
    ? formatConfidence(
      estimateEmailConfidence({
        primaryEmail,
        primaryEmailType,
        primaryEmailSource,
        ownerEmail,
        ownerName,
        emailSourceUrl
      })
    )
    : "";
  const primaryPhone = choosePrimaryPhone(Array.from(phones));
  const primaryPhoneSource = primaryPhone ? sourceForPhone(primaryPhone, phoneSourceByNumber) : "";

  let status = "no_public_data";
  if (primaryEmail || primaryPhone) {
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
    ownerName,
    ownerTitle,
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
    let rootPath = parsed.pathname.replace(/\/+$/, "");
    // Facebook email is usually visible on the main profile/page route.
    // If we were given an About/Details route, normalize back to the root profile URL.
    rootPath = rootPath.replace(/\/(?:about(?:_contact_and_basic_info)?|info_contact|details)(?:\/.*)?$/i, "");
    rootPath = rootPath.replace(/^\/pg\/([^/]+)(?:\/.*)?$/i, "/$1");
    if (!rootPath) {
      rootPath = "/";
    }
    parsed.pathname = rootPath;
    parsed.hash = "";
    parsed.search = "";
    parsed.searchParams.delete("sk");
    const baseOnly = normalizeWebsiteUrl(parsed.toString());
    if (!baseOnly || !shouldScanSocialUrl(baseOnly)) {
      return out.filter(shouldScanSocialUrl);
    }
    return [baseOnly];
  } catch (_error) {
    return out.filter(shouldScanSocialUrl);
  }

  return out.filter(shouldScanSocialUrl);
}

async function discoverLinksFromSitemap(tabId, baseOrigin, timeoutMs, intent) {
  const sitemapPaths = ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"];
  const links = new Set();
  const parseTimeoutMs = clampInt(timeoutMs, 2000, 7000, 4500);
  const tabLoadTimeoutMs = clampInt(timeoutMs, 2000, 7000, 4500);

  for (const path of sitemapPaths) {
    const sitemapUrl = `${baseOrigin}${path}`;
    let extracted = await fetchSitemapLinks(sitemapUrl, parseTimeoutMs).catch(() => []);

    // Fallback: if direct fetch fails, try tab-based extraction with strict timeouts.
    if ((!Array.isArray(extracted) || extracted.length === 0) && Number.isFinite(Number(tabId))) {
      try {
        await promiseWithTimeout(updateTabUrl(tabId, sitemapUrl), tabLoadTimeoutMs, "Timed out while opening sitemap URL");
        await promiseWithTimeout(waitForTabComplete(tabId, tabLoadTimeoutMs), tabLoadTimeoutMs, "Timed out while loading sitemap URL");
        await sleep(220);
        extracted = await promiseWithTimeout(
          executeSitemapExtraction(tabId),
          parseTimeoutMs,
          "Timed out while parsing sitemap"
        ).catch(() => []);
      } catch (_error) {
        extracted = [];
      }
    }

    for (const rawLink of Array.isArray(extracted) ? extracted : []) {
      const normalized = normalizeWebsiteUrl(rawLink);
      if (!normalized || !normalized.startsWith(baseOrigin)) continue;
      links.add(normalized);
    }

    if (links.size >= 120) {
      break;
    }
  }

  const prioritized = prioritizeCrawlLinkEntries(Array.from(links), intent);
  return prioritized.slice(0, 120).map((entry) => entry.link);
}

async function fetchSitemapLinks(sitemapUrl, timeoutMs) {
  const waitMs = clampInt(timeoutMs, 500, 120000, 4500);
  const controller = typeof AbortController === "function" ? new AbortController() : null;
  const timeoutHandle = setTimeout(() => {
    if (controller) {
      controller.abort();
    }
  }, waitMs);

  try {
    const response = await fetch(sitemapUrl, {
      method: "GET",
      redirect: "follow",
      cache: "no-store",
      signal: controller ? controller.signal : undefined
    });
    if (!response || !response.ok) {
      return [];
    }

    const contentType = normalizeText(response.headers && response.headers.get ? response.headers.get("content-type") : "").toLowerCase();
    const bodyText = await promiseWithTimeout(response.text(), waitMs, "Timed out while reading sitemap body").catch(() => "");
    return extractSitemapLinksFromText(bodyText, contentType);
  } catch (_error) {
    return [];
  } finally {
    clearTimeout(timeoutHandle);
  }
}

function extractSitemapLinksFromText(input, contentType) {
  const text = normalizeText(input);
  if (!text) return [];

  const MAX_TEXT_CHARS = 450000;
  const MAX_OUT_LINKS = 500;
  const MAX_REGEX_MATCHES = 800;
  const body = text.length > MAX_TEXT_CHARS ? text.slice(0, MAX_TEXT_CHARS) : text;
  const out = new Set();
  const isLikelyXml = /\bxml\b/i.test(contentType) || /<urlset|<sitemapindex|<loc>/i.test(body);

  if (isLikelyXml) {
    const locRegex = /<loc>\s*([^<\s]+)\s*<\/loc>/gi;
    let match = locRegex.exec(body);
    while (match && out.size < MAX_OUT_LINKS) {
      const value = normalizeText(match[1]).replace(/[),.;]+$/, "");
      if (value) out.add(value);
      match = locRegex.exec(body);
    }
  }

  if (out.size === 0) {
    const urlRegex = /https?:\/\/[a-z0-9.-]+\.[a-z]{2,}(?:\/[^\s<>"']*)?/gi;
    let match = urlRegex.exec(body);
    let guard = 0;
    while (match && guard < MAX_REGEX_MATCHES && out.size < MAX_OUT_LINKS) {
      const value = normalizeText(match[0]).replace(/[),.;]+$/, "");
      if (value) out.add(value);
      guard += 1;
      match = urlRegex.exec(body);
    }
  }

  return Array.from(out).slice(0, MAX_OUT_LINKS);
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
  const MAX_XML_LOCS = 500;
  const MAX_TEXT_CHARS = 350000;
  const MAX_REGEX_MATCHES = 700;
  const MAX_OUT_LINKS = 500;
  const out = new Set();

  const xmlNodes = Array.from(document.querySelectorAll("loc")).slice(0, MAX_XML_LOCS);
  for (const node of xmlNodes) {
    const value = normalize(node.textContent || "");
    if (value) out.add(value);
    if (out.size >= MAX_OUT_LINKS) break;
  }

  // XML sitemap pages are often huge; if we already got enough <loc> entries, skip regex fallback.
  if (out.size > 0) {
    return Array.from(out).slice(0, MAX_OUT_LINKS);
  }

  let bodyText = normalize((document.body && (document.body.innerText || document.body.textContent)) || "");
  if (bodyText.length > MAX_TEXT_CHARS) {
    bodyText = bodyText.slice(0, MAX_TEXT_CHARS);
  }
  const regex = /https?:\/\/[a-z0-9.-]+\.[a-z]{2,}(?:\/[^\s<>"']*)?/gi;
  let match = regex.exec(bodyText);
  let guard = 0;
  while (match && guard < MAX_REGEX_MATCHES && out.size < MAX_OUT_LINKS) {
    const value = normalize(match[0]).replace(/[),.;]+$/, "");
    if (value) out.add(value);
    guard += 1;
    match = regex.exec(bodyText);
  }

  return Array.from(out).slice(0, MAX_OUT_LINKS);
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

function chooseContactEmail(emails, ownerEmail, optionsInput) {
  const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
  const sourceMap = options.sourceMap instanceof Map ? options.sourceMap : null;
  const preferredHosts = Array.isArray(options.preferredHosts)
    ? options.preferredHosts.map((host) => normalizeHostForMatch(host)).filter(Boolean)
    : [];
  const list = sanitizeEmailList(emails).filter((email) => email !== ownerEmail);
  if (list.length === 0) return "";
  const facebookPreferredList = sourceMap
    ? list.filter((email) => normalizeText(sourceMap.get(email)).toLowerCase() === "facebook")
    : [];
  const candidatePool = facebookPreferredList.length > 0 ? facebookPreferredList : list;

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

  const prefixScore = (email) => {
    const local = localPartForEmail(email);
    if (!local) return 0;
    for (let i = 0; i < priorityPrefixes.length; i += 1) {
      if (hasMailboxPrefix(local, priorityPrefixes[i])) {
        return Math.max(1, priorityPrefixes.length - i);
      }
    }
    return 0;
  };

  const hasSelectionContext = preferredHosts.length > 0 || sourceMap instanceof Map;
  if (!hasSelectionContext) {
    for (const prefix of priorityPrefixes) {
      const hit = candidatePool.find((email) => hasMailboxPrefix(localPartForEmail(email), prefix));
      if (hit) return hit;
    }
    return candidatePool[0] || "";
  }

  let best = "";
  let bestScore = Number.NEGATIVE_INFINITY;
  let bestPrefixScore = 0;

  for (const email of candidatePool) {
    const domain = domainForEmail(email);
    const aligned = isEmailAlignedWithBusiness(email, preferredHosts);
    const freeMailbox = isFreeMailboxDomain(domain);
    const suspiciousDomain = isSuspiciousThirdPartyEmailDomain(domain);
    const source = sourceMap ? normalizeText(sourceMap.get(email)).toLowerCase() : "";
    const hitPrefixScore = prefixScore(email);

    let score = 0;
    score += hitPrefixScore * 1.5;
    if (aligned) score += 26;
    if (freeMailbox) score += 10;
    if (source === "website") score += 8;
    if (source === "facebook") score += 26;

    if (preferredHosts.length > 0 && source === "website" && !aligned && !freeMailbox) {
      score -= 32;
    }
    if (suspiciousDomain) {
      score -= 24;
    }

    if (
      !best ||
      score > bestScore ||
      (score === bestScore && hitPrefixScore > bestPrefixScore) ||
      (score === bestScore && hitPrefixScore === bestPrefixScore && email.length < best.length)
    ) {
      best = email;
      bestScore = score;
      bestPrefixScore = hitPrefixScore;
    }
  }

  return best || "";
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

function domainForEmail(email) {
  const value = normalizeText(email).toLowerCase();
  const at = value.indexOf("@");
  if (at <= 0 || at >= value.length - 1) return "";
  return value.slice(at + 1).replace(/^www\./, "");
}

function normalizeHostForMatch(hostname) {
  return normalizeText(hostname).toLowerCase().replace(/^www\./, "");
}

function domainMatchesHost(emailDomain, host) {
  const domain = normalizeHostForMatch(emailDomain);
  const normalizedHost = normalizeHostForMatch(host);
  if (!domain || !normalizedHost) return false;
  return (
    domain === normalizedHost ||
    domain.endsWith(`.${normalizedHost}`) ||
    normalizedHost.endsWith(`.${domain}`)
  );
}

function isFreeMailboxDomain(domainInput) {
  const domain = normalizeHostForMatch(domainInput);
  if (!domain) return false;
  return (
    domain === "gmail.com" ||
    domain === "outlook.com" ||
    domain === "hotmail.com" ||
    domain === "live.com" ||
    domain === "yahoo.com" ||
    domain === "ymail.com" ||
    domain === "icloud.com" ||
    domain === "me.com" ||
    domain === "aol.com" ||
    domain === "proton.me" ||
    domain === "protonmail.com"
  );
}

function isSuspiciousThirdPartyEmailDomain(domainInput) {
  const domain = normalizeHostForMatch(domainInput);
  if (!domain) return false;
  return (
    domain.includes("waze.com") ||
    domain.includes("google.com") ||
    domain.includes("googleusercontent.com") ||
    domain.includes("gstatic.com") ||
    domain.includes("facebookmail.com") ||
    domain.includes("meta.com") ||
    domain.includes("instagram.com") ||
    domain.includes("twitter.com") ||
    domain.includes("x.com")
  );
}

function isEmailAlignedWithBusiness(email, preferredHostsInput) {
  const domain = domainForEmail(email);
  if (!domain) return false;
  if (isFreeMailboxDomain(domain)) return true;

  const preferredHosts = Array.isArray(preferredHostsInput)
    ? preferredHostsInput
      .map((host) => normalizeHostForMatch(host))
      .filter(Boolean)
    : [];
  if (preferredHosts.length === 0) return true;
  return preferredHosts.some((host) => domainMatchesHost(domain, host));
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
  if (source === "website") score += 0.03;
  if (source === "facebook") score += 0.1;
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
  if (host.includes("facebook.com")) return isLikelyFacebookBusinessPageUrl(url) ? 10 : -8;
  if (host.includes("instagram.com")) return 3;
  if (host.includes("linkedin.com")) return 2;
  if (host.includes("x.com") || host.includes("twitter.com")) return -3;
  if (host.includes("youtube.com")) return -4;
  return 1;
}

function isLikelySocialBusinessProfileUrl(url) {
  const host = hostnameForUrl(url);
  if (!host || !isSocialNetworkHost(host)) return false;
  if (host.includes("facebook.com")) {
    return isLikelyFacebookBusinessPageUrl(url);
  }
  return extractSocialProfileKeyFromUrl(url, host.replace(/^www\./, "")) !== "";
}

function shouldScanSocialUrl(url) {
  if (!isLikelySocialBusinessProfileUrl(url)) return false;
  const host = hostnameForUrl(url);
  return Boolean(host && host.includes("facebook.com"));
}

function isLikelyFacebookBusinessPageUrl(url) {
  const normalized = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url);
  if (!normalized) return false;
  let parsed = null;
  try {
    parsed = new URL(normalized);
  } catch (_error) {
    return false;
  }

  const host = normalizeText(parsed.hostname).toLowerCase();
  if (!host.includes("facebook.com")) return false;

  const path = normalizeText(parsed.pathname || "").toLowerCase().replace(/\/+$/, "");
  if (!path || path === "/") return false;
  if (path.startsWith("/sharer") || path.startsWith("/share.php")) return false;
  if (path.startsWith("/l.php")) return false;
  if (path.startsWith("/dialog/") || path.startsWith("/plugins/")) return false;
  if (path.startsWith("/privacy") || path.startsWith("/policies") || path.startsWith("/terms")) return false;
  if (path.startsWith("/help") || path.startsWith("/legal") || path.startsWith("/settings")) return false;
  if (path.startsWith("/login") || path.startsWith("/recover") || path.startsWith("/checkpoint")) return false;
  if (path.startsWith("/watch") || path.startsWith("/reel") || path.startsWith("/story.php")) return false;
  if (path.startsWith("/groups") || path.startsWith("/events") || path.startsWith("/marketplace")) return false;

  const firstSegment = path.split("/").filter(Boolean)[0] || "";
  if (!firstSegment) return false;
  if (firstSegment === "profile.php") {
    const profileId = normalizeText(parsed.searchParams.get("id"));
    return profileId !== "";
  }
  if (firstSegment === "pg") {
    const secondSegment = path.split("/").filter(Boolean)[1] || "";
    return secondSegment !== "";
  }
  if (firstSegment === "pages") {
    const segments = path.split("/").filter(Boolean);
    const secondSegment = segments[1] || "";
    const numericId = segments.find((segment) => /^\d{5,}$/.test(segment)) || "";
    return Boolean(secondSegment || numericId);
  }
  if (firstSegment === "p") {
    const secondSegment = path.split("/").filter(Boolean)[1] || "";
    // Modern Facebook business pages are often exposed as /p/<name-id>/.
    return /^[a-z0-9._-]{4,}$/i.test(secondSegment);
  }

  const reservedTopLevel = new Set([
    "about",
    "ads",
    "business",
    "dialog",
    "events",
    "gaming",
    "groups",
    "hashtag",
    "help",
    "legal",
    "login",
    "marketplace",
    "messages",
    "notifications",
    "pages",
    "people",
    "plugins",
    "policies",
    "privacy",
    "recover",
    "search",
    "settings",
    "share.php",
    "sharer",
    "terms",
    "watch"
  ]);
  if (reservedTopLevel.has(firstSegment)) return false;

  return /^[a-z0-9._-]{3,}$/i.test(firstSegment);
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
  const domainLabels = domain.split(".").filter(Boolean);
  const domainRoot = domainLabels[0] || "";
  const domainRootFlat = domainRoot.replace(/[^a-z0-9]/g, "");
  const localFlat = local.replace(/[^a-z0-9]/g, "");

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
    "sample.com",
    "domainname.com",
    "yourcompany.com",
    "companyname.com",
    "placeholder.com",
    "dummy.com"
  ]);
  if (placeholderDomains.has(domain)) return true;
  if (/(^|\.)(example|invalid|localhost|test)$/.test(domain)) return true;
  const strongPlaceholderRoots = new Set([
    "example",
    "test",
    "domain",
    "yourdomain",
    "mydomain",
    "sample",
    "domainname",
    "placeholder",
    "dummy",
    "invalid",
    "localhost",
    "yourcompany",
    "companyname"
  ]);
  if (strongPlaceholderRoots.has(domainRootFlat)) return true;
  if (/^(n\/?a|na|null|none|unknown)$/i.test(local)) return true;

  const genericLocalParts = new Set([
    "user",
    "username",
    "your",
    "yourname",
    "youremail",
    "name",
    "email",
    "emailaddress",
    "test",
    "example",
    "sample",
    "demo",
    "mail",
    "contact",
    "firstname",
    "lastname",
    "fullname",
    "firstlast",
    "firstnamelastname",
    "namehere"
  ]);
  const localLooksGeneric =
    genericLocalParts.has(localFlat) ||
    /^(user(name)?|your-?name|your-?email|name|email|test|example|sample|demo|mail|contact|first(name)?|last(name)?|full(name)?)$/i.test(local);
  if (localLooksGeneric && /(example|domain|test|localhost|invalid|sample|demo|placeholder|dummy|companyname|yourdomain|mydomain|yourcompany)/i.test(domain)) {
    return true;
  }
  if (localLooksGeneric && strongPlaceholderRoots.has(domainRootFlat)) return true;

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
  const pageType = focusedCrawlPageType(url);

  let score = 0;
  if (pageType === "contact") {
    score += 14;
  } else if (pageType === "about") {
    score += 12;
  } else if (pageType === "team") {
    score += 11;
  } else if (pageType === "careers") {
    score += 10;
  } else {
    score -= 9;
  }
  if (inx.needsEmail && pageType === "contact") {
    score += 4;
  }
  if (inx.needsOwner && (pageType === "about" || pageType === "team" || pageType === "careers")) {
    score += 2;
  }
  if (inx.needsPhone && pageType === "contact") {
    score += 1;
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

function normalizeSeedScanUrl(rawUrl, baseOrigin) {
  const normalized = normalizeBusinessWebsiteUrl(rawUrl) || normalizeWebsiteUrl(rawUrl) || normalizeText(rawUrl);
  const fallbackOrigin = normalizeText(baseOrigin);
  if (!normalized) {
    return fallbackOrigin ? `${fallbackOrigin}/` : "";
  }

  try {
    const parsed = new URL(normalized);
    if (fallbackOrigin && parsed.origin !== fallbackOrigin) {
      return `${fallbackOrigin}/`;
    }

    if (isSitemapLikePath(parsed.pathname)) {
      return `${parsed.origin}/`;
    }

    const stripped = stripHash(parsed.toString());
    return stripped || `${parsed.origin}/`;
  } catch (_error) {
    return fallbackOrigin ? `${fallbackOrigin}/` : normalized;
  }
}

function isSitemapLikePath(pathname) {
  const lowerPath = normalizeText(pathname).toLowerCase();
  if (!lowerPath) return false;

  if (/(^|\/)(sitemap|sitemap_index|sitemap-index)(\/|$)/i.test(lowerPath)) {
    return true;
  }

  if (/\/sitemap[^/]*$/i.test(lowerPath)) {
    return true;
  }

  return false;
}

function shouldSkipCrawlPath(pathname) {
  const lowerPath = normalizeText(pathname).toLowerCase();
  if (!lowerPath) return false;

  if (isSitemapLikePath(lowerPath)) {
    return true;
  }

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
    const targetUrl = normalizeBusinessWebsiteUrl(url) || normalizeWebsiteUrl(url) || normalizeText(url);
    if (!targetUrl) {
      reject(new Error("Failed to open hidden website tab"));
      return;
    }

    const createInWindow = (windowId, allowRecreate) => {
      chrome.tabs.create({ windowId, url: targetUrl, active: false }, (tab) => {
        if (chrome.runtime.lastError || !tab) {
          if (allowRecreate) {
            hiddenScanWindowId = null;
            createHiddenWindow();
            return;
          }
          reject(new Error(chrome.runtime.lastError && chrome.runtime.lastError.message
            ? chrome.runtime.lastError.message
            : "Failed to open hidden website tab"));
          return;
        }
        chrome.windows.update(windowId, { state: "minimized", focused: false }, () => {});
        resolve(tab);
      });
    };

    const createHiddenWindow = () => {
      chrome.windows.create(
        {
          url: "about:blank",
          focused: false,
          state: "minimized",
          type: "normal"
        },
        (windowRef) => {
          if (chrome.runtime.lastError) {
            reject(new Error(chrome.runtime.lastError.message || "Failed to open hidden website tab"));
            return;
          }

          const windowId = Number(windowRef && windowRef.id);
          if (!Number.isFinite(windowId)) {
            reject(new Error("Failed to open hidden website tab"));
            return;
          }
          hiddenScanWindowId = windowId;

          const bootstrapTab = windowRef && Array.isArray(windowRef.tabs) ? windowRef.tabs[0] : null;
          const bootstrapTabId = Number(bootstrapTab && bootstrapTab.id);

          chrome.tabs.create({ windowId, url: targetUrl, active: false }, (tab) => {
            if (chrome.runtime.lastError || !tab) {
              reject(new Error(chrome.runtime.lastError && chrome.runtime.lastError.message
                ? chrome.runtime.lastError.message
                : "Failed to open hidden website tab"));
              return;
            }

            if (Number.isFinite(bootstrapTabId) && Number(tab.id) !== bootstrapTabId) {
              chrome.tabs.remove(bootstrapTabId, () => {});
            }
            chrome.windows.update(windowId, { state: "minimized", focused: false }, () => {});
            resolve(tab);
          });
        }
      );
    };

    if (Number.isFinite(Number(hiddenScanWindowId))) {
      const existingWindowId = Number(hiddenScanWindowId);
      chrome.windows.get(existingWindowId, { populate: false }, (windowRef) => {
        if (chrome.runtime.lastError || !windowRef || !Number.isFinite(Number(windowRef.id))) {
          hiddenScanWindowId = null;
          createHiddenWindow();
          return;
        }
        createInWindow(Number(windowRef.id), true);
      });
      return;
    }

    createHiddenWindow();
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

function promiseWithTimeout(promise, timeoutMs, message) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const waitMs = clampInt(timeoutMs, 250, 120000, 5000);
    const timeoutHandle = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error(message || "Operation timed out"));
    }, waitMs);

    Promise.resolve(promise)
      .then((value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeoutHandle);
        resolve(value);
      })
      .catch((error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeoutHandle);
        reject(error);
      });
  });
}

function executeExtraction(tabId, timeoutMs, extractionOptions) {
  const scriptOptions = extractionOptions && typeof extractionOptions === "object"
    ? extractionOptions
    : {};
  const task = new Promise((resolve, reject) => {
    chrome.scripting.executeScript(
      {
        target: { tabId },
        func: extractPageDataScript,
        args: [scriptOptions]
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
  if (timeoutMs == null) {
    return task;
  }
  return promiseWithTimeout(
    task,
    clampInt(timeoutMs, 1500, 120000, 12000),
    "Timed out while scanning website page"
  );
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

async function extractPageDataScript(extractionOptionsInput) {
  const extractionOptions = extractionOptionsInput && typeof extractionOptionsInput === "object"
    ? extractionOptionsInput
    : {};
  const parseSourceHtmlEnabled = extractionOptions.parseSourceHtml !== false;
  const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim();

  const isPlaceholderEmailParts = (localPart, domainPart) => {
    const local = normalize(localPart).toLowerCase();
    const domain = normalize(domainPart).toLowerCase().replace(/^www\./, "");
    if (!local || !domain) return true;
    const domainLabels = domain.split(".").filter(Boolean);
    const domainRoot = domainLabels[0] || "";
    const domainRootFlat = domainRoot.replace(/[^a-z0-9]/g, "");
    const localFlat = local.replace(/[^a-z0-9]/g, "");

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
      "sample.com",
      "domainname.com",
      "yourcompany.com",
      "companyname.com",
      "placeholder.com",
      "dummy.com"
    ]);
    if (placeholderDomains.has(domain)) return true;
    if (/(^|\.)(example|invalid|localhost|test)$/.test(domain)) return true;
    const strongPlaceholderRoots = new Set([
      "example",
      "test",
      "domain",
      "yourdomain",
      "mydomain",
      "sample",
      "domainname",
      "placeholder",
      "dummy",
      "invalid",
      "localhost",
      "yourcompany",
      "companyname"
    ]);
    if (strongPlaceholderRoots.has(domainRootFlat)) return true;
    if (/^(n\/?a|na|null|none|unknown)$/i.test(local)) return true;

    const genericLocalParts = new Set([
      "user",
      "username",
      "your",
      "yourname",
      "youremail",
      "name",
      "email",
      "emailaddress",
      "test",
      "example",
      "sample",
      "demo",
      "mail",
      "contact",
      "firstname",
      "lastname",
      "fullname",
      "firstlast",
      "firstnamelastname",
      "namehere"
    ]);
    const localLooksGeneric =
      genericLocalParts.has(localFlat) ||
      /^(user(name)?|your-?name|your-?email|name|email|test|example|sample|demo|mail|contact|first(name)?|last(name)?|full(name)?)$/i.test(local);
    if (localLooksGeneric && /(example|domain|test|localhost|invalid|sample|demo|placeholder|dummy|companyname|yourdomain|mydomain|yourcompany)/i.test(domain)) {
      return true;
    }
    if (localLooksGeneric && strongPlaceholderRoots.has(domainRootFlat)) return true;

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

  const normalizeAddressCandidate = (value) => {
    let text = normalize(value);
    if (!text) return "";
    text = text
      .replace(/^(?:our\s+)?address\s*[:\-]?\s*/i, "")
      .replace(/^located\s+at\s+/i, "");
    if (!text || text.length < 8 || text.length > 180) return "";
    if (/@|https?:\/\//i.test(text)) return "";
    return text;
  };

  const isLikelyAddressCandidate = (value) => {
    const text = normalizeAddressCandidate(value);
    if (!text) return false;
    const lower = text.toLowerCase();
    const hasStreetNumber = /\b\d{1,6}[a-z]?\b/i.test(text);
    const hasStreetType = /\b(street|st|avenue|ave|road|rd|drive|dr|lane|ln|boulevard|blvd|highway|hwy|way|court|ct|circle|cir|parkway|pkwy|place|pl|suite|ste|unit|floor|fl)\b/i.test(lower);
    const hasPostalCode = /\b\d{5}(?:-\d{4})?\b/.test(text);
    const hasComma = text.includes(",");
    return (hasStreetNumber && hasStreetType) || (hasStreetNumber && hasPostalCode) || (hasStreetNumber && hasComma);
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
      .replace(/\\u002e/gi, ".")
      .replace(/\\x2f/gi, "/")
      .replace(/\\u002f/gi, "/")
      .replace(/\\x3a/gi, ":")
      .replace(/\\u003a/gi, ":")
      .replace(/\\x3f/gi, "?")
      .replace(/\\u003f/gi, "?")
      .replace(/\\x3d/gi, "=")
      .replace(/\\u003d/gi, "=")
      .replace(/\\x26/gi, "&")
      .replace(/\\u0026/gi, "&");
    return normalize(out);
  };

  const decodeUrlEncodedContactText = (value) => {
    let out = normalize(value);
    if (!out) return "";
    out = out
      .replace(/%40/gi, "@")
      .replace(/%2e/gi, ".")
      .replace(/%2f/gi, "/")
      .replace(/%3a/gi, ":")
      .replace(/%3f/gi, "?")
      .replace(/%3d/gi, "=")
      .replace(/%26/gi, "&")
      .replace(/%23/gi, "#");
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

  const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const isElementVisible = (node) => {
    if (!node || typeof node.getBoundingClientRect !== "function") return false;
    const rect = node.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };

  const clickElement = (node) => {
    if (!node || typeof node.click !== "function") return false;
    try {
      node.dispatchEvent(new MouseEvent("mousedown", { bubbles: true }));
      node.dispatchEvent(new MouseEvent("mouseup", { bubbles: true }));
      node.click();
      return true;
    } catch (_error) {
      return false;
    }
  };

  const dispatchEscape = () => {
    const down = new KeyboardEvent("keydown", {
      key: "Escape",
      code: "Escape",
      keyCode: 27,
      which: 27,
      bubbles: true,
      cancelable: true
    });
    const up = new KeyboardEvent("keyup", {
      key: "Escape",
      code: "Escape",
      keyCode: 27,
      which: 27,
      bubbles: true,
      cancelable: true
    });
    const targets = [document.activeElement, document.body, document, window];
    let sent = 0;
    for (const target of targets) {
      if (!target || typeof target.dispatchEvent !== "function") continue;
      try {
        target.dispatchEvent(down);
        target.dispatchEvent(up);
        sent += 1;
      } catch (_error) {
        // Ignore.
      }
    }
    return sent;
  };

  const looksLikeFacebookLoginWall = (node) => {
    if (!node) return false;
    const text = normalize((node.innerText || node.textContent || "")).toLowerCase();
    if (!text) return false;
    if (text.includes("see more on facebook")) return true;
    const hasLogin = text.includes("log in") || text.includes("login");
    const hasCreate = text.includes("create new account") || text.includes("sign up");
    const hasForgot = text.includes("forgotten password") || text.includes("forgot password");
    return hasLogin && (hasCreate || hasForgot);
  };

  const hideNodeHard = (node) => {
    if (!node || !node.style) return false;
    try {
      node.style.setProperty("display", "none", "important");
      node.style.setProperty("visibility", "hidden", "important");
      node.style.setProperty("opacity", "0", "important");
      node.style.setProperty("pointer-events", "none", "important");
      node.setAttribute("aria-hidden", "true");
      return true;
    } catch (_error) {
      return false;
    }
  };

  const stripFacebookScrollLocks = () => {
    for (const root of [document.documentElement, document.body]) {
      if (!root || !root.style) continue;
      try {
        root.style.setProperty("overflow", "auto", "important");
        root.style.setProperty("position", "static", "important");
      } catch (_error) {
        // Ignore style write failures.
      }
    }
  };

  const collectVisibleFacebookLoginWalls = () => {
    const selectors = [
      "div[role='dialog']",
      "div[aria-modal='true']",
      "div[role='complementary']",
      "aside",
      "footer"
    ];
    const walls = [];
    const seen = new Set();
    for (const selector of selectors) {
      const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 60);
      for (const node of nodes) {
        if (!node || seen.has(node)) continue;
        seen.add(node);
        if (!isElementVisible(node)) continue;
        if (!looksLikeFacebookLoginWall(node)) continue;
        walls.push(node);
      }
    }
    return walls;
  };

  const hasVisibleFacebookLoginWall = () => collectVisibleFacebookLoginWalls().length > 0;

  const hideFacebookLoginPrompts = () => {
    const nodes = collectVisibleFacebookLoginWalls();
    let removed = 0;
    for (const node of nodes) {
      if (hideNodeHard(node)) {
        removed += 1;
      }
      let current = node.parentElement;
      for (let depth = 0; depth < 4 && current; depth += 1) {
        const rect = typeof current.getBoundingClientRect === "function" ? current.getBoundingClientRect() : null;
        if (rect && rect.width >= window.innerWidth * 0.75 && rect.height >= window.innerHeight * 0.6) {
          if (hideNodeHard(current)) {
            removed += 1;
          }
        }
        current = current.parentElement;
      }
    }

    if (removed > 0) {
      stripFacebookScrollLocks();
    }
    return removed;
  };

  const clickOutsideDialog = (dialog) => {
    if (!dialog || typeof dialog.contains !== "function") return 0;
    const points = [
      [10, 10],
      [Math.max(10, window.innerWidth - 10), 10],
      [10, Math.max(10, window.innerHeight - 10)],
      [Math.max(10, window.innerWidth - 10), Math.max(10, window.innerHeight - 10)]
    ];
    let clicked = 0;
    for (const [x, y] of points) {
      const target = document.elementFromPoint(x, y);
      if (!target || dialog.contains(target)) continue;
      if (clickElement(target)) clicked += 1;
    }
    return clicked;
  };

  const hardDismissFacebookLoginDialogs = () => {
    const walls = collectVisibleFacebookLoginWalls().slice(0, 12);
    if (walls.length === 0) {
      return 0;
    }

    let removed = 0;
    dispatchEscape();
    for (const wall of walls) {
      clickOutsideDialog(wall);

      let current = wall;
      for (let depth = 0; depth < 8 && current; depth += 1) {
        const rect = typeof current.getBoundingClientRect === "function" ? current.getBoundingClientRect() : null;
        const style = window.getComputedStyle(current);
        const coversMostViewport =
          rect &&
          rect.width >= window.innerWidth * 0.7 &&
          rect.height >= window.innerHeight * 0.6;
        const isOverlayLike =
          style.position === "fixed" ||
          style.position === "sticky" ||
          style.position === "absolute";
        if (coversMostViewport || isOverlayLike || current === wall) {
          if (hideNodeHard(current)) removed += 1;
        }
        current = current.parentElement;
      }
    }

    removed += hideFacebookLoginPrompts();
    if (removed > 0) {
      stripFacebookScrollLocks();
    }
    return removed;
  };

  const closeFacebookSigninOverlays = () => {
    const walls = collectVisibleFacebookLoginWalls();
    if (walls.length === 0) {
      return 0;
    }

    const closeSelectors = [
      "[aria-label='Close']",
      "[aria-label*='close' i]",
      "[aria-label*='not now' i]",
      "[aria-label*='cancel' i]",
      "[aria-label*='dismiss' i]"
    ];
    let actions = 0;

    for (const wall of walls.slice(0, 6)) {
      for (const selector of closeSelectors) {
        const nodes = Array.from(wall.querySelectorAll(selector)).slice(0, 8);
        for (const node of nodes) {
          if (!isElementVisible(node)) continue;
          if (clickElement(node)) {
            actions += 1;
          }
        }
      }
      if (actions > 0) break;
    }

    if (actions === 0) {
      const closeTokens = ["close", "not now", "cancel", "dismiss", "maybe later", "skip", "continue without"];
      for (const wall of walls.slice(0, 6)) {
        const dialogButtons = Array.from(wall.querySelectorAll("button, [role='button']")).slice(0, 24);
        for (const node of dialogButtons) {
          if (!isElementVisible(node)) continue;
          const label = normalize(node.getAttribute("aria-label") || node.textContent || "").toLowerCase();
          if (!label) continue;
          if (!closeTokens.some((token) => label === token || label.includes(token))) continue;
          if (clickElement(node)) {
            actions += 1;
            break;
          }
        }
        if (actions > 0) break;
      }
    }

    if (actions === 0) {
      dispatchEscape();
      actions += 1;
    }

    stripFacebookScrollLocks();
    return actions;
  };

  const dismissFacebookLoginWallsStably = async () => {
    let clearRounds = 0;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      if (!hasVisibleFacebookLoginWall()) {
        clearRounds += 1;
        if (clearRounds >= 1) {
          return true;
        }
        await wait(80);
        continue;
      }

      clearRounds = 0;
      closeFacebookSigninOverlays();
      await wait(120);

      if (hasVisibleFacebookLoginWall()) {
        hardDismissFacebookLoginDialogs();
      } else {
        stripFacebookScrollLocks();
      }

      await wait(100);
    }

    if (hasVisibleFacebookLoginWall()) {
      hardDismissFacebookLoginDialogs();
      await wait(100);
    }
    return !hasVisibleFacebookLoginWall();
  };

  const expandFacebookSections = () => {
    const needles = [
      "see more",
      "contact and basic info",
      "contact info",
      "intro",
      "page transparency"
    ];
    const nodes = Array.from(document.querySelectorAll("a, button, div[role='button'], span[role='button']")).slice(0, 2000);
    let expanded = 0;
    for (const node of nodes) {
      if (expanded >= 10) break;
      if (!isElementVisible(node)) continue;
      const label = normalize(
        node.getAttribute("aria-label") ||
        node.textContent ||
        ""
      ).toLowerCase();
      if (!label) continue;
      if (!needles.some((needle) => label === needle || label.includes(needle))) continue;
      if (clickElement(node)) {
        expanded += 1;
      }
    }
    return expanded;
  };

  const prepareFacebookPage = async () => {
    await dismissFacebookLoginWallsStably();
    expandFacebookSections();
    window.scrollTo(0, 0);
    await wait(220);
    await dismissFacebookLoginWallsStably();
    expandFacebookSections();
  };

  const hostname = normalize(window.location.hostname || "").toLowerCase();
  if (hostname.includes("facebook.com")) {
    await prepareFacebookPage();
  }

  const rawBodyMultiline = document.body ? String(document.body.innerText || "") : "";
  const rawBodyText = normalize(rawBodyMultiline);
  const rawBodyLines = rawBodyMultiline
    .split(/\n+/)
    .map((line) => normalize(line))
    .filter(Boolean);
  const bodyText = deobfuscateEmailText(rawBodyText);
  const emails = new Set();
  const phones = new Set();
  const addresses = new Set();
  const ownerCandidates = [];
  const orgNameSet = new Set();
  const platform = detectPlatform();
  const structuredSocialCandidates = new Set();

  const registerAddress = (value) => {
    const normalizedAddress = normalizeAddressCandidate(value);
    if (!normalizedAddress || !isLikelyAddressCandidate(normalizedAddress)) return;
    addresses.add(normalizedAddress);
  };

  const isKnownSocialHost = (hostValue) => {
    const host = normalize(hostValue).toLowerCase();
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
  };

  const normalizeSocialUrl = (value) => {
    const decoded = decodeEscapedText(decodeHtmlEntities(value));
    let candidate = normalize(decoded)
      .replace(/^[('"\\\s]+|[)'"\\\s]+$/g, "")
      .replace(/[),.;]+$/, "");
    if (!candidate) return "";

    if (/^\/\//.test(candidate)) {
      candidate = `https:${candidate}`;
    } else if (
      /^(?:www\.|m\.)?(?:facebook\.com|instagram\.com|linkedin\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com|threads\.net)\//i.test(candidate)
    ) {
      candidate = `https://${candidate.replace(/^https?:\/\//i, "")}`;
    }

    if (!/^https?:\/\//i.test(candidate)) {
      return "";
    }

    try {
      const parsed = new URL(candidate);
      if (!isKnownSocialHost(parsed.hostname)) return "";
      parsed.hash = "";
      const cleaned = normalize(parsed.toString()).replace(/[),.;]+$/, "");
      return cleaned;
    } catch (_error) {
      return "";
    }
  };

  const collectSocialUrlsFromText = (text, target) => {
    const sourceText = decodeEscapedText(decodeHtmlEntities(text));
    if (!sourceText) return;
    const patterns = [
      /https?:\/\/(?:www\.|m\.)?(?:facebook\.com|instagram\.com|linkedin\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com|threads\.net)\/[^\s"'<>]+/gi,
      /(?:^|[\s(,;])(?:www\.|m\.)?(?:facebook\.com|instagram\.com|linkedin\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com|threads\.net)\/[^\s"'<>]+/gi
    ];
    for (const pattern of patterns) {
      let match = pattern.exec(sourceText);
      while (match) {
        const candidate = normalizeSocialUrl(match[0]);
        if (candidate) {
          target.add(candidate);
        }
        match = pattern.exec(sourceText);
      }
    }
  };

  const collectSourceSignals = (text, emailTarget, socialTarget) => {
    const sourceText = normalize(text);
    if (!sourceText) return;

    const candidates = [];
    const pushCandidate = (value) => {
      const normalizedValue = normalize(value);
      if (!normalizedValue) return;
      if (!candidates.includes(normalizedValue)) {
        candidates.push(normalizedValue);
      }
    };

    pushCandidate(sourceText);
    pushCandidate(sourceText.replace(/\\\//g, "/").replace(/&quot;/gi, "\"").replace(/&#x2F;/gi, "/"));
    pushCandidate(decodeEscapedText(decodeHtmlEntities(sourceText)));
    pushCandidate(decodeUrlEncodedContactText(sourceText));
    pushCandidate(decodeUrlEncodedContactText(decodeEscapedText(decodeHtmlEntities(sourceText))));

    for (const candidate of candidates) {
      collectEmailsFromText(candidate, emailTarget);
      if (socialTarget instanceof Set) {
        collectSocialUrlsFromText(candidate, socialTarget);
      }
    }
  };

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
  for (const line of rawBodyLines.slice(0, 500)) {
    registerAddress(line);
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
    registerAddress(text);
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
      registerAddress(aria);
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
    const sameAs = Array.isArray(obj.sameAs) ? obj.sameAs : [obj.sameAs, obj.url];
    for (const value of sameAs) {
      const normalizedSocial = normalizeSocialUrl(value);
      if (normalizedSocial) {
        structuredSocialCandidates.add(normalizedSocial);
      }
    }

    const addressValue = obj.address;
    if (typeof addressValue === "string") {
      registerAddress(addressValue);
    } else if (addressValue && typeof addressValue === "object") {
      registerAddress([
        addressValue.streetAddress,
        addressValue.addressLocality,
        addressValue.addressRegion,
        addressValue.postalCode,
        addressValue.addressCountry
      ].filter(Boolean).join(", "));
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

  const relatedLinkSet = new Set();
  const internalLinkSet = new Set();
  const socialLinkSet = new Set();
  const focusedLinkMap = new Map();
  const relatedKeywords = /(about|contact|get\s+in\s+touch|reach\s+us|call\s+us|team|leadership|management|staff|company|our-story|who-we-are|founder|owner|careers?|jobs?|join\s+us|work\s+with\s+us|vacanc(?:y|ies))/i;
  const pageOrigin = window.location.origin;
  const anchors = Array.from(document.querySelectorAll("a[href]"));
  let hasFooterFacebookLink = false;

  const inferFocusedTypeFromText = (value) => {
    const text = normalize(value).toLowerCase();
    if (!text) return "";
    if (/(?:^|\b)(contact(?:\s*us)?|get\s*in\s*touch|reach\s*us|call\s*us)(?:\b|$)/i.test(text)) return "contact";
    if (/(?:^|\b)(about(?:\s*us)?|our\s*story|who\s*we\s*are|company)(?:\b|$)/i.test(text)) return "about";
    if (/(?:^|\b)(team|our\s*team|leadership|management|staff|founder|owner)(?:\b|$)/i.test(text)) return "team";
    if (/(?:^|\b)(careers?|jobs?|join\s*us|work\s*with\s*us|vacanc(?:y|ies))(?:\b|$)/i.test(text)) return "careers";
    return "";
  };

  const focusedTypeRank = (type) => {
    const normalized = normalize(type).toLowerCase();
    if (normalized === "contact") return 4;
    if (normalized === "about") return 3;
    if (normalized === "team") return 2;
    if (normalized === "careers") return 1;
    return 0;
  };

  const registerFocusedLink = (absoluteUrl, type, source) => {
    const normalizedType = inferFocusedTypeFromText(type);
    if (!absoluteUrl || !normalizedType) return;
    if (!isCrawlableInternal(absoluteUrl)) return;
    const existing = focusedLinkMap.get(absoluteUrl);
    const currentRank = existing ? focusedTypeRank(existing.type) : 0;
    const nextRank = focusedTypeRank(normalizedType);
    if (!existing || nextRank >= currentRank) {
      focusedLinkMap.set(absoluteUrl, {
        url: absoluteUrl,
        type: normalizedType,
        source: normalize(source || "")
      });
    }
  };

  const collectSocialFromWrappedUrl = (urlValue) => {
    try {
      const parsed = new URL(urlValue);
      const paramsToCheck = ["u", "url", "target", "href", "dest", "redirect", "to", "link"];
      for (const key of paramsToCheck) {
        const candidate = parsed.searchParams.get(key);
        if (!candidate) continue;
        collectSocialUrlsFromText(candidate, socialLinkSet);
      }
    } catch (_error) {
      // Ignore malformed wrapper URLs.
    }
  };

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
    const aria = normalize(anchor.getAttribute("aria-label") || "");
    const title = normalize(anchor.getAttribute("title") || "");
    collectSocialUrlsFromText(`${href} ${text} ${aria}`, socialLinkSet);

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
      let hrefCandidate = href;
      if (
        /^(?:www\.|m\.)?(?:facebook\.com|instagram\.com|linkedin\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com|threads\.net)\//i.test(hrefCandidate)
      ) {
        hrefCandidate = `https://${hrefCandidate}`;
      } else if (
        /^\/\/(?:www\.|m\.)?(?:facebook\.com|instagram\.com|linkedin\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com|threads\.net)\//i.test(hrefCandidate)
      ) {
        hrefCandidate = `https:${hrefCandidate}`;
      }
      absolute = new URL(hrefCandidate, window.location.href).toString();
    } catch (_e) {
      absolute = "";
    }

    if (!absolute) continue;

    const absoluteSocial = normalizeSocialUrl(absolute);
    if (absoluteSocial) {
      socialLinkSet.add(absoluteSocial);
      let socialHost = "";
      try {
        socialHost = normalize(new URL(absoluteSocial).hostname || "").toLowerCase();
      } catch (_socialUrlError) {
        socialHost = "";
      }
      if (socialHost.includes("facebook.com") && typeof anchor.closest === "function") {
        if (anchor.closest("footer, [role='contentinfo'], [id*='footer' i], [class*='footer' i]")) {
          hasFooterFacebookLink = true;
        }
      }
      continue;
    }
    collectSocialFromWrappedUrl(absolute);

    if (isCrawlableInternal(absolute)) {
      internalLinkSet.add(absolute);
      const focusedType = inferFocusedTypeFromText(`${text} ${aria} ${title} ${absolute}`);
      if (focusedType) {
        registerFocusedLink(absolute, focusedType, "anchor");
      }
    }

    const probe = `${text} ${aria} ${title} ${absolute}`;
    if (!relatedKeywords.test(probe)) continue;

    if (relatedLinkSet.size < 30) {
      relatedLinkSet.add(absolute);
    }
  }

  const shouldParseSourceHtmlFallback =
    parseSourceHtmlEnabled &&
    !hasFooterFacebookLink &&
    focusedLinkMap.size === 0;
  const rawHtmlSource = parseSourceHtmlEnabled && document.documentElement
    ? normalize(document.documentElement.innerHTML.slice(0, hostname.includes("facebook.com") ? 520000 : 320000))
    : "";
  if (rawHtmlSource) {
    collectSourceSignals(rawHtmlSource, emails, socialLinkSet);
  }
  if (shouldParseSourceHtmlFallback && rawHtmlSource) {
    const htmlSnippet = rawHtmlSource.slice(0, 260000);
    const sourceAnchorPattern = /<a\b[^>]{0,1200}?href\s*=\s*(["'])(.*?)\1[\s\S]{0,280}?<\/a>/gi;
    let sourceMatch = sourceAnchorPattern.exec(htmlSnippet);
    let sourceScanned = 0;
    while (sourceMatch && sourceScanned < 280) {
      sourceScanned += 1;
      const hrefValue = normalize(decodeHtmlEntities(sourceMatch[2] || ""));
      let absolute = "";
      try {
        absolute = new URL(hrefValue, window.location.href).toString();
      } catch (_error) {
        absolute = "";
      }

      if (absolute && isCrawlableInternal(absolute)) {
        internalLinkSet.add(absolute);
        const sourceText = normalize(
          decodeEscapedText(decodeHtmlEntities(sourceMatch[0] || ""))
            .replace(/<[^>]+>/g, " ")
        );
        const sourceFocusedType = inferFocusedTypeFromText(sourceText);
        if (sourceFocusedType) {
          registerFocusedLink(absolute, sourceFocusedType, "html_source");
          if (relatedLinkSet.size < 30) {
            relatedLinkSet.add(absolute);
          }
        }
      }

      sourceMatch = sourceAnchorPattern.exec(htmlSnippet);
    }
  }

  for (const candidate of structuredSocialCandidates) {
    socialLinkSet.add(candidate);
  }
  collectSocialUrlsFromText(rawBodyText, socialLinkSet);
  collectSocialUrlsFromText(bodyText, socialLinkSet);

  const socialScriptBlob = scriptBlob
    .replace(/\\\//g, "/")
    .replace(/&quot;/gi, "\"")
    .replace(/&#x2F;/gi, "/");
  collectSocialUrlsFromText(scriptBlob, socialLinkSet);
  collectSocialUrlsFromText(socialScriptBlob, socialLinkSet);

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
    addresses: Array.from(addresses).slice(0, 12),
    ownerCandidates,
    relatedLinks: Array.from(relatedLinkSet).slice(0, 50),
    focusedLinks: Array.from(focusedLinkMap.values()).slice(0, 40),
    internalLinks: Array.from(internalLinkSet).slice(0, 260),
    socialLinks: Array.from(socialLinkSet).slice(0, 12),
    hasFooterFacebookLink,
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
