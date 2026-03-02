(function () {
  const shared = window.GbpShared;
  const { MSG, DEFAULT_MAX_ROWS, CSV_COLUMNS, COLUMN_LABELS, readFilterConfig, normalizeText, sanitizeColumns } = shared;

  const el = {
    maxRows: document.getElementById("maxRows"),
    infiniteScroll: document.getElementById("infiniteScroll"),
    enableEnrichment: document.getElementById("enableEnrichment"),
    siteMaxPages: document.getElementById("siteMaxPages"),
    showEnrichmentTabs: document.getElementById("showEnrichmentTabs"),
    scanSocialLinks: document.getElementById("scanSocialLinks"),
    minRating: document.getElementById("minRating"),
    maxRating: document.getElementById("maxRating"),
    minReviews: document.getElementById("minReviews"),
    maxReviews: document.getElementById("maxReviews"),
    nameKeyword: document.getElementById("nameKeyword"),
    categoryInclude: document.getElementById("categoryInclude"),
    categoryExclude: document.getElementById("categoryExclude"),
    hasWebsite: document.getElementById("hasWebsite"),
    hasPhone: document.getElementById("hasPhone"),
    columnList: document.getElementById("columnList"),
    columnsTitle: document.getElementById("columnsTitle"),
    columnsAllBtn: document.getElementById("columnsAllBtn"),
    columnsNoneBtn: document.getElementById("columnsNoneBtn"),
    startBtn: document.getElementById("startBtn"),
    stopBtn: document.getElementById("stopBtn"),
    exportBtn: document.getElementById("exportBtn"),
    processed: document.getElementById("processed"),
    matched: document.getElementById("matched"),
    duplicates: document.getElementById("duplicates"),
    errors: document.getElementById("errors"),
    speedStat: document.getElementById("speedStat"),
    seenListingsStat: document.getElementById("seenListingsStat"),
    avgRatingStat: document.getElementById("avgRatingStat"),
    avgReviewsStat: document.getElementById("avgReviewsStat"),
    crawlVisitedStat: document.getElementById("crawlVisitedStat"),
    crawlDiscoveredStat: document.getElementById("crawlDiscoveredStat"),
    socialScannedStat: document.getElementById("socialScannedStat"),
    stateText: document.getElementById("stateText"),
    errorText: document.getElementById("errorText")
  };

  let lastRows = null;
  let scrapeRunning = false;
  let enrichRunning = false;
  let selectedColumns = [...CSV_COLUMNS];
  let infiniteScrollEnabled = false;
  let runInfiniteCurrent = false;
  let enrichmentEnabled = false;
  let siteMaxPagesValue = 40;
  let showEnrichmentTabsEnabled = false;
  let scanSocialLinksEnabled = true;

  init();

  async function init() {
    el.maxRows.value = String(DEFAULT_MAX_ROWS);
    bindEvents();
    await restoreState();

    el.infiniteScroll.checked = infiniteScrollEnabled;
    el.enableEnrichment.checked = enrichmentEnabled;
    el.siteMaxPages.value = String(siteMaxPagesValue);
    el.showEnrichmentTabs.checked = showEnrichmentTabsEnabled;
    el.scanSocialLinks.checked = scanSocialLinksEnabled;

    syncInfiniteScrollInput();
    renderColumnSelector();
    updateColumnsTitle();
    setRunningState(isBusy());

    chrome.runtime.onMessage.addListener(onRuntimeMessage);
  }

  function bindEvents() {
    el.startBtn.addEventListener("click", onStart);
    el.stopBtn.addEventListener("click", onStop);
    el.exportBtn.addEventListener("click", onExport);
    el.infiniteScroll.addEventListener("change", onInfiniteScrollToggle);
    el.enableEnrichment.addEventListener("change", onEnrichmentToggle);
    el.siteMaxPages.addEventListener("change", onSiteMaxPagesChange);
    el.showEnrichmentTabs.addEventListener("change", onShowEnrichmentTabsToggle);
    el.scanSocialLinks.addEventListener("change", onScanSocialLinksToggle);
    el.columnsAllBtn.addEventListener("click", onSelectAllColumns);
    el.columnsNoneBtn.addEventListener("click", onClearColumns);
  }

  async function onStart() {
    clearError();

    const tab = await getActiveTab();
    if (!tab || !tab.id) {
      setError("No active tab found.");
      return;
    }

    if (!isMapsUrl(tab.url)) {
      setError("Open a Google Maps search results page first.");
      return;
    }

    const config = readConfig();
    if (!config) {
      return;
    }

    scrapeRunning = true;
    runInfiniteCurrent = config.infiniteScroll === true;
    lastRows = null;
    setRunningState(isBusy());
    setState(runInfiniteCurrent ? "Scraping (infinite scroll)..." : "Scraping...");
    resetCounters();

    try {
      await ensureContentScriptReady(tab.id);

      let response;
      try {
        response = await sendMessageToTab(tab.id, {
          type: MSG.START_SCRAPE,
          config
        });
      } catch (firstErr) {
        if (!isNoReceiverError(firstErr)) {
          throw firstErr;
        }

        // Retry once after force-injecting scripts if receiver was not present yet.
        await ensureContentScriptReady(tab.id);
        response = await sendMessageToTab(tab.id, {
          type: MSG.START_SCRAPE,
          config
        });
      }

      if (!response || response.ok !== true) {
        throw new Error((response && response.error) || "Failed to start scrape");
      }

      // Fallback path: if popup missed SCRAPE_DONE runtime event, use direct response payload.
      if (response.result) {
        finalizeScrapeResult(response.result.rows, response.result.summary);
      }
    } catch (error) {
      scrapeRunning = false;
      runInfiniteCurrent = false;
      setRunningState(isBusy());
      if (isNoReceiverError(error)) {
        setError("Could not attach scraper to this tab. Refresh Google Maps and try again.");
      } else {
        setError(error && error.message ? error.message : "Could not start scrape");
      }
    }
  }

  async function onStop() {
    clearError();
    const tab = await getActiveTab();
    if (!tab || !tab.id) return;

    try {
      await ensureContentScriptReady(tab.id);
      await sendMessageToTab(tab.id, { type: MSG.STOP_SCRAPE });
      setState("Stopping...");
    } catch (_error) {
      setError("Could not send stop request");
    }
  }

  async function onExport() {
    clearError();

    if (!Array.isArray(lastRows)) {
      setError("No rows to export.");
      return;
    }

    if (!Array.isArray(selectedColumns) || selectedColumns.length === 0) {
      setError("Select at least one export column.");
      return;
    }

    if (!el.enableEnrichment.checked && selectedColumns.some(isEnrichmentColumn) && !rowsAlreadyEnriched(lastRows)) {
      setError("You selected enrichment columns. Enable 'Enrich websites' to populate them.");
      return;
    }

    if (enrichRunning) {
      setError("Website enrichment is still running. Wait for it to finish, then export.");
      return;
    }

    let rowsForExport = lastRows;

    try {
      if (el.enableEnrichment.checked) {
        rowsForExport = await enrichRowsBestEffort(rowsForExport, { force: false });
      }

      const filename = defaultFilename();
      const response = await sendRuntimeMessage({
        type: MSG.EXPORT_CSV,
        rows: rowsForExport,
        columns: sanitizeColumns(selectedColumns),
        filename
      });

      if (!response || response.type !== MSG.EXPORT_DONE) {
        throw new Error((response && response.error) || "CSV export failed");
      }

      setState(`Exported ${rowsForExport.length} row(s)`);
    } catch (error) {
      setError(error && error.message ? error.message : "CSV export failed");
    }
  }

  function onRuntimeMessage(message) {
    if (!message || !message.type) {
      return;
    }

    if (message.type === MSG.SCRAPE_PROGRESS) {
      el.processed.textContent = String(message.processed || 0);
      el.matched.textContent = String(message.matched || 0);
      el.duplicates.textContent = String(message.duplicates || 0);
      el.errors.textContent = String(message.errors || 0);
      updatePerformanceMetrics(message);
      const quickSkips = Number(message.fast_skipped || 0);
      setState(runInfiniteCurrent ? `Scraping (infinite scroll)... fast-skipped ${quickSkips}` : `Scraping... fast-skipped ${quickSkips}`);
      return;
    }

    if (message.type === MSG.SCRAPE_DONE) {
      finalizeScrapeResult(message.rows, message.summary);
      return;
    }

    if (message.type === MSG.SCRAPE_ERROR) {
      scrapeRunning = false;
      runInfiniteCurrent = false;
      setRunningState(isBusy());
      setError(message.error || "Scrape failed");
      setState("Failed");
      return;
    }

    if (message.type === MSG.ENRICH_PROGRESS) {
      const processed = Number(message.processed || 0);
      const total = Number(message.total || 0);
      const current = normalizeText(message.current || "");
      const phase = normalizeText(message.phase || "");
      const host = shortHost(message.current_url || "");
      const siteVisited = Number(message.site_pages_visited || 0);
      const siteDiscovered = Number(message.site_pages_discovered || 0);
      const socialScanned = Number(message.social_scanned || 0);
      const currentSuffix = current ? ` ${current}` : "";
      const phaseSuffix = phase ? ` (${phase})` : "";
      const hostSuffix = host ? ` @ ${host}` : "";
      const crawlSuffix = ` pages ${siteVisited}/${siteDiscovered}`;
      setState(`Enriching websites ${processed}/${total}${phaseSuffix}${hostSuffix}${crawlSuffix}...${currentSuffix}`);
      el.crawlVisitedStat.textContent = String(Number.isFinite(siteVisited) ? siteVisited : 0);
      el.crawlDiscoveredStat.textContent = String(Number.isFinite(siteDiscovered) ? siteDiscovered : 0);
      el.socialScannedStat.textContent = String(Number.isFinite(socialScanned) ? socialScanned : 0);
      return;
    }

    if (message.type === MSG.ENRICH_ERROR) {
      enrichRunning = false;
      setRunningState(isBusy());
      setError(message.error || "Website enrichment failed");
      setState("Enrichment failed");
    }
  }

  function readConfig() {
    const infiniteScroll = Boolean(el.infiniteScroll.checked);
    const maxRowsValue = Number(el.maxRows.value);
    if (!infiniteScroll && (!Number.isFinite(maxRowsValue) || maxRowsValue <= 0)) {
      setError("Max rows must be greater than 0.");
      return null;
    }
    const effectiveMaxRows = Number.isFinite(maxRowsValue) && maxRowsValue > 0 ? maxRowsValue : DEFAULT_MAX_ROWS;

    const minRating = parseOptionalNumber(el.minRating.value);
    const maxRating = parseOptionalNumber(el.maxRating.value);
    if (minRating != null && maxRating != null && minRating > maxRating) {
      setError("Min rating cannot be greater than max rating.");
      return null;
    }

    const minReviews = parseOptionalNumber(el.minReviews.value);
    const maxReviews = parseOptionalNumber(el.maxReviews.value);
    if (minReviews != null && maxReviews != null && minReviews > maxReviews) {
      setError("Min reviews cannot be greater than max reviews.");
      return null;
    }

    const formValues = {
      minRating: el.minRating.value,
      maxRating: el.maxRating.value,
      minReviews: el.minReviews.value,
      maxReviews: el.maxReviews.value,
      nameKeyword: el.nameKeyword.value,
      categoryInclude: el.categoryInclude.value,
      categoryExclude: el.categoryExclude.value,
      hasWebsite: el.hasWebsite.checked,
      hasPhone: el.hasPhone.checked
    };

    return {
      maxRows: effectiveMaxRows,
      infiniteScroll,
      filters: readFilterConfig(formValues)
    };
  }

  function setRunningState(running) {
    el.startBtn.disabled = running;
    el.stopBtn.disabled = !scrapeRunning;
    el.exportBtn.disabled = running || !Array.isArray(lastRows) || selectedColumns.length === 0;
  }

  function resetCounters() {
    el.processed.textContent = "0";
    el.matched.textContent = "0";
    el.duplicates.textContent = "0";
    el.errors.textContent = "0";
    el.speedStat.textContent = "0.00/s";
    el.seenListingsStat.textContent = "0";
    el.avgRatingStat.textContent = "-";
    el.avgReviewsStat.textContent = "-";
    el.crawlVisitedStat.textContent = "0";
    el.crawlDiscoveredStat.textContent = "0";
    el.socialScannedStat.textContent = "0";
  }

  function setState(text) {
    el.stateText.textContent = normalizeText(text);
  }

  function setError(text) {
    el.errorText.textContent = normalizeText(text);
  }

  function clearError() {
    el.errorText.textContent = "";
  }

  async function restoreState() {
    try {
      const data = await storageGet([
        "lastRows",
        "selectedColumns",
        "infiniteScrollEnabled",
        "enrichmentEnabled",
        "siteMaxPagesValue",
        "showEnrichmentTabsEnabled",
        "scanSocialLinksEnabled"
      ]);
      if (Array.isArray(data.lastRows)) {
        lastRows = data.lastRows;
      }
      if (Array.isArray(data.selectedColumns)) {
        selectedColumns = normalizeSelectedColumns(data.selectedColumns);
      }
      infiniteScrollEnabled = data.infiniteScrollEnabled === true;
      enrichmentEnabled = data.enrichmentEnabled === true;
      siteMaxPagesValue = clampInt(data.siteMaxPagesValue, 1, 120, 40);
      showEnrichmentTabsEnabled = data.showEnrichmentTabsEnabled === true;
      scanSocialLinksEnabled = data.scanSocialLinksEnabled !== false;
      el.exportBtn.disabled = isBusy() || !Array.isArray(lastRows) || selectedColumns.length === 0;
    } catch (_error) {
      el.exportBtn.disabled = true;
    }
  }

  function persistRows(rows) {
    storageSet({ lastRows: rows }).catch(() => {});
    setRunningState(isBusy());
  }

  function finalizeScrapeResult(rowsInput, summaryInput) {
    scrapeRunning = false;
    runInfiniteCurrent = false;
    setRunningState(isBusy());

    const rows = Array.isArray(rowsInput) ? rowsInput : [];
    const summary = summaryInput || {};

    lastRows = rows;
    persistRows(rows);

    el.processed.textContent = String(summary.processed || 0);
    el.matched.textContent = String(summary.matched || rows.length || 0);
    el.duplicates.textContent = String(summary.duplicates || 0);
    el.errors.textContent = String(summary.errors || 0);
    updatePerformanceMetrics(summary);

    const status = summary.stopped ? "Stopped" : "Completed";
    const fastSkipped = Number(summary.fast_skipped || 0);
    setState(`${status}: ${rows.length} unique row(s), ${summary.duplicates || 0} duplicates, ${fastSkipped} fast-skipped`);

    if (rows.length === 0 && Number(summary.processed || 0) > 0) {
      setError("No rows extracted. Try disabling strict filters or run again with fewer active constraints.");
    }

    if (!summary.stopped && el.enableEnrichment.checked && rows.length > 0) {
      void enrichRowsBestEffort(rows, { force: false }).catch((error) => {
        setError(error && error.message ? error.message : "Website enrichment failed");
      });
    }
  }

  function isMapsUrl(url) {
    if (!url) return false;
    return /^https:\/\/([a-z0-9-]+\.)?google\.[a-z.]+\/maps\//i.test(url);
  }

  function defaultFilename() {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    return `gbp_export_${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}.csv`;
  }

  function sendMessageToTab(tabId, message) {
    return new Promise((resolve, reject) => {
      chrome.tabs.sendMessage(tabId, message, (response) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
          return;
        }
        resolve(response);
      });
    });
  }

  function ensureContentScriptReady(tabId) {
    return new Promise((resolve, reject) => {
      chrome.scripting.executeScript(
        {
          target: { tabId },
          files: ["shared.js", "content.js"]
        },
        () => {
          if (chrome.runtime.lastError) {
            reject(new Error(chrome.runtime.lastError.message || "Failed to inject scraper scripts"));
            return;
          }
          resolve();
        }
      );
    });
  }

  function sendRuntimeMessage(message) {
    return new Promise((resolve, reject) => {
      chrome.runtime.sendMessage(message, (response) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
          return;
        }
        resolve(response);
      });
    });
  }

  function getActiveTab() {
    return new Promise((resolve) => {
      chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
        resolve(tabs && tabs[0] ? tabs[0] : null);
      });
    });
  }

  function storageGet(keys) {
    return new Promise((resolve, reject) => {
      chrome.storage.local.get(keys, (result) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
          return;
        }
        resolve(result || {});
      });
    });
  }

  function storageSet(items) {
    return new Promise((resolve, reject) => {
      chrome.storage.local.set(items, () => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
          return;
        }
        resolve();
      });
    });
  }

  function parseOptionalNumber(value) {
    if (value === "" || value == null) return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function onInfiniteScrollToggle() {
    infiniteScrollEnabled = el.infiniteScroll.checked;
    syncInfiniteScrollInput();
    storageSet({ infiniteScrollEnabled }).catch(() => {});
  }

  function onEnrichmentToggle() {
    enrichmentEnabled = el.enableEnrichment.checked;
    storageSet({ enrichmentEnabled }).catch(() => {});
  }

  function onSiteMaxPagesChange() {
    siteMaxPagesValue = clampInt(el.siteMaxPages.value, 1, 120, 40);
    el.siteMaxPages.value = String(siteMaxPagesValue);
    storageSet({ siteMaxPagesValue }).catch(() => {});
  }

  function onShowEnrichmentTabsToggle() {
    showEnrichmentTabsEnabled = el.showEnrichmentTabs.checked;
    storageSet({ showEnrichmentTabsEnabled }).catch(() => {});
  }

  function onScanSocialLinksToggle() {
    scanSocialLinksEnabled = el.scanSocialLinks.checked;
    storageSet({ scanSocialLinksEnabled }).catch(() => {});
  }

  function syncInfiniteScrollInput() {
    const enabled = el.infiniteScroll.checked;
    el.maxRows.disabled = enabled;
    if (enabled) {
      el.maxRows.title = "Disabled while infinite scroll is enabled";
    } else {
      el.maxRows.title = "";
    }
  }

  function renderColumnSelector() {
    el.columnList.textContent = "";
    const selectedSet = new Set(selectedColumns);

    for (const column of CSV_COLUMNS) {
      const label = document.createElement("label");
      label.className = "checkbox column-checkbox";

      const input = document.createElement("input");
      input.type = "checkbox";
      input.value = column;
      input.checked = selectedSet.has(column);
      input.addEventListener("change", onColumnSelectionChange);

      const text = document.createTextNode(COLUMN_LABELS[column] || column);
      label.appendChild(input);
      label.appendChild(text);
      el.columnList.appendChild(label);
    }
  }

  function onSelectAllColumns() {
    selectedColumns = [...CSV_COLUMNS];
    renderColumnSelector();
    updateColumnsTitle();
    persistSelectedColumns();
    setRunningState(isBusy());
  }

  function onClearColumns() {
    selectedColumns = [];
    renderColumnSelector();
    updateColumnsTitle();
    persistSelectedColumns();
    setRunningState(isBusy());
  }

  function onColumnSelectionChange() {
    const checked = Array.from(el.columnList.querySelectorAll("input[type='checkbox']:checked")).map((node) => node.value);
    selectedColumns = normalizeSelectedColumns(checked);
    updateColumnsTitle();
    persistSelectedColumns();
    setRunningState(isBusy());
  }

  function updateColumnsTitle() {
    el.columnsTitle.textContent = `Export columns (${selectedColumns.length}/${CSV_COLUMNS.length})`;
  }

  function persistSelectedColumns() {
    storageSet({ selectedColumns }).catch(() => {});
  }

  function normalizeSelectedColumns(columns) {
    if (!Array.isArray(columns)) return [...CSV_COLUMNS];
    const out = [];
    const seen = new Set();
    for (const column of columns) {
      if (!CSV_COLUMNS.includes(column) || seen.has(column)) continue;
      seen.add(column);
      out.push(column);
    }
    return out;
  }

  function isBusy() {
    return scrapeRunning || enrichRunning;
  }

  function isEnrichmentColumn(column) {
    return column === "owner_name" ||
      column === "owner_title" ||
      column === "owner_email" ||
      column === "contact_email" ||
      column === "website_scan_status" ||
      column === "site_pages_visited" ||
      column === "site_pages_discovered" ||
      column === "social_pages_scanned";
  }

  function rowsAlreadyEnriched(rows) {
    if (!Array.isArray(rows) || rows.length === 0) return false;
    return rows.some((row) => normalizeText(row && row.website_scan_status) !== "");
  }

  async function enrichRowsBestEffort(rows, options) {
    const opts = options || {};
    const force = opts.force === true;

    if (!Array.isArray(rows) || rows.length === 0) return rows;
    if (enrichRunning) return Array.isArray(lastRows) ? lastRows : rows;
    if (!force && rowsAlreadyEnriched(rows)) return rows;

    enrichRunning = true;
    setRunningState(isBusy());
    setState("Enriching websites...");

    try {
      const enrichResponse = await sendRuntimeMessage({
        type: MSG.ENRICH_ROWS,
        rows,
        options: {
          maxPagesPerSite: clampInt(el.siteMaxPages.value, 1, 120, 40),
          timeoutMs: 10000,
          visibleTabs: Boolean(el.showEnrichmentTabs.checked),
          scanSocialLinks: Boolean(el.scanSocialLinks.checked),
          maxSocialPages: 4
        }
      });

      if (!enrichResponse || enrichResponse.type !== MSG.ENRICH_DONE) {
        throw new Error((enrichResponse && enrichResponse.error) || "Website enrichment failed");
      }

      const rowsForExport = Array.isArray(enrichResponse.rows) ? enrichResponse.rows : rows;
      lastRows = rowsForExport;
      persistRows(rowsForExport);

      const enrichSummary = enrichResponse.summary || {};
      el.crawlVisitedStat.textContent = String(Number(enrichSummary.pages_visited || 0));
      el.crawlDiscoveredStat.textContent = String(Number(enrichSummary.pages_discovered || 0));
      el.socialScannedStat.textContent = String(Number(enrichSummary.social_scanned || 0));
      setState(`Enrichment: ${enrichSummary.enriched || 0} enriched, ${enrichSummary.skipped || 0} skipped, ${enrichSummary.blocked || 0} blocked, pages ${enrichSummary.pages_visited || 0}/${enrichSummary.pages_discovered || 0}`);
      return rowsForExport;
    } finally {
      enrichRunning = false;
      setRunningState(isBusy());
    }
  }

  function updatePerformanceMetrics(payload) {
    const data = payload || {};
    const speed = Number(data.rate_per_sec || 0);
    const seen = Number(data.seen_listings || 0);
    const avgRating = data.avg_rating_seen;
    const avgReviews = data.avg_reviews_seen;

    el.speedStat.textContent = `${formatNumber(speed, 2)}/s`;
    el.seenListingsStat.textContent = Number.isFinite(seen) ? String(seen) : "0";
    el.avgRatingStat.textContent = formatMaybeNumber(avgRating, 2);
    el.avgReviewsStat.textContent = formatMaybeNumber(avgReviews, 1);
  }

  function formatMaybeNumber(value, digits) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return formatNumber(num, digits);
  }

  function formatNumber(value, digits) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "0";
    return num.toFixed(digits);
  }

  function clampInt(value, min, max, fallback) {
    const num = Number(value);
    if (!Number.isFinite(num)) return fallback;
    return Math.min(max, Math.max(min, Math.floor(num)));
  }

  function shortHost(url) {
    const text = normalizeText(url);
    if (!text) return "";
    try {
      const parsed = new URL(text);
      return parsed.hostname.replace(/^www\./i, "");
    } catch (_e) {
      return "";
    }
  }

  function isNoReceiverError(error) {
    const msg = error && error.message ? String(error.message) : "";
    return /receiving end does not exist/i.test(msg) || /could not establish connection/i.test(msg);
  }
})();
