(function () {
  const shared = window.GbpShared;
  const { MSG, DEFAULT_MAX_ROWS, CSV_COLUMNS, COLUMN_LABELS, readFilterConfig, applyFilters, normalizeText, normalizePhoneText, sanitizeColumns } = shared;
  const SCRAPE_SESSION_KEY = "scrapeSession";
  const ENRICH_SESSION_KEY = "enrichSession";
  const POPUP_UI_SETTINGS_KEY = "popupUiSettings";
  const CONTROL_PANEL_ANCHOR_WINDOW_KEY = "controlPanelAnchorWindowId";
  const ACTIVE_SCRAPE_FILTERS_KEY = "activeScrapeFilters";
  const EXTENSION_PAGE_PREFIX = chrome.runtime.getURL("");
  const FOCUSED_CRAWL_MAX_PAGES = 4;
  const EMAIL_COLUMNS = ["email", "owner_name", "owner_title", "owner_email", "contact_email"];
  const RAW_EMAIL_COLUMNS = ["owner_name", "owner_title", "owner_email", "contact_email"];
  const PHONE_COLUMNS = ["phone", "listing_phone", "website_phone", "website_phone_source"];
  const RAW_PHONE_COLUMNS = ["listing_phone", "website_phone", "website_phone_source"];
  const CORE_COLUMNS = ["name", "rating", "review_count", "category", "address", "website", "maps_url"];
  const EMAIL_META_COLUMNS = [
    "primary_email",
    "primary_email_type",
    "primary_email_source",
    "owner_confidence",
    "email_confidence",
    "email_source_url",
    "no_email_reason"
  ];
  const EMAIL_GOAL_COLUMNS = new Set([...EMAIL_COLUMNS, ...EMAIL_META_COLUMNS]);
  const PHONE_GOAL_COLUMNS = new Set(PHONE_COLUMNS);
  const ADVANCED_COLUMNS = new Set([
    "place_id",
    "hours",
    "source_query",
    "source_url",
    "scraped_at",
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
    "discovered_website"
  ]);
  const DEFAULT_SELECTED_COLUMNS = [...CORE_COLUMNS, ...PHONE_COLUMNS, ...EMAIL_COLUMNS];
  const COLUMN_GROUPS = [
    {
      id: "core",
      title: "Core Listing Data",
      description: "Main listing fields",
      columns: CORE_COLUMNS
    },
    {
      id: "phone",
      title: "Phone Output",
      description: "Best phone + detail fields",
      columns: ["phone", "listing_phone", "website_phone", "website_phone_source"]
    },
    {
      id: "email",
      title: "Email Output",
      description: "Best email + detail fields",
      columns: ["email", "owner_name", "owner_title", "owner_email", "contact_email"]
    },
    {
      id: "crawl",
      title: "Enrichment Crawl Meta",
      description: "Website and social scan diagnostics",
      columns: ["website_scan_status", "site_pages_visited", "site_pages_discovered", "social_pages_scanned", "social_links"],
      advanced: true
    },
    {
      id: "discovery",
      title: "Discovery Meta",
      description: "External lead discovery diagnostics",
      columns: ["discovery_status", "discovery_source", "discovery_query", "discovered_website"],
      advanced: true
    }
  ];
  const COLUMN_BADGES = {
    email: { text: "Unified", tone: "unified" },
    owner_email: { text: "Personal", tone: "raw" },
    contact_email: { text: "Company", tone: "raw" },
    primary_email: { text: "Auto", tone: "raw" },
    primary_email_type: { text: "Meta", tone: "meta" },
    primary_email_source: { text: "Meta", tone: "meta" },
    phone: { text: "Unified", tone: "unified" },
    listing_phone: { text: "Listing", tone: "raw" },
    website_phone: { text: "Scanned", tone: "raw" },
    website_phone_source: { text: "Meta", tone: "meta" },
    owner_confidence: { text: "Meta", tone: "meta" },
    email_confidence: { text: "Meta", tone: "meta" },
    email_source_url: { text: "Meta", tone: "meta" },
    no_email_reason: { text: "Meta", tone: "meta" },
    website_scan_status: { text: "Meta", tone: "meta" },
    site_pages_visited: { text: "Meta", tone: "meta" },
    site_pages_discovered: { text: "Meta", tone: "meta" },
    social_pages_scanned: { text: "Meta", tone: "meta" },
    social_links: { text: "Meta", tone: "meta" },
    discovery_status: { text: "Meta", tone: "meta" },
    discovery_source: { text: "Meta", tone: "meta" },
    discovery_query: { text: "Meta", tone: "meta" },
    discovered_website: { text: "Meta", tone: "meta" }
  };

  const el = {
    maxRows: document.getElementById("maxRows"),
    infiniteScroll: document.getElementById("infiniteScroll"),
    enableEnrichment: document.getElementById("enableEnrichment"),
    contactGoalEmail: document.getElementById("contactGoalEmail"),
    contactGoalPhone: document.getElementById("contactGoalPhone"),
    contactGoalsHint: document.getElementById("contactGoalsHint"),
    emailOutputGroup: document.getElementById("emailOutputGroup"),
    emailOutputMode: document.getElementById("emailOutputMode"),
    emailColumnsHint: document.getElementById("emailColumnsHint"),
    phoneOutputGroup: document.getElementById("phoneOutputGroup"),
    phoneOutputMode: document.getElementById("phoneOutputMode"),
    phoneColumnsHint: document.getElementById("phoneColumnsHint"),
    showEnrichmentTabsRow: document.getElementById("showEnrichmentTabsRow"),
    showEnrichmentTabs: document.getElementById("showEnrichmentTabs"),
    requireEmailForLeads: document.getElementById("requireEmailForLeads"),
    minRating: document.getElementById("minRating"),
    maxRating: document.getElementById("maxRating"),
    minReviews: document.getElementById("minReviews"),
    maxReviews: document.getElementById("maxReviews"),
    hasWebsite: document.getElementById("hasWebsite"),
    hasPhone: document.getElementById("hasPhone"),
    columnList: document.getElementById("columnList"),
    columnsTitle: document.getElementById("columnsTitle"),
    toggleAdvancedBtn: document.getElementById("toggleAdvancedBtn"),
    columnsAllBtn: document.getElementById("columnsAllBtn"),
    columnsNoneBtn: document.getElementById("columnsNoneBtn"),
    startBtn: document.getElementById("startBtn"),
    stopBtn: document.getElementById("stopBtn"),
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
    emailsFoundStat: document.getElementById("emailsFoundStat"),
    discoveryEmailsFoundStat: document.getElementById("discoveryEmailsFoundStat"),
    stateText: document.getElementById("stateText"),
    leadSignalText: document.getElementById("leadSignalText"),
    errorText: document.getElementById("errorText")
  };

  let lastRows = null;
  let scrapeRunning = false;
  let enrichRunning = false;
  let lastScrapeSession = null;
  let lastEnrichSession = null;
  let selectedColumns = normalizeSelectedColumns(DEFAULT_SELECTED_COLUMNS);
  let showAdvancedFields = false;
  let infiniteScrollEnabled = false;
  let runInfiniteCurrent = false;
  let maxRowsValue = DEFAULT_MAX_ROWS;
  let enrichmentEnabled = false;
  let contactGoalEmailEnabled = true;
  let contactGoalPhoneEnabled = true;
  let emailOutputModeValue = "unified_only";
  let phoneOutputModeValue = "unified_only";
  let showEnrichmentTabsEnabled = false;
  let requireEmailForLeadsEnabled = true;
  let scrapeRunTabId = null;
  let uiSettingsSaveTimer = null;

  init();

  async function init() {
    el.maxRows.value = String(DEFAULT_MAX_ROWS);
    bindEvents();
    await restoreState();
    await recoverRunningScrapeState();
    await recoverRunningEnrichState();

    el.maxRows.value = String(maxRowsValue);
    el.infiniteScroll.checked = infiniteScrollEnabled;
    el.enableEnrichment.checked = enrichmentEnabled;
    el.contactGoalEmail.checked = contactGoalEmailEnabled;
    el.contactGoalPhone.checked = contactGoalPhoneEnabled;
    el.emailOutputMode.value = emailOutputModeValue;
    el.phoneOutputMode.value = phoneOutputModeValue;
    el.showEnrichmentTabs.checked = showEnrichmentTabsEnabled;
    syncEnrichmentModeUi({ persist: false });
    if (el.requireEmailForLeads) {
      el.requireEmailForLeads.checked = requireEmailForLeadsEnabled;
    }
    syncOutputModesFromSelectedColumns();
    updateAdvancedToggleLabel();

    syncInfiniteScrollInput();
    syncContactGoalDependentUi({ persistColumns: false });
    ensureColumnSelectorRendered();
    setRunningState(isBusy());

    chrome.runtime.onMessage.addListener(onRuntimeMessage);
    chrome.storage.onChanged.addListener(onStorageChanged);
  }

  function bindEvents() {
    el.startBtn.addEventListener("click", onStart);
    el.stopBtn.addEventListener("click", onStop);
    el.infiniteScroll.addEventListener("change", onInfiniteScrollToggle);
    el.enableEnrichment.addEventListener("change", onEnrichmentToggle);
    el.contactGoalEmail.addEventListener("change", onContactGoalsChange);
    el.contactGoalPhone.addEventListener("change", onContactGoalsChange);
    el.emailOutputMode.addEventListener("change", onEmailOutputModeChange);
    el.phoneOutputMode.addEventListener("change", onPhoneOutputModeChange);
    el.showEnrichmentTabs.addEventListener("change", onShowEnrichmentTabsToggle);
    if (el.requireEmailForLeads) {
      el.requireEmailForLeads.addEventListener("change", onRequireEmailForLeadsToggle);
    }
    el.toggleAdvancedBtn.addEventListener("click", onToggleAdvancedFields);
    el.columnsAllBtn.addEventListener("click", onSelectAllColumns);
    el.columnsNoneBtn.addEventListener("click", onClearColumns);

    const settingInputs = [
      el.maxRows,
      el.minRating,
      el.maxRating,
      el.minReviews,
      el.maxReviews
    ];
    for (const input of settingInputs) {
      if (!input) continue;
      input.addEventListener("input", onUiSettingsInputChanged);
      input.addEventListener("change", onUiSettingsInputChanged);
    }
    el.hasWebsite.addEventListener("change", onUiSettingsInputChanged);
    el.hasPhone.addEventListener("change", onUiSettingsInputChanged);
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
    await persistRunSettingsForBackground();

    const runId = createRunId();
    scrapeRunTabId = tab.id;
    scrapeRunning = true;
    runInfiniteCurrent = config.infiniteScroll === true;
    lastRows = null;
    setRunningState(isBusy());
    setState(runInfiniteCurrent ? "Scraping (infinite scroll)..." : "Scraping...");
    setLeadSignal("", "info");
    resetCounters();

    try {
      await ensureContentScriptReady(tab.id);
      const payloadConfig = {
        ...config,
        runId,
        runTabId: tab.id
      };

      let response;
      try {
        response = await sendMessageToTab(tab.id, {
          type: MSG.START_SCRAPE,
          config: payloadConfig
        });
      } catch (firstErr) {
        if (!isNoReceiverError(firstErr)) {
          throw firstErr;
        }

        // Retry once after force-injecting scripts if receiver was not present yet.
        await ensureContentScriptReady(tab.id);
        response = await sendMessageToTab(tab.id, {
          type: MSG.START_SCRAPE,
          config: payloadConfig
        });
      }

      if (!response || response.ok !== true) {
        const errMsg = normalizeText((response && response.error) || "Failed to start scrape");
        if (/scrape already running/i.test(errMsg)) {
          scrapeRunning = true;
          setRunningState(isBusy());
          setState("Scrape already running");
          await recoverRunningScrapeState();
          return;
        }
        throw new Error(errMsg || "Failed to start scrape");
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
    let stopRequested = false;
    await recoverRunningScrapeState();
    await recoverRunningEnrichState();

    if (enrichRunning) {
      try {
        const response = await sendRuntimeMessage({ type: MSG.STOP_ENRICH });
        if (response && response.ok === true) {
          stopRequested = true;
          setState("Stopping enrichment...");
          setLeadSignal("Stop requested", "warn");
        }
      } catch (_error) {
        // Continue and attempt scrape stop as well.
      }
    }

    const targetTabId = await resolveScrapeRunTabId();
    if (Number.isFinite(targetTabId)) {
      try {
        await ensureContentScriptReady(targetTabId);
        await sendMessageToTab(targetTabId, { type: MSG.STOP_SCRAPE });
        stopRequested = true;
        setState("Stopping...");
      } catch (_error) {
        // Ignore and report at the end if nothing was requested.
      }
    }

    if (!stopRequested) {
      setError("No active scrape/enrichment run found.");
    }
  }

  function onRuntimeMessage(message) {
    if (!message || !message.type) {
      return;
    }

    if (message.type === MSG.SCRAPE_PROGRESS) {
      scrapeRunning = true;
      applyScrapeCounters(message);
      updatePerformanceMetrics(message);
      scrapeRunTabId = Number.isFinite(Number(message.tab_id)) ? Number(message.tab_id) : scrapeRunTabId;
      setRunningState(isBusy());
      const quickSkips = Number(message.fast_skipped || 0);
      setState(runInfiniteCurrent ? `Scraping (infinite scroll)... fast-skipped ${quickSkips}` : `Scraping... fast-skipped ${quickSkips}`);
      return;
    }

    if (message.type === MSG.SCRAPE_DONE) {
      if (Number.isFinite(Number(message.tab_id))) {
        scrapeRunTabId = Number(message.tab_id);
      }
      finalizeScrapeResult(message.rows, message.summary);
      return;
    }

    if (message.type === MSG.SCRAPE_ERROR) {
      scrapeRunning = false;
      runInfiniteCurrent = false;
      setRunningState(isBusy());
      setLeadSignal("Scrape failed", "warn");
      setError(message.error || "Scrape failed");
      setState("Failed");
      return;
    }

    if (message.type === MSG.ENRICH_PROGRESS) {
      enrichRunning = true;
      setRunningState(isBusy());
      const processed = Number(message.processed || 0);
      const total = Number(message.total || 0);
      const current = normalizeText(message.current || "");
      const phase = normalizeText(message.phase || "");
      const host = shortHost(message.current_url || "");
      const siteVisited = Number(message.site_pages_visited || 0);
      const siteDiscovered = Number(message.site_pages_discovered || 0);
      const socialScanned = Number(message.social_scanned || 0);
      const personal = Number(message.personal_email_found || 0);
      const company = Number(message.company_email_found || 0);
      const discoveryEmails = Number(message.discovery_email_recovered || 0);
      const currentSuffix = current ? ` ${current}` : "";
      const phaseSuffix = phase ? ` (${phase})` : "";
      const hostSuffix = host ? ` @ ${host}` : "";
      const crawlSuffix = ` pages ${siteVisited}/${siteDiscovered}`;
      setState(`Enriching websites ${processed}/${total}${phaseSuffix}${hostSuffix}${crawlSuffix}...${currentSuffix}`);
      el.crawlVisitedStat.textContent = String(Number.isFinite(siteVisited) ? siteVisited : 0);
      el.crawlDiscoveredStat.textContent = String(Number.isFinite(siteDiscovered) ? siteDiscovered : 0);
      el.socialScannedStat.textContent = String(Number.isFinite(socialScanned) ? socialScanned : 0);
      const totalEmailsFound = Math.max(0, personal + company);
      el.emailsFoundStat.textContent = String(totalEmailsFound);
      el.discoveryEmailsFoundStat.textContent = String(Math.max(0, discoveryEmails));
      setLeadSignal(normalizeText(message.lead_signal_text), normalizeText(message.lead_signal_tone || "info"));
      return;
    }

    if (message.type === MSG.ENRICH_ERROR) {
      enrichRunning = false;
      setRunningState(isBusy());
      setLeadSignal("Enrichment failed", "warn");
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
      hasWebsite: el.hasWebsite.checked,
      hasPhone: el.hasPhone.checked,
      hasEmail: !el.requireEmailForLeads || el.requireEmailForLeads.checked !== false
    };

    return {
      maxRows: effectiveMaxRows,
      infiniteScroll,
      filters: readFilterConfig(formValues)
    };
  }

  function setRunningState(running) {
    if (el.startBtn) {
      el.startBtn.disabled = running;
    }
    if (el.stopBtn) {
      el.stopBtn.disabled = !(scrapeRunning || enrichRunning);
    }
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
    el.emailsFoundStat.textContent = "0";
    el.discoveryEmailsFoundStat.textContent = "0";
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
        SCRAPE_SESSION_KEY,
        ENRICH_SESSION_KEY,
        POPUP_UI_SETTINGS_KEY,
        "selectedColumns",
        "infiniteScrollEnabled",
        "enrichmentEnabled",
        "showEnrichmentTabsEnabled",
        "requireEmailForLeadsEnabled",
        "contactGoalEmailEnabled",
        "contactGoalPhoneEnabled"
      ]);
      if (Array.isArray(data.lastRows)) {
        lastRows = applyUnifiedOutputToRows(data.lastRows);
      }
      if (Array.isArray(data.selectedColumns)) {
        selectedColumns = normalizeSelectedColumns(data.selectedColumns);
      }
      infiniteScrollEnabled = data.infiniteScrollEnabled === true;
      enrichmentEnabled = data.enrichmentEnabled === true;
      showEnrichmentTabsEnabled = data.showEnrichmentTabsEnabled === true;
      requireEmailForLeadsEnabled = data.requireEmailForLeadsEnabled !== false;
      contactGoalEmailEnabled = data.contactGoalEmailEnabled !== false;
      contactGoalPhoneEnabled = data.contactGoalPhoneEnabled !== false;
      selectedColumns = normalizeSelectedColumnsForContactGoals(selectedColumns);
      applySavedUiSettings(data[POPUP_UI_SETTINGS_KEY]);
      applyScrapeSession(data[SCRAPE_SESSION_KEY]);
      applyEnrichSession(data[ENRICH_SESSION_KEY]);
      syncOutputModesFromSelectedColumns();
      if (el.exportBtn) {
        el.exportBtn.disabled = isBusy() || !Array.isArray(lastRows) || selectedColumns.length === 0;
      }
    } catch (_error) {
      if (el.exportBtn) {
        el.exportBtn.disabled = true;
      }
    }
  }

  function onStorageChanged(changes, areaName) {
    if (areaName !== "local" || !changes) return;

    if (changes.lastRows && Array.isArray(changes.lastRows.newValue)) {
      lastRows = applyUnifiedOutputToRows(changes.lastRows.newValue);
      applyScrapeCounters(lastScrapeSession || {}, lastRows.length);
      updatePerformanceMetrics(lastScrapeSession || {});
      setRunningState(isBusy());
    }

    if (changes[SCRAPE_SESSION_KEY]) {
      const nextScrapeSession = changes[SCRAPE_SESSION_KEY].newValue;
      if (nextScrapeSession && typeof nextScrapeSession === "object") {
        applyScrapeSession(nextScrapeSession);
      } else {
        lastScrapeSession = null;
      }
    }

    if (changes[ENRICH_SESSION_KEY]) {
      const nextEnrichSession = changes[ENRICH_SESSION_KEY].newValue;
      if (nextEnrichSession && typeof nextEnrichSession === "object") {
        applyEnrichSession(nextEnrichSession);
      } else {
        lastEnrichSession = null;
        enrichRunning = false;
        setRunningState(isBusy());
      }
    }

    if (changes[POPUP_UI_SETTINGS_KEY]) {
      applySavedUiSettings(changes[POPUP_UI_SETTINGS_KEY].newValue);
      syncContactGoalDependentUi({ persistColumns: false });
    }

    if (changes.enrichmentEnabled) {
      enrichmentEnabled = changes.enrichmentEnabled.newValue === true;
      if (el.enableEnrichment) {
        el.enableEnrichment.checked = enrichmentEnabled;
      }
      syncEnrichmentModeUi({ persist: false });
    }
    if (changes.contactGoalEmailEnabled || changes.contactGoalPhoneEnabled) {
      if (changes.contactGoalEmailEnabled) {
        contactGoalEmailEnabled = changes.contactGoalEmailEnabled.newValue !== false;
      }
      if (changes.contactGoalPhoneEnabled) {
        contactGoalPhoneEnabled = changes.contactGoalPhoneEnabled.newValue !== false;
      }
      if (el.contactGoalEmail) {
        el.contactGoalEmail.checked = contactGoalEmailEnabled;
      }
      if (el.contactGoalPhone) {
        el.contactGoalPhone.checked = contactGoalPhoneEnabled;
      }
      syncContactGoalDependentUi({ persistColumns: false });
    }
    if (changes.showEnrichmentTabsEnabled) {
      showEnrichmentTabsEnabled = changes.showEnrichmentTabsEnabled.newValue === true;
      if (el.showEnrichmentTabs && el.enableEnrichment && el.enableEnrichment.checked) {
        el.showEnrichmentTabs.checked = showEnrichmentTabsEnabled;
      }
    }
    if (changes.requireEmailForLeadsEnabled) {
      requireEmailForLeadsEnabled = changes.requireEmailForLeadsEnabled.newValue !== false;
      if (el.requireEmailForLeads) {
        el.requireEmailForLeads.checked = requireEmailForLeadsEnabled;
      }
    }
  }

  function applySavedUiSettings(settings) {
    const saved = settings && typeof settings === "object" ? settings : {};
    maxRowsValue = clampInt(saved.maxRowsValue, 1, 50000, DEFAULT_MAX_ROWS);
    contactGoalEmailEnabled = saved.contactGoalEmailEnabled != null ? saved.contactGoalEmailEnabled !== false : contactGoalEmailEnabled;
    contactGoalPhoneEnabled = saved.contactGoalPhoneEnabled != null ? saved.contactGoalPhoneEnabled !== false : contactGoalPhoneEnabled;
    emailOutputModeValue = normalizeOutputMode(saved.emailOutputMode);
    phoneOutputModeValue = normalizeOutputMode(saved.phoneOutputMode);
    if (saved.hasEmail != null) {
      requireEmailForLeadsEnabled = saved.hasEmail === true;
    } else if (saved.requireEmailForLeads != null) {
      requireEmailForLeadsEnabled = saved.requireEmailForLeads !== false;
    }
    showAdvancedFields = saved.showAdvancedFields === true;

    el.maxRows.value = String(maxRowsValue);
    el.minRating.value = sanitizeFormString(saved.minRating);
    el.maxRating.value = sanitizeFormString(saved.maxRating);
    el.minReviews.value = sanitizeFormString(saved.minReviews);
    el.maxReviews.value = sanitizeFormString(saved.maxReviews);
    el.hasWebsite.checked = saved.hasWebsite === true;
    el.hasPhone.checked = saved.hasPhone === true;
    if (el.contactGoalEmail) {
      el.contactGoalEmail.checked = contactGoalEmailEnabled;
    }
    if (el.contactGoalPhone) {
      el.contactGoalPhone.checked = contactGoalPhoneEnabled;
    }
    if (el.emailOutputMode) {
      el.emailOutputMode.value = emailOutputModeValue;
    }
    if (el.phoneOutputMode) {
      el.phoneOutputMode.value = phoneOutputModeValue;
    }
    if (el.requireEmailForLeads) {
      el.requireEmailForLeads.checked = requireEmailForLeadsEnabled;
    }
    if (!showAdvancedFields && Array.isArray(selectedColumns)) {
      selectedColumns = normalizeSelectedColumnsForContactGoals(selectedColumns.filter((column) => !ADVANCED_COLUMNS.has(column)));
    }
    selectedColumns = normalizeSelectedColumnsForContactGoals(selectedColumns);
    updateAdvancedToggleLabel();
    if (Array.isArray(lastRows) && lastRows.length > 0) {
      lastRows = applyUnifiedOutputToRows(lastRows);
    }
    if (el.columnList && el.columnList.childElementCount === 0) {
      renderColumnSelector();
      updateColumnsTitle();
    }
  }

  function applyScrapeSession(session) {
    if (!session || typeof session !== "object") return;
    lastScrapeSession = session;

    scrapeRunTabId = Number.isFinite(Number(session.tab_id)) ? Number(session.tab_id) : null;

    const status = normalizeText(session.status).toLowerCase();
    runInfiniteCurrent = session.infinite_scroll === true;
    scrapeRunning = status === "running" || status === "stopping";

    applyScrapeCounters(session, Array.isArray(lastRows) ? lastRows.length : 0);
    updatePerformanceMetrics(session);

    if (status === "running") {
      const quickSkips = Number(session.fast_skipped || 0);
      setState(runInfiniteCurrent ? `Scraping (infinite scroll)... fast-skipped ${quickSkips}` : `Scraping... fast-skipped ${quickSkips}`);
    } else if (status === "stopping") {
      setState("Stopping...");
      setLeadSignal("Stop requested", "warn");
    } else if (status === "done" || status === "stopped") {
      const rowsCount = Number(session.rows_count || (Array.isArray(lastRows) ? lastRows.length : 0));
      const duplicates = Number(session.duplicates || 0);
      const fastSkipped = Number(session.fast_skipped || 0);
      setState(`${status === "stopped" ? "Stopped" : "Completed"}: ${rowsCount} unique row(s), ${duplicates} duplicates, ${fastSkipped} fast-skipped`);
      setLeadSignal(status === "stopped" ? "Scrape stopped by user" : "Scrape finished and saved", status === "stopped" ? "warn" : "success");
    } else if (status === "error") {
      setError(normalizeText(session.error) || "Scrape failed");
      setState("Failed");
      setLeadSignal("Scrape failed", "warn");
    }

    setRunningState(isBusy());
  }

  function applyEnrichSession(session) {
    if (!session || typeof session !== "object") return;
    lastEnrichSession = session;

    const status = normalizeText(session.status).toLowerCase();
    enrichRunning = status === "queued" || status === "running" || status === "stopping";

    const siteVisited = Number(session.site_pages_visited != null ? session.site_pages_visited : session.pages_visited || 0);
    const siteDiscovered = Number(session.site_pages_discovered != null ? session.site_pages_discovered : session.pages_discovered || 0);
    const socialScanned = Number(session.social_scanned || 0);
    const personal = Number(session.personal_email_found || 0);
    const company = Number(session.company_email_found || 0);
    const discoveryEmails = Number(session.discovery_email_recovered || 0);
    el.crawlVisitedStat.textContent = String(Number.isFinite(siteVisited) ? siteVisited : 0);
    el.crawlDiscoveredStat.textContent = String(Number.isFinite(siteDiscovered) ? siteDiscovered : 0);
    el.socialScannedStat.textContent = String(Number.isFinite(socialScanned) ? socialScanned : 0);
    el.emailsFoundStat.textContent = String(Math.max(0, personal + company));
    el.discoveryEmailsFoundStat.textContent = String(Math.max(0, discoveryEmails));

    const signalText = normalizeText(session.lead_signal_text);
    const signalTone = normalizeText(session.lead_signal_tone || "info");
    if (signalText) {
      setLeadSignal(signalText, signalTone);
    }

    if (status === "queued") {
      const total = Number(session.total || 0);
      setState(`Website enrichment queued${total > 0 ? ` (${total} site${total === 1 ? "" : "s"})` : ""}...`);
      setLeadSignal("Website enrichment queued", "info");
    } else if (status === "running") {
      const processed = Number(session.processed || 0);
      const total = Number(session.total || 0);
      const phase = normalizeText(session.phase || "");
      const host = shortHost(session.current_url || "");
      const current = normalizeText(session.current || "");
      const phaseSuffix = phase ? ` (${phase})` : "";
      const hostSuffix = host ? ` @ ${host}` : "";
      const currentSuffix = current ? ` ${current}` : "";
      setState(`Enriching websites ${processed}/${total}${phaseSuffix}${hostSuffix} pages ${siteVisited}/${siteDiscovered}...${currentSuffix}`);
    } else if (status === "stopping") {
      setState("Stopping enrichment...");
      setLeadSignal("Stop requested", "warn");
    } else if (status === "done") {
      const enriched = Number(session.enriched || 0);
      const skipped = Number(session.skipped || 0);
      const blocked = Number(session.blocked || 0);
      setState(`Enrichment: ${enriched} enriched, ${skipped} skipped, ${blocked} blocked, pages ${siteVisited}/${siteDiscovered}`);
      if (personal > 0 || company > 0) {
        setLeadSignal(
          `Saved emails: ${Math.max(0, personal + company)}, discovery ${Math.max(0, discoveryEmails)}`,
          "success"
        );
      } else {
        setLeadSignal("No public emails found during enrichment", "warn");
      }
    } else if (status === "stopped") {
      const processed = Number(session.processed || 0);
      const total = Number(session.total || 0);
      setState(`Enrichment stopped: ${processed}/${total}, pages ${siteVisited}/${siteDiscovered}`);
      setLeadSignal("Enrichment stopped by user", "warn");
    } else if (status === "error") {
      setError(normalizeText(session.error) || "Website enrichment failed");
      setState("Enrichment failed");
      setLeadSignal("Enrichment failed", "warn");
    }

    setRunningState(isBusy());
  }

  async function recoverRunningScrapeState() {
    const tabIds = await collectCandidateScrapeTabs();
    for (const tabId of tabIds) {
      const runtime = await requestScrapeState(tabId);
      if (!runtime || runtime.is_running !== true) continue;

      scrapeRunTabId = tabId;
      scrapeRunning = true;
      runInfiniteCurrent = runtime.infinite_scroll === true;
      applyScrapeCounters(runtime, Array.isArray(lastRows) ? lastRows.length : 0);
      updatePerformanceMetrics(runtime);

      const quickSkips = Number(runtime.fast_skipped || 0);
      setState(runInfiniteCurrent ? `Scraping (infinite scroll)... fast-skipped ${quickSkips}` : `Scraping... fast-skipped ${quickSkips}`);
      setLeadSignal("Scrape is still running", "info");
      setRunningState(isBusy());

      storageSet({
        [SCRAPE_SESSION_KEY]: {
          run_id: normalizeText(runtime.run_id),
          tab_id: tabId,
          status: normalizeText(runtime.status || "running") || "running",
          processed: Number(runtime.processed || 0),
          matched: Number(runtime.matched || 0),
          duplicates: Number(runtime.duplicates || 0),
          fast_skipped: Number(runtime.fast_skipped || 0),
          errors: Number(runtime.errors || 0),
          seen_listings: Number(runtime.seen_listings || 0),
          rate_per_sec: Number(runtime.rate_per_sec || 0),
          avg_rating_seen: runtime.avg_rating_seen,
          avg_reviews_seen: runtime.avg_reviews_seen,
          rows_count: Number(runtime.rows_count || 0),
          source_query: normalizeText(runtime.source_query),
          source_url: normalizeText(runtime.source_url),
          infinite_scroll: runtime.infinite_scroll === true,
          updated_at: new Date().toISOString()
        }
      }).catch(() => {});
      return true;
    }

    const staleRunning = scrapeRunning === true;
    if (staleRunning) {
      scrapeRunning = false;
      scrapeRunTabId = null;
      const staleStatus = normalizeText(lastScrapeSession && lastScrapeSession.status).toLowerCase();
      if (staleStatus === "running" || staleStatus === "stopping") {
        const patchedSession = {
          ...(lastScrapeSession || {}),
          status: "stopped",
          updated_at: new Date().toISOString()
        };
        lastScrapeSession = patchedSession;
        storageSet({
          [SCRAPE_SESSION_KEY]: patchedSession
        }).catch(() => {});
      }
      setRunningState(isBusy());
    }
    return false;
  }

  async function recoverRunningEnrichState() {
    let runtimeState = null;
    try {
      const response = await sendRuntimeMessage({ type: MSG.GET_ENRICH_STATE });
      if (response && response.ok === true && response.state && response.state.is_running === true) {
        runtimeState = response.state;
      }
    } catch (_error) {
      runtimeState = null;
    }

    if (runtimeState) {
      enrichRunning = true;
      setRunningState(isBusy());
      const currentStateText = normalizeText(el.stateText && el.stateText.textContent);
      if (!currentStateText || /^idle$/i.test(currentStateText)) {
        setState("Enriching websites...");
      }
      return true;
    }

    const staleStatus = normalizeText(lastEnrichSession && lastEnrichSession.status).toLowerCase();
    if (staleStatus === "running" || staleStatus === "stopping") {
      const patchedSession = {
        ...(lastEnrichSession || {}),
        status: "stopped",
        phase: "stopped",
        updated_at: new Date().toISOString(),
        lead_signal_text: "Enrichment session reset",
        lead_signal_tone: "warn"
      };
      lastEnrichSession = patchedSession;
      enrichRunning = false;
      storageSet({
        [ENRICH_SESSION_KEY]: patchedSession
      }).catch(() => {});
      if (!scrapeRunning) {
        setState("Idle");
      }
      setRunningState(isBusy());
      return false;
    }

    enrichRunning = false;
    setRunningState(isBusy());
    return false;
  }

  async function collectCandidateScrapeTabs() {
    const out = [];
    const seen = new Set();

    if (Number.isFinite(Number(scrapeRunTabId))) {
      const id = Number(scrapeRunTabId);
      seen.add(id);
      out.push(id);
    }

    try {
      const data = await storageGet([SCRAPE_SESSION_KEY]);
      const session = data[SCRAPE_SESSION_KEY];
      if (session && Number.isFinite(Number(session.tab_id))) {
        const id = Number(session.tab_id);
        if (!seen.has(id)) {
          seen.add(id);
          out.push(id);
        }
      }
    } catch (_error) {
      // Ignore storage probe errors.
    }

    const tabs = await queryAllTabs();
    for (const tab of tabs) {
      if (!tab || !tab.id || !isMapsUrl(tab.url)) continue;
      if (seen.has(tab.id)) continue;
      seen.add(tab.id);
      out.push(tab.id);
    }

    return out;
  }

  async function requestScrapeState(tabId) {
    try {
      const response = await sendMessageToTab(tabId, { type: MSG.GET_SCRAPE_STATE });
      if (!response || response.ok !== true || !response.state) return null;
      return response.state;
    } catch (_error) {
      return null;
    }
  }

  function persistRows(rows) {
    const normalizedRows = applyUnifiedOutputToRows(rows);
    lastRows = normalizedRows;
    storageSet({ lastRows: normalizedRows }).catch(() => {});
    setRunningState(isBusy());
  }

  function finalizeScrapeResult(rowsInput, summaryInput) {
    scrapeRunning = false;
    runInfiniteCurrent = false;
    scrapeRunTabId = null;
    setRunningState(isBusy());

    const incomingRows = Array.isArray(rowsInput) ? rowsInput : [];
    const summary = summaryInput || {};
    const activeFilters = readFilterConfig({
      minRating: el.minRating.value,
      maxRating: el.maxRating.value,
      minReviews: el.minReviews.value,
      maxReviews: el.maxReviews.value,
      hasWebsite: el.hasWebsite.checked,
      hasPhone: el.hasPhone.checked,
      hasEmail: !el.requireEmailForLeads || el.requireEmailForLeads.checked !== false
    });
    const scrapeStageFilters = toScrapeStageFilters(activeFilters);
    const rows = hasAnyActiveFilter(scrapeStageFilters)
      ? incomingRows.filter((row) => applyFilters(row, scrapeStageFilters))
      : incomingRows;

    persistRows(rows);

    applyScrapeCounters(summary, rows.length);
    updatePerformanceMetrics(summary);

    const status = summary.stopped ? "Stopped" : "Completed";
    const fastSkipped = Number(summary.fast_skipped || 0);
    setState(`${status}: ${rows.length} unique row(s), ${summary.duplicates || 0} duplicates, ${fastSkipped} fast-skipped`);
    setLeadSignal(summary.stopped ? "Scrape stopped by user" : "Scrape finished and saved", summary.stopped ? "warn" : "success");

    if (rows.length === 0 && Number(summary.processed || 0) > 0) {
      setError("No rows extracted. Try disabling strict filters or run again with fewer active constraints.");
    }

    if (!summary.stopped && el.enableEnrichment.checked && rows.length > 0) {
      setLeadSignal("Website enrichment queued in background", "info");
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

  async function getActiveTab() {
    const inIncognitoContext = isCurrentContextIncognito();
    const [anchorWindowId, currentWindowId] = await Promise.all([
      getControlPanelAnchorWindowId(),
      getCurrentWindowId()
    ]);

    if (Number.isFinite(anchorWindowId)) {
      const anchoredTab = await queryActiveTabForWindow(anchorWindowId);
      if (anchoredTab && !isExtensionTab(anchoredTab) && tabMatchesContext(anchoredTab, inIncognitoContext)) {
        return anchoredTab;
      }
    }

    const activeTabs = await queryActiveTabs();
    const preferredTab = activeTabs.find((tab) => {
      if (!tab || !tab.id || isExtensionTab(tab)) return false;
      if (!tabMatchesContext(tab, inIncognitoContext)) return false;
      if (!Number.isFinite(currentWindowId)) return true;
      return Number(tab.windowId) !== Number(currentWindowId);
    });
    if (preferredTab) {
      return preferredTab;
    }

    const contextFallback = activeTabs.find((tab) => tab && tab.id && !isExtensionTab(tab) && tabMatchesContext(tab, inIncognitoContext));
    if (contextFallback) {
      return contextFallback;
    }

    const fallbackTab = activeTabs.find((tab) => tab && tab.id && !isExtensionTab(tab));
    return fallbackTab || null;
  }

  function queryActiveTabForWindow(windowId) {
    return new Promise((resolve) => {
      chrome.tabs.query({ active: true, windowId }, (tabs) => {
        resolve(tabs && tabs[0] ? tabs[0] : null);
      });
    });
  }

  function queryActiveTabs() {
    return new Promise((resolve) => {
      chrome.tabs.query({ active: true }, (tabs) => {
        resolve(Array.isArray(tabs) ? tabs : []);
      });
    });
  }

  function getCurrentWindowId() {
    return new Promise((resolve) => {
      chrome.windows.getCurrent({}, (windowRef) => {
        if (chrome.runtime.lastError || !windowRef) {
          resolve(null);
          return;
        }
        const id = Number(windowRef.id);
        resolve(Number.isFinite(id) ? id : null);
      });
    });
  }

  async function getControlPanelAnchorWindowId() {
    try {
      const data = await storageGet([CONTROL_PANEL_ANCHOR_WINDOW_KEY]);
      const id = Number(data[CONTROL_PANEL_ANCHOR_WINDOW_KEY]);
      return Number.isFinite(id) && id >= 0 ? id : null;
    } catch (_error) {
      return null;
    }
  }

  function isExtensionTab(tab) {
    const url = normalizeText(tab && tab.url);
    return url.startsWith(EXTENSION_PAGE_PREFIX);
  }

  function isCurrentContextIncognito() {
    try {
      return chrome.extension && chrome.extension.inIncognitoContext === true;
    } catch (_error) {
      return false;
    }
  }

  function tabMatchesContext(tab, inIncognitoContext) {
    if (!tab || typeof tab !== "object") return false;
    if (typeof tab.incognito !== "boolean") return true;
    return tab.incognito === inIncognitoContext;
  }

  function queryAllTabs() {
    return new Promise((resolve) => {
      chrome.tabs.query({}, (tabs) => {
        resolve(Array.isArray(tabs) ? tabs : []);
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

  function hasAnyActiveFilter(filters) {
    const f = filters || {};
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

  function toScrapeStageFilters(filters) {
    const source = filters && typeof filters === "object" ? filters : {};
    return {
      ...source,
      hasEmail: false
    };
  }

  function normalizeOutputMode(value) {
    const normalized = normalizeText(value).toLowerCase();
    if (normalized === "unified_only" || normalized === "unified_plus_raw" || normalized === "raw_only") {
      return normalized;
    }
    return "unified_only";
  }

  function getDisabledGoalColumnsSet() {
    const blocked = new Set();
    if (!contactGoalEmailEnabled) {
      for (const column of EMAIL_GOAL_COLUMNS) {
        blocked.add(column);
      }
    }
    if (!contactGoalPhoneEnabled) {
      for (const column of PHONE_GOAL_COLUMNS) {
        blocked.add(column);
      }
    }
    return blocked;
  }

  function filterColumnsByContactGoals(columnsInput) {
    if (!Array.isArray(columnsInput)) return [];
    const blocked = getDisabledGoalColumnsSet();
    if (blocked.size === 0) return [...columnsInput];
    return columnsInput.filter((column) => !blocked.has(column));
  }

  function selectedColumnsFallbackForGoals() {
    const filteredDefaults = filterColumnsByContactGoals(DEFAULT_SELECTED_COLUMNS);
    if (filteredDefaults.length > 0) {
      return filteredDefaults;
    }
    return [...CORE_COLUMNS];
  }

  function normalizeSelectedColumnsForContactGoals(columnsInput) {
    const normalized = normalizeSelectedColumns(columnsInput, {
      fallbackColumns: selectedColumnsFallbackForGoals()
    });
    const filtered = filterColumnsByContactGoals(normalized);
    if (filtered.length > 0) {
      return filtered;
    }
    return selectedColumnsFallbackForGoals();
  }

  function syncContactGoalDependentUi(optionsInput) {
    const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
    const persistColumns = options.persistColumns === true;

    if (el.emailOutputGroup) {
      el.emailOutputGroup.hidden = !contactGoalEmailEnabled;
    }
    if (el.emailOutputMode) {
      el.emailOutputMode.disabled = !contactGoalEmailEnabled;
    }
    if (el.emailColumnsHint) {
      el.emailColumnsHint.hidden = !contactGoalEmailEnabled;
    }

    if (el.phoneOutputGroup) {
      el.phoneOutputGroup.hidden = !contactGoalPhoneEnabled;
    }
    if (el.phoneOutputMode) {
      el.phoneOutputMode.disabled = !contactGoalPhoneEnabled;
    }
    if (el.phoneColumnsHint) {
      el.phoneColumnsHint.hidden = !contactGoalPhoneEnabled;
    }

    const before = selectedColumns.join("|");
    selectedColumns = normalizeSelectedColumnsForContactGoals(selectedColumns);
    syncOutputModesFromSelectedColumns();

    renderColumnSelector();
    updateColumnsTitle();
    updateContactGoalsHint();
    updateEmailColumnsHint();
    updatePhoneColumnsHint();

    if (persistColumns && before !== selectedColumns.join("|")) {
      persistSelectedColumns();
    }
  }

  function updateContactGoalsHint() {
    if (!el.contactGoalsHint) return;
    const wantsEmail = contactGoalEmailEnabled === true;
    const wantsPhone = contactGoalPhoneEnabled === true;
    if (wantsEmail && wantsPhone) {
      el.contactGoalsHint.textContent = "Goals: collect emails and phone numbers.";
      return;
    }
    if (wantsEmail) {
      el.contactGoalsHint.textContent = "Goal: collect emails only.";
      return;
    }
    if (wantsPhone) {
      el.contactGoalsHint.textContent = "Goal: collect phone numbers only.";
      return;
    }
    el.contactGoalsHint.textContent = "Goals: none. Contact enrichment will skip email and phone collection.";
  }

  function applyUnifiedOutputToRows(rows) {
    const withEmail = applyUnifiedEmailToRows(rows);
    return applyUnifiedPhoneToRows(withEmail);
  }

  function applyUnifiedEmailToRows(rows) {
    if (!Array.isArray(rows)) return [];
    return rows.map((row) => {
      const source = row || {};
      const unifiedEmail = resolveUnifiedEmail(source);
      if (normalizeText(source.email) === unifiedEmail) {
        return source;
      }
      return {
        ...source,
        email: unifiedEmail
      };
    });
  }

  function applyUnifiedPhoneToRows(rows) {
    if (!Array.isArray(rows)) return [];
    return rows.map((row) => {
      const source = row || {};
      const listingPhone = sanitizePhoneText(normalizeText(source.listing_phone || source.phone));
      const websitePhone = sanitizePhoneText(normalizeText(source.website_phone));
      const websitePhoneSource = normalizeText(source.website_phone_source);
      const unifiedPhone = resolveUnifiedPhone(source);
      if (
        sanitizePhoneText(source.phone) === unifiedPhone &&
        sanitizePhoneText(source.listing_phone) === listingPhone &&
        sanitizePhoneText(source.website_phone) === websitePhone &&
        normalizeText(source.website_phone_source) === websitePhoneSource
      ) {
        return source;
      }
      return {
        ...source,
        phone: unifiedPhone,
        listing_phone: listingPhone,
        website_phone: websitePhone,
        website_phone_source: websitePhoneSource
      };
    });
  }

  function resolveUnifiedEmail(row) {
    const value = row || {};
    const ownerEmail = normalizeText(value.owner_email);
    const primaryEmail = normalizeText(value.primary_email);
    const contactEmail = normalizeText(value.contact_email);
    const precedence = [primaryEmail, contactEmail, ownerEmail];
    return precedence.find((email) => email !== "") || "";
  }

  function resolveUnifiedPhone(row) {
    const value = row || {};
    const listingPhone = sanitizePhoneText(normalizeText(value.listing_phone || value.phone));
    const websitePhone = sanitizePhoneText(normalizeText(value.website_phone));
    const precedence = [listingPhone, websitePhone];
    return precedence.find((phone) => phone !== "") || "";
  }

  function sanitizePhoneText(value) {
    return normalizePhoneText(value);
  }

  function sanitizeFormString(value) {
    return normalizeText(value);
  }

  function onUiSettingsInputChanged() {
    maxRowsValue = clampInt(el.maxRows.value, 1, 50000, DEFAULT_MAX_ROWS);
    schedulePersistUiSettings();
  }

  function schedulePersistUiSettings() {
    if (uiSettingsSaveTimer) {
      clearTimeout(uiSettingsSaveTimer);
    }

    uiSettingsSaveTimer = setTimeout(() => {
      uiSettingsSaveTimer = null;
      persistUiSettings();
    }, 150);
  }

  function persistUiSettings() {
    maxRowsValue = clampInt(el.maxRows.value, 1, 50000, DEFAULT_MAX_ROWS);
    contactGoalEmailEnabled = el.contactGoalEmail ? el.contactGoalEmail.checked === true : true;
    contactGoalPhoneEnabled = el.contactGoalPhone ? el.contactGoalPhone.checked === true : true;
    emailOutputModeValue = normalizeOutputMode(el.emailOutputMode && el.emailOutputMode.value);
    phoneOutputModeValue = normalizeOutputMode(el.phoneOutputMode && el.phoneOutputMode.value);
    const uiSettings = {
      maxRowsValue,
      contactGoalEmailEnabled,
      contactGoalPhoneEnabled,
      emailOutputMode: emailOutputModeValue,
      phoneOutputMode: phoneOutputModeValue,
      hasEmail: requireEmailForLeadsEnabled === true,
      requireEmailForLeads: requireEmailForLeadsEnabled,
      showAdvancedFields: showAdvancedFields === true,
      minRating: sanitizeFormString(el.minRating.value),
      maxRating: sanitizeFormString(el.maxRating.value),
      minReviews: sanitizeFormString(el.minReviews.value),
      maxReviews: sanitizeFormString(el.maxReviews.value),
      hasWebsite: el.hasWebsite.checked === true,
      hasPhone: el.hasPhone.checked === true
    };

    storageSet({
      [POPUP_UI_SETTINGS_KEY]: uiSettings
    }).catch(() => {});
  }

  function onInfiniteScrollToggle() {
    infiniteScrollEnabled = el.infiniteScroll.checked;
    syncInfiniteScrollInput();
    storageSet({ infiniteScrollEnabled }).catch(() => {});
    schedulePersistUiSettings();
  }

  function onEnrichmentToggle() {
    enrichmentEnabled = el.enableEnrichment.checked;
    storageSet({ enrichmentEnabled }).catch(() => {});
    syncEnrichmentModeUi({ persist: true });
    schedulePersistUiSettings();
  }

  function onContactGoalsChange() {
    const wasEmailEnabled = contactGoalEmailEnabled === true;
    const wasPhoneEnabled = contactGoalPhoneEnabled === true;
    contactGoalEmailEnabled = el.contactGoalEmail && el.contactGoalEmail.checked === true;
    contactGoalPhoneEnabled = el.contactGoalPhone && el.contactGoalPhone.checked === true;
    if (!wasEmailEnabled && contactGoalEmailEnabled) {
      applyEmailModeToSelection();
    }
    if (!wasPhoneEnabled && contactGoalPhoneEnabled) {
      applyPhoneModeToSelection();
    }
    syncContactGoalDependentUi({ persistColumns: true });
    storageSet({
      contactGoalEmailEnabled,
      contactGoalPhoneEnabled
    }).catch(() => {});
    schedulePersistUiSettings();
  }

  function onEmailOutputModeChange() {
    emailOutputModeValue = normalizeOutputMode(el.emailOutputMode.value);
    el.emailOutputMode.value = emailOutputModeValue;
    applyEmailModeToSelection();
    renderColumnSelector();
    updateColumnsTitle();
    updateEmailColumnsHint();
    persistSelectedColumns();
    schedulePersistUiSettings();
    setRunningState(isBusy());
    if (Array.isArray(lastRows) && lastRows.length > 0) {
      persistRows(lastRows);
    }
  }

  function onPhoneOutputModeChange() {
    phoneOutputModeValue = normalizeOutputMode(el.phoneOutputMode.value);
    el.phoneOutputMode.value = phoneOutputModeValue;
    applyPhoneModeToSelection();
    renderColumnSelector();
    updateColumnsTitle();
    updatePhoneColumnsHint();
    persistSelectedColumns();
    schedulePersistUiSettings();
    setRunningState(isBusy());
    if (Array.isArray(lastRows) && lastRows.length > 0) {
      persistRows(lastRows);
    }
  }

  function onShowEnrichmentTabsToggle() {
    if (!el.enableEnrichment.checked) {
      showEnrichmentTabsEnabled = false;
      el.showEnrichmentTabs.checked = false;
      storageSet({ showEnrichmentTabsEnabled: false }).catch(() => {});
      schedulePersistUiSettings();
      return;
    }
    showEnrichmentTabsEnabled = el.showEnrichmentTabs.checked;
    storageSet({ showEnrichmentTabsEnabled }).catch(() => {});
    schedulePersistUiSettings();
  }

  function onRequireEmailForLeadsToggle() {
    requireEmailForLeadsEnabled = !el.requireEmailForLeads || el.requireEmailForLeads.checked !== false;
    storageSet({ requireEmailForLeadsEnabled }).catch(() => {});
    schedulePersistUiSettings();
  }

  function onToggleAdvancedFields() {
    showAdvancedFields = !showAdvancedFields;
    if (!showAdvancedFields) {
      const filtered = selectedColumns.filter((column) => !ADVANCED_COLUMNS.has(column));
      selectedColumns = normalizeSelectedColumnsForContactGoals(filtered);
      syncOutputModesFromSelectedColumns();
      updateEmailColumnsHint();
      updatePhoneColumnsHint();
      persistSelectedColumns();
    }
    updateAdvancedToggleLabel();
    renderColumnSelector();
    updateColumnsTitle();
    schedulePersistUiSettings();
  }

  function shouldEnableLeadDiscoveryForSelection() {
    const selected = new Set(selectedColumns);
    return selected.has("owner_name") || selected.has("owner_title") || selected.has("owner_email");
  }

  async function persistRunSettingsForBackground() {
    enrichmentEnabled = el.enableEnrichment.checked === true;
    showEnrichmentTabsEnabled = enrichmentEnabled && el.showEnrichmentTabs.checked === true;
    requireEmailForLeadsEnabled = !el.requireEmailForLeads || el.requireEmailForLeads.checked !== false;
    const leadDiscoveryEnabled = shouldEnableLeadDiscoveryForSelection();
    contactGoalEmailEnabled = el.contactGoalEmail && el.contactGoalEmail.checked === true;
    contactGoalPhoneEnabled = el.contactGoalPhone && el.contactGoalPhone.checked === true;

    const activeScrapeFilters = readFilterConfig({
      minRating: el.minRating.value,
      maxRating: el.maxRating.value,
      minReviews: el.minReviews.value,
      maxReviews: el.maxReviews.value,
      hasWebsite: el.hasWebsite.checked,
      hasPhone: el.hasPhone.checked,
      hasEmail: requireEmailForLeadsEnabled
    });

    await storageSet({
      enrichmentEnabled,
      showEnrichmentTabsEnabled,
      requireEmailForLeadsEnabled,
      leadDiscoveryEnabled,
      contactGoalEmailEnabled,
      contactGoalPhoneEnabled,
      [ACTIVE_SCRAPE_FILTERS_KEY]: activeScrapeFilters
    }).catch(() => {});
  }

  function syncEnrichmentModeUi(optionsInput) {
    const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
    const persist = options.persist === true;
    const enabled = el.enableEnrichment.checked === true;

    if (el.showEnrichmentTabsRow) {
      el.showEnrichmentTabsRow.classList.toggle("is-hidden", !enabled);
    }
    if (el.showEnrichmentTabs) {
      el.showEnrichmentTabs.disabled = !enabled;
    }

    if (!enabled) {
      showEnrichmentTabsEnabled = false;
      if (el.showEnrichmentTabs) {
        el.showEnrichmentTabs.checked = false;
      }
      if (persist) {
        storageSet({ showEnrichmentTabsEnabled: false }).catch(() => {});
      }
      return;
    }

    if (el.showEnrichmentTabs) {
      el.showEnrichmentTabs.checked = showEnrichmentTabsEnabled === true;
    }
  }

  function syncInfiniteScrollInput() {
    if (!el.maxRows) return;
    const enabled = el.infiniteScroll.checked;
    el.maxRows.disabled = enabled;
    if (enabled) {
      el.maxRows.title = "Disabled while infinite scroll is enabled";
    } else {
      el.maxRows.title = "";
    }
  }

  function ensureColumnSelectorRendered() {
    if (!el.columnList) return;
    if (el.columnList.childElementCount > 0) return;
    setTimeout(() => {
      if (!el.columnList || el.columnList.childElementCount > 0) return;
      renderColumnSelector();
      updateColumnsTitle();
    }, 0);
  }

  function updateAdvancedToggleLabel() {
    if (!el.toggleAdvancedBtn) return;
    el.toggleAdvancedBtn.textContent = showAdvancedFields ? "Advanced On" : "Advanced Off";
    el.toggleAdvancedBtn.title = showAdvancedFields
      ? "Hide metadata fields"
      : "Show metadata fields";
  }

  function renderColumnSelector() {
    el.columnList.textContent = "";
    const selectedSet = new Set(selectedColumns);
    const groupedColumns = new Set();
    const disabledGoalColumns = getDisabledGoalColumnsSet();

    for (const group of COLUMN_GROUPS) {
      if ((group.id === "email" && !contactGoalEmailEnabled) || (group.id === "phone" && !contactGoalPhoneEnabled)) {
        for (const column of group.columns) {
          groupedColumns.add(column);
        }
        continue;
      }
      if (group.advanced === true && !showAdvancedFields) {
        for (const column of group.columns) {
          groupedColumns.add(column);
        }
        continue;
      }

      const section = document.createElement("section");
      section.className = "column-group";

      const head = document.createElement("div");
      head.className = "column-group-head";
      const title = document.createElement("strong");
      title.textContent = group.title;
      const description = document.createElement("span");
      description.textContent = group.description;
      head.appendChild(title);
      head.appendChild(description);
      section.appendChild(head);

      const grid = document.createElement("div");
      grid.className = "column-grid";

      for (const column of group.columns) {
        groupedColumns.add(column);
        if (!CSV_COLUMNS.includes(column)) continue;
        if (disabledGoalColumns.has(column)) continue;
        if (!showAdvancedFields && ADVANCED_COLUMNS.has(column)) continue;
        grid.appendChild(buildColumnCheckbox(column, selectedSet));
      }

      section.appendChild(grid);
      el.columnList.appendChild(section);
    }

    const extras = CSV_COLUMNS.filter((column) => {
      if (groupedColumns.has(column)) return false;
      if (disabledGoalColumns.has(column)) return false;
      if (!showAdvancedFields && ADVANCED_COLUMNS.has(column)) return false;
      return true;
    });
    if (extras.length > 0) {
      const section = document.createElement("section");
      section.className = "column-group";

      const head = document.createElement("div");
      head.className = "column-group-head";
      const title = document.createElement("strong");
      title.textContent = "Other Fields";
      const description = document.createElement("span");
      description.textContent = "Additional columns";
      head.appendChild(title);
      head.appendChild(description);
      section.appendChild(head);

      const grid = document.createElement("div");
      grid.className = "column-grid";
      for (const column of extras) {
        grid.appendChild(buildColumnCheckbox(column, selectedSet));
      }
      section.appendChild(grid);
      el.columnList.appendChild(section);
    }
  }

  function buildColumnCheckbox(column, selectedSet) {
    const label = document.createElement("label");
    label.className = "checkbox column-checkbox";

    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = column;
    input.checked = selectedSet.has(column);
    input.addEventListener("change", onColumnSelectionChange);

    const textWrap = document.createElement("span");
    textWrap.className = "column-label";
    const name = document.createElement("span");
    name.textContent = COLUMN_LABELS[column] || column;
    textWrap.appendChild(name);

    const badge = COLUMN_BADGES[column];
    if (badge && badge.text) {
      const pill = document.createElement("span");
      pill.className = `column-pill${badge.tone === "raw" ? " raw" : badge.tone === "meta" ? " meta" : ""}`;
      pill.textContent = badge.text;
      textWrap.appendChild(pill);
    }

    label.appendChild(input);
    label.appendChild(textWrap);
    return label;
  }

  function applyEmailModeToSelection() {
    const selected = new Set(selectedColumns);
    if (!contactGoalEmailEnabled) {
      for (const column of EMAIL_GOAL_COLUMNS) {
        selected.delete(column);
      }
      selectedColumns = normalizeSelectedColumnsForContactGoals(Array.from(selected));
      return;
    }
    const mode = normalizeOutputMode(emailOutputModeValue);
    if (mode === "unified_only") {
      selected.add("email");
      for (const column of RAW_EMAIL_COLUMNS) {
        selected.delete(column);
      }
    } else if (mode === "unified_plus_raw") {
      for (const column of EMAIL_COLUMNS) {
        selected.add(column);
      }
    } else if (mode === "raw_only") {
      selected.delete("email");
      for (const column of RAW_EMAIL_COLUMNS) {
        selected.add(column);
      }
    }
    selectedColumns = normalizeSelectedColumnsForContactGoals(Array.from(selected));
  }

  function applyPhoneModeToSelection() {
    const selected = new Set(selectedColumns);
    if (!contactGoalPhoneEnabled) {
      for (const column of PHONE_GOAL_COLUMNS) {
        selected.delete(column);
      }
      selectedColumns = normalizeSelectedColumnsForContactGoals(Array.from(selected));
      return;
    }
    const mode = normalizeOutputMode(phoneOutputModeValue);
    if (mode === "unified_only") {
      selected.add("phone");
      for (const column of RAW_PHONE_COLUMNS) {
        selected.delete(column);
      }
    } else if (mode === "unified_plus_raw") {
      for (const column of PHONE_COLUMNS) {
        selected.add(column);
      }
    } else if (mode === "raw_only") {
      selected.delete("phone");
      for (const column of RAW_PHONE_COLUMNS) {
        selected.add(column);
      }
    }
    selectedColumns = normalizeSelectedColumnsForContactGoals(Array.from(selected));
  }

  function inferOutputModeFromColumns(selectedSet, unifiedColumn, rawColumns) {
    const hasUnified = selectedSet.has(unifiedColumn);
    const hasRaw = rawColumns.some((column) => selectedSet.has(column));
    if (hasUnified && !hasRaw) return "unified_only";
    if (hasUnified && hasRaw) return "unified_plus_raw";
    if (!hasUnified && hasRaw) return "raw_only";
    return "unified_only";
  }

  function syncOutputModesFromSelectedColumns() {
    const selectedSet = new Set(selectedColumns);
    emailOutputModeValue = inferOutputModeFromColumns(selectedSet, "email", RAW_EMAIL_COLUMNS);
    phoneOutputModeValue = inferOutputModeFromColumns(selectedSet, "phone", RAW_PHONE_COLUMNS);
    if (el.emailOutputMode) {
      el.emailOutputMode.value = emailOutputModeValue;
    }
    if (el.phoneOutputMode) {
      el.phoneOutputMode.value = phoneOutputModeValue;
    }
  }

  function updateEmailColumnsHint() {
    if (!el.emailColumnsHint) return;
    if (!contactGoalEmailEnabled) {
      el.emailColumnsHint.className = "hint-text";
      el.emailColumnsHint.textContent = "";
      return;
    }
    const selected = new Set(selectedColumns);
    const hasUnified = selected.has("email");
    const selectedRaw = RAW_EMAIL_COLUMNS.filter((column) => selected.has(column));

    el.emailColumnsHint.className = "hint-text";
    if (!hasUnified && selectedRaw.length === 0) {
      el.emailColumnsHint.textContent = "No email columns selected.";
      el.emailColumnsHint.classList.add("warn");
      return;
    }

    if (hasUnified && selectedRaw.length === 0) {
      el.emailColumnsHint.textContent = "Exports one best email column.";
      el.emailColumnsHint.classList.add("success");
      return;
    }

    if (hasUnified) {
      el.emailColumnsHint.textContent = `Exports best email plus ${selectedRaw.length} detail field(s).`;
      return;
    }

    el.emailColumnsHint.textContent = `Exports ${selectedRaw.length} email detail field(s).`;
    el.emailColumnsHint.classList.add("warn");
  }

  function updatePhoneColumnsHint() {
    if (!el.phoneColumnsHint) return;
    if (!contactGoalPhoneEnabled) {
      el.phoneColumnsHint.className = "hint-text";
      el.phoneColumnsHint.textContent = "";
      return;
    }
    const selected = new Set(selectedColumns);
    const hasUnified = selected.has("phone");
    const selectedRaw = RAW_PHONE_COLUMNS.filter((column) => selected.has(column));

    el.phoneColumnsHint.className = "hint-text";
    if (!hasUnified && selectedRaw.length === 0) {
      el.phoneColumnsHint.textContent = "No phone columns selected.";
      el.phoneColumnsHint.classList.add("warn");
      return;
    }

    if (hasUnified && selectedRaw.length === 0) {
      el.phoneColumnsHint.textContent = "Exports one best phone column.";
      el.phoneColumnsHint.classList.add("success");
      return;
    }

    if (hasUnified) {
      el.phoneColumnsHint.textContent = `Exports best phone plus ${selectedRaw.length} detail field(s).`;
      return;
    }

    el.phoneColumnsHint.textContent = `Exports ${selectedRaw.length} detailed phone field(s).`;
    el.phoneColumnsHint.classList.add("warn");
  }

  function onSelectAllColumns() {
    const allVisibleColumns = showAdvancedFields
      ? [...CSV_COLUMNS]
      : CSV_COLUMNS.filter((column) => !ADVANCED_COLUMNS.has(column));
    selectedColumns = normalizeSelectedColumnsForContactGoals(allVisibleColumns);
    emailOutputModeValue = inferOutputModeFromColumns(new Set(selectedColumns), "email", RAW_EMAIL_COLUMNS);
    phoneOutputModeValue = inferOutputModeFromColumns(new Set(selectedColumns), "phone", RAW_PHONE_COLUMNS);
    el.emailOutputMode.value = emailOutputModeValue;
    el.phoneOutputMode.value = phoneOutputModeValue;
    renderColumnSelector();
    updateColumnsTitle();
    updateEmailColumnsHint();
    updatePhoneColumnsHint();
    schedulePersistUiSettings();
    persistSelectedColumns();
    setRunningState(isBusy());
  }

  function onClearColumns() {
    selectedColumns = [];
    emailOutputModeValue = inferOutputModeFromColumns(new Set(selectedColumns), "email", RAW_EMAIL_COLUMNS);
    phoneOutputModeValue = inferOutputModeFromColumns(new Set(selectedColumns), "phone", RAW_PHONE_COLUMNS);
    el.emailOutputMode.value = emailOutputModeValue;
    el.phoneOutputMode.value = phoneOutputModeValue;
    renderColumnSelector();
    updateColumnsTitle();
    updateEmailColumnsHint();
    updatePhoneColumnsHint();
    schedulePersistUiSettings();
    persistSelectedColumns();
    setRunningState(isBusy());
  }

  function onColumnSelectionChange() {
    const checked = Array.from(el.columnList.querySelectorAll("input[type='checkbox']:checked")).map((node) => node.value);
    selectedColumns = normalizeSelectedColumnsForContactGoals(checked);
    const selectedSet = new Set(selectedColumns);
    emailOutputModeValue = inferOutputModeFromColumns(selectedSet, "email", RAW_EMAIL_COLUMNS);
    phoneOutputModeValue = inferOutputModeFromColumns(selectedSet, "phone", RAW_PHONE_COLUMNS);
    el.emailOutputMode.value = emailOutputModeValue;
    el.phoneOutputMode.value = phoneOutputModeValue;
    updateColumnsTitle();
    updateEmailColumnsHint();
    updatePhoneColumnsHint();
    schedulePersistUiSettings();
    persistSelectedColumns();
    setRunningState(isBusy());
  }

  function updateColumnsTitle() {
    const disabledGoalColumns = getDisabledGoalColumnsSet();
    const visibleColumns = showAdvancedFields
      ? CSV_COLUMNS.filter((column) => !disabledGoalColumns.has(column))
      : CSV_COLUMNS.filter((column) => !disabledGoalColumns.has(column) && !ADVANCED_COLUMNS.has(column));
    const visibleSet = new Set(visibleColumns);
    const availableCount = visibleColumns.length;
    const selectedVisibleCount = selectedColumns.filter((column) => visibleSet.has(column)).length;
    el.columnsTitle.textContent = `Export columns (${selectedVisibleCount}/${availableCount})`;
  }

  function persistSelectedColumns() {
    storageSet({
      selectedColumns
    }).catch(() => {});
  }

  function normalizeSelectedColumns(columns, optionsInput) {
    const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
    const fallbackColumns = Array.isArray(options.fallbackColumns) && options.fallbackColumns.length > 0
      ? options.fallbackColumns
      : DEFAULT_SELECTED_COLUMNS;
    if (!Array.isArray(columns)) return [...fallbackColumns];
    const out = [];
    const seen = new Set();
    for (const column of columns) {
      if (!CSV_COLUMNS.includes(column) || seen.has(column)) continue;
      seen.add(column);
      out.push(column);
    }
    if (out.length === 0) {
      return [...fallbackColumns];
    }
    return out;
  }

  function isBusy() {
    return scrapeRunning || enrichRunning;
  }

  function isEnrichmentColumn(column) {
    return column === "email" ||
      column === "owner_name" ||
      column === "owner_title" ||
      column === "owner_email" ||
      column === "contact_email" ||
      column === "primary_email" ||
      column === "primary_email_type" ||
      column === "primary_email_source" ||
      column === "owner_confidence" ||
      column === "email_confidence" ||
      column === "email_source_url" ||
      column === "no_email_reason" ||
      column === "website_phone" ||
      column === "website_phone_source" ||
      column === "website_scan_status" ||
      column === "site_pages_visited" ||
      column === "site_pages_discovered" ||
      column === "social_pages_scanned" ||
      column === "social_links" ||
      column === "discovery_status" ||
      column === "discovery_source" ||
      column === "discovery_query" ||
      column === "discovered_website";
  }

  function rowsAlreadyEnriched(rows) {
    if (!Array.isArray(rows) || rows.length === 0) return false;
    let hasCompletedStatuses = false;

    for (const row of rows) {
      const status = normalizeText(row && row.website_scan_status).toLowerCase();
      if (!status || status === "queued" || status === "not_requested" || status === "running" || status === "stopping" || status === "init") {
        return false;
      }
      hasCompletedStatuses = true;
    }

    return hasCompletedStatuses;
  }

  async function enrichRowsBestEffort(rows, options) {
    const opts = options || {};
    const force = opts.force === true;
    const selectedLeadDiscovery = shouldEnableLeadDiscoveryForSelection();

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
          maxPagesPerSite: FOCUSED_CRAWL_MAX_PAGES,
          timeoutMs: 10000,
          visibleTabs: Boolean(el.showEnrichmentTabs.checked),
          contactGoals: {
            email: Boolean(el.contactGoalEmail && el.contactGoalEmail.checked),
            phone: Boolean(el.contactGoalPhone && el.contactGoalPhone.checked)
          },
          filters: readFilterConfig({
            minRating: el.minRating.value,
            maxRating: el.maxRating.value,
            minReviews: el.minReviews.value,
            maxReviews: el.maxReviews.value,
            hasWebsite: el.hasWebsite.checked,
            hasPhone: el.hasPhone.checked,
            hasEmail: !el.requireEmailForLeads || el.requireEmailForLeads.checked !== false
          }),
          maxSocialPages: 4,
          leadDiscoveryEnabled: selectedLeadDiscovery,
          discoverySources: {
            google: selectedLeadDiscovery,
            linkedin: false,
            yelp: false
          },
          discoveryTrigger: "missing_website_or_missing_email",
          discoveryBudget: {
            googleQueries: 2,
            googlePages: 3,
            linkedinPages: 0,
            yelpPages: 0
          }
        }
      });

      if (!enrichResponse || enrichResponse.type !== MSG.ENRICH_DONE) {
        throw new Error((enrichResponse && enrichResponse.error) || "Website enrichment failed");
      }

      const rowsForExport = Array.isArray(enrichResponse.rows) ? enrichResponse.rows : rows;
      persistRows(rowsForExport);

      const enrichSummary = enrichResponse.summary || {};
      const stopped = enrichSummary.stopped === true;
      el.crawlVisitedStat.textContent = String(Number(enrichSummary.pages_visited || 0));
      el.crawlDiscoveredStat.textContent = String(Number(enrichSummary.pages_discovered || 0));
      el.socialScannedStat.textContent = String(Number(enrichSummary.social_scanned || 0));
      const personalCount = Number(enrichSummary.personal_email_found || 0);
      const companyCount = Number(enrichSummary.company_email_found || 0);
      const discoveryRecovered = Number(enrichSummary.discovery_email_recovered || 0);
      el.emailsFoundStat.textContent = String(Math.max(0, personalCount + companyCount));
      el.discoveryEmailsFoundStat.textContent = String(Math.max(0, discoveryRecovered));
      if (stopped) {
        setState(`Enrichment stopped: ${enrichSummary.processed || 0}/${enrichSummary.total || rowsForExport.length}, pages ${enrichSummary.pages_visited || 0}/${enrichSummary.pages_discovered || 0}`);
        setLeadSignal("Enrichment stopped by user", "warn");
        return rowsForExport;
      }

      setState(`Enrichment: ${enrichSummary.enriched || 0} enriched, ${enrichSummary.skipped || 0} skipped, ${enrichSummary.blocked || 0} blocked, pages ${enrichSummary.pages_visited || 0}/${enrichSummary.pages_discovered || 0}`);
      if (personalCount > 0 || companyCount > 0) {
        setLeadSignal(
          `Saved emails: ${Math.max(0, personalCount + companyCount)}, discovery ${Math.max(0, discoveryRecovered)}`,
          "success"
        );
      } else {
        setLeadSignal("No public emails found during enrichment", "warn");
      }
      return rowsForExport;
    } finally {
      enrichRunning = false;
      setRunningState(isBusy());
    }
  }

  function applyScrapeCounters(payload, rowsLengthFallback) {
    const data = payload || {};
    const fallback = Number(rowsLengthFallback || 0);
    const processed = Number(data.processed || 0);
    const matchedRaw = data.matched != null ? Number(data.matched) : fallback;
    const duplicates = Number(data.duplicates || 0);
    const errors = Number(data.errors || 0);

    el.processed.textContent = String(Number.isFinite(processed) ? processed : 0);
    el.matched.textContent = String(Number.isFinite(matchedRaw) ? matchedRaw : fallback);
    el.duplicates.textContent = String(Number.isFinite(duplicates) ? duplicates : 0);
    el.errors.textContent = String(Number.isFinite(errors) ? errors : 0);
  }

  function setLeadSignal(text, tone) {
    const clean = normalizeText(text);
    const level = normalizeText(tone).toLowerCase();
    const supportedLevel = level === "success" || level === "warn" || level === "info" ? level : "info";
    el.leadSignalText.textContent = clean;
    el.leadSignalText.className = clean ? `lead-signal ${supportedLevel}` : "lead-signal";
  }

  function createRunId() {
    return `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  }

  async function resolveScrapeRunTabId() {
    if (Number.isFinite(Number(scrapeRunTabId))) {
      return Number(scrapeRunTabId);
    }

    try {
      const data = await storageGet([SCRAPE_SESSION_KEY]);
      const session = data[SCRAPE_SESSION_KEY];
      if (!session || typeof session !== "object") return null;
      const status = normalizeText(session.status).toLowerCase();
      if (status !== "running" && status !== "stopping") return null;
      if (!Number.isFinite(Number(session.tab_id))) return null;
      scrapeRunTabId = Number(session.tab_id);
      return scrapeRunTabId;
    } catch (_error) {
      return null;
    }
  }

  function updatePerformanceMetrics(payload) {
    const data = payload || {};
    const speed = Number(data.rate_per_sec || 0);
    const seen = Number(data.seen_listings || 0);
    let avgRating = data.avg_rating_seen;
    let avgReviews = data.avg_reviews_seen;

    const fallbackAvgRating = averageFromRows(lastRows, "rating", 0.01, 5);
    const fallbackAvgReviews = averageFromRows(lastRows, "review_count", 0, Number.POSITIVE_INFINITY);

    if (!Number.isFinite(Number(avgRating)) && Number.isFinite(Number(fallbackAvgRating))) {
      avgRating = fallbackAvgRating;
    }

    const avgReviewsNum = Number(avgReviews);
    const shouldBackfillReviews =
      !Number.isFinite(avgReviewsNum) ||
      (avgReviewsNum === 0 && (seen > 0 || (Array.isArray(lastRows) && lastRows.length > 0)));
    if (shouldBackfillReviews && Number.isFinite(Number(fallbackAvgReviews))) {
      avgReviews = fallbackAvgReviews;
    }

    el.speedStat.textContent = `${formatNumber(speed, 2)}/s`;
    el.seenListingsStat.textContent = Number.isFinite(seen) ? String(seen) : "0";
    el.avgRatingStat.textContent = formatMaybeNumber(avgRating, 2);
    el.avgReviewsStat.textContent = formatMaybeNumber(avgReviews, 1);
  }

  function averageFromRows(rows, field, minValue, maxValue) {
    if (!Array.isArray(rows) || rows.length === 0) return "";
    let sum = 0;
    let count = 0;

    for (const row of rows) {
      const value = parseMetricNumber(row && row[field]);
      if (!Number.isFinite(value)) continue;
      if (value < minValue || value > maxValue) continue;
      sum += value;
      count += 1;
    }

    if (count === 0) return "";
    return sum / count;
  }

  function parseMetricNumber(value) {
    if (value == null) return NaN;
    if (typeof value === "number") return Number.isFinite(value) ? value : NaN;

    const text = normalizeText(value).toLowerCase().replace(/\s+/g, "");
    if (!text) return NaN;

    const match = text.match(/^(\d[\d,]*)(?:\.(\d+))?([kmb])?$/i);
    if (!match) return NaN;

    const whole = Number((match[1] || "").replace(/,/g, ""));
    if (!Number.isFinite(whole)) return NaN;
    const fraction = match[2] ? Number(`0.${match[2]}`) : 0;
    if (!Number.isFinite(fraction)) return NaN;

    const multiplier = match[3] ? (match[3].toLowerCase() === "k" ? 1000 : match[3].toLowerCase() === "m" ? 1000000 : 1000000000) : 1;
    return (whole + fraction) * multiplier;
  }

  function formatMaybeNumber(value, digits) {
    if (value === "" || value == null) return "-";
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
