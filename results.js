(function () {
  const shared = window.GbpShared;
  const { MSG, CSV_COLUMNS, COLUMN_LABELS, sanitizeColumns, normalizeText, normalizePhoneText, csvEscape } = shared;
  const MAX_RENDER_ROWS = 1000;
  const ROW_INDEX_WIDTH = 42;
  const MAX_COLUMN_WIDTH = 720;
  const DEFAULT_TRACKING_TITLE = "Status";
  const MAPS_URL_COLUMN = "maps_url";
  const VIEWER_TRACKING_COLUMN = "__viewer_tracking__";
  const URL_COLUMNS = new Set(["website", "listing_facebook", "facebook_could_be", "maps_url", "source_url", "discovered_website"]);
  const EMAIL_COLUMNS = new Set(["email", "owner_email", "contact_email", "primary_email"]);
  const PHONE_COLUMNS = new Set(["phone", "listing_phone", "website_phone"]);
  const requestedRunId = readRequestedRunId();
  const textMeasureCanvas = document.createElement("canvas");

  const el = {
    metaText: document.getElementById("metaText"),
    importBtn: document.getElementById("importBtn"),
    importFileInput: document.getElementById("importFileInput"),
    refreshBtn: document.getElementById("refreshBtn"),
    exportBtn: document.getElementById("exportBtn"),
    rowsCount: document.getElementById("rowsCount"),
    colsCount: document.getElementById("colsCount"),
    renderHint: document.getElementById("renderHint"),
    statusChip: document.getElementById("statusChip"),
    emptyState: document.getElementById("emptyState"),
    tableWrap: document.getElementById("tableWrap"),
    resultsTable: document.getElementById("resultsTable"),
    tableCols: document.getElementById("tableCols"),
    tableHead: document.getElementById("tableHead"),
    tableBody: document.getElementById("tableBody"),
    errorText: document.getElementById("errorText")
  };

  let rows = [];
  let selectedColumns = [...CSV_COLUMNS];
  let scrapeSession = null;
  let enrichSession = null;
  let enrichRuntimeState = null;
  let importedRows = null;
  let importedColumns = [];
  let importedFileName = "";
  let importedLoadedAt = "";
  let viewerTrackingTitle = DEFAULT_TRACKING_TITLE;
  let activeResize = null;
  const columnWidths = new Map();
  const viewerTrackingChecked = new Map();

  init().catch((error) => {
    setError(error && error.message ? error.message : "Failed to load results");
  });

  async function init() {
    bindEvents();
    await refreshState();
    chrome.storage.onChanged.addListener(onStorageChanged);
  }

  function bindEvents() {
    el.importBtn.addEventListener("click", () => {
      el.importFileInput.value = "";
      el.importFileInput.click();
    });
    el.importFileInput.addEventListener("change", () => {
      void handleImportSelection();
    });
    el.refreshBtn.addEventListener("click", () => {
      if (isImportedView()) {
        clearImportedView();
      }
      void refreshState();
    });
    el.exportBtn.addEventListener("click", () => {
      void exportRows();
    });
    window.addEventListener("mousemove", onColumnResizeMove);
    window.addEventListener("mouseup", stopColumnResize);
    window.addEventListener("blur", stopColumnResize);
  }

  async function refreshState() {
    clearError();
    const [data, runtime] = await Promise.all([
      storageGet(["lastRows", "selectedColumns", "scrapeSession", "enrichSession"]),
      readEnrichRuntimeState()
    ]);

    rows = Array.isArray(data.lastRows) ? data.lastRows : [];
    selectedColumns = Array.isArray(data.selectedColumns) ? sanitizeColumns(data.selectedColumns) : [...CSV_COLUMNS];
    scrapeSession = selectSessionForRun(data.scrapeSession, "scrape");
    enrichSession = selectSessionForRun(data.enrichSession, "enrich");
    enrichRuntimeState = runtime;
    render();
  }

  function onStorageChanged(changes, areaName) {
    if (areaName !== "local") return;
    let shouldRender = false;

    if (changes.lastRows) {
      rows = Array.isArray(changes.lastRows.newValue) ? changes.lastRows.newValue : [];
      shouldRender = true;
    }
    if (changes.selectedColumns) {
      selectedColumns = Array.isArray(changes.selectedColumns.newValue) ? sanitizeColumns(changes.selectedColumns.newValue) : [...CSV_COLUMNS];
      shouldRender = true;
    }
    if (changes.scrapeSession) {
      scrapeSession = selectSessionForRun(changes.scrapeSession.newValue, "scrape");
      shouldRender = true;
    }
    if (changes.enrichSession) {
      enrichSession = selectSessionForRun(changes.enrichSession.newValue, "enrich");
      shouldRender = true;
      void readEnrichRuntimeState().then((runtime) => {
        enrichRuntimeState = runtime;
        if (!isImportedView()) render();
      });
    }

    if (shouldRender && !isImportedView()) render();
  }

  function render() {
    const columns = getDisplayColumns();
    const allRows = getViewRows();
    const renderRows = allRows.slice(0, MAX_RENDER_ROWS);
    const hasRows = allRows.length > 0;
    const statusInfo = deriveStatus();

    el.rowsCount.textContent = `${allRows.length} row(s)`;
    el.colsCount.textContent = `${columns.length} column(s)`;
    el.renderHint.textContent = allRows.length > MAX_RENDER_ROWS ? `Showing first ${MAX_RENDER_ROWS}` : "";
    el.refreshBtn.textContent = isImportedView() ? "Live Data" : "Refresh";
    if (el.statusChip) {
      el.statusChip.textContent = statusInfo.label;
      el.statusChip.className = `status-chip ${statusInfo.tone}`;
    }
    el.exportBtn.disabled = allRows.length === 0 || columns.length === 0;
    el.metaText.textContent = buildMetaText(statusInfo.rawStatus);
    el.emptyState.textContent = buildEmptyStateMessage(statusInfo.rawStatus);

    if (!hasRows) {
      el.emptyState.classList.remove("hidden");
      el.tableWrap.classList.add("hidden");
      el.tableCols.textContent = "";
      el.tableHead.textContent = "";
      el.tableBody.textContent = "";
      return;
    }

    el.emptyState.classList.add("hidden");
    el.tableWrap.classList.remove("hidden");
    applyColumnWidths(columns);

    const headRow = document.createElement("tr");
    const rowNumHead = document.createElement("th");
    rowNumHead.textContent = "#";
    rowNumHead.className = "row-index";
    headRow.appendChild(rowNumHead);

    for (const column of columns) {
      const th = document.createElement("th");
      th.className = "can-autofit";
      th.title = "Double-click to auto-fit. Drag the right edge to resize.";
      th.addEventListener("dblclick", () => {
        autoFitColumn(column);
      });
      if (column === VIEWER_TRACKING_COLUMN) {
        const trackingHeader = document.createElement("div");
        trackingHeader.className = "tracking-header";
        const titleInput = document.createElement("input");
        titleInput.type = "text";
        titleInput.className = "tracking-title-input";
        titleInput.value = getViewerTrackingTitle();
        titleInput.placeholder = DEFAULT_TRACKING_TITLE;
        titleInput.spellcheck = false;
        titleInput.addEventListener("mousedown", (event) => {
          event.stopPropagation();
        });
        titleInput.addEventListener("click", (event) => {
          event.stopPropagation();
        });
        titleInput.addEventListener("dblclick", (event) => {
          event.stopPropagation();
        });
        titleInput.addEventListener("input", () => {
          viewerTrackingTitle = titleInput.value;
        });
        titleInput.addEventListener("blur", () => {
          if (normalizeText(viewerTrackingTitle)) return;
          viewerTrackingTitle = DEFAULT_TRACKING_TITLE;
          titleInput.value = DEFAULT_TRACKING_TITLE;
        });
        trackingHeader.appendChild(titleInput);

        const actions = document.createElement("div");
        actions.className = "tracking-header-actions";

        const checkAllBtn = document.createElement("button");
        checkAllBtn.type = "button";
        checkAllBtn.className = "tracking-bulk-btn";
        checkAllBtn.textContent = "All";
        checkAllBtn.title = "Check all rows";
        checkAllBtn.addEventListener("mousedown", stopHeaderEvent);
        checkAllBtn.addEventListener("click", (event) => {
          stopHeaderEvent(event);
          setAllTrackingChecked(true);
        });
        actions.appendChild(checkAllBtn);

        const clearAllBtn = document.createElement("button");
        clearAllBtn.type = "button";
        clearAllBtn.className = "tracking-bulk-btn";
        clearAllBtn.textContent = "None";
        clearAllBtn.title = "Clear all rows";
        clearAllBtn.addEventListener("mousedown", stopHeaderEvent);
        clearAllBtn.addEventListener("click", (event) => {
          stopHeaderEvent(event);
          setAllTrackingChecked(false);
        });
        actions.appendChild(clearAllBtn);

        trackingHeader.appendChild(actions);
        th.appendChild(trackingHeader);
      } else {
        const label = document.createElement("span");
        label.className = "header-label";
        label.textContent = getColumnHeaderLabel(column);
        th.appendChild(label);
      }

      const resizeHandle = document.createElement("div");
      resizeHandle.className = "col-resize-handle";
      resizeHandle.title = "Drag to resize. Double-click to auto-fit.";
      resizeHandle.addEventListener("mousedown", (event) => {
        startColumnResize(event, column);
      });
      resizeHandle.addEventListener("dblclick", (event) => {
        event.preventDefault();
        event.stopPropagation();
        autoFitColumn(column);
      });
      th.appendChild(resizeHandle);
      headRow.appendChild(th);
    }
    el.tableHead.textContent = "";
    el.tableHead.appendChild(headRow);

    const fragment = document.createDocumentFragment();
    for (let i = 0; i < renderRows.length; i += 1) {
      const row = renderRows[i];
      const tr = document.createElement("tr");

      const indexCell = document.createElement("td");
      indexCell.className = "row-index";
      indexCell.textContent = String(i + 1);
      tr.appendChild(indexCell);

      for (const column of columns) {
        const td = document.createElement("td");
        renderCell(td, column, row, i);
        tr.appendChild(td);
      }
      fragment.appendChild(tr);
    }
    el.tableBody.textContent = "";
    el.tableBody.appendChild(fragment);
  }

  function renderCell(td, column, row, rowIndex) {
    if (column === VIEWER_TRACKING_COLUMN) {
      renderTrackingCell(td, row, rowIndex);
      return;
    }

    const value = row && row[column];
    const { clean } = getDisplayValueParts(column, value);

    if (!clean) {
      td.classList.add("empty-cell");
      td.textContent = "-";
      return;
    }

    td.title = clean;

    if (column === "website_scan_status") {
      td.classList.add("cell-status");
      td.textContent = humanizeStatus(clean);
      return;
    }

    if (URL_COLUMNS.has(column) && isLikelyUrl(clean)) {
      const link = document.createElement("a");
      link.className = "cell-link";
      link.href = clean;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.title = clean;
      link.textContent = clean;
      td.appendChild(link);
      return;
    }

    if (EMAIL_COLUMNS.has(column) && clean.includes("@")) {
      const copyButton = document.createElement("button");
      copyButton.type = "button";
      copyButton.className = "cell-copy";
      copyButton.title = `${clean} (click to copy)`;
      copyButton.textContent = clean;
      copyButton.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        void copyEmailValue(copyButton, clean);
      });
      td.appendChild(copyButton);
      return;
    }

    td.textContent = clean;
  }

  function deriveStatus() {
    if (isImportedView()) {
      return {
        rawStatus: "imported",
        tone: "success",
        label: "Imported File"
      };
    }

    const scrapeStatus = normalizeText(scrapeSession && scrapeSession.status).toLowerCase();
    const enrichStatus = normalizeText(enrichSession && enrichSession.status).toLowerCase();

    let status = enrichStatus || scrapeStatus || "idle";

    if ((status === "running" || status === "stopping") && enrichRuntimeState && enrichRuntimeState.is_running !== true && enrichStatus) {
      status = "stopped";
    }

    let tone = "idle";
    if (status === "done" || status === "enriched") tone = "success";
    else if (status === "error" || status === "stopped" || status === "stopping") tone = "warn";

    return {
      rawStatus: status,
      tone,
      label: humanizeStatus(status)
    };
  }

  function buildMetaText(status) {
    if (isImportedView()) {
      const loadedLabel = importedLoadedAt ? new Date(importedLoadedAt).toLocaleString() : "";
      const parts = [
        "Status: Imported File",
        importedFileName ? `File: ${importedFileName}` : "",
        loadedLabel ? `Loaded: ${loadedLabel}` : ""
      ].filter(Boolean);
      return parts.join(" | ") || "Imported file";
    }

    const scrapeUpdated = normalizeText(scrapeSession && scrapeSession.updated_at);
    const enrichUpdated = normalizeText(enrichSession && enrichSession.updated_at);
    const updatedAt = enrichUpdated || scrapeUpdated;

    const runId = normalizeText((enrichSession && enrichSession.source_run_id) || (scrapeSession && scrapeSession.run_id));
    const updatedLabel = updatedAt ? new Date(updatedAt).toLocaleString() : "";
    const parts = [
      status ? `Status: ${humanizeStatus(status)}` : "",
      runId ? `run ${runId}` : "",
      updatedLabel ? `Updated: ${updatedLabel}` : ""
    ].filter(Boolean);

    return parts.join(" | ") || "Waiting for data...";
  }

  function buildEmptyStateMessage(status) {
    if (isImportedView()) {
      return "Imported file has no rows.";
    }

    const liveStatus = normalizeText(status).toLowerCase();
    const filters = scrapeSession && typeof scrapeSession.filters === "object" ? scrapeSession.filters : {};
    const storedRowCount = Number(scrapeSession && scrapeSession.rows_count);
    const hasPersistedRunRows = Number.isFinite(storedRowCount) && storedRowCount > 0;
    const completedRun =
      liveStatus === "done" ||
      liveStatus === "stopped" ||
      liveStatus === "error" ||
      liveStatus === "enriched";

    if (hasPersistedRunRows) {
      return "Run metadata is available, but the viewer could not restore the row payload. Refresh the viewer and, if this keeps happening on large runs, rerun after reloading the extension.";
    }

    if (filters.hasEmail === true && completedRun) {
      const enrichStatus = normalizeText(enrichSession && enrichSession.status).toLowerCase();
      if (!enrichStatus || enrichStatus === "idle") {
        return "No rows matched the final output filters. 'Keep only leads with email' is enabled. Enable website enrichment or turn that filter off.";
      }
      return "No rows matched the final output filters. 'Keep only leads with email' is enabled for this run.";
    }

    if (hasAnyConfiguredFilter(filters) && completedRun) {
      return "No rows matched the current filters for this run.";
    }

    return "No rows yet. Run a scrape first.";
  }

  function hasAnyConfiguredFilter(filtersInput) {
    const filters = filtersInput && typeof filtersInput === "object" ? filtersInput : {};
    return (
      filters.minRating !== "" && filters.minRating != null ||
      filters.maxRating !== "" && filters.maxRating != null ||
      filters.minReviews !== "" && filters.minReviews != null ||
      filters.maxReviews !== "" && filters.maxReviews != null ||
      normalizeText(filters.nameKeyword) !== "" ||
      normalizeText(filters.categoryInclude) !== "" ||
      normalizeText(filters.categoryExclude) !== "" ||
      filters.hasWebsite === true ||
      filters.hasPhone === true ||
      filters.hasEmail === true
    );
  }

  function selectSessionForRun(sessionValue, sessionKind) {
    if (!sessionValue || typeof sessionValue !== "object") return null;
    if (!requestedRunId) return sessionValue;

    const session = sessionValue;
    const runId =
      sessionKind === "enrich"
        ? normalizeText(session.source_run_id || session.run_id)
        : normalizeText(session.run_id);

    if (!runId) return session;
    return runId === requestedRunId ? session : null;
  }

  function readRequestedRunId() {
    try {
      const url = new URL(window.location.href);
      return normalizeText(url.searchParams.get("run_id"));
    } catch (_error) {
      return "";
    }
  }

  function getDisplayColumns() {
    if (isImportedView()) {
      if (importedColumns.length > 0) return appendViewerTrackingColumn(importedColumns);
      if (importedRows && importedRows.length > 0) return appendViewerTrackingColumn(Object.keys(importedRows[0] || {}));
      return [];
    }

    const normalized = sanitizeColumns(Array.isArray(selectedColumns) ? selectedColumns : []);
    if (normalized.length > 0) return appendViewerTrackingColumn(normalized);

    if (rows.length > 0) {
      const keys = Object.keys(rows[0] || {});
      const fromRow = sanitizeColumns(keys);
      if (fromRow.length > 0) return appendViewerTrackingColumn(fromRow);
    }

    return appendViewerTrackingColumn(CSV_COLUMNS);
  }

  async function exportRows() {
    clearError();
    const columns = getDisplayColumns();
    const dataRows = getViewRows();

    if (dataRows.length === 0) {
      setError("No rows to export.");
      return;
    }
    if (columns.length === 0) {
      setError("No export columns selected.");
      return;
    }

    try {
      const rowsForExport = dataRows.map((row) => normalizePhoneColumnsForExport(row, columns));
      const csvText = rowsToCsvLoose(rowsForExport, columns);
      await downloadCsvFile(csvText, exportFilename());
    } catch (error) {
      setError(error && error.message ? error.message : "CSV export failed");
    }
  }

  async function readEnrichRuntimeState() {
    try {
      const response = await sendRuntimeMessage({ type: MSG.GET_ENRICH_STATE });
      if (response && response.ok === true && response.state && typeof response.state === "object") {
        return response.state;
      }
      return null;
    } catch (_error) {
      return null;
    }
  }

  function normalizeCell(value) {
    if (value == null) return "";
    if (Array.isArray(value)) {
      return normalizeText(value.join(" | "));
    }
    if (typeof value === "object") {
      try {
        return normalizeText(JSON.stringify(value));
      } catch (_error) {
        return "";
      }
    }
    return normalizeText(value);
  }

  function getDisplayValueParts(column, value) {
    const rawClean = normalizeCell(value);
    const clean = PHONE_COLUMNS.has(column) ? normalizePhoneText(rawClean) || rawClean : rawClean;
    return { rawClean, clean };
  }

  function normalizePhoneColumnsForExport(row, columns) {
    const source = row && typeof row === "object" ? row : {};
    const out = { ...source };
    for (const column of PHONE_COLUMNS) {
      if (!columns.includes(column)) continue;
      const normalized = normalizePhoneText(out[column]);
      out[column] = normalized || normalizeText(out[column]);
    }
    return out;
  }

  function getViewRows() {
    return isImportedView() ? importedRows || [] : rows;
  }

  function isImportedView() {
    return Array.isArray(importedRows);
  }

  function clearImportedView() {
    clearTrackingScope(buildTrackingScopeId(`import:${importedFileName}`));
    importedRows = null;
    importedColumns = [];
    importedFileName = "";
    importedLoadedAt = "";
  }

  async function handleImportSelection() {
    clearError();
    const file = el.importFileInput.files && el.importFileInput.files[0];
    if (!file) return;

    try {
      await loadImportedCsv(file);
      render();
    } catch (error) {
      setError(error && error.message ? error.message : "Failed to import CSV");
    } finally {
      el.importFileInput.value = "";
    }
  }

  async function loadImportedCsv(file) {
    const importScope = buildTrackingScopeId(`import:${normalizeText(file && file.name) || "csv"}`);
    const text = await file.text();
    const parsed = parseCsvText(text);
    if (parsed.columns.length === 0) {
      throw new Error("Imported CSV does not contain a header row.");
    }

    importedFileName = normalizeText(file && file.name) || defaultFilename();
    const importedData = extractImportedTrackingColumn(parsed, importScope);
    importedColumns = importedData.columns;
    importedRows = importedData.rows;
    importedLoadedAt = new Date().toISOString();
  }

  function parseCsvText(text) {
    const source = String(text == null ? "" : text).replace(/^\uFEFF/, "");
    const matrix = [];
    let row = [];
    let cell = "";
    let inQuotes = false;

    for (let i = 0; i < source.length; i += 1) {
      const ch = source[i];
      if (inQuotes) {
        if (ch === '"') {
          if (source[i + 1] === '"') {
            cell += '"';
            i += 1;
          } else {
            inQuotes = false;
          }
        } else {
          cell += ch;
        }
        continue;
      }

      if (ch === '"') {
        inQuotes = true;
        continue;
      }
      if (ch === ",") {
        row.push(cell);
        cell = "";
        continue;
      }
      if (ch === "\n") {
        row.push(cell);
        matrix.push(row);
        row = [];
        cell = "";
        continue;
      }
      if (ch === "\r") {
        if (source[i + 1] === "\n") i += 1;
        row.push(cell);
        matrix.push(row);
        row = [];
        cell = "";
        continue;
      }
      cell += ch;
    }

    if (cell !== "" || row.length > 0) {
      row.push(cell);
      matrix.push(row);
    }

    const nonEmptyRows = matrix.filter((entry) => entry.some((value) => normalizeText(value) !== ""));
    if (nonEmptyRows.length === 0) {
      return { columns: [], rows: [] };
    }

    const columns = sanitizeImportedColumns(nonEmptyRows[0]);
    const parsedRows = [];
    for (let rowIndex = 1; rowIndex < nonEmptyRows.length; rowIndex += 1) {
      const values = nonEmptyRows[rowIndex];
      const out = {};
      let hasValue = false;

      for (let columnIndex = 0; columnIndex < columns.length; columnIndex += 1) {
        const column = columns[columnIndex];
        const value = columnIndex < values.length ? values[columnIndex] : "";
        if (normalizeText(value) !== "") hasValue = true;
        out[column] = value;
      }

      if (hasValue) {
        parsedRows.push(out);
      }
    }

    return { columns, rows: parsedRows };
  }

  function sanitizeImportedColumns(columns) {
    const out = [];
    const seen = new Set();

    for (let index = 0; index < columns.length; index += 1) {
      const raw = normalizeText(columns[index]);
      const base = raw || `column_${index + 1}`;
      let candidate = base;
      let suffix = 2;
      while (seen.has(candidate.toLowerCase())) {
        candidate = `${base}_${suffix}`;
        suffix += 1;
      }
      seen.add(candidate.toLowerCase());
      out.push(candidate);
    }

    return out;
  }

  function rowsToCsvLoose(rowsForExport, columns) {
    const header = columns.map((column) => csvEscape(getExportColumnName(column))).join(",");
    const body = (rowsForExport || []).map((row, rowIndex) => {
      return columns
        .map((key) => {
          const value = getExportCellValue(row, key, rowIndex);
          if (typeof value === "string") {
            return csvEscape(normalizeText(value));
          }
          return csvEscape(value);
        })
        .join(",");
    });
    return [header, ...body].join("\n");
  }

  function getExportColumnName(column) {
    if (column === VIEWER_TRACKING_COLUMN) {
      return getViewerTrackingTitle();
    }
    return normalizeText(column) || column;
  }

  function getExportCellValue(row, column, rowIndex) {
    if (column === VIEWER_TRACKING_COLUMN) {
      return isRowTrackingChecked(row, rowIndex) ? "TRUE" : "";
    }
    return row ? row[column] : "";
  }

  function exportFilename() {
    if (isImportedView() && importedFileName) return importedFileName;
    return defaultFilename();
  }

  function downloadCsvFile(csvText, filename) {
    const url = `data:text/csv;charset=utf-8,${encodeURIComponent(csvText)}`;
    return new Promise((resolve, reject) => {
      chrome.downloads.download(
        {
          url,
          filename: filename || defaultFilename(),
          saveAs: true
        },
        (downloadId) => {
          if (chrome.runtime.lastError) {
            reject(new Error(chrome.runtime.lastError.message || "CSV export failed"));
            return;
          }
          resolve(downloadId);
        }
      );
    });
  }

  function applyColumnWidths(columns) {
    const fragment = document.createDocumentFragment();
    const indexCol = document.createElement("col");
    indexCol.style.width = `${ROW_INDEX_WIDTH}px`;
    fragment.appendChild(indexCol);
    let totalWidth = ROW_INDEX_WIDTH;

    for (const column of columns) {
      const width = getColumnWidth(column);
      const col = document.createElement("col");
      col.style.width = `${width}px`;
      fragment.appendChild(col);
      totalWidth += width;
    }

    el.tableCols.textContent = "";
    el.tableCols.appendChild(fragment);
    el.resultsTable.style.width = `${Math.max(totalWidth, 1)}px`;
  }

  function getColumnWidth(column) {
    if (columnWidths.has(column)) {
      return columnWidths.get(column);
    }
    if (column === VIEWER_TRACKING_COLUMN) return 140;
    if (EMAIL_COLUMNS.has(column)) return 240;
    if (URL_COLUMNS.has(column)) return 260;
    if (PHONE_COLUMNS.has(column)) return 150;
    if (column === "website_scan_status") return 160;
    return 180;
  }

  function appendViewerTrackingColumn(columns) {
    const ordered = orderColumnsForViewer(columns).filter((column) => column !== VIEWER_TRACKING_COLUMN);
    if (ordered.length === 0) return ordered;
    ordered.push(VIEWER_TRACKING_COLUMN);
    return ordered;
  }

  function orderColumnsForViewer(columns) {
    const ordered = Array.isArray(columns) ? [...columns] : [];
    const mapsIndex = ordered.indexOf(MAPS_URL_COLUMN);
    if (mapsIndex < 0) return ordered;
    ordered.splice(mapsIndex, 1);
    ordered.push(MAPS_URL_COLUMN);
    return ordered;
  }

  function autoFitColumn(column) {
    const rowsForMeasure = getViewRows();
    const label = getColumnHeaderLabel(column);
    const headerCell = findHeaderCell(column);
    const sampleBodyCell = findBodyCell(column);
    const sampleBodyTarget = (sampleBodyCell && sampleBodyCell.querySelector("a")) || sampleBodyCell;
    const headerFont = headerCell ? window.getComputedStyle(headerCell).font : "700 12px Segoe UI";
    const bodyFont = sampleBodyTarget ? window.getComputedStyle(sampleBodyTarget).font : "12px Segoe UI";
    let widest = measureTextWidth(label, headerFont);

    for (let rowIndex = 0; rowIndex < rowsForMeasure.length; rowIndex += 1) {
      const row = rowsForMeasure[rowIndex];
      const text = getDisplayTextForMeasure(column, row, rowIndex);
      widest = Math.max(widest, measureTextWidth(text, bodyFont));
    }

    columnWidths.set(column, clampWidth(Math.ceil(widest + 28)));
    applyColumnWidths(getDisplayColumns());
  }

  function startColumnResize(event, column) {
    if (!event || event.button !== 0) return;
    event.preventDefault();
    event.stopPropagation();
    activeResize = {
      column,
      startX: Number(event.clientX) || 0,
      startWidth: getColumnWidth(column)
    };
    document.body.classList.add("is-resizing-columns");
  }

  function onColumnResizeMove(event) {
    if (!activeResize) return;
    const clientX = Number(event && event.clientX);
    const delta = (Number.isFinite(clientX) ? clientX : activeResize.startX) - activeResize.startX;
    const nextWidth = clampWidth(activeResize.startWidth + delta);
    columnWidths.set(activeResize.column, nextWidth);
    applyColumnWidths(getDisplayColumns());
  }

  function stopColumnResize() {
    if (!activeResize) return;
    activeResize = null;
    document.body.classList.remove("is-resizing-columns");
  }

  function findHeaderCell(column) {
    const columns = getDisplayColumns();
    const index = columns.indexOf(column);
    if (index < 0) return null;
    return el.tableHead.querySelector(`th:nth-child(${index + 2})`);
  }

  function findBodyCell(column) {
    const columns = getDisplayColumns();
    const index = columns.indexOf(column);
    if (index < 0) return null;
    return el.tableBody.querySelector(`td:nth-child(${index + 2})`);
  }

  function getDisplayTextForMeasure(column, row, rowIndex) {
    if (column === VIEWER_TRACKING_COLUMN) {
      return isRowTrackingChecked(row, rowIndex) ? "Checked" : "";
    }
    const value = row && row[column];
    const { clean } = getDisplayValueParts(column, value);
    if (!clean) return "-";
    if (column === "website_scan_status") return humanizeStatus(clean);
    if (URL_COLUMNS.has(column) && isLikelyUrl(clean)) return clean;
    if (EMAIL_COLUMNS.has(column) && clean.includes("@")) return clean;
    return clean;
  }

  function getColumnHeaderLabel(column) {
    if (column === VIEWER_TRACKING_COLUMN) {
      return getViewerTrackingTitle();
    }
    return COLUMN_LABELS[column] || column;
  }

  function getViewerTrackingTitle() {
    return normalizeText(viewerTrackingTitle) || DEFAULT_TRACKING_TITLE;
  }

  function renderTrackingCell(td, row, rowIndex) {
    td.classList.add("tracking-cell");
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.className = "tracking-checkbox";
    checkbox.checked = isRowTrackingChecked(row, rowIndex);
    checkbox.addEventListener("change", () => {
      setRowTrackingChecked(row, rowIndex, checkbox.checked);
      td.title = checkbox.checked ? `${getViewerTrackingTitle()}: checked` : `${getViewerTrackingTitle()}: not checked`;
    });
    td.title = checkbox.checked ? `${getViewerTrackingTitle()}: checked` : `${getViewerTrackingTitle()}: not checked`;
    td.appendChild(checkbox);
  }

  function stopHeaderEvent(event) {
    if (!event) return;
    event.preventDefault();
    event.stopPropagation();
  }

  function setRowTrackingChecked(row, rowIndex, checked) {
    const key = getRowTrackingKey(row, rowIndex);
    if (checked) {
      viewerTrackingChecked.set(key, true);
      return;
    }
    viewerTrackingChecked.delete(key);
  }

  function isRowTrackingChecked(row, rowIndex) {
    return viewerTrackingChecked.get(getRowTrackingKey(row, rowIndex)) === true;
  }

  function setAllTrackingChecked(checked) {
    const scopeId = getTrackingScopeId();
    const rowsForView = getViewRows();

    if (checked) {
      for (let rowIndex = 0; rowIndex < rowsForView.length; rowIndex += 1) {
        setRowTrackingChecked(rowsForView[rowIndex], rowIndex, true);
      }
    } else {
      clearTrackingScope(scopeId);
    }

    render();
  }

  function getRowTrackingKey(row, rowIndex) {
    return buildTrackingRowKey(row, rowIndex, getTrackingScopeId());
  }

  function getTrackingScopeId() {
    if (isImportedView()) {
      return buildTrackingScopeId(`import:${importedFileName}`);
    }
    return buildTrackingScopeId(requestedRunId ? `run:${requestedRunId}` : "live");
  }

  function buildTrackingScopeId(value) {
    return normalizeText(value).toLowerCase() || "viewer";
  }

  function buildTrackingRowKey(row, rowIndex, scopeId) {
    const source = row && typeof row === "object" ? row : {};
    const phoneValue = normalizePhoneText(source.phone || source.listing_phone || source.website_phone || "");
    const parts = [
      normalizeText(source.place_id).toLowerCase(),
      normalizeText(source.maps_url).toLowerCase(),
      normalizeText(source.source_url).toLowerCase(),
      normalizeText(source.website).toLowerCase(),
      normalizeText(source.name).toLowerCase(),
      normalizeText(source.address).toLowerCase(),
      normalizeText(phoneValue).toLowerCase()
    ];
    const stableParts = parts.filter(Boolean);
    if (stableParts.length > 0) {
      return `${scopeId}|${stableParts.join("|")}`;
    }
    return `${scopeId}|row:${Number(rowIndex) || 0}`;
  }

  function clearTrackingScope(scopeId) {
    const prefix = `${buildTrackingScopeId(scopeId)}|`;
    for (const key of Array.from(viewerTrackingChecked.keys())) {
      if (key.startsWith(prefix)) {
        viewerTrackingChecked.delete(key);
      }
    }
  }

  function extractImportedTrackingColumn(parsed, importScope) {
    const columns = Array.isArray(parsed && parsed.columns) ? [...parsed.columns] : [];
    const rows = Array.isArray(parsed && parsed.rows) ? parsed.rows.map((row) => ({ ...(row || {}) })) : [];
    clearTrackingScope(importScope);

    const lastColumn = columns.length > 0 ? columns[columns.length - 1] : "";
    if (!lastColumn || !isLikelyImportedTrackingColumn(lastColumn, rows)) {
      return { columns, rows };
    }

    viewerTrackingTitle = normalizeText(lastColumn) || DEFAULT_TRACKING_TITLE;
    columns.pop();

    for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
      const row = rows[rowIndex];
      const checked = parseTrackingCellValue(row && row[lastColumn]);
      if (checked) {
        const key = buildTrackingRowKey(row, rowIndex, importScope);
        viewerTrackingChecked.set(key, true);
      }
      if (row && typeof row === "object") {
        delete row[lastColumn];
      }
    }

    return { columns, rows };
  }

  function isLikelyImportedTrackingColumn(column, rows) {
    if (!column || !Array.isArray(rows) || rows.length === 0) return false;
    if (CSV_COLUMNS.includes(normalizeText(column))) return false;
    return rows.every((row) => isTrackingCellValue(row && row[column]));
  }

  function isTrackingCellValue(value) {
    const text = normalizeText(value).toLowerCase();
    return text === "" || text === "true" || text === "false" || text === "1" || text === "0" || text === "yes" || text === "no" || text === "checked" || text === "unchecked" || text === "x";
  }

  function parseTrackingCellValue(value) {
    const text = normalizeText(value).toLowerCase();
    return text === "true" || text === "1" || text === "yes" || text === "checked" || text === "x";
  }

  function measureTextWidth(text, font) {
    const context = textMeasureCanvas.getContext("2d");
    if (!context) {
      return normalizeText(text).length * 8;
    }
    context.font = font || "12px Segoe UI";
    return context.measureText(text).width;
  }

  function clampWidth(width) {
    return Math.max(0, Math.min(MAX_COLUMN_WIDTH, width));
  }

  function humanizeStatus(value) {
    const text = normalizeText(value).toLowerCase();
    if (!text) return "Idle";
    return text
      .replace(/_/g, " ")
      .replace(/\b\w/g, (ch) => ch.toUpperCase());
  }

  function isLikelyUrl(value) {
    const text = normalizeText(value);
    if (!text) return false;
    return /^https?:\/\//i.test(text);
  }

  function shortenUrl(url) {
    const text = normalizeText(url);
    if (!text) return "";
    try {
      const parsed = new URL(text);
      const host = normalizeText(parsed.hostname || "").replace(/^www\./i, "");
      const path = normalizeText(parsed.pathname || "");
      const query = normalizeText(parsed.search || "");
      const compact = `${host}${path}${query}`;
      return compact.length > 56 ? `${compact.slice(0, 53)}...` : compact;
    } catch (_error) {
      return text.length > 56 ? `${text.slice(0, 53)}...` : text;
    }
  }

  function setError(text) {
    el.errorText.textContent = normalizeText(text);
  }

  function clearError() {
    el.errorText.textContent = "";
  }

  async function copyEmailValue(button, value) {
    const copied = await copyTextToClipboard(value);
    if (!copied) {
      setError("Could not copy email.");
      return;
    }

    clearError();
    flashCopiedState(button, value);
  }

  async function copyTextToClipboard(text) {
    const value = normalizeText(text);
    if (!value) return false;

    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
        await navigator.clipboard.writeText(value);
        return true;
      }
    } catch (_error) {
      // Fall back to execCommand below.
    }

    try {
      const helper = document.createElement("textarea");
      helper.value = value;
      helper.setAttribute("readonly", "readonly");
      helper.style.position = "fixed";
      helper.style.top = "-9999px";
      helper.style.left = "-9999px";
      document.body.appendChild(helper);
      helper.focus();
      helper.select();
      const copied = document.execCommand("copy");
      document.body.removeChild(helper);
      return copied === true;
    } catch (_error) {
      return false;
    }
  }

  function flashCopiedState(button, value) {
    if (!button) return;
    const baseText = normalizeText(value);
    const resetState = () => {
      button.classList.remove("copied");
      button.textContent = baseText;
      button.title = `${baseText} (click to copy)`;
      delete button.dataset.copyTimer;
    };

    if (button.dataset.copyTimer) {
      clearTimeout(Number(button.dataset.copyTimer));
    }

    button.classList.add("copied");
    button.textContent = "Copied";
    button.title = `Copied: ${baseText}`;
    const timerId = window.setTimeout(resetState, 1200);
    button.dataset.copyTimer = String(timerId);
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

  function sendRuntimeMessage(message) {
    return new Promise((resolve, reject) => {
      chrome.runtime.sendMessage(message, (response) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message || "Runtime request failed"));
          return;
        }
        resolve(response);
      });
    });
  }

  function defaultFilename() {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    const stamp = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
    return `gbp_export_${stamp}.csv`;
  }
})();
