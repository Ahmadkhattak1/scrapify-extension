(function () {
  const shared = window.GbpShared;
  const { MSG, CSV_COLUMNS, COLUMN_LABELS, sanitizeColumns, normalizeText } = shared;
  const MAX_RENDER_ROWS = 1000;
  const URL_COLUMNS = new Set(["website", "maps_url", "source_url"]);
  const EMAIL_COLUMNS = new Set(["email", "owner_email", "contact_email", "primary_email"]);
  const requestedRunId = readRequestedRunId();

  const el = {
    metaText: document.getElementById("metaText"),
    refreshBtn: document.getElementById("refreshBtn"),
    exportBtn: document.getElementById("exportBtn"),
    rowsCount: document.getElementById("rowsCount"),
    colsCount: document.getElementById("colsCount"),
    renderHint: document.getElementById("renderHint"),
    statusChip: document.getElementById("statusChip"),
    emptyState: document.getElementById("emptyState"),
    tableWrap: document.getElementById("tableWrap"),
    tableHead: document.getElementById("tableHead"),
    tableBody: document.getElementById("tableBody"),
    errorText: document.getElementById("errorText")
  };

  let rows = [];
  let selectedColumns = [...CSV_COLUMNS];
  let scrapeSession = null;
  let enrichSession = null;
  let enrichRuntimeState = null;

  init().catch((error) => {
    setError(error && error.message ? error.message : "Failed to load results");
  });

  async function init() {
    bindEvents();
    await refreshState();
    chrome.storage.onChanged.addListener(onStorageChanged);
  }

  function bindEvents() {
    el.refreshBtn.addEventListener("click", () => {
      void refreshState();
    });
    el.exportBtn.addEventListener("click", () => {
      void exportRows();
    });
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
        render();
      });
    }

    if (shouldRender) render();
  }

  function render() {
    const columns = getDisplayColumns();
    const renderRows = rows.slice(0, MAX_RENDER_ROWS);
    const hasRows = rows.length > 0;
    const statusInfo = deriveStatus();

    el.rowsCount.textContent = `${rows.length} row(s)`;
    el.colsCount.textContent = `${columns.length} column(s)`;
    el.renderHint.textContent = rows.length > MAX_RENDER_ROWS ? `Showing first ${MAX_RENDER_ROWS}` : "";
    if (el.statusChip) {
      el.statusChip.textContent = statusInfo.label;
      el.statusChip.className = `status-chip ${statusInfo.tone}`;
    }
    el.exportBtn.disabled = rows.length === 0 || columns.length === 0;
    el.metaText.textContent = buildMetaText(statusInfo.rawStatus);

    if (!hasRows) {
      el.emptyState.classList.remove("hidden");
      el.tableWrap.classList.add("hidden");
      el.tableHead.textContent = "";
      el.tableBody.textContent = "";
      return;
    }

    el.emptyState.classList.add("hidden");
    el.tableWrap.classList.remove("hidden");

    const headRow = document.createElement("tr");
    const rowNumHead = document.createElement("th");
    rowNumHead.textContent = "#";
    rowNumHead.className = "row-index";
    headRow.appendChild(rowNumHead);

    for (const column of columns) {
      const th = document.createElement("th");
      th.textContent = COLUMN_LABELS[column] || column;
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
        renderCell(td, column, row && row[column]);
        tr.appendChild(td);
      }
      fragment.appendChild(tr);
    }
    el.tableBody.textContent = "";
    el.tableBody.appendChild(fragment);
  }

  function renderCell(td, column, value) {
    const clean = normalizeCell(value);

    if (!clean) {
      td.classList.add("empty-cell");
      td.textContent = "—";
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
      link.textContent = shortenUrl(clean);
      td.appendChild(link);
      return;
    }

    if (EMAIL_COLUMNS.has(column) && clean.includes("@")) {
      const link = document.createElement("a");
      link.className = "cell-link";
      link.href = `mailto:${clean}`;
      link.title = clean;
      link.textContent = clean.length > 56 ? `${clean.slice(0, 53)}...` : clean;
      td.appendChild(link);
      return;
    }

    td.textContent = clean;
  }

  function deriveStatus() {
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
    const normalized = sanitizeColumns(Array.isArray(selectedColumns) ? selectedColumns : []);
    if (normalized.length > 0) return normalized;

    if (rows.length > 0) {
      const keys = Object.keys(rows[0] || {});
      const fromRow = sanitizeColumns(keys);
      if (fromRow.length > 0) return fromRow;
    }

    return [...CSV_COLUMNS];
  }

  async function exportRows() {
    clearError();
    const columns = getDisplayColumns();
    if (rows.length === 0) {
      setError("No rows to export.");
      return;
    }
    if (columns.length === 0) {
      setError("No export columns selected.");
      return;
    }

    try {
      const response = await sendRuntimeMessage({
        type: MSG.EXPORT_CSV,
        rows,
        columns: sanitizeColumns(columns),
        filename: defaultFilename()
      });
      if (!response || response.type !== MSG.EXPORT_DONE) {
        throw new Error((response && response.error) || "CSV export failed");
      }
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
