(function () {
  if (window.__GBP_MAPS_SCRAPER_BOOTSTRAPPED__ === true) {
    return;
  }
  window.__GBP_MAPS_SCRAPER_BOOTSTRAPPED__ = true;

  const shared = window.GbpShared;
  const {
    MSG,
    DEFAULT_MAX_ROWS,
    normalizeText,
    normalizePhoneText,
    parseFlexibleNumber,
    parseRating,
    parseReviewCount,
    normalizeMapsUrl,
    normalizeBusinessWebsiteUrl,
    dedupeKey,
    applyFilters
  } = shared;
  const SCRAPE_SESSION_KEY = "scrapeSession";
  const ROW_SNAPSHOT_INTERVAL = 8;

  const state = {
    isRunning: false,
    stopRequested: false,
    runId: "",
    runTabId: null,
    runStartedAtIso: "",
    runInfiniteScroll: false,
    activeFilters: {},
    sourceQuery: "",
    sourceUrl: "",
    lastProgressPersistAtMs: 0,
    persistedRowsCount: 0,
    seenCardKeys: new Set(),
    seenKeys: new Set(),
    websiteHostOwners: new Map(),
    rows: [],
    startedAtMs: 0,
    seenListings: 0,
    seenRatingSum: 0,
    seenRatingCount: 0,
    seenReviewsSum: 0,
    seenReviewsCount: 0,
    processed: 0,
    matched: 0,
    duplicates: 0,
    fastSkipped: 0,
    errors: 0
  };

  chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (!message || !message.type) {
      return false;
    }

    if (message.type === MSG.START_SCRAPE) {
      if (state.isRunning) {
        sendResponse({ ok: false, error: "Scrape already running" });
        return false;
      }

      const incomingConfig = message.config || {};
      const senderTabId = sender && sender.tab && sender.tab.id != null ? sender.tab.id : null;
      if (incomingConfig.runTabId == null && senderTabId != null) {
        incomingConfig.runTabId = senderTabId;
      }

      runScrape(incomingConfig)
        .then((result) => {
          sendResponse({ ok: true, result });
        })
        .catch((error) => {
          sendResponse({ ok: false, error: error && error.message ? error.message : "Scrape failed" });
        });

      return true;
    }

    if (message.type === MSG.STOP_SCRAPE) {
      state.stopRequested = true;
      persistScrapeSession({ status: "stopping", force: true });
      sendResponse({ ok: true });
      return false;
    }

    if (message.type === MSG.GET_SCRAPE_STATE) {
      sendResponse({
        ok: true,
        type: MSG.SCRAPE_STATE,
        state: getScrapeRuntimeState()
      });
      return false;
    }

    return false;
  });

  async function runScrape(config) {
    resetState();
    state.isRunning = true;
    state.runId = createRunId(config.runId);
    state.runTabId = Number.isFinite(Number(config.runTabId)) ? Number(config.runTabId) : null;
    state.runStartedAtIso = new Date().toISOString();

    const maxRows = Number(config.maxRows) > 0 ? Number(config.maxRows) : DEFAULT_MAX_ROWS;
    const infiniteScroll = Boolean(config.infiniteScroll);
    state.runInfiniteScroll = infiniteScroll;
    const filters = normalizeRuntimeFilters(config.filters || {});
    state.activeFilters = { ...filters };
    const sourceQuery = getCurrentQuery();
    const sourceUrl = window.location.href;
    state.sourceQuery = normalizeText(sourceQuery);
    state.sourceUrl = normalizeMapsUrl(sourceUrl);

    persistScrapeSession({
      status: "running",
      infinite_scroll: infiniteScroll,
      filters: state.activeFilters,
      force: true
    });

    try {
      let feed = await ensureResultsFeedReady(findResultsFeed(), 2600, { attemptBack: true });
      if (!feed) {
        throw new Error("Could not find Google Maps results list. Open a search results page first.");
      }

      let noNewCardsScrolls = 0;

      while (!state.stopRequested) {
        const resolvedFeed = await ensureResultsFeedReady(feed, 2600, { attemptBack: true });
        if (!resolvedFeed) {
          throw new Error("Could not return to Google Maps results list. Keep the results panel open and try again.");
        }
        feed = resolvedFeed;

        const cards = getResultCards(feed);
        const unseenCards = [];

        for (const card of cards) {
          const cardKey = getCardIdentity(card);
          if (!cardKey || state.seenCardKeys.has(cardKey)) continue;
          state.seenCardKeys.add(cardKey);
          unseenCards.push(card);
        }

        if (unseenCards.length === 0) {
          await scrollResults(feed);
          await sleep(700);

          const scrolledFeed = await ensureResultsFeedReady(feed, 1200, { attemptBack: false });
          if (scrolledFeed) {
            feed = scrolledFeed;
          }

          const hasAnyNewCards = getResultCards(feed).some((card) => {
            const cardKey = getCardIdentity(card);
            return cardKey && !state.seenCardKeys.has(cardKey);
          });

          if (hasAnyNewCards) {
            noNewCardsScrolls = 0;
          } else {
            noNewCardsScrolls += 1;
          }

          if (noNewCardsScrolls >= 10) {
            break;
          }
          continue;
        }

        noNewCardsScrolls = 0;

        for (const card of unseenCards) {
          if (state.stopRequested) break;
          state.processed += 1;

          const quickData = buildQuickCardData(card);
          const seenCapture = updateSeenStats(quickData);

          if (!quickCardPassesFilter(card, quickData, filters)) {
            state.fastSkipped += 1;
            sendProgress();
            continue;
          }

          try {
            const row = mergeQuickMetricsIntoRow(
              await processCard(card, sourceQuery, sourceUrl),
              quickData
            );
            backfillSeenStatsFromRow(row, seenCapture);

            if (row && applyFilters(row, filters)) {
              const guardedRow = applyWebsiteOwnershipGuard(row);
              const key = dedupeKey(guardedRow);
              if (key) {
                if (state.seenKeys.has(key)) {
                  state.duplicates += 1;
                } else {
                  state.seenKeys.add(key);
                  state.rows.push(guardedRow);
                  state.matched += 1;
                }
              } else {
                state.rows.push(guardedRow);
                state.matched += 1;
              }
            }

            sendProgress();

            if (!infiniteScroll && state.rows.length >= maxRows) {
              break;
            }
          } catch (_cardErr) {
            state.errors += 1;
            sendProgress();
          }

          await sleep(250);
          const refreshedFeed = await ensureResultsFeedReady(feed, 1400, { attemptBack: true });
          if (refreshedFeed) {
            feed = refreshedFeed;
          }
        }

        if (!infiniteScroll && state.rows.length >= maxRows) {
          break;
        }
      }

      if (hasAnyActiveFilter(filters) && state.rows.length > 0) {
        const finalFilteredRows = state.rows.filter((row) => applyFilters(row, filters));
        if (finalFilteredRows.length !== state.rows.length) {
          state.rows = finalFilteredRows;
          state.matched = finalFilteredRows.length;
        }
      }

      const summary = {
        processed: state.processed,
        matched: state.matched,
        duplicates: state.duplicates,
        fast_skipped: state.fastSkipped,
        ...getPerformanceStats(),
        errors: state.errors,
        stopped: state.stopRequested
      };

      persistScrapeSession({
        status: summary.stopped ? "stopped" : "done",
        summary,
        filters: state.activeFilters,
        rows: state.rows,
        force: true
      });

      chrome.runtime.sendMessage({
        type: MSG.SCRAPE_DONE,
        run_id: state.runId,
        tab_id: state.runTabId,
        rows: state.rows,
        summary,
        filters: state.activeFilters
      });

      return { rows: state.rows, summary };
    } catch (error) {
      persistScrapeSession({
        status: "error",
        error: error && error.message ? error.message : "Unexpected scrape error",
        rows: state.rows,
        force: true
      });
      chrome.runtime.sendMessage({
        type: MSG.SCRAPE_ERROR,
        run_id: state.runId,
        tab_id: state.runTabId,
        error: error && error.message ? error.message : "Unexpected scrape error"
      });
      throw error;
    } finally {
      state.isRunning = false;
    }
  }

  async function processCard(card, sourceQuery, sourceUrl) {
    const fallbackRow = extractBusinessRowFromCard(card, sourceQuery, sourceUrl);
    const expectedIdentity = getExpectedDetailIdentity(card, fallbackRow);

    safeClick(card);
    await sleep(700);
    let detailMatched = await waitForDetails(expectedIdentity, 3200);
    if (!detailMatched) {
      safeClick(card);
      await sleep(750);
      detailMatched = await waitForDetails(expectedIdentity, 3200);
    }
    if (!detailMatched && fallbackRow) {
      return fallbackRow;
    }

    const detailRow = extractBusinessRow(sourceQuery, sourceUrl);
    if (
      detailRow &&
      expectedIdentity &&
      expectedIdentity.hasIdentity === true &&
      !isRowMatchingExpected(detailRow, expectedIdentity)
    ) {
      return fallbackRow || detailRow;
    }

    if (detailRow && fallbackRow) {
      return mergeRows(detailRow, fallbackRow);
    }
    return detailRow || fallbackRow;
  }

  function extractBusinessRow(sourceQuery, sourceUrl) {
    const name =
      textFrom("h1.DUwDvf") ||
      textFrom("h1") ||
      textFrom("[role='main'] h1");

    if (!name) return null;

    const ratingText =
      attrFrom("div.F7nice span[aria-hidden='true']", "textContent") ||
      attrFrom("span.ceNzKf", "textContent") ||
      attrFrom("[role='main'] span[aria-label*='star' i]", "aria-label") ||
      attrFrom("[role='main'] span[aria-label*='rating' i]", "aria-label") ||
      "";

    const reviewsText =
      attrFrom("button[aria-label*='review' i]", "aria-label") ||
      attrFrom("span[aria-label*='review' i]", "aria-label") ||
      attrFrom("button[jsaction*='pane.reviewChart.moreReviews']", "aria-label") ||
      textFrom("div.F7nice span[aria-label*='review' i]") ||
      textFrom("span[aria-label*='reviews' i]") ||
      textFrom("[role='main'] span[aria-label*='review' i]") ||
      textFrom("div.F7nice") ||
      "";
    const category =
      textFrom("button.DkEaL") ||
      textFrom("button[jsaction*='category']") ||
      firstChipsText();

    const address =
      textFrom("button[data-item-id='address']") ||
      textFrom("button[aria-label^='Address']") ||
      textFrom("button[aria-label*='Address:']");

    const phone =
      textFrom("button[data-item-id^='phone:tel']") ||
      textFrom("button[aria-label^='Phone']") ||
      textFrom("button[aria-label*='Phone:']");
    const listingPhone = sanitizePhoneText(normalizeFieldValue(phone, "Phone"));

    const websiteHref = extractWebsiteFromDetailPanel();

    const hours =
      textFrom("div.t39EBf") ||
      textFrom("table.eK4R0e") ||
      textFrom("div[aria-label*='Hours']");

    const mapsUrl = normalizeMapsUrl(window.location.href);
    const placeId = parsePlaceIdFromUrl(mapsUrl);
    const parsedRating =
      parseRating(ratingText) !== "" ? parseRating(ratingText)
      : parseRating(reviewsText) !== "" ? parseRating(reviewsText)
      : "";
    const parsedReviewCount =
      parseReviewCount(reviewsText) !== "" ? parseReviewCount(reviewsText)
      : parseReviewCount(ratingText) !== "" ? parseReviewCount(ratingText)
      : "";

    return {
      place_id: normalizeText(placeId),
      name: normalizeText(name),
      rating: parsedRating,
      review_count: parsedReviewCount,
      category: normalizeText(category),
      address: normalizeFieldValue(address, "Address"),
      phone: listingPhone,
      listing_phone: listingPhone,
      website_phone: "",
      website_phone_source: "",
      website: websiteHref,
      email: "",
      owner_name: "",
      owner_title: "",
      owner_email: "",
      contact_email: "",
      primary_email: "",
      primary_email_type: "",
      primary_email_source: "",
      website_scan_status: websiteHref ? "not_requested" : "no_website",
      site_pages_visited: 0,
      site_pages_discovered: 0,
      social_pages_scanned: 0,
      social_links: "",
      discovery_status: "not_requested",
      discovery_source: "",
      discovery_query: "",
      discovered_website: "",
      hours: normalizeText(hours),
      maps_url: mapsUrl,
      source_query: normalizeText(sourceQuery),
      source_url: normalizeMapsUrl(sourceUrl),
      scraped_at: new Date().toISOString()
    };
  }

  function normalizeFieldValue(value, prefix) {
    const text = normalizeText(value);
    if (!text) return "";
    const normalizedPrefix = `${prefix.toLowerCase()}:`;
    if (text.toLowerCase().startsWith(normalizedPrefix)) {
      return text.slice(normalizedPrefix.length).trim();
    }
    return text;
  }

  function sanitizePhoneText(value) {
    return normalizePhoneText(value);
  }

  function extractBusinessRowFromCard(card, sourceQuery, sourceUrl) {
    if (!card) return null;
    const link = resolveCardLink(card);
    const cardText = normalizeText(card.textContent || "");
    const quickData = buildQuickCardData(card);
    const lines = cardText.split(/\n+/).map((line) => normalizeText(line)).filter(Boolean);
    const websiteFromCard = extractWebsiteFromCard(card);
    const phoneFromCard = lines.map((line) => sanitizePhoneText(line)).find(Boolean) || "";

    const name =
      normalizeText((link && link.getAttribute("aria-label")) || "") ||
      lines[0] ||
      "";

    if (!name) return null;

    const mapsUrl = normalizeMapsUrl((link && (link.href || link.getAttribute("href"))) || window.location.href);
    const placeId = parsePlaceIdFromUrl(mapsUrl);

    return {
      place_id: normalizeText(placeId),
      name: normalizeText(name),
      rating: quickData.rating !== "" ? quickData.rating : parseRating(cardText),
      review_count: quickData.reviews !== "" ? quickData.reviews : parseReviewCount(cardText),
      category: lines[1] || "",
      address: "",
      phone: phoneFromCard,
      listing_phone: phoneFromCard,
      website_phone: "",
      website_phone_source: "",
      website: websiteFromCard,
      email: "",
      owner_name: "",
      owner_title: "",
      owner_email: "",
      contact_email: "",
      primary_email: "",
      primary_email_type: "",
      primary_email_source: "",
      website_scan_status: websiteFromCard ? "not_requested" : "no_website",
      site_pages_visited: 0,
      site_pages_discovered: 0,
      social_pages_scanned: 0,
      social_links: "",
      discovery_status: "not_requested",
      discovery_source: "",
      discovery_query: "",
      discovered_website: "",
      hours: "",
      maps_url: mapsUrl,
      source_query: normalizeText(sourceQuery),
      source_url: normalizeMapsUrl(sourceUrl),
      scraped_at: new Date().toISOString()
    };
  }

  function extractWebsiteFromCard(card) {
    if (!card) return "";

    const cardRoot = card.closest("[role='article']") || card;
    const roots = [cardRoot, card].filter(Boolean);
    const selectors = [
      "a[data-item-id*='authority'][href]",
      "a[data-item-id*='website'][href]",
      "a[aria-label^='Website'][href]",
      "a[aria-label*='Website:'][href]"
    ];

    for (const root of roots) {
      for (const selector of selectors) {
        const nodes = Array.from(root.querySelectorAll(selector)).slice(0, 40);
        for (const node of nodes) {
          const href = normalizeBusinessWebsiteUrl((node.getAttribute && node.getAttribute("href")) || node.href || "");
          if (isValidWebsiteLink(href)) {
            return href;
          }
          const textHit = findWebsiteInText(
            `${normalizeText(node.textContent || "")} ${normalizeText((node.getAttribute && node.getAttribute("aria-label")) || "")}`
          );
          if (textHit) {
            return textHit;
          }
        }
      }
    }

    return "";
  }

  function getExpectedDetailIdentity(card, fallbackRow) {
    const row = fallbackRow && typeof fallbackRow === "object" ? fallbackRow : {};
    const link = resolveCardLink(card);
    const href = normalizeMapsUrl((link && (link.href || link.getAttribute("href"))) || row.maps_url || "");
    const placeId = normalizeText(parsePlaceIdFromUrl(href) || row.place_id);
    const cardName =
      normalizeText((link && link.getAttribute("aria-label")) || "") ||
      normalizeText(row.name) ||
      normalizeText(card && card.getAttribute && card.getAttribute("aria-label"));

    const normalizedName = normalizeNameForMatch(cardName);
    return {
      href,
      placeId,
      name: normalizedName,
      hasIdentity: Boolean(href || placeId || normalizedName)
    };
  }

  function isRowMatchingExpected(row, expectedIdentity) {
    if (!row || !expectedIdentity) return true;
    if (expectedIdentity.hasIdentity !== true) return true;

    const rowPlaceId = normalizeText(row.place_id);
    if (expectedIdentity.placeId && rowPlaceId) {
      return expectedIdentity.placeId === rowPlaceId;
    }

    const rowMapsUrl = normalizeMapsUrl(row.maps_url || window.location.href);
    const rowSlug = mapsPlaceSlug(rowMapsUrl);
    const expectedSlug = mapsPlaceSlug(expectedIdentity.href);
    if (expectedSlug && rowSlug && expectedSlug === rowSlug) {
      return true;
    }

    const rowName = normalizeNameForMatch(row.name);
    if (expectedIdentity.name && rowName) {
      if (rowName === expectedIdentity.name) return true;
      if (rowName.includes(expectedIdentity.name) || expectedIdentity.name.includes(rowName)) return true;
    }

    return false;
  }

  function extractWebsiteFromDetailPanel() {
    const linkSelectors = [
      "a[data-item-id='authority']",
      "a[data-item-id*='authority']",
      "a[data-item-id*='website']",
      "a[aria-label^='Website']",
      "a[aria-label*='Website:']",
      "a[aria-label*='website']",
      "[role='main'] a[href^='http'][aria-label*='Website']",
      "[role='main'] a[jsaction*='authority'][href]"
    ];

    for (const selector of linkSelectors) {
      const nodes = Array.from(document.querySelectorAll(selector));
      for (const node of nodes) {
        const href = normalizeBusinessWebsiteUrl((node.getAttribute && node.getAttribute("href")) || node.href || "");
        if (isValidWebsiteLink(href)) return href;

        const textHit = findWebsiteInText(
          `${normalizeText(node.textContent || "")} ${normalizeText((node.getAttribute && node.getAttribute("aria-label")) || "")}`
        );
        if (textHit) return textHit;
      }
    }

    const textSelectors = [
      "button[data-item-id='authority']",
      "button[data-item-id*='authority']",
      "button[data-item-id*='website']",
      "button[aria-label^='Website']",
      "button[aria-label*='Website:']",
      "[role='main'] [aria-label*='Website']"
    ];

    for (const selector of textSelectors) {
      const nodes = Array.from(document.querySelectorAll(selector));
      for (const node of nodes) {
        const textHit = findWebsiteInText(
          `${normalizeText(node.textContent || "")} ${normalizeText((node.getAttribute && node.getAttribute("aria-label")) || "")}`
        );
        if (textHit) return textHit;
      }
    }

    return "";
  }

  function findWebsiteInText(text) {
    const raw = normalizeText(text);
    if (!raw) return "";

    const pattern = /(?:https?:\/\/)?(?:www\.)?[a-z0-9][a-z0-9.-]+\.[a-z]{2,}(?:\/[^\s<>()\[\]{}"']*)?/gi;
    let match = pattern.exec(raw);
    while (match) {
      const matchedText = normalizeText(match[0]);
      const startIndex = Number(match.index || 0);
      const prevChar = startIndex > 0 ? raw.charAt(startIndex - 1) : "";
      if (prevChar === "@") {
        match = pattern.exec(raw);
        continue;
      }
      const candidate = matchedText.replace(/[),.;]+$/, "");
      if (!candidate || candidate.includes("@")) {
        match = pattern.exec(raw);
        continue;
      }
      const normalized = normalizeBusinessWebsiteUrl(candidate);
      if (isValidWebsiteLink(normalized)) {
        return normalized;
      }
      match = pattern.exec(raw);
    }

    return "";
  }

  function isValidWebsiteLink(url) {
    const normalized = normalizeBusinessWebsiteUrl(url);
    if (!normalized) return false;

    try {
      const parsed = new URL(normalized);
      const host = normalizeText(parsed.hostname).toLowerCase();
      if (!host) return false;
      if (host.includes("google.")) return false;
      return true;
    } catch (_error) {
      return false;
    }
  }

  function normalizeWebsiteHost(url) {
    const normalized = normalizeBusinessWebsiteUrl(url);
    if (!normalized) return "";
    try {
      const parsed = new URL(normalized);
      const host = normalizeText(parsed.hostname).toLowerCase().replace(/^www\./, "");
      return host;
    } catch (_error) {
      return "";
    }
  }

  function normalizeBusinessNameKey(name) {
    const tokens = normalizeText(name)
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((token) => token.length >= 2);
    if (tokens.length === 0) return "";

    const stop = new Set(["the", "and", "of", "llc", "inc", "ltd", "co", "company", "services", "service"]);
    return tokens.filter((token) => !stop.has(token)).join(" ");
  }

  function areLikelySameBusinessName(nameA, nameB) {
    const left = normalizeBusinessNameKey(nameA);
    const right = normalizeBusinessNameKey(nameB);
    if (!left || !right) return false;
    if (left === right) return true;

    const leftTokens = left.split(/\s+/);
    const rightTokens = right.split(/\s+/);
    const leftSet = new Set(leftTokens);
    const rightSet = new Set(rightTokens);
    let overlap = 0;
    for (const token of leftSet) {
      if (rightSet.has(token)) overlap += 1;
    }
    const union = new Set([...leftSet, ...rightSet]).size;
    if (union === 0) return false;
    const jaccard = overlap / union;
    return jaccard >= 0.75;
  }

  function applyWebsiteOwnershipGuard(row) {
    if (!row || typeof row !== "object") return row;
    const website = normalizeBusinessWebsiteUrl(row.website);
    if (!website) {
      row.website = "";
      if (!normalizeText(row.website_scan_status)) {
        row.website_scan_status = "no_website";
      }
      return row;
    }

    const host = normalizeWebsiteHost(website);
    if (!host) {
      row.website = "";
      row.website_scan_status = "no_website";
      return row;
    }

    const placeId = normalizeText(row.place_id);
    const mapsUrl = normalizeMapsUrl(row.maps_url || "");
    const businessName = normalizeText(row.name);
    const existing = state.websiteHostOwners.get(host);

    if (!existing) {
      state.websiteHostOwners.set(host, {
        placeIds: new Set(placeId ? [placeId] : []),
        mapsUrls: new Set(mapsUrl ? [mapsUrl] : []),
        names: new Set(businessName ? [businessName] : []),
        primaryName: businessName
      });
      row.website = website;
      row.website_scan_status = "not_requested";
      return row;
    }

    const sameIdentity =
      (placeId && existing.placeIds.has(placeId)) ||
      (mapsUrl && existing.mapsUrls.has(mapsUrl)) ||
      (businessName && existing.primaryName && areLikelySameBusinessName(businessName, existing.primaryName));

    if (!sameIdentity) {
      row.website = "";
      row.website_scan_status = "no_website";
      return row;
    }

    if (placeId) existing.placeIds.add(placeId);
    if (mapsUrl) existing.mapsUrls.add(mapsUrl);
    if (businessName) existing.names.add(businessName);
    row.website = website;
    row.website_scan_status = "not_requested";
    return row;
  }

  function mergeRows(primary, fallback) {
    if (!fallback) return primary;
    const merged = { ...primary };
    for (const key of Object.keys(fallback)) {
      if (!hasRowValue(merged[key])) {
        merged[key] = fallback[key];
      }
    }
    return merged;
  }

  function mergeQuickMetricsIntoRow(row, quickData) {
    if (!row || typeof row !== "object") return row;
    const merged = { ...row };

    // Card-level metrics are tied to the specific listing card and are safer than
    // broad detail-panel text fallbacks, so prefer them whenever available.
    if (quickData && quickData.rating !== "") {
      merged.rating = quickData.rating;
    }

    if (quickData && quickData.reviews !== "") {
      merged.review_count = quickData.reviews;
    }

    return merged;
  }

  function hasRowValue(value) {
    if (value == null) return false;
    if (typeof value === "number") return Number.isFinite(value);
    return normalizeText(value) !== "";
  }

  function resolveCardLink(card) {
    if (!card) return null;
    if (card.tagName === "A") return card;
    const selectors = [
      "a.hfpxzc[href]",
      "a[href*='/maps/place/']",
      "a[href*='/maps/search/']",
      "a[href*='?cid=']",
      "a[href]"
    ];
    for (const selector of selectors) {
      const found = card.closest(selector) || card.querySelector(selector);
      if (found) return found;
    }
    return null;
  }

  function getCardIdentity(card) {
    if (!card) return "";
    const link = resolveCardLink(card);
    const href = normalizeMapsUrl((link && (link.href || link.getAttribute("href"))) || "");
    const placeId = normalizeText(parsePlaceIdFromUrl(href));
    const label = normalizeText(
      (link && link.getAttribute("aria-label")) ||
      card.getAttribute("aria-label") ||
      ""
    );
    const dataId =
      normalizeText(card.getAttribute("data-result-id")) ||
      normalizeText(card.getAttribute("data-cid")) ||
      normalizeText(card.getAttribute("data-place-id")) ||
      normalizeText(card.getAttribute("jslog")) ||
      normalizeText(link && link.getAttribute("data-result-id")) ||
      normalizeText(link && link.getAttribute("data-cid")) ||
      normalizeText(link && link.getAttribute("data-place-id")) ||
      normalizeText(link && link.getAttribute("jslog"));
    const textSnippet = normalizeText(getCardText(card)).slice(0, 140).toLowerCase();

    if (placeId) return `place:${placeId.toLowerCase()}`;
    if (href) return `url:${href.toLowerCase()}`;
    if (dataId) return `data:${dataId.toLowerCase()}`;
    if (label) return `label:${label.toLowerCase()}`;
    if (textSnippet) return `text:${textSnippet}`;
    return "";
  }

  function parsePlaceIdFromUrl(url) {
    const raw = normalizeText(url);
    if (!raw) return "";

    const cidMatch = raw.match(/[?&]cid=(\d+)/i);
    if (cidMatch && cidMatch[1]) return cidMatch[1];

    const dataCidMatch = raw.match(/!1s0x[\da-f]+:0x([\da-f]+)/i);
    if (dataCidMatch && dataCidMatch[1]) return dataCidMatch[1];

    return "";
  }

  function quickCardPassesFilter(card, quickData, filters) {
    const f = filters || {};
    const cardText = quickData && quickData.text ? quickData.text : "";
    if (!cardText) return true;

    const quickRating = parseFlexibleNumber(quickData && quickData.rating);
    const quickReviews = parseFlexibleNumber(quickData && quickData.reviews);
    const minRating = parseFlexibleNumber(f.minRating);
    const maxRating = parseFlexibleNumber(f.maxRating);
    const minReviews = parseFlexibleNumber(f.minReviews);
    const maxReviews = parseFlexibleNumber(f.maxReviews);

    if (minRating !== "" && quickRating !== "" && quickRating < minRating) {
      return false;
    }
    if (maxRating !== "" && quickRating !== "" && quickRating > maxRating) {
      return false;
    }
    if (minReviews !== "" && quickReviews !== "" && quickReviews < minReviews) {
      return false;
    }
    if (maxReviews !== "" && quickReviews !== "" && quickReviews > maxReviews) {
      return false;
    }

    const lower = quickData.lower;
    if (normalizeText(f.nameKeyword) !== "" && !lower.includes(normalizeText(f.nameKeyword).toLowerCase())) {
      return false;
    }
    if (normalizeText(f.categoryInclude) !== "" && !lower.includes(normalizeText(f.categoryInclude).toLowerCase())) {
      return false;
    }
    if (normalizeText(f.categoryExclude) !== "" && lower.includes(normalizeText(f.categoryExclude).toLowerCase())) {
      return false;
    }

    return true;
  }

  function getCardText(card) {
    if (!card) return "";
    const link = resolveCardLink(card);
    const linkLabel = normalizeText((link && link.getAttribute("aria-label")) || "");
    const rawText = normalizeText(card.textContent || "");
    return normalizeText(`${linkLabel} ${rawText}`);
  }

  function buildQuickCardData(card) {
    const text = getCardText(card);
    return {
      text,
      lower: text.toLowerCase(),
      rating: parseCardRating(card, text),
      reviews: parseCardReviewCount(card, text)
    };
  }

  function parseCardRating(card, text) {
    const candidates = [
      attrFromWithin(card, "span.MW4etd", "textContent"),
      attrFromWithin(card, "span[aria-label*='rating' i]", "aria-label"),
      attrFromWithin(card, "span[role='img'][aria-label*='star' i]", "aria-label"),
      attrFromWithin(card, "span[aria-label*='star' i]", "aria-label"),
      attrFromWithin(card, "span[aria-hidden='true']", "textContent"),
      normalizeText(text)
    ];

    for (const candidate of candidates) {
      const value = parseRatingFromStarContext(candidate);
      if (value !== "") return value;
    }

    return "";
  }

  function parseCardReviewCount(card, text) {
    const normalizedText = normalizeText(text);
    const candidates = [
      attrFromWithin(card, "span.UY7F9", "textContent"),
      attrFromWithin(card, "span[aria-label*='reviews' i]", "aria-label"),
      attrFromWithin(card, "button[aria-label*='review' i]", "aria-label"),
      attrFromWithin(card, "span[aria-label*='review' i]", "aria-label")
    ];
    if (
      /\breviews?\b/i.test(normalizedText) ||
      /\b[0-5](?:\.\d)?\s*(?:[·•]|\(\s*\d)/i.test(normalizedText)
    ) {
      candidates.push(normalizedText);
    }

    for (const candidate of candidates) {
      const value = parseReviewCountFromReviewContext(candidate);
      if (value !== "") return value;
    }

    return "";
  }

  function parseRatingFromStarContext(text) {
    const clean = normalizeText(text).replace(/,/g, ".");
    if (!clean) return "";

    const starPattern = clean.match(/([0-5](?:\.\d)?)\s*(?:stars?|★|⭐)/i);
    if (starPattern && starPattern[1]) {
      const value = Number(starPattern[1]);
      if (Number.isFinite(value) && value > 0 && value <= 5) return value;
    }

    const ratedPattern = clean.match(/rated\s*([0-5](?:\.\d)?)/i);
    if (ratedPattern && ratedPattern[1]) {
      const value = Number(ratedPattern[1]);
      if (Number.isFinite(value) && value > 0 && value <= 5) return value;
    }

    const compactPattern = clean.match(/\b([0-5](?:\.\d)?)\s*\(([\d,]+)\)/);
    if (compactPattern && compactPattern[1]) {
      const value = Number(compactPattern[1]);
      if (Number.isFinite(value) && value > 0 && value <= 5) return value;
    }

    const bulletPattern = clean.match(/\b([0-5](?:\.\d)?)\s*[·•]\s*([\d,]+)\b/);
    if (bulletPattern && bulletPattern[1]) {
      const value = Number(bulletPattern[1]);
      if (Number.isFinite(value) && value > 0 && value <= 5) return value;
    }

    const reviewsContextPattern = clean.match(/\b([0-5](?:\.\d)?)\s+[\d,]+\s+reviews?\b/i);
    if (reviewsContextPattern && reviewsContextPattern[1]) {
      const value = Number(reviewsContextPattern[1]);
      if (Number.isFinite(value) && value > 0 && value <= 5) return value;
    }

    return "";
  }

  function parseReviewCountFromReviewContext(text) {
    const clean = normalizeText(text);
    if (!clean) return "";

    const wordPattern = clean.match(/(\d[\d,.'’\u00A0\u202F\s]*[kmb]?)\s+reviews?\b/i);
    if (wordPattern && wordPattern[1]) {
      const value = parseAbbreviatedCount(wordPattern[1]);
      if (Number.isFinite(value)) return value;
    }

    const bulletPattern = clean.match(/\b([0-5](?:\.\d)?)\s*[·•]\s*(\d[\d,.'’\u00A0\u202F\s]*[kmb]?)\b/i);
    if (bulletPattern && bulletPattern[2]) {
      const value = parseAbbreviatedCount(bulletPattern[2]);
      if (Number.isFinite(value)) return value;
    }

    const compactPattern = clean.match(/\b([0-5](?:\.\d)?)\s*\((\d[\d,.'’\u00A0\u202F\s]*[kmb]?)\)/i);
    if (compactPattern && compactPattern[2]) {
      const value = parseAbbreviatedCount(compactPattern[2]);
      if (Number.isFinite(value)) return value;
    }

    const strictStandalonePattern = clean.match(/^\(?\s*(\d[\d,.'’\u00A0\u202F\s]*[kmb]?)\s*\)?$/i);
    if (strictStandalonePattern && strictStandalonePattern[1]) {
      const rawStandalone = normalizeText(strictStandalonePattern[1]);
      const standaloneDigits = rawStandalone.replace(/\D/g, "");
      const standaloneHasSuffix = /[kmb]$/i.test(rawStandalone);
      if (!standaloneHasSuffix && standaloneDigits.length > 7) {
        return "";
      }
      const value = parseAbbreviatedCount(strictStandalonePattern[1]);
      if (Number.isFinite(value)) return value;
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

    if (!suffix) return Math.round(base);

    const multiplier = suffix === "k" ? 1000 : suffix === "m" ? 1000000 : suffix === "b" ? 1000000000 : 1;
    const scaled = Math.round(base * multiplier);
    return Number.isFinite(scaled) ? scaled : "";
  }

  function attrFromWithin(root, selector, attrName) {
    if (!root || typeof root.querySelector !== "function") return "";
    const node = root.querySelector(selector);
    if (!node) return "";
    return normalizeText(node[attrName] || node.getAttribute(attrName) || "");
  }

  function findResultsFeed() {
    return findResultsFeedWithCards() ||
      document.querySelector("div[role='feed']") ||
      document.querySelector("div[aria-label*='Results' i]") ||
      document.querySelector("div.m6QErb.DxyBCb");
  }

  function findResultsFeedWithCards() {
    const feeds = Array.from(
      document.querySelectorAll("div[role='feed'], div[aria-label*='Results' i], div.m6QErb.DxyBCb")
    );
    for (const feed of feeds) {
      if (!feed || !document.contains(feed)) continue;
      if (getResultCards(feed).length > 0) {
        return feed;
      }
    }
    return null;
  }

  function getResultCards(feed) {
    if (!feed) return [];
    const selectors = [
      "div[role='article']",
      "div.Nv2PK",
      "a.hfpxzc",
      "div[role='article'] a[href*='/maps/place/']",
      "a[href*='/maps/place/']",
      "a[href*='/maps/search/']",
      "a[href*='?cid=']"
    ];

    for (const selector of selectors) {
      const nodes = Array.from(feed.querySelectorAll(selector)).map((node) => normalizeCardNode(node));
      const unique = dedupeNodes(nodes).filter(isVisible);
      if (unique.length > 0) {
        return unique;
      }
    }

    return [];
  }

  function normalizeCardNode(node) {
    if (!node || typeof node.closest !== "function") return node;
    return (
      node.closest("div[role='article']") ||
      node.closest("div.Nv2PK") ||
      node
    );
  }

  function dedupeNodes(nodes) {
    const seen = new Set();
    const out = [];
    for (const node of nodes) {
      if (!node) continue;
      const link = resolveCardLink(node);
      const href = normalizeMapsUrl((link && (link.href || link.getAttribute("href"))) || "");
      const label = normalizeText((link && link.getAttribute("aria-label")) || node.getAttribute("aria-label") || "");
      const key = href || label || normalizeText(node.textContent || "").slice(0, 160);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push(node);
    }
    return out;
  }

  function isVisible(node) {
    if (!node) return false;
    const rect = node.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  }

  async function scrollResults(feed) {
    if (!feed) return;
    const cards = getResultCards(feed);
    const lastCard = cards.length > 0 ? cards[cards.length - 1] : null;
    const before = feed.scrollTop;
    if (lastCard && typeof lastCard.scrollIntoView === "function") {
      lastCard.scrollIntoView({ block: "end", behavior: "auto" });
    }
    feed.scrollTop = feed.scrollTop + Math.max(600, Math.floor(feed.clientHeight * 0.8));
    await sleep(120);
    if (feed.scrollTop === before) {
      const wheel = new WheelEvent("wheel", { deltaY: 1200, bubbles: true, cancelable: true });
      feed.dispatchEvent(wheel);
      document.dispatchEvent(wheel);
    }
  }

  function safeClick(node) {
    if (!node) return;
    const target = resolveCardLink(node) || node;
    target.dispatchEvent(new MouseEvent("mousedown", { bubbles: true }));
    target.dispatchEvent(new MouseEvent("mouseup", { bubbles: true }));
    target.click();
  }

  async function waitForDetails(expectedIdentity, timeoutMs) {
    const timeoutAt = Date.now() + timeoutMs;
    while (Date.now() < timeoutAt) {
      const hasName = Boolean(document.querySelector("h1.DUwDvf") || document.querySelector("[role='main'] h1"));
      if (hasName && isDetailPanelMatch(expectedIdentity)) {
        return true;
      }
      await sleep(120);
    }
    return false;
  }

  async function ensureResultsFeedReady(feed, timeoutMs, options) {
    const limitMs = Number.isFinite(Number(timeoutMs)) && Number(timeoutMs) > 0 ? Number(timeoutMs) : 2200;
    const opts = options && typeof options === "object" ? options : {};
    const attemptBack = opts.attemptBack !== false;
    const deadline = Date.now() + limitMs;

    let resolvedFeed = resolveFeedWithCards(feed);
    if (resolvedFeed) return resolvedFeed;

    while (Date.now() < deadline) {
      if (attemptBack && isDetailPanelVisible()) {
        await attemptReturnToResultsList();
      }

      resolvedFeed = resolveFeedWithCards(feed);
      if (resolvedFeed) return resolvedFeed;
      await sleep(150);
    }

    return null;
  }

  function resolveFeedWithCards(preferredFeed) {
    if (preferredFeed && document.contains(preferredFeed) && getResultCards(preferredFeed).length > 0) {
      return preferredFeed;
    }
    return findResultsFeedWithCards();
  }

  function isDetailPanelVisible() {
    return Boolean(
      document.querySelector("h1.DUwDvf") ||
      document.querySelector("[role='main'] h1") ||
      document.querySelector("button[aria-label*='Back' i][jsaction*='back']")
    );
  }

  async function attemptReturnToResultsList() {
    const backSelectors = [
      "button[aria-label*='Back to results' i]",
      "button[jsaction*='pane.place.backToList']",
      "button[jsaction*='back']",
      "button[aria-label='Back']"
    ];

    for (const selector of backSelectors) {
      const candidates = Array.from(document.querySelectorAll(selector)).filter(isVisible);
      if (candidates.length === 0) continue;
      safeClick(candidates[0]);
      await sleep(450);
      if (findResultsFeedWithCards()) {
        return true;
      }
    }

    try {
      window.history.back();
    } catch (_error) {
      return false;
    }
    await sleep(500);
    return Boolean(findResultsFeedWithCards());
  }

  function isDetailPanelMatch(expectedIdentity) {
    if (!expectedIdentity || expectedIdentity.hasIdentity !== true) {
      return true;
    }

    const detailUrl = normalizeMapsUrl(window.location.href);
    const detailPlaceId = normalizeText(parsePlaceIdFromUrl(detailUrl));
    if (expectedIdentity.placeId && detailPlaceId) {
      return expectedIdentity.placeId === detailPlaceId;
    }

    const detailSlug = mapsPlaceSlug(detailUrl);
    const expectedSlug = mapsPlaceSlug(expectedIdentity.href);
    if (expectedSlug && detailSlug && expectedSlug === detailSlug) {
      return true;
    }

    const detailName = normalizeNameForMatch(
      textFrom("h1.DUwDvf") || textFrom("[role='main'] h1") || ""
    );
    if (expectedIdentity.name && detailName) {
      if (detailName === expectedIdentity.name) return true;
      if (detailName.includes(expectedIdentity.name) || expectedIdentity.name.includes(detailName)) return true;
    }

    return false;
  }

  function mapsPlaceSlug(url) {
    const value = normalizeMapsUrl(url);
    if (!value) return "";
    const match = value.match(/\/maps\/place\/([^/@?]+)/i);
    if (!match || !match[1]) return "";
    try {
      return decodeURIComponent(match[1]).toLowerCase();
    } catch (_error) {
      return normalizeText(match[1]).toLowerCase();
    }
  }

  function normalizeNameForMatch(value) {
    return normalizeText(value)
      .toLowerCase()
      .replace(/[^a-z0-9'& -]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function textFrom(selector) {
    const node = document.querySelector(selector);
    if (!node) return "";
    return normalizeText(node.textContent || "");
  }

  function attrFrom(selector, attrName) {
    const node = document.querySelector(selector);
    if (!node) return "";
    return normalizeText(node[attrName] || node.getAttribute(attrName) || "");
  }

  function hrefFrom(selector) {
    const node = document.querySelector(selector);
    if (!node) return "";
    const href = node.getAttribute("href") || node.href || "";
    return normalizeText(href);
  }

  function firstChipsText() {
    const chips = Array.from(document.querySelectorAll("button[jsaction*='pane.rating.category']"));
    if (chips.length === 0) return "";
    return normalizeText(chips.map((n) => n.textContent || "").join(" | "));
  }

  function getCurrentQuery() {
    const input = document.querySelector("input#searchboxinput");
    if (!input) return "";
    return normalizeText(input.value || "");
  }

  function createRunId(existing) {
    const supplied = normalizeText(existing);
    if (supplied) return supplied;
    return `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  }

  function persistScrapeSession(options) {
    const opts = options || {};
    const now = Date.now();
    const force = opts.force === true;
    if (!force && now - state.lastProgressPersistAtMs < 400) return;

    state.lastProgressPersistAtMs = now;
    const summary = opts.summary || {};
    const progress = opts.progress || {};
    const status = normalizeText(opts.status || "running") || "running";
    const perf = getPerformanceStats();
    const shouldPersistRows = force || state.rows.length - state.persistedRowsCount >= ROW_SNAPSHOT_INTERVAL;
    const rows = Array.isArray(opts.rows) ? opts.rows : state.rows;

    const snapshot = {
      run_id: state.runId,
      tab_id: Number.isFinite(Number(state.runTabId)) ? Number(state.runTabId) : null,
      status,
      processed: Number(progress.processed != null ? progress.processed : state.processed),
      matched: Number(progress.matched != null ? progress.matched : state.matched),
      duplicates: Number(progress.duplicates != null ? progress.duplicates : state.duplicates),
      fast_skipped: Number(progress.fast_skipped != null ? progress.fast_skipped : state.fastSkipped),
      errors: Number(progress.errors != null ? progress.errors : state.errors),
      seen_listings: Number(progress.seen_listings != null ? progress.seen_listings : perf.seen_listings),
      rate_per_sec: Number(progress.rate_per_sec != null ? progress.rate_per_sec : perf.rate_per_sec),
      avg_rating_seen: progress.avg_rating_seen != null ? progress.avg_rating_seen : perf.avg_rating_seen,
      avg_reviews_seen: progress.avg_reviews_seen != null ? progress.avg_reviews_seen : perf.avg_reviews_seen,
      rows_count: rows.length,
      source_query: state.sourceQuery,
      source_url: state.sourceUrl,
      filters: state.activeFilters,
      infinite_scroll: state.runInfiniteScroll,
      updated_at: new Date().toISOString()
    };

    if (opts.infinite_scroll != null) {
      snapshot.infinite_scroll = opts.infinite_scroll === true;
    }
    if (summary && typeof summary === "object") {
      snapshot.summary = summary;
      if (summary.stopped === true) {
        snapshot.status = "stopped";
      }
    }
    if (opts.error) {
      snapshot.error = normalizeText(opts.error);
    }
    if (opts.filters && typeof opts.filters === "object") {
      snapshot.filters = { ...opts.filters };
    }
    snapshot.started_at = state.runStartedAtIso || new Date().toISOString();
    if (status === "done" || status === "stopped" || status === "error") {
      snapshot.completed_at = new Date().toISOString();
    }

    const payload = {
      [SCRAPE_SESSION_KEY]: snapshot
    };
    if (shouldPersistRows) {
      payload.lastRows = rows;
      state.persistedRowsCount = rows.length;
    }

    chrome.storage.local.set(payload, () => {});
  }

  function getScrapeRuntimeState() {
    const perf = getPerformanceStats();
    return {
      is_running: state.isRunning,
      stop_requested: state.stopRequested,
      run_id: state.runId,
      tab_id: Number.isFinite(Number(state.runTabId)) ? Number(state.runTabId) : null,
      status: state.isRunning ? (state.stopRequested ? "stopping" : "running") : "idle",
      processed: state.processed,
      matched: state.matched,
      duplicates: state.duplicates,
      fast_skipped: state.fastSkipped,
      errors: state.errors,
      rows_count: state.rows.length,
      source_query: state.sourceQuery,
      source_url: state.sourceUrl,
      filters: state.activeFilters,
      infinite_scroll: state.runInfiniteScroll,
      ...perf
    };
  }

  function resetState() {
    state.stopRequested = false;
    state.runId = "";
    state.runTabId = null;
    state.runStartedAtIso = "";
    state.runInfiniteScroll = false;
    state.activeFilters = {};
    state.sourceQuery = "";
    state.sourceUrl = "";
    state.lastProgressPersistAtMs = 0;
    state.persistedRowsCount = 0;
    state.seenCardKeys = new Set();
    state.seenKeys = new Set();
    state.websiteHostOwners = new Map();
    state.rows = [];
    state.startedAtMs = Date.now();
    state.seenListings = 0;
    state.seenRatingSum = 0;
    state.seenRatingCount = 0;
    state.seenReviewsSum = 0;
    state.seenReviewsCount = 0;
    state.processed = 0;
    state.matched = 0;
    state.duplicates = 0;
    state.fastSkipped = 0;
    state.errors = 0;
  }

  function sendProgress() {
    const perf = getPerformanceStats();
    const payload = {
      type: MSG.SCRAPE_PROGRESS,
      run_id: state.runId,
      tab_id: state.runTabId,
      processed: state.processed,
      matched: state.matched,
      duplicates: state.duplicates,
      fast_skipped: state.fastSkipped,
      filters: state.activeFilters,
      ...perf,
      errors: state.errors
    };
    chrome.runtime.sendMessage(payload);
    persistScrapeSession({
      status: state.stopRequested ? "stopping" : "running",
      progress: payload
    });
  }

  function normalizeSeenRating(value) {
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0 || num > 5) return "";
    return num;
  }

  function normalizeSeenReviews(value) {
    const num = Number(value);
    if (!Number.isFinite(num) || num < 0) return "";
    return num;
  }

  function updateSeenStats(quickData) {
    const captured = {
      rating: false,
      reviews: false
    };
    if (!quickData || !quickData.text) return captured;

    state.seenListings += 1;

    const rating = normalizeSeenRating(quickData.rating);
    if (rating !== "") {
      state.seenRatingSum += rating;
      state.seenRatingCount += 1;
      captured.rating = true;
    }

    const reviews = normalizeSeenReviews(quickData.reviews);
    if (reviews !== "") {
      state.seenReviewsSum += reviews;
      state.seenReviewsCount += 1;
      captured.reviews = true;
    }

    return captured;
  }

  function backfillSeenStatsFromRow(row, captured) {
    if (!row || typeof row !== "object") return;
    const seen = captured || {};

    if (!seen.rating) {
      const rating = normalizeSeenRating(row.rating);
      if (rating !== "") {
        state.seenRatingSum += rating;
        state.seenRatingCount += 1;
      }
    }

    if (!seen.reviews) {
      const reviews = normalizeSeenReviews(row.review_count);
      if (reviews !== "") {
        state.seenReviewsSum += reviews;
        state.seenReviewsCount += 1;
      }
    }
  }

  function getPerformanceStats() {
    const elapsedSec = Math.max((Date.now() - state.startedAtMs) / 1000, 0.001);
    const ratePerSec = state.seenListings / elapsedSec;
    return {
      seen_listings: state.seenListings,
      rate_per_sec: Number(ratePerSec.toFixed(3)),
      avg_rating_seen: state.seenRatingCount > 0 ? Number((state.seenRatingSum / state.seenRatingCount).toFixed(3)) : "",
      avg_reviews_seen: state.seenReviewsCount > 0 ? Number((state.seenReviewsSum / state.seenReviewsCount).toFixed(3)) : ""
    };
  }

  function normalizeRuntimeFilters(input) {
    const source = input && typeof input === "object" ? input : {};
    return {
      minRating: parseFlexibleNumber(source.minRating),
      maxRating: parseFlexibleNumber(source.maxRating),
      minReviews: parseFlexibleNumber(source.minReviews),
      maxReviews: parseFlexibleNumber(source.maxReviews),
      nameKeyword: normalizeText(source.nameKeyword),
      categoryInclude: normalizeText(source.categoryInclude),
      categoryExclude: normalizeText(source.categoryExclude),
      hasWebsite: source.hasWebsite === true,
      hasPhone: source.hasPhone === true
    };
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

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
})();
