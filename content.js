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
    lastProgressDispatchAtMs: 0,
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
    const scrapeStageFilters = toScrapeStageFilters(filters);
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

    let progressHeartbeat = null;
    try {
      progressHeartbeat = window.setInterval(() => {
        if (!state.isRunning) return;
        sendProgress({ force: true });
      }, 500);
    } catch (_error) {
      progressHeartbeat = null;
    }

    try {
      sendProgress({ force: true });
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

          sendProgress({ force: true });

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

          if (!quickCardPassesFilter(card, quickData, scrapeStageFilters)) {
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

            if (row && applyFilters(row, scrapeStageFilters)) {
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

      if (hasAnyActiveFilter(scrapeStageFilters) && state.rows.length > 0) {
        const finalFilteredRows = state.rows.filter((row) => applyFilters(row, scrapeStageFilters));
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
      if (progressHeartbeat) {
        clearInterval(progressHeartbeat);
      }
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
      return sanitizeWebsiteForRowIdentity(fallbackRow, fallbackRow);
    }

    const detailRow = await extractBusinessRowStable(sourceQuery, sourceUrl, expectedIdentity, fallbackRow);
    if (
      detailRow &&
      expectedIdentity &&
      expectedIdentity.hasIdentity === true &&
      !isRowMatchingExpected(detailRow, expectedIdentity)
    ) {
      return sanitizeWebsiteForRowIdentity(fallbackRow || detailRow, fallbackRow);
    }
    if (
      detailRow &&
      fallbackRow &&
      normalizeText(detailRow.name) &&
      normalizeText(fallbackRow.name) &&
      !areLikelySameBusinessName(detailRow.name, fallbackRow.name)
    ) {
      return sanitizeWebsiteForRowIdentity(fallbackRow, fallbackRow);
    }

    if (detailRow && fallbackRow) {
      return sanitizeWebsiteForRowIdentity(mergeRows(detailRow, fallbackRow), fallbackRow);
    }
    return sanitizeWebsiteForRowIdentity(detailRow || fallbackRow, fallbackRow);
  }

  async function extractBusinessRowStable(sourceQuery, sourceUrl, expectedIdentity, fallbackRow) {
    const maxAttempts = 6;
    let lastRow = null;
    let previousWebsite = "";

    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const row = extractBusinessRow(sourceQuery, sourceUrl);
      if (!row) {
        await sleep(220);
        continue;
      }
      if (expectedIdentity && expectedIdentity.hasIdentity === true && !isRowMatchingExpected(row, expectedIdentity)) {
        await sleep(250);
        continue;
      }
      alignRowWithExpectedIdentity(row, expectedIdentity);

      lastRow = row;
      const website = normalizeBusinessWebsiteUrl(row.website);
      const hasSocialFallback = Boolean(normalizeFacebookProfileUrl(row.listing_facebook));
      const hasContactSignals =
        Boolean(normalizeText(row.address)) ||
        Boolean(sanitizePhoneText(row.phone)) ||
        hasSocialFallback;
      if (!website) {
        if (attempt >= maxAttempts - 1 || (attempt >= 2 && hasContactSignals)) {
          return row;
        }
        await sleep(320 + attempt * 80);
        continue;
      }

      const businessName = normalizeText(row.name) || normalizeText(fallbackRow && fallbackRow.name);
      const likelyForBusiness = isWebsiteLikelyForBusinessName(website, businessName);
      if (likelyForBusiness && (website === previousWebsite || attempt >= 1)) {
        return row;
      }

      previousWebsite = website;
      await sleep(280);
    }

    return lastRow;
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
    const listingFacebook = extractFacebookFromDetailPanel();

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
      listing_facebook: listingFacebook,
      email: "",
      owner_name: "",
      owner_title: "",
      owner_email: "",
      contact_email: "",
      primary_email: "",
      primary_email_type: "",
      primary_email_source: "",
      website_scan_status: websiteHref || listingFacebook ? "not_requested" : "no_website",
      site_pages_visited: 0,
      site_pages_discovered: 0,
      social_pages_scanned: 0,
      social_links: listingFacebook ? listingFacebook : "",
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
    const listingFacebook = extractFacebookFromCard(card);
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
      listing_facebook: listingFacebook,
      email: "",
      owner_name: "",
      owner_title: "",
      owner_email: "",
      contact_email: "",
      primary_email: "",
      primary_email_type: "",
      primary_email_source: "",
      website_scan_status: websiteFromCard || listingFacebook ? "not_requested" : "no_website",
      site_pages_visited: 0,
      site_pages_discovered: 0,
      social_pages_scanned: 0,
      social_links: listingFacebook ? listingFacebook : "",
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

    // Keep extraction scoped to the card itself to avoid leaking links from
    // neighboring listings when Google Maps virtualizes the list.
    const roots = [card].filter(Boolean);
    const selectors = [
      "a[data-item-id*='authority'][href]",
      "a[data-item-id*='website'][href]",
      "a[aria-label^='Website'][href]",
      "a[aria-label*='Website:'][href]",
      "a[aria-label*='open website' i][href]",
      "[data-tooltip*='website' i]",
      "[aria-label*='website' i]"
    ];

    for (const root of roots) {
      for (const selector of selectors) {
        const nodes = Array.from(root.querySelectorAll(selector)).slice(0, 40);
        for (const node of nodes) {
          const match = extractWebsiteCandidateFromNode(node, {
            matchBusinessName: null,
            matchCard: card
          });
          if (match) return match;
        }
      }
    }

    return "";
  }

  function extractFacebookFromCard(card) {
    if (!card) return "";

    const roots = [card].filter(Boolean);
    const selectors = [
      "a[href*='facebook.com'][href]",
      "a[aria-label*='facebook' i][href]",
      "a[data-item-id*='authority'][href]"
    ];

    for (const root of roots) {
      for (const selector of selectors) {
        const nodes = Array.from(root.querySelectorAll(selector)).slice(0, 40);
        for (const node of nodes) {
          const href = normalizeFacebookProfileUrl((node.getAttribute && node.getAttribute("href")) || node.href || "");
          if (href) return href;

          const textHit = findFacebookInText(
            `${normalizeText(node.textContent || "")} ${normalizeText((node.getAttribute && node.getAttribute("aria-label")) || "")}`
          );
          if (textHit) return textHit;
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
    const slug = mapsPlaceSlug(href);
    const cardName =
      normalizeText((link && link.getAttribute("aria-label")) || "") ||
      normalizeText(row.name) ||
      normalizeText(card && card.getAttribute && card.getAttribute("aria-label"));

    const normalizedName = normalizeNameForMatch(cardName);
    return {
      href,
      placeId,
      slug,
      name: normalizedName,
      hasIdentity: Boolean(href || placeId || slug || normalizedName)
    };
  }

  function alignRowWithExpectedIdentity(row, expectedIdentity) {
    if (!row || typeof row !== "object") return;
    if (!expectedIdentity || expectedIdentity.hasIdentity !== true) return;

    const expectedHref = normalizeMapsUrl(expectedIdentity.href);
    if (expectedHref) {
      row.maps_url = expectedHref;
    }

    if (!normalizeText(row.place_id)) {
      const expectedPlaceId = normalizeText(expectedIdentity.placeId || parsePlaceIdFromUrl(expectedHref));
      if (expectedPlaceId) {
        row.place_id = expectedPlaceId;
      }
    }
  }

  function isRowMatchingExpected(row, expectedIdentity) {
    if (!row || !expectedIdentity) return true;
    if (expectedIdentity.hasIdentity !== true) return true;

    const expectedPlaceId = normalizeText(expectedIdentity.placeId);
    const expectedSlug = normalizeText(expectedIdentity.slug || mapsPlaceSlug(expectedIdentity.href));
    const rowPlaceId = normalizeText(row.place_id);
    if (expectedPlaceId) {
      if (rowPlaceId) {
        return expectedPlaceId === rowPlaceId;
      }
      const rowSlugWithPlaceFallback = mapsPlaceSlug(normalizeMapsUrl(row.maps_url || window.location.href));
      if (expectedSlug && rowSlugWithPlaceFallback) {
        return expectedSlug === rowSlugWithPlaceFallback;
      }
      return false;
    }

    if (expectedSlug) {
      const rowMapsUrl = normalizeMapsUrl(row.maps_url || window.location.href);
      const rowSlug = mapsPlaceSlug(rowMapsUrl);
      if (rowSlug) {
        return expectedSlug === rowSlug;
      }
      return false;
    }

    const rowName = normalizeNameForMatch(row.name);
    if (expectedIdentity.name && rowName) {
      if (rowName === expectedIdentity.name) return true;
      if (rowName.includes(expectedIdentity.name) || expectedIdentity.name.includes(rowName)) return true;
    }

    return false;
  }

  function extractWebsiteFromDetailPanel() {
    const panelRoot = resolveActiveDetailPanelRoot();
    const roots = collectDetailPanelRoots(panelRoot);
    const detailName = normalizeText(
      textFromWithin(panelRoot, "h1.DUwDvf") ||
      textFromWithin(panelRoot, "h1") ||
      textFrom("h1.DUwDvf") ||
      textFrom("[role='main'] h1")
    );
    const linkSelectors = [
      "a[data-item-id='authority']",
      "a[data-item-id*='authority']",
      "a[data-item-id*='website']",
      "a[aria-label^='Website']",
      "a[aria-label*='Website:']",
      "a[aria-label*='website']",
      "a[aria-label*='open website' i]",
      "[role='main'] a[href^='http'][aria-label*='Website']",
      "[role='main'] a[jsaction*='authority'][href]",
      "[data-tooltip*='website' i]",
      "[aria-label*='website' i]"
    ];

    for (const root of roots) {
      for (const selector of linkSelectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          if (isNodeInsideResultsList(node)) continue;
          if (!isRenderedElement(node)) continue;
          const match = extractWebsiteCandidateFromNode(node, {
            matchBusinessName: detailName
          });
          if (match) return match;
        }
      }
    }

    const textSelectors = [
      "button[data-item-id='authority']",
      "button[data-item-id*='authority']",
      "button[data-item-id*='website']",
      "button[aria-label^='Website']",
      "button[aria-label*='Website:']",
      "button[aria-label*='open website' i]",
      "[role='button'][aria-label*='website' i]",
      "[data-tooltip*='website' i]",
      "[role='main'] [aria-label*='Website']"
    ];

    for (const root of roots) {
      for (const selector of textSelectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          if (isNodeInsideResultsList(node)) continue;
          if (!isRenderedElement(node)) continue;
          const match = extractWebsiteCandidateFromNode(node, {
            matchBusinessName: detailName
          });
          if (match) return match;
        }
      }
    }

    return "";
  }

  function extractFacebookFromDetailPanel() {
    const panelRoot = resolveActiveDetailPanelRoot();
    const roots = collectDetailPanelRoots(panelRoot);
    const linkSelectors = [
      "a[href*='facebook.com']",
      "a[aria-label*='facebook' i][href]",
      "[role='main'] a[href*='facebook.com']"
    ];

    for (const root of roots) {
      for (const selector of linkSelectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          if (isNodeInsideResultsList(node)) continue;
          if (!isRenderedElement(node)) continue;
          const href = normalizeFacebookProfileUrl((node.getAttribute && node.getAttribute("href")) || node.href || "");
          if (href) return href;

          const textHit = findFacebookInText(
            `${normalizeText(node.textContent || "")} ${normalizeText((node.getAttribute && node.getAttribute("aria-label")) || "")}`
          );
          if (textHit) return textHit;
        }
      }
    }

    const textSelectors = [
      "button[aria-label*='facebook' i]",
      "[role='button'][aria-label*='facebook' i]",
      "[aria-label*='facebook.com' i]"
    ];

    for (const root of roots) {
      for (const selector of textSelectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          if (isNodeInsideResultsList(node)) continue;
          if (!isRenderedElement(node)) continue;
          const textHit = findFacebookInText(
            `${normalizeText(node.textContent || "")} ${normalizeText((node.getAttribute && node.getAttribute("aria-label")) || "")}`
          );
          if (textHit) return textHit;
        }
      }
    }

    return "";
  }

  function resolveActiveDetailPanelRoot() {
    const heading = document.querySelector("h1.DUwDvf") || document.querySelector("[role='main'] h1");
    if (!heading) {
      return document.querySelector("[role='main']") || document.body || document.documentElement;
    }

    let current = heading;
    for (let depth = 0; current && depth < 10; depth += 1) {
      const hasActionButtons = Boolean(
        current.querySelector("a[data-item-id*='authority'], button[data-item-id*='authority'], button[data-item-id='address'], button[data-item-id^='phone:tel']")
      );
      if (hasActionButtons) {
        return current;
      }
      current = current.parentElement;
    }

    return heading.closest("[role='main']") || heading.parentElement || document.body || document.documentElement;
  }

  function collectDetailPanelRoots(panelRoot) {
    const seen = new Set();
    const out = [];
    const push = (node) => {
      if (!node || typeof node !== "object") return;
      if (seen.has(node)) return;
      seen.add(node);
      out.push(node);
    };

    push(panelRoot);
    if (panelRoot && typeof panelRoot.closest === "function") {
      push(panelRoot.closest("[role='main']"));
    }
    push(document.querySelector("[role='main']"));
    push(document.body);
    push(document.documentElement);
    return out;
  }

  function isNodeInsideResultsList(node) {
    if (!node || typeof node.closest !== "function") return false;
    return Boolean(
      node.closest("div[role='feed']") ||
      node.closest("div[aria-label*='Results' i]") ||
      node.closest("div[role='article']") ||
      node.closest("div.Nv2PK")
    );
  }

  function isRenderedElement(node) {
    if (!node || typeof node.getBoundingClientRect !== "function") return false;
    const style = window.getComputedStyle(node);
    if (!style) return true;
    if (style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) {
      return false;
    }
    const rect = node.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  }

  function extractWebsiteCandidateFromNode(node, optionsInput) {
    const options = optionsInput && typeof optionsInput === "object" ? optionsInput : {};
    const matchBusinessName = normalizeText(options.matchBusinessName);
    const matchCard = options.matchCard || null;
    const candidates = collectUrlCarrierNodes(node);

    for (const candidateNode of candidates) {
      const hrefMatch = extractDirectWebsiteUrlFromNode(candidateNode);
      if (hrefMatch) {
        return hrefMatch;
      }
    }

    for (const candidateNode of candidates) {
      const textBlob = buildNodeTextBlob(candidateNode);
      if (!textBlob) continue;
      const textHit = findWebsiteInText(textBlob);
      if (!textHit) continue;
      if (matchCard) {
        if (isWebsiteLikelyForCard(textHit, matchCard)) {
          return textHit;
        }
        continue;
      }
      if (!matchBusinessName || isWebsiteLikelyForBusinessName(textHit, matchBusinessName)) {
        return textHit;
      }
    }

    return "";
  }

  function collectUrlCarrierNodes(node) {
    const out = [];
    const seen = new Set();
    const push = (candidate) => {
      if (!candidate || typeof candidate !== "object") return;
      if (seen.has(candidate)) return;
      seen.add(candidate);
      out.push(candidate);
    };

    push(node);
    if (node && typeof node.closest === "function") {
      push(node.closest("a[href]"));
      push(node.closest("[data-href]"));
      push(node.closest("[data-url]"));
      push(node.closest("[data-value]"));
      push(node.closest("[role='link']"));
      push(node.closest("button"));
    }
    if (node && typeof node.querySelectorAll === "function") {
      const descendants = node.querySelectorAll("a[href], [data-href], [data-url], [data-value], [role='link'], button");
      for (const descendant of Array.from(descendants).slice(0, 10)) {
        push(descendant);
      }
    }
    return out;
  }

  function extractDirectWebsiteUrlFromNode(node) {
    if (!node || typeof node !== "object") return "";

    const directAttributes = [
      node.href,
      node.src,
      readNodeAttribute(node, "href"),
      readNodeAttribute(node, "data-href"),
      readNodeAttribute(node, "data-url"),
      readNodeAttribute(node, "data-link"),
      readNodeAttribute(node, "data-link-url"),
      readNodeAttribute(node, "data-target-url"),
      readNodeAttribute(node, "data-value"),
      readNodeAttribute(node, "data-website"),
      readNodeAttribute(node, "data-website-url"),
      readNodeAttribute(node, "ping")
    ];

    const dataset = node && node.dataset && typeof node.dataset === "object" ? Object.values(node.dataset) : [];
    for (const candidate of [...directAttributes, ...dataset]) {
      const normalized = normalizeBusinessWebsiteUrl(candidate);
      if (isValidWebsiteLink(normalized)) {
        return normalized;
      }
    }

    return "";
  }

  function readNodeAttribute(node, name) {
    if (!node || typeof node.getAttribute !== "function") return "";
    return normalizeText(node.getAttribute(name) || "");
  }

  function buildNodeTextBlob(node) {
    if (!node || typeof node !== "object") return "";
    const parts = [
      normalizeText(node.textContent || ""),
      readNodeAttribute(node, "aria-label"),
      readNodeAttribute(node, "title"),
      readNodeAttribute(node, "data-href"),
      readNodeAttribute(node, "data-url"),
      readNodeAttribute(node, "data-link"),
      readNodeAttribute(node, "data-link-url"),
      readNodeAttribute(node, "data-target-url"),
      readNodeAttribute(node, "data-value"),
      readNodeAttribute(node, "data-website"),
      readNodeAttribute(node, "data-website-url"),
      normalizeText(node.href || ""),
      normalizeText(node.src || "")
    ];
    const dataset = node && node.dataset && typeof node.dataset === "object" ? Object.values(node.dataset) : [];
    for (const value of dataset) {
      parts.push(normalizeText(value));
    }
    return normalizeText(parts.join(" "));
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

  function findFacebookInText(text) {
    const raw = normalizeText(text);
    if (!raw) return "";

    const pattern = /(?:https?:\/\/)?(?:www\.|m\.)?facebook\.com\/[^\s<>()\[\]{}"']+/gi;
    let match = pattern.exec(raw);
    while (match) {
      const matchedText = normalizeText(match[0]).replace(/[),.;]+$/, "");
      const normalized = normalizeFacebookProfileUrl(matchedText);
      if (normalized) return normalized;
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

  function normalizeFacebookProfileUrl(url) {
    const normalized = normalizeBusinessWebsiteUrl(url);
    if (!normalized) return "";

    try {
      const parsed = new URL(normalized);
      const host = normalizeText(parsed.hostname).toLowerCase();
      if (!host.includes("facebook.com")) return "";

      const path = normalizeText(parsed.pathname || "").toLowerCase().replace(/\/+$/, "");
      if (!path || path === "/") return "";
      if (path.startsWith("/sharer") || path.startsWith("/share.php")) return "";
      if (path.startsWith("/l.php")) return "";
      if (path.startsWith("/dialog/") || path.startsWith("/plugins/")) return "";
      if (path.startsWith("/privacy") || path.startsWith("/policies") || path.startsWith("/terms")) return "";
      if (path.startsWith("/help") || path.startsWith("/legal") || path.startsWith("/settings")) return "";
      if (path.startsWith("/login") || path.startsWith("/recover") || path.startsWith("/checkpoint")) return "";
      if (path.startsWith("/watch") || path.startsWith("/reel") || path.startsWith("/story.php")) return "";
      if (path.startsWith("/groups") || path.startsWith("/events") || path.startsWith("/marketplace")) return "";

      const segments = path.split("/").filter(Boolean);
      const firstSegment = segments[0] || "";
      if (!firstSegment) return "";
      if (firstSegment === "profile.php") {
        return normalizeText(parsed.searchParams.get("id")) ? normalized : "";
      }
      if (firstSegment === "pg") {
        const secondSegment = segments[1] || "";
        return secondSegment ? normalized : "";
      }
      if (firstSegment === "pages") {
        const secondSegment = segments[1] || "";
        const numericId = segments.find((segment) => /^\d{5,}$/.test(segment)) || "";
        return secondSegment || numericId ? normalized : "";
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
      if (reservedTopLevel.has(firstSegment)) return "";
      return /^[a-z0-9._-]{3,}$/i.test(firstSegment) ? normalized : "";
    } catch (_error) {
      return "";
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

  function hasFacebookFallbackInRow(row) {
    const source = row && typeof row === "object" ? row : {};
    if (normalizeFacebookProfileUrl(source.listing_facebook)) return true;

    const socialLinks = normalizeText(source.social_links)
      .split(/\s*\|\s*|\s*,\s*|\n+/)
      .map((entry) => normalizeFacebookProfileUrl(entry))
      .filter(Boolean);
    return socialLinks.length > 0;
  }

  function normalizeWebsiteOwnerKey(url) {
    const normalized = normalizeBusinessWebsiteUrl(url);
    if (!normalized) return "";
    const facebookProfile = normalizeFacebookProfileUrl(normalized);
    if (facebookProfile) {
      return facebookProfile.toLowerCase();
    }
    return normalizeWebsiteHost(normalized);
  }

  function isWebsiteLikelyForBusinessName(url, businessName) {
    if (normalizeFacebookProfileUrl(url)) {
      return true;
    }
    const host = normalizeWebsiteHost(url);
    if (!host) return false;
    const hostRoot = host.split(".")[0].replace(/[^a-z0-9]/g, "");
    if (!hostRoot) return false;

    const nameTokens = normalizeBusinessNameKey(businessName)
      .split(/\s+/)
      .filter((token) => token.length >= 3);
    if (nameTokens.length === 0) {
      return true;
    }
    return nameTokens.some((token) => hostRoot.includes(token) || token.includes(hostRoot));
  }

  function sanitizeWebsiteForRowIdentity(row, fallbackRow) {
    if (!row || typeof row !== "object") return row;
    const website = normalizeBusinessWebsiteUrl(row.website);
    if (!website) {
      row.website = "";
      row.website_scan_status = hasFacebookFallbackInRow(row) ? "not_requested" : "no_website";
      return row;
    }

    const fallbackName = normalizeText(fallbackRow && fallbackRow.name);
    const rowName = normalizeText(row.name);
    if (fallbackName && rowName && !areLikelySameBusinessName(rowName, fallbackName)) {
      row.name = fallbackName;
    }

    row.website = website;
    if (!normalizeText(row.website_scan_status)) {
      row.website_scan_status = "not_requested";
    }
    return row;
  }

  function isWebsiteLikelyForCard(url, card) {
    if (normalizeFacebookProfileUrl(url)) {
      return true;
    }
    const host = normalizeWebsiteHost(url);
    if (!host) return false;
    const hostRoot = host.split(".")[0].replace(/[^a-z0-9]/g, "");
    if (!hostRoot) return false;

    const link = resolveCardLink(card);
    const cardName = normalizeText(
      (link && link.getAttribute("aria-label")) ||
      (card && card.getAttribute && card.getAttribute("aria-label")) ||
      (card && card.textContent) ||
      ""
    );
    const nameTokens = normalizeBusinessNameKey(cardName)
      .split(/\s+/)
      .filter((token) => token.length >= 3);
    if (nameTokens.length === 0) {
      return true;
    }
    return nameTokens.some((token) => hostRoot.includes(token) || token.includes(hostRoot));
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
    const isSocialWebsite = Boolean(normalizeFacebookProfileUrl(website));
    if (!website) {
      row.website = "";
      if (!normalizeText(row.website_scan_status)) {
        row.website_scan_status = hasFacebookFallbackInRow(row) ? "not_requested" : "no_website";
      }
      return row;
    }

    const ownerKey = normalizeWebsiteOwnerKey(website);
    if (!ownerKey) {
      row.website = "";
      row.website_scan_status = hasFacebookFallbackInRow(row) ? "not_requested" : "no_website";
      return row;
    }

    const placeId = normalizeText(row.place_id);
    const mapsUrl = normalizeMapsUrl(row.maps_url || "");
    const businessName = normalizeText(row.name);
    const existing = state.websiteHostOwners.get(ownerKey);

    if (!existing) {
      state.websiteHostOwners.set(ownerKey, {
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

    // Allow multiple leads to share one normal website host.
    // Keep strict matching for social profiles to avoid cross-assignment.
    if (!sameIdentity && isSocialWebsite) {
      row.website = "";
      row.website_scan_status = hasFacebookFallbackInRow(row) ? "not_requested" : "no_website";
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

  function textFromWithin(root, selector) {
    if (!root || typeof root.querySelector !== "function") return "";
    const node = root.querySelector(selector);
    if (!node) return "";
    return normalizeText(node.textContent || "");
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
    const targets = collectScrollTargets(feed, lastCard);
    const beforeSnapshot = targets.map((node) => [node, readScrollTop(node)]);

    const maxViewport = targets.reduce((acc, node) => {
      const height = Number(node && node.clientHeight ? node.clientHeight : 0);
      return Math.max(acc, height);
    }, Math.max(window.innerHeight || 0, feed.clientHeight || 0));
    const step = Math.max(720, Math.floor(maxViewport * 0.85));

    if (lastCard && typeof lastCard.scrollIntoView === "function") {
      lastCard.scrollIntoView({ block: "end", behavior: "auto" });
    }
    for (const node of targets) {
      writeScrollTop(node, readScrollTop(node) + step);
    }
    await sleep(120);
    const moved = beforeSnapshot.some(([node, before]) => readScrollTop(node) !== before);
    if (!moved) {
      const deltaY = Math.max(1000, step);
      for (const node of targets.slice(0, 4)) {
        dispatchScrollWheel(node, deltaY);
      }
      window.scrollBy(0, deltaY);
      await sleep(120);
    }
  }

  function collectScrollTargets(feed, lastCard) {
    const out = [];
    const seen = new Set();

    const push = (node) => {
      if (!node || typeof node !== "object") return;
      if (seen.has(node)) return;
      seen.add(node);
      out.push(node);
    };

    const addScrollableAncestors = (startNode) => {
      let current = startNode && startNode.nodeType === 1 ? startNode : null;
      let depth = 0;
      while (current && depth < 12) {
        if (isLikelyScrollableElement(current)) {
          push(current);
        }
        current = current.parentElement;
        depth += 1;
      }
    };

    if (lastCard) addScrollableAncestors(lastCard);
    if (feed) addScrollableAncestors(feed);
    push(feed);
    push(document.scrollingElement);
    push(document.documentElement);
    push(document.body);
    return out.filter(Boolean);
  }

  function isLikelyScrollableElement(node) {
    if (!node || node.nodeType !== 1) return false;
    if (node === document.documentElement || node === document.body) return true;
    const style = window.getComputedStyle(node);
    const overflowY = normalizeText(style.overflowY || style.overflow || "").toLowerCase();
    if (!/(auto|scroll|overlay)/.test(overflowY)) return false;
    return Number(node.scrollHeight || 0) - Number(node.clientHeight || 0) > 24;
  }

  function readScrollTop(node) {
    if (!node) return 0;
    if (node === document.documentElement || node === document.body || node === document.scrollingElement) {
      return Number(window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0);
    }
    return Number(node.scrollTop || 0);
  }

  function writeScrollTop(node, value) {
    const next = Number.isFinite(Number(value)) ? Number(value) : 0;
    if (!node) return;
    if (node === document.documentElement || node === document.body || node === document.scrollingElement) {
      window.scrollTo(0, next);
      return;
    }
    node.scrollTop = next;
  }

  function dispatchScrollWheel(node, deltaY) {
    if (!node || typeof node.dispatchEvent !== "function") return;
    const wheel = new WheelEvent("wheel", {
      deltaY,
      bubbles: true,
      cancelable: true,
      view: window
    });
    node.dispatchEvent(wheel);
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
    let matchedAt = 0;
    while (Date.now() < timeoutAt) {
      const hasName = Boolean(document.querySelector("h1.DUwDvf") || document.querySelector("[role='main'] h1"));
      if (hasName && isDetailPanelMatch(expectedIdentity)) {
        matchedAt = matchedAt || Date.now();
        if (hasDetailPanelActionSignals()) {
          return true;
        }
        if (Date.now() - matchedAt >= 900) {
          return true;
        }
      } else {
        matchedAt = 0;
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

  function hasDetailPanelActionSignals() {
    const root = resolveActiveDetailPanelRoot();
    const roots = collectDetailPanelRoots(root);
    const selectors = [
      "a[data-item-id*='authority']",
      "a[data-item-id*='website']",
      "button[data-item-id*='authority']",
      "button[data-item-id*='website']",
      "button[data-item-id='address']",
      "button[data-item-id^='phone:tel']",
      "[aria-label*='website' i]",
      "[aria-label*='facebook' i]",
      "a[href*='facebook.com']"
    ];
    return roots.some((candidateRoot) => {
      if (!candidateRoot || typeof candidateRoot.querySelector !== "function") {
        return false;
      }
      return selectors.some((selector) => {
        const matches = Array.from(candidateRoot.querySelectorAll(selector)).slice(0, 20);
        return matches.some((match) => !isNodeInsideResultsList(match) && isRenderedElement(match));
      });
    });
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
    const expectedPlaceId = normalizeText(expectedIdentity.placeId);
    const expectedSlug = normalizeText(expectedIdentity.slug || mapsPlaceSlug(expectedIdentity.href));
    const detailPlaceId = normalizeText(parsePlaceIdFromUrl(detailUrl));
    if (expectedPlaceId) {
      if (detailPlaceId) {
        return expectedPlaceId === detailPlaceId;
      }
      const detailSlugWithPlaceFallback = mapsPlaceSlug(detailUrl);
      if (expectedSlug && detailSlugWithPlaceFallback) {
        return expectedSlug === detailSlugWithPlaceFallback;
      }
      return false;
    }

    const detailSlug = mapsPlaceSlug(detailUrl);
    if (expectedSlug) {
      if (detailSlug) {
        return expectedSlug === detailSlug;
      }
      return false;
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
    state.lastProgressDispatchAtMs = 0;
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

  function sendProgress(options) {
    const opts = options && typeof options === "object" ? options : {};
    const force = opts.force === true;
    const now = Date.now();
    if (!force && now - state.lastProgressDispatchAtMs < 180) {
      return;
    }
    state.lastProgressDispatchAtMs = now;

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
      hasPhone: source.hasPhone === true,
      hasEmail: source.hasEmail === true || source.requireEmailForLeads === true
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

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
})();
