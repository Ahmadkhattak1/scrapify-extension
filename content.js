(function () {
  if (window.__GBP_MAPS_SCRAPER_BOOTSTRAPPED__ === true) {
    return;
  }
  window.__GBP_MAPS_SCRAPER_BOOTSTRAPPED__ = true;

  const shared = window.GbpShared;
  const { MSG, DEFAULT_MAX_ROWS, normalizeText, parseRating, parseReviewCount, normalizeMapsUrl, dedupeKey, applyFilters } = shared;

  const state = {
    isRunning: false,
    stopRequested: false,
    seenCardKeys: new Set(),
    seenKeys: new Set(),
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

  chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (!message || !message.type) {
      return false;
    }

    if (message.type === MSG.START_SCRAPE) {
      if (state.isRunning) {
        sendResponse({ ok: false, error: "Scrape already running" });
        return false;
      }

      runScrape(message.config || {})
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
      sendResponse({ ok: true });
      return false;
    }

    return false;
  });

  async function runScrape(config) {
    resetState();
    state.isRunning = true;

    const maxRows = Number(config.maxRows) > 0 ? Number(config.maxRows) : DEFAULT_MAX_ROWS;
    const infiniteScroll = Boolean(config.infiniteScroll);
    const filters = config.filters || {};
    const sourceQuery = getCurrentQuery();
    const sourceUrl = window.location.href;

    try {
      const feed = findResultsFeed();
      if (!feed) {
        throw new Error("Could not find Google Maps results list. Open a search results page first.");
      }

      let noNewCardsScrolls = 0;

      while (!state.stopRequested) {
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

          const quickData = buildQuickCardData(card);
          updateSeenStats(quickData);

          if (!quickCardPassesFilter(quickData, filters)) {
            state.fastSkipped += 1;
            sendProgress();
            continue;
          }

          try {
            const row = await processCard(card, sourceQuery, sourceUrl);
            state.processed += 1;

            if (row && applyFilters(row, filters)) {
              const key = dedupeKey(row);
              if (key) {
                if (state.seenKeys.has(key)) {
                  state.duplicates += 1;
                } else {
                  state.seenKeys.add(key);
                  state.rows.push(row);
                  state.matched += 1;
                }
              } else {
                state.rows.push(row);
                state.matched += 1;
              }
            }

            sendProgress();

            if (!infiniteScroll && state.rows.length >= maxRows) {
              break;
            }
          } catch (_cardErr) {
            state.processed += 1;
            state.errors += 1;
            sendProgress();
          }

          await sleep(250);
        }

        if (!infiniteScroll && state.rows.length >= maxRows) {
          break;
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

      chrome.runtime.sendMessage({
        type: MSG.SCRAPE_DONE,
        rows: state.rows,
        summary
      });

      return { rows: state.rows, summary };
    } catch (error) {
      chrome.runtime.sendMessage({
        type: MSG.SCRAPE_ERROR,
        error: error && error.message ? error.message : "Unexpected scrape error"
      });
      throw error;
    } finally {
      state.isRunning = false;
    }
  }

  async function processCard(card, sourceQuery, sourceUrl) {
    const fallbackRow = extractBusinessRowFromCard(card, sourceQuery, sourceUrl);

    safeClick(card);
    await sleep(900);
    await waitForDetails(3000);

    const detailRow = extractBusinessRow(sourceQuery, sourceUrl);
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
      "";

    const reviewsText =
      textFrom("div.F7nice span[aria-label*='review']") ||
      textFrom("button[aria-label*='review']") ||
      textFrom("span[aria-label*='reviews']") ||
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

    const websiteHref =
      hrefFrom("a[data-item-id='authority']") ||
      hrefFrom("a[aria-label^='Website']") ||
      hrefFrom("a[aria-label*='Website:']");

    const hours =
      textFrom("div.t39EBf") ||
      textFrom("table.eK4R0e") ||
      textFrom("div[aria-label*='Hours']");

    const mapsUrl = normalizeMapsUrl(window.location.href);
    const placeId = parsePlaceIdFromUrl(mapsUrl);

    return {
      place_id: normalizeText(placeId),
      name: normalizeText(name),
      rating: parseRating(ratingText),
      review_count: parseReviewCount(reviewsText),
      category: normalizeText(category),
      address: normalizeFieldValue(address, "Address"),
      phone: normalizeFieldValue(phone, "Phone"),
      website: normalizeMapsUrl(websiteHref),
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

  function extractBusinessRowFromCard(card, sourceQuery, sourceUrl) {
    if (!card) return null;
    const link = resolveCardLink(card);
    const cardText = normalizeText(card.textContent || "");
    const lines = cardText.split(/\n+/).map((line) => normalizeText(line)).filter(Boolean);

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
      rating: parseRating(cardText),
      review_count: parseReviewCount(cardText),
      category: lines[1] || "",
      address: "",
      phone: "",
      website: "",
      hours: "",
      maps_url: mapsUrl,
      source_query: normalizeText(sourceQuery),
      source_url: normalizeMapsUrl(sourceUrl),
      scraped_at: new Date().toISOString()
    };
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

  function hasRowValue(value) {
    if (value == null) return false;
    if (typeof value === "number") return Number.isFinite(value);
    return normalizeText(value) !== "";
  }

  function resolveCardLink(card) {
    if (!card) return null;
    if (card.tagName === "A") return card;
    return card.closest("a[href*='/maps/place/']") || card.querySelector("a[href*='/maps/place/']");
  }

  function getCardIdentity(card) {
    const link = resolveCardLink(card);
    if (!link) return "";
    const href = normalizeMapsUrl(link.href || link.getAttribute("href") || "");
    const label = normalizeText(link.getAttribute("aria-label") || "");
    return `${href}::${label}`;
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

  function quickCardPassesFilter(quickData, filters) {
    const f = filters || {};
    const cardText = quickData && quickData.text ? quickData.text : "";
    if (!cardText) return true;

    const quickRating = quickData.rating;
    if (f.minRating !== "" && f.minRating != null && quickRating !== "") {
      const minRating = Number(f.minRating);
      if (Number.isFinite(minRating) && quickRating < minRating) return false;
    }
    if (f.maxRating !== "" && f.maxRating != null && quickRating !== "") {
      const maxRating = Number(f.maxRating);
      if (Number.isFinite(maxRating) && quickRating > maxRating) return false;
    }

    const quickReviews = quickData.reviews;
    if (f.minReviews !== "" && f.minReviews != null && quickReviews !== "") {
      const minReviews = Number(f.minReviews);
      if (Number.isFinite(minReviews) && quickReviews < minReviews) return false;
    }
    if (f.maxReviews !== "" && f.maxReviews != null && quickReviews !== "") {
      const maxReviews = Number(f.maxReviews);
      if (Number.isFinite(maxReviews) && quickReviews > maxReviews) return false;
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
      rating: parseCardRating(text),
      reviews: parseCardReviewCount(text)
    };
  }

  function parseCardRating(text) {
    const clean = normalizeText(text).replace(",", ".");
    if (!clean) return "";

    const starPattern = clean.match(/(\d(?:\.\d)?)\s*[★⭐]/);
    if (starPattern && starPattern[1]) {
      const value = Number(starPattern[1]);
      return Number.isFinite(value) ? value : "";
    }

    const directPattern = clean.match(/\b([0-5](?:\.\d)?)\b/);
    if (directPattern && directPattern[1]) {
      const value = Number(directPattern[1]);
      return Number.isFinite(value) ? value : "";
    }

    return "";
  }

  function parseCardReviewCount(text) {
    const clean = normalizeText(text);
    if (!clean) return "";

    const parenPattern = clean.match(/\(([\d,]+)\)/);
    if (parenPattern && parenPattern[1]) {
      const value = Number(parenPattern[1].replace(/,/g, ""));
      return Number.isFinite(value) ? value : "";
    }

    const wordPattern = clean.match(/([\d,]+)\s+reviews?/i);
    if (wordPattern && wordPattern[1]) {
      const value = Number(wordPattern[1].replace(/,/g, ""));
      return Number.isFinite(value) ? value : "";
    }

    return "";
  }

  function findResultsFeed() {
    return (
      document.querySelector("div[role='feed']") ||
      document.querySelector("div[aria-label*='Results']") ||
      document.querySelector("div.m6QErb.DxyBCb")
    );
  }

  function getResultCards(feed) {
    if (!feed) return [];
    const selectors = [
      "a.hfpxzc",
      "div[role='article'] a[href*='/maps/place/']",
      "a[href*='/maps/place/']"
    ];

    for (const selector of selectors) {
      const nodes = Array.from(feed.querySelectorAll(selector));
      const unique = dedupeNodes(nodes).filter(isVisible);
      if (unique.length > 0) {
        return unique;
      }
    }

    return [];
  }

  function dedupeNodes(nodes) {
    const seen = new Set();
    const out = [];
    for (const node of nodes) {
      const key = node.href || node.getAttribute("aria-label") || node.textContent;
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
    const before = feed.scrollTop;
    feed.scrollTop = feed.scrollTop + Math.max(600, Math.floor(feed.clientHeight * 0.8));
    if (feed.scrollTop === before) {
      feed.dispatchEvent(new WheelEvent("wheel", { deltaY: 1000 }));
    }
  }

  function safeClick(node) {
    if (!node) return;
    node.dispatchEvent(new MouseEvent("mousedown", { bubbles: true }));
    node.dispatchEvent(new MouseEvent("mouseup", { bubbles: true }));
    node.click();
  }

  async function waitForDetails(timeoutMs) {
    const timeoutAt = Date.now() + timeoutMs;
    while (Date.now() < timeoutAt) {
      const hasName = Boolean(document.querySelector("h1.DUwDvf") || document.querySelector("[role='main'] h1"));
      if (hasName) {
        return;
      }
      await sleep(120);
    }
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

  function resetState() {
    state.stopRequested = false;
    state.seenCardKeys = new Set();
    state.seenKeys = new Set();
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
    chrome.runtime.sendMessage({
      type: MSG.SCRAPE_PROGRESS,
      processed: state.processed,
      matched: state.matched,
      duplicates: state.duplicates,
      fast_skipped: state.fastSkipped,
      ...perf,
      errors: state.errors
    });
  }

  function updateSeenStats(quickData) {
    if (!quickData || !quickData.text) return;
    state.seenListings += 1;

    if (quickData.rating !== "") {
      state.seenRatingSum += quickData.rating;
      state.seenRatingCount += 1;
    }

    if (quickData.reviews !== "") {
      state.seenReviewsSum += quickData.reviews;
      state.seenReviewsCount += 1;
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

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
})();
