/**
 * Fetch verified amenities from choicehotels.com property pages.
 * Source: public HTML / JSON-LD only — never invent labels.
 */

import { load as loadCheerio } from "cheerio";

export const CHOICE_CONTENT_SOURCE = "choicehotels_property_page";
export const CHOICE_CONTENT_SOURCE_PUPPETEER = "choicehotels_property_page_puppeteer";

const FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml",
  "Accept-Language": "en-US,en;q=0.9",
};

function cleanText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function dedupeLabels(labels) {
  const seen = new Set();
  /** @type {string[]} */
  const out = [];
  for (const raw of labels) {
    const label = cleanText(raw);
    if (!label || label.length < 2 || label.length > 120) continue;
    if (/^amenities$/i.test(label)) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  return out.sort((a, b) => a.localeCompare(b));
}

/**
 * Extract a JSON array starting at `start` (index of `[`) from HTML/text.
 * @param {string} text
 * @param {number} start
 */
function extractJsonArrayAt(text, start) {
  if (text[start] !== "[") return null;
  let depth = 0;
  let inStr = false;
  let esc = false;
  for (let i = start; i < text.length; i++) {
    const c = text[i];
    if (inStr) {
      if (esc) esc = false;
      else if (c === "\\") esc = true;
      else if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') {
      inStr = true;
      continue;
    }
    if (c === "[") depth++;
    else if (c === "]") {
      depth--;
      if (depth === 0) return text.slice(start, i + 1);
    }
  }
  return null;
}

/**
 * Property-page embedded Redux/state amenities (official Choice payload).
 * Looks like: "amenities":[{"code":"CONT","description":"Free Continental Breakfast",...}]
 * Does NOT use the global amenityCodes catalog dictionary.
 * @param {string} html
 */
export function extractChoiceEmbeddedPropertyAmenityLabels(html) {
  const raw = String(html || "");
  /** @type {string[]} */
  const labels = [];
  const re = /"amenities"\s*:\s*\[/g;
  let match;
  /** @type {string[][]} */
  const candidateLists = [];

  while ((match = re.exec(raw))) {
    const arrStart = match.index + match[0].length - 1;
    const json = extractJsonArrayAt(raw, arrStart);
    if (!json || json.length > 400_000) continue;
    try {
      const arr = JSON.parse(json);
      if (!Array.isArray(arr) || !arr.length) continue;
      /** @type {string[]} */
      const batch = [];
      for (const item of arr) {
        if (!item || typeof item !== "object") continue;
        // Property amenity rows always carry a code + human description
        if (!item.code) continue;
        const desc = cleanText(item.description || item.name);
        if (desc) batch.push(desc);
      }
      // Require real amenity objects (not unrelated "amenities" arrays)
      if (batch.length >= 3) candidateLists.push(batch);
    } catch {
      // skip malformed slices
    }
  }

  // Prefer the longest property amenity list (full inventory over truncated)
  candidateLists.sort((a, b) => b.length - a.length);
  if (candidateLists[0]) labels.push(...candidateLists[0]);
  return labels;
}

/**
 * True when HTML has structured Choice amenity markers (JSON-LD amenityFeature,
 * embedded property amenities payload, or amenity-tagged DOM).
 * Body-text/FAQ heuristics are not markers.
 * @param {string} html
 */
export function hasChoiceAmenityMarkers(html) {
  const raw = String(html || "");
  if (!raw.trim()) return false;
  if (/"amenityFeature"\s*:/i.test(raw)) return true;
  // Property amenity inventory in page state (Wayback / SSR / client hydrate)
  if (/"amenities"\s*:\s*\[\s*\{\s*"code"\s*:/i.test(raw)) return true;
  if (/data-testid=["'][^"']*amenit/i.test(raw)) return true;
  if (/\b(class|id)=["'][^"']*amenit/i.test(raw)) return true;
  return false;
}

/**
 * Parse amenities from Choice property HTML only — never invent labels.
 * Sources: JSON-LD `amenityFeature` / named `containsPlace`, embedded property
 * `amenities` arrays (code+description), and amenity-tagged DOM lists.
 * @param {string} html
 */
export function parseChoiceAmenitiesFromHtml(html) {
  /** @type {string[]} */
  const labels = [];
  /** @type {string[]} */
  const parseErrors = [];
  let jsonLdAmenityFeatureCount = 0;
  let domAmenityLabelCount = 0;
  let embeddedPropertyAmenityCount = 0;

  const $ = loadCheerio(html);
  const hasMarkers = hasChoiceAmenityMarkers(html);

  // JSON-LD Hotel schema — labels come from the page payload only
  $("script[type='application/ld+json']").each((_, el) => {
    try {
      const json = JSON.parse($(el).html() || "");
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        if (!obj || typeof obj !== "object") continue;
        const types = Array.isArray(obj["@type"]) ? obj["@type"] : [obj["@type"]];
        if (!types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) continue;

        for (const feat of obj.amenityFeature || []) {
          const name = cleanText(feat?.name || feat?.value);
          if (name) {
            labels.push(name);
            jsonLdAmenityFeatureCount++;
          }
        }
        for (const place of obj.containsPlace || []) {
          const name = cleanText(place?.name);
          // Only use explicit place names from JSON-LD — do not invent from @type alone
          if (name) labels.push(name);
        }
      }
    } catch {
      parseErrors.push("json_ld_parse_error");
    }
  });

  // Embedded Choice property amenities payload (common in Wayback / hydrated HTML)
  const embedded = extractChoiceEmbeddedPropertyAmenityLabels(html);
  if (embedded.length) {
    labels.push(...embedded);
    embeddedPropertyAmenityCount = embedded.length;
  }

  // Visible amenity lists (common Choice markup) — require amenit* markers on node/parent
  $("[data-testid*='amenit'], [class*='amenit'], [id*='amenit']").each((_, el) => {
    const tag = String(el.tagName || el.name || "").toLowerCase();
    // Skip large containers; prefer leaf-ish nodes
    if (tag === "section" || tag === "div" || tag === "ul" || tag === "ol") {
      const kids = $(el).children();
      if (kids.length > 0) return;
    }
    const t = cleanText($(el).text());
    if (t && t.length < 80 && !/^amenities$/i.test(t)) {
      labels.push(t);
      domAmenityLabelCount++;
    }
  });

  $("li").each((_, el) => {
    const parent = $(el).parent();
    const parentClass = String(parent.attr("class") || "");
    const parentId = String(parent.attr("id") || "");
    const parentTestId = String(parent.attr("data-testid") || "");
    if (!/amenit/i.test(parentClass + parentId + parentTestId)) return;
    const t = cleanText($(el).text());
    if (t && t.length < 80) {
      labels.push(t);
      domAmenityLabelCount++;
    }
  });

  if (!hasMarkers) {
    parseErrors.push("missing_amenity_markers");
  }

  const amenities = dedupeLabels(labels);
  if (hasMarkers && !amenities.length) {
    parseErrors.push("markers_present_but_no_labels");
  }

  return {
    amenities,
    amenitiesText: amenities.join("; "),
    parseErrors: [...new Set(parseErrors)],
    hasAmenityMarkers: hasMarkers,
    jsonLdAmenityFeatureCount,
    domAmenityLabelCount,
    embeddedPropertyAmenityCount,
    source: amenities.length ? CHOICE_CONTENT_SOURCE : null,
  };
}

/**
 * Puppeteer fetch with optional regional warm-up (property pages often Akamai-blocked).
 * @param {string} pageUrl
 * @param {object} [opts]
 */
export async function fetchChoiceHotelHtmlPuppeteer(pageUrl, opts = {}) {
  let puppeteer;
  try {
    puppeteer = await import("puppeteer");
  } catch (err) {
    throw new Error(`puppeteer unavailable: ${err?.message || err}`);
  }

  const browser = await puppeteer.launch({
    headless: opts.headless !== false ? "new" : false,
    args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
  });

  try {
    const page = await browser.newPage();
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });
    });
    await page.setUserAgent(
      opts.userAgent ||
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    );

    if (opts.regionalWarmupUrl) {
      await page.goto(opts.regionalWarmupUrl, {
        waitUntil: "networkidle2",
        timeout: opts.timeoutMs || 120000,
      });
    }

    await page.goto(pageUrl, { waitUntil: "networkidle2", timeout: opts.timeoutMs || 120000 });
    const html = await page.content();
    return {
      url: page.url(),
      status: 200,
      html,
      blocked: /access denied|robot check/i.test(html),
      source: CHOICE_CONTENT_SOURCE_PUPPETEER,
    };
  } finally {
    await browser.close();
  }
}

/**
 * Plain fetch → optional puppeteer with regional warm-up.
 * @param {string} pageUrl
 * @param {object} [opts]
 */
export async function fetchChoiceHotelAmenities(pageUrl, opts = {}) {
  const url = String(pageUrl || "").trim();
  if (!/^https:\/\/(www\.)?choicehotels\.com\//i.test(url)) {
    return {
      status: "invalid_url",
      amenities: [],
      amenitiesText: "",
      parseErrors: ["not_choice_url"],
      source: null,
    };
  }

  const fetchFn = opts.fetchFn || globalThis.fetch;
  const res = await fetchFn(url, { redirect: "follow", headers: FETCH_HEADERS });
  if (!res.ok) {
    return finalizeChoiceAmenityFetch({
      status: `http_${res.status}`,
      html: "",
      blocked: res.status === 403,
      source: null,
      parseErrors: [`http_${res.status}`],
      url,
      opts,
    });
  }

  const html = await res.text();
  const blocked = /access denied|captcha|robot check/i.test(html);
  if (blocked || html.length < 2000) {
    return finalizeChoiceAmenityFetch({
      status: blocked ? "blocked" : "empty",
      html,
      blocked,
      source: null,
      parseErrors: [blocked ? "access_denied" : "short_html"],
      url,
      opts,
    });
  }

  const parsed = parseChoiceAmenitiesFromHtml(html);
  if (parsed.amenities.length) {
    return { status: "ok", ...parsed, fetchMethod: "plain_fetch" };
  }

  return finalizeChoiceAmenityFetch({
    status: "empty",
    html,
    blocked: false,
    source: CHOICE_CONTENT_SOURCE,
    parseErrors: parsed.parseErrors,
    url,
    opts,
  });
}

async function finalizeChoiceAmenityFetch(ctx) {
  if (ctx.opts?.usePuppeteer !== true) {
    return {
      status: ctx.status,
      amenities: [],
      amenitiesText: "",
      parseErrors: ctx.parseErrors,
      source: null,
      fetchMethod: "plain_fetch",
    };
  }

  try {
    const regionalWarmupUrl = ctx.opts?.regionalWarmupUrl || inferChoiceRegionalWarmupUrl(ctx.url);
    const puppet = await fetchChoiceHotelHtmlPuppeteer(ctx.url, {
      regionalWarmupUrl,
      timeoutMs: ctx.opts?.timeoutMs,
      headless: ctx.opts?.headed ? false : "new",
    });
    if (puppet.blocked) {
      return {
        status: "blocked",
        amenities: [],
        amenitiesText: "",
        parseErrors: ["puppeteer_access_denied"],
        source: null,
        fetchMethod: "puppeteer",
      };
    }
    const parsed = parseChoiceAmenitiesFromHtml(puppet.html);
    return {
      status: parsed.amenities.length ? "ok" : "empty",
      ...parsed,
      fetchMethod: "puppeteer",
    };
  } catch (err) {
    return {
      status: ctx.status || "error",
      amenities: [],
      amenitiesText: "",
      parseErrors: [...ctx.parseErrors, `puppeteer:${err?.message || err}`],
      source: null,
      fetchMethod: "plain_fetch",
    };
  }
}

/** @param {string} propertyUrl */
function inferChoiceRegionalWarmupUrl(propertyUrl) {
  const path = String(propertyUrl).replace(/^https?:\/\/[^/]+/i, "");
  const segment = path.split("/").filter(Boolean)[0];
  if (!segment) return "";
  const placeId =
    segment.toLowerCase() === "mexico" ? "ChIJU1NoiDs6BIQREZgJa760ZO0" : "";
  return `https://www.choicehotels.com/en-uk/${segment}/regional-hotels${placeId ? `?placeId=${placeId}` : ""}`;
}

/** @param {string} url */
export function choicePropertyIdFromUrl(url) {
  const m = String(url || "").match(/\/([a-z0-9]{3,8})$/i);
  return m ? m[1].toLowerCase() : "";
}

export function formatChoiceAmenitiesText(amenities) {
  return dedupeLabels(amenities).join("; ");
}
