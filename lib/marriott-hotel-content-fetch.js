/**
 * Fetch and parse Marriott hotel Overview + Amenities from marriott.com HWS pages.
 *
 * Overview pages are Akamai-blocked for plain fetch from many IPs; use puppeteer when available.
 */

import { load as loadCheerio } from "cheerio";
import { formatMarriottAmenitiesText } from "./marriott-amenity-format.js";
import {
  MARRIOTT_FETCH_HEADERS,
  MARRIOTT_ORIGIN,
  marshaFromMarriottWebsite,
  parseNextDataFromHtml,
} from "./marriott-brand-directory-extract.js";

export const MARRIOTT_CONTENT_SOURCE_OVERVIEW_HTML = "marriott_overview_html";
export const MARRIOTT_CONTENT_SOURCE_PUPPETEER = "marriott_overview_puppeteer";
export const MARRIOTT_CONTENT_SOURCE_EXPORT = "marriott_content_export";

const OVERVIEW_HEADING_RE = /^overview$/i;
const AMENITIES_HEADING_RE = /^amenities$/i;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} websiteOrMarsha
 */
export function marriottOverviewUrlFromWebsite(websiteOrMarsha) {
  const raw = String(websiteOrMarsha || "").trim();
  if (!raw) return "";
  if (!/^https?:\/\//i.test(raw)) return "";
  const base = raw
    .replace(/\/+$/, "")
    .replace(/\/(overview|experiences|rooms|dining|events|photos|reviews)\/?$/i, "");
  if (!/\/hotels\/[a-z0-9]+-/i.test(base)) return "";
  return `${base}/overview/`;
}

/**
 * @param {string} website
 */
export function marriottHotelSlugFromWebsite(website) {
  const m = String(website || "").match(/\/hotels\/([a-z0-9]+(?:-[a-z0-9-]+)*)\//i);
  return m ? m[1] : "";
}

/**
 * @param {string} html
 */
export function parseMarriottOverviewHtml(html) {
  const text = String(html || "");
  if (!text || /access denied/i.test(text)) {
    return { description: "", amenities: [], amenitiesText: "", parseErrors: ["access_denied_or_empty"] };
  }

  /** @type {string[]} */
  const parseErrors = [];
  let description = "";
  /** @type {string[]} */
  const amenities = [];

  // __NEXT_DATA__ (Next.js overview pages)
  const nextMatch = text.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
  if (nextMatch) {
    try {
      const data = JSON.parse(nextMatch[1]);
      const str = JSON.stringify(data?.props?.pageProps || data?.props || {});
      const overviewFromJson =
        walkFindFirstString(data, (k, v) =>
          /^(overview|overviewText|propertyOverview|hotelOverview|description)$/i.test(k) &&
          typeof v === "string" &&
          v.length >= 40 &&
          !/^https?:\/\//i.test(v)
        ) ||
        str.match(/"(?:overview|overviewText|propertyOverview)"\s*:\s*"([^"\\]{40,800})"/)?.[1];
      if (overviewFromJson) description = decodeJsonString(overviewFromJson);

      const amenityArrays = walkFindArrays(data, (k, arr) =>
        /amenit/i.test(k) &&
        arr.length >= 3 &&
        arr.every((x) => typeof x === "string" || (x && typeof x.name === "string"))
      );
      for (const arr of amenityArrays) {
        for (const item of arr) {
          const label = typeof item === "string" ? item : String(item?.name || item?.label || "").trim();
          if (label) amenities.push(label);
        }
      }
    } catch (err) {
      parseErrors.push(`next_data_parse:${err?.message || err}`);
    }
  }

  const $ = loadCheerio(text);

  if (!description) {
    description = extractSectionParagraph($, OVERVIEW_HEADING_RE);
  }
  if (!amenities.length) {
    amenities.push(...extractSectionListItems($, AMENITIES_HEADING_RE));
  }

  // Chip / pill patterns on overview
  if (!amenities.length) {
    $("[class*='amenit' i] li, [class*='amenit' i] .t-font-s, [data-testid*='amenit' i]")
      .each((_, el) => {
        const label = cleanText($(el).text());
        if (label && label.length <= 120) amenities.push(label);
      });
  }

  // Meta description fallback (never preferred over Overview body)
  if (!description) {
    const meta = $("meta[name='description']").attr("content") || $("meta[property='og:description']").attr("content");
    const metaText = cleanText(meta);
    if (metaText.length >= 40 && !/indulge in moments of luxury and refined experiences/i.test(metaText)) {
      description = metaText;
      parseErrors.push("description_from_meta_fallback");
    }
  }

  const dedupedAmenities = dedupeLabels(amenities);
  return {
    description: cleanText(description),
    amenities: dedupedAmenities,
    amenitiesText: formatMarriottAmenitiesText(dedupedAmenities),
    parseErrors,
  };
}

function cleanText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .replace(/\u00a0/g, " ")
    .trim();
}

function decodeJsonString(value) {
  try {
    return cleanText(JSON.parse(`"${String(value).replace(/"/g, '\\"')}"`));
  } catch {
    return cleanText(
      String(value || "")
        .replace(/\\n/g, " ")
        .replace(/\\"/g, '"')
        .replace(/\\u0026/g, "&")
    );
  }
}

function dedupeLabels(labels) {
  const seen = new Set();
  /** @type {string[]} */
  const out = [];
  for (const raw of labels) {
    const label = cleanText(raw);
    if (!label || label.length < 2) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  return out;
}

/**
 * @param {import('cheerio').CheerioAPI} $
 * @param {RegExp} headingRe
 */
function extractSectionParagraph($, headingRe) {
  const headings = $("h1,h2,h3,h4,.heading-component__headline,[class*='headline']").toArray();
  for (const el of headings) {
    const title = cleanText($(el).text());
    if (!headingRe.test(title)) continue;
    const section =
      $(el).closest("section, .cmp-container, .aem-GridColumn, [class*='overview']").first().length > 0
        ? $(el).closest("section, .cmp-container, .aem-GridColumn, [class*='overview']").first()
        : $(el).parent();
    const p =
      section.find("p").filter((_, p) => cleanText($(p).text()).length >= 40).first().text() ||
      $(el).nextAll("p").first().text() ||
      section.find("[class*='description'], [class*='body-copy']").first().text();
    const desc = cleanText(p);
    if (desc.length >= 40) return desc;
  }
  return "";
}

/**
 * @param {import('cheerio').CheerioAPI} $
 * @param {RegExp} headingRe
 */
function extractSectionListItems($, headingRe) {
  /** @type {string[]} */
  const items = [];
  const headings = $("h1,h2,h3,h4,.heading-component__headline,[class*='headline']").toArray();
  for (const el of headings) {
    const title = cleanText($(el).text());
    if (!headingRe.test(title)) continue;
    const section =
      $(el).closest("section, .cmp-container, .aem-GridColumn, [class*='amenit']").first().length > 0
        ? $(el).closest("section, .cmp-container, .aem-GridColumn, [class*='amenit']").first()
        : $(el).parent().parent();
    section.find("li, [class*='amenity' i], [class*='pill' i], [class*='chip' i]").each((_, node) => {
      const label = cleanText($(node).text());
      if (label && label.length <= 120 && !headingRe.test(label)) items.push(label);
    });
    if (items.length) break;
  }
  return items;
}

function walkFindFirstString(obj, pred, depth = 0) {
  if (!obj || depth > 24) return "";
  if (Array.isArray(obj)) {
    for (const x of obj) {
      const hit = walkFindFirstString(x, pred, depth + 1);
      if (hit) return hit;
    }
    return "";
  }
  if (typeof obj !== "object") return "";
  for (const [k, v] of Object.entries(obj)) {
    if (typeof v === "string" && pred(k, v)) return v;
    const hit = walkFindFirstString(v, pred, depth + 1);
    if (hit) return hit;
  }
  return "";
}

function walkFindArrays(obj, pred, depth = 0, out = []) {
  if (!obj || depth > 24) return out;
  if (Array.isArray(obj)) {
    if (pred("", obj)) out.push(obj);
    for (const x of obj) walkFindArrays(x, pred, depth + 1, out);
    return out;
  }
  if (typeof obj !== "object") return out;
  for (const [k, v] of Object.entries(obj)) {
    if (Array.isArray(v) && pred(k, v)) out.push(v);
    walkFindArrays(v, pred, depth + 1, out);
  }
  return out;
}

/**
 * @param {string} url
 */
export async function fetchMarriottOverviewHtmlPlain(url) {
  const res = await fetch(url, {
    headers: {
      ...MARRIOTT_FETCH_HEADERS,
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    },
    redirect: "follow",
  });
  const html = await res.text();
  return {
    url: res.url || url,
    status: res.status,
    html,
    accessDenied: res.status === 403 || /access denied/i.test(html),
  };
}

/**
 * @param {string} url
 * @param {object} [opts]
 */
export async function fetchMarriottOverviewHtmlPuppeteer(url, opts = {}) {
  let puppeteer;
  try {
    puppeteer = await import("puppeteer");
  } catch (err) {
    throw new Error(`puppeteer unavailable: ${err?.message || err}`);
  }

  const browser = await puppeteer.launch({
    headless: opts.headless !== false ? "new" : false,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  });

  try {
    const page = await browser.newPage();
    await page.setUserAgent(
      opts.userAgent ||
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    );
    await page.setExtraHTTPHeaders({ "Accept-Language": "en-US,en;q=0.9" });
    await page.goto(url, { waitUntil: "networkidle2", timeout: opts.timeoutMs || 90000 });
    if (opts.waitMs) await sleep(opts.waitMs);
    const html = await page.content();
    return {
      url: page.url(),
      status: 200,
      html,
      accessDenied: /access denied/i.test(html),
      source: MARRIOTT_CONTENT_SOURCE_PUPPETEER,
    };
  } finally {
    await browser.close();
  }
}

/**
 * @param {string} websiteOrMarsha
 * @param {object} [opts]
 */
export async function fetchMarriottHotelContent(websiteOrMarsha, opts = {}) {
  const overviewUrl = marriottOverviewUrlFromWebsite(websiteOrMarsha);
  const marsha =
    String(opts.marshaCode || "").trim().toUpperCase() ||
    marshaFromMarriottWebsite(overviewUrl) ||
    marshaFromMarriottWebsite(websiteOrMarsha);

  if (!overviewUrl) {
    return {
      marshaCode: marsha,
      overviewUrl: "",
      source: "",
      fetchStatus: 0,
      accessDenied: true,
      description: "",
      amenities: [],
      amenitiesText: "",
      errors: ["missing_overview_url"],
    };
  }

  /** @type {{ html: string, status: number, accessDenied: boolean, source: string, url: string }} */
  let fetched = { html: "", status: 0, accessDenied: true, source: "", url: overviewUrl };

  if (opts.html) {
    fetched = {
      html: String(opts.html),
      status: 200,
      accessDenied: false,
      source: MARRIOTT_CONTENT_SOURCE_EXPORT,
      url: overviewUrl,
    };
  } else if (opts.usePuppeteer) {
    const p = await fetchMarriottOverviewHtmlPuppeteer(overviewUrl, opts);
    fetched = {
      html: p.html,
      status: p.status,
      accessDenied: p.accessDenied,
      source: MARRIOTT_CONTENT_SOURCE_PUPPETEER,
      url: p.url,
    };
  } else {
    const p = await fetchMarriottOverviewHtmlPlain(overviewUrl);
    fetched = {
      html: p.html,
      status: p.status,
      accessDenied: p.accessDenied,
      source: MARRIOTT_CONTENT_SOURCE_OVERVIEW_HTML,
      url: p.url,
    };
    if (p.accessDenied && opts.fallbackPuppeteer !== false) {
      try {
        const pp = await fetchMarriottOverviewHtmlPuppeteer(overviewUrl, opts);
        if (!pp.accessDenied) {
          fetched = {
            html: pp.html,
            status: pp.status,
            accessDenied: false,
            source: MARRIOTT_CONTENT_SOURCE_PUPPETEER,
            url: pp.url,
          };
        }
      } catch (err) {
        fetched.accessDenied = true;
        fetched.html = "";
        fetched.source = "fetch_failed";
        return {
          marshaCode: marsha,
          overviewUrl,
          source: fetched.source,
          fetchStatus: p.status,
          accessDenied: true,
          description: "",
          amenities: [],
          amenitiesText: "",
          errors: [`plain_403`, `puppeteer:${err?.message || err}`],
        };
      }
    }
  }

  const parsed = parseMarriottOverviewHtml(fetched.html);
  return {
    marshaCode: marsha,
    overviewUrl: fetched.url || overviewUrl,
    source: fetched.source,
    fetchStatus: fetched.status,
    accessDenied: fetched.accessDenied,
    description: parsed.description,
    amenities: parsed.amenities,
    amenitiesText: parsed.amenitiesText,
    errors: [
      ...(fetched.accessDenied ? ["access_denied"] : []),
      ...parsed.parseErrors,
      ...(!parsed.description ? ["missing_description"] : []),
      ...(!parsed.amenities.length ? ["missing_amenities"] : []),
    ],
  };
}

/**
 * Parse browser-exported JSON/HTML bundle.
 * @param {unknown} payload
 */
export function normalizeMarriottContentExport(payload) {
  if (!payload) return [];
  if (typeof payload === "string") {
    const parsed = parseMarriottOverviewHtml(payload);
    return [
      {
        marshaCode: marshaFromMarriottWebsite(payload) || "",
        description: parsed.description,
        amenitiesText: parsed.amenitiesText,
        website: "",
      },
    ].filter((r) => r.description || r.amenitiesText);
  }

  /** @type {object[]} */
  let rows = [];
  if (Array.isArray(payload)) rows = payload;
  else if (typeof payload === "object") {
    const p = /** @type {Record<string, unknown>} */ (payload);
    rows =
      /** @type {object[]} */ (p.hotels) ||
      /** @type {object[]} */ (p.properties) ||
      /** @type {object[]} */ (p.results) ||
      [];
    if (!rows.length && p.data) {
      const data = p.data;
      if (typeof data === "object") rows = [/** @type {object} */ (data)];
    }
    if (!rows.length && (p.html || p.overviewHtml)) {
      const html = String(p.html || p.overviewHtml);
      const parsed = parseMarriottOverviewHtml(html);
      return [
        {
          marshaCode: String(p.marsha || p.marshaCode || marshaFromMarriottWebsite(String(p.url || ""))).toUpperCase(),
          description: parsed.description,
          amenitiesText: parsed.amenitiesText,
          website: String(p.url || p.website || "").trim(),
        },
      ];
    }
  }

  return rows
    .map((row) => {
      const website = String(row.url || row.website || row.overviewUrl || "").trim();
      const marsha = String(
        row.marsha || row.marshaCode || row.propertyCode || marshaFromMarriottWebsite(website)
      ).toUpperCase();
      let description = String(
        row.description || row.overview || row.hotelDescription || row.overviewText || ""
      ).trim();
      let amenitiesText = Array.isArray(row.amenities)
        ? row.amenities.map((a) => (typeof a === "string" ? a : a?.name || "")).filter(Boolean).join(", ")
        : String(row.amenitiesText || "").trim();

      if (!description && row.html) {
        const parsed = parseMarriottOverviewHtml(String(row.html));
        description = parsed.description;
        if (!amenitiesText) amenitiesText = parsed.amenitiesText;
      }

      if (!description && !amenitiesText) return null;
      return { marshaCode: marsha, description, amenitiesText, website };
    })
    .filter(Boolean);
}

export { parseNextDataFromHtml };
