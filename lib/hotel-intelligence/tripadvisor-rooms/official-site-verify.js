/**
 * Official hotel/brand website room-count verifier.
 * Small capped path crawl — not a broad site crawler.
 */

import { fetchResearchPage, htmlToSearchableText } from "../room-count-research/fetch.js";
import { extractRoomCountsFromText } from "../room-count-research/extract.js";
import {
  isFetchEligibleUrl,
  classifySourceUrl,
  SOURCE_CATEGORIES,
} from "../room-count-research/trust.js";

export const OFFICIAL_SITE_VERIFY_VERSION = "official-site-room-verify-v1";

/** High-probability relative paths (CALA EN/ES). */
export const OFFICIAL_ROOM_PATHS = Object.freeze([
  "",
  "/about",
  "/about-us",
  "/hotel",
  "/the-hotel",
  "/rooms",
  "/accommodations",
  "/accommodation",
  "/stay",
  "/suites",
  "/meetings",
  "/events",
  "/press",
  "/media",
  "/news",
  "/facts",
  "/fact-sheet",
  "/factsheet",
  "/ficha-tecnica",
  "/ficha-del-hotel",
  "/el-hotel",
  "/habitaciones",
  "/alojamiento",
  "/alojamientos",
  "/sitemap",
  "/sitemap.xml",
]);

function originOf(url) {
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.host}`;
  } catch {
    return null;
  }
}

function pageTitle(html) {
  const m = String(html || "").match(/<title[^>]*>([^<]{1,200})<\/title>/i);
  return m ? m[1].replace(/\s+/g, " ").trim() : null;
}

function isPdfUrl(url) {
  return /\.pdf(\?|#|$)/i.test(String(url || ""));
}

/**
 * Composition totals: "120 guestrooms and 30 suites" → 150
 * Only when wording clearly lists mutually exclusive components.
 */
export function extractCompositionRoomTotals(text) {
  const t = String(text || "");
  const hits = [];
  const patterns = [
    [
      /(\d{1,4})\s+(?:guest\s*)?rooms?\s+and\s+(\d{1,4})\s+suites?\b/gi,
      "composition_rooms_and_suites",
    ],
    [
      /(\d{1,4})\s+guestrooms?\s+and\s+(\d{1,4})\s+suites?\b/gi,
      "composition_guestrooms_and_suites",
    ],
    [
      /(\d{1,4})\s+habitaciones?\s+y\s+(\d{1,4})\s+suites?\b/gi,
      "composition_habitaciones_y_suites",
    ],
    [
      /(\d{1,4})\s+habitaciones?\s+y\s+(\d{1,4})\s+(?:villas?|bungalows?)\b/gi,
      "composition_habitaciones_y_villas",
    ],
    [
      /(\d{1,4})\s+(?:guest\s*)?rooms?\s+(?:and|&)\s+(\d{1,4})\s+(?:villas?|residences?)\b/gi,
      "composition_rooms_and_villas",
    ],
    [
      /comprises?\s+(\d{1,4})\s+(?:guest\s*)?rooms?\s+and\s+(\d{1,4})\s+suites?\b/gi,
      "composition_comprises_rooms_suites",
    ],
    [
      /cuenta\s+con\s+(\d{1,4})\s+habitaciones?\s+y\s+(\d{1,4})\s+suites?\b/gi,
      "composition_cuenta_con_habitaciones_suites",
    ],
  ];
  for (const [rx, method] of patterns) {
    rx.lastIndex = 0;
    let m;
    while ((m = rx.exec(t)) !== null) {
      const a = Number(m[1]);
      const b = Number(m[2]);
      if (!Number.isFinite(a) || !Number.isFinite(b)) continue;
      if (a < 1 || b < 1 || a > 5000 || b > 5000) continue;
      const total = a + b;
      if (total < 5 || total > 5000) continue;
      hits.push({
        count: total,
        method,
        confidence: "High",
        quote: m[0].slice(0, 160),
        parts: [a, b],
        rejected: false,
      });
    }
  }
  return hits;
}

function collectPdfLinks(html, baseUrl) {
  const origin = originOf(baseUrl);
  if (!origin) return [];
  const out = [];
  const re = /href=["']([^"']+\.pdf[^"']*)["']/gi;
  let m;
  while ((m = re.exec(String(html || ""))) !== null) {
    try {
      const abs = new URL(m[1], baseUrl).href;
      if (isFetchEligibleUrl(abs)) out.push(abs);
    } catch {
      /* skip */
    }
    if (out.length >= 3) break;
  }
  return out;
}

function toEvidence(hit, meta) {
  return {
    value: Number(hit.count),
    source_category: meta.source_category,
    source_domain: meta.source_domain,
    url: meta.url,
    page_title: meta.page_title || null,
    quote: hit.quote || null,
    evidence_text: hit.quote || null,
    retrieval_timestamp: meta.retrieved_at,
    extraction_method: hit.method,
    confidence: hit.confidence || "High",
    provider: "official_site",
    is_official_hotel_site: meta.is_official_hotel_site,
    is_official_brand_site: meta.is_official_brand_site,
    is_pdf_factsheet: meta.is_pdf_factsheet,
    source_provider: "official_site",
    upstream_source_if_known: null,
    independence_confidence: 0.95,
  };
}

/**
 * Crawl a small set of official pages for explicit room-count evidence.
 * @param {string} website
 * @param {{ hotelName?: string, brand?: string, maxPages?: number, stopOnHit?: boolean }} [opts]
 */
export async function verifyOfficialWebsiteRoomCount(website, opts = {}) {
  const retrieved_at = new Date().toISOString();
  const steps = [];
  /** @type {Array<object>} */
  const evidence = [];
  let official_website_room_count_found = false;
  let official_pdf_factsheet_found = false;

  const base = String(website || "").trim();
  if (!base || !isFetchEligibleUrl(base) || /tripadvisor\./i.test(base)) {
    return {
      version: OFFICIAL_SITE_VERIFY_VERSION,
      evidence: [],
      steps: [{ step: "official_skip", reason: "no_eligible_website" }],
      official_website_room_count_found: false,
      official_pdf_factsheet_found: false,
      pages_fetched: 0,
    };
  }

  const origin = originOf(base);
  const maxPages = Math.min(10, Math.max(1, Number(opts.maxPages ?? 8)));
  const urls = [];
  const seen = new Set();
  function pushUrl(u) {
    const s = String(u || "").split("#")[0];
    if (!s || seen.has(s) || !isFetchEligibleUrl(s)) return;
    if (/tripadvisor\.|booking\.com|expedia\.|hotels\.com|agoda\./i.test(s)) return;
    seen.add(s);
    urls.push(s);
  }

  pushUrl(base);
  if (origin) {
    for (const p of OFFICIAL_ROOM_PATHS) {
      if (!p) continue;
      pushUrl(`${origin}${p}`);
    }
  }

  let pages_fetched = 0;
  const pdfQueue = [];

  for (const url of urls) {
    if (pages_fetched >= maxPages) break;
    pages_fetched += 1;
    steps.push({ step: "official_fetch", url });
    const page = await fetchResearchPage(url, { timeoutMs: opts.timeoutMs ?? 20000 });
    if (!page.ok) {
      steps.push({
        step: "official_fetch_failed",
        url,
        status: page.status,
        error: page.error || (page.blocked ? "blocked" : "fail"),
      });
      continue;
    }

    const cat = classifySourceUrl(page.url, {
      hotelWebsite: base,
      brand: opts.brand,
    });
    const isHotel =
      cat === SOURCE_CATEGORIES.OFFICIAL_HOTEL ||
      cat === SOURCE_CATEGORIES.OFFICIAL_OPERATOR;
    const isBrand = cat === SOURCE_CATEGORIES.OFFICIAL_BRAND;
    const domain = (() => {
      try {
        return new URL(page.url).hostname.replace(/^www\./, "");
      } catch {
        return null;
      }
    })();
    const title = pageTitle(page.text);
    const searchable = htmlToSearchableText(page.text);
    const extracted = extractRoomCountsFromText(page.text, { url: page.url });
    const composition = extractCompositionRoomTotals(searchable);
    const hits = [
      ...(extracted.hits || []).filter((h) => !h.rejected),
      ...composition,
    ];

    const meta = {
      source_category: cat,
      source_domain: domain,
      url: page.url,
      page_title: title,
      retrieved_at,
      is_official_hotel_site: isHotel,
      is_official_brand_site: isBrand,
      is_pdf_factsheet: isPdfUrl(page.url),
    };

    for (const hit of hits) {
      if (!Number.isFinite(Number(hit.count)) || Number(hit.count) <= 0) continue;
      // Only accept official-tier categories from this crawler
      if (
        ![
          SOURCE_CATEGORIES.OFFICIAL_HOTEL,
          SOURCE_CATEGORIES.OFFICIAL_BRAND,
          SOURCE_CATEGORIES.OFFICIAL_OWNER,
          SOURCE_CATEGORIES.OFFICIAL_OPERATOR,
        ].includes(cat) &&
        !isPdfUrl(page.url)
      ) {
        // Still allow if path is on same origin as hotel website
        const sameOrigin = origin && page.url.startsWith(origin);
        if (!sameOrigin) continue;
        meta.source_category = SOURCE_CATEGORIES.OFFICIAL_HOTEL;
        meta.is_official_hotel_site = true;
      }
      evidence.push(toEvidence(hit, meta));
      official_website_room_count_found = true;
    }

    if (isPdfUrl(page.url) && hits.length) official_pdf_factsheet_found = true;

    for (const pdf of collectPdfLinks(page.text, page.url)) {
      pdfQueue.push(pdf);
    }

    if (opts.stopOnHit !== false && evidence.length > 0) {
      // Prefer continuing a couple more high-value paths if only weak hits
      const high = evidence.some((e) => e.confidence === "High" || e.confidence === "high");
      if (high && pages_fetched >= 2) break;
    }
  }

  // Fact-sheet PDFs (text-extractable only; skip opaque binary quietly)
  for (const pdf of pdfQueue.slice(0, 2)) {
    if (pages_fetched >= maxPages) break;
    pages_fetched += 1;
    steps.push({ step: "official_pdf_fetch", url: pdf });
    const page = await fetchResearchPage(pdf, { timeoutMs: opts.timeoutMs ?? 25000 });
    if (!page.ok) continue;
    const searchable = htmlToSearchableText(page.text);
    // If mostly binary garbage, skip
    if (searchable.length < 40 && page.text.length > 5000) {
      steps.push({ step: "official_pdf_not_text", url: pdf });
      continue;
    }
    const extracted = extractRoomCountsFromText(searchable || page.text, { url: pdf });
    const composition = extractCompositionRoomTotals(searchable);
    const hits = [
      ...(extracted.hits || []).filter((h) => !h.rejected),
      ...composition,
    ];
    if (!hits.length) continue;
    official_pdf_factsheet_found = true;
    official_website_room_count_found = true;
    const domain = (() => {
      try {
        return new URL(pdf).hostname.replace(/^www\./, "");
      } catch {
        return null;
      }
    })();
    for (const hit of hits) {
      evidence.push(
        toEvidence(hit, {
          source_category: SOURCE_CATEGORIES.OFFICIAL_HOTEL,
          source_domain: domain,
          url: pdf,
          page_title: null,
          retrieved_at,
          is_official_hotel_site: true,
          is_official_brand_site: false,
          is_pdf_factsheet: true,
        })
      );
    }
  }

  return {
    version: OFFICIAL_SITE_VERIFY_VERSION,
    evidence,
    steps,
    official_website_room_count_found,
    official_pdf_factsheet_found,
    pages_fetched,
  };
}
