/**
 * Marriott DAM factsheet URL discovery + PDF revalidation.
 *
 * Webhound may discover candidate URLs; this module re-fetches the official
 * Marriott DAM PDF and extracts Level 2 fields. Webhound is never Census SoT.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PDFParse } from "pdf-parse";
import {
  MARRIOTT_DAM_BRAND_PREFIX,
  buildMarriottDamFactsheetUrlCandidate,
  extractFromFactsheetText,
  validateMarriottRoomsCandidate,
} from "./marriott-factsheet-adapter.js";
import { isForbiddenPhoneSourceUrl } from "./census-phone-number-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const MARRIOTT_DAM_DISCOVERY_VERSION =
  "marriott-dam-factsheet-discovery-v1";

const BROWSER_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

/**
 * Documented exception / evidence map — official Marriott DAM URLs only.
 * Seeded from Webhound pattern discovery; each entry must be revalidated.
 * Do not treat Webhound narrative as Census SoT.
 */
export const MARRIOTT_DAM_FACTSHEET_SEED_INDEX = Object.freeze({
  SJULU: Object.freeze({
    marsha: "SJULU",
    brand: "Fairfield by Marriott",
    property: "Fairfield by Marriott Luquillo Beach",
    url: "https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf",
    filename_kind: "fact-sheet",
    evidence_note: "Webhound-confirmed official DAM factsheet; revalidate before write",
    webhound_as_sot: false,
  }),
  GYECY: Object.freeze({
    marsha: "GYECY",
    brand: "Courtyard",
    property: "Courtyard by Marriott Guayaquil",
    url: "https://www.marriott.com/content/dam/marriott-digital/cy/cala/hws/g/gyecy/en_us/document/assets/cy-gyecy-11-23-hotel-fact-sheet-19316.pdf",
    filename_kind: "hotel-fact-sheet",
    evidence_note: "Webhound-confirmed official DAM hotel factsheet; revalidate before write",
    webhound_as_sot: false,
  }),
});

/** Reject meeting-event / wedding brochures as primary rooms sources. */
export function isGuestFactsheetFilename(urlOrName = "") {
  const s = String(urlOrName || "").toLowerCase();
  if (!/\.pdf($|\?)/i.test(s)) return false;
  if (/meeting-event|event-fact-sheet|wedding-brochure|menus?/i.test(s)) {
    return false;
  }
  return /fact-sheet|hotel-fact-sheet|guest-rooms?-brochure/i.test(s);
}

export function marshaFromDamUrl(url = "") {
  const u = String(url || "");
  const m = u.match(/\/hws\/[a-z0-9]\/([a-z0-9]{4,6})\//i);
  if (m) return m[1].toUpperCase();
  const n = u.match(/\/([a-z]{2})-([a-z0-9]{4,6})-/i);
  return n ? n[2].toUpperCase() : null;
}

/**
 * Parse Marriott DAM PDF URLs from Webhound (or any) report text.
 * Returns candidates only — never marked as Census SoT.
 */
export function extractDamUrlsFromText(text = "") {
  const raw = String(text || "");
  const urls = [
    ...raw.matchAll(
      /https?:\/\/[^\s)"'<>]*marriott\.com\/content\/dam\/marriott-digital[^\s)"'<>]*\.pdf/gi
    ),
  ].map((m) => m[0].replace(/[.,;]+$/, "").replace(/&amp;/g, "&"));
  const unique = [...new Set(urls)];
  return unique.map((url) => {
    const marsha = marshaFromDamUrl(url);
    const guest = isGuestFactsheetFilename(url);
    return {
      url,
      marsha,
      guest_factsheet: guest,
      source_type: "official_factsheet",
      webhound_as_sot: false,
      requires_underlying_revalidation: true,
      discovery_source: "webhound_or_report_text",
    };
  });
}

/**
 * Merge seed index + report-parsed URLs into MARSHA → preferred guest factsheet URL.
 */
export function buildDamFactsheetUrlIndex(opts = {}) {
  /** @type {Map<string, object>} */
  const byMarsha = new Map();

  for (const [marsha, row] of Object.entries(MARRIOTT_DAM_FACTSHEET_SEED_INDEX)) {
    byMarsha.set(marsha.toUpperCase(), {
      ...row,
      marsha: marsha.toUpperCase(),
      discovery_source: "documented_seed_exception_map",
      webhound_as_sot: false,
    });
  }

  const texts = [];
  if (opts.reportText) texts.push(String(opts.reportText));
  if (opts.reportPath && fs.existsSync(opts.reportPath)) {
    texts.push(fs.readFileSync(opts.reportPath, "utf8"));
  }
  const defaultReport = path.join(
    ROOT,
    "reports/research-engine-v2/marriott-webhound-source-patterns-report.md"
  );
  if (!opts.skipDefaultReport && fs.existsSync(defaultReport)) {
    texts.push(fs.readFileSync(defaultReport, "utf8"));
  }

  for (const text of texts) {
    for (const hit of extractDamUrlsFromText(text)) {
      if (!hit.marsha || !hit.guest_factsheet) continue;
      if (byMarsha.has(hit.marsha)) continue;
      byMarsha.set(hit.marsha, {
        marsha: hit.marsha,
        url: hit.url,
        filename_kind: "fact-sheet",
        discovery_source: hit.discovery_source,
        webhound_as_sot: false,
        evidence_note: "Parsed from pattern report; revalidate before write",
      });
    }
  }

  if (opts.extraUrls) {
    for (const u of opts.extraUrls) {
      const url = typeof u === "string" ? u : u?.url;
      const marsha = (typeof u === "object" && u?.marsha) || marshaFromDamUrl(url);
      if (!url || !marsha || !isGuestFactsheetFilename(url)) continue;
      if (!byMarsha.has(String(marsha).toUpperCase())) {
        byMarsha.set(String(marsha).toUpperCase(), {
          marsha: String(marsha).toUpperCase(),
          url,
          discovery_source: "extra_url",
          webhound_as_sot: false,
        });
      }
    }
  }

  return {
    version: MARRIOTT_DAM_DISCOVERY_VERSION,
    webhound_as_census_sot: false,
    count: byMarsha.size,
    by_marsha: Object.fromEntries(byMarsha),
  };
}

export function resolveDamFactsheetUrlForMarsha(marsha, index = null) {
  const code = String(marsha || "").trim().toUpperCase();
  if (!code) return { ok: false, reason: "missing_marsha" };
  const idx = index || buildDamFactsheetUrlIndex();
  const row = idx.by_marsha?.[code];
  if (!row?.url) {
    return {
      ok: false,
      reason: "dam_url_not_in_index",
      marsha: code,
      webhound_as_sot: false,
      next: "expand_dam_index_via_webhound_or_official_link_harvest",
    };
  }
  return {
    ok: true,
    marsha: code,
    url: row.url,
    discovery_source: row.discovery_source,
    webhound_as_sot: false,
    requires_underlying_revalidation: true,
  };
}

/**
 * Fetch official DAM PDF bytes (GET + browser UA; HEAD is unreliable on this CDN).
 */
export async function fetchMarriottDamPdf(url, opts = {}) {
  const target = String(url || "").trim();
  if (!target || isForbiddenPhoneSourceUrl(target)) {
    return { ok: false, reason: "forbidden_or_blank_url", webhound_as_sot: false };
  }
  if (!/marriott\.com\/content\/dam\/marriott-digital/i.test(target)) {
    return { ok: false, reason: "not_marriott_dam_url", webhound_as_sot: false };
  }
  const timeoutMs = opts.timeoutMs || 30000;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(target, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: {
        "User-Agent": BROWSER_UA,
        Accept: "application/pdf,*/*",
      },
    });
    if (!res.ok) {
      return {
        ok: false,
        reason: `http_${res.status}`,
        status: res.status,
        url: target,
        webhound_as_sot: false,
      };
    }
    const ctype = String(res.headers.get("content-type") || "");
    const buf = Buffer.from(await res.arrayBuffer());
    const isPdf =
      /pdf/i.test(ctype) || buf.slice(0, 5).toString("utf8") === "%PDF-";
    if (!isPdf || buf.length < 500) {
      return {
        ok: false,
        reason: "not_pdf_body",
        status: res.status,
        content_type: ctype,
        bytes: buf.length,
        url: target,
        webhound_as_sot: false,
      };
    }
    return {
      ok: true,
      url: target,
      buffer: buf,
      bytes: buf.length,
      content_type: ctype,
      webhound_as_sot: false,
    };
  } catch (err) {
    return {
      ok: false,
      reason: err?.name === "AbortError" ? "timeout" : "network_error",
      error: err?.message || String(err),
      url: target,
      webhound_as_sot: false,
    };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Extract text from Marriott DAM PDF buffer via pdf-parse.
 */
export async function extractTextFromMarriottDamPdf(buffer) {
  const parser = new PDFParse({ data: buffer });
  try {
    const result = await parser.getText();
    const text = String(result?.text || "").trim();
    return {
      ok: text.length > 40,
      text,
      pages: result?.total ?? result?.pages?.length ?? null,
      webhound_as_sot: false,
    };
  } catch (err) {
    return {
      ok: false,
      reason: "pdf_parse_failed",
      error: err?.message || String(err),
      webhound_as_sot: false,
    };
  }
}

/**
 * Discover + revalidate DAM factsheet for a Census-like record.
 * Returns High field candidates only after official PDF revalidation.
 */
export async function discoverAndExtractMarriottDamFactsheet(record, opts = {}) {
  const f = record?.fields || record || {};
  const marsha = String(
    opts.marsha ||
      f["MARSHA Code"] ||
      f.MARSHA ||
      f["Property Code"] ||
      ""
  )
    .trim()
    .toUpperCase();
  const propertyName =
    opts.propertyName || f["Property Name"] || f["Canonical Property Name"];
  const brand = opts.brand || f["Current Brand"] || f.Brand;

  const index = opts.index || buildDamFactsheetUrlIndex(opts);
  let resolved = resolveDamFactsheetUrlForMarsha(marsha, index);

  // Optional: build candidate when caller supplies assetId
  if (!resolved.ok && opts.assetId) {
    const built = buildMarriottDamFactsheetUrlCandidate({
      marsha,
      brand,
      brandPrefix: opts.brandPrefix,
      assetId: opts.assetId,
      region: opts.region || "cala",
    });
    if (built.ok) {
      resolved = {
        ok: true,
        marsha,
        url: built.url,
        discovery_source: "constructed_with_asset_id",
        webhound_as_sot: false,
        requires_underlying_revalidation: true,
      };
    }
  }

  if (!resolved.ok) {
    return {
      ok: false,
      reason: resolved.reason || "dam_url_unknown",
      marsha: marsha || null,
      webhound_as_sot: false,
      version: MARRIOTT_DAM_DISCOVERY_VERSION,
    };
  }

  const fetched = await fetchMarriottDamPdf(resolved.url, opts);
  if (!fetched.ok) {
    return {
      ok: false,
      reason: fetched.reason,
      url: resolved.url,
      marsha,
      discovery_source: resolved.discovery_source,
      webhound_as_sot: false,
      version: MARRIOTT_DAM_DISCOVERY_VERSION,
    };
  }

  const parsed = await extractTextFromMarriottDamPdf(fetched.buffer);
  if (!parsed.ok) {
    return {
      ok: false,
      reason: parsed.reason || "pdf_text_empty",
      url: resolved.url,
      marsha,
      webhound_as_sot: false,
      version: MARRIOTT_DAM_DISCOVERY_VERSION,
    };
  }

  const extracted = extractFromFactsheetText(parsed.text, {
    sourceUrl: resolved.url,
    propertyName,
  });

  // Strengthen rooms validation with PDF evidence context
  let rooms = extracted.rooms || null;
  if (!rooms?.ok) {
    const m =
      parsed.text.match(
        /\b(\d{2,4})\s+generously\s+sized\s+rooms?\b/i
      ) ||
      parsed.text.match(
        /\b(?:boasts|features|offers|with)\s+(\d{2,4})\s+(?:guest\s*)?rooms?\b/i
      ) ||
      parsed.text.match(
        /\b(\d{2,4})\s+(?:guest\s*)?(?:rooms?|guestrooms|keys)\b/i
      );
    if (m) {
      const v = validateMarriottRoomsCandidate({
        count: Number(m[1]),
        evidence: m[0],
        source_url: resolved.url,
        source_type: "official_factsheet",
        evidence_tier: "official_marriott_dam_pdf",
        property_name: propertyName,
        source_property_name: propertyName,
      });
      if (v.ok) rooms = v;
    }
  } else {
    rooms = {
      ...rooms,
      evidence_tier: "official_marriott_dam_pdf",
      source_type: "official_factsheet",
    };
  }

  const address =
    extracted.address?.ok ||
    (extracted.address && isStreetLevelAddress(extracted.address.address))
      ? extracted.address
      : null;

  return {
    ok: Boolean(rooms?.ok || address || extracted.phone?.ok),
    webhound_as_sot: false,
    version: MARRIOTT_DAM_DISCOVERY_VERSION,
    marsha,
    url: resolved.url,
    discovery_source: resolved.discovery_source,
    rooms: rooms?.ok ? rooms : null,
    address,
    phone: extracted.phone?.ok ? extracted.phone : null,
    text_sample: parsed.text.slice(0, 400),
  };
}

/**
 * Build Census field-completion patch from DAM extraction (High only).
 */
export function buildDamFactsheetCensusPatch(extraction, existingFields = {}) {
  const patch = {};
  const f = existingFields || {};
  const blank = (v) => v == null || !String(v).trim();

  if (extraction?.rooms?.ok && blank(f["Rooms / Keys"])) {
    patch["Rooms / Keys"] = extraction.rooms.count;
    patch["Rooms Confidence"] = "High";
    patch["Rooms Source URL"] = extraction.url;
    patch["Rooms Source Type"] = "official_factsheet";
    patch["Rooms Evidence Tier"] = "official_marriott_dam_pdf";
    patch["Rooms Reviewed Date"] = new Date().toISOString().slice(0, 10);
    patch["Rooms Review Status"] = "Autopilot High — Marriott DAM factsheet";
  }
  if (extraction?.address && blank(f.Address)) {
    const addr =
      extraction.address.address ||
      extraction.address.value ||
      extraction.address;
    if (typeof addr === "string" && isStreetLevelAddress(addr)) {
      patch.Address = addr;
      patch["Address Confidence"] = "High";
      patch["Address Source URL"] = extraction.url;
    }
  }
  if (extraction?.phone?.ok && blank(f.Phone) && blank(f["Phone Number"])) {
    const phone = extraction.phone.phone || extraction.phone.value;
    if (phone) {
      patch.Phone = phone;
    }
  }

  return {
    has_writes: Object.keys(patch).length > 0,
    patch,
    webhound_as_sot: false,
    source_url: extraction?.url || null,
  };
}

export function defaultDamIndexPath() {
  return path.join(
    ROOT,
    "reports/research-engine-v2/marriott-dam-factsheet-url-index.json"
  );
}

export function writeDamFactsheetUrlIndex(index, writePath = defaultDamIndexPath()) {
  fs.mkdirSync(path.dirname(writePath), { recursive: true });
  fs.writeFileSync(writePath, `${JSON.stringify(index, null, 2)}\n`, "utf8");
  return writePath;
}

export { MARRIOTT_DAM_BRAND_PREFIX, buildMarriottDamFactsheetUrlCandidate };
