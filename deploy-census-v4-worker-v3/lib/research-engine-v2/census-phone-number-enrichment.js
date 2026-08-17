/**
 * Hotel Property Census — Phone Number enrichment (official sources only).
 *
 * Field: Phone (production census field contract).
 * Never OTAs / Google Maps / Mapbox / third-party directories.
 * Never infer. Never overwrite a different existing phone (steward).
 * Never Brand Setup / Brand Explorer / VIC / owner-operator writes.
 */

import { extractDeepOfficialPageSignals } from "./clean-census/field-research.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";

export const PHONE_ENRICHMENT_VERSION = "census-phone-number-enrichment-v1";
export const PHONE_NUMBER_QUEUE_ID = "phone_number_enrichment";
export const PHONE_FIELD = "Phone";

/** Forbidden phone source families / hosts. */
export const FORBIDDEN_PHONE_SOURCE_HOSTS = Object.freeze([
  "booking.com",
  "expedia.com",
  "hotels.com",
  "tripadvisor.com",
  "google.com",
  "maps.google",
  "mapbox.com",
  "kayak.com",
  "agoda.com",
]);

export const PHONE_SOURCE_TYPES = Object.freeze({
  OFFICIAL_PROPERTY_PAGE: "official_property_page",
  OFFICIAL_BRAND_DIRECTORY: "official_brand_directory",
  OFFICIAL_JSON_LD: "official_json_ld",
  OFFICIAL_PARENT_API: "official_parent_api",
});

/** Choice central reservation / brand hotlines — never property-level Phone. */
export const CHOICE_CENTRAL_RESERVATION_PHONE_DIGITS = Object.freeze(
  new Set([
    "18887706800",
    "8887706800",
    "8663513033",
    "18663513033",
    "8774246423",
    "18774246423",
    "8003008800",
    "18003008800",
    "8002285050",
    "18002285050",
    "18005444444",
  ])
);

export function isChoiceCentralReservationPhone(phone) {
  const digits = String(phone || "").replace(/[^\d]/g, "");
  if (!digits) return false;
  if (CHOICE_CENTRAL_RESERVATION_PHONE_DIGITS.has(digits)) return true;
  // US toll-free brand lines often shared across Choice pages
  if (/^1?8(00|33|44|55|66|77|88)\d{7}$/.test(digits) && /choicehotels/i.test(String(phone))) {
    return true;
  }
  return false;
}

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

/**
 * Normalize property-level phone toward E.164-ish international format when digits allow.
 * Does not invent country codes — preserves leading + when present.
 * @param {string} raw
 */
export function normalizePhoneNumber(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  // Reject obviously non-phone
  if (/^https?:/i.test(s) || /@/.test(s)) return null;
  const hasPlus = s.trim().startsWith("+");
  const digits = s.replace(/[^\d]/g, "");
  if (digits.length < 8 || digits.length > 15) return null;
  if (hasPlus) {
    return `+${digits}`;
  }
  // Keep readable grouping only when no country code signal — still High if official
  return digits.length >= 10 ? digits : s.replace(/\s+/g, " ").trim();
}

/**
 * @param {string} url
 */
export function isForbiddenPhoneSourceUrl(url) {
  const raw = String(url || "").trim().toLowerCase();
  if (!raw) return false;
  let host = "";
  try {
    host = new URL(raw.includes("://") ? raw : `https://${raw}`).hostname;
  } catch {
    return false;
  }
  return FORBIDDEN_PHONE_SOURCE_HOSTS.some((h) => {
    const needle = String(h || "").toLowerCase();
    if (!needle) return false;
    // Host equality or subdomain (never substring of brand names like sirenishotels.com)
    return host === needle || host.endsWith(`.${needle}`);
  });
}

/**
 * Extract High-confidence phone from official HTML (tel: / JSON-LD telephone).
 * @param {string} html
 * @param {string} [url]
 */
export function extractOfficialPhoneFromHtml(html, url = "") {
  if (isForbiddenPhoneSourceUrl(url)) {
    return { ok: false, reason: "forbidden_third_party_source", phone: null };
  }
  const deep = extractDeepOfficialPageSignals(html, url);
  const jsonLd =
    String(html || "").match(/"telephone"\s*:\s*"([^"]+)"/i) ||
    String(html || "").match(/"telephone"\s*:\s*"([^"]+)"/i);
  const raw = deep.phone || (jsonLd ? jsonLd[1] : null);
  const normalized = normalizePhoneNumber(raw);
  if (!normalized) {
    return { ok: false, reason: "no_official_phone_in_page", phone: null };
  }
  if (isChoiceCentralReservationPhone(normalized)) {
    return {
      ok: false,
      reason: "choice_central_reservation_phone_rejected",
      phone: null,
      rejected_phone: normalized,
    };
  }
  return {
    ok: true,
    phone: normalized,
    source_type: jsonLd && !deep.phone
      ? PHONE_SOURCE_TYPES.OFFICIAL_JSON_LD
      : PHONE_SOURCE_TYPES.OFFICIAL_PROPERTY_PAGE,
    source_url: url || null,
    confidence: "High",
  };
}

/**
 * Classify phone write for one Census record.
 * @param {object} record
 * @param {{
 *   phoneFieldExists?: boolean,
 *   officialPhone?: string|null,
 *   officialPhoneSourceUrl?: string|null,
 *   officialPhoneSourceType?: string|null,
 *   pageHtml?: string|null,
 *   pageUrl?: string|null,
 * }} [opts]
 */
export function classifyPhoneEnrichment(record, opts = {}) {
  const fields = record?.fields || {};
  if (opts.phoneFieldExists === false) {
    return {
      action: "schema_missing",
      reason: "phone_field_missing",
      write_allowed: false,
    };
  }

  const clean = evaluateCleanCorePass(record, opts);
  if (!clean.pass) {
    return {
      action: "blocked_clean_core",
      reason: "clean_core_not_pass",
      write_allowed: false,
      clean_core: clean,
    };
  }

  const existing = normalizePhoneNumber(fields[PHONE_FIELD]);
  let candidate = null;
  let sourceUrl = opts.officialPhoneSourceUrl || null;
  let sourceType = opts.officialPhoneSourceType || null;

  if (opts.officialPhone) {
    candidate = normalizePhoneNumber(opts.officialPhone);
    sourceType = sourceType || PHONE_SOURCE_TYPES.OFFICIAL_BRAND_DIRECTORY;
  } else if (opts.pageHtml) {
    const extracted = extractOfficialPhoneFromHtml(
      opts.pageHtml,
      opts.pageUrl || fields["Official Property URL"] || fields["Source URL"] || ""
    );
    if (extracted.ok) {
      candidate = extracted.phone;
      sourceUrl = extracted.source_url;
      sourceType = extracted.source_type;
    }
  }

  if (!candidate) {
    const hasOfficialUrl = Boolean(
      String(fields["Official Property URL"] || fields["Source URL"] || "").trim()
    );
    return {
      action: hasOfficialUrl ? "source_available_needs_fetch" : "source_insufficient",
      reason: hasOfficialUrl ? "official_url_present_phone_not_fetched" : "no_official_phone_source",
      write_allowed: false,
      phone_source_available: hasOfficialUrl,
    };
  }

  if (isForbiddenPhoneSourceUrl(sourceUrl)) {
    return {
      action: "blocked_forbidden_source",
      reason: "third_party_phone_source",
      write_allowed: false,
    };
  }

  if (existing) {
    const same =
      existing.replace(/\D/g, "").slice(-8) === candidate.replace(/\D/g, "").slice(-8);
    if (same) {
      return {
        action: "skip_identical",
        reason: "phone_already_matches",
        write_allowed: false,
        phone_complete: true,
      };
    }
    return {
      action: "steward_conflict",
      reason: "phone_value_conflict",
      write_allowed: false,
      existing,
      candidate,
    };
  }

  return {
    action: "autofill",
    reason: "official_phone_high",
    write_allowed: true,
    confidence: "High",
    patch: { [PHONE_FIELD]: candidate },
    source_url: sourceUrl,
    source_type: sourceType,
    before: null,
    after: candidate,
  };
}

/**
 * Build High phone proposals (only when official phone already resolved).
 * Does not fetch pages — caller supplies officialPhoneByRecordId or pageHtmlByRecordId.
 *
 * @param {{
 *   censusRecords?: object[],
 *   phoneFieldExists?: boolean,
 *   officialPhoneByRecordId?: Record<string, { phone: string, source_url?: string, source_type?: string }>,
 *   pageHtmlByRecordId?: Record<string, { html: string, url?: string }>,
 * }} [opts]
 */
export function buildPhoneEnrichmentProposals(opts = {}) {
  const records = opts.censusRecords || [];
  const phoneFieldExists = opts.phoneFieldExists !== false;
  const proposals = [];
  const steward = [];
  const blocked = [];
  let sourceAvailable = 0;
  let complete = 0;

  for (const rec of records) {
    const fields = rec.fields || {};
    if (!isBlank(fields[PHONE_FIELD])) complete += 1;

    const official = opts.officialPhoneByRecordId?.[rec.id] || null;
    const page = opts.pageHtmlByRecordId?.[rec.id] || null;
    const classified = classifyPhoneEnrichment(rec, {
      phoneFieldExists,
      officialPhone: official?.phone,
      officialPhoneSourceUrl: official?.source_url,
      officialPhoneSourceType: official?.source_type,
      pageHtml: page?.html,
      pageUrl: page?.url,
    });

    if (classified.phone_source_available) sourceAvailable += 1;
    if (classified.action === "source_available_needs_fetch") sourceAvailable += 1;

    if (classified.action === "autofill" && classified.write_allowed) {
      proposals.push({
        record_id: rec.id,
        queue: PHONE_NUMBER_QUEUE_ID,
        action: "propose_high_write",
        confidence: "High",
        write_allowed_now: true,
        patch: classified.patch,
        current_fields: { [PHONE_FIELD]: fields[PHONE_FIELD] ?? null },
        method: "phone_number_enrichment_official_only",
        notes: "Official property/directory/JSON-LD phone only; no OTA/Google/Mapbox",
        source_url: classified.source_url,
        source_type: classified.source_type,
      });
    } else if (classified.action === "steward_conflict") {
      steward.push({
        record_id: rec.id,
        reason: classified.reason,
        existing: classified.existing,
        candidate: classified.candidate,
        queue: PHONE_NUMBER_QUEUE_ID,
      });
    } else if (
      classified.action === "blocked_clean_core" ||
      classified.action === "blocked_forbidden_source"
    ) {
      blocked.push({
        record_id: rec.id,
        reason: classified.reason,
        queue: PHONE_NUMBER_QUEUE_ID,
      });
    }
  }

  return {
    version: PHONE_ENRICHMENT_VERSION,
    queue_id: PHONE_NUMBER_QUEUE_ID,
    phone_field: PHONE_FIELD,
    phone_field_exists: phoneFieldExists,
    proposals,
    steward_review: steward,
    blocked,
    counters: {
      records_scanned: records.length,
      phone_complete: complete,
      phone_source_available: sourceAvailable,
      high_proposals: proposals.length,
      steward_conflicts: steward.length,
      blocked_clean_core_or_source: blocked.length,
    },
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Fetch official property pages for Clean Core records missing Phone, extract High phones.
 * Autopilot entry — bounded fetch; never OTAs/Google/Mapbox.
 * @param {{
 *   censusRecords?: object[],
 *   phoneFieldExists?: boolean,
 *   fetchLimit?: number,
 *   delayMs?: number,
 *   log?: Function,
 * }} [opts]
 */
export async function runPhoneEnrichmentQueueDryRun(opts = {}) {
  const records = opts.censusRecords || [];
  const fetchLimit = Math.max(
    0,
    Number(opts.fetchLimit ?? process.env.AUTOPILOT_PHONE_FETCH_LIMIT ?? 120) || 120
  );
  const delayMs = Math.max(50, Number(opts.delayMs) || 150);
  const log = opts.log || (() => {});
  const pageHtmlByRecordId = {};
  let fetchAttempted = 0;
  let fetchOk = 0;

  const needingFetch = [];
  for (const rec of records) {
    const fields = rec.fields || {};
    if (!isBlank(fields[PHONE_FIELD])) continue;
    if (fields["Human Review Required"] === true) continue;
    const url = String(
      fields["Official Property URL"] || fields["Source URL"] || ""
    ).trim();
    if (!url || !/^https?:\/\//i.test(url)) continue;
    if (isForbiddenPhoneSourceUrl(url)) continue;
    const clean = evaluateCleanCorePass(rec, opts);
    if (!clean.pass) continue;
    needingFetch.push({ rec, url });
  }

  const work = needingFetch.slice(0, fetchLimit);
  log(
    `[phone] fetching official pages for ${work.length}/${needingFetch.length} Clean Core missing Phone (cap=${fetchLimit})`
  );

  for (const row of work) {
    fetchAttempted += 1;
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), 20000);
    try {
      const res = await fetch(row.url, {
        headers: {
          "user-agent":
            "DealalityCensusBot/1.0 (+https://dealality.com; census-phone-enrichment)",
          accept: "text/html,application/xhtml+xml",
        },
        redirect: "follow",
        signal: controller.signal,
      });
      const html = await res.text();
      const blocked =
        res.status === 403 ||
        res.status === 429 ||
        /access denied|cf-challenge|attention required/i.test(html);
      if (res.ok && !blocked && html.length > 400) {
        pageHtmlByRecordId[row.rec.id] = { html, url: res.url || row.url };
        fetchOk += 1;
      }
    } catch {
      // skip — source remaining
    } finally {
      clearTimeout(t);
    }
    await sleep(delayMs);
  }

  const report = buildPhoneEnrichmentProposals({
    censusRecords: records,
    phoneFieldExists: opts.phoneFieldExists !== false,
    pageHtmlByRecordId,
  });
  report.counters = {
    ...report.counters,
    fetch_attempted: fetchAttempted,
    fetch_ok: fetchOk,
    needing_fetch: needingFetch.length,
    fetch_limit: fetchLimit,
  };
  return report;
}
