/**
 * Phase 4T — Reconcile Choice property IDs from OSM candidate website URLs (report-only).
 */

import { readFileSync } from "fs";
import { CANDIDATE_FIELDS, CANDIDATES_TABLE, SOURCE_TYPES } from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";
import { parseChoicePropertyUrl } from "./brand-directory-property-url-extract.js";
import {
  loadCandidateRetentionReport,
  buildIncludedCandidateIds,
  parseRetentionIncludeList,
  DEFAULT_EXCLUDED_RETENTION,
  CALA_OSM_EXPANSION_BATCH_SUFFIX,
} from "./match-brand-directory-properties.js";
import { mapCandidateRecord } from "./promotion-review.js";
import {
  nameSimilarity,
  normalizeKey,
  normalizeText,
  normalizeCountry,
  countriesMatch,
  citiesMatch,
  websiteHost,
} from "./match-current-census.js";

export const MATCH_TYPE = {
  DIRECT_PROPERTY_ID: "direct_property_id_match",
  DIRECT_PROPERTY_URL: "direct_property_url_match",
  WEBSITE_HOST_ONLY: "website_host_only",
  NO_SITEMAP_MATCH: "no_sitemap_match",
  NON_CHOICE_URL: "non_choice_url",
};

export const RECONCILE_ACTION = {
  READY: "ready_for_choice_evidence",
  MANUAL: "needs_manual_review",
  HOLD_POLICY: "hold_for_source_policy",
  REJECT: "reject_no_direct_property_match",
};

const MATCH_TYPE_RANK = {
  [MATCH_TYPE.DIRECT_PROPERTY_URL]: 4,
  [MATCH_TYPE.DIRECT_PROPERTY_ID]: 3,
  [MATCH_TYPE.NO_SITEMAP_MATCH]: 2,
  [MATCH_TYPE.WEBSITE_HOST_ONLY]: 1,
  [MATCH_TYPE.NON_CHOICE_URL]: 0,
};

export const CHOICE_FAMILY_HOSTS = [
  "choicehotels.com",
  "radissonhotelsamericas.com",
  "woodspring.com",
];

const PROPERTY_ID_TAIL = /\/([a-z]{2}\d{2,8})\/?$/i;

const RECONCILE_FIELDS = [
  CANDIDATE_FIELDS.sourceRecordId,
  CANDIDATE_FIELDS.sourceName,
  CANDIDATE_FIELDS.sourceType,
  CANDIDATE_FIELDS.rawHotelName,
  CANDIDATE_FIELDS.rawCity,
  CANDIDATE_FIELDS.rawCountry,
  CANDIDATE_FIELDS.rawLatitude,
  CANDIDATE_FIELDS.rawLongitude,
  CANDIDATE_FIELDS.rawWebsite,
  CANDIDATE_FIELDS.rawBrand,
  CANDIDATE_FIELDS.rawPayloadJson,
  CANDIDATE_FIELDS.importBatchId,
];

function escapeFormulaString(s) {
  return String(s).replace(/'/g, "\\'");
}

export function normalizePropertyUrl(url) {
  try {
    const u = new URL(normalizeText(url));
    u.hostname = u.hostname.replace(/^www\./i, "").toLowerCase();
    u.hash = "";
    u.search = "";
    let path = u.pathname.replace(/\/+$/, "");
    return `${u.protocol}//${u.hostname}${path}`.toLowerCase();
  } catch {
    return normalizeKey(url);
  }
}

export function isChoiceFamilyHost(urlOrHost) {
  const host = urlOrHost.includes("://")
    ? websiteHost(urlOrHost)
    : normalizeKey(urlOrHost);
  if (!host) return false;
  return CHOICE_FAMILY_HOSTS.some(
    (h) => host === h || host.endsWith(`.${h}`)
  );
}

/**
 * @param {object} extractReport
 */
export function indexSitemapProperties(extractReport) {
  const byPropertyId = new Map();
  const byUrl = new Map();
  for (const r of extractReport.propertyRows || []) {
    const id = normalizeKey(r.propertyId);
    if (id) byPropertyId.set(id, r);
    const url = normalizePropertyUrl(r.propertyUrl);
    if (url) byUrl.set(url, r);
  }
  return { byPropertyId, byUrl };
}

function extractUrlsFromPayload(payload) {
  const urls = [];
  const visit = (val) => {
    if (typeof val === "string") {
      if (/https?:\/\//i.test(val) || val.includes("choicehotels")) {
        urls.push(val.trim());
      }
      return;
    }
    if (Array.isArray(val)) {
      for (const item of val) visit(item);
      return;
    }
    if (val && typeof val === "object") {
      for (const v of Object.values(val)) visit(v);
    }
  };
  visit(payload);
  return urls;
}

/**
 * @param {object} candidate — mapped candidate with raw fields
 */
export function collectChoiceFamilyUrls(candidate) {
  const seen = new Set();
  const urls = [];

  const add = (raw) => {
    const u = normalizeText(raw);
    if (!u || seen.has(u)) return;
    if (!isChoiceFamilyHost(u)) return;
    seen.add(u);
    urls.push(u);
  };

  add(candidate.rawWebsite);

  let payload = {};
  try {
    const raw = candidate.rawPayloadJson;
    payload = typeof raw === "string" ? JSON.parse(raw) : raw || {};
  } catch {
    payload = {};
  }

  for (const key of [
    "website",
    "url",
    "contact:website",
    "official_website",
    "brand:website",
  ]) {
    if (payload[key]) add(payload[key]);
  }
  for (const u of extractUrlsFromPayload(payload)) add(u);

  return urls;
}

/**
 * @param {string} url
 */
export function extractPropertyIdFromUrl(url) {
  const parsed = parseChoicePropertyUrl(url);
  if (parsed?.propertyId) {
    return {
      propertyId: normalizeKey(parsed.propertyId),
      parsed,
      extractMethod: "choice_path",
    };
  }

  try {
    const u = new URL(normalizeText(url));
    const m = u.pathname.match(PROPERTY_ID_TAIL);
    if (m) {
      return {
        propertyId: normalizeKey(m[1]),
        parsed: null,
        extractMethod: "tail_segment",
      };
    }
  } catch {
    /* ignore */
  }

  return { propertyId: "", parsed: null, extractMethod: "" };
}

/**
 * @param {string} url
 * @param {object} sitemapIndex
 */
export function classifyOsmWebsiteUrl(url, sitemapIndex) {
  if (!isChoiceFamilyHost(url)) {
    return { matchType: MATCH_TYPE.NON_CHOICE_URL, propertyId: "", sitemapRow: null };
  }

  const normUrl = normalizePropertyUrl(url);
  const urlHit = sitemapIndex.byUrl.get(normUrl);
  if (urlHit) {
    return {
      matchType: MATCH_TYPE.DIRECT_PROPERTY_URL,
      propertyId: normalizeKey(urlHit.propertyId),
      sitemapRow: urlHit,
      parsed: parseChoicePropertyUrl(url),
    };
  }

  const { propertyId, parsed, extractMethod } = extractPropertyIdFromUrl(url);
  if (propertyId) {
    const idHit = sitemapIndex.byPropertyId.get(propertyId);
    if (idHit) {
      return {
        matchType: MATCH_TYPE.DIRECT_PROPERTY_ID,
        propertyId,
        sitemapRow: idHit,
        parsed: parsed || parseChoicePropertyUrl(idHit.propertyUrl),
        extractMethod,
      };
    }
    return {
      matchType: MATCH_TYPE.NO_SITEMAP_MATCH,
      propertyId,
      sitemapRow: null,
      parsed,
      extractMethod,
    };
  }

  return {
    matchType: MATCH_TYPE.WEBSITE_HOST_ONLY,
    propertyId: "",
    sitemapRow: null,
    parsed: parseChoicePropertyUrl(url),
    extractMethod: "",
  };
}

/**
 * @param {object} osm
 * @param {object} sitemapRow
 * @param {string} matchType
 */
export function assessReconciliationConfidence(osm, sitemapRow, matchType) {
  if (
    matchType === MATCH_TYPE.WEBSITE_HOST_ONLY ||
    matchType === MATCH_TYPE.NO_SITEMAP_MATCH ||
    matchType === MATCH_TYPE.NON_CHOICE_URL
  ) {
    return "low";
  }

  const choiceCountry = sitemapRow?.inferredCountry || "";
  const choiceCity = sitemapRow?.citySlug
    ? sitemapRow.citySlug.replace(/-/g, " ")
    : "";
  const choiceBrand = sitemapRow?.matchedBrandSetupBrand || sitemapRow?.inferredBrandName || "";

  const countryOk = countriesMatch(osm.rawCountry, choiceCountry);
  const cityOk =
    citiesMatch(osm.rawCity, choiceCity) ||
    nameSimilarity(osm.rawCity, choiceCity) >= 0.55;
  const nameOk = nameSimilarity(osm.rawHotelName, choiceBrand) >= 0.45;
  const nameOsmChoice = nameSimilarity(
    osm.rawHotelName,
    sitemapRow?.propertyUrl || choiceBrand
  );

  if (matchType === MATCH_TYPE.DIRECT_PROPERTY_URL && countryOk && (cityOk || nameOk)) {
    return "high";
  }
  if (countryOk && cityOk && (nameOk || nameOsmChoice >= 0.4)) return "high";
  if (countryOk && (cityOk || nameOk)) return "medium";
  if (countryOk) return "medium";
  return "low";
}

export function recommendReconciliationAction(matchType, confidence, sitemapRow) {
  if (
    matchType === MATCH_TYPE.WEBSITE_HOST_ONLY ||
    matchType === MATCH_TYPE.NO_SITEMAP_MATCH ||
    matchType === MATCH_TYPE.NON_CHOICE_URL
  ) {
    return {
      recommendedAction: RECONCILE_ACTION.REJECT,
      notes: "No direct Choice property ID or canonical sitemap URL on OSM website.",
    };
  }

  const policy = sitemapRow?.sourcePolicy || "";
  if (policy === "review_required") {
    if (confidence === "high" && matchType !== MATCH_TYPE.WEBSITE_HOST_ONLY) {
      return {
        recommendedAction: RECONCILE_ACTION.HOLD_POLICY,
        notes:
          "Direct sitemap match; source policy review_required before evidence ingest.",
      };
    }
  }

  if (
    matchType === MATCH_TYPE.DIRECT_PROPERTY_URL ||
    matchType === MATCH_TYPE.DIRECT_PROPERTY_ID
  ) {
    if (confidence === "high") {
      return {
        recommendedAction: RECONCILE_ACTION.READY,
        notes: "OSM website property ID/URL aligns with Choice sitemap; strong geo/brand signals.",
      };
    }
    if (confidence === "medium") {
      return {
        recommendedAction: RECONCILE_ACTION.MANUAL,
        notes: "Direct sitemap match; verify city/brand alignment before Choice evidence.",
      };
    }
    return {
      recommendedAction: RECONCILE_ACTION.MANUAL,
      notes: "Direct sitemap match with weak city/country alignment.",
    };
  }

  return {
    recommendedAction: RECONCILE_ACTION.REJECT,
    notes: "Unable to classify as direct property match.",
  };
}

function pickBestAttempt(attempts) {
  if (!attempts.length) return null;
  return attempts.sort((a, b) => {
    const ra = MATCH_TYPE_RANK[a.matchType] || 0;
    const rb = MATCH_TYPE_RANK[b.matchType] || 0;
    if (rb !== ra) return rb - ra;
    const ca = { high: 3, medium: 2, low: 1 }[a.reconciliationConfidence] || 0;
    const cb = { high: 3, medium: 2, low: 1 }[b.reconciliationConfidence] || 0;
    return cb - ca;
  })[0];
}

/**
 * @param {Array<object>} candidates
 * @param {object} sitemapIndex
 */
export function reconcileCandidatesFromOsmWebsites(candidates, sitemapIndex) {
  const rows = [];
  let withChoiceUrls = 0;

  for (const c of candidates) {
    const urls = collectChoiceFamilyUrls(c);
    if (!urls.length) continue;
    withChoiceUrls++;

    const attempts = [];
    for (const url of urls) {
      const classified = classifyOsmWebsiteUrl(url, sitemapIndex);
      const sitemapRow = classified.sitemapRow;
      const confidence = assessReconciliationConfidence(
        c,
        sitemapRow,
        classified.matchType
      );
      const { recommendedAction, notes } = recommendReconciliationAction(
        classified.matchType,
        confidence,
        sitemapRow
      );

      attempts.push({
        osmWebsite: url,
        extractedChoicePropertyId: classified.propertyId || "",
        matchedChoicePropertyUrl: sitemapRow?.propertyUrl || "",
        matchedChoiceBrand:
          sitemapRow?.matchedBrandSetupBrand || sitemapRow?.inferredBrandName || "",
        matchedChoiceCountry: sitemapRow?.inferredCountry || "",
        matchedChoiceCitySlug: sitemapRow?.citySlug || "",
        matchType: classified.matchType,
        reconciliationConfidence: confidence,
        recommendedAction,
        notes,
        extractMethod: classified.extractMethod || "",
        sourcePolicy: sitemapRow?.sourcePolicy || "",
      });
    }

    const best = pickBestAttempt(attempts);
    if (!best) continue;

    rows.push({
      osmCandidateRecordId: c.airtableRecordId,
      osmCandidateName: c.rawHotelName,
      osmCountry: c.rawCountry,
      osmCity: c.rawCity,
      osmWebsite: c.rawWebsite,
      extractedChoicePropertyId: best.extractedChoicePropertyId,
      matchedChoicePropertyUrl: best.matchedChoicePropertyUrl,
      matchedChoiceBrand: best.matchedChoiceBrand,
      matchedChoiceCountry: best.matchedChoiceCountry,
      matchedChoiceCitySlug: best.matchedChoiceCitySlug,
      matchType: best.matchType,
      reconciliationConfidence: best.reconciliationConfidence,
      recommendedAction: best.recommendedAction,
      notes: best.notes,
      osmWebsiteUsed: best.osmWebsite,
      allChoiceUrls: urls,
      attemptCount: attempts.length,
      sourceRecordId: c.sourceRecordId,
      importBatchId: c.importBatchId,
    });
  }

  return { rows, withChoiceUrls };
}

export function summarizeReconciliation(rows) {
  const summary = {
    reconciliationRows: rows.length,
    direct_property_id_match: 0,
    direct_property_url_match: 0,
    website_host_only: 0,
    no_sitemap_match: 0,
    ready_for_choice_evidence: 0,
    needs_manual_review: 0,
    hold_for_source_policy: 0,
    reject_no_direct_property_match: 0,
    confidenceHigh: 0,
    confidenceMedium: 0,
    confidenceLow: 0,
  };

  for (const r of rows) {
    summary[r.matchType] = (summary[r.matchType] || 0) + 1;
    summary[r.recommendedAction] = (summary[r.recommendedAction] || 0) + 1;
    if (r.reconciliationConfidence === "high") summary.confidenceHigh++;
    else if (r.reconciliationConfidence === "medium") summary.confidenceMedium++;
    else summary.confidenceLow++;
  }

  return summary;
}

/**
 * Load OSM candidates (CALA expansion), optionally filtered by Phase 4O retention.
 */
export async function loadOsmCandidatesForReconciliation(opts) {
  const {
    retentionReportPath,
    includeRetention = "",
    useRetentionFilter = true,
  } = opts;

  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const st = escapeFormulaString(SOURCE_TYPES.OSM);
  const suffix = escapeFormulaString(CALA_OSM_EXPANSION_BATCH_SUFFIX);
  const filterByFormula = `AND({${CANDIDATE_FIELDS.sourceType}} = '${st}', FIND('${suffix}', {${CANDIDATE_FIELDS.importBatchId}}))`;

  let retentionIds = null;
  let retentionMeta = null;
  if (useRetentionFilter && retentionReportPath) {
    const retention = loadCandidateRetentionReport(retentionReportPath);
    const includeSet = parseRetentionIncludeList(includeRetention);
    const { ids, includedCounts } = buildIncludedCandidateIds(
      retention.rows,
      includeSet,
      DEFAULT_EXCLUDED_RETENTION
    );
    retentionIds = ids;
    retentionMeta = { includedCounts, includeRetention: [...includeSet] };
  }

  const rows = [];
  await new Promise((resolve, reject) => {
    base(CANDIDATES_TABLE)
      .select({ filterByFormula, fields: RECONCILE_FIELDS })
      .eachPage(
        (page, next) => {
          for (const rec of page) {
            if (retentionIds && !retentionIds.has(rec.id)) continue;
            rows.push(mapCandidateRecord(rec));
          }
          next();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  return {
    totalScanned: rows.length,
    filterByFormula,
    retentionMeta,
    rows,
  };
}

export function loadPropertyUrlExtractReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  return data;
}

export const RECONCILE_CSV_COLUMNS = [
  "OSM Candidate Record ID",
  "OSM Candidate Name",
  "OSM Country",
  "OSM City",
  "OSM Website",
  "Extracted Choice Property ID",
  "Matched Choice Property URL",
  "Matched Choice Brand",
  "Matched Choice Country",
  "Matched Choice City Slug",
  "Match Type",
  "Reconciliation Confidence",
  "Recommended Action",
  "Notes",
];

export function reconciliationRowToCsv(r) {
  return {
    "OSM Candidate Record ID": r.osmCandidateRecordId,
    "OSM Candidate Name": r.osmCandidateName,
    "OSM Country": r.osmCountry,
    "OSM City": r.osmCity,
    "OSM Website": r.osmWebsite,
    "Extracted Choice Property ID": r.extractedChoicePropertyId,
    "Matched Choice Property URL": r.matchedChoicePropertyUrl,
    "Matched Choice Brand": r.matchedChoiceBrand,
    "Matched Choice Country": r.matchedChoiceCountry,
    "Matched Choice City Slug": r.matchedChoiceCitySlug,
    "Match Type": r.matchType,
    "Reconciliation Confidence": r.reconciliationConfidence,
    "Recommended Action": r.recommendedAction,
    Notes: r.notes,
  };
}
