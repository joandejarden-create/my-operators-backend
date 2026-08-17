/**
 * Resolve stewarded Choice Radisson Individuals insert candidates.
 *
 * Official property-level Choice URLs encode city; property pages are often Akamai-blocked.
 * Clean names by stripping ", a member of Radisson Individuals" when property URL is verified.
 * Never writes Brand Setup / Brand Explorer / VIC / old Census.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  choiceCitySlugFromPropertyUrl,
  canonicalChoicePropertyUrl,
} from "../choice-regional-directory-extract.js";
import { isChoicePropertyLevelUrl } from "./census-autopilot-choice-address-resourcing.js";
import {
  indexHotelPropertyCensus,
  matchDiscoveredProperty,
  MATCH_CLASS,
  sanitizeInsertFields,
} from "./census-autopilot-source-discovery.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const CHOICE_RADISSON_STEWARD_RESOLUTION_VERSION =
  "census-autopilot-choice-radisson-steward-resolution-v1";

export const RESOLUTION_CLASS = Object.freeze({
  RESOLVED: "resolved_high_confidence_insert_candidate",
  STILL_STEWARD: "still_steward_review_required",
  DUPLICATE: "duplicate_risk",
  SOURCE_INSUFFICIENT: "source_insufficient",
  IDENTITY_CONFLICT: "blocked_identity_conflict",
});

export const FINAL_STATUS = Object.freeze({
  COMPLETE: "production_census_choice_radisson_steward_resolved_production_cycle_complete",
  PARTIAL: "production_census_choice_radisson_steward_partial_remaining",
  BLOCKED: "production_census_choice_radisson_steward_blocked",
});

const DEFAULT_STEWARD_QUEUE =
  "reports/research-engine-v2/autopilot/2026-08-06T09-57-03_CALA-production-cycle/steward-review-queue.json";

const MEMBER_OF_RE = /,?\s*a\s+member\s+of\s+radisson\s+individuals\.?/gi;

function writeJson(fp, data) {
  mkdirSync(dirname(fp), { recursive: true });
  writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}
function writeMd(fp, text) {
  mkdirSync(dirname(fp), { recursive: true });
  writeFileSync(fp, text, "utf8");
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Title-case an official Choice URL city slug (medellin → Medellin, cerro-punta → Cerro Punta).
 * This is URL-structure parsing, not fuzzy geography inference.
 */
export function titleCaseCitySlug(slug) {
  return String(slug || "")
    .split("-")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

/**
 * Strip Choice marketing member-of suffix from Radisson Individuals names.
 */
export function cleanRadissonIndividualsPropertyName(name) {
  const cleaned = String(name || "")
    .replace(MEMBER_OF_RE, "")
    .replace(/\s{2,}/g, " ")
    .trim()
    .replace(/,\s*$/, "")
    .trim();
  return cleaned;
}

export function isChoiceRadissonIndividualsStewardCase(row = {}) {
  const name = String(row.property_name || row.fields?.["Property Name"] || "");
  const brand = String(row.brand || row.fields?.["Current Brand"] || "");
  const family = String(row.source_family || row.fields?.["Family / Source Family"] || "");
  const city = String(row.fields?.City || row.discovery?.city || "").trim();
  const reason = String(row.steward_reason || "");
  if (family && !/choice/i.test(family)) return false;
  if (/choice_radisson_individuals/i.test(reason)) return true;
  if (/a\s+member\s+of\s+radisson\s+individuals/i.test(name)) return true;
  if (/radisson\s+individuals/i.test(brand) && /^unknown$/i.test(city)) return true;
  return false;
}

export function loadChoiceRadissonStewardCases(opts = {}) {
  const path = resolve(opts.stewardQueuePath || join(ROOT, DEFAULT_STEWARD_QUEUE));
  if (!existsSync(path)) {
    return { ok: false, error: `steward_queue_missing:${path}`, path, items: [] };
  }
  const doc = JSON.parse(readFileSync(path, "utf8"));
  const items = (doc.items || []).filter((it) => isChoiceRadissonIndividualsStewardCase(it));
  return {
    ok: true,
    path,
    count: items.length,
    items,
    raw_count: (doc.items || []).length,
  };
}

/**
 * Resolve one steward insert candidate from official Choice property URL + name cleanup.
 */
export function resolveChoiceRadissonStewardCase(row, opts = {}) {
  const identityKey = row.identity_key;
  const rawName = String(row.property_name || row.fields?.["Property Name"] || "");
  const brand = String(row.brand || row.fields?.["Current Brand"] || "Radisson Individuals by Choice");
  const country = String(row.fields?.Country || row.discovery?.country || "").trim();
  const officialId = String(row.official_property_id || "").toUpperCase();

  const rawUrl =
    row.fields?.["Official Property URL"] ||
    row.discovery?.official_property_url ||
    row.fields?.["Source URL"] ||
    null;
  const propertyUrl = rawUrl ? canonicalChoicePropertyUrl(rawUrl) : null;

  if (!propertyUrl || !isChoicePropertyLevelUrl(propertyUrl)) {
    return {
      identity_key: identityKey,
      classification: RESOLUTION_CLASS.SOURCE_INSUFFICIENT,
      reason: "missing_or_non_property_level_official_url",
      property_url: propertyUrl,
      original: row,
    };
  }

  const citySlug = choiceCitySlugFromPropertyUrl(propertyUrl);
  const city = titleCaseCitySlug(citySlug);
  if (!city || /^unknown$/i.test(city)) {
    return {
      identity_key: identityKey,
      classification: RESOLUTION_CLASS.SOURCE_INSUFFICIENT,
      reason: "city_not_resolvable_from_official_property_url",
      property_url: propertyUrl,
      original: row,
    };
  }

  const cleanName = cleanRadissonIndividualsPropertyName(rawName);
  if (!cleanName || cleanName.length < 3) {
    return {
      identity_key: identityKey,
      classification: RESOLUTION_CLASS.STILL_STEWARD,
      reason: "clean_name_empty_after_member_of_strip",
      property_url: propertyUrl,
      original: row,
    };
  }
  if (/a\s+member\s+of\s+radisson\s+individuals/i.test(cleanName)) {
    return {
      identity_key: identityKey,
      classification: RESOLUTION_CLASS.STILL_STEWARD,
      reason: "member_of_suffix_not_removed",
      property_url: propertyUrl,
      original: row,
    };
  }

  // Optional: first path segment as State/Region when it is not the country slug
  const pathParts = String(new URL(propertyUrl).pathname)
    .replace(/\/en-[a-z]{2}\//i, "/")
    .split("/")
    .filter(Boolean);
  const regionSlug = pathParts[0] || "";
  const countrySlug = String(country || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
  let stateRegion = null;
  if (regionSlug && countrySlug && regionSlug !== countrySlug && regionSlug !== citySlug) {
    stateRegion = titleCaseCitySlug(regionSlug);
  }

  const fields = {
    ...(row.fields || {}),
    "Property Name": cleanName,
    "Property Identity Key": identityKey || row.fields?.["Property Identity Key"],
    "Current Brand": brand,
    "Brand Family": row.fields?.["Brand Family"] || "Choice",
    "Affiliation Status": row.fields?.["Affiliation Status"] || "Branded",
    City: city,
    Country: country || row.fields?.Country,
    "Source URL": propertyUrl,
    "Official Property URL": propertyUrl,
    "Family / Source Family": "Choice",
    "Source Type": "official_property_page",
    "Source Confidence": "High",
    "Identity Confidence": "High",
    "Data Eligible": true,
    "Production Use Status":
      row.fields?.["Production Use Status"] || "Census Only / Not Owner-Facing",
    "Enrichment Status": row.fields?.["Enrichment Status"] || "Discovered — pending enrichment",
    "Enrichment Priority": row.fields?.["Enrichment Priority"] || "High",
    "Human Review Required": false,
    "Last Reviewed Date": todayIsoDate(),
    "Discovery Date": row.fields?.["Discovery Date"] || todayIsoDate(),
  };
  if (stateRegion) fields["State / Region"] = stateRegion;

  const sanitized = sanitizeInsertFields(fields);
  if (!sanitized.fields["Property Name"] || !sanitized.fields["Property Identity Key"]) {
    return {
      identity_key: identityKey,
      classification: RESOLUTION_CLASS.SOURCE_INSUFFICIENT,
      reason: "sanitize_dropped_core_fields",
      dropped: sanitized.dropped,
      original: row,
    };
  }

  // Rededupe against live/injected Census when provided
  if (opts.censusIndex) {
    const discovered = {
      identity_key: identityKey,
      official_property_id: officialId,
      property_name: cleanName,
      brand,
      city,
      country: fields.Country,
      official_property_url: propertyUrl,
      source_family: "Choice",
      identity_confidence: "High",
      source_confidence: "High",
    };
    const match = matchDiscoveredProperty(discovered, opts.censusIndex);
    if (
      match.classification === MATCH_CLASS.EXISTING_EXACT ||
      match.classification === MATCH_CLASS.DUPLICATE_RISK
    ) {
      return {
        identity_key: identityKey,
        classification: RESOLUTION_CLASS.DUPLICATE,
        reason: match.classification,
        match,
        property_url: propertyUrl,
        clean_name: cleanName,
        city,
        original: row,
      };
    }
    if (
      match.classification === MATCH_CLASS.EXISTING_PROBABLE ||
      match.classification === MATCH_CLASS.STEWARD
    ) {
      return {
        identity_key: identityKey,
        classification: RESOLUTION_CLASS.IDENTITY_CONFLICT,
        reason: match.classification,
        match,
        property_url: propertyUrl,
        clean_name: cleanName,
        city,
        original: row,
      };
    }
  }

  const resolvedInsert = {
    action: "insert",
    queue: "source_discovery",
    confidence: "High",
    identity_key: identityKey,
    property_name: cleanName,
    brand,
    source_family: "Choice",
    official_property_id: officialId,
    fields: sanitized.fields,
    field_keys: Object.keys(sanitized.fields),
    dropped: sanitized.dropped,
    discovery: {
      official_property_url: propertyUrl,
      official_directory_url: row.discovery?.official_directory_url || null,
      city,
      country: fields.Country,
      match_classification: "new_property_candidate",
      city_source: "choice_official_property_url_slug",
      name_source: "strip_member_of_radisson_individuals_suffix",
    },
    steward_resolution: {
      classification: RESOLUTION_CLASS.RESOLVED,
      version: CHOICE_RADISSON_STEWARD_RESOLUTION_VERSION,
      resolved_at: new Date().toISOString(),
      previous_name: rawName,
      previous_city: row.fields?.City || null,
      clean_name: cleanName,
      city,
      property_url: propertyUrl,
    },
  };

  return {
    identity_key: identityKey,
    classification: RESOLUTION_CLASS.RESOLVED,
    reason: "official_property_url_city_slug_and_clean_name",
    property_url: propertyUrl,
    clean_name: cleanName,
    city,
    state_region: stateRegion,
    country: fields.Country,
    resolved_insert: resolvedInsert,
    original: row,
  };
}

/**
 * Resolve a list of steward cases.
 */
export function resolveChoiceRadissonStewardBatch(items = [], opts = {}) {
  const censusIndex = opts.censusRecords
    ? indexHotelPropertyCensus(opts.censusRecords)
    : opts.censusIndex || null;

  const results = [];
  for (const row of items) {
    results.push(resolveChoiceRadissonStewardCase(row, { ...opts, censusIndex }));
  }

  const byClass = {};
  for (const c of Object.values(RESOLUTION_CLASS)) byClass[c] = [];
  for (const r of results) {
    byClass[r.classification] = byClass[r.classification] || [];
    byClass[r.classification].push(r);
  }

  const resolvedInserts = results
    .filter((r) => r.classification === RESOLUTION_CLASS.RESOLVED && r.resolved_insert)
    .map((r) => r.resolved_insert);

  return {
    version: CHOICE_RADISSON_STEWARD_RESOLUTION_VERSION,
    generated_at: new Date().toISOString(),
    input_count: items.length,
    counts: Object.fromEntries(
      Object.entries(byClass).map(([k, arr]) => [k, arr.length])
    ),
    resolved_inserts: resolvedInserts,
    still_steward: byClass[RESOLUTION_CLASS.STILL_STEWARD] || [],
    duplicate_risk: byClass[RESOLUTION_CLASS.DUPLICATE] || [],
    source_insufficient: byClass[RESOLUTION_CLASS.SOURCE_INSUFFICIENT] || [],
    identity_conflict: byClass[RESOLUTION_CLASS.IDENTITY_CONFLICT] || [],
    results,
  };
}

/**
 * Apply resolution to insert candidates: replace stewarded Radisson Individuals
 * rows with cleaned High inserts when resolution passes.
 */
export function applyChoiceRadissonStewardResolutionToInserts(inserts = [], opts = {}) {
  const out = [];
  const reportRows = [];
  for (const row of inserts) {
    if (!isChoiceRadissonIndividualsStewardCase(row)) {
      out.push(row);
      continue;
    }
    // Already resolved in a prior pass
    if (row.steward_resolution?.classification === RESOLUTION_CLASS.RESOLVED) {
      out.push(row);
      continue;
    }
    const resolved = resolveChoiceRadissonStewardCase(row, opts);
    reportRows.push(resolved);
    if (resolved.classification === RESOLUTION_CLASS.RESOLVED && resolved.resolved_insert) {
      out.push(resolved.resolved_insert);
    } else {
      out.push({
        ...row,
        steward_reason:
          resolved.reason || row.steward_reason || "choice_radisson_individuals_unresolved",
        steward_resolution: {
          classification: resolved.classification,
          reason: resolved.reason,
        },
      });
    }
  }
  return { inserts: out, resolution_rows: reportRows };
}

export function renderChoiceRadissonStewardResolutionMarkdown(report) {
  const lines = [
    `# Choice Radisson Individuals Steward Resolution`,
    ``,
    `- Generated: ${report.generated_at}`,
    `- Version: ${report.version}`,
    `- Input steward cases: ${report.input_count}`,
    `- Resolved High insert candidates: **${report.counts?.[RESOLUTION_CLASS.RESOLVED] ?? 0}**`,
    `- Still steward: ${report.counts?.[RESOLUTION_CLASS.STILL_STEWARD] ?? 0}`,
    `- Duplicate risk: ${report.counts?.[RESOLUTION_CLASS.DUPLICATE] ?? 0}`,
    `- Source insufficient: ${report.counts?.[RESOLUTION_CLASS.SOURCE_INSUFFICIENT] ?? 0}`,
    `- Identity conflict: ${report.counts?.[RESOLUTION_CLASS.IDENTITY_CONFLICT] ?? 0}`,
    ``,
    `## Resolved`,
    ``,
  ];
  for (const r of report.results || []) {
    if (r.classification !== RESOLUTION_CLASS.RESOLVED) continue;
    lines.push(
      `- \`${r.identity_key}\` → **${r.clean_name}** / ${r.city}, ${r.country} — ${r.property_url}`
    );
  }
  lines.push(``, `## Remaining steward / blocked`, ``);
  for (const r of report.results || []) {
    if (r.classification === RESOLUTION_CLASS.RESOLVED) continue;
    lines.push(`- \`${r.identity_key}\` — ${r.classification}: ${r.reason}`);
  }
  lines.push(``);
  return lines.join("\n");
}

export function writeChoiceRadissonStewardResolutionReports(report, opts = {}) {
  const reportsRoot = opts.reportsRoot || join(ROOT, "reports/research-engine-v2");
  const docsRoot = opts.docsRoot || join(ROOT, "docs/data-intelligence");
  const runDir = opts.runDir || null;

  writeJson(join(reportsRoot, "production-census-choice-radisson-steward-resolution.json"), report);
  writeMd(
    join(reportsRoot, "production-census-choice-radisson-steward-resolution.md"),
    renderChoiceRadissonStewardResolutionMarkdown(report)
  );
  writeMd(
    join(docsRoot, "production-census-choice-radisson-steward-resolution.md"),
    [
      `# Choice Radisson Individuals Steward Resolution`,
      ``,
      `Resolved **${report.counts?.[RESOLUTION_CLASS.RESOLVED] ?? 0}** / ${report.input_count} steward insert candidates using official Choice property-level URLs (city from URL slug; strip member-of marketing suffix).`,
      ``,
      `Property pages are often Akamai-blocked; regional JSON-LD + property URL structure is the official source basis.`,
      ``,
      `See \`reports/research-engine-v2/production-census-choice-radisson-steward-resolution.json\`.`,
      ``,
    ].join("\n")
  );

  if (runDir) {
    writeJson(join(runDir, "steward-resolution-report.json"), report);
    writeMd(
      join(runDir, "steward-resolution-report.md"),
      renderChoiceRadissonStewardResolutionMarkdown(report)
    );
    writeJson(join(runDir, "resolved-insert-bundle.json"), {
      version: CHOICE_RADISSON_STEWARD_RESOLUTION_VERSION,
      type: "hotel_property_census_insert_approval_bundle",
      queue: "source_discovery",
      mode: "production-cycle",
      status: "steward_resolved_high_confidence",
      proposed_inserts: report.resolved_inserts || [],
      records_proposed_for_insert: (report.resolved_inserts || []).length,
    });
  }

  return {
    reports: join(reportsRoot, "production-census-choice-radisson-steward-resolution.json"),
    docs: join(docsRoot, "production-census-choice-radisson-steward-resolution.md"),
  };
}
