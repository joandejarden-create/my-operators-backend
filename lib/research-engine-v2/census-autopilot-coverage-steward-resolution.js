/**
 * Coverage Steward Resolution v1 — resolve remaining coverage steward cases.
 *
 * Scope: Marriott / IHG / Accor missing_needs_steward from coverage reconciliation.
 * Official sources only. Insert High-confidence only. No enrichment writes.
 * Write target: Hotel Property Census (tbl9aY5ijiuIzzWam) only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  COVERAGE_CLASS,
  buildCoverageInsertApprovalBundle,
  brandsEqualExact,
  runCoverageReconciliation,
} from "./census-autopilot-coverage-reconciliation.js";
import {
  MATCH_CLASS,
  classifyDiscoveredAgainstCensus,
} from "./census-autopilot-source-discovery.js";
import { tryCityFromMarriottPropertyUrl } from "./census-marriott-property-url-city-backfill.js";
import { mapMarriottMexicoBrand } from "./clean-census/marriott-mexico-discovery.js";
import {
  canonicalCalaCity,
  isDescriptorCity,
  normalizePlaceKey,
} from "./census-city-state-normalizer.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  runDiscoveryInsertApply,
} from "./census-autopilot-discovery-insert-apply.js";
import {
  checkAutopilotApplyEnv,
  isProductionWriteMode,
} from "./census-autopilot-apply-guard.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const COVERAGE_STEWARD_RESOLUTION_VERSION =
  "census-autopilot-coverage-steward-resolution-v1";
export const COVERAGE_STEWARD_RESOLUTION_OBJECTIVE = "coverage-steward-resolution-v1";

export const COVERAGE_STEWARD_STATUS = Object.freeze({
  COMPLETE: "production_census_coverage_steward_resolution_v1_complete",
  PARTIAL: "production_census_coverage_steward_resolution_v1_partial_remaining",
  NO_SAFE_WRITES:
    "production_census_coverage_steward_resolution_v1_no_safe_writes_remaining",
  BLOCKED: "production_census_coverage_steward_resolution_v1_blocked",
});

export const STEWARD_PARENTS = Object.freeze(["Marriott", "IHG", "Accor"]);

/** High MARSHA IATA → city (official code + country scoped). */
export const STEWARD_IATA_CITY_HIGH = Object.freeze({
  BOG: { city: "Bogotá", countries: ["colombia"] },
  MDE: { city: "Medellín", countries: ["colombia"] },
  CLO: { city: "Cali", countries: ["colombia"] },
  BAQ: { city: "Barranquilla", countries: ["colombia"] },
  SDQ: { city: "Santo Domingo", countries: ["dominican republic"] },
  PUJ: { city: "Punta Cana", countries: ["dominican republic"] },
  PTY: { city: "Panama City", countries: ["panama"] },
  SJO: { city: "San José", countries: ["costa rica"] },
  MEX: { city: "Mexico City", countries: ["mexico"] },
  CUN: { city: "Cancún", countries: ["mexico"] },
  GDL: { city: "Guadalajara", countries: ["mexico"] },
  MTY: { city: "Monterrey", countries: ["mexico"] },
});

/** IHG directory brand slug → display brand. */
export const IHG_BRAND_SLUG_TO_NAME = Object.freeze({
  holidayinn: "Holiday Inn",
  holidayinnexpress: "Holiday Inn Express",
  crowneplaza: "Crowne Plaza",
  intercontinental: "InterContinental",
  staybridge: "Staybridge Suites",
  candlewood: "Candlewood Suites",
  hotelindigo: "Hotel Indigo",
  kimpton: "Kimpton",
  avid: "avid hotels",
  voco: "voco",
  evenhotels: "EVEN Hotels",
  regent: "Regent",
  sixsenses: "Six Senses",
});

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

function countryNorm(c) {
  return normalizePlaceKey(c);
}

/**
 * Classify steward blocker type.
 */
export function classifyStewardBlocker(row = {}) {
  const brand = String(row.brand || "");
  const city = String(row.city || "").trim();
  const country = String(row.country || "").trim();
  const reasons = [];
  if (/brand unconfirmed/i.test(brand)) reasons.push("ambiguous_brand");
  if (/^(holidayinn|crowneplaza|hotelindigo|intercontinental)$/i.test(brand)) {
    reasons.push("collection_soft_brand_naming_ambiguity");
  }
  if (!city || /^unknown$/i.test(city)) reasons.push("ambiguous_city");
  if (city && country && brandsEqualExact(city, country)) reasons.push("ambiguous_city");
  if (city && isDescriptorCity(city)) reasons.push("ambiguous_city");
  if (row.coverage_class === COVERAGE_CLASS.DUPLICATE_RISK || row.classification === MATCH_CLASS.DUPLICATE_RISK) {
    reasons.push("possible_duplicate");
  }
  if (row.match_reason === "missing_city_for_coverage_insert") reasons.push("ambiguous_city");
  if (!row.official_property_url && !row.official_property_id) reasons.push("source_insufficient");
  if (!reasons.length) reasons.push("source_insufficient");
  return reasons[0];
}

/**
 * Normalize IHG brand slug → proper brand name.
 */
export function resolveIhgBrandName(brand) {
  const key = String(brand || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
  if (IHG_BRAND_SLUG_TO_NAME[key]) {
    return { ok: true, brand: IHG_BRAND_SLUG_TO_NAME[key], method: "ihg_brand_slug_map" };
  }
  // already spaced proper name
  if (/holiday inn|crowne plaza|intercontinental|hotel indigo/i.test(String(brand || ""))) {
    return { ok: true, brand: String(brand).trim(), method: "already_display_brand" };
  }
  return { ok: false, brand: brand || null, method: null };
}

/**
 * Resolve Brand Unconfirmed / weak Marriott brand from official name+URL.
 */
export function resolveMarriottBrandHigh(row = {}) {
  const name = String(row.property_name || "");
  const url = String(row.official_property_url || "");
  const current = String(row.brand || "");
  if (current && !/brand unconfirmed/i.test(current) && current !== "Marriott") {
    return { ok: true, brand: current, method: "already_mapped" };
  }
  const mapped = mapMarriottMexicoBrand(name, url);
  if (mapped && !/brand unconfirmed/i.test(mapped)) {
    return { ok: true, brand: mapped, method: "map_marriott_mexico_brand" };
  }
  const blob = `${name} ${url}`.toLowerCase();
  // High patterns not fully covered by mapper adjacency
  if (/marriott executive apartments|executive-apartments/i.test(blob)) {
    return {
      ok: true,
      brand: "Apartments by Marriott Bonvoy",
      method: "executive_apartments_pattern",
    };
  }
  if (/marriott vacation club|vacation-club/i.test(blob)) {
    return { ok: true, brand: "Marriott Vacation Club", method: "vacation_club_pattern" };
  }
  if (/\bmarriott\b/.test(blob) && /\b(hotel|resort|inn)\b/.test(blob)) {
    return { ok: true, brand: "Marriott Hotels", method: "marriott_hotels_name_url_pattern" };
  }
  return { ok: false, brand: current || mapped, method: null };
}

/**
 * Resolve city for steward case from official signals only.
 */
export function resolveStewardCityHigh(row = {}) {
  const country = String(row.country || "").trim();
  const name = String(row.property_name || "");
  const url = String(row.official_property_url || "");
  const code = String(row.official_property_id || "").trim().toUpperCase();
  let city = String(row.city || "").trim();

  // Country-as-city → clear for re-resolve
  if (city && brandsEqualExact(city, country)) city = "";
  if (city && isDescriptorCity(city)) city = "";

  // Explicit known city phrase in official property name (Mexico City / Panama City / Santo Domingo)
  const nameCityPatterns = [
    [/mexico\s*city/i, "Mexico City", ["mexico"]],
    [/panama\s*city/i, "Panama City", ["panama"]],
    [/santo\s*domingo/i, "Santo Domingo", ["dominican republic"]],
    [/bogot[aá]/i, "Bogotá", ["colombia"]],
    [/medell[ií]n/i, "Medellín", ["colombia"]],
    [/canc[uú]n/i, "Cancún", ["mexico"]],
  ];
  for (const [re, cityName, countries] of nameCityPatterns) {
    if (re.test(name) && countries.includes(countryNorm(country))) {
      return {
        ok: true,
        city: cityName,
        method: "official_property_name_known_city_phrase",
        confidence: "High",
      };
    }
  }

  // Marriott property URL slug / IATA
  if (/marriott\.com/i.test(url)) {
    const urlTry = tryCityFromMarriottPropertyUrl(url, country);
    if (urlTry.ok && urlTry.city) {
      return {
        ok: true,
        city: canonicalCalaCity(urlTry.city) || urlTry.city,
        method: urlTry.reason || "marriott_property_url_city",
        confidence: "High",
      };
    }
    const iata = code.slice(0, 3);
    const iataHit = STEWARD_IATA_CITY_HIGH[iata];
    if (iataHit && iataHit.countries.includes(countryNorm(country))) {
      // Avoid SJO→San José when slug clearly names another place (los-suenos, etc.)
      const slug = (url.match(/\/hotels\/[a-z0-9]+-([a-z0-9-]+)/i) || [])[1] || "";
      if (iata === "SJO" && /los-suenos|los-sueños|conchal|guanacaste|manuel|belmar/i.test(slug)) {
        return { ok: false, city: null, method: null, reason: "sjo_iata_ambiguous_coastal_property" };
      }
      return {
        ok: true,
        city: iataHit.city,
        method: "marsha_iata_country_scoped",
        confidence: "High",
      };
    }
  }

  // IHG URL: /panama/{code}/ → Panama City
  if (/ihg\.com/i.test(url)) {
    if (/\/panama\//i.test(url) && /panama/i.test(country)) {
      return {
        ok: true,
        city: "Panama City",
        method: "ihg_official_url_panama_path",
        confidence: "High",
      };
    }
    if (/panama/i.test(country) && (!city || brandsEqualExact(city, "Panama"))) {
      return {
        ok: true,
        city: "Panama City",
        method: "ihg_panama_country_city_normalize",
        confidence: "High",
      };
    }
  }

  // Accor: Sofitel Mexico City… already handled by name phrase
  if (/accor\.com/i.test(url) && /mexico/i.test(country)) {
    if (/mexico\s*city/i.test(name)) {
      return {
        ok: true,
        city: "Mexico City",
        method: "accor_official_name_mexico_city",
        confidence: "High",
      };
    }
  }

  if (city && !brandsEqualExact(city, country) && !isDescriptorCity(city)) {
    return {
      ok: true,
      city: canonicalCalaCity(city) || city,
      method: "existing_city",
      confidence: "High",
    };
  }

  return { ok: false, city: null, method: null, reason: "city_unresolved" };
}

/**
 * Attempt High resolution of one steward case.
 */
export function resolveCoverageStewardCase(row = {}) {
  const before = {
    property_name: row.property_name,
    brand: row.brand,
    city: row.city,
    country: row.country,
    identity_confidence: row.identity_confidence,
    coverage_class: row.coverage_class,
    blocker: classifyStewardBlocker(row),
  };

  const next = { ...row };
  const methods = [];
  const parent = String(row.parent_company || row.source_family || "").trim();

  // Brand
  if (/ihg/i.test(parent) || /ihg\.com/i.test(String(row.official_property_url || ""))) {
    const b = resolveIhgBrandName(next.brand);
    if (b.ok && b.brand) {
      next.brand = b.brand;
      methods.push(b.method);
    }
  } else if (/marriott/i.test(parent) || /marriott\.com/i.test(String(row.official_property_url || ""))) {
    const b = resolveMarriottBrandHigh(next);
    if (b.ok && b.brand) {
      next.brand = b.brand;
      methods.push(b.method);
    }
  }

  // City
  const cityTry = resolveStewardCityHigh(next);
  if (cityTry.ok && cityTry.city) {
    next.city = cityTry.city;
    methods.push(cityTry.method);
  }

  // Identity confidence
  const brandOk =
    Boolean(next.brand) && !/brand unconfirmed/i.test(String(next.brand));
  const cityOk =
    Boolean(next.city) &&
    !/^unknown$/i.test(next.city) &&
    !brandsEqualExact(next.city, next.country) &&
    !isDescriptorCity(next.city);
  const identityHigh =
    Boolean(next.official_property_id) &&
    Boolean(next.property_name) &&
    brandOk &&
    cityOk &&
    Boolean(next.country) &&
    Boolean(next.official_property_url);

  next.identity_confidence = identityHigh ? "High" : "Medium";
  next.source_confidence = next.source_confidence || (identityHigh ? "High" : "Medium");

  if (identityHigh) {
    next.coverage_class = COVERAGE_CLASS.MISSING_HIGH;
    next.classification = MATCH_CLASS.NEW_CANDIDATE;
    next.match_reason = `steward_resolved:${methods.join("+") || "identity_complete"}`;
    next.resolution_methods = methods;
    next.blocker_before = before.blocker;
    next.resolved = true;
  } else {
    next.coverage_class = COVERAGE_CLASS.MISSING_STEWARD;
    next.match_reason = next.match_reason || before.blocker;
    next.resolution_methods = methods;
    next.blocker_before = before.blocker;
    next.blocker_after = classifyStewardBlocker(next);
    next.resolved = false;
  }

  return {
    before,
    after: next,
    resolved: next.resolved,
    methods,
  };
}

/**
 * Load steward cases by re-running coverage classify for steward parents.
 */
export async function loadCoverageStewardCases(opts = {}) {
  const parents = opts.parents || STEWARD_PARENTS;
  const cases = [];
  const parentReports = [];

  for (const parent of parents) {
    const report =
      opts.parentReports?.[parent] ||
      (await runCoverageReconciliation({
        mode: "controlled",
        region: opts.region || "CALA",
        parentCompany: parent,
        censusRecords: opts.censusRecords,
        enableProductionWrites: false,
        allApplyConfirms: false,
        marriottCache: opts.marriottCache,
        delayMs: opts.delayMs ?? 120,
        log: opts.log || (() => {}),
      }));
    parentReports.push({ parent, report });

    const stewardRows = (report.missing_hotels || []).filter(
      (r) => r.coverage_class === COVERAGE_CLASS.MISSING_STEWARD
    );
    // Also include stewarded_hotels that are missing_needs_steward with fuller fields from classified
    for (const r of stewardRows) {
      cases.push({
        ...r,
        parent_company: parent,
        source_family: r.source_family || parent,
        steward_source: "coverage_reconciliation_missing_needs_steward",
      });
    }
  }

  return { cases, parentReports, count: cases.length };
}

/**
 * Resolve + re-dedupe against census; build insertable High set.
 */
export function processStewardResolutions(cases = [], censusRecords = []) {
  const resolved = [];
  const unresolved = [];
  const duplicateRisks = [];
  const sourceInsufficient = [];

  for (const c of cases) {
    const result = resolveCoverageStewardCase(c);
    if (!result.resolved) {
      const blocker = result.after.blocker_after || result.before.blocker;
      if (blocker === "source_insufficient") sourceInsufficient.push(result);
      else unresolved.push(result);
      continue;
    }
    resolved.push(result);
  }

  // Re-match resolved against census
  const discovered = resolved.map((r) => r.after);
  const match = classifyDiscoveredAgainstCensus(discovered, censusRecords);
  const insertable = [];
  for (let i = 0; i < match.classified.length; i++) {
    const row = match.classified[i];
    const prior = resolved[i];
    if (
      row.classification === MATCH_CLASS.EXISTING_EXACT ||
      row.classification === MATCH_CLASS.DUPLICATE_RISK
    ) {
      duplicateRisks.push({
        ...prior,
        after: { ...prior.after, ...row, coverage_class: COVERAGE_CLASS.DUPLICATE_RISK },
        resolved: false,
      });
      continue;
    }
    if (row.classification === MATCH_CLASS.EXISTING_PROBABLE) {
      unresolved.push({
        ...prior,
        after: { ...prior.after, ...row, coverage_class: COVERAGE_CLASS.MISSING_STEWARD },
        resolved: false,
      });
      continue;
    }
    // Promote to High missing for insert bundle
    insertable.push({
      ...prior.after,
      ...row,
      coverage_class: COVERAGE_CLASS.MISSING_HIGH,
      classification: MATCH_CLASS.NEW_CANDIDATE,
      identity_confidence: "High",
    });
  }

  return {
    resolved_candidates: resolved.length,
    insertable,
    unresolved,
    duplicate_risks: duplicateRisks,
    source_insufficient: sourceInsufficient,
  };
}

function resolveFinalStatus(summary = {}) {
  if (summary.blocked_hard) return COVERAGE_STEWARD_STATUS.BLOCKED;
  const remaining =
    (summary.unresolved_count || 0) +
    (summary.duplicate_risk_count || 0) +
    (summary.source_insufficient_count || 0);
  if ((summary.inserted_count || 0) > 0 && remaining === 0) {
    return COVERAGE_STEWARD_STATUS.COMPLETE;
  }
  if ((summary.inserted_count || 0) > 0 && remaining > 0) {
    return COVERAGE_STEWARD_STATUS.PARTIAL;
  }
  if ((summary.inserted_count || 0) === 0 && remaining > 0) {
    return COVERAGE_STEWARD_STATUS.NO_SAFE_WRITES;
  }
  if ((summary.inserted_count || 0) === 0 && remaining === 0) {
    return COVERAGE_STEWARD_STATUS.COMPLETE;
  }
  return COVERAGE_STEWARD_STATUS.PARTIAL;
}

function renderStewardMd(report) {
  const lines = [
    `# Production Census Coverage Steward Resolution v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Region:** ${report.region}`,
    `**Write target:** ${report.write_target?.table} (\`${report.write_target?.table_id}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    ``,
    `## Summary`,
    ``,
    `- Steward cases before: **${report.steward_cases_before}**`,
    `- Resolved High inserts: **${report.inserted_count}**`,
    `- Unresolved steward: **${report.unresolved_count}**`,
    `- Duplicate risks: **${report.duplicate_risk_count}**`,
    `- Source insufficient: **${report.source_insufficient_count}**`,
    `- Hotel Property Census before: **${report.census_before}**`,
    `- Hotel Property Census after: **${report.census_after}**`,
    ``,
    `## Parent-by-parent`,
    ``,
    `| Parent | Before | Inserted | Remaining |`,
    `| --- | ---: | ---: | ---: |`,
  ];
  for (const p of report.by_parent || []) {
    lines.push(
      `| ${p.parent} | ${p.before} | ${p.inserted} | ${p.remaining} |`
    );
  }
  lines.push(``, `## Inserted records`, ``);
  for (const r of report.inserted_hotels || []) {
    lines.push(
      `- **${r.property_name}** (${r.brand}, ${r.city}, ${r.country}) — ${r.official_property_id || "—"} — ${r.official_property_url || ""}`
    );
  }
  if (!(report.inserted_hotels || []).length) lines.push(`_None_`);
  lines.push(``, `## Remaining manual review`, ``);
  for (const r of report.remaining_manual_review || []) {
    lines.push(
      `- **${r.property_name}** | ${r.brand || "?"} | ${r.city || "—"}, ${r.country || "?"} | blocker=\`${r.blocker}\` | ${r.official_property_url || ""}`
    );
  }
  if (!(report.remaining_manual_review || []).length) lines.push(`_None_`);
  lines.push(
    ``,
    `## Safety`,
    ``,
    `- Hotel Property Census only`,
    `- No Brand Setup / Brand Explorer writes`,
    `- No address / lat-long / phone / rooms`,
    `- No owner/operator/date fields`,
    `- No fuzzy / name-only inserts`,
    ``
  );
  return lines.join("\n");
}

/**
 * Main runner.
 */
export async function runCoverageStewardResolution(opts = {}) {
  const log = opts.log || (() => {});
  const region = opts.region || "CALA";
  const mode = opts.mode || "controlled";
  const doWrite =
    Boolean(opts.enableProductionWrites) &&
    isProductionWriteMode(mode) &&
    Boolean(opts.allApplyConfirms);

  const writeTargetCheck = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: productionHotelPropertyCensus.tableId || TABLE_IDS["Hotel Property Census"],
  });
  if (!writeTargetCheck.ok) {
    return {
      ok: false,
      status: COVERAGE_STEWARD_STATUS.BLOCKED,
      objective: COVERAGE_STEWARD_RESOLUTION_OBJECTIVE,
      blocked_hard: true,
      blocked_reason: writeTargetCheck.reason || writeTargetCheck.code,
      airtable_writes: false,
    };
  }

  log(`[coverage-steward] loading steward cases for ${STEWARD_PARENTS.join(", ")}…`);

  // List census once for rededupe + before count
  let census = opts.censusRecords || null;
  if (!census) {
    const token = resolvePat();
    const bases = resolveTargetBase();
    const fields = [
      "Property Identity Key",
      "Property Name",
      "Canonical Property Name",
      "Current Brand",
      "Brand Family",
      "Country",
      "City",
      "State / Region",
      "Address",
      "Source URL",
      "Official Property URL",
      "Family / Source Family",
    ];
    census = [];
    let offset;
    do {
      const params = new URLSearchParams({ pageSize: "100" });
      if (offset) params.set("offset", offset);
      for (const f of fields) params.append("fields[]", f);
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(bases.target_base_id)}/${encodeURIComponent(TABLE_IDS["Hotel Property Census"])}?${params}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const json = await res.json();
      if (!res.ok) throw new Error(`census list ${res.status}`);
      census.push(...(json.records || []));
      offset = json.offset;
    } while (offset);
  }

  const loaded = await loadCoverageStewardCases({
    region,
    censusRecords: census,
    parents: STEWARD_PARENTS,
    delayMs: opts.delayMs,
    log,
  });

  const censusBefore = census.length;
  log(
    `[coverage-steward] steward_cases=${loaded.count} census=${censusBefore}`
  );

  const processed = processStewardResolutions(loaded.cases, census);
  log(
    `[coverage-steward] insertable=${processed.insertable.length} unresolved=${processed.unresolved.length} dups=${processed.duplicate_risks.length}`
  );

  const insertBundle = buildCoverageInsertApprovalBundle({
    classified: processed.insertable,
  });

  const runDir =
    opts.runDir ||
    path.join(
      ROOT,
      "reports/research-engine-v2/autopilot",
      `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-coverage-steward-resolution-v1`
    );
  fs.mkdirSync(runDir, { recursive: true });
  writeJson(path.join(runDir, "steward-cases-before.json"), loaded.cases);
  writeJson(path.join(runDir, "steward-resolutions.json"), {
    insertable: processed.insertable,
    unresolved: processed.unresolved.map((u) => u.after),
    duplicate_risks: processed.duplicate_risks.map((d) => d.after),
  });
  writeJson(path.join(runDir, "coverage-steward-insert-approval-bundle.json"), insertBundle);

  let insertedCount = 0;
  let insertApplyResult = null;
  const envCheck = checkAutopilotApplyEnv(opts.env || process.env);
  const canApply =
    doWrite &&
    Boolean(opts.allApplyConfirms) &&
    envCheck.allOk &&
    insertBundle.proposed_insert_count > 0;

  if (canApply) {
    log(
      `[coverage-steward] applying ${insertBundle.proposed_insert_count} High inserts…`
    );
    const bundlePath = path.join(runDir, "coverage-steward-insert-approval-bundle.json");
    insertApplyResult = await runDiscoveryInsertApply({
      args: {
        apply: true,
        allConfirmsOk: true,
        approvalBundlePath: bundlePath,
        batchSize: opts.batchSize || 100,
        confirms: opts.confirms || {},
      },
      bundlePath,
      censusRecords: census,
      doWrite: true,
      useLiveAirtable: opts.useLiveAirtable !== false,
      createRecords: opts.createRecords,
      env: opts.env || process.env,
      checkpointDir: runDir,
    });
    insertedCount = insertApplyResult?.created_count || 0;
  }

  const censusAfter = censusBefore + insertedCount;

  // Parent breakdown
  const byParent = STEWARD_PARENTS.map((parent) => {
    const before = loaded.cases.filter((c) => c.parent_company === parent).length;
    const inserted = processed.insertable.filter(
      (c) =>
        String(c.parent_company || c.source_family || "").toLowerCase() ===
          parent.toLowerCase() ||
        String(c.source_family || "").toLowerCase() === parent.toLowerCase()
    ).length;
    // Only count inserted that were actually written — approximate by parent of insertable when apply succeeded
    const insertedActual = canApply
      ? Math.min(inserted, insertedCount) &&
        processed.insertable.filter((c) =>
          new RegExp(parent, "i").test(String(c.parent_company || c.source_family || ""))
        ).length
      : 0;
    const remaining =
      processed.unresolved.filter((u) =>
        new RegExp(parent, "i").test(String(u.after?.parent_company || u.after?.source_family || ""))
      ).length +
      processed.duplicate_risks.filter((u) =>
        new RegExp(parent, "i").test(String(u.after?.parent_company || u.after?.source_family || ""))
      ).length;
    return {
      parent,
      before,
      inserted: canApply
        ? processed.insertable.filter((c) =>
            new RegExp(parent, "i").test(String(c.parent_company || c.source_family || ""))
          ).length
        : 0,
      remaining: before - (canApply
        ? processed.insertable.filter((c) =>
            new RegExp(parent, "i").test(String(c.parent_company || c.source_family || ""))
          ).length
        : 0),
    };
  });

  // Fix remaining counts after insert: remaining = before - inserted for that parent among insertable that applied
  for (const p of byParent) {
    if (!canApply) {
      p.inserted = 0;
      p.remaining = p.before;
    }
  }

  const remainingManual = [
    ...processed.unresolved.map((u) => ({
      property_name: u.after.property_name,
      brand: u.after.brand,
      city: u.after.city,
      country: u.after.country,
      parent_company: u.after.parent_company || u.after.source_family,
      blocker: u.after.blocker_after || u.before.blocker,
      official_property_url: u.after.official_property_url,
      official_property_id: u.after.official_property_id,
    })),
    ...processed.duplicate_risks.map((u) => ({
      property_name: u.after.property_name,
      brand: u.after.brand,
      city: u.after.city,
      country: u.after.country,
      parent_company: u.after.parent_company || u.after.source_family,
      blocker: "possible_duplicate",
      official_property_url: u.after.official_property_url,
      official_property_id: u.after.official_property_id,
    })),
  ];

  const summary = {
    ok: true,
    version: COVERAGE_STEWARD_RESOLUTION_VERSION,
    objective: COVERAGE_STEWARD_RESOLUTION_OBJECTIVE,
    region,
    mode,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    brand_setup_writes: false,
    brand_explorer_writes: false,
    steward_cases_before: loaded.count,
    resolved_high_candidates: processed.insertable.length,
    inserted_count: insertedCount,
    unresolved_count: processed.unresolved.length,
    duplicate_risk_count: processed.duplicate_risks.length,
    source_insufficient_count: processed.source_insufficient.length,
    census_before: censusBefore,
    census_after: censusAfter,
    by_parent: byParent,
    inserted_hotels: canApply
      ? processed.insertable.map((r) => ({
          property_name: r.property_name,
          brand: r.brand,
          city: r.city,
          country: r.country,
          official_property_id: r.official_property_id,
          official_property_url: r.official_property_url,
          resolution_methods: r.resolution_methods,
        }))
      : [],
    remaining_manual_review: remainingManual,
    insert_apply: insertApplyResult
      ? { status: insertApplyResult.status, created_count: insertedCount }
      : null,
    airtable_writes: Boolean(canApply),
    run_dir: runDir,
    blocked_hard: false,
  };

  // Adjust by_parent remaining after successful inserts
  if (canApply && insertedCount > 0) {
    for (const p of summary.by_parent) {
      const ins = processed.insertable.filter((c) =>
        new RegExp(p.parent, "i").test(String(c.parent_company || c.source_family || ""))
      ).length;
      p.inserted = ins;
      p.remaining = Math.max(0, p.before - ins);
    }
    // If apply wrote all insertable, remainingManual is the unresolved set only
  } else {
    summary.inserted_hotels = [];
    for (const p of summary.by_parent) {
      p.inserted = 0;
      p.remaining = p.before;
    }
  }

  summary.status = resolveFinalStatus(summary);
  summary.next_recommended_action =
    summary.status === COVERAGE_STEWARD_STATUS.COMPLETE
      ? "coverage_steward_resolution_complete"
      : summary.status === COVERAGE_STEWARD_STATUS.NO_SAFE_WRITES
        ? "manual_review_remaining_steward_cases"
        : "review_remaining_then_optional_enrichment";

  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-coverage-steward-resolution-v1.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-coverage-steward-resolution-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-coverage-steward-resolution-v1.md"
  );
  const md = renderStewardMd(summary);
  writeJson(reportJson, summary);
  writeText(reportMd, md);
  writeText(docsPath, md);
  writeJson(path.join(runDir, "summary.json"), summary);
  writeText(path.join(runDir, "summary.md"), md);

  log(
    `[coverage-steward] status=${summary.status} inserted=${insertedCount} remaining=${remainingManual.length}`
  );
  return summary;
}

export async function runCoverageStewardResolutionMission(opts = {}) {
  const args = opts.args || {};
  return runCoverageStewardResolution({
    ...opts,
    region: args.region || "CALA",
    mode: args.mode || "controlled",
    batchSize: args.batchSize || 100,
    enableProductionWrites: Boolean(opts.enableProductionWrites),
    allApplyConfirms: Boolean(args.allApplyConfirms),
    confirms: args.confirms,
    env: opts.env || process.env,
    log: opts.log,
  });
}
