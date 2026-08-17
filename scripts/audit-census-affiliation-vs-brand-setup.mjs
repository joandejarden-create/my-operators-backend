#!/usr/bin/env node
/**
 * Audit Hotel Census Affiliation vs Brand Setup Active/Live Brand Name.
 *
 * Authority: Brand Setup - Brand Basics (AIRTABLE_BASE_ID) · Brand Status Active/Live
 * Census + Brand Alias: platform base (AIRTABLE_BASE_ID_ALT)
 *
 * Safe rewrite candidates:
 * - Exact case/whitespace variants of Brand Setup Brand Name
 * - Brand Alias Mapping (Active) where Alias → Canonical resolves to Brand Setup Brand Name
 *
 *   node scripts/audit-census-affiliation-vs-brand-setup.mjs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import {
  HOTEL_CENSUS_TABLE,
  CENSUS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
} from "../lib/hotel-census/fields.js";
import {
  exactMatchKey,
  loadActiveBrandAliasRows,
} from "../lib/hotel-census/brand-alias-resolve.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const BRAND_NAME_FIELD = "Brand Name";
const PARENT_FIELD = "Parent Company";
const STATUS_FIELD = "Brand Status";

const REPORT_DIR = "reports";
const AUDIT_JSON = join(REPORT_DIR, "census-affiliation-vs-brand-setup-audit.json");
const AUDIT_CSV = join(REPORT_DIR, "census-affiliation-vs-brand-setup-mismatches.csv");
const READY_CSV = join(REPORT_DIR, "census-affiliation-vs-brand-setup-ready.csv");
const STEWARD_CSV = join(REPORT_DIR, "census-affiliation-vs-brand-setup-steward.csv");
const INVENTORY_CSV = join(REPORT_DIR, "census-affiliation-vs-brand-setup-inventory.csv");

const PROTECTED = new Set(["Independent", CENSUS_INDEPENDENT_AFFILIATION, ""]);

function foldKey(value) {
  return exactMatchKey(value)
    .toLowerCase()
    .replace(/[’']/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {object[]} aliasRows
 * @param {object[]} brandSetup
 */
function buildSafeAliasMaps(aliasRows, brandSetup) {
  const setupExact = new Set(brandSetup.map((b) => b.brandName));
  const setupFold = new Map(brandSetup.map((b) => [foldKey(b.brandName), b.brandName]));

  /** @type {Map<string, Set<string>>} */
  const aliasToTargets = new Map();
  /** @type {Map<string, object[]>} */
  const evidence = new Map();

  for (const row of aliasRows) {
    if (!row.active) continue;
    const alias = exactMatchKey(row.aliasSourceBrandName);
    const canonical = exactMatchKey(row.canonicalBrandName);
    if (!alias || !canonical) continue;

    let target = null;
    if (setupExact.has(canonical)) target = canonical;
    else if (setupFold.has(foldKey(canonical))) target = setupFold.get(foldKey(canonical));
    else continue;

    if (!aliasToTargets.has(alias)) aliasToTargets.set(alias, new Set());
    aliasToTargets.get(alias).add(target);
    if (!evidence.has(alias)) evidence.set(alias, []);
    evidence.get(alias).push({ ...row, resolvedBrandSetupName: target });
  }

  /** @type {Map<string, string>} */
  const safe = new Map();
  /** @type {object[]} */
  const conflicts = [];

  for (const [alias, targets] of aliasToTargets) {
    if (targets.size === 1) safe.set(alias, [...targets][0]);
    else conflicts.push({ alias, targets: [...targets], evidence: evidence.get(alias) });
  }

  /** @type {Map<string, string>} */
  const safeFold = new Map();
  for (const [alias, target] of safe) {
    const fk = foldKey(alias);
    if (!safeFold.has(fk)) safeFold.set(fk, target);
    else if (safeFold.get(fk) !== target) safeFold.delete(fk);
  }

  return { safe, safeFold, conflicts, setupExact, setupFold };
}

async function loadActiveBrandSetup(mvpBase) {
  const records = await mvpBase(BASICS_TABLE)
    .select({
      filterByFormula: BRAND_STATUS_ACTIVE_FORMULA,
      fields: [BRAND_NAME_FIELD, PARENT_FIELD, STATUS_FIELD],
    })
    .all();

  return records
    .map((r) => ({
      recordId: r.id,
      brandName: exactMatchKey(r.fields[BRAND_NAME_FIELD]),
      parentCompany: exactMatchKey(r.fields[PARENT_FIELD]),
      brandStatus: exactMatchKey(r.fields[STATUS_FIELD]),
    }))
    .filter((r) => r.brandName);
}

async function loadCensusAffiliationInventory(platformBase) {
  const records = await platformBase(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
      ],
      pageSize: 100,
    })
    .all();

  return records.map((r) => ({
    id: r.id,
    name: exactMatchKey(r.fields[CENSUS_FIELDS.name]),
    affiliation: exactMatchKey(r.fields[CENSUS_FIELDS.affiliation]),
    parentCompany: exactMatchKey(r.fields[CENSUS_FIELDS.parentCompany]),
    country: exactMatchKey(r.fields[CENSUS_FIELDS.country]),
    city: exactMatchKey(r.fields[CENSUS_FIELDS.city]),
    isCala: isCalaCountry(r.fields[CENSUS_FIELDS.country]),
  }));
}

export async function auditCensusAffiliationVsBrandSetup() {
  mkdirSync(REPORT_DIR, { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const mvpBaseId = process.env.AIRTABLE_BASE_ID;
  const platformBaseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !mvpBaseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  if (!platformBaseId) throw new Error("Set AIRTABLE_BASE_ID_ALT");

  const mvpBase = new Airtable({ apiKey }).base(mvpBaseId);
  const platformBase = new Airtable({ apiKey }).base(platformBaseId);

  console.log("Loading Brand Setup Active/Live (MVP base)…");
  const brandSetup = await loadActiveBrandSetup(mvpBase);
  console.log(`  ${brandSetup.length} brands`);

  console.log("Loading Brand Alias Mapping + Hotel Census (platform base)…");
  const aliasRows = await loadActiveBrandAliasRows();
  const { safe, safeFold, conflicts, setupExact, setupFold } = buildSafeAliasMaps(
    aliasRows,
    brandSetup
  );
  const census = await loadCensusAffiliationInventory(platformBase);
  console.log(`  aliases active=${aliasRows.length} safeMap=${safe.size} census=${census.length}`);

  /** @type {Map<string, { total: number, cala: number }>} */
  const inventory = new Map();
  for (const row of census) {
    const a = row.affiliation || "(blank)";
    if (!inventory.has(a)) inventory.set(a, { total: 0, cala: 0 });
    inventory.get(a).total++;
    if (row.isCala) inventory.get(a).cala++;
  }

  /** @type {object[]} */
  const brandSummaries = [];
  /** @type {object[]} */
  const ready = [];
  /** @type {object[]} */
  const steward = [];
  /** @type {Set<string>} */
  const queued = new Set();

  for (const brand of brandSetup) {
    const exactName = brand.brandName;
    const fold = foldKey(exactName);
    const exactRows = census.filter((r) => r.affiliation === exactName);
    const foldMismatch = census.filter(
      (r) => r.affiliation && r.affiliation !== exactName && foldKey(r.affiliation) === fold
    );
    const aliasHits = census.filter((r) => {
      if (!r.affiliation || r.affiliation === exactName) return false;
      if (PROTECTED.has(r.affiliation)) return false;
      const mapped = safe.get(r.affiliation) || safeFold.get(foldKey(r.affiliation));
      return mapped === exactName;
    });

    for (const r of [...foldMismatch, ...aliasHits]) {
      if (queued.has(r.id)) continue;
      queued.add(r.id);
      const isFold = foldKey(r.affiliation) === fold && r.affiliation !== exactName;
      ready.push({
        censusRecordId: r.id,
        censusName: r.name,
        country: r.country,
        isCala: r.isCala,
        fromAffiliation: r.affiliation,
        toAffiliation: exactName,
        reason: isFold ? "case_or_whitespace_variant" : "brand_alias_to_brand_setup",
        brandSetupRecordId: brand.recordId,
        parentCompany: brand.parentCompany,
      });
    }

    brandSummaries.push({
      brandName: exactName,
      parentCompany: brand.parentCompany,
      brandSetupRecordId: brand.recordId,
      censusExactTotal: exactRows.length,
      censusExactCala: exactRows.filter((r) => r.isCala).length,
      readyNormalize: foldMismatch.length + aliasHits.filter((r) => !queued.has(r.id) || true).length,
      missingInCalaOk: exactRows.filter((r) => r.isCala).length === 0,
    });
  }

  // Fix readyNormalize counts properly
  for (const b of brandSummaries) {
    b.readyNormalize = ready.filter((r) => r.toAffiliation === b.brandName).length;
  }

  for (const [aff, counts] of inventory) {
    if (aff === "(blank)" || PROTECTED.has(aff) || setupExact.has(aff)) continue;
    if (setupFold.has(foldKey(aff))) continue;
    if (safe.has(aff) || safeFold.has(foldKey(aff))) continue;

    for (const brand of brandSetup) {
      const bn = foldKey(brand.brandName);
      const af = foldKey(aff);
      const brandCore = bn
        .replace(/\bby (choice|hilton|hyatt|marriott|ihg|accor)\b/g, "")
        .replace(/\b(collection|hotels?|suites?|inn)\b/g, "")
        .replace(/[&]/g, " ")
        .replace(/\s+/g, " ")
        .trim();
      if (brandCore.length >= 4 && (af.includes(brandCore) || brandCore.split(" ").every((t) => t.length < 3 || af.includes(t)))) {
        // require stronger overlap: at least one meaningful token >=4 chars shared
        const tokens = brandCore.split(" ").filter((t) => t.length >= 4);
        if (!tokens.some((t) => af.includes(t))) continue;
        const samples = census.filter((r) => r.affiliation === aff).slice(0, 5);
        steward.push({
          affiliation: aff,
          suggestedBrandSetup: brand.brandName,
          reason: "near_active_brand_no_safe_alias",
          censusTotal: counts.total,
          censusCala: counts.cala,
          sampleRecordIds: samples.map((s) => s.id).join("|"),
          sampleNames: samples.map((s) => s.name).join(" || "),
        });
        break;
      }
    }
  }

  /** @type {object[]} */
  const aliasCanonicalDrift = [];
  for (const row of aliasRows) {
    if (!row.active) continue;
    const canonical = exactMatchKey(row.canonicalBrandName);
    if (!canonical || setupExact.has(canonical)) continue;
    if (setupFold.has(foldKey(canonical))) {
      aliasCanonicalDrift.push({
        alias: row.aliasSourceBrandName,
        aliasCanonical: canonical,
        brandSetupName: setupFold.get(foldKey(canonical)),
        issue: "canonical_case_whitespace_differs_from_brand_setup",
      });
    }
  }

  // Known Choice/Hilton naming drift: census often uses "(Choice)" / short Curio while Brand Setup uses "by Choice" / "by Hilton"
  const knownDriftPairs = [
    { fromRe: /^radisson \(choice\)$/i, to: "Radisson by Choice" },
    { fromRe: /^radisson blu \(choice\)$/i, to: "Radisson Blu by Choice" },
    { fromRe: /^radisson red\s*\(choice\)$/i, to: "Radisson RED by Choice" },
    { fromRe: /^radisson individual(?:s)? \(choice\)$/i, to: "Radisson Individuals by Choice" },
    { fromRe: /^curio collection$/i, to: "Curio Collection by Hilton" },
    { fromRe: /^tapestry collection$/i, to: "Tapestry Collection by Hilton" },
    { fromRe: /^kimpton(?:\s+hotels)?$/i, to: "Kimpton Hotels" },
    { fromRe: /^preferred hotels(?:\s+&\s+resorts)?$/i, to: "Preferred Hotels & Resorts" },
    { fromRe: /^country inn & suites(?: by (?:radisson|choice))?(?:\s*\(choice\))?$/i, to: "Country Inn & Suites by Choice" },
    { fromRe: /^ascend collection$/i, to: "Ascend Hotel Collection" },
    { fromRe: /^best western premier(?:\s+collection)?$/i, to: "BW Premier Collection" },
    { fromRe: /^best western signature(?:\s+collection)?$/i, to: "BW Signature Collection" },
  ];

  for (const pair of knownDriftPairs) {
    const target = brandSetup.find((b) => b.brandName === pair.to);
    if (!target) continue;
    for (const r of census) {
      if (!r.affiliation || r.affiliation === pair.to) continue;
      if (!pair.fromRe.test(r.affiliation)) continue;
      if (queued.has(r.id)) continue;
      // Only if Brand Setup has this exact option
      if (!setupExact.has(pair.to)) continue;
      queued.add(r.id);
      ready.push({
        censusRecordId: r.id,
        censusName: r.name,
        country: r.country,
        isCala: r.isCala,
        fromAffiliation: r.affiliation,
        toAffiliation: pair.to,
        reason: "known_brand_setup_naming_drift",
        brandSetupRecordId: target.recordId,
        parentCompany: target.parentCompany,
      });
    }
  }

  for (const b of brandSummaries) {
    b.readyNormalize = ready.filter((r) => r.toAffiliation === b.brandName).length;
  }

  const audit = {
    generatedAt: new Date().toISOString(),
    authority: {
      table: BASICS_TABLE,
      base: "AIRTABLE_BASE_ID",
      formula: BRAND_STATUS_ACTIVE_FORMULA,
      field: BRAND_NAME_FIELD,
      activeBrandCount: brandSetup.length,
    },
    brandSetupNames: brandSetup.map((b) => b.brandName),
    aliasSafeMapSize: safe.size,
    aliasConflicts: conflicts,
    aliasCanonicalDrift,
    brandSummaries,
    readyCount: ready.length,
    stewardNearBrandCount: steward.length,
    ready,
    steward,
  };

  writeFileSync(AUDIT_JSON, JSON.stringify(audit, null, 2));
  writeCsv(
    INVENTORY_CSV,
    [...inventory.entries()]
      .sort((a, b) => b[1].total - a[1].total)
      .map(([affiliation, c]) => ({
        affiliation,
        total: c.total,
        cala: c.cala,
        isActiveBrandSetup: setupExact.has(affiliation) ? "yes" : "no",
        foldMatchesActive: setupFold.has(foldKey(affiliation))
          ? setupFold.get(foldKey(affiliation))
          : "",
        safeAliasTarget: safe.get(affiliation) || safeFold.get(foldKey(affiliation)) || "",
      }))
  );
  writeCsv(READY_CSV, ready);
  writeCsv(STEWARD_CSV, steward);
  writeCsv(AUDIT_CSV, brandSummaries);

  console.log(`\nActive Brand Setup brands: ${brandSetup.length}`);
  console.log(`Safe alias→Brand Setup maps: ${safe.size}`);
  console.log(`Alias conflicts excluded: ${conflicts.length}`);
  console.log(`Ready Affiliation rewrites: ${ready.length}`);
  console.log(`Steward near-brand (no safe alias): ${steward.length}`);
  console.log(`\nPer-brand census exact / ready:`);
  for (const b of brandSummaries.sort(
    (a, c) => c.readyNormalize - a.readyNormalize || a.brandName.localeCompare(c.brandName)
  )) {
    console.log(
      `  ${b.readyNormalize ? "!" : " "} ${b.brandName}: exact=${b.censusExactTotal} (CALA ${b.censusExactCala}) ready=${b.readyNormalize}`
    );
  }
  if (ready.length) {
    /** @type {Map<string, number>} */
    const byPair = new Map();
    for (const r of ready) {
      const k = `${r.fromAffiliation} → ${r.toAffiliation}`;
      byPair.set(k, (byPair.get(k) || 0) + 1);
    }
    console.log("\nRewrite pairs:");
    for (const [k, n] of [...byPair.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`  ${n}\t${k}`);
    }
  }
  if (steward.length) {
    console.log("\nSteward near-brand:");
    for (const s of steward.slice(0, 25)) {
      console.log(`  "${s.affiliation}" (~${s.suggestedBrandSetup}) n=${s.censusTotal} cala=${s.censusCala}`);
    }
  }
  console.log(`\nWrote ${AUDIT_JSON}`);
  return audit;
}

import { pathToFileURL } from "node:url";

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  auditCensusAffiliationVsBrandSetup().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
