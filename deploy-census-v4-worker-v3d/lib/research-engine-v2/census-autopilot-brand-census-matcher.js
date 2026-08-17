/**
 * Match Active / Live Brand Setup brands → production Hotel Property Census records
 * (read Hotel Property Census only; Brand Setup read-only control list).
 */

import fs from "node:fs";
import path from "node:path";

import { MAP_FIRST_PASS, loadActiveBrandUniverse, mapCensusBrand } from "./production-census-first-pass-enrichment.js";
import {
  formatBrandToHotelPropertyCensusMatchLine,
  PRECISE_MATCH_SUMMARY_LINE,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function isBlank(v) {
  return v == null || (typeof v === "string" && !v.trim());
}

/**
 * Soft-brand mappings (explicit only — no fuzzy).
 * Census brand string → active slug.
 */
export const APPROVED_SOFT_BRAND_MAPPINGS = Object.freeze({
  tapestry: "tapestry-collection-by-hilton",
  "tapestry by hilton": "tapestry-collection-by-hilton",
  curio: "curio-collection",
  "curio collection": "curio-collection",
  ascend: "ascend",
  "ascend hotel collection": "ascend",
});

/**
 * @param {object} controlList - from buildActiveBrandSetupControlList
 * @param {Array<{id: string, fields?: object}>} censusRecords
 * @param {{ region?: string, country?: string|null }} [opts]
 */
export function matchActiveBrandsToCensus(controlList, censusRecords = [], opts = {}) {
  const brands = controlList.brands || [];
  const bySlug = new Map(brands.map((b) => [b.brand_slug, b]));
  const aliasToSlug = new Map();
  for (const b of brands) {
    aliasToSlug.set(norm(b.brand_name), b.brand_slug);
    aliasToSlug.set(b.brand_slug, b.brand_slug);
    for (const a of b.census_matching_aliases || []) aliasToSlug.set(norm(a), b.brand_slug);
  }
  for (const [alias, slug] of Object.entries(APPROVED_SOFT_BRAND_MAPPINGS)) {
    if (bySlug.has(slug)) aliasToSlug.set(norm(alias), slug);
  }

  let universe;
  try {
    universe = loadActiveBrandUniverse();
  } catch {
    universe = { bySlug, byName: aliasToSlug };
  }

  const matched = [];
  const unmatchedCensus = [];
  const matchMethodCounts = { exact_match: 0, alias_match: 0, soft_brand_mapping: 0 };
  const softUsed = [];
  const aliasesUsed = [];

  for (const row of censusRecords) {
    const fields = row.fields || {};
    const country = String(fields[MAP_FIRST_PASS.country] || fields.Country || "").trim();
    if (opts.country && country && norm(country) !== norm(opts.country)) {
      unmatchedCensus.push({
        record_id: row.id,
        reason: "country_filter",
        country,
      });
      continue;
    }

    const brandMap = mapCensusBrand(fields, universe);
    const slugField = String(fields[MAP_FIRST_PASS.brandSlug] || "").trim();
    const brandName = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();

    let matchedSlug = null;
    let method = null;

    if (slugField && bySlug.has(slugField)) {
      matchedSlug = slugField;
      method = "exact_match";
    } else if (brandMap.active && brandMap.slug && bySlug.has(brandMap.slug)) {
      matchedSlug = brandMap.slug;
      method = brandMap.classification === "alias_match" ? "alias_match" : "exact_match";
    } else if (brandName && aliasToSlug.has(norm(brandName))) {
      matchedSlug = aliasToSlug.get(norm(brandName));
      const soft = APPROVED_SOFT_BRAND_MAPPINGS[norm(brandName)];
      method = soft ? "soft_brand_mapping" : "alias_match";
    }

    if (!matchedSlug) {
      unmatchedCensus.push({
        record_id: row.id,
        identity_key: fields[MAP_FIRST_PASS.identityKey],
        property_name: fields[MAP_FIRST_PASS.propertyName],
        brand: brandName,
        reason: brandMap.classification || "no_active_brand_match",
        relevant_for_steward: Boolean(brandName),
      });
      continue;
    }

    matchMethodCounts[method] = (matchMethodCounts[method] || 0) + 1;
    if (method === "soft_brand_mapping") softUsed.push({ record_id: row.id, slug: matchedSlug, brand: brandName });
    if (method === "alias_match") aliasesUsed.push({ record_id: row.id, slug: matchedSlug, brand: brandName });

    const ctrl = bySlug.get(matchedSlug);
    matched.push({
      record_id: row.id,
      identity_key: fields[MAP_FIRST_PASS.identityKey],
      property_name: fields[MAP_FIRST_PASS.propertyName],
      brand_slug: matchedSlug,
      brand_name: ctrl?.brand_name || brandName,
      parent_company: ctrl?.parent_company || null,
      brand_family: ctrl?.brand_family || null,
      match_method: method,
      fields,
    });
  }

  const matchedSlugs = new Set(matched.map((m) => m.brand_slug));
  const brandsWithNoCensus = brands
    .filter((b) => !matchedSlugs.has(b.brand_slug))
    .map((b) => ({
      brand_slug: b.brand_slug,
      brand_name: b.brand_name,
      parent_company: b.parent_company,
      status: "source_discovery_needed",
    }));

  const report = {
    version: "census-autopilot-brand-to-census-match-v1",
    generated_at: new Date().toISOString(),
    region: opts.region || controlList.region || "CALA",
    country: opts.country || null,
    match_summary_line: PRECISE_MATCH_SUMMARY_LINE,
    production_target: {
      baseName: productionHotelPropertyCensus.baseName,
      tableName: productionHotelPropertyCensus.tableName,
      tableId: productionHotelPropertyCensus.tableId,
      role: productionHotelPropertyCensus.role,
    },
    brand_setup_role: "read_only_active_live_control_list",
    active_brands_in_scope: brands.length,
    parent_companies_in_scope: controlList.parent_companies_in_scope || [],
    hotel_property_census_records_input: censusRecords.length,
    census_records_input: censusRecords.length,
    hotel_property_census_records_matched: matched.length,
    census_records_matched: matched.length,
    hotel_property_census_records_not_matched: unmatchedCensus.length,
    census_records_not_matched: unmatchedCensus.length,
    active_brands_with_no_census_records: brandsWithNoCensus.length,
    source_discovery_needed: brandsWithNoCensus,
    soft_brand_mappings_used: softUsed,
    aliases_used: aliasesUsed,
    match_method_counts: matchMethodCounts,
    records_excluded: unmatchedCensus.filter((u) => !u.relevant_for_steward),
    steward_candidates: unmatchedCensus.filter((u) => u.relevant_for_steward),
    matched_record_ids: matched.map((m) => m.record_id),
    matched,
  };

  return report;
}

export function renderBrandToCensusMatchMarkdown(report) {
  return [
    `# Brand Setup → Hotel Property Census Match Report`,
    ``,
    formatBrandToHotelPropertyCensusMatchLine(report),
    ``,
    `- **Region:** ${report.region}${report.country ? ` / ${report.country}` : ""}`,
    `- **Active / Live Brand Setup brands in scope:** ${report.active_brands_in_scope}`,
    `- **Parent companies:** ${(report.parent_companies_in_scope || []).join(", ") || "(n/a)"}`,
    `- **Production target:** ${report.production_target?.baseName || "Deal Capture Platform"} → ${report.production_target?.tableName || "Hotel Property Census"} (\`${report.production_target?.tableId || ""}\`)`,
    `- **Hotel Property Census matched:** ${report.census_records_matched}`,
    `- **Hotel Property Census not matched:** ${report.census_records_not_matched}`,
    `- **Active brands with no Hotel Property Census yet:** ${report.active_brands_with_no_census_records}`,
    `- **Match methods:** ${JSON.stringify(report.match_method_counts)}`,
    ``,
    `## Source discovery needed`,
    ``,
    ...(report.source_discovery_needed || []).slice(0, 40).map(
      (b) => `- ${b.brand_name} (\`${b.brand_slug}\`) — ${b.parent_company || "?"}`
    ),
    (report.source_discovery_needed || []).length > 40 ? `\n… +${report.source_discovery_needed.length - 40} more` : "",
    ``,
  ].join("\n");
}

export function writeBrandToCensusMatchReport(runDir, report) {
  fs.mkdirSync(runDir, { recursive: true });
  fs.writeFileSync(
    path.join(runDir, "brand-to-census-match-report.json"),
    JSON.stringify(report, null, 2),
    "utf8"
  );
  fs.writeFileSync(
    path.join(runDir, "brand-to-census-match-report.md"),
    renderBrandToCensusMatchMarkdown(report),
    "utf8"
  );
}
