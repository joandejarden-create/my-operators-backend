/**
 * Build two-source / promote-ready plan for independent DR pilot (READ-ONLY).
 *
 * Joins:
 *  - brand-exclusion audit rows
 *  - Wikidata ↔ OSM match report
 *
 * No Airtable writes. No --apply.
 */
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";
import { classifyCandidateBrandRoute } from "../lib/independent-census/brand-exclusion-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = join(__dirname, "..", "reports");
const DOCS_DIR = join(__dirname, "..", "docs", "data-intelligence");

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error("--apply is not supported. Promote plan is dry-run only.");
  }
  let exclusion = "";
  let wikidataMatch = "";
  let wikidataDry = "";
  let batchId = "osm-dominican-republic-hotel-focused-2026-08-07";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--exclusion" && argv[i + 1]) exclusion = argv[++i];
    else if (a.startsWith("--exclusion=")) exclusion = a.slice("--exclusion=".length);
    else if (a === "--wikidata-match" && argv[i + 1]) wikidataMatch = argv[++i];
    else if (a.startsWith("--wikidata-match="))
      wikidataMatch = a.slice("--wikidata-match=".length);
    else if (a === "--wikidata-dry-run" && argv[i + 1]) wikidataDry = argv[++i];
    else if (a.startsWith("--wikidata-dry-run="))
      wikidataDry = a.slice("--wikidata-dry-run=".length);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i];
    else if (a.startsWith("--batch-id=")) batchId = a.slice("--batch-id=".length);
  }

  if (!exclusion || !wikidataMatch) {
    throw new Error("Required: --exclusion and --wikidata-match report paths");
  }

  return {
    exclusionPath: join(process.cwd(), exclusion),
    matchPath: join(process.cwd(), wikidataMatch),
    wikidataDryPath: wikidataDry ? join(process.cwd(), wikidataDry) : "",
    batchId,
  };
}

function loadJson(path) {
  if (!existsSync(path)) throw new Error(`Not found: ${path}`);
  return JSON.parse(readFileSync(path, "utf8"));
}

function planTier(row) {
  if (row.route !== "independent_unaffiliated_candidate") {
    return {
      tier: "excluded_not_independent",
      next: "route_to_branded_or_steward",
    };
  }

  const hasWebsite = Boolean(String(row.rawWebsite || "").trim());
  const hasCity = Boolean(String(row.rawCity || "").trim());
  const wd = row.wikidataMatchConfidence;
  const wdSite = Boolean(String(row.wikidataWebsite || "").trim());
  const wdCity = Boolean(String(row.wikidataCity || "").trim());
  const twoSource =
    wd === "high" || wd === "medium" || (hasWebsite && (wdSite || wdCity));
  const cityResolved = hasCity || wdCity;
  const websiteResolved = hasWebsite || wdSite;
  const notLegacyDup = row.censusRecommendedAction !== "likely_existing";

  if (
    websiteResolved &&
    cityResolved &&
    notLegacyDup &&
    (row.qualityScore ?? 0) >= 55 &&
    (twoSource || (hasWebsite && (row.qualityScore ?? 0) >= 70))
  ) {
    return {
      tier: "promote_plan_l1_ready",
      next: "dry_run_census_insert_affiliation_independent",
    };
  }

  if (websiteResolved && notLegacyDup && (row.qualityScore ?? 0) >= 55) {
    return {
      tier: "enrich_city_then_promote",
      next: "city_from_wikidata_or_geocode_steward",
    };
  }

  if (!websiteResolved && (wd === "high" || wd === "medium")) {
    return {
      tier: "wikidata_matched_needs_website",
      next: "official_site_lookup_or_webhound_hard_case",
    };
  }

  if (!websiteResolved && !hasCity) {
    return {
      tier: "hard_case_no_website_no_city",
      next: "webhound_candidate_after_wikidata",
    };
  }

  if (row.censusRecommendedAction === "likely_existing") {
    return {
      tier: "reconcile_legacy_census",
      next: "link_not_insert",
    };
  }

  return {
    tier: "needs_research",
    next: "manual_or_secondary_source",
  };
}

function toMarkdown(report) {
  const t = report.tier_counts;
  const lines = [
    `# Independent Census — DR Two-Source Promote Plan`,
    ``,
    `**Status:** \`independent_census_dr_promote_plan_dry_run_ready\``,
    `**Batch:** ${report.batch_id}`,
    `**Generated:** ${report.generated_at}`,
    `**Airtable writes:** no`,
    ``,
    `## Tier counts`,
    ``,
    `| Tier | Count |`,
    `| --- | ---: |`,
    ...Object.entries(t).map(([k, v]) => `| \`${k}\` | ${v} |`),
    ``,
    `## Promote L1 sample`,
    ``,
    `| Name | City (OSM) | City (WD) | Website | WD conf |`,
    `| --- | --- | --- | --- | --- |`,
    ...report.promote_l1_sample.map(
      (r) =>
        `| ${r.rawHotelName} | ${r.rawCity || ""} | ${r.wikidataCity || ""} | ${r.resolvedWebsite || ""} | ${r.wikidataMatchConfidence || ""} |`
    ),
    ``,
    `## Hard-case Webhound sample (≤25)`,
    ``,
    `| Name | City | Quality | Reason |`,
    `| --- | --- | ---: | --- |`,
    ...report.webhound_candidates_sample.map(
      (r) =>
        `| ${r.rawHotelName} | ${r.rawCity || ""} | ${r.qualityScore ?? ""} | ${r.plan_tier} |`
    ),
    ``,
    `## Steward possible branded (from exclusion)`,
    ``,
    `| Name | OSM brand | Reason |`,
    `| --- | --- | --- |`,
    ...report.steward_sample.map(
      (r) => `| ${r.rawHotelName} | ${r.rawBrand || r.matchedBrand || ""} | ${r.reason} |`
    ),
    ``,
    `## Next`,
    ``,
    `1. Review promote_plan_l1_ready rows (human spot-check)`,
    `2. Do **not** apply Census inserts until founder gate`,
    `3. Optional Webhound on hard_case sample only (10–25)`,
    `4. City enrichment lane for enrich_city_then_promote`,
    ``,
  ];
  return lines.join("\n");
}

async function main() {
  const args = parseArgs();
  const exclusion = loadJson(args.exclusionPath);
  const matchReport = loadJson(args.matchPath);
  const wdDry = args.wikidataDryPath ? loadJson(args.wikidataDryPath) : null;

  const wdByOsmId = new Map();
  for (const m of matchReport.matches || []) {
    const osmId = String(m.matchedStagingSourceRecordId || "");
    if (!osmId) continue;
    const conf = m.matchConfidence || "none";
    const prev = wdByOsmId.get(osmId);
    if (!prev || (conf === "high" && prev.matchConfidence !== "high")) {
      wdByOsmId.set(osmId, m);
    } else if (!prev) {
      wdByOsmId.set(osmId, m);
    } else if (
      (conf === "high" || conf === "medium") &&
      (m.matchScore || 0) > (prev.matchScore || 0)
    ) {
      wdByOsmId.set(osmId, m);
    }
  }

  const wdByQid = new Map();
  for (const c of wdDry?.candidates || []) {
    wdByQid.set(String(c.sourceRecordId || ""), c);
  }

  const tier_counts = {};
  const rows = [];

  for (const r of exclusion.rows || []) {
    const wd = wdByOsmId.get(String(r.sourceRecordId || "")) || null;
    const wdCand = wd ? wdByQid.get(String(wd.wikidataQid || "")) : null;

    const enriched = {
      ...r,
      wikidataQid: wd?.wikidataQid || "",
      wikidataName: wd?.wikidataName || "",
      wikidataCity: wd?.wikidataCity || wdCand?.rawCity || "",
      wikidataWebsite: wd?.wikidataWebsite || wdCand?.rawWebsite || "",
      wikidataMatchConfidence: wd?.matchConfidence || "",
      wikidataMatchScore: wd?.matchScore ?? "",
      wikidataRecommendedAction: wd?.recommendedAction || "",
      resolvedCity: r.rawCity || wd?.wikidataCity || wdCand?.rawCity || "",
      resolvedWebsite: r.rawWebsite || wd?.wikidataWebsite || wdCand?.rawWebsite || "",
    };

    // Re-check brand exclusion if Wikidata website reveals a known chain domain
    if (
      enriched.route === "independent_unaffiliated_candidate" &&
      enriched.resolvedWebsite &&
      enriched.resolvedWebsite !== r.rawWebsite
    ) {
      const reclass = classifyCandidateBrandRoute({
        rawHotelName: r.rawHotelName,
        rawBrand: r.rawBrand,
        rawWebsite: enriched.resolvedWebsite,
        qualityScore: r.qualityScore,
        missingFields: String(r.missingFields || "")
          .split("|")
          .filter(Boolean),
      });
      if (!reclass.independent_lane_eligible) {
        enriched.route = reclass.route;
        enriched.reason = `wikidata_website_reclass:${reclass.reason}`;
        enriched.matchedBrand = reclass.matched_brand || enriched.matchedBrand;
        enriched.independentLaneEligible = false;
      }
    }

    const plan = planTier(enriched);
    enriched.plan_tier = plan.tier;
    enriched.plan_next = plan.next;
    tier_counts[plan.tier] = (tier_counts[plan.tier] || 0) + 1;
    rows.push(enriched);
  }

  const promote = rows.filter((r) => r.plan_tier === "promote_plan_l1_ready");
  const hard = rows.filter(
    (r) =>
      r.plan_tier === "hard_case_no_website_no_city" ||
      r.plan_tier === "wikidata_matched_needs_website"
  );
  const steward = rows.filter((r) => r.route === "steward_possible_branded");

  const report = {
    version: "independent-census-dr-promote-plan-v1",
    generated_at: new Date().toISOString(),
    batch_id: args.batchId,
    airtable_writes: false,
    hotel_property_census_writes: false,
    exclusion_report: args.exclusionPath,
    wikidata_match_report: args.matchPath,
    wikidata_dry_run: args.wikidataDryPath || null,
    wikidata_match_summary: matchReport.summary || null,
    tier_counts,
    promote_l1_count: promote.length,
    hard_case_count: hard.length,
    steward_possible_branded_count: steward.length,
    promote_l1_sample: promote.slice(0, 40),
    webhound_candidates_sample: hard.slice(0, 25),
    steward_sample: steward.slice(0, 40),
    rows,
  };

  const slug = args.batchId;
  const jsonPath = join(REPORTS_DIR, `independent-census-dr-promote-plan-${slug}.json`);
  const csvPath = join(REPORTS_DIR, `independent-census-dr-promote-plan-${slug}.csv`);
  const mdPath = join(REPORTS_DIR, `independent-census-dr-promote-plan-${slug}.md`);
  const docPath = join(DOCS_DIR, `independent-census-dr-promote-plan.md`);

  mkdirSync(REPORTS_DIR, { recursive: true });
  writeJson(jsonPath, report);
  writeCsv(csvPath, rows, [
    "sourceRecordId",
    "rawHotelName",
    "rawCity",
    "resolvedCity",
    "rawWebsite",
    "resolvedWebsite",
    "qualityScore",
    "route",
    "reason",
    "matchedBrand",
    "censusRecommendedAction",
    "wikidataQid",
    "wikidataCity",
    "wikidataWebsite",
    "wikidataMatchConfidence",
    "wikidataMatchScore",
    "plan_tier",
    "plan_next",
  ]);
  const md = toMarkdown(report);
  writeFileSync(mdPath, md, "utf8");
  mkdirSync(DOCS_DIR, { recursive: true });
  writeFileSync(docPath, md, "utf8");

  console.log(`DR promote plan: ${slug}`);
  console.log(`  promote_plan_l1_ready: ${promote.length}`);
  console.log(`  hard_case / webhound pool: ${hard.length}`);
  console.log(`  steward_possible_branded: ${steward.length}`);
  console.log(`  tiers:`, tier_counts);
  console.log(`  wrote: ${jsonPath}`);
  console.log(`  wrote: ${mdPath}`);
  console.log(`  wrote: ${docPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
