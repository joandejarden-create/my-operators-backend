/**
 * Dual-lane Hotel Property Census intake plan (READ-ONLY / dry-run).
 *
 * Lane A — Independent unaffiliated (promote_plan_l1_ready)
 * Lane B — Known brand / chain hotels held out of independent lane
 *           (Active/Live Autopilot-ready + known-chain-not-Active census backlog)
 *
 * Principle: brand-exclusion must NOT drop inventory. Non-Active known chains
 * still need Census rows; Active Brand Setup only gates Autopilot enrichment
 * priority, not whether the property may enter Hotel Property Census.
 *
 * No Airtable writes. Rejects --apply.
 */
import { createHash } from "crypto";
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";
import { ROUTE_BUCKETS } from "../lib/independent-census/brand-exclusion-audit.js";
import {
  isHostelOrHostalProperty,
  normalizeIntakeCensusFamilyFields,
} from "../lib/independent-census/intake-census-field-normalize.js";
import {
  productionHotelPropertyCensus,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = join(__dirname, "..", "reports");
const DOCS_DIR = join(__dirname, "..", "docs", "data-intelligence");

const BATCH_FREEZE = "independent_census_dr_osm_2026-08-07";
const PRODUCTION_USE = "Census Only / Not Owner-Facing";

const COUNTRY_NORMALIZE = {
  do: "Dominican Republic",
  "dominican republic": "Dominican Republic",
  "rep dominicana": "Dominican Republic",
  "republica dominicana": "Dominican Republic",
};

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error(
      "--apply is not supported. Dual-lane intake plan is dry-run only. No Census writes."
    );
  }

  let promotePlan = "";
  let exclusion = "";
  let osm = "";
  let hpcMatch = "";
  let batchId = "osm-dominican-republic-hotel-focused-2026-08-07";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--promote-plan" && argv[i + 1]) promotePlan = argv[++i];
    else if (a.startsWith("--promote-plan="))
      promotePlan = a.slice("--promote-plan=".length);
    else if (a === "--exclusion" && argv[i + 1]) exclusion = argv[++i];
    else if (a.startsWith("--exclusion="))
      exclusion = a.slice("--exclusion=".length);
    else if (a === "--osm" && argv[i + 1]) osm = argv[++i];
    else if (a.startsWith("--osm=")) osm = a.slice("--osm=".length);
    else if (a === "--hpc-match" && argv[i + 1]) hpcMatch = argv[++i];
    else if (a.startsWith("--hpc-match="))
      hpcMatch = a.slice("--hpc-match=".length);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i];
    else if (a.startsWith("--batch-id=")) batchId = a.slice("--batch-id=".length);
  }

  if (!promotePlan || !exclusion || !hpcMatch) {
    throw new Error(
      "Required: --promote-plan, --exclusion, and --hpc-match (Hotel Property Census match). Legacy Hotel Census is forbidden."
    );
  }

  return {
    promotePath: join(process.cwd(), promotePlan),
    exclusionPath: join(process.cwd(), exclusion),
    osmPath: osm ? join(process.cwd(), osm) : "",
    hpcMatchPath: join(process.cwd(), hpcMatch),
    batchId,
  };
}

function loadJson(p) {
  if (!existsSync(p)) throw new Error(`Not found: ${p}`);
  return JSON.parse(readFileSync(p, "utf8"));
}

function normCountry(raw) {
  const k = String(raw || "")
    .trim()
    .toLowerCase();
  return COUNTRY_NORMALIZE[k] || String(raw || "").trim() || "Dominican Republic";
}

function slugPart(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function osmIdentityKey(sourceRecordId, name, country) {
  const id = String(sourceRecordId || "")
    .replace(/\//g, "_")
    .toLowerCase();
  if (id) return `osm_do_${id}`;
  const h = createHash("sha1")
    .update(`${normCountry(country)}|${String(name || "").toLowerCase()}`)
    .digest("hex")
    .slice(0, 10);
  return `osm_do_${slugPart(name) || "hotel"}_${h}`;
}

function softBrand(name) {
  return /collection|individuals|ascend|tribute|autograph|tapestry|curio|design hotels|mgallery|trademark|lxr|preferred/i.test(
    String(name || "")
  );
}

/**
 * Classify known-brand intake subclass.
 */
function brandIntakeClass(route, matchedBrand) {
  if (
    route === ROUTE_BUCKETS.BRANDED_ACTIVE ||
    route === ROUTE_BUCKETS.BRANDED_SOFT ||
    route === ROUTE_BUCKETS.BRANDED_DOMAIN
  ) {
    return {
      intake_class: "active_or_soft_brand_census_plus_autopilot",
      enrichment_priority: "High",
      notes:
        "Eligible for Active Brand Autopilot coverage after Census insert (if brand is Active/Live).",
    };
  }
  if (route === ROUTE_BUCKETS.KNOWN_CHAIN_HOLD) {
    return {
      intake_class: "known_chain_census_backlog_not_active_setup",
      enrichment_priority: "High",
      notes:
        "Not in Active/Live Brand Setup control list — still required for Hotel Property Census inventory. Do not drop. Autopilot Active scope may not enrich until Brand Status promotion.",
    };
  }
  if (route === ROUTE_BUCKETS.POSSIBLE_BRANDED) {
    return {
      intake_class: "steward_brand_tag_review",
      enrichment_priority: "Medium",
      notes:
        "OSM brand tag unresolved — steward before Census insert; may be owner name noise or real chain.",
    };
  }
  return {
    intake_class: "other_excluded",
    enrichment_priority: "Low",
    notes: "",
  };
}

function buildIndependentPayload(row, osmById) {
  const osm = osmById.get(String(row.sourceRecordId || "")) || {};
  const country = normCountry(row.rawCountry || osm.rawCountry);
  const city =
    row.resolvedCity || row.rawCity || row.wikidataCity || osm.rawCity || "";
  const website =
    row.resolvedWebsite || row.rawWebsite || row.wikidataWebsite || osm.rawWebsite || "";
  const name = row.rawHotelName || osm.rawHotelName || "";
  const identityKey = osmIdentityKey(row.sourceRecordId, name, country);

  const validation_failures = [];
  if (!name) validation_failures.push("missing_property_name");
  if (!country) validation_failures.push("missing_country");
  if (!city) validation_failures.push("missing_city");
  if (!website) validation_failures.push("missing_source_or_official_url");
  if (isHostelOrHostalProperty(name)) {
    validation_failures.push("hostel_or_hostal_out_of_scope");
  }

  /** Exact field mapping → Hotel Property Census */
  const fields = normalizeIntakeCensusFamilyFields({
    "Property Name": name,
    "Canonical Property Name": name,
    "Property Identity Key": identityKey,
    "Current Brand": "Independent",
    "Affiliation Status": "Independent",
    "Independent Hotel Flag": true,
    Country: country,
    City: city || "Unknown",
    // State / Region omitted unless known — never invent "Unknown"
    "Official Property URL": website.startsWith("http") ? website : website ? `https://${website}` : "",
    "Source URL": osm.sourceUrl || `https://www.openstreetmap.org/${row.sourceRecordId}`,
    "Source Type": website ? "official_property_page" : "other",
    "Source Confidence": website ? "High" : "Medium",
    "Identity Confidence": validation_failures.length ? "Medium" : "High",
    "VIC Freeze Hash": BATCH_FREEZE,
    "Production Use Status": PRODUCTION_USE,
    "Enrichment Status": "Discovered — pending enrichment",
    "Enrichment Priority": "High",
    "Human Review Required": true,
    "Data Eligible": validation_failures.length === 0,
    Latitude: osm.rawLatitude ?? row.rawLatitude ?? null,
    Longitude: osm.rawLongitude ?? row.rawLongitude ?? null,
    Phone: osm.rawPhone || "",
    Address: osm.rawAddress || "",
  });

  return {
    lane: "independent_unaffiliated",
    intake_class: "independent_l1_promote",
    plan_tier: row.plan_tier,
    source_record_id: row.sourceRecordId,
    hpc_recommended_action: row.hpcRecommendedAction || "",
    hpc_matched_record_id: row.hpcMatchedRecordId || "",
    hpc_matched_name: row.hpcMatchedName || "",
    wikidata_qid: row.wikidataQid || "",
    validation: {
      pass: validation_failures.length === 0,
      failures: validation_failures,
    },
    field_mapping_used: Object.keys(fields),
    sanitized_payload_preview: fields,
    error_handling: {
      validation_error: "Do not send; fix city/website/name first",
      api_error: "Retry / steward; never partial silent write",
      network_error: "Retry with backoff; user message: census intake delayed",
    },
  };
}

function tryState() {
  // Prefer omit over inventing "Unknown" — State/Region stays blank until stewarded.
  return "";
}

function buildBrandedPayload(row, osmById) {
  const osm = osmById.get(String(row.sourceRecordId || "")) || {};
  const country = normCountry(row.rawCountry || osm.rawCountry);
  const city = row.resolvedCity || row.rawCity || osm.rawCity || "";
  const website =
    row.resolvedWebsite || row.rawWebsite || osm.rawWebsite || "";
  const name = row.rawHotelName || osm.rawHotelName || "";
  const brand =
    row.matchedBrand ||
    row.matchedFamily ||
    row.rawBrand ||
    "Brand-Unconfirmed";
  const identityKey = osmIdentityKey(row.sourceRecordId, name, country);
  const meta = brandIntakeClass(row.route, brand);
  const affiliation = softBrand(brand)
    ? "Soft-Branded / Collection"
    : row.route === ROUTE_BUCKETS.POSSIBLE_BRANDED
      ? "Brand-Unconfirmed"
      : "Branded";

  const validation_failures = [];
  if (!name) validation_failures.push("missing_property_name");
  if (!country) validation_failures.push("missing_country");
  if (isHostelOrHostalProperty(name)) {
    validation_failures.push("hostel_or_hostal_out_of_scope");
  }
  // City may be Unknown temporarily for known-chain backlog (flag Human Review)
  if (!brand || brand === "Brand-Unconfirmed") {
    /* steward path */
  }

  const fields = normalizeIntakeCensusFamilyFields({
    "Property Name": name,
    "Canonical Property Name": name,
    "Property Identity Key": identityKey,
    "Current Brand": brand,
    "Affiliation Status": affiliation,
    "Independent Hotel Flag": false,
    Country: country,
    City: city || "Unknown",
    // State / Region omitted unless known — never invent "Unknown"
    "Official Property URL": website
      ? website.startsWith("http")
        ? website
        : `https://${website}`
      : "",
    "Source URL":
      osm.sourceUrl || `https://www.openstreetmap.org/${row.sourceRecordId}`,
    "Source Type": website ? "official_property_page" : "other",
    "Source Confidence": website ? "High" : "Medium",
    "Identity Confidence":
      city && website ? "High" : city || website ? "Medium" : "Low",
    "VIC Freeze Hash": BATCH_FREEZE,
    "Production Use Status": PRODUCTION_USE,
    "Enrichment Status": "Discovered — pending enrichment",
    "Enrichment Priority": meta.enrichment_priority,
    "Human Review Required": true,
    "Data Eligible": Boolean(name && country && (city || website)),
    "Brand-Unassigned Reason":
      meta.intake_class === "known_chain_census_backlog_not_active_setup"
        ? "known_chain_not_in_active_brand_setup"
        : "",
    Latitude: osm.rawLatitude ?? null,
    Longitude: osm.rawLongitude ?? null,
    Phone: osm.rawPhone || "",
    Address: osm.rawAddress || "",
  });

  return {
    lane: "known_brand_census_intake",
    intake_class: meta.intake_class,
    intake_notes: meta.notes,
    route: row.route,
    reason: row.reason,
    source_record_id: row.sourceRecordId,
    hpc_recommended_action: row.hpcRecommendedAction || "",
    hpc_matched_record_id: row.hpcMatchedRecordId || "",
    hpc_matched_name: row.hpcMatchedName || "",
    validation: {
      pass: validation_failures.length === 0 && Boolean(name && country),
      failures: validation_failures,
    },
    field_mapping_used: Object.keys(fields),
    sanitized_payload_preview: fields,
    error_handling: {
      validation_error: "Do not send; steward brand/city",
      api_error: "Retry / steward",
      network_error: "Retry with backoff",
    },
  };
}

function isHpcDuplicateHold(action) {
  return (
    action === "likely_existing" || action === "possible_duplicate_review"
  );
}

function toMarkdown(report) {
  const lines = [
    `# Dual-Lane Census Intake Plan — Dominican Republic`,
    ``,
    `**Status:** \`dual_lane_census_intake_plan_dry_run_ready\``,
    `**Batch:** ${report.batch_id}`,
    `**Generated:** ${report.generated_at}`,
    `**Write target (future apply only):** ${report.write_target.base} → ${report.write_target.table}`,
    `**Dedupe SoT:** Hotel Property Census only`,
    `**Legacy Hotel Census:** forbidden / not used`,
    `**Airtable writes this run:** no`,
    ``,
    `## Principle`,
    ``,
    `Brand-exclusion routes hotels **away from the independent lane**, not out of Census.`,
    `Active/Live Brand Setup controls Autopilot **enrichment scope**, not whether a known-chain property may enter Hotel Property Census.`,
    `Duplicate prevention uses **Hotel Property Census only** — never legacy Hotel Census.`,
    ``,
    `## Lane counts`,
    ``,
    `| Lane / class | Count |`,
    `| --- | ---: |`,
    `| Independent L1 promote payloads | ${report.counts.independent_l1} |`,
    `| Known brand — Active/soft Autopilot-ready | ${report.counts.active_or_soft_brand_census_plus_autopilot} |`,
    `| Known brand — **census backlog (not Active Setup)** | ${report.counts.known_chain_census_backlog_not_active_setup} |`,
    `| Steward brand-tag review | ${report.counts.steward_brand_tag_review} |`,
    `| Independent L1 validation pass | ${report.counts.independent_l1_validation_pass} |`,
    `| Known brand validation pass | ${report.counts.known_brand_validation_pass} |`,
    `| Skipped HPC duplicate (\`likely_existing\`) | ${report.counts.skipped_hpc_likely_existing} |`,
    `| Held HPC possible duplicate (steward) | ${report.counts.held_hpc_possible_duplicate} |`,
    ``,
    `## Field mapping (both lanes)`,
    ``,
    `| Census field | Independent lane | Known-brand lane |`,
    `| --- | --- | --- |`,
    `| Affiliation Status | Independent | Branded / Soft-Branded / Brand-Unconfirmed |`,
    `| Current Brand | Independent | Matched chain / OSM brand |`,
    `| Independent Hotel Flag | true | false |`,
    `| Family / Source Family | independent_open_sources | chain / family label |`,
    `| VIC Freeze Hash | \`${BATCH_FREEZE}\` | same batch freeze |`,
    `| Production Use Status | Census Only / Not Owner-Facing | same |`,
    `| Property Identity Key | \`osm_do_<osm_id>\` | \`osm_do_<osm_id>\` |`,
    ``,
    `## Independent L1 sample`,
    ``,
    `| Name | City | HPC action | Validation |`,
    `| --- | --- | --- | --- |`,
    ...report.independent_sample.map(
      (r) =>
        `| ${r.sanitized_payload_preview["Property Name"]} | ${r.sanitized_payload_preview.City} | ${r.hpc_recommended_action || "likely_new"} | ${r.validation.pass ? "pass" : r.validation.failures.join(";")} |`
    ),
    ``,
    `## Known-chain backlog sample (keep for Census)`,
    ``,
    `| Name | Current Brand | Class | HPC action |`,
    `| --- | --- | --- | --- |`,
    ...report.known_chain_backlog_sample.map(
      (r) =>
        `| ${r.sanitized_payload_preview["Property Name"]} | ${r.sanitized_payload_preview["Current Brand"]} | ${r.intake_class} | ${r.hpc_recommended_action || ""} |`
    ),
    ``,
    `## Error handling path`,
    ``,
    `- **Validation fail:** payload not sent; listed in failures`,
    `- **HPC likely_existing:** skip insert; link / enrich existing row instead`,
    `- **HPC possible_duplicate_review:** hold for steward; do not auto-insert`,
    `- **API / network error:** retry; no silent catch`,
    ``,
    `## Next apply gates (explicit founder approval required)`,
    ``,
    `1. Spot-check Independent L1 validation-pass rows`,
    `2. Spot-check known-chain backlog vs Hotel Property Census holds`,
    `3. Separate Autopilot coverage run for Active/Live brands only`,
    `4. Keep known-chain-not-Active as Census inventory + Human Review; do not require Brand Explorer activation`,
    ``,
  ];
  return lines.join("\n");
}

async function main() {
  const args = parseArgs();
  const promote = loadJson(args.promotePath);
  const exclusion = loadJson(args.exclusionPath);
  const hpcMatch = loadJson(args.hpcMatchPath);
  if (hpcMatch.legacy_hotel_census_used === true) {
    throw new Error("HPC match report incorrectly used legacy Hotel Census");
  }
  const osm = args.osmPath ? loadJson(args.osmPath) : null;
  const osmById = new Map();
  for (const c of osm?.candidates || []) {
    osmById.set(String(c.sourceRecordId || ""), c);
  }
  const hpcById = new Map();
  for (const m of hpcMatch.matches || []) {
    hpcById.set(String(m.sourceRecordId || ""), m);
  }

  const independent = [];
  const knownBrand = [];
  let skipped_hpc_likely_existing = 0;
  let held_hpc_possible_duplicate = 0;

  function attachHpc(row) {
    const hpc = hpcById.get(String(row.sourceRecordId || "")) || null;
    return {
      ...row,
      hpcRecommendedAction: hpc?.recommendedAction || "",
      hpcMatchedRecordId: hpc?.matchedCensusRecordId || "",
      hpcMatchedName: hpc?.matchedCensusName || "",
      hpcMatchConfidence: hpc?.matchConfidence || "",
      // Clear any legacy match fields from earlier pipeline stages
      censusRecommendedAction: undefined,
      matchedCensusName: undefined,
    };
  }

  // Lane A: promote plan L1
  for (const row of promote.rows || []) {
    if (row.plan_tier !== "promote_plan_l1_ready") continue;
    const enriched = attachHpc(row);
    if (enriched.hpcRecommendedAction === "likely_existing") {
      skipped_hpc_likely_existing += 1;
      continue;
    }
    if (enriched.hpcRecommendedAction === "possible_duplicate_review") {
      held_hpc_possible_duplicate += 1;
      continue;
    }
    independent.push(buildIndependentPayload(enriched, osmById));
  }

  // Lane B: all brand-routed exclusion rows (not independent)
  const brandRoutes = new Set([
    ROUTE_BUCKETS.BRANDED_ACTIVE,
    ROUTE_BUCKETS.BRANDED_SOFT,
    ROUTE_BUCKETS.BRANDED_DOMAIN,
    ROUTE_BUCKETS.KNOWN_CHAIN_HOLD,
    ROUTE_BUCKETS.POSSIBLE_BRANDED,
  ]);

  for (const row of exclusion.rows || []) {
    if (!brandRoutes.has(row.route)) continue;
    const promo = (promote.rows || []).find(
      (p) => p.sourceRecordId === row.sourceRecordId
    );
    const enriched = attachHpc({
      ...row,
      resolvedCity: promo?.resolvedCity || row.rawCity,
      resolvedWebsite: promo?.resolvedWebsite || row.rawWebsite,
    });
    if (enriched.hpcRecommendedAction === "likely_existing") {
      skipped_hpc_likely_existing += 1;
      continue;
    }
    if (enriched.hpcRecommendedAction === "possible_duplicate_review") {
      held_hpc_possible_duplicate += 1;
      continue;
    }
    knownBrand.push(buildBrandedPayload(enriched, osmById));
  }

  const counts = {
    independent_l1: independent.length,
    independent_l1_validation_pass: independent.filter((r) => r.validation.pass)
      .length,
    active_or_soft_brand_census_plus_autopilot: knownBrand.filter(
      (r) => r.intake_class === "active_or_soft_brand_census_plus_autopilot"
    ).length,
    known_chain_census_backlog_not_active_setup: knownBrand.filter(
      (r) => r.intake_class === "known_chain_census_backlog_not_active_setup"
    ).length,
    steward_brand_tag_review: knownBrand.filter(
      (r) => r.intake_class === "steward_brand_tag_review"
    ).length,
    known_brand_validation_pass: knownBrand.filter((r) => r.validation.pass)
      .length,
    known_brand_total: knownBrand.length,
    skipped_hpc_likely_existing,
    held_hpc_possible_duplicate,
  };

  const report = {
    version: "dual-lane-census-intake-plan-v2",
    generated_at: new Date().toISOString(),
    batch_id: args.batchId,
    freeze_hash: BATCH_FREEZE,
    airtable_writes: false,
    hotel_property_census_writes: false,
    legacy_hotel_census_used: false,
    dedupe_source_of_truth: "Hotel Property Census",
    hpc_match_report: args.hpcMatchPath,
    hpc_match_summary: hpcMatch.summary || null,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    principle:
      "Known brands excluded from independent lane remain Census intake. Dedupe = Hotel Property Census only; legacy Hotel Census forbidden.",
    counts,
    independent_sample: independent.slice(0, 25),
    known_chain_backlog_sample: knownBrand
      .filter(
        (r) => r.intake_class === "known_chain_census_backlog_not_active_setup"
      )
      .slice(0, 40),
    active_brand_sample: knownBrand
      .filter(
        (r) => r.intake_class === "active_or_soft_brand_census_plus_autopilot"
      )
      .slice(0, 25),
    independent_payloads: independent,
    known_brand_payloads: knownBrand,
  };

  const slug = args.batchId;
  const jsonPath = join(REPORTS_DIR, `dual-lane-census-intake-plan-${slug}.json`);
  const mdPath = join(REPORTS_DIR, `dual-lane-census-intake-plan-${slug}.md`);
  const docPath = join(DOCS_DIR, `dual-lane-census-intake-plan-dominican-republic.md`);
  const indCsv = join(
    REPORTS_DIR,
    `dual-lane-census-intake-independent-${slug}.csv`
  );
  const brandCsv = join(
    REPORTS_DIR,
    `dual-lane-census-intake-known-brand-${slug}.csv`
  );

  const flatInd = independent.map((r) => ({
    lane: r.lane,
    intake_class: r.intake_class,
    validation_pass: r.validation.pass,
    validation_failures: r.validation.failures.join("|"),
    source_record_id: r.source_record_id,
    hpc_recommended_action: r.hpc_recommended_action,
    hpc_matched_record_id: r.hpc_matched_record_id,
    ...r.sanitized_payload_preview,
  }));
  const flatBrand = knownBrand.map((r) => ({
    lane: r.lane,
    intake_class: r.intake_class,
    intake_notes: r.intake_notes,
    route: r.route,
    validation_pass: r.validation.pass,
    source_record_id: r.source_record_id,
    hpc_recommended_action: r.hpc_recommended_action,
    hpc_matched_record_id: r.hpc_matched_record_id,
    ...r.sanitized_payload_preview,
  }));

  mkdirSync(REPORTS_DIR, { recursive: true });
  writeJson(jsonPath, report);
  writeCsv(indCsv, flatInd);
  writeCsv(brandCsv, flatBrand);
  const md = toMarkdown(report);
  writeFileSync(mdPath, md, "utf8");
  mkdirSync(DOCS_DIR, { recursive: true });
  writeFileSync(docPath, md, "utf8");

  console.log("Dual-lane Census intake plan (dry-run)");
  console.log(`  Dedupe SoT: Hotel Property Census (legacy forbidden)`);
  console.log(`  Independent L1 payloads: ${counts.independent_l1} (pass ${counts.independent_l1_validation_pass})`);
  console.log(
    `  Known brand total: ${counts.known_brand_total} (Active/soft ${counts.active_or_soft_brand_census_plus_autopilot}, backlog-not-Active ${counts.known_chain_census_backlog_not_active_setup}, steward ${counts.steward_brand_tag_review})`
  );
  console.log(
    `  HPC skip likely_existing: ${counts.skipped_hpc_likely_existing}; hold possible_dup: ${counts.held_hpc_possible_duplicate}`
  );
  console.log(`  wrote: ${jsonPath}`);
  console.log(`  wrote: ${docPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
