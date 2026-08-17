#!/usr/bin/env node
/**
 * DR official-parent coverage gap audit (read-only).
 *
 * For each Autopilot coverage parent with a discovery adapter, reconcile
 * official inventory for Country=Dominican Republic vs Hotel Property Census.
 * No Airtable writes.
 *
 * Also reports DR HPC brand rollup (all DR rows, not OSM-only) and flags
 * major resort brands that lack official adapters (RIU, Barceló, Bahía, etc.).
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  COVERAGE_PARENT_FRAMEWORK,
  COVERAGE_CLASS,
  runCoverageReconciliation,
} from "../lib/research-engine-v2/census-autopilot-coverage-reconciliation.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const COUNTRY = "Dominican Republic";

/** Major DR resort parents/brands without Autopilot discovery adapters yet. */
const NO_ADAPTER_BRAND_HINTS = [
  { label: "RIU", re: /\briu\b/i },
  { label: "Barceló / Occidental", re: /barcel[oó]|occidental/i },
  { label: "Bahía Príncipe", re: /bah[ií]a\s*pr[ií]ncipe|bahia\s*principe/i },
  { label: "Meliá", re: /meli[aá]/i },
  { label: "Catalonia", re: /catalonia/i },
  { label: "Be Live", re: /be\s*live/i },
  { label: "Hyatt Inclusive / Dreams / Breathless / Secrets", re: /hyatt|dreams|breathless|secrets|zo[eë]try/i },
  { label: "Iberostar", re: /iberostar/i },
  { label: "Hard Rock", re: /hard\s*rock/i },
  { label: "Lopesan", re: /lopesan/i },
  { label: "Sirenis", re: /sirenis/i },
  { label: "Hodelpa", re: /hodelpa/i },
];

async function listDrCensus(baseId, token) {
  const fields = [
    "Property Name",
    "Current Brand",
    "Brand Family",
    "City",
    "Official Property URL",
    "Property Identity Key",
    "VIC Freeze Hash",
    "Human Review Required",
  ];
  const formula = `AND({Country}='${COUNTRY}')`;
  const out = [];
  let offset;
  do {
    const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
    for (const f of fields) p.append("fields[]", f);
    if (offset) p.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${CENSUS_TABLE_ID}?${p}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

function brandRollup(records) {
  const byBrand = new Map();
  let osm = 0;
  for (const r of records) {
    const f = r.fields || {};
    const brand = String(f["Current Brand"] || "(blank)").trim() || "(blank)";
    byBrand.set(brand, (byBrand.get(brand) || 0) + 1);
    if (String(f["VIC Freeze Hash"] || "").includes("independent_census_dr_osm")) {
      osm += 1;
    }
  }
  return {
    total_dr: records.length,
    dr_osm_lineage: osm,
    by_brand: [...byBrand.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .map(([brand, count]) => ({ brand, count })),
  };
}

function noAdapterCensusHits(records) {
  return NO_ADAPTER_BRAND_HINTS.map((h) => {
    const hits = records.filter((r) => {
      const hay = `${r.fields?.["Current Brand"] || ""} ${r.fields?.["Brand Family"] || ""} ${r.fields?.["Property Name"] || ""}`;
      return h.re.test(hay);
    });
    return {
      label: h.label,
      census_count: hits.length,
      sample: hits.slice(0, 8).map((r) => r.fields?.["Property Name"]),
      note: "No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet",
    };
  });
}

function summarizeMissing(report) {
  const missingHotels = report.missing_hotels || [];
  const high = missingHotels.filter(
    (m) => m.coverage_class === COVERAGE_CLASS.MISSING_HIGH
  );
  const steward =
    report.stewarded_hotels ||
    missingHotels.filter(
      (m) => m.coverage_class === COVERAGE_CLASS.MISSING_STEWARD
    );
  const dup = report.duplicate_risks || [];

  const pick = (arr) =>
    (arr || []).slice(0, 25).map((m) => ({
      property_name: m.property_name || m.n,
      brand: m.brand || m.current_brand,
      city: m.city,
      coverage_class: m.coverage_class,
      official_property_url: m.official_property_url || m.url || null,
      official_property_id: m.official_property_id || m.marsha || null,
      identity_key: m.identity_key || m.property_identity_key || null,
      reason: m.reason || null,
    }));

  return {
    official_inventory_count: report.official_inventory_count ?? null,
    census_inventory_count: report.census_inventory_count ?? null,
    coverage_counts: report.coverage_counts || null,
    status: report.status,
    missing_high_sample: pick(high),
    missing_steward_sample: pick(steward),
    duplicate_risk_sample: pick(dup),
    missing_high_count:
      report.coverage_counts?.missing_high_confidence ?? high.length,
    missing_steward_count:
      report.coverage_counts?.missing_needs_steward ??
      (report.stewarded_count ?? steward.length),
    exact_count: report.coverage_counts?.existing_exact_match ?? null,
    probable_count: report.coverage_counts?.existing_probable_match ?? null,
    blocked_sources: report.source_blockers || [],
    official_sources: report.official_sources || [],
  };
}

async function main() {
  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  if (!token || !baseId) {
    console.error(JSON.stringify({ ok: false, error: "missing_airtable_credentials" }));
    process.exit(1);
  }

  console.log(`[dr-coverage] listing DR Hotel Property Census…`);
  const drRecords = await listDrCensus(baseId, token);
  const rollup = brandRollup(drRecords);
  const noAdapter = noAdapterCensusHits(drRecords);

  const parentReports = [];
  for (const parent of COVERAGE_PARENT_FRAMEWORK) {
    console.log(`[dr-coverage] reconciling parent=${parent} country=${COUNTRY}…`);
    try {
      const report = await runCoverageReconciliation({
        mode: "controlled",
        region: "CALA",
        parentCompany: parent,
        country: COUNTRY,
        discoveryCountries: [COUNTRY],
        enableProductionWrites: false,
        allApplyConfirms: false,
        discoverAllOfficialParents: true,
        delayMs: 200,
        log: (msg) => console.log(msg),
      });
      parentReports.push({
        parent,
        ok: report.ok !== false,
        ...summarizeMissing(report),
        run_dir: report.run_dir || null,
      });
    } catch (err) {
      parentReports.push({
        parent,
        ok: false,
        error: err?.message || String(err),
      });
    }
  }

  const totals = parentReports.reduce(
    (acc, p) => {
      if (!p.ok) return acc;
      acc.official += Number(p.official_inventory_count || 0);
      acc.exact += Number(p.exact_count || 0);
      acc.missing_high += Number(p.missing_high_count || 0);
      acc.missing_steward += Number(p.missing_steward_count || 0);
      return acc;
    },
    { official: 0, exact: 0, missing_high: 0, missing_steward: 0 }
  );

  const missingHighAll = parentReports.flatMap((p) =>
    (p.missing_high_sample || []).map((m) => ({ ...m, parent: p.parent }))
  );
  const missingStewardAll = parentReports.flatMap((p) =>
    (p.missing_steward_sample || []).map((m) => ({ ...m, parent: p.parent }))
  );

  const out = {
    status: "dry_run",
    airtable_writes: false,
    country: COUNTRY,
    generated_at: new Date().toISOString(),
    purpose:
      "Read-only official-parent coverage gap audit for Dominican Republic Hotel Property Census",
    dr_census: rollup,
    adapter_parents: COVERAGE_PARENT_FRAMEWORK,
    parent_results: parentReports,
    totals_across_adapter_parents: totals,
    missing_high_all: missingHighAll,
    missing_steward_all: missingStewardAll,
    no_adapter_resort_brands_in_census: noAdapter,
    next_recommended_actions: [
      totals.missing_high > 0
        ? `Review ${totals.missing_high} High-confidence missing properties from adapter parents; dry-run coverage insert before apply`
        : "No High-confidence missing properties from adapter parents for DR",
      "Build/extend official discovery adapters for RIU / Barceló / Bahía / Meliá / Hyatt Inclusive before treating those brands as coverage-complete",
      "Do not use Google Travel 2,641 as a census target — hotels+rentals mix",
    ],
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const jsonPath = "reports/census-dr-official-parent-coverage-gap-audit.json";
  const mdPath = "reports/census-dr-official-parent-coverage-gap-audit.md";
  writeFileSync(join(root, jsonPath), JSON.stringify(out, null, 2));

  const md = [
    `# DR Official Parent Coverage Gap Audit`,
    ``,
    `**Country:** ${COUNTRY}`,
    `**Airtable writes:** no`,
    `**Generated:** ${out.generated_at}`,
    ``,
    `## DR Hotel Property Census`,
    ``,
    `- Total DR rows: **${rollup.total_dr}**`,
    `- DR OSM lineage: **${rollup.dr_osm_lineage}**`,
    ``,
    `### Top brands in census`,
    ``,
    `| Brand | Count |`,
    `| --- | ---: |`,
    ...rollup.by_brand
      .slice(0, 40)
      .map((b) => `| ${b.brand} | ${b.count} |`),
    ``,
    `## Adapter parents (official discovery)`,
    ``,
    `| Parent | Official | Exact | Missing High | Missing Steward | Status |`,
    `| --- | ---: | ---: | ---: | ---: | --- |`,
    ...parentReports.map((p) =>
      p.ok
        ? `| ${p.parent} | ${p.official_inventory_count ?? "—"} | ${p.exact_count ?? "—"} | ${p.missing_high_count ?? "—"} | ${p.missing_steward_count ?? "—"} | ${p.status || "ok"} |`
        : `| ${p.parent} | — | — | — | — | ERROR: ${p.error || "failed"} |`
    ),
    ``,
    `### Totals (adapter parents only)`,
    ``,
    `- Official inventory rows discovered: **${totals.official}**`,
    `- Exact matches: **${totals.exact}**`,
    `- Missing High: **${totals.missing_high}**`,
    `- Missing Steward: **${totals.missing_steward}**`,
    ``,
    `### Missing High (sample)`,
    ``,
    ...(missingHighAll.length
      ? missingHighAll
          .slice(0, 40)
          .map(
            (m) =>
              `- **${m.property_name}** (${m.parent} / ${m.brand || "?"}, ${m.city || "?"}) — ${m.official_property_url || "no URL"}`
          )
      : ["- (none)"]),
    ``,
    `### Missing Steward (sample)`,
    ``,
    ...(missingStewardAll.length
      ? missingStewardAll
          .slice(0, 40)
          .map(
            (m) =>
              `- **${m.property_name}** (${m.parent} / ${m.brand || "?"}, ${m.city || "?"}) — ${m.official_property_url || "no URL"}`
          )
      : ["- (none)"]),
    ``,
    `## Resort brands without official adapters (census presence only)`,
    ``,
    `| Brand group | In DR census | Note |`,
    `| --- | ---: | --- |`,
    ...noAdapter.map(
      (n) =>
        `| ${n.label} | ${n.census_count} | ${n.note} |`
    ),
    ``,
    `## Next actions`,
    ``,
    ...out.next_recommended_actions.map((a) => `- ${a}`),
    ``,
  ].join("\n");
  writeFileSync(join(root, mdPath), md);

  console.log(
    JSON.stringify(
      {
        ok: true,
        output_json: jsonPath,
        output_md: mdPath,
        dr_total: rollup.total_dr,
        dr_osm: rollup.dr_osm_lineage,
        totals,
        missing_high_names: missingHighAll.map((m) => m.property_name).slice(0, 20),
        parents_ok: parentReports.filter((p) => p.ok).length,
        parents_failed: parentReports.filter((p) => !p.ok).map((p) => p.parent),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
