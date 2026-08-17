#!/usr/bin/env node
/**
 * Cvent LATAM/Caribbean census harvest.
 * Default: dry-run (probe + inventory + sample parse). No Airtable writes.
 *
 * Apply:
 *   --apply --enable-production-writes
 *   + intake confirms
 *   ENABLE_CVENT_LATAM_UPDATES=1 and/or ENABLE_CVENT_LATAM_INSERTS=1
 *
 * Useful flags:
 *   --countries Mexico,Dominican-Republic
 *   --inventory-only
 *   --parse-all
 *   --sample-per-country 2
 *   --limit-venues 20
 *   --probe-only
 */
import "../load-env.js";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import {
  runCventLatamHarvest,
  applyCventLatamHarvestProposals,
  writeCventLatamHarvestReport,
} from "../lib/research-engine-v2/census-cvent-latam-harvest.js";
import {
  probeCventCountries,
} from "../lib/research-engine-v2/census-cvent-country-results-harvester.js";
import {
  resolveCventLatamCountries,
} from "../lib/research-engine-v2/census-cvent-latam-country-registry.js";
import { mkdirSync, writeFileSync } from "node:fs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const CENSUS_READ_FIELDS = [
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Current Brand",
  "City",
  "Country",
  "Address",
  "Rooms / Keys",
  "Rooms Confidence",
  "Official Property URL",
  "Hotel Description - Source Text",
  "Amenities - Source Text",
  "Meeting Space Flag",
  "Property Type",
  "Asset Context",
  "Phone",
  "Notes for Steward",
];

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  const countriesIdx = argv.indexOf("--countries");
  let countries = null;
  if (countriesIdx >= 0 && argv[countriesIdx + 1]) {
    countries = argv[countriesIdx + 1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  const sampleIdx = argv.indexOf("--sample-per-country");
  const limitIdx = argv.indexOf("--limit-venues");
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    probeOnly: argv.includes("--probe-only"),
    inventoryOnly: argv.includes("--inventory-only"),
    parseAll: argv.includes("--parse-all"),
    skipCensus: argv.includes("--skip-census-match"),
    countries,
    samplePerCountry:
      sampleIdx >= 0 && argv[sampleIdx + 1]
        ? Number(argv[sampleIdx + 1])
        : 2,
    limitVenues:
      limitIdx >= 0 && argv[limitIdx + 1] ? Number(argv[limitIdx + 1]) : null,
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listCensusByCountry(baseId, token, countryNames) {
  /** @type {Record<string, Array<{id:string,fields:object}>>} */
  const byCountry = {};
  for (const country of countryNames) {
    const formula = `{Country}='${String(country).replace(/'/g, "\\'")}'`;
    let offset;
    const out = [];
    do {
      const p = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
      for (const f of CENSUS_READ_FIELDS) p.append("fields[]", f);
      if (offset) p.set("offset", offset);
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${p}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const json = await res.json();
      if (!res.ok) throw new Error(JSON.stringify(json.error || json));
      out.push(...(json.records || []));
      offset = json.offset;
    } while (offset);
    byCountry[country] = out;
  }
  return byCountry;
}

async function patchRecords(baseId, token, records) {
  const updated = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    updated.push(...(json.records || []));
  }
  return updated;
}

async function main() {
  const args = parseArgs();
  const countries = resolveCventLatamCountries(args.countries);
  const log = (m) => console.error(m);

  if (args.probeOnly) {
    const probe = await probeCventCountries(countries, { throttleMs: 1100 });
    mkdirSync(join(root, "reports", "research-engine-v2"), { recursive: true });
    const out = join(
      root,
      "reports",
      "research-engine-v2",
      "cvent-latam-country-probe.json"
    );
    writeFileSync(out, JSON.stringify(probe, null, 2));
    console.log(
      JSON.stringify(
        {
          ok: true,
          mode: "probe_only",
          output: out,
          scanned: probe.scanned,
          viable_count: probe.viable_count,
          total_venues_reported: probe.total_venues_reported,
          viable: probe.viable,
        },
        null,
        2
      )
    );
    return;
  }

  /** @type {Record<string, Array>} */
  let censusByCountry = {};
  let baseId = null;
  let token = null;
  if (!args.skipCensusMatch && !args.inventoryOnly) {
    try {
      token = resolvePat();
      const bases = resolveTargetBase();
      baseId = bases.target_base_id;
      assertProductionCensusWriteTarget({
        baseName: productionHotelPropertyCensus.baseName,
        baseId,
        tableName: productionHotelPropertyCensus.tableName,
        tableId: CENSUS_TABLE_ID,
      });
      const names = countries.map((c) => c.country);
      log(`[cvent-latam] loading census rows for ${names.length} countries…`);
      censusByCountry = await listCensusByCountry(baseId, token, names);
    } catch (e) {
      log(
        `[cvent-latam] census match skipped (no Airtable / config): ${String(e?.message || e)}`
      );
      censusByCountry = {};
    }
  }

  const result = await runCventLatamHarvest({
    countries: args.countries,
    inventoryOnly: args.inventoryOnly,
    parseAll: args.parseAll,
    samplePerCountry: args.samplePerCountry,
    limitVenues: args.limitVenues,
    censusByCountry,
    throttleMs: 2000,
    useCache: true,
    log,
  });

  const envCheck = checkIntakeApplyEnv();
  const enableUpdates =
    String(process.env.ENABLE_CVENT_LATAM_UPDATES || "0").trim() === "1";
  const enableInserts =
    String(process.env.ENABLE_CVENT_LATAM_INSERTS || "0").trim() === "1";
  const doWrite = Boolean(
    args.apply && args.allConfirmsOk && envCheck.allOk && (enableUpdates || enableInserts)
  );

  let applyResult = null;
  if (doWrite) {
    if (!token || !baseId) {
      token = resolvePat();
      baseId = resolveTargetBase().target_base_id;
    }
    applyResult = await applyCventLatamHarvestProposals({
      updateProposals: enableUpdates ? result.updateProposals : [],
      insertProposals: enableInserts ? result.insertProposals : [],
      baseId,
      token,
      enableUpdates,
      enableInserts,
      patchRecords,
    });
    result.report.apply = applyResult;
    result.report.mode = "applied";
  } else {
    result.report.apply = {
      attempted: false,
      reason: !args.apply
        ? "dry_run_default"
        : !args.allConfirmsOk
          ? "missing_confirms"
          : !envCheck.allOk
            ? "env_gate_failed"
            : "update_insert_flags_off",
      enableUpdates,
      enableInserts,
      env_ok: envCheck.allOk,
      confirms_ok: args.allConfirmsOk,
    };
  }

  const paths = writeCventLatamHarvestReport(root, result.report, {
    applied: doWrite,
  });
  // Stable latest pointer for dry-run
  writeFileSync(
    join(root, "reports", "research-engine-v2", "cvent-latam-country-probe.json"),
    JSON.stringify(
      {
        ...result.probe,
        inventory_summary: result.report.inventory,
      },
      null,
      2
    )
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        status: doWrite ? "applied" : "dry_run",
        output_json: paths.jsonPath,
        output_md: paths.mdPath,
        probe: result.report.probe,
        inventory: result.report.inventory,
        parsed_count: result.report.parsed_count,
        update_proposal_count: result.report.update_proposal_count,
        insert_proposal_count: result.report.insert_proposal_count,
        skipped_count: result.report.skipped_count,
        apply: result.report.apply,
        sample: result.report.parsed_sample?.slice(0, 10),
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
