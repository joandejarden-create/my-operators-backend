#!/usr/bin/env node
/**
 * Close blank Market / Submarket on DR OSM HPC rows.
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { resolveMarketFromCity } from "../lib/research-engine-v2/census-region-market-map.js";
import { proposeCensusSubmarketCorridor } from "../lib/hotel-census/census-dealality-submarket.js";
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

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const READ_FIELDS = [
  "Property Name",
  "City",
  "Country",
  "Market",
  "Submarket",
  "Property Identity Key",
];

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    allowMedium: !argv.includes("--high-only"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listOsmDr(baseId, token) {
  const out = [];
  let offset;
  const formula =
    "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
  const base = new URLSearchParams({ filterByFormula: formula });
  for (const f of READ_FIELDS) base.append("fields[]", f);
  do {
    const params = new URLSearchParams(base);
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
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

function buildProposal(fields, allowMedium) {
  const city = String(fields.City || "").trim();
  const country = String(fields.Country || "Dominican Republic").trim();
  const marketBefore = String(fields.Market || "").trim();
  const subBefore = String(fields.Submarket || "").trim();
  /** @type {Record<string, string>} */
  const patch = {};
  const reasons = [];

  let market = marketBefore;
  if (!market && city) {
    const m = resolveMarketFromCity({ city, country });
    if (m.ok && m.market) {
      market = m.market;
      patch.Market = m.market;
      reasons.push(`market_${m.method}`);
    }
  }

  if (!subBefore && city) {
    const sub = proposeCensusSubmarketCorridor(
      {
        country,
        city,
        market,
        Market: market,
        Submarket: "",
        name: fields["Property Name"] || "",
      },
      { minConfidence: allowMedium ? "Medium" : "High", normalizeLabels: true }
    );
    const conf = String(sub?.confidence || "").toLowerCase();
    const okConf =
      conf === "high" || (allowMedium && conf === "medium");
    if (sub?.submarket && !sub.skipped && okConf && sub.submarket !== "Other") {
      patch.Submarket = sub.submarket;
      reasons.push(`submarket_${sub.reason || sub.source}_${conf}`);
    } else if (
      sub?.submarket === "Other" &&
      allowMedium &&
      conf === "medium"
    ) {
      // Border / sparse corridors — still better than blank when Medium city map.
      patch.Submarket = "Other";
      reasons.push("submarket_other_medium");
    }
  }

  return {
    ok: Object.keys(patch).length > 0,
    patch,
    reasons,
    steward: !city || (!marketBefore && !patch.Market && !subBefore && !patch.Submarket),
  };
}

async function main() {
  const args = parseArgs();
  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    console.error(JSON.stringify({ ok: false, blocked: "wrong_write_target" }));
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const rows = await listOsmDr(baseId, token);
  const proposals = [];
  const steward = [];

  for (const rec of rows) {
    const f = rec.fields || {};
    if (String(f.Market || "").trim() && String(f.Submarket || "").trim()) continue;
    const built = buildProposal(f, args.allowMedium);
    if (!built.ok) {
      steward.push({
        id: rec.id,
        n: f["Property Name"],
        city: f.City || null,
        market: f.Market || null,
        submarket: f.Submarket || null,
      });
      continue;
    }
    proposals.push({
      id: rec.id,
      property_name: f["Property Name"],
      city: f.City || null,
      patch: built.patch,
      reasons: built.reasons,
    });
  }

  const envCheck = checkIntakeApplyEnv();
  const doWrite = Boolean(args.apply && args.allConfirmsOk && envCheck.allOk);
  let patched = [];
  if (doWrite && proposals.length) {
    patched = await patchRecords(
      baseId,
      token,
      proposals.map((p) => ({ id: p.id, fields: p.patch }))
    );
  }

  const report = {
    status: doWrite ? "applied" : "dry_run",
    scanned: rows.length,
    proposal_count: proposals.length,
    steward_count: steward.length,
    patched_count: patched.length,
    airtable_writes: doWrite,
    proposals,
    steward,
  };
  mkdirSync(join(root, "reports"), { recursive: true });
  const out = doWrite
    ? "reports/census-dr-osm-market-submarket-closeout-applied.json"
    : "reports/census-dr-osm-market-submarket-closeout-dry-run.json";
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: out,
        proposal_count: report.proposal_count,
        steward_count: report.steward_count,
        patched_count: report.patched_count,
        airtable_writes: report.airtable_writes,
        sample: proposals.slice(0, 10).map((p) => ({
          n: p.property_name,
          city: p.city,
          ...p.patch,
        })),
        steward,
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
