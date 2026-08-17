#!/usr/bin/env node
/**
 * Fill Choice Hotels International HPC Market + Submarket (High only).
 * Uses commercial city→Market map + High Submarket token/city rules.
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { resolveCommercialMarket } from "../lib/research-engine-v2/census-commercial-market-map.js";
import { resolveCommercialSubmarket } from "../lib/research-engine-v2/census-submarket-map.js";
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
  "Canonical Property Name",
  "City",
  "Country",
  "Address",
  "Market",
  "Submarket",
  "Property Identity Key",
];

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function blank(v) {
  return v == null || !String(v).trim();
}

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listChoice(baseId, token) {
  const formula = "{Brand Family}='Choice Hotels International'";
  let offset;
  const out = [];
  do {
    const p = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    for (const f of READ_FIELDS) p.append("fields[]", f);
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

function buildPatch(fields) {
  const city = String(fields.City || "").trim();
  const country = String(fields.Country || "").trim();
  /** @type {Record<string, string>} */
  const patch = {};
  const reasons = [];

  if (blank(fields.Market)) {
    const m = resolveCommercialMarket({ city, country });
    if (m.ok && m.market) {
      patch.Market = m.market;
      reasons.push(`market:${m.method || "ok"}`);
    } else {
      reasons.push(`market_blocked:${m.reason || "backlog"}`);
    }
  }

  const market = patch.Market || fields.Market;
  if (blank(fields.Submarket) && market) {
    const sub = resolveCommercialSubmarket({
      market,
      city,
      address: fields.Address,
      propertyName: fields["Canonical Property Name"] || fields["Property Name"],
    });
    if (sub.ok && sub.submarket) {
      patch.Submarket = sub.submarket;
      reasons.push(`submarket:${sub.method || "ok"}`);
    } else {
      reasons.push(`submarket_blocked:${sub.reason || "no_high"}`);
    }
  }

  return { patch, reasons };
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
  const rows = await listChoice(baseId, token);

  const proposals = [];
  const steward = [];
  const fieldHits = { Market: 0, Submarket: 0 };

  for (const rec of rows) {
    const f = rec.fields || {};
    if (!blank(f.Market) && !blank(f.Submarket)) continue;
    if (blank(f.City) || /^unknown$/i.test(String(f.City || ""))) {
      steward.push({
        id: rec.id,
        name: f["Property Name"],
        key: f["Property Identity Key"],
        reasons: ["blocked_missing_city"],
      });
      continue;
    }
    const { patch, reasons } = buildPatch(f);
    if (Object.keys(patch).length) {
      patch["Last Reviewed Date"] = todayIsoDate();
      if (patch.Market) fieldHits.Market += 1;
      if (patch.Submarket) fieldHits.Submarket += 1;
      proposals.push({
        id: rec.id,
        property_name: f["Property Name"],
        identity_key: f["Property Identity Key"],
        city: f.City,
        country: f.Country,
        before: { market: f.Market || null, submarket: f.Submarket || null },
        patch,
        reasons,
      });
    } else {
      steward.push({
        id: rec.id,
        name: f["Property Name"],
        key: f["Property Identity Key"],
        city: f.City,
        country: f.Country,
        reasons,
      });
    }
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
    hard_rule: "Choice Market/Submarket High only; never invent corridors",
    generated_at: new Date().toISOString(),
    scanned: rows.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    steward_count: steward.length,
    field_hits: fieldHits,
    airtable_writes: doWrite,
    proposals,
    steward: steward.slice(0, 50),
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const outRel = doWrite
    ? "reports/census-choice-market-submarket-applied.json"
    : "reports/census-choice-market-submarket-dry-run.json";
  writeFileSync(join(root, outRel), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: outRel,
        scanned: report.scanned,
        proposal_count: report.proposal_count,
        field_hits: report.field_hits,
        steward_count: report.steward_count,
        airtable_writes: report.airtable_writes,
        sample: proposals.slice(0, 15).map((p) => ({
          n: p.property_name,
          city: p.city,
          before: p.before,
          patch: { Market: p.patch.Market, Submarket: p.patch.Submarket },
          reasons: p.reasons,
        })),
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
