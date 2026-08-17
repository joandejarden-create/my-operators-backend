#!/usr/bin/env node
/**
 * DR OSM HPC — Rooms / Keys from Official Property URL (allows Human Review rows).
 * Uses Level-2 official rooms extractor (JSON-LD + High phrases). Never invent.
 *
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { isBrandHomepageOfficialUrl } from "../lib/independent-census/official-property-url-quality.js";
import { extractOfficialRoomsFromHtml } from "../lib/research-engine-v2/census-level-2-parent-extractors.js";
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

const USER_AGENT = "DealalityCensusRoomsEnrichment/1.0 (research; dry-run)";
const FETCH_TIMEOUT_MS = 25000;

const PRIORITY_BRAND_RE =
  /riu|barcel[oó]|bah[ií]a|hyatt|dreams|secrets|breathless|hilton|marriott|melia|meliá|catalonia|occidental|wyndham|hard rock|iberostar|hodelpa|be live|majestic/i;

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    limit: Number(get("--limit", "80")) || 80,
    delayMs: Number(get("--delay-ms", "350")) || 350,
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function fetchPage(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, Accept: "text/html" },
      redirect: "follow",
    });
    if (!res.ok) return { ok: false, reason: `http_${res.status}` };
    const html = await res.text();
    return { ok: true, html, final_url: String(res.url || url) };
  } catch (err) {
    return { ok: false, reason: err.message || "fetch_failed" };
  } finally {
    clearTimeout(t);
  }
}

async function listBlankRooms(baseId, token) {
  const fields = [
    "Property Name",
    "Current Brand",
    "Official Property URL",
    "Rooms / Keys",
    "Property Identity Key",
    "Family / Source Family",
  ];
  const formula =
    "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
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
  return out.filter((r) => {
    const f = r.fields || {};
    if (f["Rooms / Keys"] != null && f["Rooms / Keys"] !== "") return false;
    const url = String(f["Official Property URL"] || "").trim();
    return url && !isBrandHomepageOfficialUrl(url);
  });
}

function brandPriority(f) {
  const hay = `${f["Current Brand"] || ""} ${f["Family / Source Family"] || ""} ${f["Property Name"] || ""}`;
  return PRIORITY_BRAND_RE.test(hay) ? 0 : 1;
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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
  const candidates = await listBlankRooms(baseId, token);
  candidates.sort((a, b) => brandPriority(a.fields || {}) - brandPriority(b.fields || {}));
  const work = candidates.slice(0, args.limit);

  const proposals = [];
  const failed = [];
  for (const rec of work) {
    const f = rec.fields || {};
    const url = String(f["Official Property URL"] || "").trim();
    const page = await fetchPage(url);
    await sleep(args.delayMs);
    if (!page.ok) {
      failed.push({
        id: rec.id,
        n: f["Property Name"],
        brand: f["Current Brand"],
        reason: page.reason,
        priority: brandPriority(f) === 0,
      });
      continue;
    }
    const hit = extractOfficialRoomsFromHtml(page.html, page.final_url || url);
    if (!hit.ok) {
      failed.push({
        id: rec.id,
        n: f["Property Name"],
        brand: f["Current Brand"],
        reason: hit.reason || "no_official_rooms",
        priority: brandPriority(f) === 0,
      });
      continue;
    }
    proposals.push({
      id: rec.id,
      property_name: f["Property Name"],
      current_brand: f["Current Brand"],
      patch: {
        "Rooms / Keys": hit.rooms,
        "Rooms Confidence": "High",
        "Rooms Source URL": hit.source_url || page.final_url || url,
      },
      method: hit.method,
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
    candidates: candidates.length,
    processed: work.length,
    proposal_count: proposals.length,
    failed_count: failed.length,
    patched_count: patched.length,
    airtable_writes: doWrite,
    extractor: "extractOfficialRoomsFromHtml",
    proposals,
    failed: failed.slice(0, 60),
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const out = doWrite
    ? "reports/census-dr-osm-rooms-official-applied.json"
    : "reports/census-dr-osm-rooms-official-dry-run.json";
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: out,
        candidates: report.candidates,
        processed: report.processed,
        proposal_count: report.proposal_count,
        failed_count: report.failed_count,
        airtable_writes: report.airtable_writes,
        sample: proposals.slice(0, 15).map((p) => ({
          n: p.property_name,
          b: p.current_brand,
          rooms: p.patch["Rooms / Keys"],
          method: p.method,
        })),
        failed_priority_sample: failed.filter((f) => f.priority).slice(0, 12),
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
