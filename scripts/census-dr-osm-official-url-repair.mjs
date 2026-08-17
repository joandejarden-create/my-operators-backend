#!/usr/bin/env node
/**
 * DR OSM HPC — repair Official Property URLs that 404/403 for blank Rooms rows.
 * Uses a curated name→property-page map (brand domains only). Default dry-run.
 *
 * Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { isBrandHomepageOfficialUrl } from "../lib/independent-census/official-property-url-quality.js";
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

const USER_AGENT = "DealalityCensusUrlRepair/1.0 (research; dry-run)";

/** Exact Property Name → official property page (never brand homepage). */
export const DR_OSM_OFFICIAL_URL_REPAIRS = Object.freeze({
  "Be Live Grand Bavaro":
    "https://www.belivehotels.com/en/hotels-punta-cana/be-live-collection-puntacana/",
  "Be Live Grand Marien Hotel":
    "https://www.belivehotels.com/en/hotels-puerto-plata/be-live-collection-marien/",
  "Gran Bahia Principe Cayacoa":
    "https://www.bahia-principe.com/en/hotels/samana/resort-cayacoa/",
  "Bahía Príncipe Playa Nueva La Romana":
    "https://www.bahia-principe.com/en/resorts-in-dominican-republic/resort-playa-nueva-romana/",
  "Melia Santo Domingo Hotel":
    "https://www.melia.com/en/hotels/dominican-republic/santo-domingo/melia-santo-domingo",
  "Hotel Occidental Allegro Playa Dorada":
    "https://www.barcelo.com/en-us/occidental-allegro-playa-dorada/",
  "Barceló Puerto Plata":
    "https://www.barcelo.com/en-us/barcelo-puerto-plata/",
  "Catalonia Bavaro Royal":
    "https://www.cataloniahotels.com/en/hotel/catalonia-bavaro-beach",
  "Hotel Riu Naiboa":
    "https://www.riu.com/en/hotel/dominican-republic/punta-cana/hotel-riu-naiboa/",
  "Hotel RIU Merengue":
    "https://www.riu.com/en/hotel/dominican-republic/puerto-plata/clubhotel-riu-merengue/",
  "Hotel RIU Mambo":
    "https://www.riu.com/en/hotel/dominican-republic/puerto-plata/hotel-riu-mambo/",
  "Hotel RIU Bachata":
    "https://www.riu.com/en/hotel/dominican-republic/puerto-plata/hotel-riu-bachata/",
  "Hotel Riu Palace Bavaro":
    "https://www.riu.com/en/hotel/dominican-republic/punta-cana/hotel-riu-palace-bavaro/",
  "Grand Sirenis Punta Cana Resort":
    "https://www.grandsirenispuntacana.com/en/",
  "Renaissance Hotel":
    "https://www.marriott.com/en-us/hotels/sdqbr-renaissance-santo-domingo-jaragua-hotel/overview/",
  "Embassy Suites by Hilton":
    "https://www.hilton.com/en/hotels/sdqeses-embassy-suites-santo-domingo/",
  "Four Points by Sheraton":
    "https://www.marriott.com/en-us/hotels/sdqfp-four-points-santo-domingo/overview/",
  "Courtyard Marriott":
    "https://www.marriott.com/en-us/hotels/sdqcy-courtyard-santo-domingo/overview/",
  "Marriott Miches Beach, An All-Inclusive Resort":
    "https://www.marriott.com/en-us/hotels/popmm-marriott-miches-beach-resort/overview/",
});

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    probe: !argv.includes("--skip-probe"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function probeUrl(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 20000);
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: { "User-Agent": USER_AGENT, Accept: "text/html" },
    });
    return {
      ok: res.ok,
      status: res.status,
      final_url: String(res.url || url),
      brand_homepage: isBrandHomepageOfficialUrl(String(res.url || url)),
    };
  } catch (err) {
    return { ok: false, status: 0, reason: err.message || "fetch_failed" };
  } finally {
    clearTimeout(t);
  }
}

async function listOsmDr(baseId, token) {
  const fields = [
    "Property Name",
    "Official Property URL",
    "Rooms / Keys",
    "Property Identity Key",
    "Current Brand",
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
  const skipped = [];
  for (const rec of rows) {
    const f = rec.fields || {};
    const name = String(f["Property Name"] || "").trim();
    const repair = DR_OSM_OFFICIAL_URL_REPAIRS[name];
    if (!repair) continue;
    const current = String(f["Official Property URL"] || "").trim();
    if (current === repair) {
      skipped.push({ n: name, reason: "already_has_repair_url" });
      continue;
    }
    if (isBrandHomepageOfficialUrl(repair)) {
      skipped.push({ n: name, reason: "repair_is_brand_homepage" });
      continue;
    }
    let probe = null;
    if (args.probe) {
      probe = await probeUrl(repair);
      if (!probe.ok || probe.brand_homepage) {
        skipped.push({
          n: name,
          reason: !probe.ok
            ? `probe_failed_${probe.status || probe.reason}`
            : "probe_landed_brand_homepage",
          repair,
          probe,
        });
        continue;
      }
    }
    proposals.push({
      id: rec.id,
      property_name: name,
      current_brand: f["Current Brand"],
      current_url: current,
      rooms_blank: f["Rooms / Keys"] == null || f["Rooms / Keys"] === "",
      patch: { "Official Property URL": repair },
      probe,
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
    proposal_count: proposals.length,
    skipped_count: skipped.length,
    patched_count: patched.length,
    airtable_writes: doWrite,
    proposals,
    skipped,
  };
  mkdirSync(join(root, "reports"), { recursive: true });
  const out = doWrite
    ? "reports/census-dr-osm-official-url-repair-applied.json"
    : "reports/census-dr-osm-official-url-repair-dry-run.json";
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: out,
        proposal_count: report.proposal_count,
        skipped_count: report.skipped_count,
        airtable_writes: report.airtable_writes,
        sample: proposals.slice(0, 20).map((p) => ({
          n: p.property_name,
          from: p.current_url,
          to: p.patch["Official Property URL"],
          probe_status: p.probe?.status,
        })),
        skipped_sample: skipped.slice(0, 15),
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
