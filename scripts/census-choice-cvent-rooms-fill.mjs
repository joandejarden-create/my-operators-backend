#!/usr/bin/env node
/**
 * Apply Cvent Supplier Network Guest Rooms (+ optional Address corroboration)
 * for Choice hard-cases. Medium confidence. Default dry-run.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  resolveMapboxCoordinates,
  MAPBOX_COORDINATE_STATUSES,
} from "../lib/research-engine-v2/census-mapbox-coordinate-provider.js";
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

/** Manual Cvent venue extractions — Choice-affiliated pages. */
const CVENT = Object.freeze({
  ind_choice_mx_mx092: {
    rooms: 110,
    address: "Blvd. Villas de Irapuato 1502, Irapuato, Mexico, 36643",
    sourceUrl:
      "https://www.cvent.com/venues/irapuato/hotel/comfort-inn-irapuato/venue-f264d80b-e323-4365-842d-c91a18430d72",
    fillAddressIfBlank: false,
    fillRoomsIfBlank: true,
    retryMapbox: true,
  },
  ind_choice_mx_mx226: {
    rooms: 41,
    address: "Prol Tecnológico 1001-Norte, Queretaro, México, 76159",
    sourceUrl:
      "https://www.cvent.com/venues/es-ES/queretaro/hotel/comfort-inn-queretaro-tecnologico/venue-cd252652-75b1-454d-9360-bd48fb9000b1",
    fillAddressIfBlank: false,
    fillRoomsIfBlank: true,
    retryMapbox: false,
  },
});

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

async function listTargets(baseId, token) {
  const keys = Object.keys(CVENT);
  const or = keys.map((k) => `{Property Identity Key}='${k}'`).join(",");
  const formula = `OR(${or})`;
  const p = new URLSearchParams({ pageSize: "20", filterByFormula: formula });
  for (const f of [
    "Property Name",
    "Current Brand",
    "City",
    "Country",
    "State / Region",
    "Address",
    "Rooms / Keys",
    "Rooms Confidence",
    "Property Identity Key",
    "Latitude",
    "Longitude",
  ]) {
    p.append("fields[]", f);
  }
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${p}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json.error || json));
  return json.records || [];
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
  const rows = await listTargets(baseId, token);
  const proposals = [];
  const skipped = [];

  for (const rec of rows) {
    const f = rec.fields || {};
    const key = String(f["Property Identity Key"] || "").trim();
    const cfg = CVENT[key];
    if (!cfg) continue;
    const name = String(f["Property Name"] || "").trim();
    /** @type {Record<string, unknown>} */
    const patch = {};
    const reasons = [];

    if (cfg.fillRoomsIfBlank && blank(f["Rooms / Keys"])) {
      patch["Rooms / Keys"] = cfg.rooms;
      patch["Rooms Confidence"] = "Medium";
      patch["Rooms Source URL"] = cfg.sourceUrl;
      patch["Last Reviewed Date"] = todayIsoDate();
      reasons.push(`rooms_from_cvent_guest_rooms:${cfg.rooms}`);
    }

    if (cfg.fillAddressIfBlank && blank(f.Address) && cfg.address) {
      patch.Address = cfg.address;
      patch["Address Confidence"] = "Medium";
      patch["Address Source URL"] = cfg.sourceUrl;
      patch["Last Reviewed Date"] = todayIsoDate();
      reasons.push("address_from_cvent_venue");
    }

    if (
      cfg.retryMapbox &&
      blank(f.Latitude) &&
      (f.Address || cfg.address)
    ) {
      const mb = await resolveMapboxCoordinates(
        {
          propertyName: name,
          brand: f["Current Brand"],
          address: String(f.Address || cfg.address),
          city: f.City,
          stateRegion: f["State / Region"],
          country: f.Country || "Mexico",
        },
        { omitPropertyName: true }
      );
      if (mb.status === MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) {
        patch.Latitude = mb.latitude;
        patch.Longitude = mb.longitude;
        patch["Coordinate Source Type"] = "official_address_geocode";
        patch["Coordinate Confidence"] = "High";
        patch["Geocode Provider"] = "Mapbox";
        patch["Geocode Method"] = "permanent_geocoding_cvent_corroborated";
        patch["Geocode Reviewed Date"] = todayIsoDate();
        reasons.push(`coords_from_mapbox:${mb.reason}`);
      } else {
        reasons.push(`mapbox_${mb.status}:${mb.reason}`);
      }
    }

    if (!Object.keys(patch).length) {
      skipped.push({
        id: rec.id,
        name,
        key,
        reasons: ["nothing_to_write", `rooms=${f["Rooms / Keys"] || "blank"}`],
      });
      continue;
    }

    proposals.push({
      id: rec.id,
      property_name: name,
      identity_key: key,
      patch,
      reasons,
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
    hard_rule: "Cvent Guest Rooms Medium only; Choice-affiliated venue pages",
    generated_at: new Date().toISOString(),
    proposals,
    skipped,
    airtable_writes: doWrite,
    patched_count: patched.length,
  };
  mkdirSync(join(root, "reports"), { recursive: true });
  const outRel = doWrite
    ? "reports/census-choice-cvent-rooms-applied.json"
    : "reports/census-choice-cvent-rooms-dry-run.json";
  writeFileSync(join(root, outRel), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: outRel,
        proposals: proposals.map((p) => ({
          n: p.property_name,
          patch: p.patch,
          reasons: p.reasons,
        })),
        skipped,
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
