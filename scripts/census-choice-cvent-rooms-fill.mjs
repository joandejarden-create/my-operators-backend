#!/usr/bin/env node
/**
 * Cvent Supplier Network Guest Rooms discovery helper for Choice hard-cases.
 *
 * source-policy-v1: Cvent alone MUST NOT write canonical Rooms / Keys.
 * Default dry-run. APPLY of rooms/address from Cvent is blocked unless
 * --allow-cvent-canonical-override=1 (emergency only; still requires independent verify).
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
import {
  canPersistAsCanonical,
  createDiscoveryResearchCandidate,
  SourceContentDomain,
  SOURCE_POLICY_VERSION,
} from "../lib/data-intelligence/source-policy/v1/index.js";

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

    const roomsGate = canPersistAsCanonical({
      url: cfg.sourceUrl,
      contentDomain: SourceContentDomain.CVENT_VENUE_HOTEL,
      field: "Rooms / Keys",
      candidateValue: cfg.rooms,
    });
    const addressGate = canPersistAsCanonical({
      url: cfg.sourceUrl,
      contentDomain: SourceContentDomain.CVENT_VENUE_HOTEL,
      field: "Address",
      candidateValue: cfg.address,
    });
    const allowOverride =
      process.argv.includes("--allow-cvent-canonical-override=1") ||
      process.env.ALLOW_CVENT_CANONICAL_OVERRIDE === "1";

    if (cfg.fillRoomsIfBlank && blank(f["Rooms / Keys"])) {
      if (!roomsGate.ok && !allowOverride) {
        reasons.push(
          `rooms_discovery_only_blocked:${cfg.rooms}:${SOURCE_POLICY_VERSION}`
        );
        patch.__discoveryCandidates = patch.__discoveryCandidates || [];
        patch.__discoveryCandidates.push(
          createDiscoveryResearchCandidate(
            {
              url: cfg.sourceUrl,
              contentDomain: SourceContentDomain.CVENT_VENUE_HOTEL,
              field: "Rooms / Keys",
              candidateValue: cfg.rooms,
            },
            { notes: "Cvent rooms candidate — independent verification required" }
          )
        );
      } else if (allowOverride) {
        patch["Rooms / Keys"] = cfg.rooms;
        patch["Rooms Confidence"] = "Medium";
        patch["Rooms Source URL"] = cfg.sourceUrl;
        patch["Last Reviewed Date"] = todayIsoDate();
        reasons.push(
          `rooms_from_cvent_OVERRIDE:${cfg.rooms}:${SOURCE_POLICY_VERSION}`
        );
      }
    }

    if (cfg.fillAddressIfBlank && blank(f.Address) && cfg.address) {
      if (!addressGate.ok && !allowOverride) {
        reasons.push(`address_discovery_only_blocked:${SOURCE_POLICY_VERSION}`);
      } else if (allowOverride) {
        patch.Address = cfg.address;
        patch["Address Confidence"] = "Medium";
        patch["Address Source URL"] = cfg.sourceUrl;
        patch["Last Reviewed Date"] = todayIsoDate();
        reasons.push("address_from_cvent_OVERRIDE");
      }
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

    const discoveryCandidates = patch.__discoveryCandidates || [];
    delete patch.__discoveryCandidates;
    const airtableFields = { ...patch };

    if (!Object.keys(airtableFields).length) {
      skipped.push({
        id: rec.id,
        name,
        key,
        reasons: reasons.length
          ? reasons
          : ["nothing_to_write", `rooms=${f["Rooms / Keys"] || "blank"}`],
        discoveryCandidates,
      });
      continue;
    }

    proposals.push({
      id: rec.id,
      property_name: name,
      identity_key: key,
      patch: airtableFields,
      reasons,
      discoveryCandidates,
    });
  }

  const envCheck = checkIntakeApplyEnv();
  const doWrite = Boolean(args.apply && args.allConfirmsOk && envCheck.allOk);
  let patched = [];
  if (doWrite && proposals.length) {
    // Final safety: strip any Rooms/Keys / Address sourced solely from Cvent venue
    const writable = proposals.filter((p) => {
      const blocked = (p.reasons || []).some((r) =>
        /discovery_only_blocked|rooms_from_cvent(?!_OVERRIDE)/.test(String(r))
      );
      return !blocked;
    });
    patched = await patchRecords(
      baseId,
      token,
      writable.map((p) => ({ id: p.id, fields: p.patch }))
    );
  }

  const report = {
    status: doWrite ? "applied" : "dry_run",
    hard_rule: `${SOURCE_POLICY_VERSION}: Cvent venue Rooms/Keys CANNOT write canonical without override`,
    source_policy_version: SOURCE_POLICY_VERSION,
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
