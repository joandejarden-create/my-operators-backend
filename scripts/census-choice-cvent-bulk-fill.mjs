#!/usr/bin/env node
/**
 * Bulk Cvent Supplier Network fill for Choice Hotels International HPC.
 * Medium Address + Rooms when blank; also Official URL / description / meeting flag /
 * Property Type / Airport Asset Context when blank. Mapbox coords when new street Address.
 * Never maps meeting-room counts → Rooms / Keys; never writes Cvent lat/lng.
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 *
 * Optional: --limit N  --skip-mapbox  --ids rec1,rec2
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { isStreetLevelAddress } from "../lib/research-engine-v2/production-census-geocoding-providers.js";
import {
  resolveMapboxCoordinates,
  MAPBOX_COORDINATE_STATUSES,
} from "../lib/research-engine-v2/census-mapbox-coordinate-provider.js";
import {
  resolveCventVenueForHotel,
  normCventText,
} from "../lib/research-engine-v2/census-cvent-venue-client.js";
import {
  buildCventChoicePatch,
  CVENT_IDENTITY_STEWARD_KEYS,
} from "../lib/research-engine-v2/census-cvent-choice-matcher.js";
import {
  CHOICE_CVENT_URL_SEEDS,
  CHOICE_CVENT_NAME_SEEDS,
} from "../lib/research-engine-v2/census-cvent-choice-url-seeds.js";
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
  "Official Property URL",
  "Hotel Description - Source Text",
  "Meeting Space Flag",
  "Property Type",
  "Asset Context",
  "Phone",
  "Notes for Steward",
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
  let limit = null;
  const limIdx = argv.indexOf("--limit");
  if (limIdx >= 0 && argv[limIdx + 1]) limit = Number(argv[limIdx + 1]);
  let ids = null;
  const idsIdx = argv.indexOf("--ids");
  if (idsIdx >= 0 && argv[idsIdx + 1]) {
    ids = new Set(
      argv[idsIdx + 1]
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    );
  }
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    withMapbox: !argv.includes("--skip-mapbox"),
    seedsOnly: argv.includes("--seeds-only"),
    limit: Number.isFinite(limit) && limit > 0 ? limit : null,
    ids,
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listChoiceGaps(baseId, token) {
  const formula =
    "AND({Brand Family}='Choice Hotels International',OR({Address}='',{Address}=BLANK(),{Rooms / Keys}=BLANK()))";
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
  return out.filter((r) => {
    const f = r.fields || {};
    return blank(f.Address) || blank(f["Rooms / Keys"]);
  });
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

async function resolveWithSeeds(f, { seedsOnly = false } = {}) {
  const name = String(f["Property Name"] || "").trim();
  const brand = String(f["Current Brand"] || "").trim();
  const city = String(f.City || "").trim();
  const country = String(f.Country || "Mexico").trim();
  const key = String(f["Property Identity Key"] || "").trim();
  const hay = normCventText([name, brand, city].join(" "));

  const seedUrls = [];
  if (CHOICE_CVENT_URL_SEEDS[key]) seedUrls.push(CHOICE_CVENT_URL_SEEDS[key]);
  for (const s of CHOICE_CVENT_NAME_SEEDS) {
    if (s.match.every((t) => hay.includes(normCventText(t)))) {
      seedUrls.push(s.url);
    }
  }

  if (seedsOnly) {
    if (!seedUrls.length) {
      return { ok: false, reason: "no_seed_url", venue: null, tried: [] };
    }
    return resolveCventVenueForHotel(
      { name, brand, city, country },
      { throttleMs: 700, useCache: true, seedUrls, skipDiscover: true }
    );
  }

  return resolveCventVenueForHotel(
    { name, brand, city, country },
    { throttleMs: 900, useCache: true, seedUrls }
  );
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
  let rows = await listChoiceGaps(baseId, token);
  if (args.ids) rows = rows.filter((r) => args.ids.has(r.id));
  if (args.limit) rows = rows.slice(0, args.limit);

  const proposals = [];
  const steward = [];
  const today = todayIsoDate();
  let mapboxLookups = 0;
  let addressHits = 0;
  let roomsHits = 0;

  for (const rec of rows) {
    const f = rec.fields || {};
    const name = String(f["Property Name"] || "").trim();
    const brand = String(f["Current Brand"] || "").trim();
    const city = String(f.City || "").trim();
    const country = String(f.Country || "Mexico").trim();
    const key = String(f["Property Identity Key"] || "").trim();

    const idBlock = CVENT_IDENTITY_STEWARD_KEYS[key];
    if (idBlock && blank(f.Address) && !blank(f["Rooms / Keys"])) {
      steward.push({
        id: rec.id,
        name,
        key,
        city,
        reasons: [idBlock.reason, idBlock.note],
      });
      continue;
    }

    const resolved = await resolveWithSeeds(f, { seedsOnly: args.seedsOnly });

    if (!resolved.ok || !resolved.venue) {
      steward.push({
        id: rec.id,
        name,
        key,
        city,
        reasons: [resolved.reason || "cvent_resolve_failed"],
        tried: (resolved.tried || []).slice(0, 5),
      });
      continue;
    }

    const built = buildCventChoicePatch(f, resolved.venue, resolved.sourceUrl, {
      today,
    });

    if (!built.ok && !built.conflict) {
      steward.push({
        id: rec.id,
        name,
        key,
        city,
        reasons: built.reasons || [built.reason],
        sourceUrl: resolved.sourceUrl,
        venue_title: resolved.venue.title,
      });
      continue;
    }

    /** @type {Record<string, unknown>} */
    const patch = { ...(built.patch || {}) };
    const reasons = [...(built.reasons || [])];

    const needMapbox =
      args.withMapbox &&
      ((built.address_written && isStreetLevelAddress(patch.Address)) ||
        (blank(f.Latitude) &&
          isStreetLevelAddress(String(patch.Address || f.Address || ""))));

    if (needMapbox) {
      const addr = String(patch.Address || f.Address || "");
      const mb = await resolveMapboxCoordinates(
        {
          propertyName: name,
          brand,
          address: addr,
          city,
          stateRegion: f["State / Region"],
          country,
        },
        { omitPropertyName: true }
      );
      mapboxLookups += 1;
      if (mb.status === MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) {
        patch.Latitude = mb.latitude;
        patch.Longitude = mb.longitude;
        patch["Coordinate Source Type"] = "official_address_geocode";
        patch["Coordinate Confidence"] = "High";
        patch["Geocode Provider"] = "Mapbox";
        patch["Geocode Method"] = "permanent_geocoding_cvent_address";
        patch["Geocode Reviewed Date"] = today;
        reasons.push(`coords_from_mapbox:${mb.reason}`);
      } else {
        reasons.push(`mapbox_${mb.status}:${mb.reason}`);
      }
    }

    if (built.address_written) addressHits += 1;
    if (built.rooms_written) roomsHits += 1;

    proposals.push({
      id: rec.id,
      property_name: name,
      identity_key: key,
      city,
      country,
      sourceUrl: resolved.sourceUrl,
      venue_title: resolved.venue.title,
      prior: {
        Address: f.Address || null,
        "Rooms / Keys": f["Rooms / Keys"] ?? null,
        Latitude: f.Latitude ?? null,
        Longitude: f.Longitude ?? null,
      },
      patch,
      reasons,
      conflict: Boolean(built.conflict),
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
    hard_rule:
      "Cvent Choice-affiliated Medium Address/Rooms only; Mapbox coords; never overwrite High Rooms silently",
    generated_at: new Date().toISOString(),
    scanned: rows.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    steward_count: steward.length,
    field_hits: { Address: addressHits, "Rooms / Keys": roomsHits },
    mapbox_lookups: mapboxLookups,
    airtable_writes: doWrite,
    proposals,
    steward,
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const outRel = doWrite
    ? "reports/census-choice-cvent-bulk-applied.json"
    : "reports/census-choice-cvent-bulk-dry-run.json";
  writeFileSync(join(root, outRel), JSON.stringify(report, null, 2));

  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: outRel,
        scanned: report.scanned,
        proposal_count: report.proposal_count,
        steward_count: report.steward_count,
        field_hits: report.field_hits,
        mapbox_lookups: report.mapbox_lookups,
        sample: proposals.slice(0, 12).map((p) => ({
          n: p.property_name,
          city: p.city,
          addr: p.patch.Address || null,
          rooms: p.patch["Rooms / Keys"] ?? null,
          lat: p.patch.Latitude ?? null,
          url: p.sourceUrl,
        })),
        steward_sample: steward.slice(0, 15).map((s) => ({
          n: s.name,
          city: s.city,
          reasons: s.reasons,
        })),
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
