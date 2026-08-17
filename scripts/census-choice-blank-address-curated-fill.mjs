#!/usr/bin/env node
/**
 * Curated Medium Address fill for Choice blank hard-cases where OSM/Places
 * failed brand alignment but multi-source street evidence exists.
 * Never invents; never copies a different Choice property ID's address.
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
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

/** Curated Medium fills — multi-source, Choice-code-linked where possible. */
const CURATED = Object.freeze({
  ind_choice_mx_mx092: {
    address: "Blvd. Villas de Irapuato 1502, 36643 Irapuato, Guanajuato, Mexico",
    sourceUrl:
      "https://www.choicehotels.com/en-ca/guanajuato/irapuato/top-rated-hotels",
    evidence: [
      "choice_listing_snippet_mx092_villas_1502",
      "hotel_directory_corroboration",
      "mx_business_registry_confort_inn_building_36643",
    ],
  },
  ind_choice_mx_mx073: {
    address:
      "Blvd. Manuel Ávila Camacho 2221, Ignacio Zaragoza, 91910 Veracruz, Veracruz, Mexico",
    sourceUrl: "https://directoriodehoteles.com.mx/listado.php?id=3425",
    evidence: [
      "hotel_directory_mx073_avila_camacho_2221",
      "mxfirmas_choice_url_mx073",
      "vivehotels_comfort_inn_veracruz_location",
    ],
  },
});

const STEWARD_ONLY = Object.freeze({
  ind_choice_mx_mx086: {
    reasons: [
      "not_in_live_choice_cala_regional",
      "live_queretaro_comfort_inn_is_mx226_already_addressed",
      "do_not_copy_mx226_address_onto_mx086_without_identity_retire_or_merge",
    ],
  },
  ind_choice_mx_mx104: {
    reasons: [
      "not_in_live_choice_cala_regional",
      "no_ascend_zacatecas_street_evidence",
      "likely_soft_brand_placeholder_or_pipeline",
    ],
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
    withMapbox: !argv.includes("--skip-mapbox"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listBlankTargets(baseId, token) {
  const keys = [...Object.keys(CURATED), ...Object.keys(STEWARD_ONLY)];
  const or = keys.map((k) => `{Property Identity Key}='${k}'`).join(",");
  const formula = `AND({Brand Family}='Choice Hotels International',OR(${or}))`;
  const p = new URLSearchParams({ pageSize: "20", filterByFormula: formula });
  for (const f of [
    "Property Name",
    "Current Brand",
    "City",
    "Country",
    "State / Region",
    "Address",
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
  const rows = await listBlankTargets(baseId, token);
  const proposals = [];
  const steward = [];
  let mapboxLookups = 0;

  for (const rec of rows) {
    const f = rec.fields || {};
    const key = String(f["Property Identity Key"] || "").trim();
    const name = String(f["Property Name"] || "").trim();
    const city = String(f.City || "").trim();
    const country = String(f.Country || "Mexico").trim();
    const brand = String(f["Current Brand"] || "").trim();

    if (!blank(f.Address)) {
      steward.push({
        id: rec.id,
        name,
        key,
        reasons: ["address_already_present"],
      });
      continue;
    }

    if (STEWARD_ONLY[key]) {
      steward.push({ id: rec.id, name, key, city, ...STEWARD_ONLY[key] });
      continue;
    }

    const curated = CURATED[key];
    if (!curated) {
      steward.push({
        id: rec.id,
        name,
        key,
        reasons: ["no_curated_fill"],
      });
      continue;
    }

    if (!isStreetLevelAddress(curated.address)) {
      steward.push({
        id: rec.id,
        name,
        key,
        reasons: ["curated_address_not_street_level", curated.address],
      });
      continue;
    }

    /** @type {Record<string, unknown>} */
    const patch = {
      Address: curated.address,
      "Address Confidence": "Medium",
      "Address Source URL": curated.sourceUrl,
      "Last Reviewed Date": todayIsoDate(),
    };
    const reasons = [
      "address_from_curated_multi_source_medium",
      ...curated.evidence,
    ];

    if (args.withMapbox) {
      const mb = await resolveMapboxCoordinates(
        {
          propertyName: name,
          brand,
          address: curated.address,
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
        patch["Geocode Method"] = "permanent_geocoding_curated_address";
        patch["Geocode Reviewed Date"] = todayIsoDate();
        reasons.push(`coords_from_mapbox:${mb.reason}`);
      } else {
        reasons.push(`mapbox_${mb.status}:${mb.reason}`);
      }
    }

    proposals.push({
      id: rec.id,
      property_name: name,
      identity_key: key,
      city,
      country,
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
    hard_rule:
      "Curated Medium only; no mx226→mx086 copy; Mapbox coords only; never invent",
    generated_at: new Date().toISOString(),
    scanned: rows.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    steward_count: steward.length,
    mapbox_lookups: mapboxLookups,
    airtable_writes: doWrite,
    proposals,
    steward,
  };
  mkdirSync(join(root, "reports"), { recursive: true });
  const outRel = doWrite
    ? "reports/census-choice-blank-address-curated-applied.json"
    : "reports/census-choice-blank-address-curated-dry-run.json";
  writeFileSync(join(root, outRel), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: outRel,
        proposal_count: report.proposal_count,
        steward_count: report.steward_count,
        sample: proposals.map((p) => ({
          n: p.property_name,
          addr: p.patch.Address,
          lat: p.patch.Latitude,
        })),
        steward: steward.map((s) => ({ n: s.name, reasons: s.reasons })),
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
