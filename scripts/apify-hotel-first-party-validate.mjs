#!/usr/bin/env node
/**
 * Inventory / sample / harvest Apify first-party hotel Actors.
 *
 * Default: sample + validate (no Census writes).
 * Production NULL_FILL: --apply with existing write-gate env flags.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { MAP_MASTER } from "../lib/research-engine-v2/master-census-enrichment-v1.js";
import { MAP_BRAND } from "../lib/research-engine-v2/master-brand-portfolio-validation-v1.js";
import { MAP_ROOMS } from "../lib/research-engine-v2/production-census-rooms-keys-queue.js";
import {
  APIFY_HOTEL_ACTOR_CATALOG,
  APIFY_USAGE_STATUS,
  SOURCE_CLASS,
  COMPANIES_WITHOUT_FIRST_PARTY_ACTOR,
  emptyActorMatrixRow,
} from "../lib/research-engine-v2/apify-first-party-extractor-v1.js";
import {
  inventoryAndSampleApifyActors,
  harvestApprovedApifyRows,
  loadPacksFromSampleCache,
  refreshMatrixFromCachedSamples,
  expandApprovedActors,
} from "../lib/research-engine-v2/apify-first-party-acquisition-v1.js";
import {
  applyNullFillToRecords,
  flushAdaptivePatches,
} from "../lib/research-engine-v2/adaptive-overnight-engine-v1.js";
import {
  loadApifyHotelSourceMatrix,
  saveApifyHotelSourceMatrix,
  upsertActorMatrixRow,
} from "../lib/research-engine-v2/apify-hotel-source-matrix-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const REPORT_FP = path.join(
  ROOT,
  "reports/research-engine-v2/apify-first-party-validation.json"
);

const READ_FIELDS = [
  MAP_MASTER.propertyName,
  MAP_MASTER.canonicalName,
  MAP_MASTER.country,
  MAP_MASTER.stateRegion,
  MAP_MASTER.city,
  MAP_MASTER.address,
  MAP_MASTER.postalCode,
  MAP_MASTER.latitude,
  MAP_MASTER.longitude,
  MAP_MASTER.currentBrand,
  MAP_MASTER.brandFamily,
  MAP_MASTER.familySourceFamily,
  MAP_MASTER.officialUrl,
  MAP_MASTER.phone,
  MAP_MASTER.roomsKeys,
  MAP_BRAND.candidateBrand,
  MAP_ROOMS.confidenceExisting,
  MAP_ROOMS.sourceUrlExisting,
];

function argFlag(name) {
  return process.argv.includes(name);
}

async function listCensusRecords(baseId, token, fields) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

function writesEnabled() {
  return (
    argFlag("--apply") &&
    process.env.ALLOW_CENSUS_AUTOPILOT_APPLY === "1" &&
    process.env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS === "1" &&
    process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES === "1" &&
    process.env.CONFIRM_NO_BRAND_SETUP_WRITES === "1"
  );
}

function seedSkippedActors(matrix) {
  for (const actor of APIFY_HOTEL_ACTOR_CATALOG) {
    if (!actor.SKIP_LIVE_SAMPLE && !actor.DEFAULT_STATUS) continue;
    upsertActorMatrixRow(
      matrix,
      emptyActorMatrixRow(actor, {
        OVERALL_STATUS: actor.DEFAULT_STATUS || APIFY_USAGE_STATUS.UNTESTED,
        USAGE_STATUS: actor.DEFAULT_STATUS || APIFY_USAGE_STATUS.UNTESTED,
        NOTES: actor.SKIP_REASON || null,
      })
    );
  }
  for (const company of COMPANIES_WITHOUT_FIRST_PARTY_ACTOR) {
    upsertActorMatrixRow(matrix, {
      ACTOR_ID: `none:${company}`,
      ACTOR_NAME: null,
      HOTEL_COMPANY: company,
      UNDERLYING_SOURCE: null,
      SOURCE_CLASS,
      FIRST_PARTY: false,
      FIELDS_AVAILABLE: [],
      ROOMS_AVAILABLE: false,
      BRAND_AVAILABLE: false,
      ADDRESS_AVAILABLE: false,
      POSTAL_AVAILABLE: false,
      LAT_LONG_AVAILABLE: false,
      PHONE_AVAILABLE: false,
      WEBSITE_AVAILABLE: false,
      PROPERTY_ID_AVAILABLE: false,
      PRICE_MODEL: null,
      SAMPLE_SIZE: 0,
      IDENTITY_ACCURACY: null,
      BRAND_ACCURACY: null,
      ROOM_ACCURACY: null,
      OVERALL_STATUS: APIFY_USAGE_STATUS.UNTESTED,
      USAGE_STATUS: APIFY_USAGE_STATUS.UNTESTED,
      EXPECTED_CALA_YIELD: "no first-party Actor found — continue custom crawl",
      NOTES: "no_store_actor_for_company",
    });
  }
  return matrix;
}

function actorStatus(matrix, company) {
  const rows = (matrix.actors || []).filter((a) => a.HOTEL_COMPANY === company);
  const approved = rows.find((a) => a.USAGE_STATUS === APIFY_USAGE_STATUS.APPROVED);
  if (approved) return approved.USAGE_STATUS;
  const tested = rows.find((a) => Number(a.SAMPLE_SIZE) > 0);
  if (tested) return tested.USAGE_STATUS;
  return rows[0]?.USAGE_STATUS || APIFY_USAGE_STATUS.UNTESTED;
}

async function main() {
  const log = (m) => console.log(m);
  let matrix = loadApifyHotelSourceMatrix();
  matrix = seedSkippedActors(matrix);
  saveApifyHotelSourceMatrix(matrix);

  log("[apify] listing Hotel Property Census for validation matches…");
  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base?.target_base_id || base?.baseId;
  const censusRecords = await listCensusRecords(baseId, token, READ_FIELDS);
  log(`[apify] census n=${censusRecords.length}`);

  const harvestOnly = argFlag("--harvest-only");
  const expand = argFlag("--expand-approved");
  let sampled = { tested: [], matrix, TOTAL_APIFY_COST: 0, matrix_path: null };
  if (!harvestOnly) {
    sampled = await inventoryAndSampleApifyActors({
      censusRecords,
      log,
      onlyCompanies: ["Hilton", "Marriott", "Choice", "IHG"],
    });
    matrix = sampled.matrix;
  }

  const refreshed = refreshMatrixFromCachedSamples(censusRecords);
  matrix = refreshed.matrix;
  let extraCost = 0;
  if (expand) {
    const expanded = await expandApprovedActors({ censusRecords, log });
    extraCost = Number(expanded.extraCost || 0);
    matrix = expanded.matrix;
  }

  const packs = [
    ...(sampled.tested || []).map((t) => ({ actor: t.actor, rows: t.summary.rows })),
    ...loadPacksFromSampleCache(),
  ];
  const harvest = harvestApprovedApifyRows({
    censusRecords,
    matrix,
    packs,
  });
  const applied = applyNullFillToRecords(censusRecords, harvest.proposals);
  const enableWrites = writesEnabled();
  const flush = await flushAdaptivePatches(applied, {
    enableProductionWrites: enableWrites,
    token,
    baseId,
  });

  const discovered = APIFY_HOTEL_ACTOR_CATALOG.length;
  const testedIds = new Set((sampled.tested || []).map((t) => t.actor.ACTOR_ID));
  const approved = (matrix.actors || []).filter(
    (a) => a.USAGE_STATUS === APIFY_USAGE_STATUS.APPROVED
  );
  const rejected = (matrix.actors || []).filter(
    (a) => a.USAGE_STATUS === APIFY_USAGE_STATUS.REJECTED
  );

  const report = {
    ok: true,
    SOURCE_CLASS,
    dry_run: !enableWrites,
    APIFY_ACTORS_DISCOVERED: discovered,
    APIFY_ACTORS_TESTED: testedIds.size,
    APIFY_ACTORS_APPROVED: approved.map((a) => a.ACTOR_ID),
    APIFY_ACTORS_REJECTED: rejected.map((a) => a.ACTOR_ID),
    HILTON_ACTOR_STATUS: actorStatus(matrix, "Hilton"),
    MARRIOTT_ACTOR_STATUS: actorStatus(matrix, "Marriott"),
    CHOICE_ACTOR_STATUS: actorStatus(matrix, "Choice"),
    IHG_ACTOR_STATUS: actorStatus(matrix, "IHG"),
    PROPERTIES_RETURNED: packs.reduce((n, p) => n + (p.rows?.length || 0), 0),
    CALA_PROPERTIES_MATCHED_HIGH: harvest.CALA_PROPERTIES_MATCHED_HIGH,
    CURRENT_BRAND_WRITES: harvest.CURRENT_BRAND_WRITES,
    BRAND_FAMILY_DERIVATIONS: harvest.BRAND_FAMILY_DERIVATIONS,
    ROOMS_WRITES: harvest.ROOMS_WRITES,
    ROOM_CANDIDATES_CORROBORATED: harvest.ROOM_CANDIDATES_CORROBORATED,
    ADDRESS_PATCHES: harvest.ADDRESS_PATCHES,
    POSTAL_PATCHES: harvest.POSTAL_PATCHES,
    STATE_PATCHES: harvest.STATE_PATCHES,
    CITY_PATCHES: harvest.CITY_PATCHES,
    COORDINATE_PATCHES: harvest.COORDINATE_PATCHES,
    PHONE_PATCHES: harvest.PHONE_PATCHES,
    WEBSITE_PATCHES: harvest.WEBSITE_PATCHES,
    TOTAL_PRODUCTION_FIELDS_WRITTEN: enableWrites
      ? applied.reduce((n, p) => n + Object.keys(p.fields).length, 0)
      : 0,
    TOTAL_PRODUCTION_FIELDS_PROPOSED: applied.reduce(
      (n, p) => n + Object.keys(p.fields).length,
      0
    ),
    TOTAL_APIFY_COST:
      Number(harvestOnly ? refreshed.TOTAL_APIFY_COST || 0 : sampled.TOTAL_APIFY_COST || 0) +
      extraCost,
    WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
    COMPANIES_WITHOUT_FIRST_PARTY_ACTOR,
    matrix_path: sampled.matrix_path,
  };

  fs.mkdirSync(path.dirname(REPORT_FP), { recursive: true });
  fs.writeFileSync(REPORT_FP, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
