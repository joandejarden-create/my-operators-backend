#!/usr/bin/env node
/**
 * Ensure CALA Radar Build Plans Airtable table + fields.
 *   node scripts/ensure-radar-build-plans-schema.mjs --dry-run
 *   node scripts/ensure-radar-build-plans-schema.mjs --apply
 */
import "../load-env.js";
import {
  RADAR_BUILD_PLANS_TABLE,
  RADAR_BUILD_PLANS_FIELDS as F,
  PRIORITY_TIER_OPTIONS,
  BUILD_STRATEGY_OPTIONS,
  BUILD_STATUS_OPTIONS,
  PRIMARY_HOTEL_DEMAND_PROFILE_OPTIONS,
} from "../lib/radar-buildout/airtable-radar-build-plans-fields.js";
import { getRadarBuildPlansBaseId } from "../lib/radar-buildout/radar-build-plans-base.js";

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}

function singleSelect(name, optionNames) {
  return { name, type: "singleSelect", options: { choices: choices(optionNames) } };
}

function numberField(name, precision = 0) {
  return { name, type: "number", options: { precision } };
}

function dateField(name) {
  return { name, type: "date", options: { dateFormat: { name: "iso" } } };
}

function hasField(table, name) {
  return (table?.fields || []).some((f) => f.name === name);
}

async function createField(baseId, token, tableId, spec) {
  if (DRY) {
    console.log(`[dry-run] would create field ${spec.name}`);
    return { ok: true };
  }
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify({
      name: spec.name,
      type: spec.type,
      ...(spec.options ? { options: spec.options } : {}),
    }),
  });
  if (!res.ok) return { ok: false, status: res.status, json };
  return { ok: true, json };
}

async function createTable(baseId, token, body) {
  if (DRY) {
    console.log(`[dry-run] would create table ${body.name}`);
    return { ok: true, json: { id: "dry_run", name: body.name } };
  }
  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) return { ok: false, status: res.status, json };
  return { ok: true, json };
}

function fieldSpecs() {
  return [
    singleSelect(F.buildStrategy, BUILD_STRATEGY_OPTIONS),
    singleSelect(F.priorityTier, PRIORITY_TIER_OPTIONS),
    singleSelect(F.buildStatus, BUILD_STATUS_OPTIONS),
    numberField(F.targetDemandAnchors),
    numberField(F.currentDemandAnchors),
    numberField(F.targetTravelInfrastructure),
    numberField(F.currentTravelInfrastructure),
    numberField(F.targetTotalRadarPoints),
    numberField(F.currentTotalRadarPoints),
    { name: F.submarketsCorridors, type: "multilineText" },
    singleSelect(F.primaryHotelDemandProfile, PRIMARY_HOTEL_DEMAND_PROFILE_OPTIONS),
    numberField(F.sourceCoveragePct, 0),
    numberField(F.coordinateCoveragePct, 0),
    { name: F.dataConfidenceMix, type: "multilineText" },
    dateField(F.lastBuildDate),
    dateField(F.lastQaDate),
    { name: F.nextRecommendedAction, type: "multilineText" },
    { name: F.notes, type: "multilineText" },
    numberField(F.recommendedBuildSequence),
    { name: F.nextBuildMarket, type: "singleLineText" },
    { name: F.buildApproachNotes, type: "multilineText" },
    { name: F.firstPassTargetDescription, type: "multilineText" },
  ];
}

function initialTableFields() {
  return [
    { name: F.country, type: "singleLineText" },
    { name: F.region, type: "singleLineText" },
    ...fieldSpecs(),
  ];
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = getRadarBuildPlansBaseId();
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed ${res.status}`);

  let table = (json.tables || []).find((t) => t.name === RADAR_BUILD_PLANS_TABLE);
  if (!table) {
    console.log(`Create table: ${RADAR_BUILD_PLANS_TABLE}`);
    const cr = await createTable(baseId, token, {
      name: RADAR_BUILD_PLANS_TABLE,
      description: "CALA Radar Buildout status and targets by country.",
      fields: initialTableFields(),
    });
    if (!cr.ok) throw new Error(`Create table failed: ${JSON.stringify(cr.json)}`);
    console.log("  created table");
    return;
  }

  console.log(`\n${table.name} — ensure missing fields`);
  for (const spec of initialTableFields()) {
    if (hasField(table, spec.name)) {
      console.log(`  skip (exists): ${spec.name}`);
      continue;
    }
    const r = await createField(baseId, token, table.id, spec);
    if (!r.ok) console.error(`  FAIL ${spec.name}`, r.status, JSON.stringify(r.json));
    else console.log(`  created: ${spec.name}`);
    await new Promise((r) => setTimeout(r, 220));
  }
  console.log("\nDone.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
