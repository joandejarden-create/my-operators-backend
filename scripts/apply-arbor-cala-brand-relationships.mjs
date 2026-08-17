#!/usr/bin/env node
/**
 * Apply Arbor Lodging (CALA) Brand & Relationships content to:
 *   - Operator Setup - Profile & Positioning (legacy JSON + scalar mirror)
 *   - Operator Setup - Brand Relationships (child table rows)
 *   - Operator Setup - Platform & Markets (geo_cala_* footprint scalars)
 *
 *   node scripts/apply-arbor-cala-brand-relationships.mjs
 *   node scripts/apply-arbor-cala-brand-relationships.mjs --dry-run
 *   node scripts/apply-arbor-cala-brand-relationships.mjs --fixture=fixtures/operator-brand-explorer-arbor-cala.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
  NEW_BASE_PROFILE_TABLE,
  NEW_BASE_PLATFORM_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorBrandRelationshipsRows } from "../api/lib/operator-setup-new-base-writer.js";
import { applyBrandRelationshipsToLegacyPrefill } from "../api/lib/operator-brand-relationships-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recF5Z87OAqFgndoq";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-brand-explorer-arbor-cala.json");

const JSON_PROFILE_KEYS = [
  "brand_portfolio_mix_json",
  "brand_relationship_depth_json",
  "brand_execution_capabilities_json",
  "brand_governance_compliance_json",
];

const PLATFORM_GEO_KEYS = [
  "geo_cala_existing_hotels",
  "geo_cala_existing_rooms",
  "geo_cala_pipeline_hotels",
  "geo_cala_pipeline_rooms",
  "geo_cala_total_hotels",
  "geo_cala_total_rooms",
];

function parseArgs(argv) {
  const args = argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const fixtureArg = args.find((a) => a.startsWith("--fixture="));
  const masterArg = args.find((a) => a.startsWith("--master="));
  const pos = args.filter((a) => !a.startsWith("--"));
  return {
    dryRun,
    masterId: masterArg ? masterArg.split("=")[1] : pos[0] || DEFAULT_MASTER,
    fixturePath: fixtureArg ? fixtureArg.split("=")[1] : pos[1] || DEFAULT_FIXTURE,
  };
}

function enc(s) {
  return encodeURIComponent(s);
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function buildProfilePatch(profileRaw, brandRelationships) {
  const patch = { ...(profileRaw || {}) };
  const prefill = {};
  applyBrandRelationshipsToLegacyPrefill(prefill, brandRelationships);
  for (const key of JSON_PROFILE_KEYS) {
    if (prefill[key] != null && prefill[key] !== "") patch[key] = prefill[key];
  }
  for (const spec of [
    "brand_soft_independent_narrative",
    "brand_narrative_compliance",
    "brand_narrative_relationship",
    "brand_signal_audit",
    "brand_signal_reflag",
    "brand_signal_franchise_align",
    "brand_signal_soft_retention",
    "numberOfBrands",
    "brandedVsIndependentMix",
    "brand_conversion_project_count",
  ]) {
    if (prefill[spec] != null && prefill[spec] !== "") patch[spec] = prefill[spec];
  }
  return patch;
}

function buildPlatformPatch(platformRaw) {
  const patch = {};
  for (const key of PLATFORM_GEO_KEYS) {
    if (platformRaw[key] == null || platformRaw[key] === "") continue;
    patch[key] = platformRaw[key];
  }
  return patch;
}

async function patchRecord(table, recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(table)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) {
    const msg = json?.error?.message || JSON.stringify(json);
    throw new Error(`${table} PATCH failed (${status}): ${msg}`);
  }
  return json;
}

function countBrand(br) {
  return {
    portfolioMix: (br.portfolioMix || []).length,
    relationshipDepth: (br.relationshipDepth || []).length,
    executionCapabilities: (br.executionCapabilities || []).length,
    governanceCompliance: (br.governanceCompliance || []).length,
    brandSignals: (br.brandSignals || []).length,
    snapshotMetrics: (br.snapshotMetrics || []).length,
    hasSoftNarrative: Boolean(br.softIndependentNarrative),
    narrativeKeys: Object.keys(br.narratives || {}).length,
  };
}

async function main() {
  const { dryRun, masterId, fixturePath } = parseArgs(process.argv);
  if (!fs.existsSync(fixturePath)) {
    throw new Error(`Missing fixture: ${fixturePath}`);
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const brandRelationships = fixture.brandRelationships;
  if (!brandRelationships || typeof brandRelationships !== "object") {
    throw new Error("Fixture missing brandRelationships object");
  }

  const profRows = await fetchRecordsLinkedToMaster(NEW_BASE_PROFILE_TABLE, masterId);
  if (!profRows.length) {
    throw new Error(`No Profile row linked to Master ${masterId}`);
  }
  const profId = profRows[0].id;
  const profilePatch = {
    ...buildProfilePatch(fixture.profileFields || {}, brandRelationships),
    ...Object.fromEntries(
      Object.entries(fixture.profileFields || {}).filter(([, v]) => v != null && v !== "")
    ),
  };
  if (!Object.keys(profilePatch).length) {
    throw new Error("Fixture has no profile fields to patch");
  }

  const platRows = await fetchRecordsLinkedToMaster(NEW_BASE_PLATFORM_TABLE, masterId);
  const platId = platRows[0]?.id || null;
  const platformPatch = buildPlatformPatch(fixture.platformFields || {});

  const summary = {
    dryRun,
    masterId,
    fixturePath,
    operatorName: fixture._meta?.operatorName || null,
    profileRecordId: profId,
    profileFieldCount: Object.keys(profilePatch).length,
    platformRecordId: platId,
    platformFieldCount: Object.keys(platformPatch).length,
    brandRowCounts: countBrand(brandRelationships),
    sampleMix: brandRelationships.portfolioMix?.[0]?.portfolioMix || null,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (dryRun) {
    console.log("[arbor-brand] Dry run — no Airtable writes.");
    return;
  }

  await patchRecord(NEW_BASE_PROFILE_TABLE, profId, profilePatch);

  let brRes = null;
  if (platId && Object.keys(platformPatch).length) {
    await patchRecord(NEW_BASE_PLATFORM_TABLE, platId, platformPatch);
  } else if (Object.keys(platformPatch).length && !platId) {
    console.warn("[arbor-brand] Platform row missing — geo_cala_* not written.");
  }

  brRes = await replaceOperatorBrandRelationshipsRows(
    masterId,
    { brandRelationships },
    randomUUID()
  );

  const publicFixture = path.join(ROOT, "public", "fixtures", "operator-brand-explorer-arbor-cala.json");
  fs.mkdirSync(path.dirname(publicFixture), { recursive: true });
  fs.copyFileSync(fixturePath, publicFixture);

  console.log(
    JSON.stringify(
      {
        publicFixture,
        airtable: brRes,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
