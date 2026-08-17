/**
 * Apply HE CALA Brand & Relationships content to:
 *   - Operator Setup - Profile & Positioning (legacy JSON + scalar mirror)
 *   - Operator Setup - Brand Relationships (child table rows)
 *
 *   node scripts/restore-he-cala-brand-relationships.mjs [masterRecordId] [path-to-fixture.json]
 *
 * Default master: recWPKu5laVZxsvpn
 * Default fixture: fixtures/operator-brand-explorer-he-cala.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
  NEW_BASE_PROFILE_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorBrandRelationshipsRows } from "../api/lib/operator-setup-new-base-writer.js";
import { applyBrandRelationshipsToLegacyPrefill } from "../api/lib/operator-brand-relationships-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recWPKu5laVZxsvpn";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-brand-explorer-he-cala.json");

const JSON_PROFILE_KEYS = [
  "brand_portfolio_mix_json",
  "brand_relationship_depth_json",
  "brand_execution_capabilities_json",
  "brand_governance_compliance_json",
];

function parseArgs(argv) {
  const pos = argv.slice(2).filter((a) => !a.startsWith("--"));
  return {
    masterId: (pos[0] || "").trim() || DEFAULT_MASTER,
    fixturePath: (pos[1] || "").trim() || DEFAULT_FIXTURE,
  };
}

function enc(s) {
  return encodeURIComponent(s);
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

async function patchProfileRecord(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_PROFILE_TABLE)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) {
    const msg = json?.error?.message || JSON.stringify(json);
    throw new Error(`Profile PATCH failed (${status}): ${msg}`);
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
  const { masterId, fixturePath } = parseArgs(process.argv);
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

  await patchProfileRecord(profId, profilePatch);
  const brRes = await replaceOperatorBrandRelationshipsRows(
    masterId,
    { brandRelationships },
    randomUUID()
  );

  const publicFixture = path.join(ROOT, "public", "fixtures", "operator-brand-explorer-he-cala.json");
  fs.mkdirSync(path.dirname(publicFixture), { recursive: true });
  fs.copyFileSync(fixturePath, publicFixture);

  console.log(
    JSON.stringify(
      {
        masterId,
        fixturePath,
        publicFixture,
        operatorName: fixture._meta?.operatorName || null,
        profileRecordId: profId,
        profileFieldCount: Object.keys(profilePatch).length,
        brandRowCounts: countBrand(brandRelationships),
        airtable: brRes,
        sampleMix: brandRelationships.portfolioMix?.[0]?.brandFlagType || null,
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
