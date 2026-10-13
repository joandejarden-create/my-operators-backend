/**
 * Tests: Airtable canonicalization uniqueness + wrong-base guard + naming alias.
 *   node scripts/test-airtable-canonicalization-v1.mjs
 */
import "../load-env.js";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId,
  isLegacyMvpBase,
} from "../lib/decision-outcomes/airtable-base.js";
import { GDI_OPPORTUNITIES_TABLE_NAME } from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { HOTEL_ADP_ATTRIBUTES_TABLE, buildAttributeDedupeKey } from "../lib/hotel-intelligence/adp-attributes/field-map.js";
import { HI_TABLES } from "../lib/hotel-intelligence/schema/hotel-intelligence-schema-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const AUDIT = path.join(ROOT, "reports", "airtable-canonicalization-v1", "AUDIT.json");

assert.equal(HOTEL_ADP_ATTRIBUTES_TABLE, "Hotel ADP Attributes");
assert.equal(HI_TABLES.adpAttributes, "Hotel ADP Attributes");
assert.equal(GDI_OPPORTUNITIES_TABLE_NAME, "Group Demand Opportunities");

const key = buildAttributeDedupeKey("recTEST", "Rooms / Keys", "adp_attr_v1", "Commercial");
assert.match(key, /^recTEST::commercial::rooms_\/_keys::adp_attr_v1$/);

assert.equal(isLegacyMvpBase(LEGACY_DEAL_CAPTURE_MVP_BASE_ID), true);
assert.equal(isLegacyMvpBase(CANONICAL_INTELLIGENCE_BASE_ID), false);

assert.throws(
  () => assertNotLegacyMvpCanonicalBase(LEGACY_DEAL_CAPTURE_MVP_BASE_ID, { surface: "test" }),
  /WRONG_CANONICAL_AIRTABLE_BASE/
);
assert.doesNotThrow(() =>
  assertNotLegacyMvpCanonicalBase(CANONICAL_INTELLIGENCE_BASE_ID, { surface: "test" })
);

const gdiBase = getGdiOpportunitiesAirtableBaseId();
if (gdiBase) {
  assert.equal(gdiBase, CANONICAL_INTELLIGENCE_BASE_ID);
  assert.notEqual(gdiBase, LEGACY_DEAL_CAPTURE_MVP_BASE_ID);
}

assert.ok(fs.existsSync(AUDIT), "run audit-airtable-canonicalization-v1.mjs first");
const audit = JSON.parse(fs.readFileSync(AUDIT, "utf8"));
assert.equal(audit.bases.platform, CANONICAL_INTELLIGENCE_BASE_ID);
assert.equal(audit.attributeTables.length, 1);
assert.equal(audit.attributeTables[0].id, "tblMA6v0HAmsY9ImW");
assert.ok(audit.expectedHiIdCheck.every((x) => x.match), "HI table IDs must match");

for (const hotel of ["Bethesda Marriott", "AC Hotel A Coruña", "Spice Island Beach Resort"]) {
  const a = audit.attrAudit[hotel];
  assert.ok(a, hotel);
  assert.equal(a.multipleActiveSameKey, 0, `${hotel} multi-active`);
  assert.equal(a.clean, true, `${hotel} attr clean`);
  const g = audit.gdiAudit[hotel];
  assert.equal(g.fitsReported, g.uniqueFitIds, `${hotel} fit uniqueness`);
  assert.equal(g.targetsReported, g.uniqueTargetIds, `${hotel} target uniqueness`);
  assert.equal(g.clean, true, `${hotel} gdi clean`);
}

// AC/Spice exact counts from prior onboarding
assert.equal(audit.gdiAudit["AC Hotel A Coruña"].fitsReported, 7);
assert.equal(audit.gdiAudit["AC Hotel A Coruña"].targetsReported, 14);
assert.equal(audit.gdiAudit["Spice Island Beach Resort"].fitsReported, 7);
assert.equal(audit.gdiAudit["Spice Island Beach Resort"].targetsReported, 14);

// Group Demand Opportunities ID must exist even if founder label differs
const opp = (audit.allPlatformTables || []).find((t) => t.id === "tblRuReslJMwsfRQj");
assert.ok(opp, "opportunities table id present");
assert.equal(opp.name, "Group Demand Opportunities");

console.log(
  JSON.stringify(
    {
      ok: true,
      attributeTables: 1,
      gdiOpportunitiesAirtableName: GDI_OPPORTUNITIES_TABLE_NAME,
      hotelsClean: true,
    },
    null,
    2
  )
);
