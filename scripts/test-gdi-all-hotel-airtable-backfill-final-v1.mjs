#!/usr/bin/env node
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  canonicalizeJson,
  fingerprintResearchFields,
  diffMaterialResearchFields,
  buildResearchInventoryFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  buildAllHotelResearchAllowlist,
  validateAllHotelBackfillAuthorization,
  ALL_HOTEL_RESEARCH_BACKFILL_EXACT_WRITE_SET_AUTHORIZATION,
} from "../lib/group-demand-intelligence/workflow/gdi-all-hotel-research-backfill-final-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// JSON canonicalize ignores key order
{
  const a = canonicalizeJson({ b: 1, a: 2 });
  const b = canonicalizeJson({ a: 2, b: 1 });
  assert.deepEqual(a, b);
}

// null vs missing equivalence in material diff
{
  const proposed = { [F.title]: "T", [F.demandThesis]: null };
  const existing = { [F.title]: "T" };
  const d = diffMaterialResearchFields(proposed, existing);
  assert.equal(d.materialChanged, false);
}

// material change detected
{
  const proposed = { [F.title]: "New", [F.hotelId]: "h", [F.opportunityId]: "o", [F.compositeKey]: "h::o" };
  const existing = { [F.title]: "Old", [F.hotelId]: "h", [F.opportunityId]: "o", [F.compositeKey]: "h::o" };
  const d = diffMaterialResearchFields(proposed, existing);
  assert.equal(d.materialChanged, true);
  assert.ok(d.changedFields.includes(F.title));
}

// payload semantic equality despite key order
{
  const p1 = JSON.stringify({ z: 1, a: { y: 2, x: 3 } });
  const p2 = JSON.stringify({ a: { x: 3, y: 2 }, z: 1 });
  const d = diffMaterialResearchFields(
    { [F.researchPayloadJson]: p1 },
    { [F.researchPayloadJson]: p2 },
  );
  assert.equal(d.materialChanged, false);
}

// fingerprints stable
{
  const f1 = fingerprintResearchFields({ [F.title]: "A", [F.priority]: null });
  const f2 = fingerprintResearchFields({ [F.priority]: "", [F.title]: "A" });
  assert.equal(f1, f2);
}

// allowlist counts
{
  const a = buildAllHotelResearchAllowlist(ROOT);
  assert.equal(a.countsMatchExpected, true, JSON.stringify(a.errors));
  assert.equal(a.totalResearch, 659);
  assert.equal(a.remainingCount, 574);
}

assert.equal(
  validateAllHotelBackfillAuthorization({
    authorizeProductionWrites: true,
    exactWriteSetAuthorization: ALL_HOTEL_RESEARCH_BACKFILL_EXACT_WRITE_SET_AUTHORIZATION,
    apply: true,
  }).ok,
  true,
);
assert.equal(validateAllHotelBackfillAuthorization({ apply: true }).ok, false);

{
  const built = buildResearchInventoryFields(
    { id: "opp1", title: "t", organizationName: "o" },
    { hotelId: "recH" },
  );
  assert.equal(built.ok, true);
}

console.log("test-gdi-all-hotel-airtable-backfill-final-v1: PASS");
