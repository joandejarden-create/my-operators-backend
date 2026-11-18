#!/usr/bin/env node
/**
 * Ensure operational provenance stamps inside researchPayloadJson are non-material.
 */
import assert from "node:assert/strict";
import {
  PAYLOAD_VOLATILE_KEYS,
  diffMaterialResearchFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

assert.ok(PAYLOAD_VOLATILE_KEYS.includes("firstDiscoveredRunId"));
assert.ok(PAYLOAD_VOLATILE_KEYS.includes("firstDiscoveredAt"));
assert.ok(PAYLOAD_VOLATILE_KEYS.includes("lastResearchedAt"));
assert.ok(PAYLOAD_VOLATILE_KEYS.includes("runId"));

const existing = {
  [F.opportunityId]: "demo",
  [F.hotelId]: "h1",
  [F.compositeKey]: "h1::demo",
  [F.title]: "Stable title",
  [F.researchPayloadJson]: JSON.stringify({
    opportunityId: "demo",
    title: "Stable title",
    sources: [{ url: "https://example.com/evidence" }],
  }),
};

const proposed = {
  ...existing,
  [F.researchPayloadJson]: JSON.stringify({
    opportunityId: "demo",
    title: "Stable title",
    sources: [{ url: "https://example.com/evidence" }],
    firstDiscoveredRunId: "injected_probe_run",
    firstDiscoveredAt: "2026-11-18T00:00:00.000Z",
    lastResearchedAt: "2026-11-18T12:00:00.000Z",
    runId: "injected_probe_run",
  }),
};

const diff = diffMaterialResearchFields(proposed, existing);
assert.equal(diff.materialChanged, false);
assert.deepEqual(diff.changedFields, []);

const material = {
  ...proposed,
  [F.researchPayloadJson]: JSON.stringify({
    opportunityId: "demo",
    title: "Stable title",
    sources: [
      { url: "https://example.com/evidence" },
      { url: "https://example.com/new-evidence" },
    ],
    firstDiscoveredRunId: "injected_probe_run",
  }),
};
const materialDiff = diffMaterialResearchFields(material, existing);
assert.equal(materialDiff.materialChanged, true);
assert.ok(materialDiff.changedFields.includes("researchPayloadJson"));

console.log(
  JSON.stringify(
    {
      ok: true,
      volatileKeys: [...PAYLOAD_VOLATILE_KEYS],
      provenanceStampNonMaterial: true,
      realSourceChangeMaterial: true,
    },
    null,
    2,
  ),
);
