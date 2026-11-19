#!/usr/bin/env node
/**
 * Gate: ADP methodology fingerprint + portfolio compatibility audit.
 * Fails if legitimate hotels lack a known compatibility status, or if
 * CURRENT_COMPARABLE hotels drift from the canonical fingerprint stamps.
 */
import assert from "node:assert/strict";
import {
  ADP_METHODOLOGY_CANONICAL_V1,
  ADP_METHODOLOGY_COMPATIBILITY,
  ADP_METHODOLOGY_FINGERPRINT_CURRENT,
  listCanonicalMethodologyKeys,
} from "../lib/ai-demand-positioning/methodology/adp-methodology-canonical-v1.js";
import { buildAdpMethodologyCompatibilityAudit } from "../lib/ai-demand-positioning/methodology/adp-methodology-compatibility-audit-v1.js";
import { planAdpMethodologyStagedNormalize } from "../lib/ai-demand-positioning/methodology/adp-methodology-staged-normalize-v1.js";
import { extractMethodologyProvenanceFromPayload } from "../lib/ai-demand-positioning/methodology/adp-methodology-provenance-v1.js";

assert.equal(ADP_METHODOLOGY_CANONICAL_V1, "ADP_METHODOLOGY_CANONICAL_V1");
assert.ok(listCanonicalMethodologyKeys().includes("appearanceMethodologyId"));
assert.equal(
  ADP_METHODOLOGY_FINGERPRINT_CURRENT.appearanceMethodologyId,
  "ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1"
);
assert.equal(
  ADP_METHODOLOGY_FINGERPRINT_CURRENT.realityGapMethodologyId,
  "RG_WEIGHTED_PARTIAL_V2"
);

const empty = extractMethodologyProvenanceFromPayload(null);
assert.equal(empty.appearanceMethodologyId, null);

const audit = buildAdpMethodologyCompatibilityAudit({});
assert.equal(audit.inventoryCount, 39, "expected 39 inventory rows");
assert.equal(audit.legitimateCount, 38, "expected 38 legitimate hotels");
assert.equal(audit.blockedSyntheticCount, 1);

for (const row of audit.rows) {
  assert.ok(
    Object.values(ADP_METHODOLOGY_COMPATIBILITY).includes(row.compatibility),
    `unknown compatibility for ${row.propertyId}: ${row.compatibility}`
  );
}

const drift = audit.rows.filter(
  (r) =>
    r.compatibility === ADP_METHODOLOGY_COMPATIBILITY.CURRENT_COMPARABLE &&
    r.provenance?.appearanceMethodologyId !==
      ADP_METHODOLOGY_FINGERPRINT_CURRENT.appearanceMethodologyId
);
assert.equal(
  drift.length,
  0,
  `CURRENT_COMPARABLE appearance drift: ${drift.map((d) => d.propertyId).join(",")}`
);

const plan = planAdpMethodologyStagedNormalize({ apply: false });
assert.equal(plan.recomputableCount, audit.recomputableCount);
assert.ok(Array.isArray(plan.stagedPlans));
assert.ok(
  plan.notes.some((n) => /STAGED|durable CURRENT/i.test(n)),
  "expected staged-only / no-overwrite guidance"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-methodology-consistency-v1",
      inventoryCount: audit.inventoryCount,
      byCompatibility: audit.byCompatibility,
      recomputableCount: plan.recomputableCount,
    },
    null,
    2
  )
);
