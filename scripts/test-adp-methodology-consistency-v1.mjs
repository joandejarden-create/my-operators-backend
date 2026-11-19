#!/usr/bin/env node
/**
 * Gate: ADP methodology fingerprint + portfolio compatibility audit.
 * Fails if legitimate hotels lack a known compatibility status, or if
 * CURRENT_COMPARABLE hotels drift from the canonical fingerprint stamps.
 * Live inventory excludes synthetics permanently.
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
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import { listPublishedPropertyIds } from "../lib/ai-demand-positioning/published-snapshot.js";
import { isAdpSyntheticOrTestPropertyId } from "../lib/ai-demand-positioning/governance/adp-operational-registry-hygiene-v1.js";

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
assert.ok(
  !Object.prototype.hasOwnProperty.call(
    ADP_METHODOLOGY_COMPATIBILITY,
    "BLOCKED_SYNTHETIC"
  ),
  "BLOCKED_SYNTHETIC must not remain a live compatibility class"
);

const empty = extractMethodologyProvenanceFromPayload(null);
assert.equal(empty.appearanceMethodologyId, null);

const published = listPublishedPropertyIds();
assert.equal(published.length, 38, "expected 38 live published hotels");
assert.ok(
  !published.some((id) => isAdpSyntheticOrTestPropertyId(id)),
  "synthetic must not appear in listPublishedPropertyIds"
);
assert.ok(
  !published.includes("adp_smoke_test_synthetic"),
  "adp_smoke_test_synthetic must be excluded from live inventory"
);

const coverage = buildPublishedAdpReviewCoverageV1({});
assert.equal(coverage.universeCount, 38);
assert.equal(coverage.reviewReadyCount, 38);
assert.equal(coverage.missingCount, 0);
assert.equal(coverage.blockedCount, 0);
assert.ok(
  !(coverage.properties || []).some(
    (p) => p.propertyId === "adp_smoke_test_synthetic"
  )
);

const audit = buildAdpMethodologyCompatibilityAudit({});
assert.equal(audit.inventoryCount, 38, "expected 38 inventory rows");
assert.equal(audit.legitimateCount, 38, "expected 38 legitimate hotels");
assert.equal(audit.comparableCount, 38);
assert.equal(audit.recomputableCount, 0);
assert.ok(!("BLOCKED_SYNTHETIC" in (audit.byCompatibility || {})));

for (const row of audit.rows) {
  assert.ok(
    Object.values(ADP_METHODOLOGY_COMPATIBILITY).includes(row.compatibility),
    `unknown compatibility for ${row.propertyId}: ${row.compatibility}`
  );
  assert.equal(row.compatibility, ADP_METHODOLOGY_COMPATIBILITY.CURRENT_COMPARABLE);
}

const drift = audit.rows.filter(
  (r) =>
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
      coverage: {
        universe: coverage.universeCount,
        ready: coverage.reviewReadyCount,
        missing: coverage.missingCount,
        blocked: coverage.blockedCount,
      },
      byCompatibility: audit.byCompatibility,
      recomputableCount: plan.recomputableCount,
    },
    null,
    2
  )
);
