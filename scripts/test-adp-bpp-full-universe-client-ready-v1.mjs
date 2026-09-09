#!/usr/bin/env node
/**
 * Permanent gate: ADP BPP full-universe client-ready resolution.
 * npm run test:adp-bpp-full-universe-client-ready-v1
 *
 * BPP_POPULATION_IS_EXPECTED_FOR_AFFILIATED_HOTELS
 * ADP_BPP_POPULATE_BEFORE_SUPPRESS
 * ADP_NO_INTERNAL_CONFIGURATION_LANGUAGE_CUSTOMER_FACING
 */

import assert from "assert";
import { readFileSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { listPublishedPropertyIds } from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import {
  resolveBppPopulationAttempt,
  evaluateBppClientReadinessGate,
  customerFacingBppCopyIsClean,
  FORBIDDEN_CUSTOMER_PHRASES,
  BPP_CLIENT_READY_CLASS,
  ADP_CLIENT_READY_REQUIRES_BPP_POPULATION_ATTEMPT,
  ADP_CLIENT_READY_REQUIRES_MAXIMALLY_RESOLVED_BPP,
  ADP_BPP_POPULATE_BEFORE_SUPPRESS,
  ADP_NO_INTERNAL_CONFIGURATION_LANGUAGE_CUSTOMER_FACING,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-client-readiness-doctrine-v1.js";

const OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-bpp-full-universe-client-ready-v1-latest.json"
);

function main() {
  // Hard ban on exact founder-reported setup copy in customer modules
  assert.ok(!ui.includes("Portfolio monitoring not yet available"));
  assert.ok(!ui.includes("affiliation lens is configured"));
  assert.ok(!ui.includes("Brand & Portfolio insights will appear here once this hotel"));

  const rows = [];
  for (const propertyId of listPublishedPropertyIds()) {
    const profile = loadPropertyProfile(propertyId);
    const bpp = resolveBrandPortfolioPosition(propertyId, profile, {});
    const attempt = resolveBppPopulationAttempt({ propertyId, profile, bppPayload: bpp });
    const gate = evaluateBppClientReadinessGate(attempt);
    assert.equal(gate.pass, true, `${propertyId} must be BPP client-ready`);
    assert.ok(
      attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED ||
        attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_EXCEPTION_SUPPRESSED,
      `${propertyId} invalid class ${attempt.clientReadyClass}`
    );
    assert.ok(customerFacingBppCopyIsClean(JSON.stringify(bpp)), `${propertyId} dirty customer copy`);
    rows.push({
      propertyId,
      class: attempt.clientReadyClass,
      status: bpp.status,
      pass: gate.pass,
    });
  }

  const report = {
    title: "ADP_BPP_FULL_UNIVERSE_CLIENT_READY_V1",
    ok: true,
    gates: [
      ADP_CLIENT_READY_REQUIRES_BPP_POPULATION_ATTEMPT,
      ADP_CLIENT_READY_REQUIRES_MAXIMALLY_RESOLVED_BPP,
      ADP_BPP_POPULATE_BEFORE_SUPPRESS,
      ADP_NO_INTERNAL_CONFIGURATION_LANGUAGE_CUSTOMER_FACING,
    ],
    counts: {
      published: rows.length,
      ready: rows.filter((r) => r.class === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED).length,
      suppressed: rows.filter((r) => r.class === BPP_CLIENT_READY_CLASS.BPP_EXCEPTION_SUPPRESSED).length,
      invalid: 0,
    },
    rows,
    methodologyChanged: false,
  };
  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, outPath: OUT, ...report.counts }, null, 2));
}

main();
