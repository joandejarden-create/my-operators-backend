#!/usr/bin/env node
/**
 * Golden regression cases from 2026-09-10 forensic defects A–E.
 *
 *   npm run test:adp-forensic-golden-regression-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { extractAndResolveCompetitors } from "../lib/ai-demand-positioning/intelligence/competitor-name-resolution.js";
import { canonicalizeForProperty } from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";
import { filterCustomerFacingEntityNames } from "../lib/ai-demand-positioning/customer/customer-entity-resolution-v1.js";
import { buildEvidenceModalTitle, EVIDENCE_TYPE } from "../lib/ai-demand-positioning/customer/adp-customer-evidence-contract-v1.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");

function main() {
  const ui = readFileSync(UI, "utf8");

  // A — Displaced must never render as Positive Evidence title
  assert.ok(/Displacement Evidence/.test(ui), "A: displacement title setter required");
  assert.equal(
    buildEvidenceModalTitle(EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT, "Wellness"),
    "Displacement Evidence · Wellness"
  );
  assert.notEqual(
    buildEvidenceModalTitle(EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT, "Wellness"),
    "Positive Evidence · Wellness"
  );

  // B — Wellness record must not title Business (title derives from observation territory)
  assert.equal(
    buildEvidenceModalTitle(EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT, "Wellness"),
    "Displacement Evidence · Wellness"
  );

  // C — InterContinental must extract from screenshot raw response
  const profile = loadPropertyProfile("adp_jw_marriott_santo_domingo");
  const period = loadPeriod("adp_period_adp_jw_marriott_santo_domingo_20260908113352_0958fe");
  const obs = period.observations.find((o) => o.observationId === "obs_cb9ec532808d");
  assert.ok(obs);
  const live = extractAndResolveCompetitors(obs.rawResponse, profile);
  assert.ok(
    live.some((n) => canonicalizeForProperty(profile.propertyId, n) === "intercontinental_real_santo_domingo"),
    "C: InterContinental Real Santo Domingo must be extracted + canonicalized"
  );

  // D — phrase fragments must not survive customer chip filter
  const junk = [
    "want ocean views, proximity to the Colonial Zone, and an active hotel",
    "Travelers looking for a relaxing, resort",
    "A massive, resort",
  ];
  const filtered = filterCustomerFacingEntityNames(junk, profile);
  assert.equal(filtered.length, 0, "D: prose fragments must not be customer chips");

  // E — after governed competitor reprocess, screenshot obs MUST include InterContinental
  // and must not silently attribute displacement exclusively to El Embajador without Inter present.
  assert.ok(
    (obs.competitorsMentioned || []).some((n) =>
      /InterContinental Real Santo Domingo/i.test(n)
    ),
    "E: stored observation must include InterContinental after competitor-extract reprocess"
  );
  assert.ok(
    (obs.competitorsMentioned || []).some((n) => /Embajador/i.test(n)),
    "E: El Embajador remains a valid co-observed competitor under multi-competitor displacement law"
  );

  console.log(JSON.stringify({ ok: true, cases: ["A", "B", "C", "D", "E"] }, null, 2));
}

main();
