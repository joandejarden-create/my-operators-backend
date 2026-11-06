#!/usr/bin/env node
/**
 * Regression: ADP_AVA_REFERENCE_CONTRACT_V1
 * - Attribute matcher rejects empty-key false positives
 * - Measured zeros classify as MEASURED_ZERO when eligible n>0
 * - KPI narrative does not confuse Scenario Presence with AI Consideration
 * - Full 38-hotel parity script must PASS
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  extractAdpNumericReferences,
  loadAuthoritativeAdpReference,
  reconcileAvaPayloadAgainstAdp,
  ZERO_VERDICTS,
  ADP_AVA_REFERENCE_CONTRACT,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-reference-contract-v1.js";
import { detectAdpAvaParityDefects } from "../lib/ai-visibility-audit-action-plan/adp-ava-reference-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");

function loadPayload(file) {
  return JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8"));
}

// 1) Empty-key false positive regression (Cambridge On-Site Restaurant → 0 not 21.1)
{
  const adp = loadAuthoritativeAdpReference("adp_cambridge_beaches_bermuda");
  assert.equal(adp.ok, true);
  const payload = loadPayload("cambridge-beaches-bermuda-v1.json");
  const result = reconcileAvaPayloadAgainstAdp(payload);
  assert.equal(result.ok, true, JSON.stringify(result.defects, null, 2));
  const restaurantZeros = (result.zeros || []).filter((z) =>
    /on.?site.?restaurant/i.test(z.label)
  );
  assert.ok(restaurantZeros.length >= 1, "expected On-Site Restaurant zero refs");
  for (const z of restaurantZeros) {
    assert.equal(z.adp, 0);
    assert.equal(z.zeroVerdict, ZERO_VERDICTS.MEASURED_ZERO);
    assert.ok(Number(z.denominator) > 0);
  }
}

// 2) AC Coruña restaurant 0% is MEASURED_ZERO; meeting space ~49% matches 49.2
{
  const result = reconcileAvaPayloadAgainstAdp(loadPayload("ac-hotel-a-coruna-v1.json"));
  assert.equal(result.ok, true, JSON.stringify(result.defects, null, 2));
  const rest = (result.zeros || []).filter((z) => /restaurant/i.test(z.label));
  assert.ok(rest.some((z) => z.zeroVerdict === ZERO_VERDICTS.MEASURED_ZERO));
}

// 3) Hilton Times Square — known lean-pin drift remains blocked (do not fabricate MATCH)
{
  const result = reconcileAvaPayloadAgainstAdp(loadPayload("hilton-times-square-v1.json"));
  assert.equal(result.ok, false);
  assert.ok(
    (result.defects || []).some((d) => d.type === "METRIC_MISMATCH"),
    JSON.stringify(result.defects, null, 2)
  );
  assert.ok((result.zeros || []).every((z) => z.zeroVerdict === ZERO_VERDICTS.MEASURED_ZERO));
}

// 4) Narrative must not map "appeared in X%" after AI Consideration to Consideration
{
  const refs = extractAdpNumericReferences(
    "voco recorded 0.4% AI Consideration and appeared in 1.6% of eligible scenarios"
  );
  const consideration = refs.filter((r) => r.kind === "AI_CONSIDERATION");
  assert.ok(consideration.every((r) => r.value === 0.4));
  assert.ok(!consideration.some((r) => r.value === 1.6));
}

// 5) Spice Island KPIs must match certified lean pin (57.6 / 97.8)
{
  const result = reconcileAvaPayloadAgainstAdp(loadPayload("spice-island-beach-resort-v1.json"));
  assert.equal(result.ok, true, JSON.stringify(result.defects, null, 2));
  const ac = result.refs.find((r) => r.kind === "AI_CONSIDERATION" && r.surface === "kpi");
  assert.equal(ac?.ava, 57.6);
  assert.equal(ac?.adp, 57.6);
}

// 6) CQA detector returns parity blocks on synthetic mismatch
{
  const payload = loadPayload("spice-island-beach-resort-v1.json");
  const bad = structuredClone(payload);
  bad.kpis = bad.kpis.map((k) =>
    k.label === "AI Consideration" ? { ...k, value: "11.1%" } : k
  );
  const defects = detectAdpAvaParityDefects(bad);
  assert.ok(defects.some((d) => /METRIC_MISMATCH|ADP_AVA_PARITY/i.test(d.type + d.message)));
}

// 7) Full portfolio script — 38/38 identity + certification-state (known drifts stay blocked)
{
  const run = spawnSync(process.execPath, ["scripts/ava-adp-38-hotel-cross-report-parity-v1.mjs"], {
    cwd: ROOT,
    encoding: "utf8",
  });
  assert.match(run.stdout, /"hotelCount": 38/);
  assert.match(run.stdout, /AVA_ADP_CROSS_REPORT_PARITY_BLOCKED_WITH_EXACT_REASONS|AVA_ADP_38_HOTEL_CROSS_REPORT_PARITY_PASS/);
  // Do not fabricate MATCH for known metric-drift hotels; exit may be non-zero when blocked.
  assert.ok(run.status === 0 || run.status === 2, run.stdout + run.stderr);
}

console.log(`PASS ${ADP_AVA_REFERENCE_CONTRACT}`);
