#!/usr/bin/env node
/**
 * Brand AI Coverage Diagnostics Unification V2 — certification-aware render audit.
 * PROVIDER_CALLS = 0
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import {
  buildUnifiedOwnerIntentCoverage,
  computePeerPresentGapFromFixture,
} from "../lib/ai-visibility/competitive-moat/unified-owner-intent-coverage.js";
import {
  BENCHMARK_SCOPES,
  loadProviderScopedCertificationRegistry,
  lookupScopeCertification,
  verifyAllProvidersFrozenBaseline,
} from "../lib/ai-visibility/competitive-moat/provider-scoped-benchmark-certification.js";
import { verifyFrozenBaseline } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-longitudinal-recertification.js";
import { auditPayloadForCanonicalPromptLeaks } from "../lib/ai-visibility/customer-prompt-disclosure.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { findMatchingSummaries } from "../lib/ai-visibility/brand-read-service.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const REGISTRY_PATH = path.join(
  ROOT,
  "reports/ai-visibility/provider-scoped-benchmark-certification-v1.json"
);
const BRAND_JS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"),
  "utf8"
);
const BRAND_HTML = fs.readFileSync(path.join(ROOT, "public/ai-visibility-brand.html"), "utf8");

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

async function loadObsForProvider(providerId) {
  const store = createBrandAiVisibilityReadStore();
  const geo = { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };
  const summary = (await findMatchingSummaries(store, geo, providerId, { language: "en" }))[0];
  if (!summary) return [];
  const { observations } = await loadObservationsFromBatchSummary(store, summary, {
    language: "en",
  });
  return observations || [];
}

function softRow(payload) {
  return (payload.rows || []).find((r) => r.scenarioId === S.SOFT_BRAND);
}

console.log("\nBrand AI Coverage Diagnostics Unification V2\n");

await test("unified table location and taxonomy in HTML/JS", () => {
  assert.match(BRAND_HTML, /AI Presence by Owner Intent/);
  assert.doesNotMatch(BRAND_HTML, /Owner-Intent Coverage/);
  assert.match(BRAND_JS, /aiv-unified-intent-table/);
  assert.match(BRAND_JS, /Owner Intent/);
  assert.doesNotMatch(BRAND_JS, /Prompt Family.*no-sort/);
});

await test("old duplicate owner-intent benchmark table removed from competitors", () => {
  assert.doesNotMatch(BRAND_JS, /renderOwnerIntentBenchmarks\(data \|\| \{\}, peerTableHtml\)/);
  assert.match(BRAND_JS, /peerTableHtml \+/);
});

await test("decision context lives in expanded Owner Intent detail", () => {
  assert.match(BRAND_JS, /aiv-intent-decision-context/);
  assert.match(BRAND_JS, /r\.decisionContext/);
  assert.match(BRAND_JS, /aiv-unified-intent-detail/);
  assert.match(BRAND_JS, /aiv-intent-expand/);
  assert.match(BRAND_JS, /aria-expanded/);
});

await test("peer-present gap deterministic cases", () => {
  const sub = "recSub";
  const core = "recCore";
  const unrelated = "recOther";
  assert.equal(
    computePeerPresentGapFromFixture([{ success: true, presentEntityIds: [sub, core] }], sub, [core])
      .peerPresentGapCount,
    0
  );
  assert.equal(
    computePeerPresentGapFromFixture([{ success: true, presentEntityIds: [unrelated] }], sub, [core])
      .peerPresentGapCount,
    0
  );
  assert.equal(
    computePeerPresentGapFromFixture([{ success: true, presentEntityIds: [core] }], sub, [core])
      .peerPresentGapCount,
    1
  );
  assert.equal(
    computePeerPresentGapFromFixture([{ success: false, presentEntityIds: [] }], sub, [core]).missing,
    0
  );
});

await test("Autograph All Providers Soft Brand — index 103 position 3% above", async () => {
  const obs = await loadObsForProvider("openai");
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  const row = softRow(payload);
  assert.ok(row);
  assert.equal(row.subjectPresenceDisplay, "100%");
  assert.equal(row.indexValue, 103);
  assert.match(row.position, /3% above benchmark/);
  assert.equal(row.peerPresentGapCount, 0);
});

await test("Autograph OpenAI Soft Brand — index 100 at parity", async () => {
  const obs = (await loadObsForProvider("openai")).filter(
    (o) => String(o.provider || "").toLowerCase() === "openai"
  );
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "openai",
    observations: obs,
  });
  const row = softRow(payload);
  assert.ok(row);
  assert.equal(row.subjectPresenceDisplay, "100%");
  assert.equal(row.indexValue, 100);
  assert.equal(row.position, "At parity");
  assert.equal(row.benchmarkStatus, "CERTIFIED");
});

await test("Autograph Perplexity Soft Brand — registry certified value", async () => {
  const cert = lookupScopeCertification(IDS.AUTOGRAPH, S.SOFT_BRAND, BENCHMARK_SCOPES.PERPLEXITY);
  assert.ok(cert?.certifiedIndex);
  const obs = (await loadObsForProvider("perplexity")).filter(
    (o) => String(o.provider || "").toLowerCase() === "perplexity"
  );
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "perplexity",
    observations: obs,
  });
  const row = softRow(payload);
  assert.equal(row.indexValue, cert.certifiedIndex);
  assert.equal(row.benchmarkStatus, "CERTIFIED");
});

await test("Autograph Claude Soft Brand — registry certified when present", async () => {
  const cert = lookupScopeCertification(IDS.AUTOGRAPH, S.SOFT_BRAND, BENCHMARK_SCOPES.CLAUDE);
  if (cert?.certificationStatus === "PRODUCTION_VALIDATED") {
    const obs = (await loadObsForProvider("claude")).filter(
      (o) => String(o.provider || "").toLowerCase() === "claude"
    );
    const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
      allProvidersMode: false,
      provider: "claude",
      observations: obs,
    });
    const row = softRow(payload);
    assert.equal(row.indexValue, cert.certifiedIndex);
    assert.equal(row.position, "At parity");
  }
});

await test("Autograph Gemini Soft Brand — presence yes, index developing", async () => {
  const obs = (await loadObsForProvider("gemini")).filter(
    (o) => String(o.provider || "").toLowerCase() === "gemini"
  );
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "gemini",
    observations: obs,
  });
  const row = softRow(payload);
  assert.ok(row);
  assert.match(row.subjectPresenceDisplay, /%/);
  assert.equal(row.indexValue, null);
  assert.equal(row.benchmarkStatus, "Benchmark still developing");
  assert.equal(row.position, null);
});

await test("Branded Residences row — presence without certified index", async () => {
  const obs = await loadObsForProvider("openai");
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  const row = (payload.rows || []).find((r) => r.scenarioId === S.BRANDED_RESIDENCES);
  assert.ok(row, "Branded Residences row expected");
  assert.match(row.subjectPresenceDisplay, /%/);
  assert.equal(row.indexValue, null);
  assert.equal(row.benchmarkStatus, "Benchmark still developing");
});

await test("eight+ owner intents visible vs legacy five-only benchmark table", async () => {
  const obs = await loadObsForProvider("openai");
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  assert.ok((payload.rows || []).length >= 8);
  assert.ok((payload.customerVisibleIntents || []).length >= 8);
  const labels = payload.rows.map((r) => r.intentLabel);
  assert.ok(labels.includes("Lifestyle / Individuality"));
  assert.ok(labels.includes("Branded Residences"));
});

await test("true zero peer-present gaps render as 0 in payload", async () => {
  const obs = await loadObsForProvider("openai");
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  const soft = softRow(payload);
  assert.equal(soft.peerPresentGapCount, 0);
  assert.equal(soft.missingCount, 0);
});

await test("customer API payload has no raw prompt leaks", async () => {
  const obs = await loadObsForProvider("openai");
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  const leak = auditPayloadForCanonicalPromptLeaks(payload);
  assert.equal(leak.leakCount, 0);
});

await test("certified registry counts and frozen baselines", () => {
  const reg = loadProviderScopedCertificationRegistry({ refresh: true, registryPath: REGISTRY_PATH });
  assert.equal(reg.counts.ALL_PROVIDERS_CERTIFIED, 3);
  assert.equal(reg.counts.OPENAI_CERTIFIED_NOW, 6);
  assert.equal(reg.counts.GEMINI_CERTIFIED_NOW, 0);
  assert.equal(reg.counts.PERPLEXITY_CERTIFIED_NOW, 6);
  assert.equal(reg.counts.CLAUDE_CERTIFIED_NOW, 6);
  const frozen = verifyFrozenBaseline();
  assert.equal(frozen.AUTOGRAPH_103_DIFF, 0);
  assert.equal(frozen.TAPESTRY_103_DIFF, 0);
  assert.equal(frozen.ASCEND_67_DIFF, 0);
  const scopeFrozen = verifyAllProvidersFrozenBaseline({ registryPath: REGISTRY_PATH });
  assert.equal(scopeFrozen.AUTOGRAPH_103_DIFF, 0);
});

await test("peer-gap audit — no display bug or mapping broken on live corpus", async () => {
  const obs = await loadObsForProvider("openai");
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  assert.equal(payload.peerPresentGapAudit.DISPLAY_BUG, 0);
  assert.equal(payload.peerPresentGapAudit.PEER_CONTEXT_NOT_JOINED, 0);
  assert.equal(payload.unmappedMonitoredObservations, 0);
  assert.match(payload.qualifyingPeerPolicy, /CORE/);
});

console.log("\n" + passed + " passed, " + failed + " failed\n");

if (failed > 0) {
  process.exitCode = 1;
  console.log("BRAND_AI_COVERAGE_DIAGNOSTICS_UNIFICATION_V2_REMEDIATION_REQUIRED");
} else {
  console.log("BRAND_AI_COVERAGE_DIAGNOSTICS_UNIFICATION_V2_PASS");
}
