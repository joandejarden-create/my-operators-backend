#!/usr/bin/env node
/**
 * ADP Lite research mode gate for free Leak Audits.
 * npm run test:adp-leak-audit-lite-research-mode-v1
 */
import assert from "node:assert/strict";
import { readFileSync, mkdtempSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import {
  FREE_AUDIT_SCOPE,
  RESEARCH_MODES,
  INTERNAL_RESEARCH_MODE,
  LEAK_AUDIT_LITE_COST_CONTROLS,
  PROMPT_SET_VERSION_LITE,
  LITE_SCENARIO_COUNT,
  buildLitePromptPlan,
  buildScenariosMonitoredLabel,
  buildCoverMetaLine,
  assertLiteCostControls,
  assertNoFullPromptLibrary,
  assertNoProductionMutationImports,
  FORBIDDEN_PAID_ADP_WRITERS,
  createLeakAuditStore,
  runLeakAuditPhase1,
  loadLeakAuditSampleReport,
  LITE_PROVIDERS,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const sample = loadLeakAuditSampleReport();

assert.equal(FREE_AUDIT_SCOPE.researchMode, RESEARCH_MODES.LEAK_AUDIT_LITE);
assert.equal(INTERNAL_RESEARCH_MODE, "adp_lite_leak_audit");
assert.equal(LEAK_AUDIT_LITE_COST_CONTROLS.maxScenarios, 15);
assert.equal(LEAK_AUDIT_LITE_COST_CONTROLS.maxProviders, 4);
assert.equal(LEAK_AUDIT_LITE_COST_CONTROLS.maxObservations, 60);
assert.equal(PROMPT_SET_VERSION_LITE, "leak_audit_lite_v1");
assert.equal(LITE_SCENARIO_COUNT, 15);
assert.equal(LITE_PROVIDERS.length, 4);

const cost = assertLiteCostControls();
assert.equal(cost.ok, true);

const planned = buildLitePromptPlan(
  ["leisure", "couples", "business", "meetings_groups", "wellness", "family"],
  [...LITE_PROVIDERS]
);
assert.equal(planned.scenariosRun, 15);
assert.equal(planned.providersRun, 4);
assert.equal(planned.observationsPlanned, 60);
assert.equal(planned.promptSetVersion, "leak_audit_lite_v1");
assert.equal(assertNoFullPromptLibrary(planned).ok, true);

const label = buildScenariosMonitoredLabel();
assert.equal(label.value, "15 × 4");
assert.equal(label.valueSubLabel, "60 observations");
assert.match(label.description, /15 traveler scenarios across/);

assert.equal(
  buildCoverMetaLine(),
  "PROVIDERS 4 · SCENARIOS 15 · OBSERVATIONS 60 · ACTION ITEMS 3"
);

assert.equal(sample.scenariosMonitored.value, "15 × 4");
assert.equal(sample.scenariosMonitored.valueSubLabel, "60 observations");
assert.equal(sample.coverMeta.coverMetaLine, buildCoverMetaLine());
assert.equal(sample.researchMode, "leak_audit_lite");

const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
assert.match(html, /PROVIDERS 4 · SCENARIOS 15 · OBSERVATIONS 60 · ACTION ITEMS 3/);

const runAuditSrc = readFileSync(
  join(root, "lib/ai-demand-positioning/leak-audit/run-audit-v1.js"),
  "utf8"
);
const genSrc = readFileSync(
  join(root, "lib/ai-demand-positioning/leak-audit/report-generation-v1.js"),
  "utf8"
);
assert.equal(assertNoProductionMutationImports(runAuditSrc).ok, true);
assert.equal(assertNoProductionMutationImports(genSrc).ok, true);
for (const writer of FORBIDDEN_PAID_ADP_WRITERS) {
  assert.equal(runAuditSrc.includes(writer + "("), false, `must not call ${writer}`);
}

const base = join(root, "data/ai-demand-positioning/leak-audit/_tmp");
mkdirSync(base, { recursive: true });
const tmp = mkdtempSync(join(base, "lite-mode-"));
const store = createLeakAuditStore({ root: tmp });
const request = store.createRequest({
  hotelName: "Lite Mode Test Hotel",
  source: "manual",
});
store.updateRequest(request.id, { status: "approved" });

const full = await runLeakAuditPhase1(store, {
  requestId: request.id,
  providers: [...LITE_PROVIDERS],
  liveProviderCalls: false,
});
assert.equal(full.researchMode, "leak_audit_lite");
assert.equal(full.run.researchMode, "leak_audit_lite");
assert.equal(full.run.maxScenarios, 15);
assert.equal(full.run.maxProviders, 4);
assert.equal(full.run.maxObservations, 60);
assert.equal(full.productionAdpTouched, false);
assert.equal(full.report.scenariosMonitored.value, "15 × 4");
assert.match(JSON.stringify(full.report), /15 × 4/);
assert.equal(/fullPromptText/.test(JSON.stringify(full.report)), false);
assert.equal(/_internalFullPromptText/.test(JSON.stringify(full.observations)), false);

const partial = await (async () => {
  const req2 = store.createRequest({
    hotelName: "Partial Provider Hotel",
    source: "manual",
  });
  store.updateRequest(req2.id, { status: "approved" });
  return runLeakAuditPhase1(store, {
    requestId: req2.id,
    providers: [...LITE_PROVIDERS],
    failedProviders: ["claude"],
    liveProviderCalls: false,
  });
})();
assert.equal(partial.run.completenessFlag, "partial");
assert.equal(partial.run.actualProvidersRun, 3);
assert.equal(partial.report.scenariosMonitored.value, "15 × 3");
assert.equal(partial.report.scenariosMonitored.valueSubLabel, "45 observations");

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_LITE_RESEARCH_MODE_V1",
    researchMode: "leak_audit_lite",
    scenarios: 15,
    providers: 4,
    observations: 60,
    partialProviderOk: true,
    noPaidAdpWrite: true,
  })
);
