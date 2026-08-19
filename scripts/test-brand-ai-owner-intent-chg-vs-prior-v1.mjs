#!/usr/bin/env node
/**
 * Brand AI Owner Intent Chg vs Prior Run V1.
 * Stored corpus only. PROVIDER_CALLS = 0.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import { buildUnifiedOwnerIntentCoverage } from "../lib/ai-visibility/competitive-moat/unified-owner-intent-coverage.js";
import {
  BENCHMARK_SCOPES,
  lookupScopeCertification,
  verifyAllProvidersFrozenBaseline,
} from "../lib/ai-visibility/competitive-moat/provider-scoped-benchmark-certification.js";
import {
  BASELINE_MEASUREMENT_PERIOD,
} from "../lib/ai-visibility/competitive-moat/period-scoped-grain.js";
import {
  COMPARISON_STATUS,
  CHG_VS_PRIOR_PROVIDER_CALLS,
  CUSTOMER_TREND_LABELS_ENABLED,
  attachChgVsPriorToCoverageRows,
  auditOwnerIntentChgVsPriorUniverse,
  auditPeriodIntegrity,
  buildOwnerIntentChgVsPrior,
  comparisonUnitKey,
  formatIndexChangeDisplay,
  inferCurrentPeriodId,
  loadLongitudinalPeriodObservations,
  selectCurrentAndPriorPeriods,
  smokeAutographChgVsPrior,
  toCustomerSafeChgVsPrior,
} from "../lib/ai-visibility/competitive-moat/owner-intent-chg-vs-prior.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { findMatchingSummaries } from "../lib/ai-visibility/brand-read-service.js";
import { auditPayloadForCanonicalPromptLeaks } from "../lib/ai-visibility/customer-prompt-disclosure.js";
import { listShowcaseMonitoringBrandIds } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BRAND_JS = fs.readFileSync(path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"), "utf8");
const BRAND_CSS = fs.readFileSync(path.join(ROOT, "public/js/ai-visibility/ai-visibility-shared.css"), "utf8");
const BRAND_HTML = fs.readFileSync(path.join(ROOT, "public/ai-visibility-brand.html"), "utf8");
const LONGITUDINAL_PERIOD = "aiv_brand_longitudinal_period_20260818_6579d2";

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

function unifiedFn() {
  const start = BRAND_JS.indexOf("function renderIntentCoverage");
  const end = BRAND_JS.indexOf("function renderLegacyIntentCoverage");
  assert.ok(start > 0 && end > start, "renderIntentCoverage bounds");
  return BRAND_JS.slice(start, end);
}

async function loadLiveObservations(providerId = "openai") {
  const store = createBrandAiVisibilityReadStore();
  const geo = { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };
  const summary = (await findMatchingSummaries(store, geo, providerId, { language: "en" }))[0];
  if (!summary) return [];
  const { observations } = await loadObservationsFromBatchSummary(store, summary, { language: "en" });
  return observations || [];
}

async function loadAllProviderLiveObservations() {
  const providers = ["openai", "gemini", "perplexity", "claude"];
  const out = [];
  for (const p of providers) {
    const rows = await loadLiveObservations(p);
    out.push(...rows.filter((o) => String(o.provider || "").toLowerCase() === p));
  }
  return out;
}

function period(id, date, extras = {}) {
  return {
    measurementPeriodId: id,
    measurementDate: date,
    completedAt: `${date}T12:00:00.000Z`,
    qualification: { valid: true, qualityState: "VALID" },
    geography: { key: "CALA", commercialRegion: "CALA" },
    language: "en",
    measurementContractKey: extras.contract || "peer:v2|cert:same",
    ...extras,
  };
}

console.log("\nBrand AI Owner Intent Chg vs Prior Run V1\n");

await test("same exact scope current/prior matching", () => {
  const a = comparisonUnitKey({
    subjectBrandId: IDS.AUTOGRAPH,
    scenarioId: S.SOFT_BRAND,
    providerScope: BENCHMARK_SCOPES.PERPLEXITY,
    geography: "CALA",
    language: "en",
  });
  const b = comparisonUnitKey({
    subjectBrandId: IDS.AUTOGRAPH,
    scenarioId: S.SOFT_BRAND,
    providerScope: BENCHMARK_SCOPES.PERPLEXITY,
    geography: "CALA",
    language: "en",
  });
  assert.equal(a, b);
  const h = buildOwnerIntentChgVsPrior({
    currentUnit: { subjectBrandId: IDS.AUTOGRAPH, scenarioId: S.SOFT_BRAND, providerScope: BENCHMARK_SCOPES.PERPLEXITY, geography: "CALA", language: "en" },
    priorUnit: { subjectBrandId: IDS.AUTOGRAPH, scenarioId: S.SOFT_BRAND, providerScope: BENCHMARK_SCOPES.PERPLEXITY, geography: "CALA", language: "en" },
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 114,
    priorIndex: 105,
    currentPresence: 0.9,
    priorPresence: 0.8,
  });
  assert.equal(h.comparisonStatus, COMPARISON_STATUS.COMPARABLE);
  assert.equal(h.indexChangePoints, 9);
  assert.equal(h.chgVsPriorDisplay, "+9 pts");
});

await test("cross-provider comparison prohibited", () => {
  const h = buildOwnerIntentChgVsPrior({
    currentUnit: { subjectBrandId: IDS.AUTOGRAPH, scenarioId: S.SOFT_BRAND, providerScope: BENCHMARK_SCOPES.ALL_PROVIDERS, geography: "CALA", language: "en" },
    priorUnit: { subjectBrandId: IDS.AUTOGRAPH, scenarioId: S.SOFT_BRAND, providerScope: BENCHMARK_SCOPES.OPENAI, geography: "CALA", language: "en" },
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 103,
    priorIndex: 100,
  });
  assert.equal(h.comparisonStatus, COMPARISON_STATUS.INSUFFICIENT_COMPARABLE_HISTORY);
  assert.equal(h.chgVsPriorDisplay, null);
  assert.equal(h._internal.CROSS_SCOPE_COLLISION, true);
});

await test("cross-scenario comparison prohibited", () => {
  const h = buildOwnerIntentChgVsPrior({
    currentUnit: { subjectBrandId: IDS.AUTOGRAPH, scenarioId: S.SOFT_BRAND, providerScope: BENCHMARK_SCOPES.OPENAI, geography: "CALA", language: "en" },
    priorUnit: { subjectBrandId: IDS.AUTOGRAPH, scenarioId: S.CONVERSION_SUITABILITY, providerScope: BENCHMARK_SCOPES.OPENAI, geography: "CALA", language: "en" },
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 100,
    priorIndex: 90,
  });
  assert.equal(h.chgVsPriorDisplay, null);
  assert.equal(h._internal.CROSS_SCOPE_COLLISION, true);
});

await test("cross-geography comparison prohibited", () => {
  const h = buildOwnerIntentChgVsPrior({
    currentUnit: { subjectBrandId: IDS.AUTOGRAPH, scenarioId: S.SOFT_BRAND, providerScope: BENCHMARK_SCOPES.OPENAI, geography: "CALA", language: "en" },
    priorUnit: { subjectBrandId: IDS.AUTOGRAPH, scenarioId: S.SOFT_BRAND, providerScope: BENCHMARK_SCOPES.OPENAI, geography: "Europe", language: "en" },
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 100,
    priorIndex: 90,
  });
  assert.equal(h.chgVsPriorDisplay, null);
});

await test("current certified + prior certified → index change", () => {
  const up = buildOwnerIntentChgVsPrior({
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 114,
    priorIndex: 105,
  });
  assert.equal(up.chgVsPriorDisplay, "+9 pts");
  const down = buildOwnerIntentChgVsPrior({
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 105,
    priorIndex: 114,
  });
  assert.equal(down.chgVsPriorDisplay, "-9 pts");
  assert.equal(formatIndexChangeDisplay(9), "+9 pts");
  assert.equal(formatIndexChangeDisplay(-9), "-9 pts");
});

await test("current certified + prior uncertified → no numeric index change", () => {
  const h = buildOwnerIntentChgVsPrior({
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: false,
    currentIndex: 103,
    priorIndex: 99,
    currentPresence: 1,
    priorPresence: 0.9,
  });
  assert.equal(h.comparisonStatus, COMPARISON_STATUS.PRIOR_NOT_CERTIFIED);
  assert.equal(h.chgVsPriorDisplay, null);
  assert.equal(h.indexChangePoints, null);
  assert.equal(h.presenceHistoryAvailable, true);
  assert.equal(h.presenceChangePoints, 10);
});

await test("Presence history independent from benchmark certification", () => {
  const h = buildOwnerIntentChgVsPrior({
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: false,
    priorCertified: false,
    currentPresence: 1,
    priorPresence: 0.8,
  });
  assert.equal(h.chgVsPriorDisplay, null);
  assert.equal(h.presenceHistoryAvailable, true);
  assert.equal(h.presenceChangePoints, 20);
  assert.equal(h.historyAvailable, true);
});

await test("first period → no numeric change", () => {
  const h = buildOwnerIntentChgVsPrior({
    currentPeriod: period("p1", "2026-08-14"),
    priorPeriod: null,
    currentCertified: true,
    currentIndex: 103,
    currentPresence: 1,
  });
  assert.equal(h.comparisonStatus, COMPARISON_STATUS.NO_PRIOR_PERIOD);
  assert.equal(h.chgVsPriorDisplay, null);
  assert.equal(h.historyAvailable, false);
});

await test("0 point movement → No change", () => {
  const h = buildOwnerIntentChgVsPrior({
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 100,
    priorIndex: 100,
  });
  assert.equal(h.indexChangePoints, 0);
  assert.equal(h.chgVsPriorDisplay, "No change");
  assert.notEqual(h.chgVsPriorDisplay, "0");
  assert.notEqual(h.chgVsPriorDisplay, "0 pts");
});

await test("measurement contract changed suppresses index change", () => {
  const h = buildOwnerIntentChgVsPrior({
    currentPeriod: period("p2", "2026-08-18"),
    priorPeriod: period("p1", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 110,
    priorIndex: 103,
    measurementContractCompatible: false,
    currentPresence: 0.9,
    priorPresence: 0.8,
  });
  assert.equal(h.comparisonStatus, COMPARISON_STATUS.MEASUREMENT_CONTRACT_CHANGED);
  assert.equal(h.chgVsPriorDisplay, null);
  assert.equal(h.presenceHistoryAvailable, true);
});

await test("no trend wording in customer history payload or compact UI", () => {
  assert.equal(CUSTOMER_TREND_LABELS_ENABLED, false);
  const fn = unifiedFn();
  assert.doesNotMatch(fn, /Trending up/);
  assert.doesNotMatch(fn, /Trending down/);
  assert.doesNotMatch(fn, /Improving/);
  assert.doesNotMatch(fn, /Declining/);
  assert.doesNotMatch(fn, />Trend</);
  assert.match(fn, /Your<br>Presence[\s\S]{0,400}Δ vs<br>prior run/);
  assert.match(fn, /renderChgVsPriorCell/);
  const hist = BRAND_JS.slice(
    BRAND_JS.indexOf("function renderHistoricalComparison"),
    BRAND_JS.indexOf("function renderOwnerIntentPeerChips")
  );
  assert.match(hist, /period-over-period movement, not a long-term trend/);
  assert.doesNotMatch(hist, /Trending/);
  assert.doesNotMatch(hist, /Improving/);
  assert.doesNotMatch(hist, /Declining/);
});

await test("compact UI column, info icon, colspan 8, no sparkline", () => {
  const fn = unifiedFn();
  assert.match(fn, /CHG_VS_PRIOR_RUN/);
  assert.match(fn, /colspan="8"/);
  assert.doesNotMatch(fn, /sparkline/i);
  assert.match(BRAND_JS, /Shows the change in this Owner Intent's certified AI Presence Index versus the most recent comparable prior measurement run/);
  assert.match(BRAND_JS, /Insufficient History/);
  assert.match(BRAND_JS, /insufficient_history/);
  assert.match(BRAND_CSS, /nth-child\(8\)/);
  assert.match(BRAND_HTML, /owner-intent-chg-vs-prior-v1/);
  assert.doesNotMatch(fn, /Autograph/);
});

await test("customer payload strips internal period IDs", () => {
  const raw = buildOwnerIntentChgVsPrior({
    currentPeriod: period("aiv_secret_period", "2026-08-18"),
    priorPeriod: period("aiv_secret_prior", "2026-08-14"),
    currentCertified: true,
    priorCertified: true,
    currentIndex: 114,
    priorIndex: 105,
  });
  const customer = toCustomerSafeChgVsPrior(raw);
  const serialized = JSON.stringify(customer);
  assert.equal(serialized.includes("aiv_secret_period"), false);
  assert.equal(customer.currentPeriodDate, "2026-08-18");
  assert.equal(customer.priorPeriodDate, "2026-08-14");
});

await test("later wave is not treated as prior to live baseline current", () => {
  const { currentPeriod, priorPeriod } = selectCurrentAndPriorPeriods({
    currentPeriodId: BASELINE_MEASUREMENT_PERIOD,
    geography: "CALA",
    language: "en",
    anchorToLiveCurrent: true,
  });
  assert.equal(currentPeriod?.measurementPeriodId, BASELINE_MEASUREMENT_PERIOD);
  assert.equal(priorPeriod, null);
});

await test("live Autograph certified values unchanged + no numeric Chg vs Prior yet", async () => {
  const obs = await loadLiveObservations("openai");
  const all = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
    geography: "CALA",
    language: "en",
  });
  const openai = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "openai",
    observations: obs.filter((o) => String(o.provider || "").toLowerCase() === "openai"),
    geography: "CALA",
    language: "en",
  });
  const perpObs = (await loadLiveObservations("perplexity")).filter(
    (o) => String(o.provider || "").toLowerCase() === "perplexity"
  );
  const perp = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "perplexity",
    observations: perpObs,
    geography: "CALA",
    language: "en",
  });
  const softAll = (all.rows || []).find((r) => r.scenarioId === S.SOFT_BRAND);
  const softOai = (openai.rows || []).find((r) => r.scenarioId === S.SOFT_BRAND);
  const softPerp = (perp.rows || []).find((r) => r.scenarioId === S.SOFT_BRAND);
  assert.equal(softAll.indexValue, 103);
  assert.equal(softOai.indexValue, 100);
  assert.equal(softPerp.indexValue, 114);
  assert.equal(softAll.chgVsPriorDisplay, null);
  assert.equal(softAll.comparisonStatus, COMPARISON_STATUS.NO_PRIOR_PERIOD);
  assert.notEqual(String(softAll.chgVsPriorDisplay || "—"), "0");
});

await test("Tapestry 103 and Ascend 67 frozen", () => {
  const tap = lookupScopeCertification(IDS.TAPESTRY, S.SOFT_BRAND, BENCHMARK_SCOPES.ALL_PROVIDERS);
  const asc = lookupScopeCertification(IDS.ASCEND, S.SOFT_BRAND, BENCHMARK_SCOPES.ALL_PROVIDERS);
  assert.equal(tap.certifiedIndex, 103);
  assert.equal(asc.certifiedIndex, 67);
  const frozen = verifyAllProvidersFrozenBaseline();
  assert.equal(frozen.ok, true);
  assert.equal(frozen.AUTOGRAPH_103_DIFF, 0);
  assert.equal(frozen.TAPESTRY_103_DIFF, 0);
  assert.equal(frozen.ASCEND_67_DIFF, 0);
});

await test("period integrity: no cross-scope collisions or duplicate comparisons", () => {
  const integrity = auditPeriodIntegrity();
  assert.equal(integrity.CROSS_SCOPE_COLLISIONS, 0);
  assert.equal(integrity.DUPLICATE_PERIOD_COMPARISONS, 0);
  assert.ok(integrity.PERIODS_FOUND >= 2, "expected baseline + longitudinal period");
});

await test("full universe live current has no invalid numeric change", async () => {
  const currentObservations = await loadAllProviderLiveObservations();
  const storedCurrent = loadLongitudinalPeriodObservations(LONGITUDINAL_PERIOD);
  const audit = auditOwnerIntentChgVsPriorUniverse({
    brandIds: listShowcaseMonitoringBrandIds(),
    currentObservations,
    storedCurrentObservations: storedCurrent,
    storedPriorObservations: currentObservations,
    currentPeriodId: BASELINE_MEASUREMENT_PERIOD,
  });
  assert.equal(audit.ROWS_WITH_VALID_CHANGE_BUT_NOT_RENDERABLE, 0);
  assert.equal(audit.ROWS_WITHOUT_VALID_PRIOR_SHOWING_NUMERIC_CHANGE, 0);
  assert.equal(audit.ROWS_ELIGIBLE_FOR_CHANGE_NOW, 0);
  assert.equal(audit.livePriorPeriodId, null);
  assert.ok(audit.BRANDS_WITH_2_PLUS_VALID_PERIODS >= 19);
  assert.ok(audit.COMPARABLE_PRESENCE_ROWS > 0, "stored corpus should have comparable presence rows");
  assert.equal(audit.COMPARABLE_CERTIFIED_INDEX_ROWS, 0);
  globalThis.__CHG_VS_PRIOR_AUDIT__ = audit;
});

await test("Autograph smoke by provider scope", async () => {
  const obs = await loadAllProviderLiveObservations();
  const smoke = smokeAutographChgVsPrior({
    brandId: IDS.AUTOGRAPH,
    scenarioId: S.SOFT_BRAND,
    currentObservations: obs,
    currentPeriodId: BASELINE_MEASUREMENT_PERIOD,
  });
  for (const scope of Object.keys(smoke)) {
    assert.equal(smoke[scope].CURRENT_PERIOD, BASELINE_MEASUREMENT_PERIOD);
    assert.equal(smoke[scope].PRIOR_PERIOD, null);
    assert.equal(smoke[scope].CHG_VS_PRIOR, null);
    assert.equal(smoke[scope].DISPLAY_STATUS, COMPARISON_STATUS.NO_PRIOR_PERIOD);
  }
  assert.equal(smoke.ALL_PROVIDERS.CURRENT_INDEX, 103);
  assert.equal(smoke.OPENAI.CURRENT_INDEX, 100);
  assert.equal(smoke.PERPLEXITY.CURRENT_INDEX, 114);
  assert.equal(smoke.GEMINI.CURRENT_INDEX, null);
  assert.equal(smoke.CLAUDE.CURRENT_INDEX, lookupScopeCertification(IDS.AUTOGRAPH, S.SOFT_BRAND, BENCHMARK_SCOPES.CLAUDE)?.certifiedIndex ?? smoke.CLAUDE.CURRENT_INDEX);
  globalThis.__AUTOGRAPH_SMOKE__ = smoke;
});

await test("prompt moat on unified payload with history fields", async () => {
  const obs = await loadLiveObservations("openai");
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  const leak = auditPayloadForCanonicalPromptLeaks(payload);
  assert.equal(leak.leakCount, 0, leak.leaks?.join(", "));
  const fn = unifiedFn();
  assert.doesNotMatch(fn, /questionText|promptText|canonicalPrompt/);
});

await test("attach does not invent numeric change without prior observations", () => {
  const rows = attachChgVsPriorToCoverageRows(
    [{ scenarioId: S.SOFT_BRAND, subjectPresence: 1, indexValue: 103, intentLabel: "Soft Brand Affiliation" }],
    {
      brandId: IDS.AUTOGRAPH,
      allProvidersMode: true,
      currentObservations: [{ success: true, provider: "openai", language: "en", geography: "CALA", presentEntityIds: [IDS.AUTOGRAPH] }],
      currentPeriodId: BASELINE_MEASUREMENT_PERIOD,
      geography: "CALA",
      language: "en",
    }
  );
  assert.equal(rows[0].chgVsPriorDisplay, null);
  assert.equal(rows[0].indexValue, 103);
});

await test("no provider calls / operator freeze / spend", () => {
  assert.equal(CHG_VS_PRIOR_PROVIDER_CALLS, 0);
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
  assert.equal(inferCurrentPeriodId([]), BASELINE_MEASUREMENT_PERIOD);
});

console.log("\n" + passed + " passed, " + failed + " failed\n");

const audit = globalThis.__CHG_VS_PRIOR_AUDIT__ || {};
const smoke = globalThis.__AUTOGRAPH_SMOKE__ || {};
const integrity = audit.integrity || auditPeriodIntegrity();

console.log("BRAND_AI_OWNER_INTENT_CHG_VS_PRIOR_V1_COMPLETE");
console.log("TOTAL_MEASUREMENT_PERIODS:", integrity.PERIODS_FOUND);
console.log("BRANDS_WITH_2_PLUS_VALID_PERIODS:", audit.BRANDS_WITH_2_PLUS_VALID_PERIODS);
console.log("COMPARABLE_PRESENCE_ROWS:", audit.COMPARABLE_PRESENCE_ROWS);
console.log("COMPARABLE_CERTIFIED_INDEX_ROWS:", audit.COMPARABLE_CERTIFIED_INDEX_ROWS);
console.log("ROWS_ELIGIBLE_FOR_CHANGE_NOW:", audit.ROWS_ELIGIBLE_FOR_CHANGE_NOW);
console.log("CROSS_SCOPE_COLLISIONS:", integrity.CROSS_SCOPE_COLLISIONS);
console.log("DUPLICATE_PERIOD_COMPARISONS:", integrity.DUPLICATE_PERIOD_COMPARISONS);
console.log("INCOMPATIBLE_PERIOD_PAIRS:", integrity.INCOMPATIBLE_PERIOD_PAIRS);
console.log("AUTOGRAPH_SMOKE:", JSON.stringify(smoke, null, 2));

if (failed) {
  console.log("BRAND_AI_OWNER_INTENT_CHG_VS_PRIOR_V1_REMEDIATION_REQUIRED");
  process.exit(1);
}
console.log("BRAND_AI_OWNER_INTENT_CHG_VS_PRIOR_V1_PASS");
