#!/usr/bin/env node
/**
 * Regression tests for Brand AI Visibility correctness audit fixes.
 * No live provider / public crawl / Airtable writes.
 */
import assert from "node:assert/strict";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  HEADLINE_GEOGRAPHIES,
  findMatchingSummaries,
  getBrandQuestionsPayload,
  getBrandSourcesPayload,
  getBrandTrendPayload,
  parseGeographyQuery,
  resolveBrandGeographyMonitoringState,
} from "../lib/ai-visibility/brand-read-service.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { resolvePeerSetMembership, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { isBlockedFixtureDomain } from "../lib/ai-visibility/fixture-domain-guard.js";
import { computeBrandQuestionMetrics, computePortfolioQuestionMetrics } from "../lib/ai-visibility/portfolio-question-metrics.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok — ${name}`);
}

const brandId = "recEJCTDj1zrsjPM6";

function graph() {
  const membership = resolvePeerSetMembership({
    peerSetId: PEER_SET_ID_V2,
    commercialRegion: "CALA",
  });
  return buildFixtureEntitlementGraph({
    entitledBrandIds: [brandId],
    peerBrandIds: membership.entityIds || [],
    source: "correctness-test",
  });
}

async function main() {
  console.log("test:ai-visibility-brand-correctness-audit\n");
  const store = createBrandAiVisibilityReadStore({});
  const entitlementGraph = graph();

  await test("METRIC_BOUNDS — questions won/missing <= denom; rates in [0,1]", async () => {
    for (const g of HEADLINE_GEOGRAPHIES.filter((x) =>
      ["CALA", "Europe", "Global", "North America"].includes(x.key)
    )) {
      const mon = await resolveBrandGeographyMonitoringState({
        store,
        brandId,
        geoFilter: g,
        provider: "openai",
        language: "en",
      });
      if (!mon.monitored) continue;
      assert.ok(mon.promptDenominator > 0);
      assert.ok(mon.questionsWon <= mon.promptDenominator);
      assert.ok(mon.questionsMissing <= mon.promptDenominator);
      assert.ok(mon.presenceVal >= 0 && mon.presenceVal <= 1);
      if (mon.firstVal != null) assert.ok(mon.firstVal >= 0 && mon.firstVal <= 1);
    }
  });

  await test("GEOGRAPHY_PURITY — REGIONAL_ROWS_RESOLVE_INDEPENDENT_GEOGRAPHIES", async () => {
    const rows = [];
    for (const g of HEADLINE_GEOGRAPHIES) {
      const mon = await resolveBrandGeographyMonitoringState({
        store,
        brandId,
        geoFilter: g,
        provider: "openai",
        language: "en",
      });
      if (!mon.monitored) continue;
      rows.push({
        geo: g.key,
        slots: (mon.latestSummary?._matchedSlotKeys || []).join(","),
      });
    }
    assert.ok(rows.length >= 2);
    const uniqueSlots = new Set(rows.map((r) => r.slots));
    assert.ok(
      uniqueSlots.size === rows.length,
      `expected independent slots, got ${JSON.stringify(rows)}`
    );
  });

  await test("PROVIDER_PURITY — NO_PROVIDER_FALLBACK / OpenAI not returned for others", async () => {
    const openai = await resolveBrandGeographyMonitoringState({
      store,
      brandId,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: "openai",
      language: "en",
    });
    for (const provider of ["perplexity", "gemini", "claude"]) {
      const mon = await resolveBrandGeographyMonitoringState({
        store,
        brandId,
        geoFilter: parseGeographyQuery({ geography: "CALA" }),
        provider,
        language: "en",
      });
      if (!mon.monitored) continue;
      assert.notEqual(
        mon.latestSummary?.batchId,
        openai.latestSummary?.batchId,
        `${provider} must not reuse OpenAI batch`
      );
    }
  });

  await test("UNMONITORED_PROVIDER_RETURNS_GOVERNED_STATE", async () => {
    const mon = await resolveBrandGeographyMonitoringState({
      store,
      brandId,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: "does_not_exist_provider",
      language: "en",
    });
    assert.equal(mon.monitored, false);
    assert.equal(mon.code, "no_batch");
  });

  await test("FIXTURE_SOURCE_NOT_VISIBLE_IN_RUNTIME_REPORT", async () => {
    const sources = await getBrandSourcesPayload({
      dealalityUser: { id: "t" },
      entitlementGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "en",
    });
    for (const s of sources.sources || []) {
      assert.equal(isBlockedFixtureDomain(s.domain), false, s.domain);
    }
  });

  await test("TREND_COMPARABILITY — CALA has points when monitored", async () => {
    const trend = await getBrandTrendPayload({
      dealalityUser: { id: "t" },
      entitlementGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "en",
    });
    assert.ok(trend.pointCount >= 1, "CALA should have at least one trend point");
    assert.notEqual(trend.renderState, "NO_CURRENT_MONITORING");
    assert.ok(!/Not Monitored for this geography yet/i.test(trend.message || ""));
  });

  await test("QUESTION_STATUS_RECONCILES_WITH_PRESENCE", async () => {
    const mon = await resolveBrandGeographyMonitoringState({
      store,
      brandId,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: "openai",
      language: "en",
    });
    const q = await getBrandQuestionsPayload({
      dealalityUser: { id: "t" },
      entitlementGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "en",
      filter: "all",
    });
    const first = (q.questions || []).filter((r) => r.brandStatus === "First Recommended")
      .length;
    assert.equal(first, mon.questionsWon);
    const missing = (q.questions || []).filter((r) => r.brandStatus === "Missing").length;
    assert.equal(missing, mon.questionsMissing);
  });

  await test("PORTFOLIO unique-prompt metrics invariants", async () => {
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "openai",
      { language: "en" }
    );
    assert.ok(summaries.length);
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
    });
    const brandQ = computeBrandQuestionMetrics(observations, brandId);
    assert.ok(brandQ.INVARIANT_RATE_LE_1);
    const port = computePortfolioQuestionMetrics(observations, [brandId]);
    assert.ok(port.INVARIANT_RATE_LE_1);
    assert.ok(port.questionsWonRate == null || port.questionsWonRate <= 1);
  });

  await test("LANGUAGE_PURITY — CALA exposes en+es; no silent es→en fallback", async () => {
    const {
      listAvailableAiVisibilityLanguages,
      resolveMonitoringLanguageForRead,
      getBrandOverviewPayload,
    } = await import("../lib/ai-visibility/brand-read-service.js");
    const avail = await listAvailableAiVisibilityLanguages({
      store,
      provider: "openai",
      geographyFilter: parseGeographyQuery({ geography: "CALA" }),
    });
    assert.deepEqual(avail.availableLanguages, ["en", "es"]);
    assert.equal(avail.filterContract.visible, true);

    const esResolved = await resolveMonitoringLanguageForRead({
      store,
      provider: "openai",
      geographyFilter: parseGeographyQuery({ geography: "CALA" }),
      language: "es",
    });
    assert.equal(esResolved.status, "ok");
    assert.equal(esResolved.language, "es");
    assert.equal(esResolved.SILENT_LANGUAGE_FALLBACK, false);

    const monEn = await resolveBrandGeographyMonitoringState({
      store,
      brandId,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: "openai",
      language: "en",
    });
    const monEs = await resolveBrandGeographyMonitoringState({
      store,
      brandId,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: "openai",
      language: "es",
    });
    assert.equal(monEn.monitored, true);
    assert.equal(monEs.monitored, true);
    assert.deepEqual(monEn.latestSummary?._matchedSlotKeys, ["CALA_EN"]);
    assert.deepEqual(monEs.latestSummary?._matchedSlotKeys, ["CALA_ES"]);
    // Values may coincide by chance, but slot keys must differ (no English reuse).
    assert.notEqual(
      (monEn.latestSummary?._matchedSlotKeys || []).join(","),
      (monEs.latestSummary?._matchedSlotKeys || []).join(",")
    );

    const ovEs = await getBrandOverviewPayload({
      dealalityUser: { id: "t" },
      entitlementGraph,
      brandNamesById: { [brandId]: "Autograph" },
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "es",
    });
    assert.notEqual(ovEs.availabilityReason, "no_batch");
    assert.ok(
      ovEs.kpis?.aiPresence?.availability === "observed" ||
        ovEs.kpis?.aiPresence?.availability === "zero"
    );
  });

  await test("LANGUAGE_PURITY — Europe OpenAI is English-only (filter hidden)", async () => {
    const { listAvailableAiVisibilityLanguages } = await import(
      "../lib/ai-visibility/brand-read-service.js"
    );
    const avail = await listAvailableAiVisibilityLanguages({
      store,
      provider: "openai",
      geographyFilter: parseGeographyQuery({ geography: "Europe" }),
    });
    assert.deepEqual(avail.availableLanguages, ["en"]);
    assert.equal(avail.filterContract.visible, false);
  });

  console.log(`\n${passed} passed`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
