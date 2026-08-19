#!/usr/bin/env node
/**
 * Brand AI Visibility — Language purity audit tests (Part 18).
 * No live provider / public crawl / Airtable writes.
 */
import assert from "node:assert/strict";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  listAvailableAiVisibilityLanguages,
  resolveMonitoringLanguageForRead,
  findMatchingSummaries,
  parseGeographyQuery,
  resolveBrandGeographyMonitoringState,
  getBrandOverviewPayload,
  getBrandQuestionsPayload,
  getBrandSourcesPayload,
  getBrandEvidencePayload,
  getBrandTrendPayload,
} from "../lib/ai-visibility/brand-read-service.js";
import { listLanguagesFromMultiSlotSummary } from "../lib/ai-visibility/multi-slot-geography.js";
import {
  compareTrendObservations,
  TREND_LANGUAGE_MATCH_REQUIRED,
} from "../lib/ai-visibility/trend-comparability.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { resolvePeerSetMembership, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";

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
    source: "language-purity-test",
  });
}

async function main() {
  console.log("test:ai-visibility-language-purity\n");
  const store = createBrandAiVisibilityReadStore({});
  const entitlementGraph = graph();
  const common = {
    dealalityUser: { id: "t" },
    entitlementGraph,
    brandNamesById: { [brandId]: "Autograph Collection" },
    store,
    brandId,
    provider: "openai",
    geography: "CALA",
  };

  await test("LANGUAGE_SELECTOR_VISIBLE_WHEN_MULTILINGUAL", async () => {
    const avail = await listAvailableAiVisibilityLanguages({
      store,
      provider: "openai",
      geographyFilter: parseGeographyQuery({ geography: "CALA" }),
    });
    assert.ok(avail.availableLanguages.length > 1);
    assert.equal(avail.filterContract.visible, true);
  });

  await test("LANGUAGE_SELECTOR_HIDDEN_WHEN_SINGLE_LANGUAGE", async () => {
    const avail = await listAvailableAiVisibilityLanguages({
      store,
      provider: "openai",
      geographyFilter: parseGeographyQuery({ geography: "Europe" }),
    });
    assert.equal(avail.availableLanguages.length, 1);
    assert.equal(avail.filterContract.visible, false);
  });

  await test("CALA_OPENAI_EXPOSES_ENGLISH_AND_SPANISH", async () => {
    const avail = await listAvailableAiVisibilityLanguages({
      store,
      provider: "openai",
      geographyFilter: parseGeographyQuery({ geography: "CALA" }),
    });
    assert.deepEqual(avail.availableLanguages, ["en", "es"]);
  });

  await test("EUROPE_OPENAI_EXPOSES_ONLY_ENGLISH", async () => {
    const avail = await listAvailableAiVisibilityLanguages({
      store,
      provider: "openai",
      geographyFilter: parseGeographyQuery({ geography: "Europe" }),
    });
    assert.deepEqual(avail.availableLanguages, ["en"]);
  });

  await test("SPANISH_DOES_NOT_FALLBACK_TO_ENGLISH", async () => {
    const resolved = await resolveMonitoringLanguageForRead({
      store,
      provider: "openai",
      geographyFilter: parseGeographyQuery({ geography: "Europe" }),
      language: "es",
    });
    assert.equal(resolved.SILENT_LANGUAGE_FALLBACK, false);
    assert.equal(resolved.status, "not_monitored");
    assert.equal(resolved.language, "es");
    const ov = await getBrandOverviewPayload({
      ...common,
      geography: "Europe",
      language: "es",
    });
    assert.equal(ov.availability || ov.kpis?.aiPresence?.availability, "not_monitored");
    assert.equal(ov.kpis?.aiPresence?.display, undefined);
    assert.match(String(ov.availabilityMessage || ""), /Spanish/i);
  });

  await test("ENGLISH_DOES_NOT_FALLBACK_TO_SPANISH", async () => {
    const monEn = await resolveBrandGeographyMonitoringState({
      store,
      brandId,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: "openai",
      language: "en",
    });
    assert.deepEqual(monEn.latestSummary?._matchedSlotKeys, ["CALA_EN"]);
    assert.notEqual(monEn.latestSummary?._matchedSlotKeys?.[0], "CALA_ES");
  });

  await test("LANGUAGE_FILTER_CHANGES_METRICS_WHEN_DATA_DIFFERS", async () => {
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
    assert.ok(Math.abs(monEn.presenceVal - 0.917) < 0.01);
    assert.equal(monEn.questionsWon, 8);
    assert.ok(Math.abs(monEs.presenceVal - 0.833) < 0.01);
    assert.equal(monEs.questionsWon, 6);
    assert.notEqual(monEn.presenceVal, monEs.presenceVal);
    assert.notEqual(monEn.questionsWon, monEs.questionsWon);
  });

  await test("LANGUAGE_FILTER_CHANGES_QUESTION_ROWS", async () => {
    const qEn = await getBrandQuestionsPayload({ ...common, language: "en", filter: "all" });
    const qEs = await getBrandQuestionsPayload({ ...common, language: "es", filter: "all" });
    const presentEn = (qEn.questions || []).filter((r) => r.brandStatus === "Present").length;
    const presentEs = (qEs.questions || []).filter((r) => r.brandStatus === "Present").length;
    assert.ok(presentEn > 0);
    assert.ok(presentEs > 0);
    assert.notEqual(presentEn, presentEs);
    for (const row of qEn.questions || []) {
      assert.equal(row.question, undefined);
      assert.equal(row.promptText, undefined);
      assert.ok(row.ownerIntent || row.intentLabel);
    }
    const shared = [...new Set((qEn.questions || []).map((q) => q.evidenceId))].filter((id) =>
      (qEs.questions || []).some((q) => q.evidenceId === id)
    );
    assert.equal(shared.length, 0);
  });

  await test("AI_VS_DEALALITY_RESPECTS_LANGUAGE_FILTER", async () => {
    const oEn = await getBrandOverviewPayload({ ...common, language: "en" });
    const oEs = await getBrandOverviewPayload({ ...common, language: "es" });
    const rowsEn = oEn.aiVsDealalityContext?.rows || [];
    const rowsEs = oEs.aiVsDealalityContext?.rows || [];
    assert.ok(rowsEn.length > 0, "expected EN AI vs Dealality rows");
    assert.ok(rowsEs.length > 0, "expected ES AI vs Dealality rows");
    for (const r of rowsEn) {
      assert.equal(r.question, undefined);
      assert.equal(r.promptText, undefined);
      assert.ok(r.ownerIntent);
      assert.ok(r.decisionContext);
    }
    const shared = [...new Set(rowsEn.map((r) => r.evidenceId).filter(Boolean))].filter((id) =>
      rowsEs.some((r) => r.evidenceId === id)
    );
    assert.equal(shared.length, 0, "EN/ES AI vs Dealality must not share evidence rows");
  });

  await test("LANGUAGE_FILTER_CHANGES_SOURCES", async () => {
    const sEn = await getBrandSourcesPayload({ ...common, language: "en" });
    const sEs = await getBrandSourcesPayload({ ...common, language: "es" });
    const dEn = new Set((sEn.sources || []).map((s) => s.domain));
    const dEs = new Set((sEs.sources || []).map((s) => s.domain));
    assert.ok(dEn.size > 0 && dEs.size > 0);
    const onlyEn = [...dEn].filter((d) => !dEs.has(d));
    const onlyEs = [...dEs].filter((d) => !dEn.has(d));
    assert.ok(onlyEn.length > 0 || onlyEs.length > 0 || dEn.size !== dEs.size);
  });

  await test("LANGUAGE_FILTER_CHANGES_EVIDENCE", async () => {
    const qEs = await getBrandQuestionsPayload({ ...common, language: "es", filter: "all" });
    const evId = qEs.questions[0]?.evidenceId;
    assert.ok(evId);
    const raw = await store.getEvidence(evId);
    assert.equal(raw.language, "es");
    const viaEs = await getBrandEvidencePayload({ ...common, language: "es", evidenceId: evId });
    const viaEn = await getBrandEvidencePayload({ ...common, language: "en", evidenceId: evId });
    assert.ok(Array.isArray(viaEs.evidence) && viaEs.evidence.length === 1);
    assert.ok(Array.isArray(viaEn.evidence) && viaEn.evidence.length === 0);
  });

  await test("TREND_LANGUAGE_PURITY_PASS", async () => {
    assert.equal(TREND_LANGUAGE_MATCH_REQUIRED, true);
    const tEn = await getBrandTrendPayload({ ...common, language: "en" });
    const tEs = await getBrandTrendPayload({ ...common, language: "es" });
    assert.equal(tEn.TREND_LANGUAGE_MATCH_REQUIRED, true);
    assert.equal(tEs.language, "es");
    for (const p of tEn.points || []) {
      assert.equal(p.language, "en");
      assert.ok((p.matchedSlotKeys || []).every((k) => k.endsWith("_EN") || k.includes("_EN")));
    }
    for (const p of tEs.points || []) {
      assert.equal(p.language, "es");
      assert.ok((p.matchedSlotKeys || []).every((k) => k.includes("_ES")));
    }
    const cross = compareTrendObservations(
      { language: "en", provider: "openai", geographyKey: "CALA" },
      { language: "es", provider: "openai", geographyKey: "CALA" }
    );
    assert.equal(cross.comparable, false);
    assert.equal(cross.reasonCode, "NON_COMPARABLE_LANGUAGE");
  });

  await test("SLOT_LANGUAGE_OVERRIDES_PARENT_SUMMARY_STAMP", async () => {
    const geo = parseGeographyQuery({ geography: "CALA" });
    const allLang = await findMatchingSummaries(store, geo, "openai", {
      includeAllLanguages: true,
    });
    assert.ok(allLang[0]);
    // Parent includeAllLanguages projection may stamp language=en even when ES slots match.
    const slotLangs = listLanguagesFromMultiSlotSummary(allLang[0], geo);
    assert.deepEqual(slotLangs, ["en", "es"]);
    const avail = await listAvailableAiVisibilityLanguages({
      store,
      provider: "openai",
      geographyFilter: geo,
    });
    assert.deepEqual(avail.availableLanguages, ["en", "es"]);

    const esSums = await findMatchingSummaries(store, geo, "openai", { language: "es" });
    assert.deepEqual(esSums[0]._matchedSlotKeys, ["CALA_ES"]);
    assert.equal(esSums[0].language, "es");
  });

  console.log(`\n${passed} passed`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
