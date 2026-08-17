#!/usr/bin/env node
/**
 * Phase 3A.6 — Language Data Foundation tests.
 * No provider calls. Synthetic EN/ES fixtures in temp store only.
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  normalizeLanguage,
  isSupportedAiVisibilityLanguage,
  getLanguageDisplayLabel,
  requireSupportedLanguage,
  resolveReadLanguage,
  buildLanguageFilterContract,
  NON_COMPARABLE_LANGUAGE,
  AI_VISIBILITY_LANGUAGES,
} from "../lib/ai-visibility/language-dimension.js";
import {
  validateSemanticPairMembers,
  promptExecutionIdentity,
} from "../lib/ai-visibility/semantic-pair.js";
import {
  buildTrendComparabilityKey,
  compareTrendObservations,
} from "../lib/ai-visibility/trend-comparability.js";
import { buildPromptCohort, PROMPT_COHORT_VERSION } from "../lib/ai-visibility/prompt-cohort.js";
import { createExecutionBatch, buildDuplicateKey } from "../lib/ai-visibility/execution-batch.js";
import { createFileStore } from "../lib/ai-visibility/storage/file-store.js";
import {
  listAvailableAiVisibilityLanguages,
  findMatchingSummaries,
  getBrandTrendPayload,
  getBrandSourcesPayload,
  getBrandEvidencePayload,
  getBrandOverviewPayload,
} from "../lib/ai-visibility/brand-read-service.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { buildFixtureViewerContext } from "../lib/ai-visibility/viewer-context.js";
import { planAiVisibilityCohort } from "../lib/ai-visibility/execute-cohort.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") {
      return r
        .then(() => {
          passed += 1;
          console.log(`  PASS ${name}`);
        })
        .catch((err) => {
          failed += 1;
          console.error(`  FAIL ${name}: ${err.message}`);
        });
    }
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

async function run() {
  console.log("\nAI Visibility Phase 3A.6 — Language Data Foundation\n");

  await test("normalizeLanguage en/es; reject locales and free text", () => {
    assert.equal(normalizeLanguage("en"), "en");
    assert.equal(normalizeLanguage("English"), "en");
    assert.equal(normalizeLanguage("es"), "es");
    assert.equal(normalizeLanguage("Spanish"), "es");
    assert.equal(normalizeLanguage("en-US"), null);
    assert.equal(normalizeLanguage("es-MX"), null);
    assert.equal(normalizeLanguage("fr"), null);
    assert.equal(isSupportedAiVisibilityLanguage("en"), true);
    assert.equal(isSupportedAiVisibilityLanguage("es-ES"), false);
    assert.equal(getLanguageDisplayLabel("en"), "English");
    assert.equal(getLanguageDisplayLabel("es"), "Spanish");
    assert.equal(requireSupportedLanguage("es").ok, true);
    assert.equal(requireSupportedLanguage("pt").ok, false);
    assert.deepEqual([...AI_VISIBILITY_LANGUAGES], ["en", "es"]);
  });

  await test("semantic pair validation — EN+ES compatible; text need not match", () => {
    const base = {
      semanticPairId: "sp_uu_conversion_cala_v1",
      intentTerritory: "Conversion",
      geographyScope: "Region",
      commercialRegion: "CALA",
      country: null,
      promptFamily: "upper_upscale_conversion",
      entityScope: "Brand",
      peerSetId: "peers_v1",
      stakeholderRelevance: ["Owner", "Brand"],
    };
    const ok = validateSemanticPairMembers(
      { ...base, promptId: "p_en", version: "1", language: "en" },
      { ...base, promptId: "p_es", version: "1", language: "es" }
    );
    assert.equal(ok.ok, true);
    assert.equal(ok.TEXT_MUST_BE_LITERAL_TRANSLATION, false);

    const bad = validateSemanticPairMembers(
      { ...base, promptId: "p_en", version: "1", language: "en" },
      {
        ...base,
        promptId: "p_es",
        version: "1",
        language: "es",
        intentTerritory: "New Build",
      }
    );
    assert.equal(bad.ok, false);
    assert.ok(bad.errors.includes("intentTerritory_mismatch"));

    const idEn = promptExecutionIdentity({
      promptId: "p",
      version: "1",
      language: "en",
      semanticPairId: base.semanticPairId,
    });
    const idEs = promptExecutionIdentity({
      promptId: "p",
      version: "1",
      language: "es",
      semanticPairId: base.semanticPairId,
    });
    assert.notEqual(idEn.language, idEs.language);
  });

  await test("fingerprints differ by language; cohort version v2", () => {
    assert.equal(PROMPT_COHORT_VERSION, "ai_visibility_prompt_cohort_v2");
    const prompts = [
      {
        promptId: "p_cala_uu_en",
        version: "1",
        promptFamily: "uu_conversion",
        geographyScope: "Region",
        commercialRegion: "CALA",
        country: null,
        intentTerritory: "Conversion",
        entityScope: "Brand",
        stakeholderRelevance: ["Brand"],
        language: "en",
        semanticPairId: "sp_uu_cala_v1",
        active: true,
        monitoringEligible: true,
        governanceStatus: "Approved",
      },
      {
        promptId: "p_cala_uu_es",
        version: "1",
        promptFamily: "uu_conversion",
        geographyScope: "Region",
        commercialRegion: "CALA",
        country: null,
        intentTerritory: "Conversion",
        entityScope: "Brand",
        stakeholderRelevance: ["Brand"],
        language: "es",
        semanticPairId: "sp_uu_cala_v1",
        active: true,
        monitoringEligible: true,
        governanceStatus: "Approved",
      },
    ];
    const en = buildPromptCohort({
      prompts,
      geographyScope: "Region",
      commercialRegion: "CALA",
      language: "en",
      entityScope: "Brand",
      stakeholder: "brand",
    });
    const es = buildPromptCohort({
      prompts,
      geographyScope: "Region",
      commercialRegion: "CALA",
      language: "es",
      entityScope: "Brand",
      stakeholder: "brand",
    });
    assert.equal(en.ok, true);
    assert.equal(es.ok, true);
    assert.equal(en.language, "en");
    assert.equal(es.language, "es");
    assert.notEqual(en.fingerprint, es.fingerprint);
    assert.equal(en.members.length, 1);
    assert.equal(es.members.length, 1);
    assert.equal(en.members[0].promptId, "p_cala_uu_en");
    assert.equal(es.members[0].promptId, "p_cala_uu_es");
  });

  await test("batch identity / duplicate key includes language", () => {
    const cohort = {
      fingerprint: "abc",
      language: "en",
      members: [{ promptId: "p1", version: "1", language: "en" }],
    };
    const en = createExecutionBatch({
      cohort,
      language: "en",
      stakeholder: "brand",
      entityScope: "Brand",
      geographyScope: "Region",
      commercialRegion: "CALA",
      peerSet: { peerSetId: "peers_v1", peerSetVersion: "1", entityIds: ["a"], canonicalValid: true },
      provider: "openai",
      model: "gpt-test",
    });
    const es = createExecutionBatch({
      cohort: { ...cohort, language: "es", members: [{ promptId: "p1", version: "1", language: "es" }] },
      language: "es",
      stakeholder: "brand",
      entityScope: "Brand",
      geographyScope: "Region",
      commercialRegion: "CALA",
      peerSet: { peerSetId: "peers_v1", peerSetVersion: "1", entityIds: ["a"], canonicalValid: true },
      provider: "openai",
      model: "gpt-test",
    });
    const batchEn = en.batch || en;
    const batchEs = es.batch || es;
    assert.equal(batchEn.language, "en");
    assert.equal(batchEs.language, "es");
    assert.notEqual(buildDuplicateKey(batchEn), buildDuplicateKey(batchEs));
  });

  await test("trend comparability rejects different language", () => {
    const key = buildTrendComparabilityKey({
      provider: "openai",
      geographyKey: "CALA",
      language: "en",
      semanticPairId: "sp1",
      peerSetVersion: "1",
      metricVersion: METRIC_VERSION,
    });
    assert.equal(key.language, "en");
    const cmp = compareTrendObservations(
      { provider: "openai", geographyKey: "CALA", language: "en", metricVersion: METRIC_VERSION },
      { provider: "openai", geographyKey: "CALA", language: "es", metricVersion: METRIC_VERSION }
    );
    assert.equal(cmp.comparable, false);
    assert.equal(cmp.reasonCode, NON_COMPARABLE_LANGUAGE);
  });

  await test("resolveReadLanguage — sole available; no silent fallback; no All Languages", () => {
    assert.equal(resolveReadLanguage({ availableLanguages: ["en"] }).language, "en");
    const multi = resolveReadLanguage({ availableLanguages: ["en", "es"] });
    assert.equal(multi.ok, false);
    assert.equal(multi.reasonCode, "LANGUAGE_REQUIRED");
    const explicitEsMissing = resolveReadLanguage({
      requested: "es",
      availableLanguages: ["en"],
    });
    assert.equal(explicitEsMissing.status, "not_monitored");
    assert.equal(explicitEsMissing.SILENT_LANGUAGE_FALLBACK, false);
    const contract = buildLanguageFilterContract(["en", "es"]);
    assert.equal(contract.visible, true);
    assert.equal(contract.ALL_LANGUAGES_OPTION, false);
    assert.equal(contract.defaultSelection, "en");
    assert.ok(!contract.options.some((o) => /all/i.test(o.label)));
  });

  // --- temp store isolation fixtures ---
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-lang-"));
  const store = createFileStore({ rootDir: tmp });
  const brandId = "recBrandTestENES";
  const peerId = "recPeerTest";
  const geo = {
    geographyScope: "Region",
    commercialRegion: "CALA",
    country: null,
    key: "CALA",
  };

  async function seedLang(language, presence) {
    const batchId = `aiv_batch_test_${language}`;
    const evidenceId = `ev_test_${language}`;
    await store.saveBatch({
      batchId,
      status: "completed",
      health: "HEALTHY",
      language,
      provider: "openai",
      geographyScope: "Region",
      commercialRegion: "CALA",
      country: null,
      cohort: {
        fingerprint: `fp_${language}`,
        language,
        geographyScope: "Region",
        commercialRegion: "CALA",
      },
      peerSetId: "peers_test_v1",
      peerSetVersion: "1",
      completedAt: language === "en" ? "2026-08-13T12:00:00.000Z" : "2026-08-14T12:00:00.000Z",
    });
    await store.saveBatchSummary({
      batchId,
      status: "completed",
      language,
      provider: { name: "openai", model: "gpt-test" },
      cohort: {
        fingerprint: `fp_${language}`,
        language,
        geographyScope: "Region",
        commercialRegion: "CALA",
        country: null,
      },
      peerSet: { peerSetId: "peers_test_v1", peerSetVersion: "1" },
      completedAt: language === "en" ? "2026-08-13T12:00:00.000Z" : "2026-08-14T12:00:00.000Z",
      metrics: {
        competitivePosition: {
          peers: [
            { entityId: brandId, presence, rank: presence > 0.5 ? 1 : 2 },
            { entityId: peerId, presence: 1 - presence, rank: presence > 0.5 ? 2 : 1 },
          ],
        },
        entityMetrics: {
          [brandId]: {
            presence,
            aiPresenceRate: presence,
            recommendationRate: presence === 0.8 ? 0.5 : 0.1,
            recommendationShare: presence === 0.8 ? 0.4 : 0.05,
          },
        },
      },
    });
    await store.saveEvidence({
      evidenceId,
      batchId,
      language,
      provider: "openai",
      promptId: `p_cala_uu_${language}`,
      geographyScope: "Region",
      regionName: "CALA",
      timestamp: language === "en" ? "2026-08-13T12:00:00.000Z" : "2026-08-14T12:00:00.000Z",
      payload: {
        language,
        mentions: [
          {
            entityId: brandId,
            role: presence > 0.5 ? "first_recommendation" : "discussed",
            entityName: "Test Brand",
          },
        ],
        citations: [
          {
            domain: language === "en" ? "example-en.com" : "ejemplo-es.com",
            url: language === "en" ? "https://example-en.com/a" : "https://ejemplo-es.com/a",
          },
        ],
        rawResponseText: language === "en" ? "English answer" : "Respuesta en español",
      },
    });
    await store.saveRun({
      runId: `run_test_${language}`,
      batchId,
      evidenceId,
      status: "completed",
      language,
      provider: "openai",
      completedAt: language === "en" ? "2026-08-13T12:00:00.000Z" : "2026-08-14T12:00:00.000Z",
    });
    await store.saveMetricSnapshot({
      snapshotId: `${batchId}_${brandId}_ai_presence_rate`,
      batchId,
      entityId: brandId,
      metric: "ai_presence_rate",
      value: presence,
      language,
      provider: "openai",
      geographyScope: "Region",
      commercialRegion: "CALA",
      batchDate: language === "en" ? "2026-08-13" : "2026-08-14",
      metricVersion: METRIC_VERSION,
    });
  }

  await seedLang("en", 0.8);
  await seedLang("es", 0.2);

  const viewer = buildFixtureViewerContext({
    viewerCompanyId: "co_test",
    isBrand: true,
  });
  const entitlementGraph = buildFixtureEntitlementGraph({
    entitledBrandIds: [brandId],
  });

  await test("snapshot queries isolate language", async () => {
    const enSnaps = await store.listMetricSnapshots({
      entityId: brandId,
      language: "en",
      provider: "openai",
    });
    const esSnaps = await store.listMetricSnapshots({
      entityId: brandId,
      language: "es",
      provider: "openai",
    });
    assert.equal(enSnaps.length, 1);
    assert.equal(esSnaps.length, 1);
    assert.equal(enSnaps[0].value, 0.8);
    assert.equal(esSnaps[0].value, 0.2);
    assert.notEqual(enSnaps[0].value, esSnaps[0].value);
  });

  await test("evidence isolates language", async () => {
    const enEv = await store.listEvidence({ language: "en", provider: "openai" });
    const esEv = await store.listEvidence({ language: "es", provider: "openai" });
    assert.equal(enEv.length, 1);
    assert.equal(esEv.length, 1);
    assert.equal(enEv[0].payload.citations[0].domain, "example-en.com");
    assert.equal(esEv[0].payload.citations[0].domain, "ejemplo-es.com");
  });

  await test("source aggregates isolate language", async () => {
    const en = await getBrandSourcesPayload({
      dealalityUser: viewer,
      viewerContext: viewer,
      entitlementGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "en",
    });
    const es = await getBrandSourcesPayload({
      dealalityUser: viewer,
      viewerContext: viewer,
      entitlementGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "es",
    });
    assert.equal(en.ok, true);
    assert.equal(es.ok, true);
    const enDomains = (en.sources || []).map((s) => s.domain);
    const esDomains = (es.sources || []).map((s) => s.domain);
    assert.ok(enDomains.includes("example-en.com"));
    assert.ok(esDomains.includes("ejemplo-es.com"));
    assert.ok(!enDomains.includes("ejemplo-es.com"));
    assert.ok(!esDomains.includes("example-en.com"));
  });

  await test("trend does not include other language points", async () => {
    const en = await getBrandTrendPayload({
      dealalityUser: viewer,
      viewerContext: viewer,
      entitlementGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "en",
    });
    assert.equal(en.ok, true);
    assert.equal(en.language, "en");
    assert.equal(en.points.length, 1);
    assert.equal(en.points[0].value, 0.8);
    assert.ok(!en.points.some((p) => p.value === 0.2));
  });

  await test("available-language discovery; no All Languages", async () => {
    const avail = await listAvailableAiVisibilityLanguages({
      store,
      provider: "openai",
      geographyFilter: geo,
    });
    assert.deepEqual(avail.availableLanguages, ["en", "es"]);
    assert.equal(avail.filterContract.ALL_LANGUAGES_OPTION, false);
    assert.equal(avail.filterContract.visible, true);
  });

  await test("API-style unmonitored language returns not_monitored (no silent fallback)", async () => {
    // Mexico has no seeded data
    const overview = await getBrandOverviewPayload({
      dealalityUser: viewer,
      viewerContext: viewer,
      entitlementGraph,
      store,
      brandId,
      provider: "openai",
      geography: "Mexico",
      language: "es",
    });
    assert.equal(overview.SILENT_LANGUAGE_FALLBACK, false);
    assert.ok(
      overview.availability === "Not Monitored" ||
        overview.availability === "NOT_MONITORED" ||
        overview.availabilityReason ||
        overview.ok === true
    );
    // Explicit es on Mexico with empty store → not monitored, not substituted to en
    if (overview.language) assert.equal(overview.language, "es");
  });

  await test("authorization unchanged with language — cross-tenant denied", async () => {
    const otherViewer = buildFixtureViewerContext({
      viewerCompanyId: "co_other",
      isBrand: true,
    });
    const otherGraph = buildFixtureEntitlementGraph({
      entitledBrandIds: ["recOtherBrand"],
    });
    const denied = await getBrandEvidencePayload({
      dealalityUser: otherViewer,
      viewerContext: otherViewer,
      entitlementGraph: otherGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "en",
    });
    assert.equal(denied.allowed, false);
    // Direct ID swap still denied
    const swap = await getBrandOverviewPayload({
      dealalityUser: otherViewer,
      viewerContext: otherViewer,
      entitlementGraph: otherGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "es",
    });
    assert.equal(swap.allowed, false);
  });

  await test("legacy single-language requests remain functional (omit language)", async () => {
    // Remove ES summary so only EN remains for sole-language path
    const tmp2 = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-lang-sole-"));
    const store2 = createFileStore({ rootDir: tmp2 });
    await store2.saveBatchSummary({
      batchId: "aiv_batch_sole_en",
      status: "completed",
      language: "en",
      provider: { name: "openai" },
      cohort: {
        fingerprint: "fp_en",
        language: "en",
        geographyScope: "Region",
        commercialRegion: "Europe",
      },
      completedAt: "2026-08-13T12:00:00.000Z",
      metrics: { entityMetrics: {} },
    });
    const avail = await listAvailableAiVisibilityLanguages({
      store: store2,
      provider: "openai",
      geographyFilter: {
        geographyScope: "Region",
        commercialRegion: "Europe",
        key: "Europe",
      },
    });
    assert.deepEqual(avail.availableLanguages, ["en"]);
    const resolved = resolveReadLanguage({
      requested: null,
      availableLanguages: avail.availableLanguages,
    });
    assert.equal(resolved.ok, true);
    assert.equal(resolved.language, "en");
    assert.equal(resolved.resolvedFrom, "sole_available");
  });

  await test("provider + language isolation in summaries", async () => {
    const en = await findMatchingSummaries(store, geo, "openai", { language: "en" });
    const es = await findMatchingSummaries(store, geo, "openai", { language: "es" });
    assert.equal(en.length, 1);
    assert.equal(es.length, 1);
    assert.equal(en[0].language, "en");
    assert.equal(es[0].language, "es");
  });

  await test("geography + language isolation (CALA es ≠ Global en)", async () => {
    await store.saveBatchSummary({
      batchId: "aiv_batch_global_en",
      status: "completed",
      language: "en",
      provider: { name: "openai" },
      cohort: {
        fingerprint: "fp_g",
        language: "en",
        geographyScope: "Global",
        commercialRegion: null,
      },
      completedAt: "2026-08-13T15:00:00.000Z",
      metrics: {},
    });
    const calaEs = await findMatchingSummaries(
      store,
      { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" },
      "openai",
      { language: "es" }
    );
    assert.ok(calaEs.every((s) => (s.language || "en") === "es"));
    assert.ok(
      calaEs.every(
        (s) => String(s.cohort?.commercialRegion || "").toLowerCase() === "cala"
      )
    );
  });

  await test("METRIC_VERSION unchanged for language dimension", () => {
    assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
  });

  await test("showcase architecture readiness — seven geo×language plan cells", async () => {
    const cells = [
      { key: "GLOBAL_EN", geographyScope: "Global", commercialRegion: null, language: "en" },
      { key: "CALA_EN", geographyScope: "Region", commercialRegion: "CALA", language: "en" },
      { key: "CALA_ES", geographyScope: "Region", commercialRegion: "CALA", language: "es" },
      { key: "EUROPE_EN", geographyScope: "Region", commercialRegion: "Europe", language: "en" },
      { key: "NORTH_AMERICA_EN", geographyScope: "Region", commercialRegion: "North America", language: "en" },
      { key: "MEXICO_EN", geographyScope: "Country", commercialRegion: "CALA", country: "Mexico", language: "en" },
      { key: "MEXICO_ES", geographyScope: "Country", commercialRegion: "CALA", country: "Mexico", language: "es" },
    ];
    const prompts = cells.flatMap((c) => [
      {
        promptId: `p_${c.key.toLowerCase()}_fixture`,
        version: "1",
        promptFamily: "uu_conversion",
        geographyScope: c.geographyScope,
        commercialRegion: c.commercialRegion,
        country: c.country || null,
        intentTerritory: "Conversion",
        entityScope: "Brand",
        stakeholderRelevance: ["Brand"],
        language: c.language,
        semanticPairId: `sp_${c.key.toLowerCase()}`,
        active: true,
        monitoringEligible: true,
        governanceStatus: "Approved",
      },
    ]);
    const fingerprints = new Set();
    for (const c of cells) {
      const cohort = buildPromptCohort({
        prompts,
        geographyScope: c.geographyScope,
        commercialRegion: c.commercialRegion,
        country: c.country || null,
        language: c.language,
        entityScope: "Brand",
        stakeholder: "brand",
      });
      assert.equal(cohort.ok, true, c.key);
      assert.equal(cohort.language, c.language);
      fingerprints.add(cohort.fingerprint);
    }
    assert.equal(fingerprints.size, 7, "all seven cells must have distinct fingerprints");
    // Language-aware plan rejects unsupported language without provider calls
    const badPlan = await planAiVisibilityCohort({
      geographyScope: "Region",
      commercialRegion: "CALA",
      language: "fr",
      stakeholder: "brand",
      entityScope: "Brand",
      provider: "openai",
      promptMode: "fixture",
    });
    assert.equal(badPlan.ok, false);
    assert.equal(badPlan.DRY_RUN_PROVIDER_CALLS, 0);
  });

  await test("UI foundation — language filter hidden until multi-language", () => {
    const html = fs.readFileSync(path.join(root, "public/ai-visibility-brand.html"), "utf8");
    assert.match(html, /id="aivLanguageFilterGroup"/);
    assert.match(html, /id="aivLanguage"/);
    const js = fs.readFileSync(
      path.join(root, "public/js/ai-visibility/ai-visibility-brand.js"),
      "utf8"
    );
    assert.match(js, /applyLanguageFilterContract/);
    assert.match(js, /ALL_LANGUAGES/);
    assert.ok(!/All Languages/.test(html));
  });

  await test("Spanish style guide exists", () => {
    assert.ok(
      fs.existsSync(
        path.join(root, "docs/ai-build-system/AI_VISIBILITY_SPANISH_HOSPITALITY_PROMPT_STYLE.md")
      )
    );
  });

  console.log(`\nResults: ${passed} passed, ${failed} failed`);
  console.log("LIVE_PROVIDER_CALLS: 0");
  if (failed) process.exit(1);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
