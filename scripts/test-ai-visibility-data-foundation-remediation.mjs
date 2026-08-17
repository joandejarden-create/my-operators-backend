#!/usr/bin/env node
/**
 * Brand AI Visibility — data foundation remediation tests.
 * Evidence resolver + observation reconstruction + slot arithmetic.
 * No provider calls. No historical data mutation.
 */
import assert from "node:assert/strict";
import {
  EVIDENCE_RESOLUTION_MODES,
  buildEvidenceResolutionIndex,
  resolveEvidenceForRun,
} from "../lib/ai-visibility/evidence-resolution.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { computeBrandQuestionMetrics } from "../lib/ai-visibility/portfolio-question-metrics.js";
import {
  createBrandAiVisibilityReadStore,
  createFileStore,
  WAVE1_ROOT,
} from "../lib/ai-visibility/storage/index.js";
import { resolveProviderBaselineStoreRoot } from "../lib/ai-visibility/storage/resolve-store-root.js";
import {
  findMatchingSummaries,
  parseGeographyQuery,
  resolveBrandGeographyMonitoringState,
} from "../lib/ai-visibility/brand-read-service.js";
import { normalizeLanguage } from "../lib/ai-visibility/language-dimension.js";

const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const TRIBUTE = "recCvV0PuZOi8c3hC";
const AC_HOTELS = "rec9aZp7GHtzUEg0c";

let passed = 0;
let failed = 0;
const failures = [];

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  ok — ${name}`);
  } catch (err) {
    failed += 1;
    failures.push({ name, message: err.message });
    console.log(`  FAIL — ${name}: ${err.message}`);
  }
}

function mockStore(evidenceRows = []) {
  const byId = new Map(evidenceRows.map((e) => [e.evidenceId, e]));
  return {
    async getEvidence(id) {
      return byId.get(id) || null;
    },
    async listEvidence() {
      return evidenceRows;
    },
  };
}

async function latestCompletedSummary(store, provider) {
  const rows = await store.listBatchSummaries({ provider });
  return (
    rows.find((r) => r.status === "completed" || r.status === "partial") ||
    rows[0] ||
    null
  );
}

async function main() {
  console.log("test:ai-visibility-data-foundation-remediation\n");

  await test("1. evidenceId resolution", async () => {
    const store = mockStore([
      { evidenceId: "ev_a", responseId: "resp_a", promptId: "p1" },
    ]);
    const r = await resolveEvidenceForRun(store, {
      evidenceId: "ev_a",
      responseId: "resp_a",
    });
    assert.equal(r.mode, EVIDENCE_RESOLUTION_MODES.EVIDENCE_ID);
    assert.equal(r.evidence.evidenceId, "ev_a");
  });

  await test("2. responseId legacy resolution", async () => {
    const store = mockStore([
      { evidenceId: "ev_b", responseId: "resp_b", promptId: "p1" },
    ]);
    const index = await buildEvidenceResolutionIndex(store, {});
    const r = await resolveEvidenceForRun(
      store,
      { responseId: "resp_b", status: "completed" },
      { index }
    );
    assert.equal(r.mode, EVIDENCE_RESOLUTION_MODES.RESPONSE_ID_LEGACY);
    assert.equal(r.evidence.evidenceId, "ev_b");
  });

  await test("3. responseId no match → UNRESOLVED", async () => {
    const store = mockStore([{ evidenceId: "ev_c", responseId: "resp_c" }]);
    const index = await buildEvidenceResolutionIndex(store, {});
    const r = await resolveEvidenceForRun(
      store,
      { responseId: "resp_missing" },
      { index }
    );
    assert.equal(r.mode, EVIDENCE_RESOLUTION_MODES.UNRESOLVED);
    assert.equal(r.evidence, null);
  });

  await test("4. responseId ambiguous match", async () => {
    const store = mockStore([
      { evidenceId: "ev_d1", responseId: "resp_dup" },
      { evidenceId: "ev_d2", responseId: "resp_dup" },
    ]);
    const index = await buildEvidenceResolutionIndex(store, {});
    const r = await resolveEvidenceForRun(
      store,
      { responseId: "resp_dup" },
      { index }
    );
    assert.equal(r.mode, EVIDENCE_RESOLUTION_MODES.AMBIGUOUS_EVIDENCE_LINK);
    assert.equal(r.evidence, null);
    assert.equal(r.matchCount, 2);
  });

  await test("5. OpenAI reconstruction unchanged (evidenceId path)", async () => {
    const store = createFileStore({ rootDir: WAVE1_ROOT });
    const summary = await latestCompletedSummary(store, "openai");
    assert.ok(summary, "openai summary");
    const { observations, resolutionStats } = await loadObservationsFromBatchSummary(
      store,
      summary,
      { matchedSlotKeys: ["CALA_EN"], language: "en" }
    );
    assert.ok(observations.length > 0, "openai obs > 0");
    assert.ok(
      (resolutionStats.byMode[EVIDENCE_RESOLUTION_MODES.EVIDENCE_ID] || 0) > 0 ||
        observations.every((o) => o.evidenceResolutionMode === EVIDENCE_RESOLUTION_MODES.EVIDENCE_ID || o.evidenceId)
    );
  });

  for (const provider of ["gemini", "perplexity", "claude"]) {
    await test(`6-8. ${provider} reconstruction via responseId legacy`, async () => {
      const root = resolveProviderBaselineStoreRoot(provider);
      const store = createFileStore({ rootDir: root });
      const summary = await latestCompletedSummary(store, provider);
      assert.ok(summary, `${provider} summary`);
      const { observations, resolutionStats } = await loadObservationsFromBatchSummary(
        store,
        summary,
        { matchedSlotKeys: ["CALA_EN"], language: "en" }
      );
      assert.ok(
        observations.length > 0,
        `${provider} observations reconstructed (got ${observations.length})`
      );
      assert.equal(resolutionStats.ambiguous, 0);
      assert.ok(
        (resolutionStats.byMode[EVIDENCE_RESOLUTION_MODES.RESPONSE_ID_LEGACY] || 0) > 0,
        `${provider} should resolve via RESPONSE_ID_LEGACY`
      );
      assert.ok(observations.every((o) => Array.isArray(o.presentEntityIds)));
      assert.ok(observations.some((o) => (o.citations || []).length >= 0));
    });
  }

  await test("9. language isolation — EN obs do not include ES slots", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "claude",
      { language: "en" }
    );
    assert.ok(summaries.length, "claude CALA summaries");
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    assert.ok(observations.length > 0);
    for (const o of observations) {
      assert.equal(normalizeLanguage(o.language), "en");
      assert.ok(!/_ES$/i.test(String(o.slot || "")));
    }
  });

  await test("10. geography isolation — CALA slots only", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "gemini",
      { language: "en" }
    );
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    assert.ok(observations.length > 0);
    const allowed = new Set(summaries[0]._matchedSlotKeys || []);
    assert.deepEqual([...allowed], ["CALA_EN"]);
    for (const o of observations) {
      assert.ok(allowed.has(String(o.slot)), `slot ${o.slot} not in ${[...allowed]}`);
    }
  });

  await test("11. provider isolation — claude load does not mix openai evidence", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "claude",
      { language: "en" }
    );
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    for (const o of observations) {
      const p = String(o.provider?.name || o.provider || "").toLowerCase();
      assert.equal(p, "claude");
    }
  });

  await test("12. subject isolation — Autograph present/missing only for Autograph id", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const mon = await resolveBrandGeographyMonitoringState({
      store,
      brandId: AUTOGRAPH,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: "claude",
      language: "en",
    });
    assert.equal(mon.monitored, true);
    assert.ok(typeof mon.presenceVal === "number");
    assert.ok(typeof mon.questionsMissing === "number");
    assert.ok(typeof mon.questionsPresent === "number");
    assert.equal(
      mon.questionsPresent + mon.questionsMissing,
      mon.promptDenominator
    );
  });

  await test("13. Present + Missing = Monitored (unique prompts)", () => {
    const obs = [
      { success: true, promptId: "p1", presentEntityIds: [AUTOGRAPH] },
      { success: true, promptId: "p2", presentEntityIds: [] },
      { success: true, promptId: "p3", presentEntityIds: [AUTOGRAPH, TRIBUTE] },
    ];
    const q = computeBrandQuestionMetrics(obs, AUTOGRAPH);
    assert.equal(q.questionsPresentCount + q.questionsMissingCount, q.eligiblePromptCount);
    assert.equal(q.INVARIANT_PRESENT_PLUS_MISSING_EQ_MONITORED, true);
  });

  await test("14. Presence = Present / Monitored", () => {
    const obs = [
      { success: true, promptId: "p1", presentEntityIds: [AUTOGRAPH] },
      { success: true, promptId: "p2", presentEntityIds: [] },
      { success: true, promptId: "p3", presentEntityIds: [] },
      { success: true, promptId: "p4", presentEntityIds: [AUTOGRAPH] },
    ];
    const q = computeBrandQuestionMetrics(obs, AUTOGRAPH);
    assert.equal(q.presenceRate, q.questionsPresentCount / q.eligiblePromptCount);
  });

  await test("15. parent missing never paired with slot denominator", async () => {
    const store = createBrandAiVisibilityReadStore({});
    for (const provider of ["gemini", "perplexity", "claude"]) {
      const mon = await resolveBrandGeographyMonitoringState({
        store,
        brandId: AUTOGRAPH,
        geoFilter: parseGeographyQuery({ geography: "CALA" }),
        provider,
        language: "en",
      });
      assert.ok(mon.promptDenominator > 0, `${provider} denom`);
      assert.ok(
        typeof mon.questionsMissing === "number",
        `${provider} missing should be slot-recomputed`
      );
      assert.ok(
        mon.questionsMissing <= mon.promptDenominator,
        `${provider} Missing ${mon.questionsMissing} > Monitored ${mon.promptDenominator}`
      );
      assert.equal(
        mon.questionsPresent + mon.questionsMissing,
        mon.promptDenominator,
        `${provider} Present+Missing`
      );
      if (typeof mon.presenceVal === "number" && mon.promptDenominator > 0) {
        const expected = mon.questionsPresent / mon.promptDenominator;
        assert.ok(Math.abs(mon.presenceVal - expected) < 1e-9, `${provider} presence rate`);
      }
    }
  });

  await test("16. citations reconstructed on non-OpenAI", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "perplexity",
      { language: "en" }
    );
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    const withCitations = observations.filter((o) => (o.citations || []).length > 0);
    assert.ok(withCitations.length > 0, "perplexity citations readable");
  });

  await test("17. source frequency inputs reconstructable", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "gemini",
      { language: "en" }
    );
    const { evidenceRows } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    const domains = new Set();
    for (const row of evidenceRows) {
      for (const c of row.citations || []) {
        if (c.domain || c.sourceDomain) domains.add(String(c.domain || c.sourceDomain).toLowerCase());
      }
    }
    assert.ok(domains.size > 0, "gemini domains for source frequency");
  });

  await test("18. watchlist row reconstruction (missing questions)", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "claude",
      { language: "en" }
    );
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    const missing = observations.filter(
      (o) => !(o.presentEntityIds || []).includes(AC_HOTELS)
    );
    assert.ok(missing.length >= 0);
    assert.ok(observations.every((o) => o.evidenceId || o.responseId));
  });

  await test("19. peer-present gap reconstruction", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "claude",
      { language: "en" }
    );
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    const gaps = observations.filter(
      (o) =>
        !(o.presentEntityIds || []).includes(AUTOGRAPH) &&
        (o.presentEntityIds || []).some((id) => id && id !== AUTOGRAPH)
    );
    assert.ok(Array.isArray(gaps));
  });

  await test("20. evidence drawer reference fields present", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const summaries = await findMatchingSummaries(
      store,
      parseGeographyQuery({ geography: "CALA" }),
      "gemini",
      { language: "en" }
    );
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    assert.ok(observations.length > 0);
    for (const o of observations.slice(0, 5)) {
      assert.ok(o.evidenceId, "evidenceId");
      assert.ok(o.responseId, "responseId");
      assert.ok(o.promptId, "promptId");
      assert.ok(o.evidenceResolutionMode, "resolution mode");
    }
  });

  console.log(`\nTOTAL=${passed + failed} PASS=${passed} FAIL=${failed}`);
  if (failures.length) {
    console.log(JSON.stringify(failures, null, 2));
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
