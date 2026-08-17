#!/usr/bin/env node
/**
 * Phase 3B.4 tests — model finalization, Claude tool, Evidence Footprint, integrity.
 */
import assert from "node:assert/strict";
import {
  GEMINI_MODEL_PROBE_ORDER,
  FAILED_GEMINI_MODEL_HISTORY,
  auditGeminiRequestCompatibility,
  finalizeGeminiProductionModel,
} from "../lib/ai-visibility/providers/gemini-model-probe.js";
import { finalizeClaudeWebSearchTool } from "../lib/ai-visibility/providers/claude-tool-audit.js";
import { buildClaudeVisibilityRequest } from "../lib/ai-visibility/providers/claude.js";
import {
  buildEvidenceFootprint,
  countBrandMentions,
  countRecommendationMentions,
  isEvidenceBearingResponse,
  resolveEvidenceAssociationLevel,
  EVIDENCE_ASSOCIATION_LEVEL,
} from "../lib/ai-visibility/evidence-footprint.js";
import {
  buildCitedSourceIntelligence,
  buildMatchedPromptGroups,
  TOP_CITED_SOURCE_SORT,
  LONGITUDINAL_SOURCE_STATUS,
} from "../lib/ai-visibility/cited-source-intelligence.js";
import {
  BASELINE_COMPLETENESS,
  resolveBaselineCompleteness,
} from "../lib/ai-visibility/provider-baseline-state.js";
import { MONITORING_RUN_PURPOSE, isBaselineMonitoringRun } from "../lib/ai-visibility/monitoring-run-purpose.js";
import { CLAUDE_RETRY_POLICY } from "../lib/ai-visibility/providers/provider-retry-policy.js";
import { PHASE_3B4_ORCHESTRATOR_VERSION } from "../lib/ai-visibility/phase3b4-orchestrator.js";

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

console.log("\nAI Visibility Phase 3B.4 — Four-Provider Baseline Completion\n");

test("gemini probe order — 3.6 first, preview fallback only", () => {
  assert.equal(GEMINI_MODEL_PROBE_ORDER[0], "gemini-3.6-flash");
  assert.equal(GEMINI_MODEL_PROBE_ORDER[1], "gemini-3-flash-preview");
  assert.equal(GEMINI_MODEL_PROBE_ORDER.length, 2);
  assert.ok(FAILED_GEMINI_MODEL_HISTORY.includes("gemini-2.5-flash"));
});

test("gemini request — no deprecated generation params", () => {
  const audit = auditGeminiRequestCompatibility("gemini-3.6-flash");
  assert.equal(audit.GEMINI_REQUEST_MIGRATION_REQUIRED, "NO");
  assert.equal(audit.SEMANTIC_PROMPT_CHANGED, "NO");
  assert.equal(audit.hasGoogleSearchTool, true);
});

test("gemini finalize — no silent third model", async () => {
  const mock = async ({ model }) => {
    if (model === "gemini-3.6-flash") {
      return {
        provider: "gemini",
        model,
        text: "OK",
        citations: [{ url: "https://example.com", domain: "example.com" }],
        searchResults: [{ url: "https://example.com" }],
        searchMetadata: { webSearchUsed: true },
        usage: { inputTokens: 10, outputTokens: 5 },
        raw: { candidates: [{ groundingMetadata: { groundingChunks: [{}] } }] },
      };
    }
    throw new Error("should_not_probe_preview_when_preferred_passes");
  };
  const out = await finalizeGeminiProductionModel({ runVisibilityPrompt: mock });
  assert.equal(out.SELECTED_GEMINI_BASELINE_MODEL, "gemini-3.6-flash");
  assert.equal(out.FAILED_MODEL_HISTORY_PRESERVED, true);
});

test("claude tool — keep 20260209 + direct callers", () => {
  const audit = finalizeClaudeWebSearchTool();
  assert.equal(audit.SELECTED_TOOL_VERSION, "web_search_20260209");
  assert.deepEqual(audit.SELECTED_ALLOWED_CALLERS, ["direct"]);
  assert.equal(audit.DYNAMIC_FILTERING_ENABLED, false);
  assert.equal(audit.MAX_USES, 5);
  assert.equal(audit.CHANGE_REQUIRED, false);
  assert.equal(audit.REQUEST_HAS_ALLOWED_CALLERS, true);
});

test("claude timeout — 300000 default", () => {
  assert.equal(CLAUDE_RETRY_POLICY.timeoutMsDefault, 300000);
  const built = buildClaudeVisibilityRequest({
    prompt: { text: "OK", promptId: "t" },
  });
  assert.ok(built.timeoutMs >= 300000 || built.timeoutMs === Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS));
  assert.deepEqual(built.body.tools[0].allowed_callers, ["direct"]);
});

test("evidence footprint — deterministic, no composite", () => {
  const mentions = [
    { entityId: "b1", role: "mentioned" },
    { entityId: "b1", role: "recommended", explicitRecommendation: true },
  ];
  assert.equal(countBrandMentions(mentions, { entityId: "b1" }), 2);
  assert.equal(countRecommendationMentions(mentions, { entityId: "b1" }), 1);
  assert.equal(isEvidenceBearingResponse({ citations: [{ url: "https://a.com" }] }), true);
  const fp = buildEvidenceFootprint([
    {
      responseId: "r1",
      mentions,
      citations: [{ url: "https://a.com", domain: "a.com" }],
    },
    {
      responseId: "r2",
      mentions: [],
      citations: [{ url: "https://a.com", domain: "a.com" }],
    },
  ]);
  assert.equal(fp.COMPOSITE_SCORE, null);
  assert.equal(fp.READY, true);
  assert.equal(fp.UNIQUE_CITED_SOURCES.uniqueDomains, 1);
  assert.equal(fp.REPEATED_ACROSS_MONITORED_RESPONSES[0].distinctResponses, 2);
});

test("evidence association levels — provider-specific", () => {
  assert.equal(
    resolveEvidenceAssociationLevel("openai"),
    EVIDENCE_ASSOCIATION_LEVEL.DIRECT_CITATION_ASSOCIATION
  );
  assert.equal(
    resolveEvidenceAssociationLevel("perplexity"),
    EVIDENCE_ASSOCIATION_LEVEL.RESPONSE_LEVEL_ASSOCIATION
  );
  assert.equal(
    resolveEvidenceAssociationLevel("gemini"),
    EVIDENCE_ASSOCIATION_LEVEL.RESPONSE_LEVEL_ASSOCIATION
  );
});

test("cited source intelligence — sort by distinct responses", () => {
  const intel = buildCitedSourceIntelligence([
    {
      responseId: "r1",
      promptId: "p1",
      intent: "Conversion",
      geographyKey: "CALA",
      language: "en",
      provider: "openai",
      citations: [{ url: "https://news.com/a", domain: "news.com" }],
    },
    {
      responseId: "r2",
      promptId: "p2",
      intent: "Conversion",
      geographyKey: "CALA",
      language: "en",
      provider: "openai",
      citations: [
        { url: "https://news.com/b", domain: "news.com" },
        { url: "https://news.com/c", domain: "news.com" },
      ],
    },
    {
      responseId: "r3",
      promptId: "p3",
      intent: "Lifestyle",
      geographyKey: "GLOBAL",
      language: "es",
      provider: "openai",
      citations: [
        { url: "https://other.com/x", domain: "other.com" },
        { url: "https://other.com/y", domain: "other.com" },
        { url: "https://other.com/z", domain: "other.com" },
      ],
    },
  ]);
  assert.equal(intel.TOP_CITED_SOURCES[0].domain, "news.com");
  assert.equal(intel.TOP_CITED_SOURCES[0].responsesAppearingIn, 2);
  assert.equal(TOP_CITED_SOURCE_SORT.primary, "distinct_monitored_responses");
  assert.equal(intel.OWNED_SOURCE_CLASSIFICATION_READY, "NO");
  assert.equal(LONGITUDINAL_SOURCE_STATUS.NEW_SOURCES, "NOT_YET_AVAILABLE");
  assert.equal(intel.ALL_AI_SOURCES, "NOT_IMPLEMENTED");
});

test("matched groups foundation — no consensus", () => {
  const m = buildMatchedPromptGroups({
    openai: [{ promptId: "p1", language: "en", geographyKey: "GLOBAL", intent: "Conversion" }],
    perplexity: [{ promptId: "p1", language: "en", geographyKey: "GLOBAL", intent: "Conversion" }],
  });
  assert.equal(m.FULLY_MATCHED_PROMPT_COUNT, 1);
  assert.equal(m.CONSENSUS_METRICS, "NOT_IMPLEMENTED");
});

test("baseline states — OpenAI/Perplexity full remain", () => {
  assert.equal(
    resolveBaselineCompleteness({
      monitoringRunPurpose: MONITORING_RUN_PURPOSE.BASELINE,
      SUCCEEDED: 84,
      status: "completed",
      baselineSeriesId: "aiv_wave1_perplexity_peer_v2_showcase_prompts_v1",
    }),
    BASELINE_COMPLETENESS.FULL_BASELINE
  );
  assert.equal(
    isBaselineMonitoringRun({ monitoringRunPurpose: "validation" }),
    false
  );
});

test("orchestrator version present", () => {
  assert.ok(PHASE_3B4_ORCHESTRATOR_VERSION.includes("3b4"));
});

await Promise.resolve();
console.log(`\nPhase 3B.4 tests: ${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exit(1);
