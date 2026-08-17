#!/usr/bin/env node
/**
 * Phase 3B.1 — Multi-provider adapter foundation tests.
 * No live provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildWave1ShowcaseDryRunPlan,
  buildWave1ExecutionFingerprint,
  WAVE1_PEER_SET_ID,
} from "../lib/ai-visibility/wave1-showcase-plan.js";
import {
  listRegisteredProviders,
  getProviderAdapter,
  validateProviderAdapter,
  CANONICAL_PROVIDER_IDS,
  buildMultiProviderDryRunReport,
  validateFingerprintProviderIsolation,
  auditSemanticPromptParity,
  findSampleExecution,
  buildGeminiVisibilityRequest,
  buildPerplexityVisibilityRequest,
  buildClaudeVisibilityRequest,
  buildOpenAiVisibilityRequest,
  geminiProvider,
  perplexityProvider,
  claudeProvider,
  openaiProvider,
  normalizeProviderCitation,
  listNormalizedCitationContractFields,
  NORMALIZED_PROVIDER_RESPONSE_VERSION_V1_1,
  listNormalizedProviderContractFields,
  classifyProviderError,
  PROVIDER_ERROR_CATEGORIES,
  RECOMMENDED_PERPLEXITY_EXECUTION_MODE,
  GEMINI_EVIDENCE_READY,
  PERPLEXITY_EVIDENCE_READY,
  CLAUDE_EVIDENCE_READY,
  buildControlledValidationPlan,
  CONTROLLED_VALIDATION_PROMPT_IDS,
  CITATION_RATE_COMPATIBILITY,
  CROSS_PROVIDER_SIGNAL_FOUNDATION_READY,
  auditRawTextRepair,
} from "../lib/ai-visibility/providers/index.js";
import { listAvailableAiVisibilityProviders } from "../lib/ai-visibility/provider-dimension.js";
import { ProviderError } from "../lib/ai-visibility/providers/base-provider.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";
import { createAiVisibilityStore } from "../lib/ai-visibility/storage/file-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const fixtures = path.join(root, "fixtures/ai-visibility");

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3B.1 — Multi-Provider Adapter Foundation\n");

const plan = buildWave1ShowcaseDryRunPlan();

test("provider registration — openai, gemini, perplexity, claude", () => {
  assert.deepEqual(listRegisteredProviders(), CANONICAL_PROVIDER_IDS);
  for (const id of CANONICAL_PROVIDER_IDS) {
    assert.ok(getProviderAdapter(id), id);
  }
});

test("adapter interface — all providers implement required contract", () => {
  for (const provider of [openaiProvider, geminiProvider, perplexityProvider, claudeProvider]) {
    const v = validateProviderAdapter(provider);
    assert.equal(v.ok, true, `${provider.id}: ${v.errors.join(", ")}`);
  }
});

test("84 Gemini + 84 Perplexity + 84 Claude requests build without calls", () => {
  const report = buildMultiProviderDryRunReport();
  assert.equal(report.GEMINI_BUILDABLE, 84);
  assert.equal(report.PERPLEXITY_BUILDABLE, 84);
  assert.equal(report.CLAUDE_BUILDABLE, 84);
  assert.equal(report.TOTAL_BUILDABLE, 252);
  assert.equal(report.FAILURES.length, 0);
  assert.equal(report.LIVE_PROVIDER_CALLS, 0);
});

test("fingerprints — provider isolation, no collision (Mexico ES Conversion framing A)", () => {
  const exec = findSampleExecution(plan, {
    country: "MEXICO",
    language: "es",
    intent: "Conversion",
    promptFamily: "showcase_conversion_existing_asset_reposition",
  });
  assert.equal(exec.promptId, "p_mx_existing_asset_reposition_es_v1");
  const iso = validateFingerprintProviderIsolation(exec);
  assert.equal(iso.valid, true, iso.collision || "");
  assert.equal(iso.samples.length, 4);
  const fps = iso.samples.map((s) => s.fingerprint);
  assert.equal(new Set(fps).size, 4);
});

test("language EN/ES unchanged in built requests", () => {
  const esExec = plan.EXECUTIONS.find((e) => e.language === "es");
  const enExec = plan.EXECUTIONS.find((e) => e.language === "en");
  for (const builder of [
    buildGeminiVisibilityRequest,
    buildPerplexityVisibilityRequest,
    buildClaudeVisibilityRequest,
  ]) {
    const es = builder({ prompt: { text: esExec.promptText } });
    const en = builder({ prompt: { text: enExec.promptText } });
    assert.ok(es.body.contents?.[0]?.parts?.[0]?.text || es.body.messages?.[0]?.content);
    assert.notEqual(
      es.body.contents?.[0]?.parts?.[0]?.text || es.body.messages?.[0]?.content,
      en.body.messages?.[0]?.content || en.body.contents?.[0]?.parts?.[0]?.text
    );
  }
});

test("peer v2 exact in fingerprints", () => {
  const fp = buildWave1ExecutionFingerprint({
    provider: "gemini",
    promptId: "x",
    promptVersion: "1",
    geographyKey: "CALA",
    language: "en",
    intent: "Conversion",
    promptFamily: "f",
    peerSetId: WAVE1_PEER_SET_ID,
    peerSetVersion: "2",
    metricVersion: METRIC_VERSION,
  });
  assert.equal(fp.peerSetId, WAVE1_PEER_SET_ID);
  assert.equal(fp.peerSetVersion, "2");
});

test("84 prompts unchanged — semantic prompt parity YES", () => {
  const parity = auditSemanticPromptParity(plan);
  assert.equal(parity.SEMANTIC_PROMPT_PARITY, "YES");
  assert.equal(parity.GEMINI, "YES");
  assert.equal(parity.PERPLEXITY, "YES");
  assert.equal(parity.CLAUDE, "YES");
});

test("normalization — Gemini fixture → canonical normalized response", () => {
  const fixture = JSON.parse(
    fs.readFileSync(path.join(fixtures, "provider-gemini-showcase-fixture.json"), "utf8")
  );
  const norm = geminiProvider.normalizeResponse(fixture.raw, {
    promptId: "p_test",
    useV1_1: true,
    fingerprint: "fp_test",
  });
  assert.equal(norm.contractVersion, NORMALIZED_PROVIDER_RESPONSE_VERSION_V1_1);
  assert.equal(norm.provider, "gemini");
  assert.ok(norm.rawText.length > 0);
  assert.ok(norm.citations.length > 0);
  assert.ok(norm.searchMetadata);
  assert.equal(norm.raw, fixture.raw);
});

test("normalization — Perplexity fixture preserves raw evidence and normalized URLs", () => {
  const fixture = JSON.parse(
    fs.readFileSync(path.join(fixtures, "provider-perplexity-showcase-fixture.json"), "utf8")
  );
  const norm = perplexityProvider.normalizeResponse(fixture.raw, { useV1_1: true });
  assert.ok(norm.citations.length >= 2);
  assert.ok(norm.searchResults.length >= 2);
  for (const c of norm.citations) {
    const nc = normalizeProviderCitation(c, { provider: "perplexity" });
    assert.ok(nc.url);
    assert.ok(nc.normalizedUrl || nc.url);
    assert.equal(nc.startIndex, null);
    assert.equal(nc.endIndex, null);
  }
});

test("normalization — Claude fixture preserves citations and raw blocks", () => {
  const fixture = JSON.parse(
    fs.readFileSync(path.join(fixtures, "provider-claude-showcase-fixture.json"), "utf8")
  );
  const norm = claudeProvider.normalizeResponse(fixture.raw, { useV1_1: true });
  assert.ok(norm.rawText.length > 0);
  assert.ok(norm.citations.length > 0);
  assert.equal(norm.stopReason, "end_turn");
  assert.equal(norm.raw, fixture.raw);
});

test("missing provider fields — NULL/unavailable, never fabricated", () => {
  const fixture = JSON.parse(
    fs.readFileSync(path.join(fixtures, "provider-perplexity-showcase-fixture.json"), "utf8")
  );
  const norm = perplexityProvider.normalizeResponse(fixture.raw, { useV1_1: true });
  for (const c of norm.citations) {
    assert.equal(c.startIndex, null);
    assert.equal(c.endIndex, null);
  }
  const fields = listNormalizedCitationContractFields();
  const nc = normalizeProviderCitation({ url: "https://example.com" }, { provider: "perplexity" });
  for (const f of ["title", "citedText", "startIndex", "endIndex", "searchResultRank"]) {
    assert.ok(fields.includes(f));
    assert.equal(nc[f], null);
  }
});

test("errors — canonical categories; provider failure != brand absence", () => {
  const auth = classifyProviderError(new ProviderError("auth", { type: "auth_error", status: 401 }));
  assert.equal(auth.category, PROVIDER_ERROR_CATEGORIES.AUTH);
  assert.equal(auth.retryable, false);
  const rl = classifyProviderError(new ProviderError("rl", { type: "rate_limit", status: 429 }));
  assert.equal(rl.category, PROVIDER_ERROR_CATEGORIES.RATE_LIMIT);
  assert.equal(rl.retryable, true);
});

test("metrics — provider remains scope dimension; no blended metrics in adapters", () => {
  const metricsSrc = fs.readFileSync(path.join(root, "lib/ai-visibility/metrics.js"), "utf8");
  assert.ok(!/all_ai|blended|consensus|provider_weight/i.test(metricsSrc));
  const v1_1Fields = listNormalizedProviderContractFields("v1_1");
  assert.ok(v1_1Fields.includes("provider"));
  assert.ok(v1_1Fields.includes("searchResults"));
});

test("UI — unmonitored providers not exposed as measured", async () => {
  const store = createAiVisibilityStore({
    rootDir: path.join(root, "data/ai-visibility/runtime/wave1-showcase"),
  });
  const available = await listAvailableAiVisibilityProviders({ store });
  const ids = available.map((p) => p.id);
  assert.ok(ids.includes("openai"));
  assert.ok(!ids.includes("gemini"));
  assert.ok(!ids.includes("perplexity"));
  assert.ok(!ids.includes("claude"));
});

test("OpenAI Wave-1 untouched — v1 contract still default without useV1_1", () => {
  const built = buildOpenAiVisibilityRequest({
    prompt: { text: plan.EXECUTIONS[0].promptText },
    model: "gpt-5.6",
  });
  assert.equal(built.LIVE_PROVIDER_CALL, false);
  assert.equal(built.provider, "openai");
});

test("Perplexity execution mode — Sonar recommended", () => {
  assert.equal(RECOMMENDED_PERPLEXITY_EXECUTION_MODE, "sonar");
  const built = buildPerplexityVisibilityRequest({
    prompt: { text: "test" },
  });
  assert.equal(built.executionMode, "sonar");
  assert.equal(built.body.return_citations, true);
});

test("evidence ready flags", () => {
  assert.equal(GEMINI_EVIDENCE_READY, true);
  assert.equal(PERPLEXITY_EVIDENCE_READY, true);
  assert.equal(CLAUDE_EVIDENCE_READY, true);
});

test("controlled validation plan — 12 prompts × 3 providers", () => {
  const vp = buildControlledValidationPlan();
  assert.equal(vp.CALLS_PER_PROVIDER, 12);
  assert.equal(vp.TOTAL_CALLS, 36);
  assert.equal(CONTROLLED_VALIDATION_PROMPT_IDS.length, 12);
  for (const id of vp.PROMPT_IDS) {
    assert.ok(plan.EXECUTIONS.some((e) => e.promptId === id), id);
  }
});

test("citation rate compatibility documented per provider", () => {
  assert.equal(CITATION_RATE_COMPATIBILITY.openai.CITATION_RATE_SUPPORTED, true);
  assert.equal(CITATION_RATE_COMPATIBILITY.gemini.SEMANTICALLY_COMPARABLE_TO_OPENAI, false);
  assert.equal(CITATION_RATE_COMPATIBILITY.perplexity.SEMANTICALLY_COMPARABLE_TO_OPENAI, false);
  assert.equal(CITATION_RATE_COMPATIBILITY.claude.SEMANTICALLY_COMPARABLE_TO_OPENAI, false);
});

test("cross-provider signal foundation ready (architecture only)", () => {
  assert.equal(CROSS_PROVIDER_SIGNAL_FOUNDATION_READY, true);
});

test("rawText repair — flagged Wave-1 run not safe (empty artifacts)", () => {
  const audit = auditRawTextRepair();
  assert.equal(audit.RAW_TEXT_REPAIR_SAFE, false);
  assert.equal(audit.APPLIED, false);
  assert.equal(audit.RUN_ID, "run_83fe4c10721f4d9b");
  assert.equal(audit.FLAGGED_RUN.repairSafe, false);
  assert.equal(audit.FLAGGED_RUN.fingerprint, "055e743962880bf527279eb8");
});

console.log(`\nPhase 3B.1 results: ${passed} passed, ${failed} failed`);
console.log("LIVE_OPENAI_CALLS: 0");
console.log("LIVE_GEMINI_CALLS: 0");
console.log("LIVE_PERPLEXITY_CALLS: 0");
console.log("LIVE_CLAUDE_CALLS: 0");
if (failed) process.exit(1);
