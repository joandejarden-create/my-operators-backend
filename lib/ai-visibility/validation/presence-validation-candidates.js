/**
 * Fresh Presence validation candidate batch — generation + candidate build.
 * Presence signal only. Does not score Holdout v2. No resolver/alias changes.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { runVisibilityPrompt } from "../providers/index.js";
import { findEntitySpans } from "../normalize-entities.js";
import { buildGoldenSetScoringEntityIndex } from "./golden-set-entity-index.js";
import { loadGoldenSet } from "./golden-set.js";
import { isAiVisibilityLiveTestAllowed } from "../config.js";
import { resolveOpenAiCredential } from "../provider-credentials.js";
import {
  CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
  PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
  enrichCandidatesWithResponseGovernance,
  selectPrimaryReviewQueueByResponse,
  evaluateOpenAiFreezeGate,
  uniqueResponseIds,
} from "./presence-validation-pool-governance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const OUT_ROOT = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-validation-candidates"
);
const BATCH_ID = "presence_validation_candidate_batch_v1";
export const OPENAI_BATCH_ID = "presence_validation_openai_batch_v1";
const COST_CAP_USD = 30;
const EST_USD_PER_CALL = 0.25;
const OPENAI_COST_CAP_USD = 15;
const OPENAI_EST_USD_PER_CALL = 0.25;

export const PRESENCE_VALIDATION_BATCH_VERSION =
  "ai_intelligence_presence_validation_candidate_batch_v1";
export const PRESENCE_VALIDATION_OPENAI_BATCH_VERSION =
  "ai_intelligence_presence_validation_openai_batch_v1";

const PARENT_BRAND_NAMES = new Set([
  "Marriott",
  "Hilton",
  "IHG",
  "Hyatt",
  "Accor",
  "Choice Hotels",
  "Wyndham",
]);

function sha256(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex");
}

function ensureDirs() {
  for (const d of ["responses", "candidates", "reviews", "manifests"]) {
    fs.mkdirSync(path.join(OUT_ROOT, d), { recursive: true });
  }
}

export function presenceValidationPaths() {
  return {
    root: OUT_ROOT,
    plan: path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-candidate-batch-v1-plan.json"
    ),
    openaiPlan: path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-openai-batch-v1-plan.json"
    ),
    responsesDir: path.join(OUT_ROOT, "responses"),
    candidatesPath: path.join(OUT_ROOT, "candidates", "candidates.json"),
    reviewsPath: path.join(OUT_ROOT, "reviews", "reviews.json"),
    generationManifest: path.join(OUT_ROOT, "manifests", "generation-manifest.json"),
    openaiGenerationManifest: path.join(
      OUT_ROOT,
      "manifests",
      "openai-generation-manifest.json"
    ),
    afterGenerationReport: path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-candidates-ready.json"
    ),
    openaiAfterGenerationReport: path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-openai-candidates-ready.json"
    ),
  };
}

/**
 * Select 24 governed prompts for diversity (EN/ES × geos × intents).
 */
export function selectPresenceValidationPrompts() {
  const seedPath = path.join(
    ROOT,
    "fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json"
  );
  const seed = JSON.parse(fs.readFileSync(seedPath, "utf8"));
  const prompts = (seed.prompts || []).filter(
    (p) => p.active !== false && p.promptText && p.promptId
  );

  const wanted = [
    // Conversion
    "p_global_existing_asset_reposition_v1",
    "p_cala_existing_asset_reposition_v1",
    "p_cala_existing_asset_reposition_es_v1",
    "p_europe_existing_asset_reposition_v1",
    "p_na_existing_asset_reposition_v1",
    "p_mx_existing_asset_reposition_es_v1",
    // Soft brand / collection
    "p_global_soft_brand_shortlist_v1",
    "p_cala_collection_affiliation_v1",
    "p_cala_collection_affiliation_es_v1",
    "p_mx_soft_brand_shortlist_v1",
    "p_mx_soft_brand_shortlist_es_v1",
    "p_europe_collection_affiliation_v1",
    // Lifestyle
    "p_global_lifestyle_strategy_v1",
    "p_cala_lifestyle_strategy_es_v1",
    "p_mx_lifestyle_strategy_v1",
    "p_na_lifestyle_strategy_v1",
    // Upper-upscale
    "p_europe_uu_positioning_strategy_v1",
    "p_global_uu_owner_shortlist_v1",
    "p_cala_uu_positioning_strategy_es_v1",
    "p_mx_uu_owner_shortlist_v1",
    // Residences + flexibility
    "p_cala_residences_capability_v1",
    "p_mx_residences_capability_es_v1",
    "p_cala_affiliation_flexibility_es_v1",
    "p_global_affiliation_flexibility_v1",
  ];

  const byId = Object.fromEntries(prompts.map((p) => [p.promptId, p]));
  const selected = [];
  for (const id of wanted) {
    if (byId[id]) selected.push(byId[id]);
  }

  // Fill if some IDs missing
  if (selected.length < 24) {
    for (const p of prompts) {
      if (selected.length >= 24) break;
      if (selected.some((s) => s.promptId === p.promptId)) continue;
      selected.push(p);
    }
  }

  return selected.slice(0, 24).map((p) => ({
    promptId: p.promptId,
    promptVersion: p.version || "1",
    promptText: p.promptText,
    language: p.language || "en",
    geography: resolvePromptGeographyLabel(p),
    intentTerritory: p.intentTerritory || null,
    promptFamily: p.promptFamily || null,
  }));
}

function resolvePromptGeographyLabel(p) {
  if (p.country) {
    const c = String(p.country);
    if (/mexico/i.test(c)) return "Mexico";
    return c;
  }
  if (p.commercialRegion) {
    const r = String(p.commercialRegion);
    if (/cala/i.test(r) || /caribbean|latin/i.test(r)) return "CALA";
    if (/europe/i.test(r)) return "Europe";
    if (/north.?america/i.test(r)) return "North America";
    return r;
  }
  const g = String(p.geographyScope || "Global");
  if (/global/i.test(g)) return "Global";
  // Infer from promptId
  const id = String(p.promptId || "");
  if (id.includes("_cala_")) return "CALA";
  if (id.includes("_mx_")) return "Mexico";
  if (id.includes("_europe_")) return "Europe";
  if (id.includes("_na_")) return "North America";
  if (id.includes("_global_")) return "Global";
  return normalizeGeo(g);
}

export function buildLeakageIndex() {
  const hashes = new Set();
  const responseIds = new Set();
  const caseIds = new Set();

  function addText(t) {
    if (!t) return;
    hashes.add(sha256(String(t).replace(/\s+/g, " ").trim().toLowerCase()));
  }

  const goldenFiles = [
    path.join(ROOT, "fixtures/ai-visibility/ai-intelligence-golden-set-v2.json"),
    path.join(ROOT, "fixtures/ai-visibility/ai-intelligence-golden-set-v1.json"),
  ];
  for (const fp of goldenFiles) {
    if (!fs.existsSync(fp)) continue;
    const doc = JSON.parse(fs.readFileSync(fp, "utf8"));
    for (const c of doc.cases || []) {
      if (c.caseId) caseIds.add(c.caseId);
      if (c.responseId) responseIds.add(c.responseId);
      if (c.sourceResponseId) responseIds.add(c.sourceResponseId);
      addText(c.rawResponseExcerpt || c.text || "");
    }
  }

  // Prior presence validation batch responses + candidates (no reuse / no leakage)
  const paths = presenceValidationPaths();
  if (fs.existsSync(paths.responsesDir)) {
    for (const f of fs.readdirSync(paths.responsesDir).filter((x) => x.endsWith(".json"))) {
      try {
        const r = JSON.parse(fs.readFileSync(path.join(paths.responsesDir, f), "utf8"));
        if (r.responseId) responseIds.add(r.responseId);
        if (r.textHash) hashes.add(r.textHash);
        else addText(r.rawText || "");
      } catch {
        // skip corrupt response file
      }
    }
  }
  if (fs.existsSync(paths.candidatesPath)) {
    try {
      const doc = JSON.parse(fs.readFileSync(paths.candidatesPath, "utf8"));
      for (const c of doc.cases || []) {
        if (c.caseId) caseIds.add(c.caseId);
        if (c.responseId) responseIds.add(c.responseId);
        if (c.sourceResponseId) responseIds.add(c.sourceResponseId);
        if (c.responseHash) hashes.add(c.responseHash);
        else if (c.textHash) hashes.add(c.textHash);
      }
    } catch {
      // skip
    }
  }

  // Holdout / monitoring fixtures (response ids + text hashes when present)
  const holdoutPath = path.join(
    ROOT,
    "data/ai-visibility/validation/ai-intelligence-presence-holdout-v2.json"
  );
  if (fs.existsSync(holdoutPath)) {
    try {
      const doc = JSON.parse(fs.readFileSync(holdoutPath, "utf8"));
      for (const c of doc.cases || doc.pairs || []) {
        if (c.caseId) caseIds.add(c.caseId);
        if (c.responseId) responseIds.add(c.responseId);
        if (c.sourceResponseId) responseIds.add(c.sourceResponseId);
        addText(c.rawText || c.rawResponseExcerpt || "");
      }
    } catch {
      // skip
    }
  }

  return { hashes, responseIds, caseIds };
}

function normalizeGeo(g) {
  const s = String(g || "Global");
  if (/north.?america/i.test(s) || s === "NA") return "North America";
  if (/mexico/i.test(s) || s === "MX") return "Mexico";
  if (/cala/i.test(s)) return "CALA";
  if (/europe/i.test(s)) return "Europe";
  if (/global/i.test(s)) return "Global";
  return s;
}

/**
 * Build execution slots: each selected prompt × available providers.
 * @param {object} [options]
 * @param {string} [options.batchId]
 * @param {string[]} [options.providersOnly] — if set, only these providers (when available)
 */
export function buildExecutionSlots(prompts, providerAvailability, options = {}) {
  const batchId = options.batchId || BATCH_ID;
  const providers = (
    options.providersOnly?.length
      ? options.providersOnly
      : ["gemini", "perplexity", "claude"]
  ).filter((p) => providerAvailability[p]);
  const slots = [];
  for (const prompt of prompts) {
    for (const provider of providers) {
      slots.push({
        slotId: `${batchId}__${provider}__${prompt.promptId}`,
        provider,
        prompt,
      });
    }
  }
  return slots;
}

export function detectProviderAvailability() {
  try {
    resolveOpenAiCredential();
  } catch {
    // leave env as-is
  }
  return {
    openai: !!(
      process.env.OPENAI_API_KEY ||
      process.env.AI_VISIBILITY_OPENAI_API_KEY ||
      process.env.FDD_OPENAI_API_KEY
    ),
    gemini: !!process.env.GEMINI_API_KEY,
    perplexity: !!process.env.PERPLEXITY_API_KEY,
    claude: !!(process.env.ANTHROPIC_API_KEY || process.env.CLAUDE_API_KEY),
  };
}

/**
 * Approved OpenAI-only Presence validation plan (24 responses).
 * Does not redesign prompt set — same governed prompts as batch v1.
 */
export function buildOpenAiPresenceValidationPlan() {
  const availability = detectProviderAvailability();
  const prompts = selectPresenceValidationPrompts();
  const slots = buildExecutionSlots(prompts, availability, {
    batchId: OPENAI_BATCH_ID,
    providersOnly: ["openai"],
  });
  const estimated = slots.length * OPENAI_EST_USD_PER_CALL;
  return {
    phase: "OPENAI_PRESENCE_VALIDATION_PLAN_READY",
    batchId: OPENAI_BATCH_ID,
    OPENAI_API_KEY_AVAILABLE: availability.openai,
    OPENAI_PROVIDER_ADAPTER_READY: true,
    MODEL: process.env.AI_VISIBILITY_MODEL || "gpt-5.6",
    PLANNED_RESPONSES: slots.length,
    BY_LANGUAGE: {
      en: slots.filter((s) => s.prompt.language === "en").length,
      es: slots.filter((s) => s.prompt.language === "es").length,
    },
    BY_GEOGRAPHY: slots.reduce((acc, s) => {
      const g = s.prompt.geography || "Global";
      acc[g] = (acc[g] || 0) + 1;
      return acc;
    }, {}),
    BY_PROVIDER: { openai: slots.length },
    promptIdsPlanned: prompts.map((p) => p.promptId),
    ESTIMATED_COST: estimated,
    COST_CAP: OPENAI_COST_CAP_USD,
    EST_USD_PER_CALL: OPENAI_EST_USD_PER_CALL,
    LEAKAGE_CHECK_READY: true,
    READY_TO_RUN:
      availability.openai && estimated <= OPENAI_COST_CAP_USD && slots.length > 0
        ? "YES"
        : "NO",
    HOLDOUT_V2_FINAL_FREEZE_ALLOWED: "NO",
    slots,
  };
}

/**
 * Run bounded live generation. Stops if cost estimate exceeds cap.
 * @param {object} [options]
 * @param {string} [options.batchId]
 * @param {string[]} [options.providersOnly]
 * @param {number} [options.costCap]
 * @param {number} [options.estPerCall]
 * @param {boolean} [options.resume]
 * @param {boolean} [options.requireLiveFlag]
 * @param {string} [options.manifestPath]
 * @param {boolean} [options.excludeOwnBatchFromLeakage] — when regenerating, don't self-leakage against this batch's prior files
 */
export async function generatePresenceValidationResponses(options = {}) {
  ensureDirs();
  const paths = presenceValidationPaths();
  const batchId = options.batchId || BATCH_ID;
  const costCap = options.costCap ?? COST_CAP_USD;
  const estPerCall = options.estPerCall ?? EST_USD_PER_CALL;
  const availability = detectProviderAvailability();
  const prompts = selectPresenceValidationPrompts();
  const slots = buildExecutionSlots(prompts, availability, {
    batchId,
    providersOnly: options.providersOnly,
  });
  const plannedCost = slots.length * estPerCall;

  if (!slots.length) {
    return {
      ok: false,
      status: "BLOCKED_NO_SLOTS",
      PLANNED_RESPONSES: 0,
      providerAvailability: availability,
      message: options.providersOnly
        ? `No slots for providers: ${options.providersOnly.join(",")}`
        : "No provider slots available",
    };
  }

  if (plannedCost > costCap) {
    return {
      ok: false,
      status: "BLOCKED_COST_CAP",
      PLANNED_RESPONSES: slots.length,
      ESTIMATED_COST: plannedCost,
      COST_CAP: costCap,
    };
  }

  if (!isAiVisibilityLiveTestAllowed() && options.requireLiveFlag !== false) {
    return {
      ok: false,
      status: "BLOCKED_LIVE_FLAG",
      message: "Set AI_VISIBILITY_LIVE_TEST=true to run live provider calls",
    };
  }

  // Leakage index: when generating a new batch, temporarily ignore this batch's
  // own response files so resume/retry does not self-reject; still check other batches + golden.
  const leakage = buildLeakageIndex();
  if (options.excludeOwnBatchFromLeakage !== false && batchId) {
    for (const slot of slots) {
      const outFile = path.join(paths.responsesDir, `${slot.slotId}.json`);
      if (!fs.existsSync(outFile)) continue;
      try {
        const existing = JSON.parse(fs.readFileSync(outFile, "utf8"));
        if (existing.responseId) leakage.responseIds.delete(existing.responseId);
        if (existing.textHash) leakage.hashes.delete(existing.textHash);
      } catch {
        // ignore
      }
    }
  }

  const results = [];
  let spent = 0;
  let duplicatesRejected = 0;
  let leakageRejected = 0;

  for (const slot of slots) {
    if (spent + estPerCall > costCap) {
      results.push({
        slotId: slot.slotId,
        status: "skipped_cost_cap",
        provider: slot.provider,
      });
      break;
    }

    const outFile = path.join(paths.responsesDir, `${slot.slotId}.json`);
    if (fs.existsSync(outFile) && options.resume !== false) {
      const existing = JSON.parse(fs.readFileSync(outFile, "utf8"));
      // Resume only successful/rejected slots; retry errors
      if (existing.status === "ok" || existing.status === "rejected") {
        results.push(existing);
        spent += Number(existing.estimatedCostUsd) || estPerCall;
        continue;
      }
    }

    const startedAt = new Date().toISOString();
    let record;
    try {
      const providerResult = await runVisibilityPrompt({
        provider: slot.provider,
        prompt: {
          promptId: slot.prompt.promptId,
          promptVersion: slot.prompt.promptVersion,
          text: slot.prompt.promptText,
          promptText: slot.prompt.promptText,
          language: slot.prompt.language,
          geographyScope: slot.prompt.geography,
        },
        enableWebSearch: true,
      });
      const rawText = providerResult?.text || "";
      const textHash = sha256(rawText.replace(/\s+/g, " ").trim().toLowerCase());
      const responseId =
        providerResult?.responseId ||
        `presval_${sha256(slot.slotId + startedAt).slice(0, 16)}`;

      let rejected = null;
      if (leakage.responseIds.has(responseId)) {
        rejected = "LEAKAGE_RESPONSE_ID";
        leakageRejected += 1;
      } else if (leakage.hashes.has(textHash)) {
        rejected = "LEAKAGE_TEXT_HASH";
        leakageRejected += 1;
        duplicatesRejected += 1;
      }

      const cost =
        providerResult?.usage?.providerCostUsd != null
          ? Number(providerResult.usage.providerCostUsd)
          : estPerCall;
      spent += cost;

      record = {
        batchId,
        VALIDATION_CANDIDATE_ONLY: true,
        clientPublishable: false,
        slotId: slot.slotId,
        status: rejected ? "rejected" : "ok",
        rejectReason: rejected,
        responseId,
        provider: slot.provider,
        model: providerResult?.model || null,
        promptId: slot.prompt.promptId,
        promptVersion: slot.prompt.promptVersion,
        language: slot.prompt.language,
        geography: normalizeGeo(slot.prompt.geography),
        intentTerritory: slot.prompt.intentTerritory,
        promptText: slot.prompt.promptText,
        rawText,
        textHash,
        citations: providerResult?.citations || [],
        usage: providerResult?.usage || null,
        estimatedCostUsd: cost,
        latencyMs: providerResult?.latencyMs ?? null,
        runTimestamp: startedAt,
        NEW_RESPONSE: rejected ? "NO" : "YES",
      };
      // Prevent duplicate hash within this run
      if (!rejected && textHash) leakage.hashes.add(textHash);
      if (!rejected && responseId) leakage.responseIds.add(responseId);
    } catch (err) {
      spent += estPerCall;
      record = {
        batchId,
        VALIDATION_CANDIDATE_ONLY: true,
        slotId: slot.slotId,
        status: "error",
        provider: slot.provider,
        promptId: slot.prompt.promptId,
        language: slot.prompt.language,
        geography: normalizeGeo(slot.prompt.geography),
        error: err?.message || String(err),
        estimatedCostUsd: estPerCall,
        runTimestamp: startedAt,
      };
    }

    fs.writeFileSync(outFile, JSON.stringify(record, null, 2) + "\n", "utf8");
    results.push(record);
  }

  const okResponses = results.filter((r) => r.status === "ok");
  const manifest = {
    batchId,
    version:
      batchId === OPENAI_BATCH_ID
        ? PRESENCE_VALIDATION_OPENAI_BATCH_VERSION
        : PRESENCE_VALIDATION_BATCH_VERSION,
    generatedAt: new Date().toISOString(),
    providerAvailability: availability,
    PLANNED_RESPONSES: slots.length,
    COMPLETED_OK: okResponses.length,
    ERRORS: results.filter((r) => r.status === "error").length,
    REJECTED: results.filter((r) => r.status === "rejected").length,
    DUPLICATES_REJECTED: duplicatesRejected,
    LEAKAGE_REJECTED: leakageRejected,
    ESTIMATED_SPENT_USD: spent,
    COST_CAP_USD: costCap,
    slots: results.map((r) => ({
      slotId: r.slotId,
      status: r.status,
      provider: r.provider,
      responseId: r.responseId || null,
    })),
  };
  const manifestPath =
    options.manifestPath ||
    (batchId === OPENAI_BATCH_ID
      ? paths.openaiGenerationManifest
      : paths.generationManifest);
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");

  return { ok: true, manifest, results, okResponses, paths };
}

/**
 * OpenAI-only bounded generation using the approved openai batch plan.
 */
export async function generateOpenAiPresenceValidationResponses(options = {}) {
  return generatePresenceValidationResponses({
    batchId: OPENAI_BATCH_ID,
    providersOnly: ["openai"],
    costCap: OPENAI_COST_CAP_USD,
    estPerCall: OPENAI_EST_USD_PER_CALL,
    resume: options.resume !== false,
    requireLiveFlag: options.requireLiveFlag,
    excludeOwnBatchFromLeakage: true,
    ...options,
  });
}

/**
 * Build Presence TRUE + FALSE candidates from fresh responses.
 * @param {object[]} okResponses
 * @param {object} [options]
 * @param {boolean} [options.mergeWithExisting] — preserve prior pool cases (default false)
 * @param {string} [options.replaceBatchId] — drop prior cases from this batchId before merge
 * @param {number} [options.primaryQueueTarget]
 */
export function buildPresenceValidationCandidates(okResponses, options = {}) {
  const index = buildGoldenSetScoringEntityIndex({});
  const leakage = buildLeakageIndex();
  const candidates = [];
  let duplicatesRejected = 0;
  let leakageRejected = 0;

  // Hard-negative pool: brands often confused / parent-related
  const hardNegativeNames = [
    "Playa Hotels & Resorts",
    "Autograph Collection",
    "Curio Collection by Hilton",
    "Canopy by Hilton",
    "Tribute Portfolio",
    "The Luxury Collection",
    "Kimpton Hotels",
    "Hotel Indigo",
    "Tapestry Collection by Hilton",
    "Design Hotels",
    "JW Marriott",
    "W Hotels",
  ];

  const entityByName = new Map();
  for (const e of index.entities || []) {
    if (e?.name) entityByName.set(e.name, e);
  }

  for (const resp of okResponses) {
    if (!resp.rawText) continue;
    const spans = findEntitySpans(resp.rawText, index.aliasIndex);
    const presentNames = new Set(spans.map((s) => s.entity?.name).filter(Boolean));
    const presentIds = new Set(spans.map((s) => s.entity?.id).filter(Boolean));

    // TRUE candidates — up to 3 present entities (prefer non-parent)
    const presentEntities = [...presentNames]
      .filter((n) => !PARENT_BRAND_NAMES.has(n))
      .slice(0, 3);
    if (!presentEntities.length && presentNames.size) {
      presentEntities.push([...presentNames][0]);
    }

    for (const name of presentEntities) {
      const ent = entityByName.get(name);
      const cand = makeCandidate({
        resp,
        entityName: name,
        entityId: ent?.id || null,
        candidateType: "PRESENCE_TRUE",
        systemSuggestion: "YES",
        rationale: "Entity span resolved in fresh response",
      });
      if (leakage.caseIds.has(cand.caseId)) {
        leakageRejected += 1;
        continue;
      }
      candidates.push(cand);
    }

    // FALSE candidates — meaningful hard negatives absent from response
    const falsePicks = [];
    for (const name of hardNegativeNames) {
      if (presentNames.has(name)) continue;
      const ent = entityByName.get(name);
      if (!ent) continue;
      // Prefer geographic Playa false friend when "playa" appears without company
      if (name === "Playa Hotels & Resorts") {
        if (/\bplaya\b/i.test(resp.rawText) && !/playa\s+hotels/i.test(resp.rawText)) {
          falsePicks.unshift({ name, ent, rationale: "geographic/common-language Playa without Playa Hotels & Resorts" });
          continue;
        }
      }
      // Parent present / child absent
      if (
        (name.includes("Hilton") || name.includes("Curio") || name.includes("Canopy") || name.includes("Tapestry")) &&
        [...presentNames].some((n) => /\bhilton\b/i.test(n) && n !== name)
      ) {
        falsePicks.push({
          name,
          ent,
          rationale: "sibling/parent Hilton context without this specific brand",
        });
        continue;
      }
      if (
        (name.includes("Autograph") || name.includes("Tribute") || name.includes("Luxury Collection") || name.includes("JW Marriott") || name === "W Hotels") &&
        [...presentNames].some((n) => /marriott/i.test(n) && n !== name)
      ) {
        falsePicks.push({
          name,
          ent,
          rationale: "Marriott family context without this specific child brand",
        });
        continue;
      }
      if (/collection/i.test(resp.rawText) && /Collection/.test(name)) {
        falsePicks.push({
          name,
          ent,
          rationale: "generic collection language without this Collection brand",
        });
        continue;
      }
    }

    // Always add at least one absent brand if none matched heuristics
    if (!falsePicks.length) {
      for (const name of hardNegativeNames) {
        if (presentNames.has(name)) continue;
        const ent = entityByName.get(name);
        if (ent) {
          falsePicks.push({
            name,
            ent,
            rationale: "canonical brand absent from response (hard negative pool)",
          });
          break;
        }
      }
    }

    for (const fp of falsePicks.slice(0, 2)) {
      const cand = makeCandidate({
        resp,
        entityName: fp.name,
        entityId: fp.ent.id,
        candidateType: "PRESENCE_FALSE",
        systemSuggestion: "NO",
        rationale: fp.rationale,
      });
      if (leakage.caseIds.has(cand.caseId)) {
        leakageRejected += 1;
        continue;
      }
      // Dedupe same response×entity
      if (candidates.some((c) => c.caseId === cand.caseId)) {
        duplicatesRejected += 1;
        continue;
      }
      candidates.push(cand);
    }
  }

  // Response-level enrichment + primary queue (whole responses stay together)
  let newCases = enrichCandidatesWithResponseGovernance(candidates);
  const primaryTarget =
    options.primaryQueueTarget ??
    Math.max(150, newCases.length);
  const primary = selectPrimaryReviewQueueByResponse(newCases, primaryTarget);
  const primaryIds = new Set(primary.selectedCases.map((c) => c.caseId));
  for (const c of newCases) {
    c.primaryReviewQueue = primaryIds.has(c.caseId);
  }

  let cases = newCases;
  if (options.mergeWithExisting) {
    const prev = loadPresenceValidationCandidates();
    const replaceBatchId = options.replaceBatchId || null;
    const kept = (prev?.cases || []).filter((c) => {
      if (replaceBatchId && c.batchId === replaceBatchId) return false;
      // Also drop any case tied to responses we just rebuilt
      const rid = c.sourceResponseId || c.responseId;
      if (rid && okResponses.some((r) => r.responseId === rid)) return false;
      return true;
    });
    // Preserve prior primaryReviewQueue flags on kept cases
    cases = enrichCandidatesWithResponseGovernance([...kept, ...newCases]);
  }

  const openaiGate = evaluateOpenAiFreezeGate(null, cases);
  const uniqAll = uniqueResponseIds(cases);
  const openaiCases = cases.filter((c) => c.provider === "openai" || c.batchId === OPENAI_BATCH_ID);

  const paths = presenceValidationPaths();
  ensureDirs();
  const doc = {
    version: PRESENCE_VALIDATION_BATCH_VERSION,
    batchId: options.mergeWithExisting
      ? "presence_validation_pool_merged"
      : BATCH_ID,
    batchesIncluded: options.mergeWithExisting
      ? [...new Set(cases.map((c) => c.batchId).filter(Boolean))]
      : [BATCH_ID],
    createdAt: new Date().toISOString(),
    governanceVersion: "presence_validation_pool_governance_v1",
    TOTAL_CANDIDATES: cases.length,
    CANDIDATE_PAIR_N: cases.length,
    UNIQUE_RESPONSE_N: uniqAll.size,
    PRIMARY_REVIEW_QUEUE: cases.filter((c) => c.primaryReviewQueue).length,
    OPENAI_CANDIDATE_N: openaiCases.length,
    OPENAI_PRIMARY_REVIEW_QUEUE: openaiCases.filter((c) => c.primaryReviewQueue).length,
    OPENAI_UNIQUE_RESPONSES: uniqueResponseIds(openaiCases).size,
    RESPONSE_LEVEL_PARTITIONING: true,
    CANDIDATE_CAP_PER_RESPONSE: CANDIDATE_CAP_PER_RESPONSE_HOLDOUT,
    OPENAI_REQUIRED_BEFORE_FINAL_FREEZE: true,
    // Final freeze stays blocked until OpenAI cases are human-reviewed (not merely present).
    HOLDOUT_V2_FINAL_FREEZE_ALLOWED: "NO",
    openaiFreezeGate: openaiGate,
    PRESENCE_HOLDOUT_V2_METRIC_CONTRACT,
    POTENTIAL_TRUE: cases.filter((c) => c.candidateType === "PRESENCE_TRUE").length,
    POTENTIAL_FALSE: cases.filter((c) => c.candidateType === "PRESENCE_FALSE").length,
    OPENAI_POTENTIAL_TRUE: openaiCases.filter((c) => c.candidateType === "PRESENCE_TRUE").length,
    OPENAI_POTENTIAL_FALSE: openaiCases.filter((c) => c.candidateType === "PRESENCE_FALSE").length,
    DUPLICATES_REJECTED: duplicatesRejected,
    LEAKAGE_REJECTED: leakageRejected,
    SYSTEM_SUGGESTION_IS_NOT_GROUND_TRUTH: true,
    cases,
  };
  fs.writeFileSync(paths.candidatesPath, JSON.stringify(doc, null, 2) + "\n");
  if (!fs.existsSync(paths.reviewsPath)) {
    fs.writeFileSync(
      paths.reviewsPath,
      JSON.stringify({ version: "presence_validation_reviews_v1", reviews: {} }, null, 2) +
        "\n"
    );
  }
  return doc;
}

/**
 * Deterministic stratified sample for primary human review.
 * Targets ~110 PRESENCE_TRUE + ~40 PRESENCE_FALSE so Holdout v2 (75/25)
 * remains reachable after INVALID/DEFER attrition.
 */
export function selectPrimaryReviewQueue(candidates, targetN = 150) {
  const trueTarget = Math.min(
    Math.round(targetN * 0.73),
    candidates.filter((c) => c.candidateType === "PRESENCE_TRUE").length
  );
  const falseTarget = Math.min(
    targetN - trueTarget,
    candidates.filter((c) => c.candidateType === "PRESENCE_FALSE").length
  );

  function pickType(type, n) {
    const list = candidates
      .filter((c) => c.candidateType === type)
      .sort((a, b) => String(a.caseId).localeCompare(String(b.caseId)));
    const buckets = new Map();
    for (const c of list) {
      const key = [c.provider, c.language, c.geography].join("|");
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key).push(c);
    }
    const selected = [];
    const keys = [...buckets.keys()].sort();
    let progressed = true;
    while (selected.length < n && progressed) {
      progressed = false;
      for (const key of keys) {
        if (selected.length >= n) break;
        const bucket = buckets.get(key);
        if (bucket.length) {
          selected.push(bucket.shift());
          progressed = true;
        }
      }
    }
    return selected;
  }

  return [...pickType("PRESENCE_TRUE", trueTarget), ...pickType("PRESENCE_FALSE", falseTarget)].sort(
    (a, b) => String(a.caseId).localeCompare(String(b.caseId))
  );
}

function makeCandidate({ resp, entityName, entityId, candidateType, systemSuggestion, rationale }) {
  const caseId = `presval_${sha256(
    [resp.responseId, entityId || entityName, candidateType].join("|")
  ).slice(0, 12)}`;
  return {
    caseId,
    batchId: resp.batchId || BATCH_ID,
    VALIDATION_CANDIDATE_ONLY: true,
    candidateType,
    responseId: resp.responseId,
    sourceResponseId: resp.responseId,
    responseHash: resp.textHash || null,
    provider: resp.provider,
    model: resp.model,
    language: resp.language,
    geography: resp.geography,
    promptId: resp.promptId,
    promptVersion: resp.promptVersion,
    promptText: resp.promptText,
    intentTerritory: resp.intentTerritory,
    canonicalEntityName: entityName,
    canonicalEntityId: entityId,
    rawText: resp.rawText,
    textHash: resp.textHash,
    SYSTEM_PRESENCE_SUGGESTION: systemSuggestion,
    systemSuggestionRationale: rationale,
    NEW_RESPONSE: "YES",
    NOT_IN_GOLDEN_SET: true,
    NOT_IN_DEV: true,
    NOT_IN_HOLDOUT_V1: true,
    NOT_IN_CLASSIFIER_LAB: true,
    NOT_PREVIOUSLY_HUMAN_REVIEWED: true,
    validationPartition: "UNASSIGNED",
    humanLabel: null,
    reviewStatus: "PENDING",
    reviewer: null,
    reviewedAt: null,
  };
}

export function loadPresenceValidationCandidates() {
  const paths = presenceValidationPaths();
  if (!fs.existsSync(paths.candidatesPath)) return null;
  return JSON.parse(fs.readFileSync(paths.candidatesPath, "utf8"));
}

export function loadPresenceValidationReviews() {
  const paths = presenceValidationPaths();
  if (!fs.existsSync(paths.reviewsPath)) {
    return { version: "presence_validation_reviews_v1", reviews: {} };
  }
  return JSON.parse(fs.readFileSync(paths.reviewsPath, "utf8"));
}

export function savePresenceValidationReview(caseId, payload) {
  const allowed = new Set(["PRESENT", "NOT_PRESENT", "INVALID", "DEFER"]);
  if (!allowed.has(payload.action)) {
    const err = new Error("INVALID_PRESENCE_REVIEW_ACTION");
    err.code = "INVALID_PRESENCE_REVIEW_ACTION";
    throw err;
  }
  if (!payload.reviewer || !String(payload.reviewer).trim()) {
    const err = new Error("REVIEWER_REQUIRED");
    err.code = "REVIEWER_REQUIRED";
    throw err;
  }
  const candDoc = loadPresenceValidationCandidates();
  const candRow = candDoc?.cases?.find((c) => c.caseId === caseId);
  if (!candRow) {
    const err = new Error("CASE_NOT_FOUND");
    err.code = "CASE_NOT_FOUND";
    throw err;
  }
  const reviews = loadPresenceValidationReviews();
  const now = new Date().toISOString();

  // Lazy-load assisted proposals without hard dependency cycle at module top
  let assistedSnapshot = null;
  try {
    const assistedPath = path.join(
      presenceValidationPaths().root,
      "assisted-proposals",
      "assisted-proposals.json"
    );
    if (fs.existsSync(assistedPath)) {
      const store = JSON.parse(fs.readFileSync(assistedPath, "utf8"));
      assistedSnapshot = store.proposals?.[caseId] || null;
    }
  } catch {
    assistedSnapshot = null;
  }

  let humanAction = payload.humanAction || null;
  if (!humanAction && assistedSnapshot) {
    if (payload.action === assistedSnapshot.proposedDecision) {
      humanAction = "ACCEPTED_ASSISTED_PROPOSAL";
    } else {
      humanAction = "CHANGED_ASSISTED_PROPOSAL";
    }
  }
  if (!humanAction && payload.acceptAssisted === true && assistedSnapshot) {
    humanAction = "ACCEPTED_ASSISTED_PROPOSAL";
  }
  if (!humanAction) humanAction = "MANUAL_DECISION";

  reviews.reviews[caseId] = {
    caseId,
    action: payload.action,
    humanLabel:
      payload.action === "PRESENT"
        ? "PRESENT"
        : payload.action === "NOT_PRESENT"
          ? "NOT_PRESENT"
          : payload.action === "INVALID"
            ? "INVALID_SUBJECT"
            : "DEFER",
    humanFinalDecision: payload.action,
    reviewer: String(payload.reviewer).trim(),
    reviewedAt: now,
    notes: payload.notes || null,
    AUTO_HUMAN_LABEL: false,
    systemSuggestion: candRow.SYSTEM_PRESENCE_SUGGESTION ?? null,
    assistedProposal: assistedSnapshot
      ? {
          source: assistedSnapshot.source,
          proposalVersion: assistedSnapshot.proposalVersion,
          proposedDecision: assistedSnapshot.proposedDecision,
          proposedNotes: assistedSnapshot.proposedNotes,
          importedAt: assistedSnapshot.importedAt,
          sourceExport: assistedSnapshot.sourceExport,
        }
      : null,
    humanAction,
  };
  reviews.updatedAt = now;
  fs.writeFileSync(
    presenceValidationPaths().reviewsPath,
    JSON.stringify(reviews, null, 2) + "\n"
  );
  return reviews.reviews[caseId];
}

export function summarizePresenceValidationReview() {
  const cand = loadPresenceValidationCandidates();
  const reviews = loadPresenceValidationReviews();
  const cases = cand?.cases || [];
  let present = 0,
    notPresent = 0,
    invalid = 0,
    deferred = 0,
    pending = 0;
  const reviewedResponseIds = new Set();
  const allResponseIds = new Set();
  for (const c of cases) {
    const rid = c.sourceResponseId || c.responseId;
    if (rid) allResponseIds.add(rid);
    const r = reviews.reviews?.[c.caseId];
    if (!r) {
      pending += 1;
      continue;
    }
    if (rid) reviewedResponseIds.add(rid);
    if (r.action === "PRESENT") present += 1;
    else if (r.action === "NOT_PRESENT") notPresent += 1;
    else if (r.action === "INVALID") invalid += 1;
    else if (r.action === "DEFER") deferred += 1;
  }
  const reviewedPairs = present + notPresent + invalid + deferred;

  let assistedProgress = {
    TOTAL_ASSISTED: 0,
    ACCEPTED: 0,
    CHANGED: 0,
    DEFERRED: 0,
    REMAINING: 0,
    ALREADY_REVIEWED: 0,
    SYSTEM_DISAGREEMENTS: 0,
    HUMAN_ACTION_COUNTS: {},
  };
  try {
    const assistedPath = path.join(
      presenceValidationPaths().root,
      "assisted-proposals",
      "assisted-proposals.json"
    );
    if (fs.existsSync(assistedPath)) {
      // Keep in sync with summarizeAssistedProposalProgress (avoid circular import).
      // Accepted vs Changed = human final vs ASSISTED proposal (not system suggestion).
      const store = JSON.parse(fs.readFileSync(assistedPath, "utf8"));
      const proposals = store.proposals || {};
      assistedProgress.TOTAL_ASSISTED = Object.keys(proposals).length;
      for (const [caseId, prop] of Object.entries(proposals)) {
        const human = reviews.reviews?.[caseId];
        const candRow = cases.find((c) => c.caseId === caseId);
        const sys = String(
          (prop.systemSuggestionFromProposal ?? candRow?.SYSTEM_PRESENCE_SUGGESTION) || ""
        ).toUpperCase();
        const sysMapped =
          sys === "YES" || sys === "PRESENT"
            ? "PRESENT"
            : sys === "NO" || sys === "NOT_PRESENT"
              ? "NOT_PRESENT"
              : null;
        if (
          sysMapped &&
          (prop.proposedDecision === "PRESENT" || prop.proposedDecision === "NOT_PRESENT") &&
          prop.proposedDecision !== sysMapped
        ) {
          assistedProgress.SYSTEM_DISAGREEMENTS += 1;
        }
        if (!human) {
          assistedProgress.REMAINING += 1;
          continue;
        }
        const actionKey = human.humanAction || human.action || "UNKNOWN";
        assistedProgress.HUMAN_ACTION_COUNTS[actionKey] =
          (assistedProgress.HUMAN_ACTION_COUNTS[actionKey] || 0) + 1;
        if (human.action === "DEFER") {
          assistedProgress.DEFERRED += 1;
          continue;
        }
        const proposed = String(prop.proposedDecision || "").toUpperCase();
        const finalDecision = String(human.humanFinalDecision || human.action || "").toUpperCase();
        if (finalDecision && proposed && finalDecision === proposed) {
          assistedProgress.ACCEPTED += 1;
        } else {
          assistedProgress.CHANGED += 1;
        }
      }
      assistedProgress.ALREADY_REVIEWED =
        assistedProgress.ACCEPTED + assistedProgress.CHANGED + assistedProgress.DEFERRED;
    }
  } catch {
    // keep zeros
  }

  const primaryCases = cases.filter((c) => c.primaryReviewQueue);
  const primaryReviewed = primaryCases.filter((c) => reviews.reviews?.[c.caseId]).length;
  const nonPrimary = cases.filter((c) => !c.primaryReviewQueue);

  return {
    TOTAL: cases.length,
    Reviewed: reviewedPairs,
    ReviewedCandidatePairs: reviewedPairs,
    UniqueResponsesReviewed: reviewedResponseIds.size,
    UniqueResponsesTotal: allResponseIds.size,
    Present: present,
    NotPresent: notPresent,
    Invalid: invalid,
    Deferred: deferred,
    Remaining: pending,
    CANDIDATE_PAIR_N: cases.length,
    UNIQUE_RESPONSE_N: allResponseIds.size,
    PRIMARY_REVIEW_QUEUE_N: primaryCases.length,
    PRIMARY_REVIEWED: primaryReviewed,
    NON_PRIMARY_RESERVE: nonPrimary.length,
    NON_PRIMARY_UNREVIEWED: nonPrimary.filter((c) => !reviews.reviews?.[c.caseId]).length,
    TOTAL_ASSISTED: assistedProgress.TOTAL_ASSISTED,
    ACCEPTED: assistedProgress.ACCEPTED,
    CHANGED: assistedProgress.CHANGED,
    DEFERRED_ASSISTED: assistedProgress.DEFERRED,
    REMAINING_ASSISTED: assistedProgress.REMAINING,
    ALREADY_REVIEWED_ASSISTED: assistedProgress.ALREADY_REVIEWED,
    SYSTEM_DISAGREEMENTS: assistedProgress.SYSTEM_DISAGREEMENTS,
    HUMAN_ACTION_COUNTS: assistedProgress.HUMAN_ACTION_COUNTS,
  };
}
