/**
 * Presence Holdout v3 — fresh unseen candidate universe.
 *
 * Does NOT select / freeze / score Holdout v3.
 * Does NOT mutate Holdout v2.
 * Does NOT change entity resolver or aliases.
 * Responses must be newly generated; prompt text may reuse governed seeds.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { runVisibilityPrompt } from "../providers/index.js";
import { isAiVisibilityLiveTestAllowed } from "../config.js";
import {
  buildPresenceValidationCandidates,
  detectProviderAvailability,
  buildExecutionSlots,
  presenceValidationPaths,
  buildLeakageIndex,
} from "./presence-validation-candidates.js";
import { uniqueResponseIds } from "./presence-validation-pool-governance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const HOLDOUT_V3_BATCH_ID = "presence_validation_holdout_v3_candidate_batch_v1";
export const HOLDOUT_V3_BATCH_VERSION =
  "ai_intelligence_presence_holdout_v3_candidate_batch_v1";
export const HOLDOUT_V3_EST_USD_PER_CALL = 0.28;
export const HOLDOUT_V3_COST_CAP_USD = 40;

const OUT_ROOT = path.join(
  ROOT,
  "data/ai-visibility/validation/presence-holdout-v3-candidates"
);

function sha256(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex");
}

function ensureDirs() {
  for (const d of ["responses", "candidates", "reviews", "manifests"]) {
    fs.mkdirSync(path.join(OUT_ROOT, d), { recursive: true });
  }
}

export function presenceHoldoutV3Paths() {
  return {
    root: OUT_ROOT,
    plan: path.join(
      ROOT,
      "data/ai-visibility/validation/presence-holdout-v3-generation-plan.json"
    ),
    responsesDir: path.join(OUT_ROOT, "responses"),
    candidatesPath: path.join(OUT_ROOT, "candidates", "candidates.json"),
    reviewsPath: path.join(OUT_ROOT, "reviews", "reviews.json"),
    generationManifest: path.join(OUT_ROOT, "manifests", "generation-manifest.json"),
    readyReport: path.join(
      ROOT,
      "data/ai-visibility/validation/presence-holdout-v3-fresh-candidates-ready.json"
    ),
    activePoolMarker: path.join(
      ROOT,
      "data/ai-visibility/validation/presence-validation-active-pool.json"
    ),
  };
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
  const id = String(p.promptId || "");
  if (id.includes("_cala_")) return "CALA";
  if (id.includes("_mx_")) return "Mexico";
  if (id.includes("_europe_")) return "Europe";
  if (id.includes("_na_")) return "North America";
  if (id.includes("_global_")) return "Global";
  return normalizeGeo(p.geographyScope || "Global");
}

/**
 * 26 governed prompts — ~62% EN / ~38% ES; CALA+Mexico prioritized.
 * Includes conversion, collection/soft, lifestyle, UU, residences/new-build,
 * flexibility, brand shortlist (selection).
 */
export function selectPresenceHoldoutV3Prompts() {
  const seedPath = path.join(
    ROOT,
    "fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json"
  );
  const seed = JSON.parse(fs.readFileSync(seedPath, "utf8"));
  const prompts = (seed.prompts || []).filter(
    (p) => p.active !== false && p.promptText && p.promptId
  );
  const wanted = [
    // Conversion / reposition (Brand Selection adjacent)
    "p_global_existing_asset_reposition_v1",
    "p_cala_existing_asset_reposition_v1",
    "p_cala_existing_asset_reposition_es_v1",
    "p_europe_existing_asset_reposition_v1",
    "p_na_existing_asset_reposition_v1",
    "p_mx_existing_asset_reposition_es_v1",
    // Collection / soft brand
    "p_global_soft_brand_shortlist_v1",
    "p_cala_collection_affiliation_v1",
    "p_cala_collection_affiliation_es_v1",
    "p_mx_soft_brand_shortlist_v1",
    "p_mx_soft_brand_shortlist_es_v1",
    "p_europe_collection_affiliation_v1",
    "p_cala_soft_brand_shortlist_es_v1",
    "p_na_collection_affiliation_v1",
    // Lifestyle
    "p_global_lifestyle_strategy_v1",
    "p_cala_lifestyle_strategy_es_v1",
    "p_mx_lifestyle_strategy_v1",
    "p_mx_lifestyle_strategy_es_v1",
    "p_na_lifestyle_strategy_v1",
    // Upper-upscale
    "p_europe_uu_positioning_strategy_v1",
    "p_global_uu_owner_shortlist_v1",
    "p_cala_uu_positioning_strategy_es_v1",
    "p_mx_uu_owner_shortlist_v1",
    // Residences / new-build development strategy
    "p_global_residences_hotel_project_v1",
    "p_cala_residences_capability_v1",
    "p_mx_residences_capability_es_v1",
    // Owner flexibility
    "p_cala_affiliation_flexibility_es_v1",
    "p_global_affiliation_flexibility_v1",
  ];

  const byId = Object.fromEntries(prompts.map((p) => [p.promptId, p]));
  const selected = [];
  for (const id of wanted) {
    if (byId[id]) selected.push(byId[id]);
  }
  // Cap at 28 for ~112 provider slots (still under $40 cap)
  return selected.slice(0, 28).map((p) => ({
    promptId: p.promptId,
    promptVersion: p.version || "1",
    promptText: p.promptText,
    language: p.language || "en",
    geography: resolvePromptGeographyLabel(p),
    intentTerritory: p.intentTerritory || null,
    promptFamily: p.promptFamily || null,
  }));
}

/**
 * Expanded leakage — every prior inspected validation universe.
 */
export function buildHoldoutV3LeakageIndex() {
  const base = buildLeakageIndex();
  const hashes = new Set(base.hashes);
  const responseIds = new Set(base.responseIds);
  const caseIds = new Set(base.caseIds);

  function addText(t) {
    if (!t) return;
    hashes.add(sha256(String(t).replace(/\s+/g, " ").trim().toLowerCase()));
  }

  const extraFiles = [
    path.join(ROOT, "data/ai-visibility/validation/presence-validation-reserve.json"),
    path.join(
      ROOT,
      "data/ai-visibility/validation/ai-intelligence-holdout-v1-inspected-diagnostic.json"
    ),
    path.join(
      ROOT,
      "data/ai-visibility/validation/ai-intelligence-presence-holdout-v2.json"
    ),
    path.join(
      ROOT,
      "data/ai-visibility/validation/presence-holdout-validation.json"
    ),
    path.join(
      ROOT,
      "fixtures/ai-visibility/contextual-canopy-regression.json"
    ),
  ];

  for (const fp of extraFiles) {
    if (!fs.existsSync(fp)) continue;
    try {
      const doc = JSON.parse(fs.readFileSync(fp, "utf8"));
      for (const id of doc.caseIds || []) caseIds.add(id);
      for (const id of doc.sourceResponseIds || []) responseIds.add(id);
      for (const c of doc.cases || doc.pairs || []) {
        if (c.caseId) caseIds.add(c.caseId);
        if (c.responseId) responseIds.add(c.responseId);
        if (c.sourceResponseId) responseIds.add(c.sourceResponseId);
        if (c.responseHash) hashes.add(c.responseHash);
        if (c.textHash) hashes.add(c.textHash);
        addText(c.rawText || c.rawResponseExcerpt || c.text || "");
      }
      for (const row of [...(doc.positive || []), ...(doc.negative || [])]) {
        addText(row.text || "");
      }
    } catch {
      // skip
    }
  }

  // Classifier lab + any validation trees
  const scanRoots = [
    path.join(ROOT, "data/ai-visibility/validation"),
    path.join(ROOT, "data/ai-visibility/classifier-lab"),
    path.join(ROOT, "fixtures/ai-visibility"),
  ];
  for (const root of scanRoots) {
    if (!fs.existsSync(root)) continue;
    const stack = [root];
    while (stack.length) {
      const dir = stack.pop();
      let names;
      try {
        names = fs.readdirSync(dir);
      } catch {
        continue;
      }
      for (const name of names) {
        const p = path.join(dir, name);
        let st;
        try {
          st = fs.statSync(p);
        } catch {
          continue;
        }
        if (st.isDirectory()) {
          if (p.includes("presence-holdout-v3-candidates")) continue;
          stack.push(p);
          continue;
        }
        if (!name.endsWith(".json")) continue;
        // Assisted review exports / monitoring dumps often store responseId/textHash
        if (
          !/assisted|export|classifier|golden|holdout|presence|monitoring|fixture/i.test(
            name
          ) &&
          !dir.includes("classifier")
        ) {
          continue;
        }
        try {
          const doc = JSON.parse(fs.readFileSync(p, "utf8"));
          const rows = [
            ...(doc.cases || []),
            ...(doc.responses || []),
            ...(doc.items || []),
            ...(doc.proposals ? Object.values(doc.proposals) : []),
          ];
          for (const c of rows) {
            if (!c || typeof c !== "object") continue;
            if (c.caseId) caseIds.add(c.caseId);
            if (c.responseId) responseIds.add(c.responseId);
            if (c.sourceResponseId) responseIds.add(c.sourceResponseId);
            if (c.responseHash) hashes.add(c.responseHash);
            if (c.textHash) hashes.add(c.textHash);
            addText(c.rawText || c.text || c.rawResponseExcerpt || "");
          }
        } catch {
          // skip
        }
      }
    }
  }

  // Prior presence validation responses dir (v1 + openai batches)
  const priorResponses = presenceValidationPaths().responsesDir;
  if (fs.existsSync(priorResponses)) {
    for (const f of fs.readdirSync(priorResponses).filter((x) => x.endsWith(".json"))) {
      try {
        const r = JSON.parse(fs.readFileSync(path.join(priorResponses, f), "utf8"));
        if (r.responseId) responseIds.add(r.responseId);
        if (r.textHash) hashes.add(r.textHash);
        else addText(r.rawText || "");
      } catch {
        // skip
      }
    }
  }

  return {
    hashes,
    responseIds,
    caseIds,
    LEAKAGE_SOURCES: [
      "golden_v1",
      "golden_v2",
      "presence_validation_pool_v1",
      "presence_openai_batch",
      "presence_reserve",
      "holdout_v1",
      "holdout_v2",
      "classifier_lab",
      "regression_fixtures",
      "assisted_review_exports",
    ],
  };
}

export function buildPresenceHoldoutV3Plan() {
  const availability = detectProviderAvailability();
  const prompts = selectPresenceHoldoutV3Prompts();
  const providers = ["openai", "gemini", "perplexity", "claude"];
  const missingProviders = providers.filter((p) => !availability[p]);
  const slots = buildExecutionSlots(prompts, availability, {
    batchId: HOLDOUT_V3_BATCH_ID,
    providersOnly: providers,
  });

  const byProvider = {
    openai: slots.filter((s) => s.provider === "openai").length,
    gemini: slots.filter((s) => s.provider === "gemini").length,
    perplexity: slots.filter((s) => s.provider === "perplexity").length,
    claude: slots.filter((s) => s.provider === "claude").length,
  };
  const byLanguage = {
    en: slots.filter((s) => s.prompt.language === "en").length,
    es: slots.filter((s) => s.prompt.language === "es").length,
  };
  const byGeography = slots.reduce((acc, s) => {
    const g = s.prompt.geography || "Global";
    acc[g] = (acc[g] || 0) + 1;
    return acc;
  }, {});

  const estimated = slots.length * HOLDOUT_V3_EST_USD_PER_CALL;
  const leakage = buildHoldoutV3LeakageIndex();
  const allProvidersReady = missingProviders.length === 0;
  const withinCap = estimated <= HOLDOUT_V3_COST_CAP_USD;
  const ready =
    allProvidersReady && withinCap && slots.length >= 90 ? "YES" : "NO";

  return {
    phase: "PRESENCE_HOLDOUT_V3_GENERATION_PLAN_READY",
    batchId: HOLDOUT_V3_BATCH_ID,
    version: HOLDOUT_V3_BATCH_VERSION,
    PLANNED_RESPONSES: slots.length,
    EXPECTED_CANDIDATE_PAIRS: "150–180",
    EXPECTED_UNIQUE_RESPONSES: `${slots.length} (target band 90–110)`,
    BY_PROVIDER: byProvider,
    BY_LANGUAGE: byLanguage,
    BY_GEOGRAPHY: byGeography,
    PROMPT_N: prompts.length,
    promptIds: prompts.map((p) => p.promptId),
    FINAL_TARGET: 100,
    TARGET_PRESENT: 60,
    TARGET_NOT_PRESENT: 40,
    TARGET_UNIQUE_RESPONSES: ">=80",
    ESTIMATED_COST: Number(estimated.toFixed(2)),
    COST_CAP: HOLDOUT_V3_COST_CAP_USD,
    EST_USD_PER_CALL: HOLDOUT_V3_EST_USD_PER_CALL,
    providerAvailability: availability,
    missingProviders,
    plannedModels: {
      openai: process.env.AI_VISIBILITY_MODEL || "gpt-5.6",
      gemini: process.env.AI_VISIBILITY_GEMINI_MODEL || "gemini-2.5-flash (adapter default)",
      perplexity: process.env.AI_VISIBILITY_PERPLEXITY_MODEL || "sonar (adapter default)",
      claude: process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude adapter default",
    },
    LEAKAGE_CHECK_READY: leakage.hashes.size > 0 ? "YES" : "NO",
    LEAKAGE_INDEX_SIZES: {
      hashes: leakage.hashes.size,
      responseIds: leakage.responseIds.size,
      caseIds: leakage.caseIds.size,
    },
    LEAKAGE_SOURCES: leakage.LEAKAGE_SOURCES,
    HOLDOUT_V3_SELECTION_ALLOWED: "NO",
    HOLDOUT_V3_FREEZE_ALLOWED: "NO",
    HOLDOUT_V3_SCORING_ALLOWED: "NO",
    READY_TO_RUN: ready,
    stopReasons: [
      ...(!allProvidersReady
        ? [`MISSING_PROVIDERS:${missingProviders.join(",")}`]
        : []),
      ...(!withinCap ? [`COST_EXCEEDS_CAP:${estimated}>${HOLDOUT_V3_COST_CAP_USD}`] : []),
      ...(slots.length < 90 ? [`INSUFFICIENT_SLOTS:${slots.length}`] : []),
    ],
    hardGuards: {
      HOLDOUT_V2_CHANGES: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      HOLDOUT_V3_SELECTION: 0,
      HOLDOUT_V3_FREEZE: 0,
      HOLDOUT_V3_SCORING: 0,
      REGIONALIZATION_EXECUTION: 0,
      AIRTABLE_WRITES: 0,
    },
    slots,
  };
}

/**
 * Live generation into Holdout v3 candidate store (separate from v1 pool files,
 * then merged into review pool for the existing review UI).
 */
export async function generatePresenceHoldoutV3Responses(options = {}) {
  ensureDirs();
  const paths = presenceHoldoutV3Paths();
  const plan = buildPresenceHoldoutV3Plan();
  if (plan.READY_TO_RUN !== "YES" && options.force !== true) {
    return { ok: false, status: "NOT_READY", plan };
  }
  if (!isAiVisibilityLiveTestAllowed() && options.requireLiveFlag !== false) {
    return {
      ok: false,
      status: "BLOCKED_LIVE_FLAG",
      message: "Set AI_VISIBILITY_LIVE_TEST=true",
      plan,
    };
  }

  const leakage = buildHoldoutV3LeakageIndex();
  const slots = plan.slots;
  const estPerCall = HOLDOUT_V3_EST_USD_PER_CALL;
  const costCap = HOLDOUT_V3_COST_CAP_USD;
  const results = [];
  let spent = 0;
  let duplicatesRejected = 0;
  let leakageRejected = 0;
  const modelsUsed = {};

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
      if (existing.status === "ok" || existing.status === "rejected") {
        results.push(existing);
        spent += Number(existing.estimatedCostUsd) || estPerCall;
        if (existing.model) {
          modelsUsed[existing.provider] = modelsUsed[existing.provider] || new Set();
          modelsUsed[existing.provider].add(existing.model);
        }
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
        `presval_v3_${sha256(slot.slotId + startedAt).slice(0, 16)}`;

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
        batchId: HOLDOUT_V3_BATCH_ID,
        HOLDOUT_V3_CANDIDATE_ONLY: true,
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
        promptFamily: slot.prompt.promptFamily,
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
      if (!rejected && textHash) leakage.hashes.add(textHash);
      if (!rejected && responseId) leakage.responseIds.add(responseId);
      if (record.model) {
        modelsUsed[record.provider] = modelsUsed[record.provider] || new Set();
        modelsUsed[record.provider].add(record.model);
      }
    } catch (err) {
      spent += estPerCall;
      record = {
        batchId: HOLDOUT_V3_BATCH_ID,
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
  const modelsUsedPlain = Object.fromEntries(
    Object.entries(modelsUsed).map(([k, v]) => [k, [...v]])
  );

  const manifest = {
    batchId: HOLDOUT_V3_BATCH_ID,
    version: HOLDOUT_V3_BATCH_VERSION,
    generatedAt: new Date().toISOString(),
    PLANNED_RESPONSES: slots.length,
    COMPLETED_OK: okResponses.length,
    ERRORS: results.filter((r) => r.status === "error").length,
    REJECTED: results.filter((r) => r.status === "rejected").length,
    DUPLICATES_REJECTED: duplicatesRejected,
    LEAKAGE_REJECTED: leakageRejected,
    ESTIMATED_SPENT_USD: Number(spent.toFixed(4)),
    COST_CAP_USD: costCap,
    ACTUAL_MODELS_USED: modelsUsedPlain,
    UNIQUE_RESPONSE_N: uniqueResponseIds(
      okResponses.map((r) => ({ sourceResponseId: r.responseId }))
    ).size,
    HOLDOUT_V3_SELECTION: 0,
    HOLDOUT_V3_FREEZE: 0,
    HOLDOUT_V3_SCORING: 0,
  };

  fs.writeFileSync(paths.generationManifest, JSON.stringify(manifest, null, 2) + "\n");

  // Build candidates into v3 store via temporary path swap through merge API:
  // write responses are already in v3 dir; buildPresenceValidationCandidates reads
  // from shared paths — so build inline here by importing builder with okResponses
  // and then writing to both v3 + shared review pool.
  const cand = buildPresenceValidationCandidates(okResponses, {
    mergeWithExisting: true,
    replaceBatchId: HOLDOUT_V3_BATCH_ID,
    primaryQueueTarget: Math.min(180, Math.max(150, okResponses.length * 2)),
  });

  // Persist v3-only snapshot
  const v3Only = {
    ...cand,
    version: HOLDOUT_V3_BATCH_VERSION,
    batchId: HOLDOUT_V3_BATCH_ID,
    cases: (cand.cases || []).filter((c) => c.batchId === HOLDOUT_V3_BATCH_ID),
  };
  v3Only.TOTAL_CANDIDATES = v3Only.cases.length;
  v3Only.CANDIDATE_PAIR_N = v3Only.cases.length;
  v3Only.UNIQUE_RESPONSE_N = uniqueResponseIds(v3Only.cases).size;
  v3Only.POTENTIAL_TRUE = v3Only.cases.filter((c) => c.candidateType === "PRESENCE_TRUE")
    .length;
  v3Only.POTENTIAL_FALSE = v3Only.cases.filter((c) => c.candidateType === "PRESENCE_FALSE")
    .length;
  v3Only.PRIMARY_REVIEW_QUEUE = v3Only.cases.filter((c) => c.primaryReviewQueue).length;
  fs.writeFileSync(paths.candidatesPath, JSON.stringify(v3Only, null, 2) + "\n");
  if (!fs.existsSync(paths.reviewsPath)) {
    fs.writeFileSync(
      paths.reviewsPath,
      JSON.stringify({ version: "presence_holdout_v3_reviews_v1", reviews: {} }, null, 2) +
        "\n"
    );
  }

  // Point review UI at merged pool (existing path) but mark active batch
  fs.writeFileSync(
    paths.activePoolMarker,
    JSON.stringify(
      {
        activePool: "merged_with_holdout_v3",
        holdoutV3BatchId: HOLDOUT_V3_BATCH_ID,
        reviewRoute: "/ai-intelligence-presence-validation-review",
        note: "Review primary queue prioritizes fresh Holdout v3 candidates. Do not select/freeze/score yet.",
        updatedAt: new Date().toISOString(),
      },
      null,
      2
    ) + "\n"
  );

  return {
    ok: true,
    manifest,
    okResponses,
    candidates: v3Only,
    mergedCandidateN: cand.TOTAL_CANDIDATES,
  };
}

export function classifyNegativeControlCategories(cases) {
  const cats = {
    PARENT_CHILD: 0,
    SIBLING: 0,
    GENERIC_COLLECTION: 0,
    GEOGRAPHIC_PLAYA: 0,
    ORDINARY_LANGUAGE_FALSE_FRIEND: 0,
    SIMILAR_NAME: 0,
    SHORT_NAME_AMBIGUITY: 0,
    PARENT_CONTEXT_WITHOUT_TARGET: 0,
    NO_ENTITY_OCCURRENCE: 0,
    OTHER: 0,
  };
  for (const c of cases || []) {
    if (c.candidateType !== "PRESENCE_FALSE") continue;
    const rat = String(c.systemSuggestionRationale || "").toLowerCase();
    if (/playa/.test(rat)) cats.GEOGRAPHIC_PLAYA += 1;
    else if (/sibling/.test(rat)) cats.SIBLING += 1;
    else if (/parent/.test(rat)) cats.PARENT_CHILD += 1;
    else if (/generic collection/.test(rat)) cats.GENERIC_COLLECTION += 1;
    else if (/hard negative|absent|canonical brand absent/.test(rat)) {
      cats.NO_ENTITY_OCCURRENCE += 1;
    } else cats.OTHER += 1;
  }
  return cats;
}
