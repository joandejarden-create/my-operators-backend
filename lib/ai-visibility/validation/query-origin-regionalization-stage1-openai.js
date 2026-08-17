/**
 * QUERY_ORIGIN_REGIONALIZATION_EXPERIMENT_V1 — Stage 1 OpenAI only.
 * Research experiment — not production. No Recommended/First metrics.
 */

import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { runVisibilityPrompt } from "../providers/index.js";
import { buildOpenAiWebSearchUserLocation } from "../providers/openai.js";
import { isAiVisibilityLiveTestAllowed } from "../config.js";
import { findEntitySpans } from "../normalize-entities.js";
import { buildGoldenSetScoringEntityIndex } from "./golden-set-entity-index.js";
import { getPresenceProductionCertificationStatus } from "../presence-product-certification.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const EXPERIMENT_ID = "QUERY_ORIGIN_REGIONALIZATION_EXPERIMENT_V1";
export const EXPERIMENT_STAGE = "STAGE_1_OPENAI";
export const EXECUTION_ORDER_VERSION = "query_origin_stage1_interleave_v1";
export const EXECUTION_SEED = "query_origin_stage1_openai_20260815";
export const COST_CAP_USD = 100;
export const EST_USD_PER_CALL = 0.28;
export const REPEAT_COUNT = 3;
export const ASSET_GEOGRAPHY = "Mexico";
export const DEFAULT_MODEL = "gpt-4.1";

export const ORIGINS = Object.freeze([
  {
    queryOriginGeography: "US_NORTHEAST",
    referenceCity: "New York",
    displayOrigin: "NEW_YORK",
    providerLocationContext: {
      type: "approximate",
      city: "New York",
      region: "New York",
      country: "US",
      timezone: "America/New_York",
    },
  },
  {
    queryOriginGeography: "US_SOUTHEAST",
    referenceCity: "Miami",
    displayOrigin: "MIAMI",
    providerLocationContext: {
      type: "approximate",
      city: "Miami",
      region: "Florida",
      country: "US",
      timezone: "America/New_York",
    },
  },
  {
    queryOriginGeography: "MEXICO",
    referenceCity: "Mexico City",
    displayOrigin: "MEXICO_CITY",
    providerLocationContext: {
      type: "approximate",
      city: "Mexico City",
      region: "Ciudad de México",
      country: "MX",
      timezone: "America/Mexico_City",
    },
  },
  {
    queryOriginGeography: "SPAIN",
    referenceCity: "Madrid",
    displayOrigin: "MADRID",
    providerLocationContext: {
      type: "approximate",
      city: "Madrid",
      region: "Madrid",
      country: "ES",
      timezone: "Europe/Madrid",
    },
  },
]);

/** 8 families × EN+ES Mexico twins from governed showcase seed. */
export const STAGE1_PROMPT_PAIRS = Object.freeze([
  {
    family: "Conversion",
    en: "p_mx_existing_asset_reposition_v1",
    es: "p_mx_existing_asset_reposition_es_v1",
  },
  {
    family: "Soft Brand / Collection",
    en: "p_mx_soft_brand_shortlist_v1",
    es: "p_mx_soft_brand_shortlist_es_v1",
  },
  {
    family: "Lifestyle",
    en: "p_mx_lifestyle_strategy_v1",
    es: "p_mx_lifestyle_strategy_es_v1",
  },
  {
    family: "Upper-Upscale",
    en: "p_mx_uu_owner_shortlist_v1",
    es: "p_mx_uu_owner_shortlist_es_v1",
  },
  {
    family: "Owner Flexibility",
    en: "p_mx_affiliation_flexibility_v1",
    es: "p_mx_affiliation_flexibility_es_v1",
  },
  {
    family: "Branded Residences",
    en: "p_mx_residences_capability_v1",
    es: "p_mx_residences_capability_es_v1",
  },
  {
    family: "Brand Selection",
    en: "p_mx_collection_affiliation_v1",
    es: "p_mx_collection_affiliation_es_v1",
  },
  {
    family: "Development Strategy",
    en: "p_mx_uu_positioning_strategy_v1",
    es: "p_mx_uu_positioning_strategy_es_v1",
  },
]);

function sha256(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex");
}

function seededRank(seed, key) {
  return sha256(`${seed}::${key}`);
}

function loadShowcasePrompts() {
  const seedPath = path.join(
    ROOT,
    "fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json"
  );
  const seed = JSON.parse(fs.readFileSync(seedPath, "utf8"));
  const byId = new Map();
  for (const p of seed.prompts || []) {
    if (p.active === false || !p.promptText || !p.promptId) continue;
    byId.set(p.promptId, p);
  }
  return byId;
}

/**
 * Build Stage 1 plan — recalculate exact call count from governed prompts.
 */
export function buildStage1OpenAiPlan(options = {}) {
  const cert = getPresenceProductionCertificationStatus();
  const byId = loadShowcasePrompts();
  const model = options.model || process.env.AI_VISIBILITY_MODEL || DEFAULT_MODEL;
  const prompts = [];
  const missing = [];

  for (const pair of STAGE1_PROMPT_PAIRS) {
    for (const [lang, id] of [
      ["en", pair.en],
      ["es", pair.es],
    ]) {
      const p = byId.get(id);
      if (!p) {
        missing.push(id);
        continue;
      }
      prompts.push({
        promptId: id,
        promptFamily: pair.family,
        language: lang,
        promptText: p.promptText,
        promptVersion: p.version || "1",
      });
    }
  }

  const promptN = prompts.length;
  const plannedCalls = promptN * ORIGINS.length * REPEAT_COUNT;
  const estimatedCost = Math.round(plannedCalls * EST_USD_PER_CALL * 100) / 100;
  const withinCap = estimatedCost <= COST_CAP_USD && missing.length === 0;
  const presenceReady = cert.CERTIFICATION_STATUS === "PRODUCTION_VALIDATED";

  // Deterministic interleaved execution order
  const slots = [];
  for (const prompt of prompts) {
    for (let repeatIndex = 1; repeatIndex <= REPEAT_COUNT; repeatIndex += 1) {
      for (const origin of ORIGINS) {
        slots.push({
          promptId: prompt.promptId,
          promptFamily: prompt.promptFamily,
          language: prompt.language,
          promptText: prompt.promptText,
          promptVersion: prompt.promptVersion,
          repeatIndex,
          queryOriginGeography: origin.queryOriginGeography,
          displayOrigin: origin.displayOrigin,
          referenceCity: origin.referenceCity,
          providerLocationContext: origin.providerLocationContext,
        });
      }
    }
  }
  slots.sort((a, b) => {
    // Interleave by (repeat, family, language, origin) via seeded hash — not all NY first
    const ka = [
      a.repeatIndex,
      a.promptFamily,
      a.language,
      a.queryOriginGeography,
      a.promptId,
    ].join("|");
    const kb = [
      b.repeatIndex,
      b.promptFamily,
      b.language,
      b.queryOriginGeography,
      b.promptId,
    ].join("|");
    const ra = seededRank(EXECUTION_SEED, ka);
    const rb = seededRank(EXECUTION_SEED, kb);
    if (ra !== rb) return ra.localeCompare(rb);
    return ka.localeCompare(kb);
  });

  const plan = {
    phase: "QUERY_ORIGIN_STAGE1_OPENAI_PLAN_READY",
    experimentId: EXPERIMENT_ID,
    experimentStage: EXPERIMENT_STAGE,
    ASSET_GEOGRAPHY,
    ORIGINS: ORIGINS.map((o) => o.displayOrigin),
    queryOriginGeographies: ORIGINS.map((o) => o.queryOriginGeography),
    PROMPT_FAMILIES: STAGE1_PROMPT_PAIRS.map((p) => p.family),
    LANGUAGES: ["en", "es"],
    REPEAT_COUNT,
    promptCount: promptN,
    originCount: ORIGINS.length,
    PLANNED_CALLS: plannedCalls,
    ESTIMATED_COST: estimatedCost,
    COST_CAP: COST_CAP_USD,
    EST_USD_PER_CALL,
    MODEL: model,
    EXECUTION_SEED,
    executionOrderVersion: EXECUTION_ORDER_VERSION,
    PROVIDER: "openai",
    GEMINI_REGIONALIZATION_CALLS: 0,
    CLAUDE_REGIONALIZATION_CALLS: 0,
    PERPLEXITY_REGIONALIZATION_CALLS: 0,
    missingPromptIds: missing,
    presenceCertification: cert.CERTIFICATION_STATUS,
    READY_TO_RUN: withinCap && presenceReady && missing.length === 0 ? "YES" : "NO",
    stopReasons: [
      ...(presenceReady ? [] : ["PRESENCE_NOT_PRODUCTION_VALIDATED"]),
      ...(withinCap ? [] : ["ESTIMATED_COST_EXCEEDS_CAP"]),
      ...(missing.length ? [`MISSING_PROMPTS:${missing.join(",")}`] : []),
    ],
    slots,
    note: "Identical prompt text across origins; only providerLocationContext changes. Research only — not production UI.",
  };

  return plan;
}

function domainFromUrl(url) {
  try {
    const u = new URL(String(url));
    return u.hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return null;
  }
}

/**
 * Execute Stage 1 OpenAI experiment (provider calls).
 */
export async function executeStage1OpenAiExperiment(options = {}) {
  const plan = options.plan || buildStage1OpenAiPlan(options);
  if (plan.READY_TO_RUN !== "YES") {
    return {
      ok: false,
      phase: "QUERY_ORIGIN_STAGE1_OPENAI_BLOCKED",
      plan,
      PROVIDER_CALLS: 0,
    };
  }
  if (!isAiVisibilityLiveTestAllowed() && !options.forceLive) {
    return {
      ok: false,
      phase: "QUERY_ORIGIN_STAGE1_OPENAI_BLOCKED",
      reason: "AI_VISIBILITY_LIVE_TEST not allowed",
      plan,
      PROVIDER_CALLS: 0,
    };
  }

  const openaiKey = String(process.env.OPENAI_API_KEY || "").trim();
  const fddKey = String(process.env.FDD_INTELLIGENCE_MODEL_API_KEY || "").trim();
  if (!openaiKey && fddKey) {
    process.env.OPENAI_API_KEY = fddKey;
  }
  if (!String(process.env.OPENAI_API_KEY || "").trim()) {
    return {
      ok: false,
      phase: "QUERY_ORIGIN_STAGE1_OPENAI_BLOCKED",
      reason: "OPENAI_API_KEY not configured",
      plan,
      PROVIDER_CALLS: 0,
    };
  }

  const outDir = path.join(
    ROOT,
    "data/ai-visibility/experiments/query-origin-regionalization-stage1-openai"
  );
  fs.mkdirSync(path.join(outDir, "observations"), { recursive: true });

  const observations = [];
  let success = 0;
  let failed = 0;
  let actualCost = 0;
  const model = plan.MODEL;
  const startedAt = new Date().toISOString();

  for (let i = 0; i < plan.slots.length; i += 1) {
    const slot = plan.slots[i];
    const observationId = `qor_s1_${String(i + 1).padStart(4, "0")}_${sha256(
      `${slot.promptId}|${slot.queryOriginGeography}|${slot.repeatIndex}`
    ).slice(0, 10)}`;
    const requestTimestamp = new Date().toISOString();
    let observation = {
      experimentId: EXPERIMENT_ID,
      experimentStage: EXPERIMENT_STAGE,
      observationId,
      executionIndex: i,
      provider: "openai",
      model,
      assetGeography: ASSET_GEOGRAPHY,
      queryOriginGeography: slot.queryOriginGeography,
      displayOrigin: slot.displayOrigin,
      providerLocationContext: slot.providerLocationContext,
      language: slot.language,
      promptId: slot.promptId,
      promptFamily: slot.promptFamily,
      promptVersion: slot.promptVersion,
      promptText: slot.promptText,
      repeatIndex: slot.repeatIndex,
      requestTimestamp,
      success: false,
    };

    try {
      const result = await runVisibilityPrompt({
        prompt: { text: slot.promptText, promptId: slot.promptId },
        model,
        context: {
          providerLocationContext: slot.providerLocationContext,
        },
        enableWebSearch: true,
      });
      const responseTimestamp = new Date().toISOString();
      const cost =
        typeof result.usage?.totalTokens === "number"
          ? Math.max(0.05, (result.usage.totalTokens / 1e6) * 15)
          : EST_USD_PER_CALL;
      actualCost += cost;
      success += 1;
      observation = {
        ...observation,
        success: true,
        responseTimestamp,
        rawResponse: result.text || "",
        citations: result.citations || [],
        sourceDomains: [
          ...new Set(
            (result.citations || [])
              .map((c) => domainFromUrl(c.url))
              .filter(Boolean)
          ),
        ],
        usage: result.usage || null,
        cost,
        providerMeta: result.providerMeta || null,
        latencyMs: result.latencyMs ?? null,
      };
    } catch (err) {
      failed += 1;
      observation = {
        ...observation,
        success: false,
        responseTimestamp: new Date().toISOString(),
        error: String(err?.message || err),
        cost: 0,
      };
    }

    observations.push(observation);
    fs.writeFileSync(
      path.join(outDir, "observations", `${observationId}.json`),
      JSON.stringify(observation, null, 2) + "\n",
      "utf8"
    );

    // Light pacing to reduce 429s
    if (i < plan.slots.length - 1) {
      await new Promise((r) => setTimeout(r, options.delayMs ?? 400));
    }
  }

  const completedAt = new Date().toISOString();
  const runManifest = {
    experimentId: EXPERIMENT_ID,
    experimentStage: EXPERIMENT_STAGE,
    startedAt,
    completedAt,
    MODEL: model,
    PLANNED_CALLS: plan.PLANNED_CALLS,
    SUCCESSFUL_CALLS: success,
    FAILED_CALLS: failed,
    ACTUAL_COST: Math.round(actualCost * 100) / 100,
    EXECUTION_SEED,
    executionOrderVersion: EXECUTION_ORDER_VERSION,
    observationIds: observations.map((o) => o.observationId),
  };
  fs.writeFileSync(
    path.join(outDir, "run-manifest.json"),
    JSON.stringify(runManifest, null, 2) + "\n",
    "utf8"
  );

  return {
    ok: true,
    phase: "QUERY_ORIGIN_STAGE1_OPENAI_EXECUTION_COMPLETE",
    outDir,
    runManifest,
    observations,
    plan,
  };
}

/**
 * Analyze Stage 1 results — Presence / brand-set / source only.
 */
export function analyzeStage1OpenAiResults(observations, options = {}) {
  const index = options.aliasIndex
    ? { aliasIndex: options.aliasIndex }
    : buildGoldenSetScoringEntityIndex({});
  const aliasIndex = index.aliasIndex;

  const successful = (observations || []).filter((o) => o.success && o.rawResponse);
  const byOrigin = {};
  for (const o of ORIGINS) {
    byOrigin[o.displayOrigin] = {
      queryOriginGeography: o.queryOriginGeography,
      observations: [],
      brandPresence: new Map(), // brand -> { presentN, totalN, byPromptRepeat }
      domains: new Map(),
    };
  }

  for (const o of successful) {
    const originKey = o.displayOrigin || o.queryOriginGeography;
    const bucket = byOrigin[originKey] || byOrigin[Object.keys(byOrigin)[0]];
    if (!byOrigin[originKey]) continue;
    const spans = findEntitySpans(String(o.rawResponse || ""), aliasIndex);
    const presentBrands = new Map();
    for (const s of spans) {
      const name = s.entity?.name;
      const id = s.entity?.id;
      if (!name) continue;
      const key = id || name;
      if (!presentBrands.has(key)) {
        presentBrands.set(key, { brandId: id, brandName: name });
      }
    }

    bucket.observations.push({
      observationId: o.observationId,
      promptId: o.promptId,
      promptFamily: o.promptFamily,
      language: o.language,
      repeatIndex: o.repeatIndex,
      brandCount: presentBrands.size,
      brands: [...presentBrands.values()],
      domains: o.sourceDomains || [],
    });

    // Track per-brand presence across all successful observations for this origin
    // Denominator = all successful obs for origin; also track per prompt×repeat cell
    for (const [, b] of presentBrands) {
      const k = b.brandName;
      if (!bucket.brandPresence.has(k)) {
        bucket.brandPresence.set(k, {
          brandName: k,
          brandId: b.brandId,
          presentN: 0,
          cells: new Set(),
        });
      }
      const row = bucket.brandPresence.get(k);
      row.presentN += 1;
      row.cells.add(`${o.promptId}|${o.repeatIndex}`);
    }
    for (const d of o.sourceDomains || []) {
      bucket.domains.set(d, (bucket.domains.get(d) || 0) + 1);
    }
  }

  const originKeys = ORIGINS.map((o) => o.displayOrigin);
  const presenceByOrigin = {};
  const allBrands = new Set();
  for (const origin of originKeys) {
    const b = byOrigin[origin];
    const totalN = b.observations.length;
    const brands = {};
    for (const [name, row] of b.brandPresence) {
      allBrands.add(name);
      brands[name] = {
        brandName: name,
        presentN: row.presentN,
        totalN,
        presenceRate: totalN > 0 ? row.presentN / totalN : null,
        presenceRatePct:
          totalN > 0 ? `${((row.presentN / totalN) * 100).toFixed(1)}%` : null,
        cellsPresent: row.cells.size,
      };
    }
    presenceByOrigin[origin] = { totalN, brands };
  }

  // Top brands by max-min delta
  const brandDeltas = [];
  for (const brand of allBrands) {
    const rates = {};
    let max = -1;
    let min = 2;
    for (const origin of originKeys) {
      const row = presenceByOrigin[origin].brands[brand];
      const rate = row?.presenceRate ?? 0;
      rates[origin] = {
        presentN: row?.presentN || 0,
        totalN: presenceByOrigin[origin].totalN,
        presenceRate: rate,
      };
      if (rate > max) max = rate;
      if (rate < min) min = rate;
    }
    const deltaPp = (max - min) * 100;
    brandDeltas.push({
      brandName: brand,
      rates,
      MAX_MIN_PRESENCE_DELTA_PP: Math.round(deltaPp * 10) / 10,
    });
  }
  brandDeltas.sort(
    (a, b) => b.MAX_MIN_PRESENCE_DELTA_PP - a.MAX_MIN_PRESENCE_DELTA_PP
  );

  // Brand sets by origin
  const brandSets = {};
  for (const origin of originKeys) {
    brandSets[origin] = Object.keys(presenceByOrigin[origin].brands).sort();
  }
  const brandSetOverlap = {};
  const brandsUniqueToOrigin = {};
  for (const origin of originKeys) {
    const set = new Set(brandSets[origin]);
    const unique = [...set].filter((b) =>
      originKeys.every((o) => o === origin || !brandSets[o].includes(b))
    );
    brandsUniqueToOrigin[origin] = unique;
    brandSetOverlap[origin] = {};
    for (const other of originKeys) {
      if (other === origin) continue;
      const otherSet = new Set(brandSets[other]);
      const inter = [...set].filter((b) => otherSet.has(b));
      brandSetOverlap[origin][other] = {
        overlapN: inter.length,
        overlap: inter.slice(0, 30),
      };
    }
  }

  // Source domains
  const sourceByOrigin = {};
  for (const origin of originKeys) {
    const domains = [...byOrigin[origin].domains.entries()]
      .map(([domain, frequency]) => ({ domain, frequency }))
      .sort((a, b) => b.frequency - a.frequency);
    sourceByOrigin[origin] = domains;
  }
  const sourceUnique = {};
  for (const origin of originKeys) {
    const set = new Set(sourceByOrigin[origin].map((d) => d.domain));
    sourceUnique[origin] = [...set].filter((d) =>
      originKeys.every(
        (o) => o === origin || !sourceByOrigin[o].some((x) => x.domain === d)
      )
    );
  }

  // Repeatability: for top delta brands, classify consistency
  const repeatability = brandDeltas.slice(0, 15).map((b) => {
    const originRates = originKeys.map((o) => b.rates[o].presenceRate);
    const nonzero = originRates.filter((r) => r > 0).length;
    let descriptor = "OBSERVED_ONCE";
    if (b.MAX_MIN_PRESENCE_DELTA_PP >= 25 && nonzero >= 2) {
      descriptor = "OBSERVED_ACROSS_REPEATS";
    }
    // Check consistency across prompt families for max origin
    let maxOrigin = originKeys[0];
    let maxRate = -1;
    for (const o of originKeys) {
      if (b.rates[o].presenceRate > maxRate) {
        maxRate = b.rates[o].presenceRate;
        maxOrigin = o;
      }
    }
    const cells = byOrigin[maxOrigin]?.brandPresence.get(b.brandName)?.cells.size || 0;
    if (cells >= 4 && b.MAX_MIN_PRESENCE_DELTA_PP >= 20) {
      descriptor = "CONSISTENT_ACROSS_PROMPTS";
    }
    return {
      brandName: b.brandName,
      MAX_MIN_PRESENCE_DELTA_PP: b.MAX_MIN_PRESENCE_DELTA_PP,
      rates: b.rates,
      descriptor,
    };
  });

  const materialCount = brandDeltas.filter((b) => b.MAX_MIN_PRESENCE_DELTA_PP >= 25)
    .length;
  const consistentCount = repeatability.filter(
    (r) =>
      r.descriptor === "CONSISTENT_ACROSS_PROMPTS" ||
      r.descriptor === "OBSERVED_ACROSS_REPEATS"
  ).length;

  const plannedCalls = Number(options.plannedCalls || 0) || 0;
  const successfulN = successful.length;
  const minSuccessful =
    plannedCalls > 0 ? Math.ceil(plannedCalls * 0.9) : Math.max(1, successfulN);
  const inconclusive = successfulN < minSuccessful;

  let decision = "NO_MEANINGFUL_REPEATABLE_DIFFERENCE";
  let nextStep = "QUERY_ORIGIN_REMAINS_RESEARCH_ONLY";
  if (inconclusive) {
    decision = "INCONCLUSIVE_INSUFFICIENT_SUCCESSFUL_CALLS";
    nextStep = "RERUN_STAGE_1_AFTER_PROVIDER_FIX";
  } else if (materialCount >= 3 && consistentCount >= 2) {
    decision = "MATERIAL_REPEATABLE_QUERY_ORIGIN_EFFECT_OBSERVED";
    nextStep = "READY_FOR_STAGE_2_CLAUDE_QUERY_ORIGIN_REPLICATION";
  } else if (materialCount >= 1 || consistentCount >= 1) {
    decision = "SOME_DIFFERENCES_REQUIRE_REPLICATION";
    nextStep = "QUERY_ORIGIN_REMAINS_RESEARCH_ONLY";
  }

  return {
    presenceByOrigin,
    brandDeltas: brandDeltas.slice(0, 40),
    brandSets,
    brandSetOverlap,
    brandsUniqueToOrigin,
    sourceByOrigin,
    sourceDomainsUniqueToOrigin: sourceUnique,
    repeatability,
    decision,
    nextStep,
    inconclusive,
    successfulObservationN: successfulN,
    minSuccessfulForDecision: minSuccessful,
    materialBrandDeltaCount: materialCount,
    consistentDifferenceCount: consistentCount,
  };
}

export function writeStage1Artifacts({ plan, runManifest, observations, analysis }) {
  const resultsPath = path.join(
    ROOT,
    "data/ai-visibility/validation/query-origin-regionalization-stage1-openai-results.json"
  );
  const summaryPath = path.join(
    ROOT,
    "data/ai-visibility/validation/query-origin-regionalization-stage1-openai-summary.md"
  );

  const results = {
    phase: "QUERY_ORIGIN_STAGE1_OPENAI_COMPLETE",
    experimentId: EXPERIMENT_ID,
    experimentStage: EXPERIMENT_STAGE,
    ASSET_GEOGRAPHY,
    MODEL: runManifest?.MODEL || plan.MODEL,
    OBSERVATIONS: observations.length,
    SUCCESSFUL_CALLS: runManifest?.SUCCESSFUL_CALLS ?? 0,
    FAILED_CALLS: runManifest?.FAILED_CALLS ?? 0,
    ACTUAL_COST: runManifest?.ACTUAL_COST ?? 0,
    PLANNED_CALLS: plan.PLANNED_CALLS,
    EXECUTION_SEED,
    analysis,
    hardGuards: {
      RECOMMENDED_PRODUCTION_ENABLE: 0,
      FIRST_RECOMMENDATION_PRODUCTION_ENABLE: 0,
      NEGATIVE_PRODUCTION_ENABLE: 0,
      COMPARATOR_PRODUCTION_ENABLE: 0,
      HOLDOUT_V3_CHANGES: 0,
      HOLDOUT_V3_RESCORE: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      GROUND_TRUTH_CHANGES: 0,
      GEMINI_REGIONALIZATION_CALLS: 0,
      CLAUDE_REGIONALIZATION_CALLS: 0,
      PERPLEXITY_REGIONALIZATION_CALLS: 0,
      ARBITRARY_REGIONALIZATION_SCORE: 0,
      AIRTABLE_RAW_RESPONSE_WRITES: 0,
    },
  };

  fs.mkdirSync(path.dirname(resultsPath), { recursive: true });
  fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2) + "\n", "utf8");

  const top = (analysis.brandDeltas || []).slice(0, 10);
  const md = [
    `# Query-Origin Regionalization — Stage 1 OpenAI`,
    ``,
    `**Experiment:** ${EXPERIMENT_ID} / ${EXPERIMENT_STAGE}`,
    `**Asset geography:** ${ASSET_GEOGRAPHY}`,
    `**Model:** ${results.MODEL}`,
    `**Calls:** ${results.SUCCESSFUL_CALLS} success / ${results.FAILED_CALLS} failed of ${results.PLANNED_CALLS} planned`,
    `**Actual cost (est.):** $${results.ACTUAL_COST}`,
    ``,
    `## Methodology`,
    ``,
    `- Identical Mexico-asset prompt text across four query origins`,
    `- Only OpenAI \`web_search.user_location\` changes`,
    `- ${plan.PROMPT_FAMILIES.length} families × EN+ES × 4 origins × ${REPEAT_COUNT} repeats`,
    `- Metrics: Presence / brand-set / associated source domains only (no Recommendation / First)`,
    `- No Regionalization Score`,
    ``,
    `## Presence differences (top by MAX−MIN delta)`,
    ``,
    ...top.map(
      (b) =>
        `- **${b.brandName}**: Δ ${b.MAX_MIN_PRESENCE_DELTA_PP} pp — ` +
        ORIGINS.map(
          (o) =>
            `${o.displayOrigin} ${b.rates[o.displayOrigin].presentN}/${b.rates[o.displayOrigin].totalN}`
        ).join("; ")
    ),
    ``,
    `## Decision`,
    ``,
    `**${analysis.decision}**`,
    ``,
    `Next: ${analysis.nextStep}`,
    ``,
    `## Limitations`,
    ``,
    `- OpenAI-only; Claude replication not run`,
    `- Probabilistic LLM outputs — differences need replication`,
    `- Research only — not production Query-Origin Geography`,
    ``,
  ].join("\n");
  fs.writeFileSync(summaryPath, md, "utf8");

  return { resultsPath, summaryPath, results };
}
