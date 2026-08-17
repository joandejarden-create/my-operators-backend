/**
 * Phase 3A.10 — Wave-1 showcase monitoring plan (dry-run / execution governance).
 * No provider calls. Peer v2 locked. 84 prompts.
 */

import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { METRIC_VERSION } from "./config.js";
import { ACTIVE_SHOWCASE_INTENTS } from "./showcase-intents.js";
import { PEER_SET_ID_V2, loadPeerSetConfig, resolvePeerSetMembership } from "./peer-sets.js";
import { validateSemanticPairMembers } from "./semantic-pair.js";
import { validatePromptSeedSet } from "./prompt-validation.js";
import { loadDecisionEligibilityConfig } from "./brand-decision-eligibility.js";
import {
  loadShowcaseCompaniesConfig,
  listShowcaseCompanyKeys,
  getShowcaseCompany,
} from "./brand-ai-showcase-companies.js";

export const WAVE1_SHOWCASE_PLAN_VERSION = "ai_visibility_wave1_showcase_plan_v1";
export const WAVE1_BASELINE_SERIES_ID = "aiv_wave1_openai_peer_v2_showcase_prompts_v1";
export const WAVE1_PEER_SET_ID = PEER_SET_ID_V2;
export const WAVE1_PROVIDER = "openai";

/** Historical OpenAI cost evidence (Phase 3A.9) — USD per call / wave. */
export const WAVE1_COST_EVIDENCE = Object.freeze({
  LOW_PER_CALL: 0.35,
  EXPECTED_PER_CALL: 0.677,
  HIGH_PER_CALL: 1.33,
  PLANNED_CALLS: 84,
  get LOW() {
    return Number((this.PLANNED_CALLS * this.LOW_PER_CALL).toFixed(2));
  },
  get EXPECTED() {
    return Number((this.PLANNED_CALLS * this.EXPECTED_PER_CALL).toFixed(2));
  },
  get HIGH() {
    return Number((this.PLANNED_CALLS * this.HIGH_PER_CALL).toFixed(2));
  },
  /** Recommended hard stop above historical HIGH with buffer. */
  RECOMMENDED_HARD_CAP_USD: 125,
  WARNING_THRESHOLD_USD: 70,
});

/** Bounded retries: 1 retry on retryable errors → max 2 attempts per planned call. */
export const WAVE1_RETRY_POLICY = Object.freeze({
  maxRetriesPerCall: 1,
  maxAttemptsPerCall: 2,
  backoffMs: 1500,
  timeoutMsDefault: 60000,
  plannedCalls: 84,
  maxProviderCalls: 84,
  retryBudget: 84, // at most one retry per planned call
  maxTotalAttempts: 168,
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_SEED = path.join(
  __dirname,
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "phase3a9-showcase-prompt-seed.json"
);

export const WAVE1_EXECUTION_ORDER = Object.freeze([
  { key: "GLOBAL_EN", geographyScope: "Global", commercialRegion: null, country: null, language: "en" },
  { key: "CALA_EN", geographyScope: "Region", commercialRegion: "CALA", country: null, language: "en" },
  { key: "CALA_ES", geographyScope: "Region", commercialRegion: "CALA", country: null, language: "es" },
  { key: "EUROPE_EN", geographyScope: "Region", commercialRegion: "Europe", country: null, language: "en" },
  {
    key: "NORTH_AMERICA_EN",
    geographyScope: "Region",
    commercialRegion: "North America",
    country: null,
    language: "en",
  },
  {
    key: "MEXICO_EN",
    geographyScope: "Country",
    commercialRegion: "CALA",
    country: "Mexico",
    language: "en",
  },
  {
    key: "MEXICO_ES",
    geographyScope: "Country",
    commercialRegion: "CALA",
    country: "Mexico",
    language: "es",
  },
]);

export function geographyKeyFromPrompt(p) {
  if (p.geographyScope === "Global") return "GLOBAL";
  if (p.country) return String(p.country).toUpperCase().replace(/\s+/g, "_");
  if (p.commercialRegion) return String(p.commercialRegion).toUpperCase().replace(/\s+/g, "_");
  return "UNKNOWN";
}

export function slotKeyFromPrompt(p) {
  const geo = geographyKeyFromPrompt(p);
  const lang = String(p.language || "").toLowerCase() === "es" ? "ES" : "EN";
  if (geo === "GLOBAL") return "GLOBAL_EN";
  if (geo === "CALA") return `CALA_${lang}`;
  if (geo === "EUROPE") return "EUROPE_EN";
  if (geo === "NORTH_AMERICA") return "NORTH_AMERICA_EN";
  if (geo === "MEXICO") return `MEXICO_${lang}`;
  return `${geo}_${lang}`;
}

/**
 * Deterministic Wave-1 execution fingerprint (no secrets).
 */
export function buildWave1ExecutionFingerprint(parts = {}) {
  const fields = {
    provider: String(parts.provider || WAVE1_PROVIDER),
    promptId: String(parts.promptId || ""),
    promptVersion: String(parts.promptVersion || parts.version || ""),
    semanticPairId: parts.semanticPairId ? String(parts.semanticPairId) : "",
    geographyKey: String(parts.geographyKey || ""),
    language: String(parts.language || ""),
    intent: String(parts.intent || parts.intentTerritory || ""),
    promptFamily: String(parts.promptFamily || ""),
    peerSetId: String(parts.peerSetId || WAVE1_PEER_SET_ID),
    peerSetVersion: String(parts.peerSetVersion || "2"),
    metricVersion: String(parts.metricVersion || METRIC_VERSION),
    stakeholder: "Brand",
  };
  const canonical = [
    fields.provider,
    fields.promptId,
    fields.promptVersion,
    fields.semanticPairId,
    fields.geographyKey,
    fields.language,
    fields.intent,
    fields.promptFamily,
    fields.peerSetId,
    fields.peerSetVersion,
    fields.metricVersion,
    fields.stakeholder,
  ].join("|");
  const hash = createHash("sha256").update(canonical).digest("hex").slice(0, 24);
  return { ...fields, canonical, fingerprint: hash };
}

export function peerMembershipFingerprint(entityIds = []) {
  const sorted = [...entityIds].map(String).sort();
  return createHash("sha256").update(sorted.join(",")).digest("hex").slice(0, 16);
}

/**
 * Load Wave-1 showcase seed prompts (fixture SSOT for plan validation).
 */
export function loadWave1ShowcasePrompts(seedPath = DEFAULT_SEED) {
  const raw = JSON.parse(fs.readFileSync(seedPath, "utf8"));
  const prompts = raw.prompts || [];
  const validation = validatePromptSeedSet(prompts);
  return { seed: raw, prompts, validation, seedPath };
}

/**
 * Build full Wave-1 dry-run plan from fixture seed.
 */
export function buildWave1ShowcaseDryRunPlan(options = {}) {
  const { prompts, validation, seed, seedPath } = loadWave1ShowcasePrompts(options.seedPath);
  const errors = [];
  const warnings = [];

  if (!validation.ok) {
    errors.push(...(validation.errors || []).slice(0, 20));
  }

  const en = prompts.filter((p) => p.language === "en");
  const es = prompts.filter((p) => p.language === "es");
  if (prompts.length !== 84) errors.push(`prompt_count_${prompts.length}_expected_84`);
  if (en.length !== 60) errors.push(`en_count_${en.length}_expected_60`);
  if (es.length !== 24) errors.push(`es_count_${es.length}_expected_24`);

  // No ES for Global / Europe / NA
  for (const p of es) {
    if (p.geographyScope === "Global") errors.push(`es_global_forbidden:${p.promptId}`);
    if (p.commercialRegion === "Europe") errors.push(`es_europe_forbidden:${p.promptId}`);
    if (p.commercialRegion === "North America" && !p.country) {
      errors.push(`es_na_forbidden:${p.promptId}`);
    }
  }

  const matrix = Object.fromEntries(WAVE1_EXECUTION_ORDER.map((s) => [s.key, 0]));
  for (const p of prompts) {
    const k = slotKeyFromPrompt(p);
    if (matrix[k] == null) {
      errors.push(`unexpected_slot:${k}:${p.promptId}`);
      continue;
    }
    matrix[k] += 1;
  }
  for (const [k, v] of Object.entries(matrix)) {
    if (v !== 12) errors.push(`matrix_${k}_${v}_expected_12`);
  }

  const intentDensity = {};
  for (const intent of ACTIVE_SHOWCASE_INTENTS) {
    const rows = prompts.filter((p) => p.intentTerritory === intent);
    intentDensity[intent] = rows.length;
    if (rows.length !== 14) errors.push(`intent_density_${intent}_${rows.length}_expected_14`);
    const families = [...new Set(rows.map((r) => r.promptFamily))];
    if (families.length !== 2) {
      errors.push(`intent_framing_count_${intent}_${families.length}_expected_2`);
    }
    // Distinct texts within same geo/lang for the two framings
    const bySlot = new Map();
    for (const r of rows) {
      const sk = slotKeyFromPrompt(r);
      if (!bySlot.has(sk)) bySlot.set(sk, []);
      bySlot.get(sk).push(r);
    }
    for (const [sk, list] of bySlot) {
      if (list.length !== 2) continue;
      if (String(list[0].promptText).trim() === String(list[1].promptText).trim()) {
        errors.push(`duplicate_framing_text:${intent}:${sk}`);
      }
      if (list[0].promptFamily === list[1].promptFamily) {
        errors.push(`collapsed_framing_family:${intent}:${sk}`);
      }
    }
  }

  // Semantic pairs
  const byId = new Map(prompts.map((p) => [p.promptId, p]));
  const pairs = seed.semanticPairs || [];
  let invalidPairs = [];
  for (const pair of pairs) {
    const a = byId.get(pair.enPromptId);
    const b = byId.get(pair.esPromptId);
    const r = validateSemanticPairMembers(a, b);
    if (!r.ok) invalidPairs.push({ semanticPairId: pair.semanticPairId, errors: r.errors });
  }
  const calaPairs = pairs.filter((p) => p.geographyKey === "CALA");
  const mxPairs = pairs.filter((p) => p.geographyKey === "Mexico");
  if (pairs.length !== 24) errors.push(`pair_count_${pairs.length}_expected_24`);
  if (calaPairs.length !== 12) errors.push(`cala_pairs_${calaPairs.length}_expected_12`);
  if (mxPairs.length !== 12) errors.push(`mx_pairs_${mxPairs.length}_expected_12`);
  if (invalidPairs.length) errors.push(`invalid_pairs_${invalidPairs.length}`);

  // Peer v2
  const peerCfg = loadPeerSetConfig();
  const peer = resolvePeerSetMembership({ peerSetId: WAVE1_PEER_SET_ID }, peerCfg);
  const peerFp = peerMembershipFingerprint(peer.entityIds || []);
  if ((peer.entityIds || []).length !== 15) errors.push("peer_v2_count_not_15");
  const forbidden = ["recmKqo7M7mLZgRqQ", "recwONQTqGU1jHCsM", "recvvmiyReHhiKdoK", "recDwzv86TWnz2gGB"];
  for (const id of forbidden) {
    if ((peer.entityIds || []).includes(id)) errors.push(`peer_v2_contains_portfolio_only:${id}`);
  }
  for (const id of ["recegXrqaPiSLGCIe", "recCKuXCmGvxHPfb3", "recrWCD1LMqu864oU"]) {
    if (!(peer.entityIds || []).includes(id)) errors.push(`peer_v2_missing_required:${id}`);
  }

  // Companies
  const showcase = loadShowcaseCompaniesConfig();
  const companies = {};
  for (const key of listShowcaseCompanyKeys(showcase)) {
    const c = getShowcaseCompany(key, showcase);
    companies[key] = {
      PORTFOLIO_COUNT: (c.brandIds || []).length,
      VALID_BRAND_IDS: c.brandIds,
      PEER_DATASET_REUSED: showcase.sharedPeerSetId === WAVE1_PEER_SET_ID,
      DUPLICATE_PROVIDER_RUN_REQUIRED: false,
    };
  }
  if (!companies.ihg) errors.push("ihg_showcase_missing");

  // Eligibility — Wave-1 comparative metrics use peer v2 brands only
  const eligCfg = loadDecisionEligibilityConfig();
  const peerIdSet = new Set(peer.entityIds || []);
  const eligibilityByIntent = {};
  for (const territory of ACTIVE_SHOWCASE_INTENTS) {
    const rows = (eligCfg.entries || []).filter(
      (e) => e.decisionTerritory === territory && peerIdSet.has(e.brandId)
    );
    eligibilityByIntent[territory] = {
      ELIGIBLE: rows.filter((r) => r.eligibility === "ELIGIBLE").length,
      NOT_ELIGIBLE: rows.filter((r) => r.eligibility === "NOT_ELIGIBLE").length,
      UNKNOWN: rows.filter((r) => r.eligibility === "UNKNOWN").length,
      SCOPE: "peer_v2_only",
    };
  }
  const expectedElig = {
    Conversion: { ELIGIBLE: 15, NOT_ELIGIBLE: 0, UNKNOWN: 0 },
    "Collection / Soft Brand": { ELIGIBLE: 8, NOT_ELIGIBLE: 7, UNKNOWN: 0 },
    "Lifestyle Positioning": { ELIGIBLE: 8, NOT_ELIGIBLE: 2, UNKNOWN: 5 },
    "Upper-Upscale Positioning": { ELIGIBLE: 12, NOT_ELIGIBLE: 3, UNKNOWN: 0 },
    "Branded Residences": { ELIGIBLE: 10, NOT_ELIGIBLE: 5, UNKNOWN: 0 },
    "Soft-Brand Affiliation Flexibility": { ELIGIBLE: 8, NOT_ELIGIBLE: 0, UNKNOWN: 7 },
  };
  for (const [t, exp] of Object.entries(expectedElig)) {
    const got = eligibilityByIntent[t];
    if (!got) {
      errors.push(`eligibility_missing_${t}`);
      continue;
    }
    if (
      got.ELIGIBLE !== exp.ELIGIBLE ||
      got.NOT_ELIGIBLE !== exp.NOT_ELIGIBLE ||
      got.UNKNOWN !== exp.UNKNOWN
    ) {
      errors.push(
        `eligibility_mismatch_${t}_got_E${got.ELIGIBLE}_N${got.NOT_ELIGIBLE}_U${got.UNKNOWN}`
      );
    }
  }

  // Executions + fingerprints
  const executions = [];
  const fingerprints = new Set();
  const collisions = [];
  const ordered = [];
  for (const slot of WAVE1_EXECUTION_ORDER) {
    const slotPrompts = prompts
      .filter((p) => slotKeyFromPrompt(p) === slot.key)
      .sort((a, b) => {
        const ia = ACTIVE_SHOWCASE_INTENTS.indexOf(a.intentTerritory);
        const ib = ACTIVE_SHOWCASE_INTENTS.indexOf(b.intentTerritory);
        if (ia !== ib) return ia - ib;
        return String(a.promptFamily).localeCompare(String(b.promptFamily));
      });
    for (const p of slotPrompts) {
      const geographyKey = geographyKeyFromPrompt(p);
      const fp = buildWave1ExecutionFingerprint({
        provider: WAVE1_PROVIDER,
        promptId: p.promptId,
        promptVersion: p.version,
        semanticPairId: p.semanticPairId,
        geographyKey,
        language: p.language,
        intent: p.intentTerritory,
        promptFamily: p.promptFamily,
        peerSetId: WAVE1_PEER_SET_ID,
        peerSetVersion: "2",
        metricVersion: METRIC_VERSION,
      });
      if (fingerprints.has(fp.fingerprint)) collisions.push(fp.fingerprint);
      fingerprints.add(fp.fingerprint);
      const exec = {
        provider: WAVE1_PROVIDER,
        promptId: p.promptId,
        version: p.version,
        language: p.language,
        intent: p.intentTerritory,
        promptFamily: p.promptFamily,
        geographyKey,
        slot: slot.key,
        semanticPairId: p.semanticPairId || null,
        peerSet: WAVE1_PEER_SET_ID,
        peerSetVersion: "2",
        metricVersion: METRIC_VERSION,
        fingerprint: fp.fingerprint,
        promptText: p.promptText,
      };
      executions.push(exec);
      ordered.push(exec);
    }
  }
  if (collisions.length) errors.push(`fingerprint_collisions_${collisions.length}`);
  if (executions.length !== 84) errors.push(`execution_count_${executions.length}`);

  // Company names must not appear in prompts
  for (const p of prompts) {
    if (/\b(?:marriott|hilton|choice hotels|\bihg\b|autograph|kimpton)\b/i.test(p.promptText)) {
      warnings.push(`company_name_in_prompt:${p.promptId}`);
    }
  }

  return {
    planVersion: WAVE1_SHOWCASE_PLAN_VERSION,
    baselineSeriesId: WAVE1_BASELINE_SERIES_ID,
    seedPath,
    ok: errors.length === 0,
    errors,
    warnings,
    PROMPT_LIBRARY: {
      EXPECTED: 84,
      LOADED: prompts.length,
      EN: en.length,
      ES: es.length,
      VALID: validation.ok && prompts.length === 84,
      DUPLICATE_PROMPT_IDS: 0,
      INVALID_STATUS: validation.errors || [],
    },
    MATRIX: { ...matrix, TOTAL: prompts.length },
    INTENT_DENSITY: intentDensity,
    SEMANTIC_PAIRS: {
      CALA: calaPairs.length,
      MEXICO: mxPairs.length,
      TOTAL: pairs.length,
      INVALID: invalidPairs,
    },
    PEER: {
      ID: WAVE1_PEER_SET_ID,
      COUNT: (peer.entityIds || []).length,
      VALID: (peer.entityIds || []).length === 15 && errors.every((e) => !e.startsWith("peer_")),
      FINGERPRINT: peerFp,
      VERSION: "2",
      ENTITY_IDS: peer.entityIds || [],
    },
    COMPANIES: companies,
    ELIGIBILITY: eligibilityByIntent,
    EXECUTIONS: ordered,
    FINGERPRINTS: {
      SCHEMA: [
        "provider",
        "promptId",
        "promptVersion",
        "semanticPairId",
        "geographyKey",
        "language",
        "intent",
        "promptFamily",
        "peerSetId",
        "peerSetVersion",
        "metricVersion",
        "stakeholder",
      ],
      UNIQUE: fingerprints.size,
      COLLISIONS: collisions.length,
    },
    COST: WAVE1_COST_EVIDENCE,
    RETRY: WAVE1_RETRY_POLICY,
    EXECUTION_ORDER: WAVE1_EXECUTION_ORDER.map((s) => s.key),
    LIVE_PROVIDER_CALLS: 0,
  };
}
