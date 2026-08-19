/**
 * Customer-safe prompt disclosure — SHOW WHAT WE MEASURE / PROTECT HOW WE MEASURE IT.
 * Presentation and API redaction only — no measurement logic changes.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildScenarioRegistryIndex,
  loadScenarioRegistry,
  resolvePromptScenario,
} from "./scenario-registry.js";
import { buildPromptMetadataById } from "./associations/prompt-metadata-lookup.js";
import { getCustomerScenarioDisplayLabel } from "./competitive-moat/scenario-benchmark-customer-service.js";
import { SCENARIO_IDS as S } from "./competitive-moat/benchmark-brand-ids.js";
import { normalizeLanguage } from "./language-dimension.js";

export const CUSTOMER_PROMPT_DISCLOSURE_VERSION = "customer_prompt_disclosure_v1";

/** Fields that must never appear on customer-facing payloads. */
export const INTERNAL_PROMPT_FIELD_NAMES = Object.freeze([
  "QUESTION",
  "question",
  "questionText",
  "promptText",
  "canonicalPrompt",
  "rawPrompt",
  "promptTemplate",
  "promptVariants",
  "promptGenerationRules",
  "promptTextFullCorpus",
  "observedQuery",
  "observedDemandQuery",
  "systemPrompt",
]);

/** Customer-safe decision context by governed scenarioId. */
export const CUSTOMER_DECISION_CONTEXT = Object.freeze({
  [S.SOFT_BRAND]:
    "How AI represents collection and soft-brand alternatives when an owner is considering affiliation.",
  [S.CONVERSION_SUITABILITY]:
    "How AI represents brand alternatives when an owner is evaluating a hotel conversion or reflagging.",
  [S.OWNER_FLEXIBILITY]:
    "How AI represents brand alternatives when an owner is prioritizing greater operating or property flexibility.",
  [S.LIFESTYLE]:
    "How AI represents brands when an owner wants distinctive positioning while preserving greater hotel individuality.",
  [S.BRANDED_RESIDENCES]:
    "How AI represents hotel brands when an owner is considering a project that includes branded residences.",
  [S.NEWBUILD_UU]:
    "How AI represents brand alternatives when an owner is evaluating an upper-upscale hotel project.",
  [S.INDEPENDENT_UU_CONVERSION]:
    "How AI represents conversion options when an owner is affiliating an independent upper-upscale property.",
  [S.MARKET_ENTRY]:
    "How AI represents brands when an owner is evaluating market entry or geographic relevance.",
  [S.DISTRIBUTION_LOYALTY]:
    "How AI represents brands when an owner is weighing distribution reach and loyalty program value.",
  [S.OWNER_ECONOMICS]:
    "How AI represents brand alternatives when an owner is evaluating economics and deal structure.",
  [S.CHAIN_SCALE]:
    "How AI represents brands when an owner is matching chain scale and positioning fit.",
  [S.HMA_VS_FRANCHISE]:
    "How AI represents operator and franchise models when an owner is choosing an agreement structure.",
});

export const OWNER_INTENT_INFO_COPY = Object.freeze({
  title: "Owner Intent",
  body:
    "Owner Intent represents the hotel owner or developer decision Dealality is testing, such as brand " +
    "affiliation, conversion, flexibility or market entry. Dealality may use multiple governed question " +
    "formulations to measure the same decision.",
});

export const DECISION_CONTEXT_INFO_COPY = Object.freeze({
  title: "Decision Context",
  body:
    "Decision Context describes the business situation behind the measurement. It helps explain what the AI " +
    "was being asked to evaluate without exposing Dealality's exact production prompt.",
});

export const HOW_DEALALITY_MEASURES_AI_COPY = Object.freeze({
  title: "How Dealality Measures AI",
  body:
    "Dealality tests representative hotel owner and developer decision scenarios across monitored AI " +
    "providers, markets and languages. Results are measured using governed scenarios and repeat observations. " +
    "Exact production prompts and testing sequences are proprietary.",
});

export const QUESTIONS_MISSING_INFO_COPY = Object.freeze({
  title: "Questions Missing",
  body:
    "A question is considered missing when your brand is absent across every comparable monitored provider " +
    "for that owner-decision observation. Missing does not mean zero demand or that another brand 'won.'",
});

export const BENCHMARK_STILL_DEVELOPING_INFO_COPY = Object.freeze({
  title: "Benchmark still developing",
  body:
    "Dealality shows a numeric benchmark as soon as this brand, Owner Intent and selected provider scope pass " +
    "the required measurement-quality checks. Until then, the underlying Presence observation may still be " +
    "shown without an uncertified score.",
});

export const CUSTOMER_METHODOLOGY_COPY =
  "Dealality measures how AI represents hotel brands across representative owner and developer decision " +
  "scenarios. We test these scenarios across monitored AI providers and relevant markets and languages, then " +
  "compare results over time and against appropriate commercial peers. Exact production prompts and testing " +
  "sequences are proprietary.";

export const CLIENT_CONVERSATION_PROMPT_RESPONSE =
  "Dealality shows the owner/developer decision scenarios being measured and can provide illustrative " +
  "examples of the types of questions used. We do not expose the full canonical prompt library or testing " +
  "sequence because those are part of Dealality's proprietary measurement methodology.";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_FORBIDDEN_PROMPTS_FIXTURE = path.join(
  __dirname,
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "phase3a9-showcase-prompt-seed.json"
);

const GEOGRAPHY_DISPLAY = Object.freeze({
  CALA: "Caribbean & Latin America",
  cala: "Caribbean & Latin America",
  Europe: "Europe",
  europe: "Europe",
  "North America": "North America",
  "north america": "North America",
  Mexico: "Mexico",
  mexico: "Mexico",
  Global: "Global",
  global: "Global",
});

let cachedForbiddenPromptStrings = null;

/**
 * Whether caller may receive exact production prompt text (internal diagnostics only).
 */
export function isInternalPromptAccess(viewerContext = {}) {
  return viewerContext?.internalAdmin === true || viewerContext?.role === "INTERNAL_ADMIN";
}

function formatPromptFamilyLabel(family) {
  if (!family) return null;
  const raw = String(family).trim();
  if (!raw || raw === "Unspecified") return null;
  return raw
    .replace(/_/g, " ")
    .replace(/\s*\/\s*/g, " / ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace(/\s+/g, " ")
    .trim();
}

export function formatCustomerGeographyDisplay(region) {
  if (region == null || region === "") return null;
  const key = String(region).trim();
  if (GEOGRAPHY_DISPLAY[key]) return GEOGRAPHY_DISPLAY[key];
  if (/^cala$/i.test(key)) return GEOGRAPHY_DISPLAY.CALA;
  return key;
}

export function formatCustomerLanguageDisplay(language) {
  const code = normalizeLanguage(language) || language;
  if (!code) return null;
  if (code === "en") return "English";
  if (code === "es") return "Spanish";
  return String(code).toUpperCase();
}

function resolveScenarioIdForRow(row = {}, opts = {}) {
  if (row.scenarioId) return row.scenarioId;
  const scenarioIndex = opts.scenarioIndex || buildScenarioRegistryIndex(opts.registry || loadScenarioRegistry());
  const promptMap = opts.promptMap || buildPromptMetadataById();
  const meta = row.promptId ? promptMap.get(row.promptId) : null;
  const resolved = resolvePromptScenario(
    {
      promptId: row.promptId,
      promptFamily: row.PROMPT_FAMILY || row.promptFamily || meta?.promptFamily,
      intentTerritory: row.intentTerritory || row.PROMPT_FAMILY || meta?.intentTerritory,
    },
    scenarioIndex
  );
  return resolved?.scenarioId || null;
}

export function resolveCustomerDecisionContext(scenarioId, promptFamily = null) {
  if (scenarioId && CUSTOMER_DECISION_CONTEXT[scenarioId]) {
    return CUSTOMER_DECISION_CONTEXT[scenarioId];
  }
  const family = formatPromptFamilyLabel(promptFamily);
  if (family) {
    return `How AI represents brand alternatives in ${family.toLowerCase()} owner-decision scenarios.`;
  }
  return "How AI represents brand alternatives for a monitored owner or developer decision.";
}

export function buildCustomerSafeObservationContext(row = {}, opts = {}) {
  const scenarioId = resolveScenarioIdForRow(row, opts);
  const promptFamily = row.PROMPT_FAMILY || row.promptFamily || row.intentTerritory || null;
  const ownerIntent =
    row.intentLabel ||
    row.ownerIntent ||
    (scenarioId ? getCustomerScenarioDisplayLabel(scenarioId) : null) ||
    formatPromptFamilyLabel(promptFamily) ||
    "Owner Decision Scenario";
  const decisionContext =
    row.decisionContext || resolveCustomerDecisionContext(scenarioId, promptFamily);
  const geography =
    row.geographyDisplay ||
    formatCustomerGeographyDisplay(row.REGION || row.region || row.commercialRegion || row.geography);
  const language =
    row.languageDisplay ||
    formatCustomerLanguageDisplay(row.LANGUAGE || row.language);
  return {
    scenarioId: scenarioId || null,
    ownerIntent,
    decisionContext,
    geography,
    language,
  };
}

function stripInternalPromptFields(obj = {}) {
  const out = { ...obj };
  for (const key of INTERNAL_PROMPT_FIELD_NAMES) delete out[key];
  if (out.drilldownTrace && typeof out.drilldownTrace === "object") {
    out.drilldownTrace = { ...out.drilldownTrace };
    for (const key of INTERNAL_PROMPT_FIELD_NAMES) delete out.drilldownTrace[key];
  }
  return out;
}

export function redactCustomerWatchlistRow(row = {}, opts = {}) {
  if (isInternalPromptAccess(opts.viewerContext)) return { ...row };
  const safe = buildCustomerSafeObservationContext(row, opts);
  const redacted = stripInternalPromptFields(row);
  return {
    ...redacted,
    ...safe,
    intentLabel: safe.ownerIntent,
    PROMPT_FAMILY: safe.ownerIntent,
  };
}

export function redactCustomerQuestionRow(row = {}, opts = {}) {
  if (isInternalPromptAccess(opts.viewerContext)) return { ...row };
  const safe = buildCustomerSafeObservationContext(row, opts);
  const redacted = stripInternalPromptFields(row);
  return {
    ...redacted,
    ...safe,
    intentLabel: safe.ownerIntent,
  };
}

export function redactAiVsDealalityRow(row = {}, opts = {}) {
  if (isInternalPromptAccess(opts.viewerContext)) return { ...row };
  const safe = buildCustomerSafeObservationContext(
    {
      ...row,
      PROMPT_FAMILY: row.intentTerritory || row.PROMPT_FAMILY,
      intentTerritory: row.intentTerritory,
      promptId: row.promptId,
    },
    opts
  );
  const redacted = stripInternalPromptFields(row);
  return {
    ...redacted,
    ownerIntent: row.ownerIntent || safe.ownerIntent,
    decisionContext: row.decisionContext || safe.decisionContext,
    geography: row.geography || safe.geography,
    aiRepresentation: row.aiRepresentation || row.aiPattern || "—",
    dealalityContext: row.dealalityContext || "Dealality context not yet available",
    reviewStatus: row.reviewStatus || "—",
    scenarioId: safe.scenarioId,
  };
}

export function redactAiVsDealalityContext(ctx = {}, opts = {}) {
  if (!ctx || typeof ctx !== "object") return ctx;
  if (isInternalPromptAccess(opts.viewerContext)) return ctx;
  const rows = (ctx.rows || []).map((r) => redactAiVsDealalityRow(r, opts));
  return stripInternalPromptFields({ ...ctx, rows });
}

export function redactCustomerEvidence(ev = {}, opts = {}) {
  if (!ev || typeof ev !== "object") return ev;
  if (isInternalPromptAccess(opts.viewerContext)) return ev;
  const safe = buildCustomerSafeObservationContext(
    {
      promptId: ev.promptId,
      intentTerritory: ev.intentTerritory,
      PROMPT_FAMILY: ev.intentTerritory || ev.promptFamily,
      REGION: ev.commercialRegion || ev.geographyScope,
      LANGUAGE: ev.language,
    },
    opts
  );
  const redacted = stripInternalPromptFields(ev);
  return {
    ...redacted,
    ownerIntent: safe.ownerIntent,
    decisionContext: safe.decisionContext,
    geographyDisplay: safe.geography,
    languageDisplay: safe.language,
    measurementContextLabel: safe.ownerIntent,
  };
}

export function redactCustomerQuestionsPayload(payload = {}, opts = {}) {
  if (!payload || typeof payload !== "object") return payload;
  if (isInternalPromptAccess(opts.viewerContext)) return payload;
  const redactRow = (r) => redactCustomerWatchlistRow(r, opts);
  const out = { ...payload };
  if (Array.isArray(out.questions)) out.questions = out.questions.map(redactCustomerQuestionRow);
  if (out.questionsMissingWatchlist?.rows) {
    out.questionsMissingWatchlist = {
      ...out.questionsMissingWatchlist,
      rows: out.questionsMissingWatchlist.rows.map(redactRow),
      disagreementRows: (out.questionsMissingWatchlist.disagreementRows || []).map(redactRow),
    };
  }
  if (out.crossProviderQuestions?.rows) {
    out.crossProviderQuestions = {
      ...out.crossProviderQuestions,
      rows: out.crossProviderQuestions.rows.map(redactRow),
      watchlistRows: (out.crossProviderQuestions.watchlistRows || []).map(redactRow),
      disagreementRows: (out.crossProviderQuestions.disagreementRows || []).map(redactRow),
    };
  }
  return out;
}

export function redactCustomerOverviewPayload(payload = {}, opts = {}) {
  if (!payload || typeof payload !== "object") return payload;
  if (isInternalPromptAccess(opts.viewerContext)) return payload;
  const out = { ...payload };
  if (out.aiVsDealalityContext) {
    out.aiVsDealalityContext = redactAiVsDealalityContext(out.aiVsDealalityContext, opts);
  }
  if (out.crossProviderQuestions) {
    out.crossProviderQuestions = redactCustomerQuestionsPayload(
      { crossProviderQuestions: out.crossProviderQuestions },
      opts
    ).crossProviderQuestions;
  }
  if (out.questionsMissingWatchlist) {
    const wl = { ...out.questionsMissingWatchlist };
    if (Array.isArray(wl.rows)) wl.rows = wl.rows.map((r) => redactCustomerWatchlistRow(r, opts));
    for (const groupKey of ["byPromptFamily", "byProvider", "byRegion", "byLanguage"]) {
      if (!Array.isArray(wl[groupKey])) continue;
      wl[groupKey] = wl[groupKey].map((group) => ({
        ...group,
        rows: (group.rows || []).map((r) => redactCustomerWatchlistRow(r, opts)),
      }));
    }
    out.questionsMissingWatchlist = wl;
  }
  if (out.peerPresentSubjectMissing?.rows) {
    out.peerPresentSubjectMissing = {
      ...out.peerPresentSubjectMissing,
      rows: out.peerPresentSubjectMissing.rows.map((r) => redactCustomerWatchlistRow(r, opts)),
    };
  }
  out.promptDisclosurePolicy = CUSTOMER_PROMPT_DISCLOSURE_VERSION;
  out.infoContracts = getCustomerPromptInfoContracts();
  return out;
}

export function getCustomerPromptInfoContracts() {
  return {
    OWNER_INTENT: OWNER_INTENT_INFO_COPY,
    DECISION_CONTEXT: DECISION_CONTEXT_INFO_COPY,
    HOW_DEALALITY_MEASURES_AI: HOW_DEALALITY_MEASURES_AI_COPY,
    QUESTIONS_MISSING: QUESTIONS_MISSING_INFO_COPY,
    BENCHMARK_STILL_DEVELOPING: BENCHMARK_STILL_DEVELOPING_INFO_COPY,
    METHODOLOGY: CUSTOMER_METHODOLOGY_COPY,
  };
}

/**
 * Load canonical production prompt strings for leak tests.
 */
export function loadForbiddenCanonicalPromptStrings(opts = {}) {
  if (cachedForbiddenPromptStrings && !opts.refresh) return cachedForbiddenPromptStrings;
  const filePath = opts.fixturePath || DEFAULT_FORBIDDEN_PROMPTS_FIXTURE;
  if (!fs.existsSync(filePath)) {
    cachedForbiddenPromptStrings = [];
    return cachedForbiddenPromptStrings;
  }
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  const prompts = raw.prompts || raw.items || raw;
  const strings = [];
  const list = Array.isArray(prompts) ? prompts : [];
  for (const p of list) {
    const text = p?.promptText || p?.["Prompt Text"];
    if (text && String(text).trim().length >= 40) strings.push(String(text).trim());
  }
  cachedForbiddenPromptStrings = [...new Set(strings)];
  return cachedForbiddenPromptStrings;
}

/**
 * Audit serialized payload for forbidden canonical prompt substrings.
 */
export function auditPayloadForCanonicalPromptLeaks(payload = {}, opts = {}) {
  const forbidden = opts.forbiddenStrings || loadForbiddenCanonicalPromptStrings(opts);
  const minLen = opts.minMatchLength ?? 48;
  const serialized = JSON.stringify(payload);
  const leaks = [];
  for (const phrase of forbidden) {
    if (!phrase || phrase.length < minLen) continue;
    if (serialized.includes(phrase)) leaks.push(phrase.slice(0, 80));
  }
  return {
    ok: leaks.length === 0,
    leakCount: leaks.length,
    leaks: leaks.slice(0, 10),
  };
}

export function auditInternalPromptAccessPreserved(internalPayload = {}, forbiddenStrings = []) {
  const hasPrompt =
    JSON.stringify(internalPayload).includes("promptText") ||
    JSON.stringify(internalPayload).includes("QUESTION");
  return { preserved: hasPrompt, hasPromptField: hasPrompt };
}
