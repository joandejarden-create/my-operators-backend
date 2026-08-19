/**
 * Future Competitive / Peer Analysis + Questions Missing integration contract.
 * CUSTOMER_INDEX_RENDERING = OFF — prepare shapes and copy only. No UI activation.
 */

import { auditCustomerPayloadForBlockedSignals } from "./blocked-signals.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST, INTERNAL_ONLY_FIELDS } from "./customer-payload.js";
import { ALL_INFO_CONTRACTS } from "./info-contracts.js";

export const TAB_INTEGRATION_VERSION = "scenario_benchmark_tab_integration_v1";
export const CUSTOMER_INDEX_RENDERING = "OFF";
export const NEW_TAB = "NO";
export const NEW_MAJOR_SECTION = "NO";
export const EXISTING_SECTIONS_REUSED = Object.freeze([
  "Competitive / Peer Analysis",
  "Questions Missing Watchlist",
]);

export const CUSTOMER_SAFE_PEER_LIMIT = 3;

export const FUTURE_OWNER_INTENT_BENCHMARK_FIELDS = Object.freeze([
  "scenarioId",
  "intentLabel",
  "subjectPresence",
  "indexValue",
  "relativeGapPct",
  "indexChgVsPrior",
  "benchmarkStatus",
  "selectedCorePeers",
  "selectedObservedCompetitors",
  "evidenceSummary",
]);

export const FUTURE_QUESTIONS_MISSING_FIELDS = Object.freeze([
  "scenarioId",
  "intentLabel",
  "missingProviderCount",
  "comparableProviderCount",
  "corePeersPresent",
  "observedCompetitors",
  "recurrenceState",
  "priority",
  "competitiveContext",
]);

export const CUSTOMER_STATUS_COPY = Object.freeze({
  PRODUCTION_VALIDATED: "CERTIFIED",
  PRODUCTION_VALIDATED_NARROW: "CERTIFIED_NARROW",
  DETAIL_ONLY: "Benchmark still developing",
  LIMITED: "Benchmark still developing",
  SUPPRESSED: "Benchmark still developing",
  BENCHMARK_STILL_DEVELOPING: "Benchmark still developing",
});

export const CORE_PEERS_INFO_COPY = Object.freeze({
  title: "Core Peers",
  body:
    "Brands considered direct commercial alternatives for this specific owner " +
    "decision. Dealality uses governed commercial characteristics to determine " +
    "relevant comparison groups.",
});

export const AI_PRESENCE_INDEX_INFO_COPY = Object.freeze({
  title: "AI Presence Index",
  body:
    "Measures how often your brand appears in a specific owner-decision context " +
    "relative to directly comparable brands measured across the same AI observations. " +
    "100 represents competitive parity. An index of 125 means your brand's Presence " +
    "is 25% above the relevant benchmark.",
  activateWhen: "PRODUCTION_CERTIFICATION",
});

export const OBSERVED_COMPETITORS_INFO_COPY = Object.freeze({
  title: "Observed Competitors",
  body:
    "Observed competitors are brands that appear as alternatives or peers across " +
    "relevant Dealality AI observations. They may differ from the brand's traditional " +
    "declared competitive set.",
});

export const QUESTIONS_MISSING_PRIORITY = Object.freeze(["PRIORITY", "REVIEW", "MONITOR"]);

export const EXECUTIVE_FINDING_TYPES = Object.freeze({
  COMPETITIVE_POSITION: "COMPETITIVE_POSITION",
  OWNER_DECISION_COVERAGE: "OWNER_DECISION_COVERAGE",
  NEW_CARD_REQUIRED: "NO",
});

/**
 * Customer-safe owner-intent row. Index numbers omitted unless certified AND rendering is on.
 */
export function buildFutureOwnerIntentBenchmarkRow(input = {}, opts = {}) {
  const renderingOn = opts.customerIndexRendering === true;
  const certified =
    input.productionClass === "PRODUCTION_VALIDATED" ||
    input.productionClass === "PRODUCTION_VALIDATED_NARROW";
  const showIndex =
    renderingOn &&
    opts.certifyIndex !== false &&
    certified &&
    typeof input.indexValue === "number";
  const selectedCorePeers = (input.selectedCorePeers || []).slice(0, CUSTOMER_SAFE_PEER_LIMIT).map((p) =>
    typeof p === "string" ? p : p.peerBrandName || p.canonicalName
  );
  const selectedObserved = (input.selectedObservedCompetitors || []).slice(0, CUSTOMER_SAFE_PEER_LIMIT).map((p) =>
    typeof p === "string" ? p : p.peerBrandName || p.canonicalName
  );
  return {
    scenarioId: input.scenarioId || null,
    intentLabel: input.intentLabel || null,
    subjectPresence: typeof input.subjectPresence === "number" ? input.subjectPresence : null,
    indexValue: showIndex ? input.indexValue : null,
    relativeGapPct: showIndex ? input.relativeGapPct ?? null : null,
    indexChgVsPrior:
      typeof input.indexChgVsPrior === "number" && Number.isFinite(input.indexChgVsPrior)
        ? input.indexChgVsPrior
        : null,
    benchmarkStatus: showIndex
      ? CUSTOMER_STATUS_COPY[input.productionClass] || "CERTIFIED"
      : CUSTOMER_STATUS_COPY.BENCHMARK_STILL_DEVELOPING,
    selectedCorePeers,
    selectedObservedCompetitors: selectedObserved,
    evidenceSummary: showIndex ? input.evidenceSummary || null : null,
  };
}

export function buildFutureQuestionsMissingRow(input = {}) {
  const corePresent = (input.corePeersPresent || []).slice(0, CUSTOMER_SAFE_PEER_LIMIT).map((p) =>
    typeof p === "string" ? p : p.peerBrandName || p.canonicalName
  );
  const observed = (input.observedCompetitors || []).slice(0, CUSTOMER_SAFE_PEER_LIMIT).map((p) =>
    typeof p === "string" ? p : p.peerBrandName || p.canonicalName
  );
  const priority = QUESTIONS_MISSING_PRIORITY.includes(input.priority) ? input.priority : "MONITOR";
  return {
    scenarioId: input.scenarioId || null,
    intentLabel: input.intentLabel || null,
    missingProviderCount: input.missingProviderCount ?? null,
    comparableProviderCount: input.comparableProviderCount ?? null,
    corePeersPresent: corePresent,
    observedCompetitors: observed,
    recurrenceState: input.recurrenceState || null,
    priority,
    competitiveContext: input.competitiveContext || null,
  };
}

export function redactFutureCompetitivePeerPayload(payload = {}) {
  const rows = (payload.ownerIntentBenchmarks || []).map((row) => {
    const out = {};
    for (const key of FUTURE_OWNER_INTENT_BENCHMARK_FIELDS) {
      if (row[key] !== undefined) out[key] = row[key];
    }
    delete out.peerPresenceValues;
    delete out.benchmarkMembers;
    delete out.allCompetitorScores;
    return out;
  });
  return {
    ownerIntentBenchmarks: rows,
    CUSTOMER_INDEX_RENDERING: payload.CUSTOMER_INDEX_RENDERING || CUSTOMER_INDEX_RENDERING,
  };
}

export function auditFutureCustomerPayload(payload = {}) {
  const blocked = auditCustomerPayloadForBlockedSignals(payload);
  const violations = [...blocked.violations];
  const stack = [{ path: "", value: payload }];
  while (stack.length) {
    const { path: p, value } = stack.pop();
    if (value == null || typeof value !== "object") continue;
    if (Array.isArray(value)) {
      value.forEach((v, i) => stack.push({ path: `${p}[${i}]`, value: v }));
      continue;
    }
    for (const [key, val] of Object.entries(value)) {
      const full = p ? `${p}.${key}` : key;
      if (INTERNAL_ONLY_FIELDS.includes(key)) violations.push(`internal_field:${full}`);
      if (/lost|beat|displaced|winCount|lossCount/i.test(key)) violations.push(`blocked_language_field:${full}`);
      stack.push({ path: full, value: val });
    }
  }
  return { ok: violations.length === 0, violations };
}

export function getTabIntegrationContract() {
  return {
    version: TAB_INTEGRATION_VERSION,
    NEW_TAB,
    NEW_MAJOR_SECTION,
    EXISTING_SECTIONS_REUSED,
    CUSTOMER_INDEX_RENDERING,
    competitivePeerAnalysis: {
      ROLE:
        "Primary home for AI Presence by Owner Intent, core-peer context, observed competitive set, and certified benchmark position. No benchmark engine. No full peer matrix.",
      FUTURE_COMPONENTS: [
        "SUMMARY_ROW_CERTIFIED_ONLY",
        "OWNER_INTENT_ROWS",
        "SELECTED_CORE_PEERS_CHIPS",
        "OBSERVED_COMPETITOR_CHIPS",
        "LIMITED_STATE_WITHOUT_INDEX",
      ],
      INDEX_RENDERING: CUSTOMER_INDEX_RENDERING,
      FULL_PEER_MATRIX: "INTERNAL_ONLY",
      payloadFields: FUTURE_OWNER_INTENT_BENCHMARK_FIELDS,
    },
    questionsMissingWatchlist: {
      ROLE:
        "Where the brand is absent, and which commercially relevant peers appear on those observations. Not a second benchmark table. No win/loss.",
      NEW_COMPETITIVE_CONTEXT: "CORE_PEERS_PRESENT preferred; additional observed competitor optional",
      INDEX_REQUIRED: "NO",
      payloadFields: FUTURE_QUESTIONS_MISSING_FIELDS,
      priority: QUESTIONS_MISSING_PRIORITY,
    },
    executiveSummary: {
      NEW_CARD_REQUIRED: EXECUTIVE_FINDING_TYPES.NEW_CARD_REQUIRED,
      EXISTING_FINDING_TYPE: EXECUTIVE_FINDING_TYPES.COMPETITIVE_POSITION,
      QUESTIONS_MISSING_FINDING_TYPE: EXECUTIVE_FINDING_TYPES.OWNER_DECISION_COVERAGE,
    },
    infoCopy: {
      AI_PRESENCE_INDEX: AI_PRESENCE_INDEX_INFO_COPY,
      CORE_PEERS: CORE_PEERS_INFO_COPY,
      OBSERVED_COMPETITORS: OBSERVED_COMPETITORS_INFO_COPY,
      existingObservedSet: ALL_INFO_CONTRACTS.OBSERVED_COMPETITIVE_SET,
    },
    liveAllowlistUnchanged: CUSTOMER_PAYLOAD_ALLOWLIST,
  };
}
