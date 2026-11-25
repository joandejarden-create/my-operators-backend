/**
 * GDI Competitive Account Discovery (CAD) contract V1 — Phase 3A PR-A.
 *
 * Research SoT enrichment shape only. Does not write Airtable, STAGED/CURRENT,
 * or invoke paid providers. Does not weaken FQO: FQO_CANDIDATE ≠ FQO PASS.
 *
 * Auth: FOUNDER_GO AUTHORIZE_GDI_CAD_PHASE_3A_PR_A
 */
import { normalizeOrgIdentityKey } from "./reuse-prior-intelligence-v1.js";

export const GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1 =
  "gdi-competitive-account-discovery-contract-v1";

export const CAD_CLASS = Object.freeze({
  HISTORICAL_COMPETITIVE_INTEL: "HISTORICAL_COMPETITIVE_INTEL",
  EMERGING_COMPETITIVE_OPPORTUNITY: "EMERGING_COMPETITIVE_OPPORTUNITY",
  FQO_CANDIDATE: "FQO_CANDIDATE",
});

export const CAD_ATTAINABILITY = Object.freeze({
  WINNABLE: "WINNABLE",
  OVERFLOW_ONLY: "OVERFLOW_ONLY",
  MONITOR: "MONITOR",
  UNATTAINABLE: "UNATTAINABLE",
});

export const CAD_INCUMBENT_RELATIONSHIP = Object.freeze({
  VERIFIED: "VERIFIED",
  INFERRED: "INFERRED",
  UNKNOWN: "UNKNOWN",
});

export const CAD_SOURCING_WINDOW_STATE = Object.freeze({
  CONFIRMED: "CONFIRMED",
  INFERRED: "INFERRED",
  UNKNOWN: "UNKNOWN",
});

export const CAD_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  UNKNOWN: "UNKNOWN",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function collectUrls(list = []) {
  const out = [];
  const seen = new Set();
  for (const u of list || []) {
    const s = text(typeof u === "string" ? u : u?.url || u?.sourceUrl);
    if (!s || !/^https?:\/\//i.test(s) || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out;
}

/**
 * Stable program key for cross-hotel dedupe / enrich-before-create.
 * Qualification is never transferred by this key alone.
 */
export function buildCadProgramKey(input = {}) {
  const org = normalizeOrgIdentityKey(
    input.organizationName ||
      input.organization ||
      input.demandGenerator ||
      input.company ||
      "",
  );
  const program = text(input.programName || input.programmeName || input.title || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 96);
  const cycle = text(input.cycleKey || input.eventStartDate || "").slice(0, 10);
  if (!org && !program) return null;
  return `${org || "org"}|${program || "program"}|${cycle || "undated"}`;
}

export function emptyCompetitiveAccountDiscovery(overrides = {}) {
  return {
    contractVersion: GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1,
    organizationId: null,
    demandGeneratorId: null,
    programKey: null,
    historicalDemand: [],
    incumbent: {
      hotelName: null,
      relationship: CAD_INCUMBENT_RELATIONSHIP.UNKNOWN,
      commitmentScope: null,
      evidenceUrls: [],
      venueOnlyRejected: false,
    },
    sourcingWindow: {
      state: CAD_SOURCING_WINDOW_STATE.UNKNOWN,
      start: null,
      end: null,
      basis: null,
      evidenceUrls: [],
    },
    attainability: CAD_ATTAINABILITY.MONITOR,
    attainabilityConfidence: CAD_CONFIDENCE.UNKNOWN,
    buyer: null,
    intermediary: null,
    cadClass: null,
    recommendedAction: null,
    risks: [],
    notes: [],
    classifiedAt: null,
    ...overrides,
  };
}

/**
 * Strip unsupported sourcing dates (fabrication guard).
 * Dates are kept only when state is CONFIRMED or INFERRED with explicit basis.
 */
export function sanitizeSourcingWindow(window = {}) {
  const state = text(window.state).toUpperCase() || CAD_SOURCING_WINDOW_STATE.UNKNOWN;
  const evidenceUrls = collectUrls(window.evidenceUrls);
  const basis = text(window.basis) || null;
  let start = text(window.start) || null;
  let end = text(window.end) || null;

  if (state === CAD_SOURCING_WINDOW_STATE.UNKNOWN) {
    start = null;
    end = null;
  } else if (
    state === CAD_SOURCING_WINDOW_STATE.INFERRED &&
    !basis &&
    evidenceUrls.length === 0
  ) {
    return {
      state: CAD_SOURCING_WINDOW_STATE.UNKNOWN,
      start: null,
      end: null,
      basis: "INFERRED_WITHOUT_BASIS_DOWNGRADED",
      evidenceUrls: [],
    };
  } else if (
    state === CAD_SOURCING_WINDOW_STATE.CONFIRMED &&
    evidenceUrls.length === 0 &&
    !basis
  ) {
    return {
      state: CAD_SOURCING_WINDOW_STATE.UNKNOWN,
      start: null,
      end: null,
      basis: "CONFIRMED_WITHOUT_EVIDENCE_DOWNGRADED",
      evidenceUrls: [],
    };
  }

  // Reject clearly fabricated placeholder dates
  if (/^(TBD|TODO|UNKNOWN|NULL)$/i.test(start || "")) start = null;
  if (/^(TBD|TODO|UNKNOWN|NULL)$/i.test(end || "")) end = null;

  return {
    state: Object.values(CAD_SOURCING_WINDOW_STATE).includes(state)
      ? state
      : CAD_SOURCING_WINDOW_STATE.UNKNOWN,
    start,
    end,
    basis,
    evidenceUrls,
  };
}

/**
 * Validate a CAD payload without mutating callers.
 * @returns {{ ok: boolean, errors: string[], value: object|null }}
 */
export function validateCompetitiveAccountDiscovery(payload = {}) {
  const errors = [];
  if (!payload || typeof payload !== "object") {
    return { ok: false, errors: ["CAD_PAYLOAD_REQUIRED"], value: null };
  }
  if (payload.contractVersion !== GDI_COMPETITIVE_ACCOUNT_DISCOVERY_CONTRACT_V1) {
    errors.push("CAD_CONTRACT_VERSION_MISMATCH");
  }
  if (
    payload.cadClass != null &&
    !Object.values(CAD_CLASS).includes(payload.cadClass)
  ) {
    errors.push("CAD_CLASS_INVALID");
  }
  if (
    payload.attainability != null &&
    !Object.values(CAD_ATTAINABILITY).includes(payload.attainability)
  ) {
    errors.push("CAD_ATTAINABILITY_INVALID");
  }
  const rel = payload.incumbent?.relationship;
  if (rel != null && !Object.values(CAD_INCUMBENT_RELATIONSHIP).includes(rel)) {
    errors.push("CAD_INCUMBENT_RELATIONSHIP_INVALID");
  }
  const sw = sanitizeSourcingWindow(payload.sourcingWindow || {});
  if (
    payload.cadClass === CAD_CLASS.FQO_CANDIDATE &&
    payload.attainability === CAD_ATTAINABILITY.UNATTAINABLE
  ) {
    errors.push("FQO_CANDIDATE_CANNOT_BE_UNATTAINABLE");
  }
  // Historical venue alone must not claim VERIFIED incumbent without evidence URLs
  if (
    payload.incumbent?.relationship === CAD_INCUMBENT_RELATIONSHIP.VERIFIED &&
    !(payload.incumbent.evidenceUrls || []).length &&
    payload.incumbent.venueOnlyRejected !== true
  ) {
    errors.push("VERIFIED_INCUMBENT_REQUIRES_EVIDENCE_URLS");
  }

  const value = {
    ...emptyCompetitiveAccountDiscovery(),
    ...payload,
    sourcingWindow: sw,
    historicalDemand: Array.isArray(payload.historicalDemand)
      ? payload.historicalDemand
      : [],
    risks: Array.isArray(payload.risks) ? payload.risks : [],
    notes: Array.isArray(payload.notes) ? payload.notes : [],
  };

  return { ok: errors.length === 0, errors, value };
}

/**
 * Prefer enriching an existing opportunity over creating a duplicate CAD account.
 * Cross-hotel reuse of programKey does NOT transfer hotel FQO.
 */
export function resolveCadDedupeAction({
  existingOpportunityIds = [],
  programKey = null,
  existingByProgramKey = new Map(),
} = {}) {
  const key = text(programKey);
  if (!key) {
    return { action: "CREATE_CANDIDATE_REVIEW", reason: "NO_PROGRAM_KEY" };
  }
  if (existingByProgramKey.has(key)) {
    return {
      action: "ENRICH_EXISTING",
      reason: "PROGRAM_KEY_MATCH",
      opportunityId: existingByProgramKey.get(key),
      qualificationTransferred: false,
    };
  }
  if (Array.isArray(existingOpportunityIds) && existingOpportunityIds.length) {
    return {
      action: "CREATE_CANDIDATE_REVIEW",
      reason: "NO_PROGRAM_KEY_MATCH",
      qualificationTransferred: false,
    };
  }
  return {
    action: "CREATE_CANDIDATE_REVIEW",
    reason: "NO_EXISTING_INDEX",
    qualificationTransferred: false,
  };
}

export {
  collectUrls as collectCadEvidenceUrls,
  text as cadText,
};
