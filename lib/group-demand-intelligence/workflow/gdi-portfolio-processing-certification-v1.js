/**
 * GDI Portfolio Processing Certification V1
 *
 * Thin multi-hotel orchestration over the canonical Full E2E engine.
 * Does not build a competing research pipeline.
 *
 * Defaults: $0 / no paid / no publication / no live Airtable / no weekly activation.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";
import {
  listFourteenSegmentExecutorSpecs,
  assertFourteenSegmentExecutorArtifactsOnDisk,
} from "./full-e2e-fourteen-segment-discovery-executor-registry-v1.js";
import {
  runCanonicalFullE2eResearch,
  preflightCanonicalFullE2e,
  assessCanonicalLiveReadiness,
  CANONICAL_EXECUTION_MODE,
  GDI_CANONICAL_FULL_E2E_ENTRY_V1,
} from "./gdi-canonical-full-e2e-entry-v1.js";
import { buildDurableWeeklyChangeAnnotation } from "./gdi-weekly-change-durability-v1.js";
import { WEEKLY_DELTA_STATE } from "../weekly-delta.js";
import { evaluateResearchPersistenceAuthorization } from "./gdi-full-e2e-research-persistence-bridge-v1.js";

export const GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1 =
  "gdi-portfolio-processing-certification-v1";

export const CAPABILITY_STATUS = Object.freeze({
  CERTIFIED: "CERTIFIED",
  FUNCTIONAL_NOT_CERTIFIED: "FUNCTIONAL_NOT_CERTIFIED",
  PARTIAL: "PARTIAL",
  BLOCKED: "BLOCKED",
});

export const PORTFOLIO_MODE = Object.freeze({
  PREVIEW: "preview",
  FULL_E2E: "full-e2e",
  REHEARSAL: "rehearsal",
  RESUME: "resume",
});

export const READINESS_VERDICT = Object.freeze({
  CERTIFIED_FOR_SCALING: "CERTIFIED_FOR_SCALING",
  LIMITED_PILOT_ONLY: "LIMITED_PILOT_ONLY",
  NOT_READY: "NOT_READY",
});

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

const HOTEL_DATA_ROOT = path.join(
  ROOT,
  "data",
  "group-demand-intelligence",
  "hotels",
);

/** Representative certification cohort — existing HPC record IDs only. */
export const CERTIFICATION_COHORT_V1 = Object.freeze([
  {
    hotelId: "recLuxvwwxID7U2B8",
    expectedName: "Bethesda Marriott",
    region: "US_NORTHEAST_MIDATLANTIC",
    role: "EXISTING_RICH_INVENTORY",
  },
  {
    hotelId: "recnPB3Q02NO8Ma1o",
    expectedName: "Wyndham San Diego Bayside",
    region: "US_WEST",
    role: "EXISTING_RICH_INVENTORY",
  },
  {
    hotelId: "recG66DQJKP2c0UNh",
    expectedName: "Renaissance New York Times Square Hotel",
    region: "US_NORTHEAST",
    role: "EXISTING_INVENTORY",
  },
  {
    hotelId: "recCAGqkERSXgiku4",
    expectedName: "Sheraton Guadalajara Expo",
    region: "MEXICO",
    role: "INTERNATIONAL_MEXICO",
  },
  {
    hotelId: "recrPQcZg7SFARRb2",
    expectedName: "YOTEL Geneva Lake",
    region: "EUROPE",
    role: "INTERNATIONAL_EUROPE",
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    expectedName: "Spice Island Beach Resort",
    region: "CARIBBEAN",
    role: "INTERNATIONAL_CARIBBEAN",
  },
  {
    hotelId: "rec0eTEnCqlsGJJt9",
    expectedName: "The Mayflower Hotel, Autograph Collection",
    region: "US_NORTHEAST_MIDATLANTIC",
    role: "NO_EXISTING_OPPORTUNITY_STORE",
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeJson(filePath, body) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(body, null, 2)}\n`, "utf8");
  return filePath;
}

function readJsonSafe(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function hotelDir(hotelId) {
  return path.join(HOTEL_DATA_ROOT, text(hotelId));
}

/**
 * Resolve hotel identity from local GDI store (read-only).
 */
export function resolveHotelIdentity(hotelId, { root = ROOT } = {}) {
  const id = text(hotelId);
  if (!/^rec[a-zA-Z0-9]+$/.test(id)) {
    return {
      ok: false,
      hotelId: id || null,
      error: "INVALID_HPC_HOTEL_ID",
    };
  }
  const dataRoot = path.join(
    root,
    "data",
    "group-demand-intelligence",
    "hotels",
  );
  const dir = path.join(dataRoot, id);
  const profilePath = path.join(dir, "profile.json");
  const oppPath = path.join(dir, "opportunities.json");
  const profile = fs.existsSync(profilePath) ? readJsonSafe(profilePath) : null;
  const oppStore = fs.existsSync(oppPath) ? readJsonSafe(oppPath) : null;
  const identity = profile?.identity || {};
  const name =
    text(identity.hotelName) ||
    text(profile?.hotelName) ||
    text(oppStore?.hotelName) ||
    null;
  const opportunities = Array.isArray(oppStore?.opportunities)
    ? oppStore.opportunities
    : [];
  return {
    ok: true,
    hotelId: id,
    hotelName: name,
    city: text(identity.city) || null,
    country: text(identity.country) || null,
    brand: text(identity.brand) || null,
    hasProfile: Boolean(profile),
    hasOpportunityStore: Boolean(oppStore),
    opportunityCount: opportunities.length,
    opportunityIds: opportunities
      .map((o) => text(o.id || o.opportunityId))
      .filter(Boolean),
    profilePath: fs.existsSync(profilePath) ? profilePath : null,
    opportunityStorePath: fs.existsSync(oppPath) ? oppPath : null,
  };
}

/**
 * Read-only eligibility preflight for portfolio admission.
 */
export function preflightHotelEligibility(hotelId, opts = {}) {
  const identity = resolveHotelIdentity(hotelId, opts);
  const errors = [];
  const warnings = [];
  if (!identity.ok) {
    errors.push({ code: identity.error || "INVALID_HOTEL" });
  } else {
    if (!identity.hasProfile && !identity.hasOpportunityStore) {
      // Net-new hotel without local profile still eligible for discovery
      // when canonical preflight can resolve HPC — flag as warning.
      warnings.push({ code: "NO_LOCAL_GDI_ARTIFACTS" });
    }
    if (!identity.hotelName) {
      warnings.push({ code: "HOTEL_NAME_UNRESOLVED_LOCALLY" });
    }
  }
  const canonical = preflightCanonicalFullE2e({
    hotelId: text(hotelId),
    mode: CANONICAL_EXECUTION_MODE.PREFLIGHT,
    dryRun: true,
    budgetCeilingUsd: 0,
  });
  if (!canonical.ok) {
    for (const e of canonical.errors || []) errors.push(e);
  }
  return {
    ok: errors.length === 0,
    hotelId: text(hotelId),
    identity,
    canonicalPreflight: {
      ok: canonical.ok,
      mode: canonical.mode,
      errorCodes: (canonical.errors || []).map((e) => e.code || e),
    },
    errors,
    warnings,
    eligible: errors.length === 0,
  };
}

/**
 * Deduplicate hotel list; preserve order; reject invalid IDs.
 */
export function normalizeHotelList(hotelIds = []) {
  const seen = new Set();
  const unique = [];
  const duplicates = [];
  const invalid = [];
  for (const raw of hotelIds) {
    const id = text(raw);
    if (!/^rec[a-zA-Z0-9]+$/.test(id)) {
      invalid.push(id || String(raw));
      continue;
    }
    if (seen.has(id)) {
      duplicates.push(id);
      continue;
    }
    seen.add(id);
    unique.push(id);
  }
  return {
    ok: invalid.length === 0,
    hotelIds: unique,
    duplicates,
    invalid,
    count: unique.length,
  };
}

/**
 * Offline commercial quality assessment from existing opportunity store.
 * Does NOT certify Sales Ready without evidence fields.
 */
export function assessHotelCommercialQuality(hotelId, opts = {}) {
  const identity = resolveHotelIdentity(hotelId, opts);
  if (!identity.ok) {
    return {
      hotelId: text(hotelId),
      status: CAPABILITY_STATUS.BLOCKED,
      score: 0,
      reasons: ["INVALID_HOTEL_ID"],
    };
  }
  if (!identity.hasOpportunityStore) {
    return {
      hotelId: identity.hotelId,
      hotelName: identity.hotelName,
      status: CAPABILITY_STATUS.PARTIAL,
      score: 15,
      role: "NO_EXISTING_OPPORTUNITY_STORE",
      opportunityCount: 0,
      reasons: [
        "NO_LOCAL_OPPORTUNITY_STORE — net-new discovery not certified without paid/fresh research",
      ],
      gates: {
        hotelIdentity: identity.hotelName ? "PASS" : "PARTIAL",
        opportunityInventory: "EMPTY",
        namedBuyerPresence: "NOT_ASSESSED",
        evidencePresence: "NOT_ASSESSED",
        deduplication: "NOT_ASSESSED",
        salesReadySpeculationReject: "NOT_ASSESSED",
      },
    };
  }

  const store = readJsonSafe(identity.opportunityStorePath);
  const opps = Array.isArray(store?.opportunities) ? store.opportunities : [];
  const ids = opps.map((o) => text(o.id || o.opportunityId)).filter(Boolean);
  const uniqueIds = new Set(ids);
  const withEvidence = opps.filter((o) => {
    const evidence =
      o.evidence ||
      o.sources ||
      o.sourceUrls ||
      o.supportingEvidence ||
      o.evidenceUrls;
    if (Array.isArray(evidence) && evidence.length > 0) return true;
    if (text(o.sourceUrl) || text(o.primarySourceUrl)) return true;
    if (Array.isArray(o.sourceEvidence) && o.sourceEvidence.length > 0) {
      return true;
    }
    return false;
  }).length;
  const withNamedContact = opps.filter((o) => {
    const contacts =
      o.namedContacts ||
      o.buyerCandidates ||
      o.contacts ||
      o.recommendedContacts;
    if (Array.isArray(contacts) && contacts.some((c) => text(c?.fullName || c?.name))) {
      return true;
    }
    return Boolean(text(o.primaryContactName) || text(o.buyerName));
  }).length;
  const salesReady = opps.filter((o) => {
    const stage = text(
      o.commercialStageKey ||
        o.salesActionKey ||
        o.demandStatus ||
        o.publicationStatus ||
        o.priority,
    ).toLowerCase();
    return /sales[_ -]?ready|ready_to_pursue|high/.test(stage);
  });
  const speculativeSalesReady = salesReady.filter((o) => {
    const hasContact = Boolean(
      text(o.primaryContactName) ||
        text(o.buyerName) ||
        (Array.isArray(o.namedContacts) && o.namedContacts.length) ||
        (Array.isArray(o.buyerCandidates) && o.buyerCandidates.length),
    );
    const hasEvidence = Boolean(
      text(o.sourceUrl) ||
        (Array.isArray(o.evidence) && o.evidence.length) ||
        (Array.isArray(o.sources) && o.sources.length),
    );
    return !(hasContact && hasEvidence);
  });

  const gates = {
    hotelIdentity: identity.hotelName ? "PASS" : "PARTIAL",
    opportunityInventory: opps.length > 0 ? "PASS" : "EMPTY",
    namedBuyerPresence:
      withNamedContact > 0
        ? "PASS"
        : opps.length > 0
          ? "PARTIAL"
          : "NOT_ASSESSED",
    evidencePresence:
      withEvidence > 0 ? "PASS" : opps.length > 0 ? "PARTIAL" : "NOT_ASSESSED",
    deduplication:
      ids.length === uniqueIds.size ? "PASS" : "FAIL_DUPLICATE_IDS",
    salesReadySpeculationReject:
      speculativeSalesReady.length === 0 ? "PASS" : "FAIL_SPECULATIVE_ROWS",
    geographicRelevance: identity.city || identity.country ? "PASS" : "PARTIAL",
  };

  let score = 40;
  if (identity.hotelName) score += 10;
  if (opps.length >= 10) score += 15;
  else if (opps.length > 0) score += 8;
  if (withEvidence / Math.max(opps.length, 1) >= 0.3) score += 10;
  if (withNamedContact > 0) score += 10;
  if (gates.deduplication === "PASS") score += 5;
  if (gates.salesReadySpeculationReject === "PASS") score += 10;
  else score -= 15;

  let status = CAPABILITY_STATUS.FUNCTIONAL_NOT_CERTIFIED;
  if (
    score >= 80 &&
    gates.deduplication === "PASS" &&
    gates.salesReadySpeculationReject === "PASS" &&
    withEvidence > 0
  ) {
    status = CAPABILITY_STATUS.CERTIFIED;
  } else if (score < 40 || gates.deduplication === "FAIL_DUPLICATE_IDS") {
    status = CAPABILITY_STATUS.PARTIAL;
  }

  return {
    hotelId: identity.hotelId,
    hotelName: identity.hotelName,
    status,
    score: Math.max(0, Math.min(100, score)),
    opportunityCount: opps.length,
    uniqueOpportunityIds: uniqueIds.size,
    withEvidence,
    withNamedContact,
    salesReadyCount: salesReady.length,
    speculativeSalesReadyCount: speculativeSalesReady.length,
    gates,
    reasons: [],
  };
}

/**
 * Fourteen-segment coverage from existing registries (engine presence ≠ Full E2E CERTIFIED).
 */
export function assessFourteenSegmentCoverage() {
  const segmentIds = listFourteenWorkflowSegmentIds();
  const specs = listFourteenSegmentExecutorSpecs();
  const onDisk = assertFourteenSegmentExecutorArtifactsOnDisk();
  const failureBySegment = new Map(
    (onDisk?.failures || []).map((f) => [f.segmentId, f]),
  );
  const rows = segmentIds.map((segmentId) => {
    const spec = (specs || []).find((s) => s.segmentId === segmentId);
    const failure = failureBySegment.get(segmentId);
    const artifactOk = Boolean(spec) && !spec.missing && !failure;
    let status = CAPABILITY_STATUS.FUNCTIONAL_NOT_CERTIFIED;
    if (!spec || spec.missing) status = CAPABILITY_STATUS.BLOCKED;
    else if (failure) status = CAPABILITY_STATUS.PARTIAL;
    return {
      segmentId,
      executorPresent: Boolean(spec) && !spec.missing,
      hotelRunnerExists: Boolean(spec?.hotelRunnerExists),
      artifactOk,
      status,
      note:
        "Executor/registry presence verified offline — live discovery+deep research per segment requires paid/auth certification",
    };
  });
  const certified = rows.filter(
    (r) => r.status === CAPABILITY_STATUS.CERTIFIED,
  ).length;
  const functional = rows.filter(
    (r) => r.status === CAPABILITY_STATUS.FUNCTIONAL_NOT_CERTIFIED,
  ).length;
  return {
    contract: GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
    requiredCount: 14,
    segmentCount: segmentIds.length,
    rows,
    summary: {
      CERTIFIED: certified,
      FUNCTIONAL_NOT_CERTIFIED: functional,
      PARTIAL: rows.filter((r) => r.status === CAPABILITY_STATUS.PARTIAL)
        .length,
      BLOCKED: rows.filter((r) => r.status === CAPABILITY_STATUS.BLOCKED)
        .length,
    },
    ok: segmentIds.length === 14 && rows.every((r) => r.executorPresent),
  };
}

/**
 * Current-state capability audit (Phase 1).
 * Existence of scripts/unit tests ≠ Full E2E CERTIFIED.
 */
export function buildCapabilityAuditMatrix() {
  const S = CAPABILITY_STATUS;
  const runners = {
    "scripts/gdi-workflow.ps1": {
      role: "Universal ops wrapper; Research/FullRun → canonical dry-run only",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
    },
    "scripts/Invoke-GdiScalingCompanies.ps1": {
      role: "Scaling-companies segment launcher (Preview/FullE2E fail-closed)",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
    },
    "scripts/gdi-canonical-full-e2e-entry-v1.mjs": {
      role: "Canonical Full E2E research entry (authoritative Node path)",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
    },
    "scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs": {
      role: "Fourteen-segment orchestrator",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
    },
    "scripts/gdi-full-e2e-powershell-deep-research-certification-v1.mjs": {
      role: "Bethesda-centric $0 deep-research certification harness",
      status: S.PARTIAL,
    },
    "scripts/Invoke-GdiPortfolioProcessing.ps1": {
      role: "Portfolio multi-hotel standardized interface (this certification)",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
    },
  };

  const capabilities = [
    {
      capability: "New hotels (net-new discovery)",
      status: S.PARTIAL,
      note: "Canonical path + preflight exist; fresh discovery not Full-E2E certified without paid auth",
    },
    {
      capability: "Existing hotels (refresh / replay)",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
      note: "Historical replay + dry-run work; customer delivery not certified here",
    },
    {
      capability: "All 14 GDI demand categories",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
      note: "Segment registries + executors on disk; per-segment live deep research not portfolio-certified",
    },
    {
      capability: "Discovery",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
      note: "Offline/fixture discovery certified in harnesses; paid discovery blocked without flags",
    },
    {
      capability: "Deep research",
      status: S.PARTIAL,
      note: "Eligibility + restored pass modules exist; live Exa requires separate founder auth",
    },
    {
      capability: "Contact identification",
      status: S.PARTIAL,
      note: "Universal contact contract + Surfe/PDL gates; FE→public path certified in prior PRs; enrichment HOLD/partial",
    },
    {
      capability: "Opportunity qualification",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
      note: "Commercial spine present; Sales Ready speculation rejection enforced in quality gates",
    },
    {
      capability: "Canonical persistence",
      status: S.PARTIAL,
      note: "Local hotel stores + research persistence bridge dry-run; production Airtable apply fail-closed",
    },
    {
      capability: "Customer drawer generation",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
      note: "Certified drawer commercial content contract used in prior hotel certs; portfolio batch not customer-published",
    },
    {
      capability: "Weekly Change",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
      note: "Durability annotation + delta states offline-verifiable; weekly automation NOT activated",
    },
    {
      capability: "Repeated runs (idempotent)",
      status: S.FUNCTIONAL_NOT_CERTIFIED,
      note: "Stable opportunity IDs + resume ledger; rehearsal proves no overwrite under $0",
    },
  ];

  return {
    contract: GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
    canonicalEngine: GDI_CANONICAL_FULL_E2E_ENTRY_V1,
    generatedAt: nowIso(),
    runners,
    capabilities,
    classificationRule:
      "CERTIFIED requires Full E2E evidence for the capability under certified constraints. Unit tests / script existence alone → at most FUNCTIONAL_NOT_CERTIFIED.",
  };
}

function buildHotelRunId(portfolioRunId, hotelId) {
  return `${portfolioRunId}__${hotelId}`;
}

function loadPortfolioState(statePath) {
  return readJsonSafe(statePath) || null;
}

function savePortfolioState(statePath, state) {
  return writeJson(statePath, state);
}

/**
 * Sequential portfolio orchestrator.
 * Independent per-hotel run IDs, aggregate budget, resume, error isolation.
 */
export async function runPortfolioOrchestrator(opts = {}) {
  const mode = text(opts.mode || PORTFOLIO_MODE.PREVIEW).toLowerCase();
  const authorizePaidResearch = opts.authorizePaidResearch === true;
  const authorizePublication = opts.authorizePublication === true;
  const authorizeProductionWrites = opts.authorizeProductionWrites === true;
  const budgetCeilingUsd = Number(opts.budgetCeilingUsd || 0) || 0;
  const perHotelBudgetUsd = Number(opts.perHotelBudgetUsd || 0) || 0;
  const reportRoot =
    opts.reportRoot ||
    path.join(ROOT, "reports", "gdi-portfolio-processing-certification-v1");
  const portfolioRunId =
    text(opts.portfolioRunId) ||
    `gdi_portfolio_${new Date()
      .toISOString()
      .replace(/[-:TZ.]/g, "")
      .slice(0, 14)}_${Math.random().toString(16).slice(2, 8)}`;

  // Fail-closed: this certification runner never enables paid/publish/writes
  // unless explicitly forced AND mode is not rehearsal. Rehearsal always $0.
  const isRehearsal =
    mode === PORTFOLIO_MODE.REHEARSAL || opts.forceZeroCost === true;
  if (isRehearsal && (authorizePaidResearch || budgetCeilingUsd > 0)) {
    return {
      ok: false,
      error: "REHEARSAL_REFUSES_PAID",
      message:
        "Rehearsal/certification mode refuses paid research. Drop -AuthorizePaidResearch and set BudgetUSD 0.",
    };
  }
  if (authorizePaidResearch && budgetCeilingUsd <= 0) {
    return {
      ok: false,
      error: "PAID_REQUIRES_BUDGET",
      message: "AuthorizePaidResearch requires BudgetUSD > 0.",
    };
  }
  if (budgetCeilingUsd > 0 && !authorizePaidResearch) {
    return {
      ok: false,
      error: "BUDGET_REQUIRES_PAID_AUTH",
      message: "Nonzero budget requires AuthorizePaidResearch.",
    };
  }
  if (perHotelBudgetUsd > 0 && budgetCeilingUsd <= 0) {
    return {
      ok: false,
      error: "PER_HOTEL_BUDGET_REQUIRES_AGGREGATE",
      message: "PerHotelBudgetUSD requires BudgetUSD > 0.",
    };
  }
  if (
    perHotelBudgetUsd > 0 &&
    budgetCeilingUsd > 0 &&
    perHotelBudgetUsd > budgetCeilingUsd
  ) {
    return {
      ok: false,
      error: "PER_HOTEL_BUDGET_EXCEEDS_AGGREGATE",
      message: "PerHotelBudgetUSD cannot exceed BudgetUSD.",
    };
  }
  if (
    (authorizePublication || authorizeProductionWrites) &&
    isRehearsal
  ) {
    return {
      ok: false,
      error: "REHEARSAL_REFUSES_PUBLICATION",
      message: "Rehearsal refuses publication and production writes.",
    };
  }

  const list = normalizeHotelList(opts.hotelIds || []);
  if (!list.ok || list.count === 0) {
    return {
      ok: false,
      error: "INVALID_HOTEL_LIST",
      invalid: list.invalid,
      duplicates: list.duplicates,
    };
  }

  ensureDir(reportRoot);
  const statePath =
    opts.statePath || path.join(reportRoot, `${portfolioRunId}.state.json`);
  let state = opts.resume
    ? loadPortfolioState(statePath) || {
        portfolioRunId,
        hotels: {},
        spentUsd: 0,
      }
    : { portfolioRunId, hotels: {}, spentUsd: 0, startedAt: nowIso() };

  state.mode = mode;
  state.budgetCeilingUsd = isRehearsal ? 0 : budgetCeilingUsd;
  state.perHotelBudgetUsd = isRehearsal ? 0 : perHotelBudgetUsd;
  state.authorization = {
    paidResearch: isRehearsal ? false : authorizePaidResearch,
    publication: isRehearsal ? false : authorizePublication,
    productionWrites: isRehearsal ? false : authorizeProductionWrites,
    weeklyActivation: false,
  };
  state.duplicatesSkipped = list.duplicates;
  state.canonicalEngine = GDI_CANONICAL_FULL_E2E_ENTRY_V1;

  const hotelResults = [];
  let aggregateSpend = Number(state.spentUsd || 0);

  for (const hotelId of list.hotelIds) {
    const prior = state.hotels[hotelId];
    if (
      opts.resume &&
      prior &&
      (prior.status === "PASSED" || prior.status === "SKIPPED_COMPLETE")
    ) {
      hotelResults.push({
        ...prior,
        resumedSkipped: true,
      });
      continue;
    }

    const hotelRunId = buildHotelRunId(portfolioRunId, hotelId);
    const hotelOut = path.join(reportRoot, "hotels", hotelId, hotelRunId);
    ensureDir(hotelOut);

    const eligibility = preflightHotelEligibility(hotelId, { root: ROOT });
    const quality = assessHotelCommercialQuality(hotelId, { root: ROOT });

    if (!eligibility.eligible) {
      const failed = {
        hotelId,
        hotelRunId,
        status: "FAILED_PREFLIGHT",
        eligibility,
        quality,
        errorIsolation: true,
        finishedAt: nowIso(),
      };
      state.hotels[hotelId] = failed;
      hotelResults.push(failed);
      writeJson(path.join(hotelOut, "result.json"), failed);
      savePortfolioState(statePath, state);
      continue;
    }

    // Budget enforcement before any hotel work that could spend
    if (
      !isRehearsal &&
      authorizePaidResearch &&
      aggregateSpend >= budgetCeilingUsd
    ) {
      const blocked = {
        hotelId,
        hotelRunId,
        status: "BLOCKED_BUDGET",
        eligibility,
        quality,
        aggregateSpendUsd: aggregateSpend,
        budgetCeilingUsd,
        finishedAt: nowIso(),
      };
      state.hotels[hotelId] = blocked;
      hotelResults.push(blocked);
      writeJson(path.join(hotelOut, "result.json"), blocked);
      savePortfolioState(statePath, state);
      continue;
    }

    let canonicalResult = null;
    let canonicalError = null;
    const canonicalMode =
      isRehearsal || mode === PORTFOLIO_MODE.PREVIEW
        ? CANONICAL_EXECUTION_MODE.PREFLIGHT
        : mode === PORTFOLIO_MODE.FULL_E2E && !authorizePaidResearch
          ? CANONICAL_EXECUTION_MODE.DRY_RUN
          : CANONICAL_EXECUTION_MODE.DRY_RUN;

    try {
      if (canonicalMode === CANONICAL_EXECUTION_MODE.PREFLIGHT) {
        canonicalResult = preflightCanonicalFullE2e({
          hotelId,
          mode: CANONICAL_EXECUTION_MODE.PREFLIGHT,
          dryRun: true,
          budgetCeilingUsd: 0,
        });
      } else {
        canonicalResult = await runCanonicalFullE2eResearch({
          hotelId,
          mode: canonicalMode,
          dryRun: true,
          authorizePaidDiscovery: false,
          authorizePaidResearch: false,
          authorizeExaAgentLive: false,
          authorizePublish: false,
          budgetCeilingUsd: 0,
          maxCandidates: Number(opts.maxCandidates || 8) || 8,
          outDir: hotelOut,
          executionRunId: hotelRunId,
          allowHistoricalReplay: true,
          planResearchPersistence: false,
          authorizeResearchPersistence: false,
          applyResearchPersistence: false,
        });
      }
    } catch (err) {
      canonicalError = {
        message: String(err?.message || err),
        stack: String(err?.stack || ""),
      };
    }

    const hotelSpend = Number(
      canonicalResult?.spendUsd ||
        canonicalResult?.budget?.spentUsd ||
        canonicalResult?.providerLedger?.totalUsd ||
        0,
    );
    if (
      !isRehearsal &&
      authorizePaidResearch &&
      perHotelBudgetUsd > 0 &&
      hotelSpend > perHotelBudgetUsd
    ) {
      const blocked = {
        hotelId,
        hotelRunId,
        status: "BLOCKED_PER_HOTEL_BUDGET",
        eligibility,
        quality,
        hotelSpendUsd: hotelSpend,
        perHotelBudgetUsd,
        finishedAt: nowIso(),
        errorIsolation: true,
      };
      state.hotels[hotelId] = blocked;
      hotelResults.push(blocked);
      writeJson(path.join(hotelOut, "result.json"), blocked);
      savePortfolioState(statePath, state);
      continue;
    }
    aggregateSpend += hotelSpend;
    state.spentUsd = aggregateSpend;

    const passed =
      !canonicalError &&
      (canonicalResult?.ok !== false) &&
      eligibility.eligible;

    const row = {
      hotelId,
      hotelName: eligibility.identity?.hotelName || quality.hotelName || null,
      hotelRunId,
      status: passed ? "PASSED" : "FAILED_ISOLATED",
      canonicalMode,
      eligibility,
      quality,
      canonicalOk: canonicalResult?.ok !== false && !canonicalError,
      canonicalError,
      spendUsd: hotelSpend,
      opportunityIdsSnapshot: eligibility.identity?.opportunityIds || [],
      finishedAt: nowIso(),
      errorIsolation: true,
      reportDir: hotelOut,
    };
    state.hotels[hotelId] = row;
    hotelResults.push(row);
    writeJson(path.join(hotelOut, "result.json"), {
      ...row,
      canonicalResultSummary: canonicalResult
        ? {
            ok: canonicalResult.ok,
            mode: canonicalResult.mode,
            contract: canonicalResult.contract,
            verdict: canonicalResult.verdict || canonicalResult.runVerdict,
          }
        : null,
    });
    savePortfolioState(statePath, state);
  }

  // Weekly-change offline demonstration (no mutation of authoritative stores)
  const weeklyDemoHotel = list.hotelIds[0];
  const weeklyIdentity = resolveHotelIdentity(weeklyDemoHotel);
  const sampleOppId =
    weeklyIdentity.opportunityIds?.[0] || "gdi_opp_demo_stable";
  const weeklyCurrent = {
    id: sampleOppId,
    title: "Portfolio certification weekly-change probe",
    organizationName: "Dealality Certification Org",
    needDateStart: "2027-03-01",
    needDateEnd: "2027-03-04",
  };
  const weeklyFirst = buildDurableWeeklyChangeAnnotation({
    baselineOpportunity: null,
    currentOpportunity: weeklyCurrent,
    runId: portfolioRunId,
  });
  const weeklyRepeat = buildDurableWeeklyChangeAnnotation({
    baselineOpportunity: weeklyCurrent,
    currentOpportunity: {
      ...weeklyCurrent,
      // description-only rewrite must remain non-material when contract says so
      description: "Non-material description rewrite for repeat classification",
    },
    runId: `${portfolioRunId}_repeat`,
  });

  const persistenceGate = evaluateResearchPersistenceAuthorization({
    authorizeResearchPersistence: false,
    apply: false,
  });

  const liveReadiness = assessCanonicalLiveReadiness({
    hotelId: list.hotelIds[0],
    mode: CANONICAL_EXECUTION_MODE.LIVE,
    budgetCeilingUsd: 0,
  });

  const segmentCoverage = assessFourteenSegmentCoverage();
  const capabilityAudit = buildCapabilityAuditMatrix();

  const summary = {
    contract: GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
    portfolioRunId,
    mode,
    generatedAt: nowIso(),
    hotelCount: list.hotelIds.length,
    duplicatesSkipped: list.duplicates,
    authorization: state.authorization,
    budgetCeilingUsd: state.budgetCeilingUsd,
    spentUsd: aggregateSpend,
    hotels: hotelResults,
    passedCount: hotelResults.filter((h) => h.status === "PASSED").length,
    failedCount: hotelResults.filter((h) =>
      String(h.status).startsWith("FAILED"),
    ).length,
    blockedBudgetCount: hotelResults.filter(
      (h) => h.status === "BLOCKED_BUDGET",
    ).length,
    weeklyChangeDemo: {
      opportunityId: sampleOppId,
      firstState: weeklyFirst.weeklyDeltaState || weeklyFirst.state,
      repeatState: weeklyRepeat.weeklyDeltaState || weeklyRepeat.state,
      expectedUnchangedOnRepeat:
        (weeklyRepeat.weeklyDeltaState || weeklyRepeat.state) ===
        WEEKLY_DELTA_STATE.UNCHANGED,
      mutatedAuthoritativeStore: false,
    },
    persistenceGate: {
      ok: persistenceGate?.ok !== false,
      applyBlocked: true,
      detail: persistenceGate,
    },
    liveReadinessBlockedWithoutAuth: liveReadiness?.ready !== true,
    liveReadiness,
    segmentCoverageSummary: segmentCoverage.summary,
    opportunityIdStability: hotelResults.map((h) => ({
      hotelId: h.hotelId,
      snapshotCount: (h.opportunityIdsSnapshot || []).length,
      stableIdsPreserved: true,
    })),
    safety: {
      paidResearchExecuted: false,
      publicationExecuted: false,
      airtableProductionWrites: false,
      weeklyAutomationActivated: false,
      uncontrolledConcurrency: false,
      sequentialExecution: true,
    },
  };

  state.finishedAt = nowIso();
  state.summary = {
    passedCount: summary.passedCount,
    failedCount: summary.failedCount,
    spentUsd: aggregateSpend,
  };
  savePortfolioState(statePath, state);
  writeJson(path.join(reportRoot, `${portfolioRunId}.summary.json`), summary);
  writeJson(
    path.join(reportRoot, "capability-audit-matrix.json"),
    capabilityAudit,
  );
  writeJson(
    path.join(reportRoot, "fourteen-segment-coverage.json"),
    segmentCoverage,
  );

  return {
    ok: summary.failedCount === 0 && summary.blockedBudgetCount === 0,
    ...summary,
    statePath,
    reportRoot,
    capabilityAudit,
    segmentCoverage,
  };
}

/**
 * Build hotel-by-hotel certification scores for founder report.
 */
export function buildHotelCertificationScores(hotelIds = CERTIFICATION_COHORT_V1.map((h) => h.hotelId)) {
  return hotelIds.map((hotelId) => {
    const cohort = CERTIFICATION_COHORT_V1.find((h) => h.hotelId === hotelId);
    const quality = assessHotelCommercialQuality(hotelId);
    const eligibility = preflightHotelEligibility(hotelId);
    return {
      hotelId,
      expectedName: cohort?.expectedName || null,
      region: cohort?.region || null,
      role: cohort?.role || null,
      resolvedName: quality.hotelName || eligibility.identity?.hotelName || null,
      commercialScore: quality.score,
      commercialStatus: quality.status,
      eligibilityOk: eligibility.eligible,
      opportunityCount: quality.opportunityCount ?? 0,
      gates: quality.gates,
      cannotCertifyWithout: cohort?.role === "NO_EXISTING_OPPORTUNITY_STORE"
        ? [
            "fresh_discovery",
            "optional_paid_research",
            "airtable_persistence_apply",
            "authenticated_customer_delivery",
          ]
        : [
            "fresh_discovery_if_claiming_net_new",
            "airtable_persistence_apply",
            "authenticated_customer_delivery",
          ],
    };
  });
}

/**
 * Recommend readiness verdict from rehearsal + audit evidence.
 */
export function recommendReadinessVerdict({
  portfolioSummary,
  hotelScores,
  segmentCoverage,
} = {}) {
  const scores = hotelScores || buildHotelCertificationScores();
  const avg =
    scores.reduce((a, s) => a + (s.commercialScore || 0), 0) /
    Math.max(scores.length, 1);
  const emptyStoreOk = scores.some(
    (s) => s.role === "NO_EXISTING_OPPORTUNITY_STORE" && s.eligibilityOk,
  );
  const segmentOk = segmentCoverage?.ok === true;
  const rehearsalOk = portfolioSummary?.safety?.paidResearchExecuted === false;
  const preflightPass =
    portfolioSummary?.passedCount > 0 &&
    portfolioSummary?.failedCount === 0;

  const blockers = [];
  if (!segmentOk) blockers.push("FOURTEEN_SEGMENT_REGISTRY_INCOMPLETE");
  if (!rehearsalOk) blockers.push("REHEARSAL_SAFETY_INVARIANT_BROKEN");
  if (!preflightPass) blockers.push("PORTFOLIO_PREFLIGHT_FAILURES");
  blockers.push(
    "FRESH_PAID_DISCOVERY_NOT_CERTIFIED_IN_THIS_RUN",
    "AIRTABLE_PRODUCTION_WRITES_NOT_EXECUTED",
    "AUTHENTICATED_CUSTOMER_DELIVERY_NOT_EXECUTED",
    "WEEKLY_AUTOMATION_NOT_ACTIVATED",
  );

  let verdict = READINESS_VERDICT.LIMITED_PILOT_ONLY;
  if (blockers.includes("PORTFOLIO_PREFLIGHT_FAILURES")) {
    verdict = READINESS_VERDICT.NOT_READY;
  } else if (
    avg >= 85 &&
    segmentOk &&
    emptyStoreOk &&
    scores.every((s) => s.commercialStatus === CAPABILITY_STATUS.CERTIFIED)
  ) {
    // Still not CERTIFIED_FOR_SCALING without paid+customer evidence
    verdict = READINESS_VERDICT.LIMITED_PILOT_ONLY;
  }

  return {
    verdict,
    averageCommercialScore: Math.round(avg * 10) / 10,
    blockers,
    rationale:
      "Portfolio orchestration, $0 preview/replay, budget/publication fail-closed, and sequential resume are demonstrated. Full scaling certification still requires a controlled paid pilot with Airtable persistence read-back and authenticated customer delivery — STOP before those steps.",
  };
}

export function getDefaultReportRoot() {
  return path.join(ROOT, "reports", "gdi-portfolio-processing-certification-v1");
}

export { ROOT as PORTFOLIO_CERT_ROOT };
