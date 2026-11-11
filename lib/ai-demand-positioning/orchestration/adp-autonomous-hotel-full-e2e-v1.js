/**
 * ADP_AUTONOMOUS_HOTEL_FULL_E2E_V1
 *
 * Single orchestration contract for Full ADP E2E — [Hotel Name].
 * Invokes existing canonical modules; does not rebuild measurement/report engines.
 * Default: no paid monitoring, no customer publication.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";
import { loadPropertyProfile, loadLatestCoreCustomerPeriod, loadAllPeriods, isLoyaltyMeasurementPeriod } from "../data-model.js";
import { resolveOsHotelIdentity } from "../../dealality-full-e2e-os/hotel-identity.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { independentlyRecalculateRealityGapV1V2 } from "../certification/adp-rg-v2-cross-surface-parity-certification-v1.js";
import { assessRealityGapUniquenessFromPayload } from "../certification/adp-reality-gap-canonical-attribute-uniqueness-v1.js";
import { auditHotelCustomerReleaseContent } from "../certification/adp-31-hotel-customer-release-acceptance-v1.js";
import {
  ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
  runDedupGate1PreMonitoring,
  runDedupGate2PreCalculation,
  runDedupGate3PreCustomerRelease,
  runAllDedupGates,
} from "../certification/adp-full-e2e-canonical-attribute-dedup-gate-v1.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import { PROVIDER_CONFIGS } from "../execution/multi-provider-runner.js";
import { CONDITIONAL_APPROVED_METHODOLOGY_ID } from "../intelligence/reality-gap-methodology-v2.js";
import { ADP_CERTIFIED_31_HOTEL_BASELINE_ID } from "../certification/adp-certified-31-hotel-baseline-v1.js";
import { coverageScoreForProfile } from "../property-attributes/adp-hotel-attribute-intelligence-standard-v1.js";
import { resolveCanonicalAttributeSet } from "../property-attributes/adp-reality-gap-canonical-attribute-dedup-v1.js";
import { ATTRIBUTE_DEFINITIONS } from "../intelligence/reality-gap.js";
import {
  getCensusRecordIdForAdpProperty,
  findCensusLinkedPropertiesByHotelQuery,
} from "../census-link-registry.js";
import { findPosadasGreenfieldCandidate } from "../greenfield/adp-posadas-greenfield-candidates-v1.js";
import {
  executeAuthorizedSupervisorHandoff,
  loadAuthorizationForRun,
  ADP_PAID_SUPERVISOR_HANDOFF_V1,
} from "./adp-paid-supervisor-handoff-v1.js";
import { executeAuthorizedGreenfieldLiveMonitoring } from "./adp-greenfield-live-monitoring-v1.js";
import {
  completeFullE2eArtifactsAndBrowserQa,
  evaluateFullE2eStageOrderInvariants,
  permanentStageOrderNegativeFixtures,
  ADP_FULL_E2E_ARTIFACT_AND_BROWSER_QA_V1,
} from "./adp-full-e2e-artifact-and-browser-qa-v1.js";
import {
  CONTRACT_ID as ADP_AUTONOMOUS_COMPETITOR_ENTITY_RESOLUTION_V1,
  ensureAutonomousCompetitorRegistry,
  reconcileDisplacementFourState,
  assessSilentCompetitorEvidenceLoss,
} from "../certification/adp-autonomous-competitor-entity-resolution-v1.js";
import {
  performLiveHpcIdentityCollisionCheck,
  mayReusePriorHpcLiveResult,
  hpcQueryFingerprint,
  HPC_LIVE_OUTCOME,
  HPC_LIVE_IDENTITY_COLLISION_V1,
} from "../../hotel-census/hpc-live-identity-collision-v1.js";
import {
  executeAuthorizedHpcCreate,
  loadHpcCreateAuthorization,
  buildChapultepecHpcCreateProposal,
  prepareValidatedCreateProposal,
  HPC_AUTHORIZED_CREATE_REUSE_V1,
  HPC_CREATE_REUSE_STATE,
} from "../../hotel-census/hpc-authorized-create-reuse-v1.js";
import { planFullE2eBppMeasurement } from "../certification/adp-full-e2e-bpp-measurement-completeness-v1.js";
import {
  ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
  buildMandatoryBppPremonitoringCostContract,
  assessFullE2eCompleteBppRequired,
} from "../certification/adp-full-e2e-complete-bpp-required-v1.js";
import {
  ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
  ensureFullAdpE2eAiAdminRegistration,
} from "../governance/adp-full-e2e-ai-admin-auto-registration-v1.js";
import {
  ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1,
  prepareExternalShareWorkflowAfterCertification,
  resolveExternalShareLifecycleState,
} from "../share/adp-full-e2e-external-client-share-lifecycle-v1.js";
import {
  ADP_FULL_E2E_PRODUCTION_RELEASE_WORKFLOW_V1,
  prepareProductionReleaseManifest,
} from "../share/adp-full-e2e-production-release-workflow-v1.js";
import {
  ADP_CANONICAL_HOTEL_LIFECYCLE_STATE_MACHINE_V1,
  factsFromOrchestratorManifest,
  resolveHotelLifecycleState,
  saveHotelLifecycleState,
  FOUNDER_AUTHORIZATION_SCOPES,
} from "./adp-canonical-hotel-lifecycle-state-machine-v1.js";
import {
  ADP_EXECUTIVE_PDF_CANONICAL_TEMPLATE_ENFORCEMENT_V1,
  CANONICAL_PDF_TEMPLATE_ID,
  isPortfolioTemplateReuseAuthorized,
} from "../full-adp-e2e-pdf/adp-executive-pdf-canonical-template-enforcement-v1.js";
import {
  ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
  ensureFullAdpE2eAiAdminCertificationParity,
} from "../governance/adp-full-e2e-ai-admin-certification-parity-v1.js";
import {
  ADP_FULL_E2E_PDF_VISUAL_AND_ARTIFACT_PARITY_V1,
  ensureFullAdpE2ePdfVisualAndArtifactParity,
} from "../full-adp-e2e-pdf/adp-full-e2e-pdf-visual-and-artifact-parity-v1.js";
import {
  ADP_AI_ADMIN_AUTHORIZED_PDF_DELIVERY_V1,
  ensureAiAdminAuthorizedPdfDeliveryV1,
  AUTHENTICATED_PDF_QA_PENDING,
  MEMBERSTACK_AUTHENTICATED_PDF_PASS,
} from "../full-adp-e2e-pdf/adp-ai-admin-authorized-pdf-delivery-v1.js";
import {
  ADP_FULL_E2E_AIRTABLE_HISTORY_PERSISTENCE_GATE_V1,
  HISTORY_SYNC_STATUS,
  assessAdpAirtableHistorySyncGate,
  stagedOwnerPayloadToHistoryShape,
} from "../longitudinal/adp-full-e2e-airtable-history-persistence-gate-v1.js";
import {
  ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1,
  ENFORCEMENT_POINTS,
  enforceMeasurementReconciliationGate,
  attachPermanentCertificationToManifest,
  assertFullE2eManifestCertification,
} from "../certification/permanent-full-e2e/adp-permanent-full-e2e-certification-gate-v1.js";
import {
  persistAdpHistoricalPeriodV1,
} from "../longitudinal/persist-adp-historical-period-v1.js";
import { isAdpHistoryWritesEnabled } from "../longitudinal/airtable-history-schema-final-v1.js";
import {
  ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1,
  listFullE2eMeasurementHistoryTables,
  listPublicationOnlyTables,
  resolveLiveTableName,
} from "../longitudinal/adp-airtable-ecosystem-contract-v1.js";

export const ADP_AUTONOMOUS_HOTEL_FULL_E2E_V1 = "ADP_AUTONOMOUS_HOTEL_FULL_E2E_V1";
export const ORCHESTRATOR_VERSION = "adp-autonomous-hotel-full-e2e-v1.8.0";
export {
  completeFullE2eArtifactsAndBrowserQa,
  evaluateFullE2eStageOrderInvariants,
  permanentStageOrderNegativeFixtures,
  ADP_FULL_E2E_ARTIFACT_AND_BROWSER_QA_V1,
};

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
export const ORCHESTRATOR_RUNS_ROOT = path.join(
  ROOT,
  "data/ai-demand-positioning/autonomous-full-e2e-runs"
);
export const ORCHESTRATOR_REPORTS_ROOT = path.join(
  ROOT,
  "reports/dealality-adp-autonomous-full-e2e-orchestrator-v1"
);

export const ORCHESTRATOR_MODE = Object.freeze({
  PREFLIGHT: "preflight",
  REPLAY: "replay",
  FULL: "full",
  RESUME: "resume",
  VERIFY: "verify",
  AUDIT: "audit",
});

export const RUN_STATE = Object.freeze({
  PREFLIGHT: "PREFLIGHT",
  IDENTITY_VERIFIED: "IDENTITY_VERIFIED",
  IDENTITY_APPROVAL_REQUIRED: "IDENTITY_APPROVAL_REQUIRED",
  RESEARCH_COMPLETE: "RESEARCH_COMPLETE",
  AIRTABLE_VERIFIED: "AIRTABLE_VERIFIED",
  ATTRIBUTES_CERTIFIED: "ATTRIBUTES_CERTIFIED",
  PEER_GOVERNANCE_COMPLETE: "PEER_GOVERNANCE_COMPLETE",
  AWAITING_PAID_RESEARCH_APPROVAL: "AWAITING_PAID_RESEARCH_APPROVAL",
  AWAITING_HISTORY_WRITE_APPROVAL: "AWAITING_HISTORY_WRITE_APPROVAL",
  CORE_RESEARCH_COMPLETE: "CORE_RESEARCH_COMPLETE",
  LOYALTY_RESEARCH_COMPLETE: "LOYALTY_RESEARCH_COMPLETE",
  CALCULATIONS_CERTIFIED: "CALCULATIONS_CERTIFIED",
  REPORT_STAGED: "REPORT_STAGED",
  CONTENT_READY: "CONTENT_READY",
  ARTIFACTS_PENDING: "ARTIFACTS_PENDING",
  ARTIFACTS_VERIFIED: "ARTIFACTS_VERIFIED",
  BROWSER_QA_PENDING: "BROWSER_QA_PENDING",
  PDF_SHARE_VERIFIED: "PDF_SHARE_VERIFIED",
  CUSTOMER_QA_COMPLETE: "CUSTOMER_QA_COMPLETE",
  CUSTOMER_READY: "CUSTOMER_READY",
  CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS: "CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS",
  CUSTOMER_QA_BLOCKED: "CUSTOMER_QA_BLOCKED",
  NOT_CUSTOMER_READY: "NOT_CUSTOMER_READY",
  AWAITING_PUBLICATION_APPROVAL: "AWAITING_PUBLICATION_APPROVAL",
  BLOCKED: "BLOCKED",
  COMPLETE: "COMPLETE",
});

/** Stage 0–12 contracts (stable IDs). */
export const PIPELINE_STAGES = Object.freeze([
  {
    id: "S0_INTAKE_IDENTITY",
    name: "Intake and identity resolution",
    stateOnPass: RUN_STATE.IDENTITY_VERIFIED,
    paid: false,
    approvalRequired: false,
    approvalOnAmbiguity: "IDENTITY_APPROVAL_REQUIRED",
  },
  {
    id: "S1_LIVE_PROPERTY_RESEARCH",
    name: "Live property research",
    stateOnPass: RUN_STATE.RESEARCH_COMPLETE,
    paid: false,
    approvalRequired: false,
  },
  {
    id: "S2_HPC_ADP_PERSISTENCE",
    name: "HPC and ADP data persistence",
    stateOnPass: RUN_STATE.AIRTABLE_VERIFIED,
    paid: false,
    approvalRequired: true,
    note: "Identity-affecting Airtable writes require explicit approval",
  },
  {
    id: "S3_ATTRIBUTE_INTELLIGENCE",
    name: "Attribute intelligence certification",
    stateOnPass: RUN_STATE.ATTRIBUTES_CERTIFIED,
    paid: false,
    approvalRequired: false,
    dedupGate: "GATE1_PRE_MONITORING",
    note: "ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1 Gate 1 — before paid monitoring",
  },
  {
    id: "S4_PEER_GOVERNANCE",
    name: "Brand and portfolio peer governance",
    stateOnPass: RUN_STATE.PEER_GOVERNANCE_COMPLETE,
    paid: false,
    approvalRequired: true,
    note: "Peer membership changes require founder approval when governance demands",
  },
  {
    id: "S5_COST_AUTHORIZATION",
    name: "Research cost and authorization",
    stateOnPass: RUN_STATE.AWAITING_PAID_RESEARCH_APPROVAL,
    paid: false,
    approvalRequired: true,
    hardGate: true,
  },
  {
    id: "S6_CORE_MONITORING",
    name: "Core monitoring",
    stateOnPass: RUN_STATE.CORE_RESEARCH_COMPLETE,
    paid: true,
    approvalRequired: true,
  },
  {
    id: "S7_LOYALTY_MONITORING",
    name: "Applicable Loyalty monitoring",
    stateOnPass: RUN_STATE.LOYALTY_RESEARCH_COMPLETE,
    paid: true,
    approvalRequired: true,
  },
  {
    id: "S8_ANALYTICAL_CALCULATIONS",
    name: "Independent analytical calculations",
    stateOnPass: RUN_STATE.CALCULATIONS_CERTIFIED,
    paid: false,
    approvalRequired: false,
    dedupGate: "GATE2_PRE_CALCULATION",
    note:
      "S8a archive completeness → S8b competitor discovery → S8c entity resolution (ADP_AUTONOMOUS_COMPETITOR_ENTITY_RESOLUTION_V1) → S8d displacement certification → S8e remaining analytics. Gate 2 RG uniqueness.",
  },
  {
    id: "S8_MEASUREMENT_RECONCILIATION",
    name: "Permanent measurement reconciliation certification",
    stateOnPass: RUN_STATE.CALCULATIONS_CERTIFIED,
    paid: false,
    approvalRequired: false,
    hardGate: true,
    note:
      "ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1 — original response authority, frozen eligibility, Consideration/Provider/Scenario reconciliation. Fail closed before report generation.",
  },
  {
    id: "S9_CUSTOMER_REPORT",
    name: "Customer-facing report generation",
    stateOnPass: RUN_STATE.REPORT_STAGED,
    paid: false,
    approvalRequired: false,
  },
  {
    id: "S9B_AIRTABLE_HISTORY_PERSISTENCE",
    name: "Governed ADP Airtable ecosystem persistence (measurement-history scope)",
    stateOnPass: RUN_STATE.AWAITING_HISTORY_WRITE_APPROVAL,
    paid: false,
    approvalRequired: true,
    note:
      "ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1 — sync all MEASUREMENT_HISTORY tables (incl. Prompt Ledger). Never writes without ADP_HISTORY_WRITES_ENABLED + ADP_HISTORY_AIRTABLE_WRITE_APPLY + allowGovernedProductionHistoryBase. Does not authorize paid research, publication, attributes, or leak-audit. Published Reports is publication-only.",
  },
  {
    id: "S10_PDF_SHARE",
    name: "PDF and share generation",
    stateOnPass: RUN_STATE.PDF_SHARE_VERIFIED,
    paid: false,
    approvalRequired: false,
    note: "Test-only pinned shares; no production tokens",
  },
  {
    id: "S11_CUSTOMER_RELEASE_AUDIT",
    name: "Complete content and artifact audit",
    stateOnPass: RUN_STATE.CUSTOMER_QA_COMPLETE,
    paid: false,
    approvalRequired: false,
    dedupGate: "GATE3_PRE_CUSTOMER_RELEASE",
    note: "ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1 Gate 3 — blocks CUSTOMER_READY on duplicate cards",
  },
  {
    id: "S12_FOUNDER_READINESS",
    name: "Founder review and final readiness",
    stateOnPass: RUN_STATE.AWAITING_PUBLICATION_APPROVAL,
    paid: false,
    approvalRequired: true,
    note: "Publication is a separate explicit authorization",
  },
]);

const CORE_SCENARIO_TARGET = 63;
const LOYALTY_SCENARIO_TARGET = 28;
const PROVIDERS = ["openai", "gemini", "perplexity", "claude"];
const MAX_SAFE_RETRIES = 3;

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
}
function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return fallback;
  }
}
function sha16(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}
function normName(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function tokenSet(s) {
  return new Set(normName(s).split(" ").filter((t) => t.length > 1));
}
function jaccard(a, b) {
  const A = tokenSet(a);
  const B = tokenSet(b);
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter += 1;
  return inter / new Set([...A, ...B]).size;
}

/**
 * Parse "Full ADP E2E — Hotel Name" or plain hotel string.
 */
export function parseFullAdpE2eInstruction(raw) {
  const text = String(raw || "").trim();
  const m = text.match(/^full\s+adp\s+e2e\s*[—\-–:]\s*(.+)$/i);
  return {
    instruction: text,
    hotelQuery: (m ? m[1] : text).trim(),
    naturalLanguage: Boolean(m),
  };
}

/**
 * Resolve hotel identity from adp_ id, HPC rec, or display name.
 * Ambiguous name matches → IDENTITY_APPROVAL_REQUIRED (ok:false with candidates).
 */
export function resolveHotelIdentityForOrchestrator(hotelQuery) {
  const q = String(hotelQuery || "").trim();
  if (!q) return { ok: false, error: "MISSING_HOTEL_QUERY", state: RUN_STATE.BLOCKED };

  // Exact OS path first (adp_ / rec)
  const direct = resolveOsHotelIdentity(q, { product: "ADP" });
  if (direct.ok) {
    const profile = loadPropertyProfile(direct.adpPropertyId);
    return {
      ok: true,
      state: RUN_STATE.IDENTITY_VERIFIED,
      ...direct,
      propertyName: profile?.name || direct.propertyName || q,
      matchMethod: q.startsWith("adp_") || q.startsWith("rec") ? "exact_id" : "os_resolver",
      inCertified31: listCanonicalAdp31PropertyIds().includes(direct.adpPropertyId),
      candidates: [],
    };
  }

  // Name / fuzzy against certified 31 + census-link names
  const needle = normName(q);
  const scored = [];
  for (const propertyId of listCanonicalAdp31PropertyIds()) {
    const profile = loadPropertyProfile(propertyId);
    const name = profile?.name || "";
    const score = Math.max(jaccard(q, name), needle && normName(name).includes(needle) ? 0.85 : 0);
    const exact = normName(name) === needle;
    if (exact || score >= 0.55) {
      scored.push({
        propertyId,
        hotelName: name,
        hpcId: getCensusRecordIdForAdpProperty(propertyId) || null,
        score: exact ? 1 : score,
        exact,
        inCertified31: true,
      });
    }
  }
  scored.sort((a, b) => b.score - a.score);

  if (scored.length === 1 && (scored[0].exact || scored[0].score >= 0.72)) {
    const hit = scored[0];
    const os = resolveOsHotelIdentity(hit.propertyId, { product: "ADP" });
    return {
      ok: true,
      state: RUN_STATE.IDENTITY_VERIFIED,
      ...(os.ok ? os : { adpPropertyId: hit.propertyId, hpcId: hit.hpcId }),
      propertyName: hit.hotelName,
      matchMethod: hit.exact ? "exact_name" : "fuzzy_name",
      matchScore: hit.score,
      inCertified31: true,
      candidates: scored,
    };
  }

  if (scored.length > 1) {
    return {
      ok: false,
      error: "IDENTITY_APPROVAL_REQUIRED",
      state: RUN_STATE.IDENTITY_APPROVAL_REQUIRED,
      hotelQuery: q,
      message:
        "Multiple candidate identities matched. Founder must select the exact property before continuing.",
      candidates: scored.slice(0, 8),
    };
  }

  if (scored.length === 1 && scored[0].score < 0.72) {
    return {
      ok: false,
      error: "IDENTITY_APPROVAL_REQUIRED",
      state: RUN_STATE.IDENTITY_APPROVAL_REQUIRED,
      hotelQuery: q,
      message: "Weak single match — confirm before treating as this hotel.",
      candidates: scored,
    };
  }

  // Greenfield — not in certified 31 (enriched if Posadas candidate matches).
  // Provisional ok=true so Full/Resume reach S2 + paid gates; HPC create/reuse
  // and founder approvals still block chargeable work.
  const posadas = findPosadasGreenfieldCandidate(q);
  if (posadas) {
    const profile = loadPropertyProfile(posadas.draftPropertyId);
    return {
      ok: true,
      provisionalGreenfield: true,
      state: RUN_STATE.IDENTITY_VERIFIED,
      hotelQuery: q,
      inCertified31: false,
      greenfield: true,
      posadasCandidate: true,
      adpPropertyId: posadas.draftPropertyId,
      draftPropertyId: posadas.draftPropertyId,
      propertyName: posadas.officialName,
      hpcId: posadas.knownHpcId || profile?.censusRecordId || profile?.hpcHotelId || null,
      matchMethod: "posadas_greenfield_candidate",
      proposedIdentity: posadas,
      message:
        "Grupo Posadas greenfield candidate matched (provisional). Live HPC S2 + create/reuse authorization still required before paid monitoring.",
      candidates: [
        {
          propertyId: posadas.draftPropertyId,
          hotelName: posadas.officialName,
          hpcId: posadas.knownHpcId || null,
          score: 1,
          exact: true,
          inCertified31: false,
          greenfield: true,
          runOrder: posadas.runOrder,
        },
      ],
      proposedSlug: posadas.draftPropertyId.replace(/^adp_/, ""),
      approvalsStillRequired: ["HPC_CREATE_OR_REUSE", "PEER_COHORT", "PAID_RESEARCH_BUDGET"],
    };
  }

  // Post-HPC greenfield via census-links (any brand — not Posadas-only).
  // Enables Full/Resume after founder-authorized HPC create without hotel-specific forks.
  const censusHits = findCensusLinkedPropertiesByHotelQuery(q).filter(
    (h) => h.hpcId && String(h.hpcId).startsWith("rec")
  );
  const exactCensusHits = censusHits.filter((h) => h.exact);
  const resolvableCensusHits =
    exactCensusHits.length === 1 ? exactCensusHits : censusHits;
  if (
    resolvableCensusHits.length === 1 &&
    (resolvableCensusHits[0].exact || resolvableCensusHits[0].score >= 0.85)
  ) {
    const hit = resolvableCensusHits[0];
    const profile = loadPropertyProfile(hit.propertyId);
    const os = resolveOsHotelIdentity(hit.propertyId, { product: "ADP" });
    return {
      ok: true,
      provisionalGreenfield: !listCanonicalAdp31PropertyIds().includes(hit.propertyId),
      state: RUN_STATE.IDENTITY_VERIFIED,
      hotelQuery: q,
      inCertified31: listCanonicalAdp31PropertyIds().includes(hit.propertyId),
      greenfield: !listCanonicalAdp31PropertyIds().includes(hit.propertyId),
      adpPropertyId: hit.propertyId,
      draftPropertyId: hit.propertyId,
      propertyName: profile?.name || hit.hotelName,
      hpcId: (os.ok && os.hpcId) || hit.hpcId || profile?.censusRecordId || null,
      matchMethod: "census_link_greenfield",
      message:
        "Census-linked greenfield identity matched. Live HPC S2 verification still required; paid monitoring remains authorization-gated.",
      candidates: censusHits.slice(0, 8),
      proposedSlug: hit.propertyId.replace(/^adp_/, ""),
      approvalsStillRequired: ["PAID_RESEARCH_BUDGET"],
    };
  }
  if (resolvableCensusHits.length > 1) {
    return {
      ok: false,
      error: "IDENTITY_APPROVAL_REQUIRED",
      state: RUN_STATE.IDENTITY_APPROVAL_REQUIRED,
      hotelQuery: q,
      message:
        "Multiple census-linked greenfield identities matched. Founder must select the exact property.",
      candidates: resolvableCensusHits.slice(0, 8),
      greenfield: true,
    };
  }

  return {
    ok: false,
    error: "NEW_HOTEL_INTAKE_REQUIRED",
    state: RUN_STATE.IDENTITY_APPROVAL_REQUIRED,
    hotelQuery: q,
    inCertified31: false,
    greenfield: true,
    message:
      "No certified ADP registry match. Treat as new-hotel intake: confirm official identity, HPC create/link, then proceed through preflight → paid authorization.",
    candidates: [],
    proposedSlug: normName(q).replace(/\s+/g, "_").slice(0, 48),
  };
}

export function estimateMonitoringCost({
  coreScenarios = CORE_SCENARIO_TARGET,
  loyaltyScenarios = LOYALTY_SCENARIO_TARGET,
  loyaltyApplicable = true,
  reusableCoreObservations = 0,
  reusableLoyaltyObservations = 0,
  peerLoyaltyHotelsPending = 0,
  peerLoyaltyScenarios = LOYALTY_SCENARIO_TARGET,
} = {}) {
  const perScenario =
    PROVIDERS.reduce((s, p) => s + (PROVIDER_CONFIGS[p]?.costPerCall || 0), 0);
  const coreCalls = Math.max(0, coreScenarios * PROVIDERS.length - reusableCoreObservations);
  const loyaltyCalls = loyaltyApplicable
    ? Math.max(0, loyaltyScenarios * PROVIDERS.length - reusableLoyaltyObservations)
    : 0;
  const coreUsd = Number((coreCalls * (perScenario / PROVIDERS.length)).toFixed(2));
  // clearer: sum by provider
  let coreTotal = 0;
  let loyaltyTotal = 0;
  const byProvider = {};
  for (const p of PROVIDERS) {
    const unit = PROVIDER_CONFIGS[p].costPerCall;
    const cCore = Math.max(0, coreScenarios - Math.floor(reusableCoreObservations / PROVIDERS.length));
    const cLoy = loyaltyApplicable
      ? Math.max(0, loyaltyScenarios - Math.floor(reusableLoyaltyObservations / PROVIDERS.length))
      : 0;
    byProvider[p] = {
      costPerCall: unit,
      coreCalls: cCore,
      loyaltyCalls: cLoy,
      coreUsd: Number((cCore * unit).toFixed(4)),
      loyaltyUsd: Number((cLoy * unit).toFixed(4)),
    };
    coreTotal += cCore * unit;
    loyaltyTotal += cLoy * unit;
  }
  let peerTotal = 0;
  const peerByProvider = {};
  const peerHotels = Math.max(0, Number(peerLoyaltyHotelsPending) || 0);
  if (peerHotels > 0) {
    for (const p of PROVIDERS) {
      const unit = PROVIDER_CONFIGS[p].costPerCall;
      const calls = peerHotels * peerLoyaltyScenarios;
      peerByProvider[p] = {
        costPerCall: unit,
        peerCalls: calls,
        peerUsd: Number((calls * unit).toFixed(4)),
      };
      peerTotal += calls * unit;
      if (byProvider[p]) {
        byProvider[p].peerCalls = calls;
        byProvider[p].peerUsd = Number((calls * unit).toFixed(4));
      }
    }
  }

  return {
    providers: PROVIDERS,
    coreScenarios,
    loyaltyScenarios: loyaltyApplicable ? loyaltyScenarios : 0,
    loyaltyApplicable,
    peerLoyaltyHotelsPending: peerHotels,
    peerLoyaltyScenarios: peerHotels > 0 ? peerLoyaltyScenarios : 0,
    expectedCoreResponses: coreScenarios * PROVIDERS.length,
    expectedLoyaltyResponses: loyaltyApplicable ? loyaltyScenarios * PROVIDERS.length : 0,
    expectedPeerLoyaltyResponses: peerHotels * peerLoyaltyScenarios * PROVIDERS.length,
    reusableCoreObservations,
    reusableLoyaltyObservations,
    byProvider,
    peerByProvider,
    estimatedCoreUsd: Number(coreTotal.toFixed(2)),
    estimatedLoyaltyUsd: Number(loyaltyTotal.toFixed(2)),
    estimatedPeerLoyaltyUsd: Number(peerTotal.toFixed(2)),
    estimatedTotalUsd: Number((coreTotal + loyaltyTotal + peerTotal).toFixed(2)),
    hardMaxSuggestedUsd: Number((coreTotal + loyaltyTotal + peerTotal + 2).toFixed(2)),
    bppPeerResearchNote:
      peerHotels > 0
        ? "Peer Loyalty monitoring required for portfolio rank — included in preflight total; separate founder approval if expanding prior subject-only budget"
        : "No pending peer Loyalty hotels in cost plan",
    scheduleNote: "Governed PROVIDER_CONFIGS costPerCall — invoice may differ",
  };
}

function lockPath(propertyKey) {
  return path.join(ORCHESTRATOR_RUNS_ROOT, "_locks", `${propertyKey}.lock`);
}

export function acquireHotelRunLock(propertyKey, runId) {
  ensureDir(path.dirname(lockPath(propertyKey)));
  const lp = lockPath(propertyKey);
  if (fs.existsSync(lp)) {
    const existing = readJson(lp, {});
    if (existing.runId && existing.runId !== runId) {
      const age = Date.now() - Date.parse(existing.acquiredAt || 0);
      if (age < 6 * 60 * 60 * 1000) {
        return { ok: false, error: "CROSS_PROCESS_LOCK_HELD", existing };
      }
    }
  }
  const body = { runId, propertyKey, acquiredAt: new Date().toISOString(), pid: process.pid };
  fs.writeFileSync(lp, `${JSON.stringify(body, null, 2)}\n`);
  return { ok: true, lock: body };
}

export function releaseHotelRunLock(propertyKey, runId) {
  const lp = lockPath(propertyKey);
  if (!fs.existsSync(lp)) return { ok: true };
  const existing = readJson(lp, {});
  if (existing.runId && existing.runId !== runId) {
    return { ok: false, error: "LOCK_OWNED_BY_OTHER", existing };
  }
  fs.unlinkSync(lp);
  return { ok: true };
}

function runDirFor(runId) {
  return path.join(ORCHESTRATOR_RUNS_ROOT, runId);
}

export function loadRunManifest(runId) {
  return readJson(path.join(runDirFor(runId), "run-manifest.json"), null);
}

export function saveRunManifest(manifest) {
  const dir = runDirFor(manifest.runId);
  writeJson(path.join(dir, "run-manifest.json"), manifest);
  writeJson(path.join(dir, "checkpoints", `${manifest.state}.json`), {
    at: new Date().toISOString(),
    state: manifest.state,
    completedStages: manifest.completedStages,
  });
  return manifest;
}

function newRunId(slug) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `adp_e2e_${slug || "hotel"}_${stamp}_${sha16({ stamp }).slice(0, 6)}`;
}

function stageResult(stageId, status, detail = {}) {
  return {
    stageId,
    status,
    at: new Date().toISOString(),
    ...detail,
  };
}

/**
 * Build cost + reuse forecast for a property (or greenfield).
 */
export function buildResearchCostPlan(identity) {
  const propertyId = identity.adpPropertyId || null;
  let reusableCore = 0;
  let reusableLoyalty = 0;
  let loyaltyApplicable = true;
  let peerLoyaltyHotelsPending = 0;
  let bppPlan = null;
  if (propertyId) {
    const core = loadLatestCoreCustomerPeriod(propertyId);
    reusableCore = core?.observations?.length || 0;
    const loyalty = (loadAllPeriods(propertyId) || []).find((p) => isLoyaltyMeasurementPeriod(p));
    reusableLoyalty = loyalty?.observations?.length || 0;
    const peer = getBrandPortfolioPeerSet(propertyId);
    const profile = loadPropertyProfile(propertyId);
    if (!peer && !loyalty) loyaltyApplicable = false;
    if (profile && profile.loyaltyApplicable === false) loyaltyApplicable = false;
    bppPlan = planFullE2eBppMeasurement({ propertyId });
    if (bppPlan?.bppApplies === false || bppPlan?.state === "BPP_NOT_APPLICABLE") {
      peerLoyaltyHotelsPending = 0;
    } else if (bppPlan?.bppApplies) {
      peerLoyaltyHotelsPending = (bppPlan.peers?.missingCorpora || []).length;
      // Never hide peer Loyalty cost when BPP applies but cohort is only proposed.
      if (
        peerLoyaltyHotelsPending === 0 &&
        (bppPlan.peers?.included || []).length === 0 &&
        bppPlan.state !== "BPP_NOT_APPLICABLE"
      ) {
        peerLoyaltyHotelsPending = 3;
      } else if (
        peerLoyaltyHotelsPending === 0 &&
        (bppPlan.peers?.included || []).length > 0 &&
        bppPlan.peers?.membershipApproved !== true
      ) {
        // Proposed peers still need Loyalty corpora once approved — cost them now.
        peerLoyaltyHotelsPending = (bppPlan.peers?.included || []).length;
      }
      if (bppPlan.subject?.state === "BPP_SUBJECT_RESEARCH_PENDING") {
        loyaltyApplicable = true;
      }
    }
  } else {
    loyaltyApplicable = true;
    // Provisional peer slots so greenfield preflight never hides BPP peer cost.
    peerLoyaltyHotelsPending = 3;
  }

  const cost = estimateMonitoringCost({
    reusableCoreObservations: reusableCore,
    reusableLoyaltyObservations: reusableLoyalty,
    loyaltyApplicable,
    peerLoyaltyHotelsPending,
  });
  const bppRequiredContract =
    propertyId != null
      ? buildMandatoryBppPremonitoringCostContract(propertyId)
      : null;
  return {
    ...cost,
    bppMeasurementPlan: bppPlan
      ? {
          state: bppPlan.state,
          lens: bppPlan.lens,
          subjectState: bppPlan.subject?.state || null,
          peerMissing: bppPlan.peers?.missingCorpora || [],
          estimatedPeerLoyaltyUsd:
            bppPlan.cost?.estimatedPeerLoyaltyUsd ?? cost.estimatedPeerLoyaltyUsd,
          founderApprovalsRequired: bppPlan.founderApprovalsRequired || [],
          completeBppRequired: bppRequiredContract
            ? {
                contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
                bppRequired: bppRequiredContract.bppRequired,
                state: bppRequiredContract.state,
                hardMaxSuggestedUsd:
                  bppRequiredContract.researchBudget?.hardMaxSuggestedUsd ?? null,
                blocksUnqualifiedFullE2eComplete:
                  bppRequiredContract.blocksUnqualifiedFullE2eComplete,
              }
            : null,
        }
      : {
          state: "BPP_PEER_COHORT_PENDING",
          peerLoyaltyHotelsPending,
          estimatedPeerLoyaltyUsd: cost.estimatedPeerLoyaltyUsd,
          note: "Greenfield preflight includes provisional peer Loyalty slots until cohort certified",
        },
  };
}

/**
 * Build HPC live-check query from orchestrator identity + optional profile/candidate.
 */
export function buildHpcLiveQueryFromIdentity(identity, hotelQuery) {
  const propertyId = identity?.adpPropertyId || identity?.draftPropertyId || null;
  const profile = propertyId ? loadPropertyProfile(propertyId) : null;
  const proposed = identity?.proposedIdentity || null;
  const hotelName =
    identity?.propertyName ||
    proposed?.officialName ||
    profile?.name ||
    hotelQuery ||
    "";
  const aliases = [
    ...(profile?.identityAliases || []),
    ...(proposed?.identityAliases || []),
    ...(identity?.candidates || []).map((c) => c.hotelName).filter(Boolean),
  ];
  return {
    hotelName,
    aliases: [...new Set(aliases.filter(Boolean))],
    city: profile?.city || proposed?.city || null,
    country: profile?.country || proposed?.country || null,
    address: profile?.address || proposed?.addressOfficial || null,
    brand: profile?.brand || proposed?.brand || null,
    officialUrl: profile?.officialUrl || proposed?.officialUrl || null,
    adpPropertyId: propertyId,
    knownHpcId:
      identity?.hpcId ||
      proposed?.knownHpcId ||
      profile?.hpcHotelId ||
      profile?.censusRecordId ||
      (propertyId ? getCensusRecordIdForAdpProperty(propertyId) : null) ||
      null,
  };
}

/**
 * S2 — live read-only HPC identity / collision check (automatic on every Full ADP E2E).
 * Never writes. Never treats UNAVAILABLE as verified absence.
 */
export async function ensureS2HpcLiveCheck(manifest, identity, hotelQuery) {
  const query = buildHpcLiveQueryFromIdentity(identity, hotelQuery);
  const fingerprint = hpcQueryFingerprint(query);
  const prior = manifest?.stageResults?.S2_HPC_ADP_PERSISTENCE?.detail?.hpcLive || null;

  if (mayReusePriorHpcLiveResult(prior, fingerprint)) {
    const reused = {
      ...prior,
      reusedFromPriorRun: true,
      queryFingerprint: fingerprint,
    };
    manifest.stageResults.S2_HPC_ADP_PERSISTENCE = stageResult(
      "S2_HPC_ADP_PERSISTENCE",
      reused.outcome,
      {
        hpcLive: reused,
        writeBlocked: true,
        contract: HPC_LIVE_IDENTITY_COLLISION_V1,
        reused: true,
      }
    );
    if (!manifest.completedStages.includes("S2_HPC_ADP_PERSISTENCE")) {
      manifest.completedStages.push("S2_HPC_ADP_PERSISTENCE");
    }
    return reused;
  }

  const live = await performLiveHpcIdentityCollisionCheck(query);
  live.queryFingerprint = fingerprint;
  live.reusedFromPriorRun = false;

  manifest.stageResults.S2_HPC_ADP_PERSISTENCE = stageResult(
    "S2_HPC_ADP_PERSISTENCE",
    live.outcome,
    {
      hpcLive: live,
      hpcRecordId: live.hpcRecordId || null,
      writeBlocked: true,
      createProposal: live.createProposal || null,
      paidMonitoringBlocked: live.paidMonitoringBlocked !== false,
      contract: HPC_LIVE_IDENTITY_COLLISION_V1,
    }
  );
  if (!manifest.completedStages.includes("S2_HPC_ADP_PERSISTENCE")) {
    manifest.completedStages.push("S2_HPC_ADP_PERSISTENCE");
  }

  // Surface required founder actions
  if (live.outcome === HPC_LIVE_OUTCOME.HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT) {
    manifest.approvalsRequired = manifest.approvalsRequired || [];
    if (!manifest.approvalsRequired.some((a) => a.type === "HPC_CREATE_AUTHORIZATION")) {
      manifest.approvalsRequired.push({
        type: "HPC_CREATE_AUTHORIZATION",
        outcome: live.outcome,
        createProposal: live.createProposal,
        message:
          "Live HPC search verified absence. Founder must authorize exact new-record fields before create; no auto-write.",
      });
    }
  } else if (live.outcome === HPC_LIVE_OUTCOME.HPC_IDENTITY_COLLISION_REQUIRES_REVIEW) {
    manifest.approvalsRequired = manifest.approvalsRequired || [];
    if (!manifest.approvalsRequired.some((a) => a.type === "HPC_IDENTITY_COLLISION_REVIEW")) {
      manifest.approvalsRequired.push({
        type: "HPC_IDENTITY_COLLISION_REVIEW",
        outcome: live.outcome,
        candidates: (live.candidates || []).slice(0, 8),
        message: "Ambiguous HPC matches — founder must select exact record before continuing.",
      });
    }
  } else if (live.outcome === HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE) {
    manifest.blockers = manifest.blockers || [];
    if (!manifest.blockers.includes("HPC_LIVE_LOOKUP_UNAVAILABLE")) {
      manifest.blockers.push("HPC_LIVE_LOOKUP_UNAVAILABLE");
    }
    manifest.approvalsRequired = manifest.approvalsRequired || [];
    if (!manifest.approvalsRequired.some((a) => a.type === "HPC_LIVE_LOOKUP_RESTORE")) {
      manifest.approvalsRequired.push({
        type: "HPC_LIVE_LOOKUP_RESTORE",
        outcome: live.outcome,
        message:
          "Airtable HPC live lookup unavailable. Do not treat as absence. Restore credentials/network and re-run preflight.",
      });
    }
  } else if (live.outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED) {
    // Attach verified HPC id onto identity for downstream stages (no write)
    if (manifest.identity && live.hpcRecordId) {
      manifest.identity = {
        ...manifest.identity,
        hpcId: live.hpcRecordId,
        hpcLiveVerified: true,
        hpcLiveOutcome: live.outcome,
      };
    }
  }

  return live;
}

export function hpcLiveAllowsPaidMonitoring(hpcLive) {
  return (
    hpcLive?.outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED &&
    Boolean(hpcLive?.hpcRecordId) &&
    hpcLive?.paidMonitoringBlocked === false
  );
}

/**
 * Exact-authorization HPC create. Never implies paid monitoring or publication.
 * Default dryRun=true unless apply && !hpcCreateDryRun && auth present.
 */
export async function maybeExecuteAuthorizedHpcCreate({
  manifest,
  identity,
  runId,
  authorizeHpcCreate = false,
  hpcCreateAuthorizationRef = null,
  hpcCreateDryRun = true,
  apply = false,
} = {}) {
  if (!authorizeHpcCreate) {
    return {
      ok: false,
      error: "UNAUTHORIZED_CREATE_REJECTED",
      state: HPC_CREATE_REUSE_STATE.UNAUTHORIZED_CREATE_REJECTED,
      airtableWrites: 0,
    };
  }

  const draftPropertyId =
    identity?.draftPropertyId ||
    identity?.adpPropertyId ||
    identity?.proposedIdentity?.draftPropertyId ||
    null;

  const auth = hpcCreateAuthorizationRef
    ? loadHpcCreateAuthorization({ authorizationId: hpcCreateAuthorizationRef }) ||
      loadHpcCreateAuthorization({ runId: hpcCreateAuthorizationRef, draftPropertyId })
    : loadHpcCreateAuthorization({ runId, draftPropertyId });

  if (!auth) {
    // Attach prepared proposal for founder (Chapultepec known proposal)
    let proposal = null;
    if (
      draftPropertyId === "adp_grand_fiesta_americana_chapultepec" ||
      /chapultepec/i.test(identity?.propertyName || identity?.hotelQuery || "")
    ) {
      proposal = await prepareValidatedCreateProposal(buildChapultepecHpcCreateProposal());
    }
    return {
      ok: false,
      error: "UNAUTHORIZED_CREATE_REJECTED",
      state: HPC_CREATE_REUSE_STATE.UNAUTHORIZED_CREATE_REJECTED,
      message:
        "Record exact HPC create authorization (fieldsHash + approver + runId) before --authorize-hpc-create --apply.",
      proposal,
      airtableWrites: 0,
      paidMonitoringImplied: false,
    };
  }

  const doApply = Boolean(apply) && hpcCreateDryRun === false;
  const result = await executeAuthorizedHpcCreate({
    runId,
    authorization: auth,
    dryRun: !doApply,
    apply: doApply,
    simulation: false,
  });

  if (result.ok && result.hpcRecordId && manifest?.identity) {
    manifest.identity = {
      ...manifest.identity,
      hpcId: result.hpcRecordId,
      hpcLiveVerified: result.s2Outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED,
      hpcLiveOutcome: result.s2Outcome || null,
      hpcCreateState: result.state,
    };
  }

  return result;
}

function assessAttributes(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { ok: false, error: "PROFILE_MISSING" };
  const attrs = (profile.attributes || []).filter((a) => ATTRIBUTE_DEFINITIONS[a]);
  const cov = coverageScoreForProfile(profile, attrs);
  const resolved = resolveCanonicalAttributeSet(attrs, {
    labelFor: (id) => ATTRIBUTE_DEFINITIONS[id]?.label || id,
  });
  const gate1 = runDedupGate1PreMonitoring(propertyId, { profile });
  return {
    ok: cov.metMinimum === true && gate1.ok,
    coverage: cov,
    verifiedPresent: attrs.length,
    canonicalCount: resolved.canonicalCount,
    collapsedAliases: resolved.collapsed || resolved.aliasGroups || null,
    dedupGate1: gate1,
    dedupGateContract: ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
  };
}

function replayCalculations(propertyId) {
  const staged = loadStagedOwnerPayload(propertyId);
  const live = independentlyRecalculateRealityGapV1V2(propertyId);
  const uniqueness = assessRealityGapUniquenessFromPayload(
    staged?.payload?.realityGap,
    propertyId
  );
  const gate2 = runDedupGate2PreCalculation(propertyId, { staged, liveRecalc: live });
  const gate3 = runDedupGate3PreCustomerRelease(propertyId, { staged });
  const meth = staged?.payload?.realityGap?.methodologyId;
  return {
    ok:
      Boolean(staged?.ok || staged?.payload) &&
      live.ok &&
      uniqueness.ok &&
      gate2.ok &&
      gate3.ok &&
      meth === CONDITIONAL_APPROVED_METHODOLOGY_ID,
    stagedVersionId: staged?.versionId || null,
    gapScoreV2: staged?.payload?.realityGap?.gapScore ?? null,
    liveGapScoreV2: live.ok ? live.gapScoreV2 : null,
    methodologyId: meth || null,
    uniquenessOk: uniqueness.ok,
    uniquenessIssues: uniqueness.issues,
    dedupGate2: gate2,
    dedupGate3: gate3,
    formula: "RG_WEIGHTED_PARTIAL_V2",
    parity: live.ok && Number(staged?.payload?.realityGap?.gapScore) === Number(live.gapScoreV2),
  };
}

/**
 * Core orchestrator entry.
 */
export async function runAutonomousHotelFullE2e(opts = {}) {
  const {
    hotel = "",
    mode = ORCHESTRATOR_MODE.PREFLIGHT,
    apply = false,
    authorizeMeasurement = false,
    authorizePublish = false,
    authorizationRef = null,
    budgetUsd = null,
    resumeRunId = null,
    stopAfterStage = null,
    allowPaid = false,
    authorizeHpcCreate = false,
    hpcCreateAuthorizationRef = null,
    hpcCreateDryRun = true,
  } = opts;

  const parsed = parseFullAdpE2eInstruction(hotel);
  const modeNorm = String(mode || ORCHESTRATOR_MODE.PREFLIGHT).toLowerCase();
  if (!Object.values(ORCHESTRATOR_MODE).includes(modeNorm)) {
    return { ok: false, error: "INVALID_MODE", mode };
  }

  let manifest = resumeRunId ? loadRunManifest(resumeRunId) : null;
  if (modeNorm === ORCHESTRATOR_MODE.RESUME && !manifest) {
    return { ok: false, error: "RESUME_MANIFEST_MISSING", resumeRunId };
  }

  const identity = resolveHotelIdentityForOrchestrator(
    manifest?.hotelQuery || parsed.hotelQuery
  );
  const lockKey = identity.adpPropertyId || `new_${normName(parsed.hotelQuery).replace(/\s+/g, "_")}`;
  const runId =
    manifest?.runId ||
    newRunId(identity.slug || identity.adpPropertyId || lockKey);

  const lock = acquireHotelRunLock(lockKey, runId);
  if (!lock.ok) {
    return { ok: false, error: lock.error, lock: lock.existing, runId };
  }

  try {
    if (!manifest) {
      manifest = {
        schema: ADP_AUTONOMOUS_HOTEL_FULL_E2E_V1,
        orchestratorVersion: ORCHESTRATOR_VERSION,
        runId,
        createdAt: new Date().toISOString(),
        hotelQuery: parsed.hotelQuery,
        instruction: parsed.instruction,
        mode: modeNorm,
        state: RUN_STATE.PREFLIGHT,
        apply: Boolean(apply),
        allowPaid: Boolean(allowPaid && authorizeMeasurement),
        authorizeMeasurement: Boolean(authorizeMeasurement),
        authorizePublish: Boolean(authorizePublish),
        authorizationRef,
        budgetUsd: budgetUsd != null ? Number(budgetUsd) : null,
        frozenBaselineId: ADP_CERTIFIED_31_HOTEL_BASELINE_ID,
        publicationStatus: "NOT_AUTHORIZED",
        completedStages: [],
        stageResults: {},
        costs: { estimated: null, actual: 0, chargedCalls: 0 },
        retries: {},
        blockers: [],
        approvalsRequired: [],
      };
    }

    manifest.updatedAt = new Date().toISOString();
    manifest.mode = modeNorm;
    manifest.identity = identity;
    // Resume/full may refresh founder-authorized flags without minting a new run
    if (budgetUsd != null) manifest.budgetUsd = Number(budgetUsd);
    if (authorizationRef) manifest.authorizationRef = authorizationRef;
    if (authorizeMeasurement) manifest.authorizeMeasurement = true;
    if (allowPaid && authorizeMeasurement) manifest.allowPaid = true;
    if (apply) manifest.apply = true;

    // ---- S0 Identity ----
    const s0 = PIPELINE_STAGES[0];
    if (!identity.ok) {
      manifest.state = identity.state || RUN_STATE.IDENTITY_APPROVAL_REQUIRED;
      manifest.stageResults[s0.id] = stageResult(s0.id, "BLOCKED", {
        error: identity.error,
        candidates: identity.candidates,
        greenfield: identity.greenfield || false,
      });
      manifest.approvalsRequired.push({
        type: "IDENTITY_APPROVAL",
        message: identity.message,
        candidates: identity.candidates,
      });
      if (modeNorm === ORCHESTRATOR_MODE.PREFLIGHT || identity.greenfield) {
        const costPlan = buildResearchCostPlan(identity);
        manifest.costs.estimated = costPlan;
        // S2 live HPC check is mandatory even for greenfield / unresolved identity
        const hpcLive = await ensureS2HpcLiveCheck(
          manifest,
          identity,
          parsed.hotelQuery || manifest.hotelQuery
        );
        manifest.stageResults.S1_LIVE_PROPERTY_RESEARCH = stageResult(
          "S1_LIVE_PROPERTY_RESEARCH",
          identity.proposedIdentity || identity.draftPropertyId
            ? "GREENFIELD_OFFICIAL_SOURCES_STAGED"
            : "PLAN_REQUIRED",
          {
            draftPropertyId: identity.draftPropertyId || null,
            proposedIdentity: Boolean(identity.proposedIdentity),
          }
        );
        manifest.stageResults.S5_COST_AUTHORIZATION = stageResult(
          "S5_COST_AUTHORIZATION",
          "FORECAST_ONLY",
          { costPlan, paidCallsBlocked: true }
        );
        if (hpcLive?.outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED) {
          manifest.state = RUN_STATE.AIRTABLE_VERIFIED;
        }
        saveRunManifest(manifest);
        releaseHotelRunLock(lockKey, runId);
        return {
          ok: true,
          awaiting: RUN_STATE.IDENTITY_APPROVAL_REQUIRED,
          runId,
          mode: modeNorm,
          identity: manifest.identity || identity,
          costPlan,
          hpcLive: {
            outcome: hpcLive.outcome,
            hpcRecordId: hpcLive.hpcRecordId || null,
            createProposal: hpcLive.createProposal || null,
            candidates: (hpcLive.candidates || []).slice(0, 5),
            paidMonitoringBlocked: hpcLive.paidMonitoringBlocked !== false,
          },
          paidCalls: 0,
          publication: "NOT_AUTHORIZED",
          airtableWrites: 0,
          manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
          stagesAutomated: PIPELINE_STAGES.filter((s) => !s.paid && !s.approvalRequired).map(
            (s) => s.id
          ),
          stagesRequiringApproval: PIPELINE_STAGES.filter((s) => s.approvalRequired).map(
            (s) => s.id
          ),
          note: "Preflight complete for unresolved/new hotel — live HPC read-only check executed. No paid calls, no HPC writes, no publication.",
        };
      }
      saveRunManifest(manifest);
      releaseHotelRunLock(lockKey, runId);
      return {
        ok: false,
        error: identity.error,
        state: manifest.state,
        runId,
        identity,
        manifest,
      };
    }

    manifest.stageResults[s0.id] = stageResult(s0.id, "PASS", {
      adpPropertyId: identity.adpPropertyId,
      hpcId: identity.hpcId,
      matchMethod: identity.matchMethod,
    });
    if (!manifest.completedStages.includes(s0.id)) manifest.completedStages.push(s0.id);
    manifest.state = RUN_STATE.IDENTITY_VERIFIED;
    const propertyId = identity.adpPropertyId;

    // Cost plan always
    const costPlan = buildResearchCostPlan(identity);
    manifest.costs.estimated = costPlan;

    // S2 live HPC identity/collision — mandatory for every hotel / every Full ADP E2E
    let hpcLive = await ensureS2HpcLiveCheck(
      manifest,
      identity,
      parsed.hotelQuery || manifest.hotelQuery
    );

    // Authorized HPC create (exact field auth) — does NOT authorize paid monitoring
    if (
      authorizeHpcCreate &&
      hpcLive?.outcome === HPC_LIVE_OUTCOME.HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT
    ) {
      const createResult = await maybeExecuteAuthorizedHpcCreate({
        manifest,
        identity,
        runId,
        authorizeHpcCreate,
        hpcCreateAuthorizationRef,
        hpcCreateDryRun,
        apply,
      });
      manifest.stageResults.S2_HPC_CREATE = stageResult(
        "S2_HPC_CREATE",
        createResult.state || createResult.error || "UNKNOWN",
        {
          ...createResult,
          paidMonitoringImplied: false,
          publicationImplied: false,
          contract: HPC_AUTHORIZED_CREATE_REUSE_V1,
        }
      );
      if (createResult.ok && createResult.s2Outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED) {
        hpcLive = await ensureS2HpcLiveCheck(
          manifest,
          {
            ...identity,
            hpcId: createResult.hpcRecordId,
          },
          parsed.hotelQuery || manifest.hotelQuery
        );
        // Force fresh check after create (bypass stale fingerprint reuse of ABSENT)
        if (hpcLive.outcome !== HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED && createResult.hpcRecordId) {
          const forced = await performLiveHpcIdentityCollisionCheck({
            ...buildHpcLiveQueryFromIdentity(identity, parsed.hotelQuery),
            knownHpcId: createResult.hpcRecordId,
          });
          hpcLive = forced;
          manifest.stageResults.S2_HPC_ADP_PERSISTENCE = stageResult(
            "S2_HPC_ADP_PERSISTENCE",
            forced.outcome,
            {
              hpcLive: forced,
              hpcRecordId: forced.hpcRecordId,
              writeBlocked: true,
              afterAuthorizedCreate: true,
              contract: HPC_LIVE_IDENTITY_COLLISION_V1,
            }
          );
        }
      }
      if (!createResult.ok && !createResult.dryRun) {
        saveRunManifest(manifest);
        releaseHotelRunLock(lockKey, runId);
        return {
          ok: false,
          error: createResult.error || "HPC_CREATE_FAILED",
          runId,
          hpcCreate: createResult,
          paidCalls: 0,
          airtableWrites: createResult.airtableWrites || 0,
          publication: "NOT_AUTHORIZED",
          manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
        };
      }
    }

    // Mode-specific execution paths
    if (modeNorm === ORCHESTRATOR_MODE.PREFLIGHT) {
      return finishPreflight(manifest, identity, costPlan, runId, lockKey, hpcLive);
    }

    if (modeNorm === ORCHESTRATOR_MODE.REPLAY) {
      return finishReplay(manifest, identity, propertyId, runId, lockKey);
    }

    if (modeNorm === ORCHESTRATOR_MODE.VERIFY || modeNorm === ORCHESTRATOR_MODE.AUDIT) {
      return finishVerifyAudit(manifest, identity, propertyId, runId, lockKey, modeNorm);
    }

    if (modeNorm === ORCHESTRATOR_MODE.FULL || modeNorm === ORCHESTRATOR_MODE.RESUME) {
      return await finishFullOrResume(manifest, identity, propertyId, costPlan, runId, lockKey, {
        apply,
        authorizeMeasurement,
        authorizePublish,
        allowPaid,
        stopAfterStage,
        hpcLive,
      });
    }

    saveRunManifest(manifest);
    return { ok: false, error: "UNHANDLED_MODE", mode: modeNorm, runId };
  } finally {
    // Keep lock for blocked/awaiting runs so resume is exclusive; release on terminal complete
  }
}

function finishPreflight(manifest, identity, costPlan, runId, lockKey, hpcLive = null) {
  const propertyId = identity.adpPropertyId;
  const profile = loadPropertyProfile(propertyId);
  const attrs = propertyId ? assessAttributes(propertyId) : null;
  const peer = propertyId ? getBrandPortfolioPeerSet(propertyId) : null;
  const bppPlanPreflight = propertyId ? planFullE2eBppMeasurement({ propertyId }) : null;
  const bppNaPreflight =
    bppPlanPreflight?.bppApplies === false ||
    bppPlanPreflight?.state === "BPP_NOT_APPLICABLE";
  const staged = propertyId ? loadStagedOwnerPayload(propertyId) : null;
  const core = propertyId ? loadLatestCoreCustomerPeriod(propertyId) : null;
  const live = hpcLive || manifest.stageResults?.S2_HPC_ADP_PERSISTENCE?.detail?.hpcLive || null;

  manifest.stageResults.S1_LIVE_PROPERTY_RESEARCH = stageResult(
    "S1_LIVE_PROPERTY_RESEARCH",
    propertyId && profile ? "EXISTING_PROFILE_DISCOVERED" : "PLAN_REQUIRED",
    { hasProfile: Boolean(profile), attributeCount: profile?.attributes?.length || 0 }
  );
  // S2 already populated by ensureS2HpcLiveCheck — do not overwrite with stale placeholder
  if (!manifest.stageResults.S2_HPC_ADP_PERSISTENCE?.detail?.hpcLive) {
    manifest.stageResults.S2_HPC_ADP_PERSISTENCE = stageResult(
      "S2_HPC_ADP_PERSISTENCE",
      HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE,
      {
        hpcId: identity.hpcId || null,
        writeBlocked: true,
        error: "S2_LIVE_CHECK_NOT_EXECUTED",
        note: "Unavailable — never interpret as verified absence",
      }
    );
  }
  manifest.stageResults.S3_ATTRIBUTE_INTELLIGENCE = stageResult(
    "S3_ATTRIBUTE_INTELLIGENCE",
    attrs?.ok ? "PASS_EXISTING" : "GAP",
    attrs || {}
  );
  manifest.stageResults.S4_PEER_GOVERNANCE = stageResult(
    "S4_PEER_GOVERNANCE",
    peer
      ? "PEER_SET_PRESENT"
      : bppNaPreflight
        ? "BPP_NOT_APPLICABLE"
        : "PEER_PROPOSAL_REQUIRED",
    {
      peerSetId: peer?.peerSetId || peer?.id || null,
      approvalRequired: !peer && !bppNaPreflight,
      bppApplies: bppPlanPreflight?.bppApplies ?? null,
      bppState: bppPlanPreflight?.state || null,
    }
  );
  const paidBlockedByHpc = !hpcLiveAllowsPaidMonitoring(live);
  manifest.stageResults.S5_COST_AUTHORIZATION = stageResult(
    "S5_COST_AUTHORIZATION",
    paidBlockedByHpc ? "BLOCKED_PENDING_HPC_IDENTITY" : "AWAITING_APPROVAL",
    {
      costPlan,
      paidCallsBlocked: true,
      hpcOutcome: live?.outcome || null,
      hpcRecordId: live?.hpcRecordId || null,
    }
  );
  manifest.state =
    live?.outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED
      ? RUN_STATE.AWAITING_PAID_RESEARCH_APPROVAL
      : live?.outcome === HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE
        ? RUN_STATE.BLOCKED
        : RUN_STATE.AWAITING_PAID_RESEARCH_APPROVAL;
  manifest.approvalsRequired = manifest.approvalsRequired || [];
  if (!manifest.approvalsRequired.some((a) => a.type === "PAID_RESEARCH_BUDGET")) {
    manifest.approvalsRequired.push({
      type: "PAID_RESEARCH_BUDGET",
      estimatedTotalUsd: costPlan.estimatedTotalUsd,
      hardMaxSuggestedUsd: costPlan.hardMaxSuggestedUsd,
      flags: ["--authorize-measurement", "--budget-usd", "--apply"],
      blockedUntilHpcVerified: paidBlockedByHpc,
    });
  }
  if (!peer && !bppNaPreflight) {
    if (!manifest.approvalsRequired.some((a) => a.type === "PEER_COHORT_MEMBERSHIP")) {
      manifest.approvalsRequired.push({ type: "PEER_COHORT_MEMBERSHIP" });
    }
  }

  saveRunManifest(manifest);
  releaseHotelRunLock(lockKey, runId);

  return {
    ok: true,
    mode: ORCHESTRATOR_MODE.PREFLIGHT,
    runId,
    state: manifest.state,
    identity: manifest.identity || identity,
    costPlan,
    hpcLive: live
      ? {
          outcome: live.outcome,
          hpcRecordId: live.hpcRecordId || null,
          createProposal: live.createProposal || null,
          candidates: (live.candidates || []).slice(0, 5),
          paidMonitoringBlocked: live.paidMonitoringBlocked !== false,
          matchScore: live.matchScore || null,
          matchMethod: live.matchMethod || null,
        }
      : null,
    existing: {
      stagedVersionId: staged?.versionId || null,
      corePeriodId: core?.periodId || null,
      coreObservations: core?.observations?.length || 0,
      attributesOk: attrs?.ok || false,
      peerPresent: Boolean(peer),
      hpcId: live?.hpcRecordId || identity.hpcId || null,
    },
    paidCalls: 0,
    airtableWrites: 0,
    publication: "NOT_AUTHORIZED",
    manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
    next:
      "Review identity + costPlan. To continue paid Full mode: npm run dealality:adp-full-e2e -- --hotel <id> --mode full --apply --authorize-measurement --budget-usd <n> --authorization-ref <ref>",
  };
}

function finishReplay(manifest, identity, propertyId, runId, lockKey) {
  if (!identity.inCertified31) {
    manifest.state = RUN_STATE.BLOCKED;
    manifest.blockers.push("REPLAY_REQUIRES_CERTIFIED_31_HOTEL");
    saveRunManifest(manifest);
    releaseHotelRunLock(lockKey, runId);
    return { ok: false, error: "REPLAY_REQUIRES_CERTIFIED_31_HOTEL", runId, identity };
  }

  const calc = replayCalculations(propertyId);
  const attrs = assessAttributes(propertyId);
  const audit = auditHotelCustomerReleaseContent({ propertyId });
  const staged = loadStagedOwnerPayload(propertyId);

  manifest.stageResults.S3_ATTRIBUTE_INTELLIGENCE = stageResult(
    "S3_ATTRIBUTE_INTELLIGENCE",
    attrs.ok ? "PASS" : "FAIL",
    attrs
  );
  manifest.stageResults.S8_ANALYTICAL_CALCULATIONS = stageResult(
    "S8_ANALYTICAL_CALCULATIONS",
    calc.ok && calc.parity ? "PASS" : "FAIL",
    calc
  );
  manifest.stageResults.S9_CUSTOMER_REPORT = stageResult(
    "S9_CUSTOMER_REPORT",
    staged?.payload ? "PASS_EXISTING_STAGED" : "FAIL",
    { stagedVersionId: staged?.versionId }
  );
  manifest.stageResults.S11_CUSTOMER_RELEASE_AUDIT = stageResult(
    "S11_CUSTOMER_RELEASE_AUDIT",
    audit.p0Count === 0 ? "PASS" : "FAIL",
    {
      verdict: audit.customerReleaseVerdict,
      p0: audit.p0Count,
      p1: audit.p1Count,
    }
  );
  for (const id of [
    "S0_INTAKE_IDENTITY",
    "S3_ATTRIBUTE_INTELLIGENCE",
    "S8_ANALYTICAL_CALCULATIONS",
    "S9_CUSTOMER_REPORT",
    "S11_CUSTOMER_RELEASE_AUDIT",
  ]) {
    if (!manifest.completedStages.includes(id)) manifest.completedStages.push(id);
  }

  const ok = calc.ok && calc.parity && attrs.ok && audit.p0Count === 0;
  manifest.state = ok ? RUN_STATE.COMPLETE : RUN_STATE.BLOCKED;
  manifest.costs.actual = 0;
  manifest.replay = {
    zeroPaid: true,
    isolated: true,
    frozenBaselineId: ADP_CERTIFIED_31_HOTEL_BASELINE_ID,
    note: "Replay recomputes from archives; does not overwrite golden baseline or published snapshots",
  };
  saveRunManifest(manifest);
  releaseHotelRunLock(lockKey, runId);

  return {
    ok,
    mode: ORCHESTRATOR_MODE.REPLAY,
    runId,
    state: manifest.state,
    identity,
    calc,
    attrs,
    customerReleaseVerdict: audit.customerReleaseVerdict,
    paidCalls: 0,
    goldenBaselineUntouched: true,
    manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
  };
}

function finishVerifyAudit(manifest, identity, propertyId, runId, lockKey, modeNorm) {
  const calc = replayCalculations(propertyId);
  const uniqueness = assessRealityGapUniquenessFromPayload(
    loadStagedOwnerPayload(propertyId)?.payload?.realityGap,
    propertyId
  );
  const audit = auditHotelCustomerReleaseContent({ propertyId });
  manifest.stageResults.S11_CUSTOMER_RELEASE_AUDIT = stageResult(
    "S11_CUSTOMER_RELEASE_AUDIT",
    audit.p0Count === 0 && uniqueness.ok ? "PASS" : "FAIL",
    {
      verdict: audit.customerReleaseVerdict,
      p0: audit.p0Count,
      p1: audit.p1Count,
      uniquenessOk: uniqueness.ok,
      calcParity: calc.parity,
    }
  );
  manifest.state =
    audit.customerReleaseVerdict === "CUSTOMER_READY"
      ? RUN_STATE.CUSTOMER_READY
      : audit.customerReleaseVerdict === "CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS"
        ? RUN_STATE.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS
        : RUN_STATE.BLOCKED;
  saveRunManifest(manifest);
  releaseHotelRunLock(lockKey, runId);
  return {
    ok: audit.p0Count === 0 && uniqueness.ok,
    mode: modeNorm,
    runId,
    state: manifest.state,
    identity,
    audit: {
      verdict: audit.customerReleaseVerdict,
      p0: audit.p0Count,
      p1: audit.p1Count,
      p2: audit.p2Count,
    },
    uniqueness,
    calc,
    paidCalls: 0,
    publication: "NOT_AUTHORIZED",
    manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
  };
}

function monitoringAlreadyComplete(manifest, propertyId) {
  // Peer Loyalty supplement: Core/subject may be complete while authorized peers still lack corpora.
  const peerAuth = loadAuthorizationForRun(manifest.runId, propertyId, {
    preferPeerSupplement: true,
  });
  if (
    peerAuth?.authorizationKind === "PEER_LOYALTY_SUPPLEMENT" &&
    peerAuth.peerLoyaltyAuthorized === true &&
    Array.isArray(peerAuth.approvedPeerCanonicalIds)
  ) {
    const missingPeer = peerAuth.approvedPeerCanonicalIds.some((peerId) => {
      const loy = (loadAllPeriods(peerId) || []).find((p) => isLoyaltyMeasurementPeriod(p));
      return (loy?.observations || []).length < 112 * 0.9;
    });
    if (missingPeer) return false;
  }

  const s6 = String(manifest.stageResults?.S6_CORE_MONITORING?.status || "");
  const s7 = String(manifest.stageResults?.S7_LOYALTY_MONITORING?.status || "");
  if (/PASS|REUSED_ARCHIVE/i.test(s6) && /PASS|REUSED|SKIP/i.test(s7)) return true;
  if (manifest.liveMonitoring?.corePeriodId && Number(manifest.costs?.actual || 0) > 0) {
    return true;
  }
  const core = loadLatestCoreCustomerPeriod(propertyId);
  return Boolean(core?.observations?.length >= 200);
}

function artifactsIncomplete(manifest) {
  const s10 =
    manifest.stageResults?.S10_PDF_SHARE || manifest.stageResults?.S10_PDF_AND_SHARE || null;
  return !/PASS/i.test(String(s10?.status || ""));
}

function markCompleted(manifest, stageId) {
  if (!manifest.completedStages.includes(stageId)) {
    manifest.completedStages.push(stageId);
  }
}

/**
 * S8–S12 zero-paid continuation: calculations → staged confirm → PDF/share →
 * browser QA → readiness. Never advances to AWAITING_PUBLICATION_APPROVAL
 * until S10 artifacts and S11 browser/content QA pass.
 */
async function completeZeroPaidS8ThroughS12(manifest, propertyId, { authorizePublish = false } = {}) {
  // When peer Loyalty corpora are complete, independently rank the comparable cohort
  // and merge certified BPP into a new immutable staged version before PDF/share.
  let bppCohortMerge = null;
  try {
    const { mergeComparableCohortBppIntoStagedOwnerReport } = await import(
      "../brand-portfolio/bpp-comparable-loyalty-cohort-rank-v1.js"
    );
    const prem = buildMandatoryBppPremonitoringCostContract(propertyId);
    const stagedProbe = loadStagedOwnerPayload(propertyId);
    const existingRank = (stagedProbe?.payload?.brandPortfolioPosition?.kpis || []).find(
      (k) => k.id === "portfolioRank"
    )?.value;
    const rankCertified = /^#\d+\s+of\s+\d+/i.test(String(existingRank || ""));
    if (prem.plan?.peers?.measuredRankEligible === true && !rankCertified) {
      bppCohortMerge = mergeComparableCohortBppIntoStagedOwnerReport(propertyId);
      if (bppCohortMerge?.ok) {
        manifest.bppRankStatus = "BPP_COMPARISON_CERTIFIED";
        manifest.liveMonitoring = {
          ...(manifest.liveMonitoring || {}),
          bppMeasuredRank: "BPP_COMPARISON_CERTIFIED",
          bppSubjectRank: bppCohortMerge.subjectRank,
          bppCohortSize: bppCohortMerge.cohortSize,
          bppStagedVersionId: bppCohortMerge.stagedVersionId,
        };
      }
    } else if (rankCertified && prem.plan?.peers?.measuredRankEligible === true) {
      bppCohortMerge = { ok: true, reused: true, existingRank };
      manifest.bppRankStatus = "BPP_COMPARISON_CERTIFIED";
    }
  } catch (e) {
    bppCohortMerge = { ok: false, error: e.message };
  }

  const staged = loadStagedOwnerPayload(propertyId);

  // S8a–S8d: autonomous competitor discovery + entity resolution + displacement (zero paid)
  const competitorEr = ensureAutonomousCompetitorRegistry(propertyId, { persist: true });
  const displacementReconcile = reconcileDisplacementFourState(propertyId);
  const silentLoss =
    competitorEr?.silentLoss ||
    assessSilentCompetitorEvidenceLoss(competitorEr?.ledger || { entityResolutionExecuted: false });
  manifest.stageResults.S8B_COMPETITOR_CANDIDATE_DISCOVERY = stageResult(
    "S8B_COMPETITOR_CANDIDATE_DISCOVERY",
    competitorEr?.ledger?.totals?.totalExtractedMentions >= 0 ? "PASS" : "FAIL",
    {
      contract: ADP_AUTONOMOUS_COMPETITOR_ENTITY_RESOLUTION_V1,
      extractedMentions: competitorEr?.ledger?.totals?.totalExtractedMentions ?? null,
      distinctCandidates: competitorEr?.ledger?.totals?.distinctCandidateEntities ?? null,
    }
  );
  manifest.stageResults.S8C_COMPETITOR_ENTITY_RESOLUTION = stageResult(
    "S8C_COMPETITOR_ENTITY_RESOLUTION",
    competitorEr?.ok && !silentLoss.blockCustomerReady ? "PASS" : "FAIL",
    {
      contract: ADP_AUTONOMOUS_COMPETITOR_ENTITY_RESOLUTION_V1,
      registrySource: competitorEr?.registrySource || null,
      certifiedCompetitors: competitorEr?.certifiedCompetitors?.length ?? 0,
      silentLoss,
      ledgerTotals: competitorEr?.ledger?.totals || null,
    }
  );
  markCompleted(manifest, "S8B_COMPETITOR_CANDIDATE_DISCOVERY");
  markCompleted(manifest, "S8C_COMPETITOR_ENTITY_RESOLUTION");
  const s8dExclusiveOk =
    displacementReconcile?.reconciliation?.exclusiveSumEqualsEligible === true ||
    displacementReconcile?.reconciliation?.exclusiveSumEquals63 === true;
  manifest.stageResults.S8D_DISPLACEMENT_CERTIFICATION = stageResult(
    "S8D_DISPLACEMENT_CERTIFICATION",
    s8dExclusiveOk &&
      displacementReconcile?.reconciliation?.displacementMatchesIndependent
      ? "PASS"
      : "FAIL",
    {
      displacementScenarioCount: displacementReconcile?.displacementScenarioCount,
      scenarioExclusive: displacementReconcile?.scenarioExclusiveFourState,
      reconciliation: displacementReconcile?.reconciliation,
    }
  );
  markCompleted(manifest, "S8D_DISPLACEMENT_CERTIFICATION");

  if (silentLoss.blockCustomerReady) {
    manifest.stageResults.S8_ANALYTICAL_CALCULATIONS = stageResult(
      "S8_ANALYTICAL_CALCULATIONS",
      "FAIL_COMPETITOR_EVIDENCE_LOSS",
      { silentLoss, competitorEr: { ok: competitorEr?.ok, registrySource: competitorEr?.registrySource } }
    );
    markCompleted(manifest, "S8_ANALYTICAL_CALCULATIONS");
    manifest.state = RUN_STATE.NOT_CUSTOMER_READY;
    manifest.stageResults.S12_FOUNDER_READINESS = stageResult(
      "S12_FOUNDER_READINESS",
      "NOT_READY",
      {
        readinessVerdict: "NOT_CUSTOMER_READY",
        blocker: "P0_COMPETITOR_EVIDENCE_LOSS",
        silentLoss,
      }
    );
    saveRunManifest(manifest);
    return {
      ok: false,
      error: "P0_COMPETITOR_EVIDENCE_LOSS",
      silentLoss,
      competitorEr,
      paidCalls: 0,
    };
  }

  const calc = replayCalculations(propertyId);
  manifest.stageResults.S8_ANALYTICAL_CALCULATIONS = stageResult(
    "S8_ANALYTICAL_CALCULATIONS",
    calc.ok ? "PASS" : "FAIL",
    { ...calc, competitorEr: { certified: competitorEr?.certifiedCompetitors?.length, registrySource: competitorEr?.registrySource } }
  );
  markCompleted(manifest, "S8_ANALYTICAL_CALCULATIONS");

  // Permanent Full E2E certification — mandatory before customer report staging.
  const latestCore = loadLatestCoreCustomerPeriod(propertyId);
  const permanentGate = enforceMeasurementReconciliationGate({
    propertyId,
    periodId: latestCore?.periodId || staged?.payload?.period?.periodId || null,
    period: latestCore || null,
    packagePayload: staged?.payload || null,
    enforcementPoint: ENFORCEMENT_POINTS.AFTER_MEASUREMENT_CALCULATION,
  });
  Object.assign(
    manifest,
    attachPermanentCertificationToManifest(manifest, permanentGate)
  );
  if (!permanentGate.ok) {
    manifest.state = RUN_STATE.NOT_CUSTOMER_READY;
    manifest.stageResults.S9_CUSTOMER_REPORT = stageResult(
      "S9_CUSTOMER_REPORT",
      "BLOCKED_MEASUREMENT_RECONCILIATION",
      { gate: ADP_PERMANENT_FULL_E2E_CERTIFICATION_GATE_V1 }
    );
    manifest.stageResults.S12_FOUNDER_READINESS = stageResult(
      "S12_FOUNDER_READINESS",
      "NOT_READY",
      {
        readinessVerdict: "NOT_CUSTOMER_READY",
        blocker: "PERMANENT_MEASUREMENT_RECONCILIATION_FAILED",
        failures: permanentGate.failures,
      }
    );
    saveRunManifest(manifest);
    return {
      ok: false,
      error: "PERMANENT_MEASUREMENT_RECONCILIATION_FAILED",
      permanentGate,
      calc,
      paidCalls: 0,
    };
  }
  // Pre-report generation enforcement (same artifact; fail closed if missing).
  const beforeReport = assertFullE2eManifestCertification(manifest);
  if (!beforeReport.ok) {
    return {
      ok: false,
      error: "PERMANENT_CERTIFICATION_ARTIFACT_MISSING",
      beforeReport,
      paidCalls: 0,
    };
  }

  manifest.stageResults.S9_CUSTOMER_REPORT = stageResult(
    "S9_CUSTOMER_REPORT",
    staged?.versionId ? "PASS_STAGED" : "FAIL",
    { stagedVersionId: staged?.versionId || null }
  );
  if (staged?.versionId) markCompleted(manifest, "S9_CUSTOMER_REPORT");

  manifest.state = staged?.versionId ? RUN_STATE.CONTENT_READY : RUN_STATE.NOT_CUSTOMER_READY;
  if (!staged?.versionId) {
    manifest.stageResults.S9B_AIRTABLE_HISTORY_PERSISTENCE = stageResult(
      "S9B_AIRTABLE_HISTORY_PERSISTENCE",
      "BLOCKED_NO_STAGED",
      {}
    );
    manifest.stageResults.S10_PDF_SHARE = stageResult("S10_PDF_SHARE", "BLOCKED_NO_STAGED", {});
    manifest.stageResults.S10_PDF_AND_SHARE = manifest.stageResults.S10_PDF_SHARE;
    manifest.stageResults.S11_CUSTOMER_RELEASE_AUDIT = stageResult(
      "S11_CUSTOMER_RELEASE_AUDIT",
      "FAIL",
      { reason: "STAGED_REPORT_REQUIRED_BEFORE_ARTIFACTS" }
    );
    manifest.stageResults.S12_FOUNDER_READINESS = stageResult(
      "S12_FOUNDER_READINESS",
      "NOT_READY",
      { readinessVerdict: "NOT_CUSTOMER_READY" }
    );
    return {
      ok: false,
      error: "STAGED_REPORT_MISSING",
      calc,
      artifacts: null,
      paidCalls: 0,
    };
  }

  // S9B — governed history persistence (dry-run by default; never silent PASS as complete)
  {
    const corePeriodId =
      manifest.liveMonitoring?.corePeriodId ||
      loadLatestCoreCustomerPeriod(propertyId)?.periodId ||
      null;
    const historySync = await assessAdpAirtableHistorySyncGate({
      propertyId,
      periodId: corePeriodId,
      requireCompleteForCustomerReady: false,
    });
    const stagedPayload = staged?.payload || loadStagedOwnerPayload(propertyId)?.payload;
    const historyShape = stagedOwnerPayloadToHistoryShape(stagedPayload, {
      propertyId,
      periodId: corePeriodId,
    });
    const { enrichHistoricalWriteManifestFromStaged } = await import(
      "../longitudinal/adp-history-field-completeness-v1.js"
    );
    const enrichedHistory = enrichHistoricalWriteManifestFromStaged({
      propertyId,
      periodId: corePeriodId || `missing_period_${propertyId}`,
      stagedPayload,
      stagedVersionId: staged?.versionId || "",
      monitoringDate: new Date().toISOString().slice(0, 10),
      certificationStatus: "STAGED_CERTIFIED",
      synthetic: false,
    });
    const monitoringRec = (enrichedHistory.records || []).find(
      (r) => r.table === "ADP Monitoring Periods"
    );
    const historyPayload = {
      ...(stagedPayload && typeof stagedPayload === "object" ? stagedPayload : {}),
      kpis: historyShape.kpis,
      territories: historyShape.territories,
      providers: historyShape.providers,
      competitive: historyShape.competitive,
      evidence: {
        ...(stagedPayload?.evidence || {}),
        ...historyShape.evidence,
      },
      methodologyId: historyShape.methodologyId,
    };
    const historyDry = await persistAdpHistoricalPeriodV1({
      dryRun: true,
      useMock: true,
      allowProductionWrites: false,
      allowGovernedProductionHistoryBase: true,
      propertyId,
      periodId: corePeriodId || `missing_period_${propertyId}`,
      monitoringDate: new Date().toISOString().slice(0, 10),
      certificationStatus: "STAGED_CERTIFIED",
      customerPayload: historyPayload,
      reportSnapshot: {
        snapshotId: `snap_${propertyId}_${corePeriodId || "unknown"}`,
        propertyId,
        periodId: corePeriodId,
        schema: "ADP_REPORT_SNAPSHOT_V1",
        contentHash: monitoringRec?.fields?.["Content Hash"] || null,
        envelopeHash: monitoringRec?.fields?.["Envelope Hash"] || null,
        certifiedAt: monitoringRec?.fields?.["Certification Timestamp"] || new Date().toISOString(),
        customerPayload: historyPayload,
        disclosures: [
          {
            methodologyId: historyShape.methodologyId,
            enrichment: "ADP_HISTORY_FIELD_COMPLETENESS_V1",
          },
        ],
        versions: {
          measurementContractVersion: historyShape.methodologyId || null,
          publicationCommit: staged?.versionId || null,
        },
      },
      historicalVersion: 1,
      synthetic: false,
    });
    const writeApply =
      process.env.ADP_HISTORY_AIRTABLE_WRITE_APPLY === "true" ||
      process.env.ADP_HISTORY_AIRTABLE_WRITE_APPLY === "1";
    let stageStatus = "AWAITING_HISTORY_WRITE_APPROVAL";
    if (historySync.status === HISTORY_SYNC_STATUS.COMPLETE) {
      stageStatus = "PASS_HISTORY_SYNC_COMPLETE";
    } else if (isAdpHistoryWritesEnabled() && writeApply) {
      stageStatus = "FAIL_WRITE_AUTHORIZED_BUT_INCOMPLETE";
    }
    const ecosystemTables = listFullE2eMeasurementHistoryTables().map((t) =>
      resolveLiveTableName(t)
    );
    const publicationOnly = listPublicationOnlyTables().map((t) => t.tableName);
    manifest.stageResults.S9B_AIRTABLE_HISTORY_PERSISTENCE = stageResult(
      "S9B_AIRTABLE_HISTORY_PERSISTENCE",
      stageStatus,
      {
        gate: ADP_FULL_E2E_AIRTABLE_HISTORY_PERSISTENCE_GATE_V1,
        ecosystemContract: ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1,
        measurementHistoryTables: ecosystemTables,
        publicationOnlyTablesExcluded: publicationOnly,
        historySync,
        dryRunRecordCount: historyDry?.manifest?.recordCount ?? null,
        dryRunCounts: historyDry?.manifest?.counts ?? null,
        writesEnabled: isAdpHistoryWritesEnabled(),
        writeApply,
        note: "Live writes require SUPERSEDING_FOUNDER_WRITE_APPROVAL_MANIFEST + MEASUREMENT_HISTORY auth only — not publication",
      }
    );
    markCompleted(manifest, "S9B_AIRTABLE_HISTORY_PERSISTENCE");
    if (stageStatus === "AWAITING_HISTORY_WRITE_APPROVAL") {
      manifest.airtableHistorySyncStatus = RUN_STATE.AWAITING_HISTORY_WRITE_APPROVAL;
    } else if (stageStatus === "PASS_HISTORY_SYNC_COMPLETE") {
      manifest.airtableHistorySyncStatus = HISTORY_SYNC_STATUS.COMPLETE;
    } else {
      manifest.airtableHistorySyncStatus = HISTORY_SYNC_STATUS.WRITE_AUTHORIZED_READBACK_FAILED;
    }
  }

  manifest.state = RUN_STATE.ARTIFACTS_PENDING;
  const bppStatus =
    manifest.liveMonitoring?.bppMeasuredRank ||
    manifest.bppRankStatus ||
    "SUBJECT_MEASURED_PEER_MEMBERSHIP_APPROVED_PEER_CORPORA_PENDING";

  const artifacts = await completeFullE2eArtifactsAndBrowserQa({
    propertyId,
    stagedVersionId: staged.versionId,
    corePeriodId: manifest.liveMonitoring?.corePeriodId || null,
    loyaltyPeriodId: manifest.liveMonitoring?.loyaltyPeriodId || null,
    bppStatus: { measuredRank: bppStatus },
    runId: manifest.runId,
  });

  manifest.stageResults.S10_PDF_SHARE = stageResult(
    "S10_PDF_SHARE",
    artifacts.s10Status === "PASS" ? "PASS" : "FAIL",
    {
      pdfPath: artifacts.pdf?.path || null,
      pdfSha256: artifacts.pdf?.sha256 || null,
      shareOk: artifacts.share?.ok === true,
      share: {
        ok: artifacts.share?.ok === true,
        candidateId: artifacts.share?.candidateId || null,
        tokenId: artifacts.share?.tokenId || null,
        localTestOnly: true,
      },
      deferred: artifacts.s10Status !== "PASS",
      // Persist generator/render failure for audit — do not leave FAIL with null diagnostics only.
      renderError: artifacts.pdf?.renderError || artifacts.error || null,
      pdfBytes: artifacts.pdf?.bytes ?? null,
      pdfContentChecks: artifacts.pdf
        ? {
            hasHotel: artifacts.pdf.hasHotel ?? null,
            hasCover: artifacts.pdf.hasCover ?? null,
            hasPortfolioMonthlyCover: artifacts.pdf.hasPortfolioMonthlyCover ?? null,
            isDashboardPrint: artifacts.pdf.isDashboardPrint ?? null,
            authBlocked: artifacts.pdf.authBlocked ?? null,
          }
        : null,
    }
  );
  // Alias for prior Chapultepec manifests that wrote S10_PDF_AND_SHARE
  manifest.stageResults.S10_PDF_AND_SHARE = manifest.stageResults.S10_PDF_SHARE;
  if (artifacts.s10Status === "PASS") {
    markCompleted(manifest, "S10_PDF_SHARE");
    markCompleted(manifest, "S10_PDF_AND_SHARE");
    manifest.state = RUN_STATE.ARTIFACTS_VERIFIED;
  } else {
    manifest.state = RUN_STATE.ARTIFACTS_PENDING;
  }

  if (artifacts.s10Status === "PASS") {
    manifest.state = RUN_STATE.BROWSER_QA_PENDING;
  }

  manifest.stageResults.S11_CUSTOMER_RELEASE_AUDIT = stageResult(
    "S11_CUSTOMER_RELEASE_AUDIT",
    artifacts.s11Status === "PASS" ? "PASS" : "FAIL",
    {
      verdict: artifacts.contentAudit?.verdict,
      readinessVerdict: artifacts.readinessVerdict,
      p0: artifacts.contentAudit?.p0,
      p1: artifacts.contentAudit?.p1,
      browserQaOk: artifacts.browserQa?.ok === true,
      memberstackAuth: artifacts.memberstackAuth,
      uniquenessOk: artifacts.uniqueness?.ok !== false,
      invariantOk: artifacts.invariant?.ok !== false,
    }
  );
  if (artifacts.s11Status === "PASS") {
    markCompleted(manifest, "S11_CUSTOMER_RELEASE_AUDIT");
    manifest.state = RUN_STATE.CUSTOMER_QA_COMPLETE;
  }

  const readiness = artifacts.readinessVerdict || RUN_STATE.NOT_CUSTOMER_READY;
  const bppRequiredGate = assessFullE2eCompleteBppRequired({
    propertyId,
    coreAdpCertified: Boolean(staged?.versionId),
    disclosedLimitationsAccepted: false,
  });
  // Permanent delivery parity: certified BPP must remain paintably ready at staged/API layers.
  let bppDeliveryParity = { ok: true, skipped: true };
  try {
    const { reconcileBppKpiLayers, ADP_BPP_SOURCE_API_DOM_PARITY_V1 } = await import(
      "../certification/adp-bpp-source-api-dom-parity-v1.js"
    );
    const stagedBpp = staged?.payload?.brandPortfolioPosition || null;
    bppDeliveryParity = {
      ...reconcileBppKpiLayers({
        stagedBpp,
        apiBpp: stagedBpp,
        // DOM assertions are performed by artifact/browser QA; here enforce
        // that a certified staged pack is not structurally unrenderable.
        domKpiValues: null,
        domHasAwaitingMessage: null,
      }),
      contract: ADP_BPP_SOURCE_API_DOM_PARITY_V1,
      skipped: false,
    };
  } catch (e) {
    bppDeliveryParity = { ok: false, error: e.message, skipped: false };
  }
  const bppDeliveryBlocked = bppDeliveryParity.ok === false && bppDeliveryParity.skipped !== true;
  manifest.stageResults.S8E_BPP_REQUIRED_COMPLETENESS = stageResult(
    "S8E_BPP_REQUIRED_COMPLETENESS",
    bppRequiredGate.blocksUnqualifiedFullE2eComplete || bppDeliveryBlocked
      ? "BLOCKED_PEER_RESEARCH_OR_DELIVERY_PARITY"
      : "PASS",
    {
      contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
      completionTier: bppRequiredGate.completionTier,
      state: bppRequiredGate.prem?.state,
      blocksUnqualifiedFullE2eComplete: bppRequiredGate.blocksUnqualifiedFullE2eComplete,
      failures: bppRequiredGate.failures,
      bppDeliveryParity,
    }
  );
  if (bppRequiredGate.blocksUnqualifiedFullE2eComplete) {
    if (!manifest.blockers.includes("BPP_REQUIRED_PEER_RESEARCH_PENDING")) {
      manifest.blockers.push("BPP_REQUIRED_PEER_RESEARCH_PENDING");
    }
  }
  if (bppDeliveryBlocked) {
    if (!manifest.blockers.includes("BPP_SOURCE_API_DOM_PARITY_FAILED")) {
      manifest.blockers.push("BPP_SOURCE_API_DOM_PARITY_FAILED");
    }
  }

  // S10/S11/S12: AI Admin discoverability is mandatory for customer-delivery certification.
  // Unpublished staged hotels must still appear in AI Admin; never auto-publish.
  const aiAdminRegistration = ensureFullAdpE2eAiAdminRegistration({
    propertyId,
    stagedVersionId: staged.versionId,
    runId: manifest.runId,
    customerDeliveryCertified:
      artifacts.s10Status === "PASS" && artifacts.s11Status === "PASS",
    bppCertified: !bppRequiredGate.blocksUnqualifiedFullE2eComplete && !bppDeliveryBlocked,
  });
  manifest.stageResults.S11B_AI_ADMIN_AUTO_REGISTRATION = stageResult(
    "S11B_AI_ADMIN_AUTO_REGISTRATION",
    aiAdminRegistration.ok ? "PASS" : "FAIL",
    {
      contract: ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
      discoverable: aiAdminRegistration.assessment?.inOperationalRegistry === true,
      published: false,
      publicationStatus: "NOT_AUTHORIZED",
      operationalCount: aiAdminRegistration.registration?.operationalCount || null,
      failures: aiAdminRegistration.assessment?.failures || [],
      blocksCustomerDeliveryCertification:
        aiAdminRegistration.blocksCustomerDeliveryCertification === true,
    }
  );
  if (!aiAdminRegistration.ok) {
    if (!manifest.blockers.includes("AI_ADMIN_DISCOVERABILITY_FAILED")) {
      manifest.blockers.push("AI_ADMIN_DISCOVERABILITY_FAILED");
    }
  }

  const aiAdminParity = ensureFullAdpE2eAiAdminCertificationParity(propertyId, {
    expectedStagedVersionId: staged.versionId,
    actor: "adp_autonomous_hotel_full_e2e",
  });
  manifest.stageResults.S11C_AI_ADMIN_CERTIFICATION_PARITY = stageResult(
    "S11C_AI_ADMIN_CERTIFICATION_PARITY",
    aiAdminParity.ok ? "PASS" : "FAIL",
    {
      contract: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
      stagedVersionId: staged.versionId,
      corePeriodId: aiAdminParity.assessment?.corePeriodId || null,
      archiveReviewId: aiAdminParity.recovery?.reviewId || null,
      coverageReady: aiAdminParity.assessment?.adminReadyWithoutLegacyArchive === true,
      published: false,
      publicationStatus: "NOT_AUTHORIZED",
      memberstackAuthenticatedQa: "AUTHENTICATED_QA_PENDING",
      failures: aiAdminParity.assessment?.failures || [],
      blocksCustomerDeliveryCertification:
        aiAdminParity.blocksCustomerDeliveryCertification === true,
    }
  );
  if (!aiAdminParity.ok) {
    if (!manifest.blockers.includes("AI_ADMIN_CERTIFICATION_PARITY_FAILED")) {
      manifest.blockers.push("AI_ADMIN_CERTIFICATION_PARITY_FAILED");
    }
  } else {
    manifest.blockers = (manifest.blockers || []).filter(
      (b) => b !== "AI_ADMIN_CERTIFICATION_PARITY_FAILED"
    );
  }

  // S11D: Full ADP PDF visual + artifact parity (must precede customer-delivery cert).
  const pdfParityPageDir = path.join(
    ORCHESTRATOR_REPORTS_ROOT,
    "pdf-visual",
    propertyId,
    staged.versionId || "latest"
  );
  const pdfParity = await ensureFullAdpE2ePdfVisualAndArtifactParity({
    propertyId,
    expectedStagedVersionId: staged.versionId,
    pageRenderDir: pdfParityPageDir,
    baseUrl: process.env.VERIFY_BASE || "http://127.0.0.1:8080",
  });
  manifest.stageResults.S11D_FULL_ADP_PDF_VISUAL_AND_ARTIFACT_PARITY = stageResult(
    "S11D_FULL_ADP_PDF_VISUAL_AND_ARTIFACT_PARITY",
    pdfParity.ok ? "PASS" : "FAIL",
    {
      contract: ADP_FULL_E2E_PDF_VISUAL_AND_ARTIFACT_PARITY_V1,
      stagedVersionId: staged.versionId,
      artifactType: pdfParity.artifactType,
      pdfSha256: pdfParity.pdfSha256 || null,
      pages: pdfParity.pages || null,
      pageRenders: (pdfParity.pageRenders || []).length,
      failures: pdfParity.failures || [],
      warnings: pdfParity.warnings || [],
      published: false,
      publicationStatus: "NOT_AUTHORIZED",
      blocksCustomerDeliveryCertification: true,
      founderDesignAcceptance:
        pdfParity.founderDesignAcceptanceStatus ||
        "PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING",
      founderDesignAcceptanceGate:
        pdfParity.founderDesignAcceptanceGate ||
        "ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1",
      note:
        "Technical PDF parity PASS does not imply FOUNDER_DESIGN_ACCEPTED. Customer publication remains blocked until founder design acceptance.",
    }
  );
  if (!pdfParity.ok) {
    if (!manifest.blockers.includes("FULL_ADP_PDF_VISUAL_OR_ARTIFACT_PARITY_FAILED")) {
      manifest.blockers.push("FULL_ADP_PDF_VISUAL_OR_ARTIFACT_PARITY_FAILED");
    }
  } else {
    manifest.blockers = (manifest.blockers || []).filter(
      (b) => b !== "FULL_ADP_PDF_VISUAL_OR_ARTIFACT_PARITY_FAILED"
    );
  }

  // S11E: AI Admin authorized Full ADP PDF delivery (store alone is insufficient).
  const pdfDelivery = await ensureAiAdminAuthorizedPdfDeliveryV1({
    propertyId,
    expectedStagedVersionId: staged.versionId,
    baseUrl: process.env.VERIFY_BASE || "http://127.0.0.1:8080",
    authMode: process.env.DEV_AUTH_BYPASS_EMAIL ? "dev_bypass" : "none",
  });
  manifest.stageResults.S11E_AI_ADMIN_AUTHORIZED_PDF_DELIVERY = stageResult(
    "S11E_AI_ADMIN_AUTHORIZED_PDF_DELIVERY",
    pdfDelivery.ok ? "PASS" : "FAIL",
    {
      contract: ADP_AI_ADMIN_AUTHORIZED_PDF_DELIVERY_V1,
      stagedVersionId: staged.versionId,
      storeSha256: pdfDelivery.storeSha256 || null,
      servedSha256: pdfDelivery.http?.sha256 || null,
      httpStatus: pdfDelivery.http?.status || null,
      authMode: pdfDelivery.authMode,
      authAcceptance: pdfDelivery.authAcceptance || AUTHENTICATED_PDF_QA_PENDING,
      memberstackAuthenticated:
        pdfDelivery.authAcceptance === MEMBERSTACK_AUTHENTICATED_PDF_PASS,
      failures: pdfDelivery.failures || [],
      warnings: pdfDelivery.warnings || [],
      published: false,
      note:
        "DEV_PDF_DELIVERY_PASS is not MEMBERSTACK_AUTHENTICATED_PDF_PASS. Founder design acceptance remains separate.",
    }
  );
  if (!pdfDelivery.ok) {
    if (!manifest.blockers.includes("AI_ADMIN_AUTHORIZED_PDF_DELIVERY_FAILED")) {
      manifest.blockers.push("AI_ADMIN_AUTHORIZED_PDF_DELIVERY_FAILED");
    }
  } else {
    manifest.blockers = (manifest.blockers || []).filter(
      (b) => b !== "AI_ADMIN_AUTHORIZED_PDF_DELIVERY_FAILED"
    );
  }

  const founderPdfDesignAccepted =
    pdfParity.founderDesignAcceptanceStatus === "FOUNDER_DESIGN_ACCEPTED" ||
    pdfParity.founderDesignAcceptance?.status === "FOUNDER_DESIGN_ACCEPTED";
  const mayAwaitPublication =
    artifacts.s10Status === "PASS" &&
    artifacts.s11Status === "PASS" &&
    artifacts.discoverability?.ok === true &&
    aiAdminRegistration.ok === true &&
    aiAdminParity.ok === true &&
    pdfParity.ok === true &&
    pdfDelivery.ok === true &&
    pdfDelivery.authAcceptance === MEMBERSTACK_AUTHENTICATED_PDF_PASS &&
    founderPdfDesignAccepted &&
    !bppRequiredGate.blocksUnqualifiedFullE2eComplete &&
    !bppDeliveryBlocked &&
    (readiness === RUN_STATE.CUSTOMER_READY ||
      readiness === RUN_STATE.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS);

  manifest.stageResults.S12_FOUNDER_READINESS = stageResult(
    "S12_FOUNDER_READINESS",
    mayAwaitPublication ? "AWAITING_FOUNDER_PUBLICATION" : "NOT_READY",
    {
      readinessVerdict: readiness,
      customerReleaseVerdict: artifacts.contentAudit?.verdict,
      spentUsd: manifest.costs?.actual || 0,
      bppMeasuredRank: bppStatus,
      bppRequiredGate: {
        contract: ADP_FULL_E2E_COMPLETE_BPP_REQUIRED_V1,
        completionTier: bppRequiredGate.completionTier,
        blocksUnqualifiedFullE2eComplete: bppRequiredGate.blocksUnqualifiedFullE2eComplete,
      },
      aiAdminRegistration: {
        contract: ADP_FULL_E2E_AI_ADMIN_AUTO_REGISTRATION_V1,
        ok: aiAdminRegistration.ok,
        discoverable: aiAdminRegistration.assessment?.inOperationalRegistry === true,
        published: false,
      },
      fullAdpPdfParity: {
        contract: ADP_FULL_E2E_PDF_VISUAL_AND_ARTIFACT_PARITY_V1,
        ok: pdfParity.ok,
        pdfSha256: pdfParity.pdfSha256 || null,
        failures: pdfParity.failures || [],
      },
      publicationAuthorized: false,
      pdfSha256: pdfParity.pdfSha256 || artifacts.pdf?.sha256 || null,
      stagedVersionId: staged.versionId,
      memberstackAuth: artifacts.memberstackAuth,
      authorizePublishRequested: Boolean(authorizePublish),
    }
  );
  markCompleted(manifest, "S12_FOUNDER_READINESS");

  const externalShareWorkflow = prepareExternalShareWorkflowAfterCertification({
    stagedCertified: Boolean(staged?.versionId) && mayAwaitPublication,
    founderPdfDesignAccepted,
  });
  manifest.externalShareLifecycle = {
    contract: ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1,
    state: resolveExternalShareLifecycleState({
      stagedCertified: Boolean(staged?.versionId) && (mayAwaitPublication || founderPdfDesignAccepted),
      founderApprovedForPublication: Boolean(authorizePublish),
      productionPublished: false,
      externalShareIssued: false,
      externalShareVerified: false,
      externalShareRegistered: false,
    }),
    prepared: externalShareWorkflow,
    executedPublication: false,
    executedExternalShareIssue: false,
    note:
      "Workflow prepared only. Publication, deploy, and live share mint require separate founder authorization.",
  };

  // Canonical hotel lifecycle + R0 production-release preflight (prepare only).
  const lifecycleFacts = {
    ...factsFromOrchestratorManifest(manifest),
    founderReviewReady: mayAwaitPublication,
    executivePdfCertified: Boolean(pdfParity.ok && founderPdfDesignAccepted),
    aiAdminRegistered: aiAdminRegistration.ok === true,
    customerReportStaged: Boolean(staged?.versionId),
  };
  const lifecycleAuth = {
    paidResearch: Boolean(manifest.authorizeMeasurement || manifest.allowPaid),
    productionPublication: Boolean(authorizePublish),
    externalClientShare: false,
  };
  const hotelLifecycleState = resolveHotelLifecycleState({
    ...lifecycleFacts,
    authorizations: lifecycleAuth,
  });
  try {
    saveHotelLifecycleState(propertyId, {
      facts: lifecycleFacts,
      authorizations: lifecycleAuth,
      certifications: {
        analyticsCertified: lifecycleFacts.analyticsCertified,
        customerReportStaged: lifecycleFacts.customerReportStaged,
        executivePdfCertified: lifecycleFacts.executivePdfCertified,
        aiAdminRegistered: lifecycleFacts.aiAdminRegistered,
      },
      versionPins: {
        stagedVersionId: staged?.versionId || null,
        pdfSha256: pdfParity.pdfSha256 || artifacts.pdf?.sha256 || null,
        pdfTemplateId: CANONICAL_PDF_TEMPLATE_ID,
      },
      note: "orchestrator_s12_boundary",
    });
  } catch (err) {
    console.warn(
      "[adp-full-e2e] lifecycle state persist skipped:",
      err?.message || err
    );
  }
  manifest.hotelLifecycle = {
    contract: ADP_CANONICAL_HOTEL_LIFECYCLE_STATE_MACHINE_V1,
    state: hotelLifecycleState,
    authorizationScopes: FOUNDER_AUTHORIZATION_SCOPES,
    pdfTemplateEnforcement: {
      contract: ADP_EXECUTIVE_PDF_CANONICAL_TEMPLATE_ENFORCEMENT_V1,
      templateId: CANONICAL_PDF_TEMPLATE_ID,
      portfolioReuseAuthorized: isPortfolioTemplateReuseAuthorized({
        templateId: CANONICAL_PDF_TEMPLATE_ID,
        portfolioDesignAccepted: founderPdfDesignAccepted,
      }),
    },
  };
  manifest.productionReleaseWorkflow = prepareProductionReleaseManifest({
    propertyId,
    stagedVersionId: staged?.versionId || null,
    pdfSha256: pdfParity.pdfSha256 || artifacts.pdf?.sha256 || null,
    founderPdfDesignAccepted,
    hpc: manifest.censusRecordId || null,
  });
  manifest.productionReleaseWorkflow.contract =
    ADP_FULL_E2E_PRODUCTION_RELEASE_WORKFLOW_V1;

  const technicalCustomerDeliveryReady =
    artifacts.s10Status === "PASS" &&
    artifacts.s11Status === "PASS" &&
    artifacts.discoverability?.ok === true &&
    aiAdminRegistration.ok === true &&
    aiAdminParity.ok === true &&
    pdfParity.ok === true &&
    pdfDelivery.ok === true &&
    !bppRequiredGate.blocksUnqualifiedFullE2eComplete &&
    !bppDeliveryBlocked &&
    (readiness === RUN_STATE.CUSTOMER_READY ||
      readiness === RUN_STATE.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS);

  if (mayAwaitPublication) {
    manifest.state = RUN_STATE.AWAITING_PUBLICATION_APPROVAL;
    if (readiness === RUN_STATE.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS) {
      manifest.customerReadiness = RUN_STATE.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS;
    } else {
      manifest.customerReadiness = RUN_STATE.CUSTOMER_READY;
    }
  } else if (technicalCustomerDeliveryReady) {
    // Technical Full E2E customer-delivery certification complete; publication /
    // Memberstack founder-session / PDF design acceptance remain separate gates.
    manifest.state =
      readiness === RUN_STATE.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS
        ? RUN_STATE.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS
        : RUN_STATE.CUSTOMER_READY;
    manifest.customerReadiness = manifest.state;
    if (
      pdfDelivery.authAcceptance !== MEMBERSTACK_AUTHENTICATED_PDF_PASS &&
      !manifest.blockers.includes("MEMBERSTACK_AUTHENTICATED_PDF_PASS_PENDING")
    ) {
      manifest.blockers.push("MEMBERSTACK_AUTHENTICATED_PDF_PASS_PENDING");
    }
    if (
      !founderPdfDesignAccepted &&
      !manifest.blockers.includes("PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING")
    ) {
      manifest.blockers.push("PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING");
    }
  } else if (
    readiness === RUN_STATE.CUSTOMER_QA_BLOCKED ||
    artifacts.discoverability?.ok === false ||
    aiAdminRegistration.ok === false ||
    aiAdminParity.ok === false ||
    pdfParity.ok === false
  ) {
    manifest.state = RUN_STATE.CUSTOMER_QA_BLOCKED;
    manifest.customerReadiness = RUN_STATE.CUSTOMER_QA_BLOCKED;
    if (
      artifacts.discoverability?.ok === false &&
      !manifest.blockers.includes("OWNER_UI_STAGED_REPORT_NOT_DISCOVERABLE")
    ) {
      manifest.blockers.push("OWNER_UI_STAGED_REPORT_NOT_DISCOVERABLE");
    }
    if (
      pdfParity.ok === false &&
      !manifest.blockers.includes("FULL_ADP_PDF_VISUAL_OR_ARTIFACT_PARITY_FAILED")
    ) {
      manifest.blockers.push("FULL_ADP_PDF_VISUAL_OR_ARTIFACT_PARITY_FAILED");
    }
  } else {
    manifest.state = RUN_STATE.NOT_CUSTOMER_READY;
    manifest.customerReadiness = RUN_STATE.NOT_CUSTOMER_READY;
    if (!manifest.blockers.includes("ARTIFACT_OR_BROWSER_QA_INCOMPLETE")) {
      manifest.blockers.push("ARTIFACT_OR_BROWSER_QA_INCOMPLETE");
    }
  }

  manifest.publicationStatus = "NOT_AUTHORIZED";
  manifest.orchestratorVersion = ORCHESTRATOR_VERSION;
  manifest.artifacts = {
    pdfPath: artifacts.pdf?.path || null,
    pdfSha256: artifacts.pdf?.sha256 || null,
    shareCandidateId: artifacts.share?.candidateId || null,
    shareTokenId: artifacts.share?.tokenId || null,
    readinessVerdict: readiness,
    browserQaOk: artifacts.browserQa?.ok === true,
    memberstackAuth: artifacts.memberstackAuth,
  };

  const orderCheck = evaluateFullE2eStageOrderInvariants(manifest, {
    readinessVerdict: readiness,
    pdfPath: artifacts.pdf?.path,
    pdfSha256: artifacts.pdf?.sha256,
    shareOk: artifacts.share?.ok === true,
    browserQaOk: artifacts.browserQa?.ok === true,
    bppFabricatedRank: false,
    wrongHotelFallback: artifacts.browserQa?.checks?.wrongHotelFallback === true,
    duplicateRealityGapAttributes: artifacts.uniqueness?.ok === false,
    paidCallsDuringDownstream: 0,
    contentRendersAloneMarkedReady: false,
  });
  manifest.stageOrderInvariants = orderCheck;

  return {
    // Technical customer-delivery certification can pass without publication /
    // Memberstack / founder PDF design acceptance (those remain separate gates).
    ok: (mayAwaitPublication || technicalCustomerDeliveryReady) && orderCheck.ok,
    calc,
    artifacts,
    readiness,
    paidCalls: 0,
    orderCheck,
    technicalCustomerDeliveryReady,
    mayAwaitPublication,
  };
}

async function finishFullOrResume(manifest, identity, propertyId, costPlan, runId, lockKey, flags) {
  const {
    apply,
    authorizeMeasurement,
    authorizePublish,
    allowPaid,
    stopAfterStage,
    hpcLive: hpcLiveFlag = null,
  } = flags;

  const hpcLive =
    hpcLiveFlag || manifest.stageResults?.S2_HPC_ADP_PERSISTENCE?.detail?.hpcLive || null;

  // Always run zero-cost upstream stages for existing hotels
  const attrs = assessAttributes(propertyId);
  manifest.stageResults.S3_ATTRIBUTE_INTELLIGENCE = stageResult(
    "S3_ATTRIBUTE_INTELLIGENCE",
    attrs.ok ? "PASS" : "FAIL",
    attrs
  );
  if (!manifest.completedStages.includes("S3_ATTRIBUTE_INTELLIGENCE")) {
    manifest.completedStages.push("S3_ATTRIBUTE_INTELLIGENCE");
  }

  const peer = getBrandPortfolioPeerSet(propertyId);
  const bppPlanForPeers = planFullE2eBppMeasurement({ propertyId });
  const bppNa =
    bppPlanForPeers?.bppApplies === false ||
    bppPlanForPeers?.state === "BPP_NOT_APPLICABLE";
  manifest.stageResults.S4_PEER_GOVERNANCE = stageResult(
    "S4_PEER_GOVERNANCE",
    peer ? "PASS" : bppNa ? "BPP_NOT_APPLICABLE" : "APPROVAL_REQUIRED",
    {
      peerPresent: Boolean(peer),
      bppApplies: bppPlanForPeers?.bppApplies ?? null,
      bppState: bppPlanForPeers?.state || null,
      peerLoyaltyHotelsApproved: peer ? (peer.included || []).length : 0,
      note: bppNa
        ? "Governed BPP N/A — no invented peer cohort; peer Loyalty cost = $0"
        : null,
    }
  );

  // Resume after paid monitoring: never re-charge. Complete or re-verify PDF/share/browser QA only.
  if (monitoringAlreadyComplete(manifest, propertyId)) {
    manifest.stageResults.S5_COST_AUTHORIZATION = stageResult(
      "S5_COST_AUTHORIZATION",
      "AUTHORIZED_PRIOR_SPEND_REUSED",
      {
        priorSpendUsd: manifest.costs?.actual || null,
        paidCallsBlocked: true,
        reason: artifactsIncomplete(manifest)
          ? "DOWNSTREAM_ARTIFACT_COMPLETION_ZERO_PAID"
          : "DOWNSTREAM_ARTIFACT_REVERIFY_ZERO_PAID",
      }
    );
    if (
      /PASS_LIVE|REUSED_ARCHIVE/i.test(
        String(manifest.stageResults?.S6_CORE_MONITORING?.status || "")
      ) === false
    ) {
      const core = loadLatestCoreCustomerPeriod(propertyId);
      manifest.stageResults.S6_CORE_MONITORING = stageResult(
        "S6_CORE_MONITORING",
        "REUSED_ARCHIVE_ZERO_PAID",
        { periodId: core?.periodId, observations: core?.observations?.length || 0 }
      );
      markCompleted(manifest, "S6_CORE_MONITORING");
    }
    const downstream = await completeZeroPaidS8ThroughS12(manifest, propertyId, {
      authorizePublish,
    });
    saveRunManifest(manifest);
    releaseHotelRunLock(lockKey, runId);
    return {
      ok: downstream.ok,
      mode: manifest.mode || ORCHESTRATOR_MODE.RESUME,
      runId,
      state: manifest.state,
      identity,
      costPlan,
      calc: downstream.calc,
      artifacts: downstream.artifacts,
      readiness: downstream.readiness,
      paidCalls: 0,
      spentUsd: manifest.costs?.actual || 0,
      publication: "NOT_AUTHORIZED",
      goldenBaselineUntouched: true,
      message:
        "Resumed zero-paid downstream: staged PDF, pinned test share, and browser QA. No additional provider charges. Publication not authorized.",
      manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
      customerReadiness: manifest.customerReadiness || null,
      stageOrderInvariants: downstream.orderCheck,
    };
  }

  // Hard stop: paid monitoring requires verified HPC identity (reuse or authorized create)
  if (!hpcLiveAllowsPaidMonitoring(hpcLive)) {
    manifest.blockers = manifest.blockers || [];
    if (!manifest.blockers.includes("PAID_BLOCKED_UNTIL_HPC_IDENTITY_VERIFIED")) {
      manifest.blockers.push("PAID_BLOCKED_UNTIL_HPC_IDENTITY_VERIFIED");
    }
    manifest.stageResults.S5_COST_AUTHORIZATION = stageResult(
      "S5_COST_AUTHORIZATION",
      "BLOCKED_PENDING_HPC_IDENTITY",
      {
        costPlan,
        hpcOutcome: hpcLive?.outcome || null,
        hpcRecordId: hpcLive?.hpcRecordId || null,
        paidCallsBlocked: true,
      }
    );
    manifest.state = RUN_STATE.AWAITING_PAID_RESEARCH_APPROVAL;
    saveRunManifest(manifest);
    releaseHotelRunLock(lockKey, runId);
    return {
      ok: false,
      error: "PAID_BLOCKED_UNTIL_HPC_IDENTITY_VERIFIED",
      message:
        "No paid monitoring without successful HPC identity verification and required HPC write/reuse authorization. Live outcome must be HPC_EXISTING_RECORD_VERIFIED (or founder-authorized create completed).",
      runId,
      state: manifest.state,
      hpcLive: hpcLive
        ? {
            outcome: hpcLive.outcome,
            hpcRecordId: hpcLive.hpcRecordId || null,
            createProposal: hpcLive.createProposal || null,
          }
        : null,
      costPlan,
      paidCalls: 0,
      airtableWrites: 0,
      publication: "NOT_AUTHORIZED",
      manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
    };
  }

  // Hard stop before paid
  const paidAuthorized =
    Boolean(allowPaid) &&
    Boolean(authorizeMeasurement) &&
    Boolean(apply) &&
    manifest.budgetUsd != null &&
    Number(manifest.budgetUsd) >= Number(costPlan.estimatedTotalUsd);

  manifest.stageResults.S5_COST_AUTHORIZATION = stageResult(
    "S5_COST_AUTHORIZATION",
    paidAuthorized ? "AUTHORIZED" : "AWAITING_APPROVAL",
    {
      costPlan,
      authorizeMeasurement,
      apply,
      budgetUsd: manifest.budgetUsd,
      paidAuthorized,
    }
  );
  manifest.state = RUN_STATE.AWAITING_PAID_RESEARCH_APPROVAL;

  if (!paidAuthorized) {
    // Continue with zero-paid derived stages when archives exist (resume-safe)
    const core = loadLatestCoreCustomerPeriod(propertyId);
    if (core?.observations?.length) {
      manifest.stageResults.S6_CORE_MONITORING = stageResult(
        "S6_CORE_MONITORING",
        "REUSED_ARCHIVE_ZERO_PAID",
        { periodId: core.periodId, observations: core.observations.length }
      );
      markCompleted(manifest, "S6_CORE_MONITORING");
      const downstream = await completeZeroPaidS8ThroughS12(manifest, propertyId, {
        authorizePublish,
      });
      if (authorizePublish) {
        manifest.blockers.push("PUBLICATION_REQUIRES_SEPARATE_EXPLICIT_GATE");
      }
      if (stopAfterStage) {
        manifest.stopAfterStage = stopAfterStage;
      }
      saveRunManifest(manifest);
      releaseHotelRunLock(lockKey, runId);
      return {
        ok: downstream.ok,
        mode: manifest.mode,
        runId,
        state: manifest.state,
        identity,
        costPlan,
        calc: downstream.calc,
        artifacts: downstream.artifacts,
        readiness: downstream.readiness,
        paidCalls: 0,
        paidAuthorized: false,
        message:
          "Zero-paid downstream completed from archives (S8–S12 including PDF/share/browser QA). No provider charges. Publication not authorized.",
        approvalsRequired: paidAuthorized
          ? []
          : [
              "--authorize-measurement",
              "--budget-usd >= estimatedTotalUsd",
              "--apply",
              "--authorization-ref <ledger ref>",
            ],
        manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
        goldenBaselineUntouched: true,
        customerReadiness: manifest.customerReadiness || null,
        stageOrderInvariants: downstream.orderCheck,
      };
    }

    if (authorizePublish) {
      manifest.blockers.push("PUBLICATION_REQUIRES_SEPARATE_EXPLICIT_GATE");
    }

    if (stopAfterStage) {
      manifest.stopAfterStage = stopAfterStage;
    }

    saveRunManifest(manifest);
    // Release after forecasting; paid supervisor acquires its own job lock.
    releaseHotelRunLock(lockKey, runId);

    return {
      ok: true,
      mode: manifest.mode,
      runId,
      state: manifest.state,
      identity,
      costPlan,
      paidCalls: 0,
      paidAuthorized: false,
      message:
        "Full pipeline stopped at paid-research authorization gate. No provider charges. No publication.",
      approvalsRequired: [
        "--authorize-measurement",
        "--budget-usd >= estimatedTotalUsd",
        "--apply",
        "--authorization-ref <ledger ref>",
      ],
      manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
      goldenBaselineUntouched: true,
    };
  }

  // Exact authorization + supervisor handoff (simulation unless allowPaid).
  const auth = loadAuthorizationForRun(runId, propertyId);
  if (!auth) {
    manifest.blockers.push("PAID_MONITORING_REQUIRES_EXACT_AUTHORIZATION_RECORD");
    manifest.state = RUN_STATE.AWAITING_PAID_RESEARCH_APPROVAL;
    saveRunManifest(manifest);
    releaseHotelRunLock(lockKey, runId);
    return {
      ok: false,
      error: "MISSING_EXACT_AUTHORIZATION_RECORD",
      message:
        "Measurement flags alone are insufficient. Persist an exact authorization record (hotel, scope, providers, maxSpendUsd, runId, approver) via ADP_PAID_SUPERVISOR_HANDOFF_V1 before chargeable calls.",
      runId,
      costPlan,
      state: manifest.state,
      handoffContract: ADP_PAID_SUPERVISOR_HANDOFF_V1,
    };
  }

  const handoff = executeAuthorizedSupervisorHandoff({
    runId,
    propertyId,
    hotelIdentity: identity.propertyName || propertyId,
    authorizationRecord: auth,
    simulation: !allowPaid,
    allowPaid: Boolean(allowPaid),
    authorizePublish: Boolean(authorizePublish),
  });

  manifest.stageResults.S6_CORE_MONITORING = stageResult(
    "S6_CORE_MONITORING",
    handoff.ok ? (handoff.simulation ? "PASS_SIM" : "HANDOFF_BOUND") : "FAIL",
    {
      handoffOk: handoff.ok,
      simulation: handoff.simulation || !allowPaid,
      paidCalls: handoff.paidCalls || 0,
      error: handoff.error || null,
      supervisorState: handoff.state?.status || null,
    }
  );
  manifest.costs.supervisor = handoff.state?.spend || null;

  if (!handoff.ok) {
    manifest.blockers.push(handoff.error || "SUPERVISOR_HANDOFF_FAILED");
    manifest.state = RUN_STATE.AWAITING_PAID_RESEARCH_APPROVAL;
    saveRunManifest(manifest);
    releaseHotelRunLock(lockKey, runId);
    return {
      ok: false,
      error: handoff.error,
      message: handoff.message,
      runId,
      costPlan,
      handoff,
      state: manifest.state,
      paidCalls: handoff.paidCalls || 0,
    };
  }

  if (handoff.simulation || !allowPaid) {
    manifest.blockers.push("PAID_ADAPTERS_DISABLED_SIMULATION_ONLY");
    manifest.state = RUN_STATE.AWAITING_PAID_RESEARCH_APPROVAL;
    saveRunManifest(manifest);
    releaseHotelRunLock(lockKey, runId);
    return {
      ok: true,
      simulation: true,
      paidCalls: 0,
      message:
        "Supervisor handoff simulation succeeded with automatic downstream continuation markers. Live paid adapters remain disabled until founder allowPaid + budget.",
      runId,
      costPlan,
      handoff,
      state: manifest.state,
    };
  }

  // Live greenfield path — exact auth + allowPaid: execute Core/Loyalty via greenfield monitor
  // (31-hotel aggregate core-loyalty ledger is NOT used for greenfield hotels).
  const live = await executeAuthorizedGreenfieldLiveMonitoring({
    runId,
    propertyId,
    hotelIdentity: identity.propertyName || propertyId,
    authorizationRecord: auth,
    hardMaxUsd: Number(manifest.budgetUsd) || Number(auth.maxSpendUsd),
    runLoyalty: auth.loyaltyScope?.applicable !== false,
  });

  manifest.stageResults.S6_CORE_MONITORING = stageResult(
    "S6_CORE_MONITORING",
    live.results?.core?.ok ? "PASS_LIVE" : "FAIL",
    live.results?.core || { error: live.error }
  );
  if (live.results?.core?.ok && !manifest.completedStages.includes("S6_CORE_MONITORING")) {
    manifest.completedStages.push("S6_CORE_MONITORING");
  }
  manifest.stageResults.S7_LOYALTY_MONITORING = stageResult(
    "S7_LOYALTY_MONITORING",
    live.results?.loyalty?.ok || live.results?.loyalty?.skipped ? "PASS_LIVE_OR_SKIP" : "FAIL",
    live.results?.loyalty || {}
  );
  if (
    (live.results?.loyalty?.ok || live.results?.loyalty?.skipped) &&
    !manifest.completedStages.includes("S7_LOYALTY_MONITORING")
  ) {
    manifest.completedStages.push("S7_LOYALTY_MONITORING");
  }

  manifest.costs.actual = live.spentUsd || 0;
  manifest.costs.remainingAuthorizedUsd = live.remainingAuthorizedUsd;
  manifest.liveMonitoring = {
    contract: live.contract,
    authorizationId: live.authorizationId,
    spentUsd: live.spentUsd,
    paidCalls: live.paidCalls,
    bppMeasuredRank: live.bppMeasuredRank,
    corePeriodId: live.results?.core?.periodId || null,
    loyaltyPeriodId: live.results?.loyalty?.periodId || null,
    stagedVersionId: live.results?.staged?.versionId || null,
  };

  if (!live.ok) {
    manifest.blockers.push(live.error || "GREENFIELD_LIVE_MONITORING_FAILED");
    manifest.state = RUN_STATE.BLOCKED;
    saveRunManifest(manifest);
    releaseHotelRunLock(lockKey, runId);
    return {
      ok: false,
      error: live.error || "GREENFIELD_LIVE_MONITORING_FAILED",
      runId,
      costPlan,
      live,
      state: manifest.state,
      paidCalls: live.paidCalls || 0,
      spentUsd: live.spentUsd || 0,
    };
  }

  // S8–S12 from live archives (zero additional paid) — PDF/share/browser QA required
  // before AWAITING_PUBLICATION_APPROVAL. Do not treat content-only audit as CUSTOMER_READY.
  const downstream = await completeZeroPaidS8ThroughS12(manifest, propertyId, {
    authorizePublish,
  });
  saveRunManifest(manifest);
  releaseHotelRunLock(lockKey, runId);

  return {
    ok: downstream.ok,
    mode: manifest.mode,
    runId,
    state: manifest.state,
    identity,
    costPlan,
    live,
    calc: downstream.calc,
    artifacts: downstream.artifacts,
    audit: {
      verdict: downstream.artifacts?.contentAudit?.verdict,
      p0: downstream.artifacts?.contentAudit?.p0,
      p1: downstream.artifacts?.contentAudit?.p1,
      readinessVerdict: downstream.readiness,
    },
    paidCalls: live.paidCalls || 0,
    spentUsd: live.spentUsd || 0,
    publication: "NOT_AUTHORIZED",
    goldenBaselineUntouched: true,
    customerReadiness: manifest.customerReadiness || null,
    stageOrderInvariants: downstream.orderCheck,
    message: downstream.ok
      ? "Live greenfield monitoring completed; staged PDF, pinned test share, and browser QA verified. Awaiting separate founder publication approval — not published."
      : "Live greenfield monitoring completed but artifact/browser QA incomplete — not CUSTOMER_READY; publication gate not opened.",
    manifestPath: path.join(runDirFor(runId), "run-manifest.json"),
  };
}

/** Negative regression fixtures (pure). */
export function negativeOrchestratorFixtures() {
  const wrong = resolveHotelIdentityForOrchestrator("Completely Fake Hotel XYZ 99999");
  const dupCharge = {
    code: "DUPLICATE_PROVIDER_CHARGE",
    prevented: true,
    mechanism: "cross-process lock + observation archive reuse + idempotency keys",
  };
  const pub = {
    code: "UNAUTHORIZED_PUBLISH",
    blocked: true,
    requireFlag: "--authorize-publish plus separate publication gate",
  };
  const stageOrderNegatives = permanentStageOrderNegativeFixtures();
  return {
    unresolvedHotelStops: wrong.error === "NEW_HOTEL_INTAKE_REQUIRED" || wrong.error === "IDENTITY_APPROVAL_REQUIRED",
    duplicateChargeProtection: dupCharge,
    unauthorizedPublishBlocked: pub,
    maxRetries: MAX_SAFE_RETRIES,
    stages: PIPELINE_STAGES.map((s) => s.id),
    stageOrderNegatives,
    stageOrderNegativesAllDetectDefect:
      stageOrderNegatives.length > 0 && stageOrderNegatives.every((n) => n.pass === true),
  };
}

export function getStageContractsDocument() {
  return {
    contract: ADP_AUTONOMOUS_HOTEL_FULL_E2E_V1,
    version: ORCHESTRATOR_VERSION,
    frozenBaselineId: ADP_CERTIFIED_31_HOTEL_BASELINE_ID,
    modes: Object.values(ORCHESTRATOR_MODE),
    states: Object.values(RUN_STATE),
    stages: PIPELINE_STAGES,
    costSchedule: Object.fromEntries(
      Object.entries(PROVIDER_CONFIGS).map(([k, v]) => [k, v.costPerCall])
    ),
    coreScenarioTarget: CORE_SCENARIO_TARGET,
    loyaltyScenarioTarget: LOYALTY_SCENARIO_TARGET,
    realityGapMethodologyId: CONDITIONAL_APPROVED_METHODOLOGY_ID,
  };
}
