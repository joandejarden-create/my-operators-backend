/**
 * ADP_CANONICAL_HOTEL_LIFECYCLE_STATE_MACHINE_V1
 *
 * One durable operational lifecycle per commissioned Full ADP hotel.
 * Lifecycle state is distinct from quality certifications and from
 * founder authorization scopes (which must never be inferred from PASS).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ADP_CANONICAL_HOTEL_LIFECYCLE_STATE_MACHINE_V1 =
  "ADP_CANONICAL_HOTEL_LIFECYCLE_STATE_MACHINE_V1";

export const HOTEL_LIFECYCLE_STATES = Object.freeze([
  "HOTEL_IDENTIFIED",
  "HPC_VERIFIED",
  "RESEARCH_READY",
  "PAID_AUTH_REQUIRED",
  "RESEARCH_IN_PROGRESS",
  "RESEARCH_COMPLETE",
  "ANALYTICS_CERTIFIED",
  "CUSTOMER_REPORT_STAGED",
  "EXECUTIVE_PDF_CERTIFIED",
  "AI_ADMIN_REGISTERED",
  "FOUNDER_REVIEW_READY",
  "PUBLICATION_AUTH_REQUIRED",
  "PRODUCTION_PUBLISHED",
  "PRODUCTION_VERIFIED",
  "EXTERNAL_SHARE_AUTH_REQUIRED",
  "EXTERNAL_SHARE_ISSUED",
  "EXTERNAL_SHARE_VERIFIED",
  "AI_ADMIN_CATALOG_REGISTERED",
  "CUSTOMER_DELIVERY_COMPLETE",
]);

/** Independent authorization scopes — never collapsed into a single PASS. */
export const FOUNDER_AUTHORIZATION_SCOPES = Object.freeze({
  HPC_CREATE: "HPC_RECORD_CREATE",
  PAID_RESEARCH: "PAID_PROVIDER_MONITORING",
  PEER_COHORT_BUDGET: "PEER_COHORT_OR_BUDGET",
  PDF_DESIGN_TEMPLATE: "EXECUTIVE_PDF_DESIGN",
  PRODUCTION_PUBLICATION: "CUSTOMER_PUBLICATION",
  PRODUCTION_DEPLOYMENT: "PRODUCTION_DEPLOYMENT",
  EXTERNAL_CLIENT_SHARE: "EXTERNAL_CLIENT_SHARE_ISSUANCE",
  /** Airtable MEASUREMENT_HISTORY index writes — never implied by publication/paid. */
  MEASUREMENT_HISTORY_AIRTABLE: "MEASUREMENT_HISTORY",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
export const LIFECYCLE_STATE_ROOT = path.join(
  ROOT,
  "data/ai-demand-positioning/hotel-lifecycle-state"
);

const ORDER = new Map(HOTEL_LIFECYCLE_STATES.map((s, i) => [s, i]));

export function lifecycleStateRank(state) {
  return ORDER.has(state) ? ORDER.get(state) : -1;
}

export function canTransition(from, to) {
  if (!ORDER.has(from) || !ORDER.has(to)) return false;
  // Allow stay / forward only (idempotent retries may re-enter same state).
  return ORDER.get(to) >= ORDER.get(from);
}

/**
 * Resolve operational lifecycle from durable facts + explicit auth flags.
 * Quality certifications are inputs; they do not grant authorization.
 */
export function resolveHotelLifecycleState(input = {}) {
  const auth = input.authorizations || {};
  if (
    input.externalShareRegistered &&
    input.externalShareVerified &&
    input.productionVerified
  ) {
    return "CUSTOMER_DELIVERY_COMPLETE";
  }
  if (input.externalShareRegistered) return "AI_ADMIN_CATALOG_REGISTERED";
  if (input.externalShareVerified) return "EXTERNAL_SHARE_VERIFIED";
  if (input.externalShareIssued) return "EXTERNAL_SHARE_ISSUED";
  if (input.productionVerified && !auth.externalClientShare) {
    return "EXTERNAL_SHARE_AUTH_REQUIRED";
  }
  if (input.productionVerified) return "PRODUCTION_VERIFIED";
  if (input.productionPublished) return "PRODUCTION_PUBLISHED";
  if (input.founderReviewReady && !auth.productionPublication) {
    return "PUBLICATION_AUTH_REQUIRED";
  }
  if (input.founderReviewReady) return "FOUNDER_REVIEW_READY";
  if (input.aiAdminRegistered && input.executivePdfCertified) {
    return "AI_ADMIN_REGISTERED";
  }
  if (input.executivePdfCertified) return "EXECUTIVE_PDF_CERTIFIED";
  if (input.customerReportStaged) return "CUSTOMER_REPORT_STAGED";
  if (input.analyticsCertified) return "ANALYTICS_CERTIFIED";
  if (input.researchComplete) return "RESEARCH_COMPLETE";
  if (input.researchInProgress) return "RESEARCH_IN_PROGRESS";
  if (input.researchReady && !auth.paidResearch) return "PAID_AUTH_REQUIRED";
  if (input.researchReady) return "RESEARCH_READY";
  if (input.hpcVerified) return "HPC_VERIFIED";
  if (input.hotelIdentified) return "HOTEL_IDENTIFIED";
  return "HOTEL_IDENTIFIED";
}

export function lifecycleStatePath(propertyId) {
  const id = String(propertyId || "").trim();
  return path.join(LIFECYCLE_STATE_ROOT, id, "lifecycle-state.json");
}

export function loadHotelLifecycleState(propertyId) {
  const p = lifecycleStatePath(propertyId);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Durable, idempotent write. Refuses backward transitions unless forceRepair.
 */
export function saveHotelLifecycleState(propertyId, patch = {}, opts = {}) {
  const id = String(propertyId || "").trim();
  if (!id) throw new Error("propertyId_required");
  const prev = loadHotelLifecycleState(id) || {
    contract: ADP_CANONICAL_HOTEL_LIFECYCLE_STATE_MACHINE_V1,
    propertyId: id,
    state: "HOTEL_IDENTIFIED",
    authorizations: {},
    certifications: {},
    history: [],
  };
  const nextFacts = {
    hotelIdentified: true,
    ...prev.facts,
    ...patch.facts,
  };
  const authorizations = {
    ...prev.authorizations,
    ...patch.authorizations,
  };
  const certifications = {
    ...prev.certifications,
    ...patch.certifications,
  };
  const state =
    patch.state ||
    resolveHotelLifecycleState({
      ...nextFacts,
      authorizations,
      ...certifications,
      hotelIdentified: true,
      hpcVerified: nextFacts.hpcVerified,
      researchReady: nextFacts.researchReady,
      researchInProgress: nextFacts.researchInProgress,
      researchComplete: nextFacts.researchComplete,
      analyticsCertified: nextFacts.analyticsCertified || certifications.analyticsCertified,
      customerReportStaged:
        nextFacts.customerReportStaged || certifications.customerReportStaged,
      executivePdfCertified:
        nextFacts.executivePdfCertified || certifications.executivePdfCertified,
      aiAdminRegistered: nextFacts.aiAdminRegistered || certifications.aiAdminRegistered,
      founderReviewReady: nextFacts.founderReviewReady,
      productionPublished: nextFacts.productionPublished,
      productionVerified: nextFacts.productionVerified,
      externalShareIssued: nextFacts.externalShareIssued,
      externalShareVerified: nextFacts.externalShareVerified,
      externalShareRegistered: nextFacts.externalShareRegistered,
    });

  if (!opts.forceRepair && !canTransition(prev.state, state)) {
    return {
      ok: false,
      error: "BACKWARD_TRANSITION_FORBIDDEN",
      from: prev.state,
      to: state,
    };
  }

  const doc = {
    contract: ADP_CANONICAL_HOTEL_LIFECYCLE_STATE_MACHINE_V1,
    propertyId: id,
    state,
    facts: nextFacts,
    authorizations,
    certifications,
    versionPins: {
      ...prev.versionPins,
      ...patch.versionPins,
    },
    updatedAt: new Date().toISOString(),
    history: [
      ...(prev.history || []).slice(-40),
      {
        at: new Date().toISOString(),
        from: prev.state,
        to: state,
        note: patch.note || null,
      },
    ],
  };
  fs.mkdirSync(path.dirname(lifecycleStatePath(id)), { recursive: true });
  fs.writeFileSync(lifecycleStatePath(id), `${JSON.stringify(doc, null, 2)}\n`, "utf8");
  return { ok: true, state: doc };
}

/**
 * Map Full ADP E2E orchestrator stage completion → lifecycle facts (no auth grant).
 */
export function factsFromOrchestratorManifest(manifest = {}) {
  const stages = manifest.stageResults || {};
  const pass = (id) =>
    stages[id]?.status === "PASS" ||
    stages[id]?.status === "AWAITING_FOUNDER_PUBLICATION";
  return {
    hotelIdentified: Boolean(manifest.propertyId || stages.S0_INTAKE_IDENTITY),
    hpcVerified: pass("S2_HPC_ADP_PERSISTENCE") || Boolean(manifest.censusRecordId),
    researchReady: pass("S5_COST_AUTHORIZATION") || pass("S4_PEER_GOVERNANCE"),
    researchInProgress:
      manifest.state === "CORE_RESEARCH_COMPLETE" ||
      Boolean(stages.S6_CORE_MONITORING && !pass("S7_LOYALTY_MONITORING")),
    researchComplete: pass("S6_CORE_MONITORING") && pass("S7_LOYALTY_MONITORING"),
    analyticsCertified: pass("S8_ANALYTICAL_CALCULATIONS"),
    customerReportStaged: pass("S9_CUSTOMER_REPORT") || Boolean(manifest.stagedVersionId),
    executivePdfCertified:
      pass("S10_PDF_SHARE") ||
      stages.S11D_FULL_ADP_PDF_VISUAL_AND_ARTIFACT_PARITY?.status === "PASS" ||
      stages.S11D_FULL_ADP_PDF_PARITY?.status === "PASS" ||
      Boolean(manifest.externalShareLifecycle?.prepared?.founderPdfDesignAccepted),
    aiAdminRegistered:
      stages.S11B_AI_ADMIN_AUTO_REGISTRATION?.ok === true ||
      stages.S11B_AI_ADMIN_AUTO_REGISTRATION?.status === "PASS",
    founderReviewReady:
      manifest.state === "AWAITING_PUBLICATION_APPROVAL" ||
      pass("S12_FOUNDER_READINESS"),
    productionPublished: Boolean(manifest.productionPublished),
    productionVerified: Boolean(manifest.productionVerified),
    externalShareIssued: Boolean(manifest.externalShareIssued),
    externalShareVerified: Boolean(manifest.externalShareVerified),
    externalShareRegistered: Boolean(manifest.externalShareRegistered),
  };
}

export function assertAuthorizationNotInferredFromPass(opts = {}) {
  if (opts.stagePass === true && opts.authorizationGranted === true && !opts.explicitAuthFlag) {
    return { ok: false, code: "AUTH_INFERRED_FROM_PASS" };
  }
  return { ok: true };
}

export function assertNoPaidResearchOnDeliveryRetry(opts = {}) {
  if (opts.deliveryRepairOnly && opts.paidResearchAttempted) {
    return { ok: false, code: "PAID_RESEARCH_REPEATED_ON_DELIVERY_RETRY" };
  }
  return { ok: true };
}

export function permanentLifecycleNegativeFixtures() {
  return [
    {
      id: "AUTH_NOT_FROM_PASS",
      run: () =>
        assertAuthorizationNotInferredFromPass({
          stagePass: true,
          authorizationGranted: true,
          explicitAuthFlag: false,
        }).ok === false,
    },
    {
      id: "NO_PAID_ON_DELIVERY_RETRY",
      run: () =>
        assertNoPaidResearchOnDeliveryRetry({
          deliveryRepairOnly: true,
          paidResearchAttempted: true,
        }).ok === false,
    },
    {
      id: "FORWARD_ONLY",
      run: () =>
        canTransition("CUSTOMER_REPORT_STAGED", "HOTEL_IDENTIFIED") === false &&
        canTransition("CUSTOMER_REPORT_STAGED", "EXECUTIVE_PDF_CERTIFIED") === true,
    },
    {
      id: "PUBLICATION_AUTH_REQUIRED",
      run: () =>
        resolveHotelLifecycleState({
          founderReviewReady: true,
          authorizations: {},
        }) === "PUBLICATION_AUTH_REQUIRED",
    },
    {
      id: "SHARE_AUTH_AFTER_PROD_VERIFY",
      run: () =>
        resolveHotelLifecycleState({
          productionVerified: true,
          authorizations: {},
        }) === "EXTERNAL_SHARE_AUTH_REQUIRED",
    },
    {
      id: "DELIVERY_COMPLETE",
      run: () =>
        resolveHotelLifecycleState({
          productionVerified: true,
          externalShareIssued: true,
          externalShareVerified: true,
          externalShareRegistered: true,
          authorizations: { externalClientShare: true },
        }) === "CUSTOMER_DELIVERY_COMPLETE",
    },
  ];
}

export function runPermanentLifecycleNegativeTests() {
  const results = permanentLifecycleNegativeFixtures().map((f) => ({
    id: f.id,
    pass: Boolean(f.run()),
  }));
  return {
    contract: ADP_CANONICAL_HOTEL_LIFECYCLE_STATE_MACHINE_V1,
    ok: results.every((r) => r.pass),
    results,
  };
}
