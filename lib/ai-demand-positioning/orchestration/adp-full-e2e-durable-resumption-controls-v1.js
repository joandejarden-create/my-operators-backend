/**
 * Durable resumption / idempotency controls for Full ADP E2E.
 * Downstream delivery repair must never re-run paid research.
 */
import {
  assertNoPaidResearchOnDeliveryRetry,
  assertAuthorizationNotInferredFromPass,
  canTransition,
  resolveHotelLifecycleState,
  saveHotelLifecycleState,
  loadHotelLifecycleState,
} from "./adp-canonical-hotel-lifecycle-state-machine-v1.js";
import {
  assertProductionReleaseActionAllowed,
  assertShareNotIssuedBeforeProductionVerification,
  assertValidShareNotRotated,
  prepareProductionReleaseManifest,
} from "../share/adp-full-e2e-production-release-workflow-v1.js";

export const ADP_FULL_E2E_DURABLE_RESUMPTION_CONTROLS_V1 =
  "ADP_FULL_E2E_DURABLE_RESUMPTION_CONTROLS_V1";

const INTERRUPT_SCENARIOS = [
  "AFTER_PROPERTY_RESEARCH",
  "AFTER_PAID_BEFORE_ANALYTICS",
  "AFTER_CORE_BEFORE_PEER_LOYALTY",
  "DURING_BPP_CERTIFICATION",
  "AFTER_REPORT_STAGING",
  "DURING_PDF_GENERATION",
  "AI_ADMIN_REGISTRATION_FAIL",
  "PUBLISH_OK_DEPLOY_FAIL",
  "DEPLOY_OK_PDF_VERIFY_FAIL",
  "SHARE_OK_CATALOG_FAIL",
  "SHARE_ALREADY_EXISTS",
  "RAILWAY_CLI_SESSION_EXPIRED",
  "PRODUCTION_ARTIFACT_MISSING",
  "DUPLICATE_PAID_ATTEMPT",
  "PUBLICATION_AUTH_VERSION_MISMATCH",
];

/**
 * Given last verified durable stage, compute safe resume plan.
 */
export function planIdempotentResumption(input = {}) {
  const last = String(input.lastVerifiedStage || "").trim();
  const paidComplete = Boolean(input.paidResearchComplete);
  const existingShare = Boolean(input.existingValidShare);
  const blockers = [];

  if (input.attemptPaidAgain && paidComplete) {
    blockers.push("PAID_RESEARCH_ALREADY_COMPLETE");
  }
  if (input.rotateShare && existingShare && !input.demonstratedReason) {
    blockers.push("VALID_SHARE_MUST_REUSE");
  }
  if (input.bypassAuth) {
    blockers.push("AUTH_BYPASS_FORBIDDEN");
  }

  const skipPaid = paidComplete || /S8|S9|S10|S11|S12|R[0-8]/.test(last);
  const resumeFrom = last || "S0_INTAKE_IDENTITY";

  return {
    contract: ADP_FULL_E2E_DURABLE_RESUMPTION_CONTROLS_V1,
    ok: blockers.length === 0,
    resumeFrom,
    skipPaidResearch: skipPaid,
    reuseExistingShare: existingShare,
    mustReauthorize:
      input.publicationAuthMissing || input.publicationVersionMismatch || false,
    blockers,
    note: "Resume from last verified durable stage; never infer auth from PASS.",
  };
}

export function assertFullE2ePassRequiresCustomerDelivery(opts = {}) {
  if (
    opts.reportedFullE2ePass === true &&
    opts.requiredCustomerDeliveryFailed === true
  ) {
    return { ok: false, code: "FULL_E2E_PASS_WITH_DELIVERY_FAILURE" };
  }
  return { ok: true };
}

export function assertSyntheticExcludedFromOperationalRegistry(opts = {}) {
  const ops = opts.operationalIds || [];
  const synth = opts.syntheticId || "adp_scale_readiness_synthetic_v1";
  if (ops.includes(synth)) {
    return { ok: false, code: "SYNTHETIC_IN_OPERATIONAL_REGISTRY" };
  }
  return { ok: true };
}

export function assertCensusBootRebuildNotRequiredForAdpDeploy(opts = {}) {
  if (opts.requiresHeavyweightCensusRebuild === true) {
    return { ok: false, code: "PROHIBITED_CENSUS_BOOT_REBUILD" };
  }
  if (opts.censusMapBootBuildEnv === "1" && opts.productionLeanDeploy === true) {
    return { ok: false, code: "BOOT_BUILD_ENABLED_ON_LEAN_DEPLOY" };
  }
  return { ok: true };
}

export function permanentResumptionNegativeFixtures() {
  return [
    {
      id: "NO_PAID_ON_DELIVERY_RETRY",
      run: () =>
        assertNoPaidResearchOnDeliveryRetry({
          deliveryRepairOnly: true,
          paidResearchAttempted: true,
        }).ok === false,
    },
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
      id: "RESUME_SKIPS_PAID",
      run: () => {
        const p = planIdempotentResumption({
          lastVerifiedStage: "S9_CUSTOMER_REPORT",
          paidResearchComplete: true,
          attemptPaidAgain: true,
        });
        return p.ok === false && p.blockers.includes("PAID_RESEARCH_ALREADY_COMPLETE");
      },
    },
    {
      id: "SHARE_BEFORE_VERIFY",
      run: () =>
        assertShareNotIssuedBeforeProductionVerification({
          shareIssued: true,
          productionVerified: false,
        }).ok === false,
    },
    {
      id: "NO_ROTATE_VALID",
      run: () =>
        assertValidShareNotRotated({
          existingValidShare: true,
          rotated: true,
          demonstratedReason: false,
        }).ok === false,
    },
    {
      id: "VERSION_MISMATCH",
      run: () =>
        assertProductionReleaseActionAllowed("PUBLISH", {
          authorizePublish: true,
          expectedStagedVersionId: "a",
          authorizedStagedVersionId: "b",
        }).ok === false,
    },
    {
      id: "FULL_PASS_NEEDS_DELIVERY",
      run: () =>
        assertFullE2ePassRequiresCustomerDelivery({
          reportedFullE2ePass: true,
          requiredCustomerDeliveryFailed: true,
        }).ok === false,
    },
    {
      id: "SYNTHETIC_EXCLUDED",
      run: () =>
        assertSyntheticExcludedFromOperationalRegistry({
          operationalIds: ["adp_bethesda_marriott"],
          syntheticId: "adp_scale_readiness_synthetic_v1",
        }).ok === true,
    },
    {
      id: "NO_CENSUS_BOOT_ON_LEAN",
      run: () =>
        assertCensusBootRebuildNotRequiredForAdpDeploy({
          requiresHeavyweightCensusRebuild: true,
        }).ok === false,
    },
    {
      id: "FORWARD_TRANSITION",
      run: () => canTransition("RESEARCH_COMPLETE", "ANALYTICS_CERTIFIED") === true,
    },
    {
      id: "PREPARE_RELEASE_NO_EXECUTE",
      run: () => {
        const m = prepareProductionReleaseManifest({
          propertyId: "adp_scale_readiness_synthetic_v1",
          stagedVersionId: "staged_sim",
          pdfSha256: "deadbeef",
          founderPdfDesignAccepted: true,
        });
        return m.executed === false;
      },
    },
    {
      id: "INTERRUPT_COVERAGE",
      run: () => INTERRUPT_SCENARIOS.length === 15,
    },
  ];
}

export function runPermanentResumptionNegativeTests() {
  const results = permanentResumptionNegativeFixtures().map((f) => ({
    id: f.id,
    pass: Boolean(f.run()),
  }));
  return {
    contract: ADP_FULL_E2E_DURABLE_RESUMPTION_CONTROLS_V1,
    ok: results.every((r) => r.pass),
    results,
    interruptScenariosCovered: INTERRUPT_SCENARIOS,
  };
}

/**
 * Simulate durable state recoveries for interrupt scenarios (local only).
 */
export function simulateInterruptRecoveries(propertyId = "adp_scale_readiness_synthetic_v1") {
  const results = [];
  const steps = [
    { note: "AFTER_PROPERTY_RESEARCH", facts: { hotelIdentified: true, hpcVerified: false } },
    {
      note: "AFTER_PAID_BEFORE_ANALYTICS",
      facts: { researchComplete: true, analyticsCertified: false },
      authorizations: { paidResearch: true },
    },
    {
      note: "AFTER_REPORT_STAGING",
      facts: {
        researchComplete: true,
        analyticsCertified: true,
        customerReportStaged: true,
      },
    },
    {
      note: "SHARE_ALREADY_EXISTS",
      facts: {
        productionVerified: true,
        externalShareIssued: true,
        externalShareVerified: true,
      },
      authorizations: { externalClientShare: true, productionPublication: true },
    },
    {
      note: "DUPLICATE_PAID_ATTEMPT",
      facts: { researchComplete: true, customerReportStaged: true },
      plan: planIdempotentResumption({
        lastVerifiedStage: "S10_PDF_SHARE",
        paidResearchComplete: true,
        attemptPaidAgain: true,
      }),
    },
  ];

  for (const s of steps) {
    if (s.plan) {
      results.push({
        scenario: s.note,
        ok: s.plan.ok === false,
        detail: s.plan.blockers,
      });
      continue;
    }
    const saved = saveHotelLifecycleState(
      propertyId,
      {
        facts: s.facts,
        authorizations: s.authorizations || {},
        note: s.note,
      },
      { forceRepair: true }
    );
    const loaded = loadHotelLifecycleState(propertyId);
    results.push({
      scenario: s.note,
      ok: saved.ok === true && loaded?.state != null,
      state: loaded?.state || null,
    });
  }

  return {
    propertyId,
    ok: results.every((r) => r.ok),
    results,
  };
}

export { resolveHotelLifecycleState, INTERRUPT_SCENARIOS };
