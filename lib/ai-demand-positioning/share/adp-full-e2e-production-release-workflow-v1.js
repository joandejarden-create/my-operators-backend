/**
 * ADP_FULL_E2E_PRODUCTION_RELEASE_WORKFLOW_V1
 *
 * Post-certification R0–R8 production client delivery workflow.
 * Prepares automatically after S12; executes only with explicit founder auths.
 * Idempotent: never rotates a still-valid production capability.
 */
import {
  ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1,
  prepareExternalShareWorkflowAfterCertification,
  resolveExternalShareLifecycleState,
  assertShareNotIssuedWithoutAuthorization,
  assertExternalShareUrlIsNotLocalhost,
  isProductionOrientedShareLabel,
} from "./adp-full-e2e-external-client-share-lifecycle-v1.js";
import {
  FOUNDER_AUTHORIZATION_SCOPES,
  resolveHotelLifecycleState,
  saveHotelLifecycleState,
} from "../orchestration/adp-canonical-hotel-lifecycle-state-machine-v1.js";

export const ADP_FULL_E2E_PRODUCTION_RELEASE_WORKFLOW_V1 =
  "ADP_FULL_E2E_PRODUCTION_RELEASE_WORKFLOW_V1";

export const PRODUCTION_RELEASE_STAGES = Object.freeze([
  { id: "R0", name: "Production release preflight" },
  { id: "R1", name: "Exact-version founder publication approval", auth: true },
  { id: "R2", name: "Publish approved report snapshot", requires: ["R1"] },
  { id: "R3", name: "Authorized production sync/deploy", auth: true, requires: ["R2"] },
  { id: "R4", name: "Verify production report and approved PDF", requires: ["R3"] },
  { id: "R5", name: "Founder external-share authorization", auth: true, requires: ["R4"] },
  { id: "R6", name: "Issue sealed production capability", requires: ["R5"] },
  { id: "R7", name: "Verify live external client link", requires: ["R6"] },
  { id: "R8", name: "AI Admin catalog registration + portfolio reconcile", requires: ["R7"] },
]);

/**
 * Build a durable release preflight manifest after local certification.
 * Never publishes, deploys, or mints.
 */
export function prepareProductionReleaseManifest(input = {}) {
  const propertyId = String(input.propertyId || "").trim();
  const stagedVersionId = String(input.stagedVersionId || "").trim();
  const pdfSha256 = input.pdfSha256 || null;
  const existingShare = input.existingProductionShare || null;
  const sharePrep = prepareExternalShareWorkflowAfterCertification({
    stagedCertified: Boolean(stagedVersionId),
    founderPdfDesignAccepted: Boolean(input.founderPdfDesignAccepted),
  });

  const blockers = [];
  if (!propertyId) blockers.push("PROPERTY_ID_MISSING");
  if (!stagedVersionId) blockers.push("STAGED_VERSION_MISSING");
  if (!pdfSha256) blockers.push("APPROVED_PDF_SHA_MISSING");
  if (!input.founderPdfDesignAccepted) {
    blockers.push("PDF_DESIGN_ACCEPTANCE_RECOMMENDED");
  }

  const hasValidExisting =
    existingShare &&
    isProductionOrientedShareLabel(existingShare.label) &&
    existingShare.tokenFingerprint;

  return {
    contract: ADP_FULL_E2E_PRODUCTION_RELEASE_WORKFLOW_V1,
    shareLifecycleContract: ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1,
    ok: blockers.filter((b) => !b.includes("RECOMMENDED")).length === 0,
    executed: false,
    propertyId,
    stagedVersionId: stagedVersionId || null,
    pdfSha256,
    hpc: input.hpc || null,
    existingValidProductionShare: Boolean(hasValidExisting),
    existingShareFingerprint: hasValidExisting
      ? existingShare.tokenFingerprint
      : null,
    reuseExistingShare: Boolean(hasValidExisting),
    stages: PRODUCTION_RELEASE_STAGES.map((s) => ({
      ...s,
      status: "PENDING",
    })),
    authorizationsRequired: [
      FOUNDER_AUTHORIZATION_SCOPES.PRODUCTION_PUBLICATION,
      FOUNDER_AUTHORIZATION_SCOPES.PRODUCTION_DEPLOYMENT,
      FOUNDER_AUTHORIZATION_SCOPES.EXTERNAL_CLIENT_SHARE,
    ],
    sharePrep,
    blockers,
    note:
      "Prepared only. R1/R3/R5 require explicit founder authorization; R6 refuses without R4 PASS.",
  };
}

/**
 * Gate a proposed action. Fail closed without matching exact-version auth.
 */
export function assertProductionReleaseActionAllowed(action, ctx = {}) {
  const a = String(action || "").toUpperCase();
  if (a === "PUBLISH" || a === "R2") {
    if (!ctx.authorizePublish) {
      return { ok: false, code: "PUBLICATION_AUTH_MISSING" };
    }
    if (
      ctx.expectedStagedVersionId &&
      ctx.authorizedStagedVersionId &&
      ctx.expectedStagedVersionId !== ctx.authorizedStagedVersionId
    ) {
      return { ok: false, code: "PUBLICATION_VERSION_MISMATCH" };
    }
    return { ok: true };
  }
  if (a === "DEPLOY" || a === "R3") {
    if (!ctx.authorizeDeploy) {
      return { ok: false, code: "DEPLOY_AUTH_MISSING" };
    }
    return { ok: true };
  }
  if (a === "ISSUE_SHARE" || a === "R6") {
    const auth = assertShareNotIssuedWithoutAuthorization({
      issueAttempted: true,
      authorizeExternalClientLinks: Boolean(ctx.authorizeExternalClientLinks),
    });
    if (!auth.ok) return auth;
    if (!ctx.productionVerified) {
      return { ok: false, code: "SHARE_BEFORE_PRODUCTION_VERIFICATION" };
    }
    if (ctx.existingValidShare && !ctx.forceRotate) {
      return {
        ok: true,
        reuse: true,
        code: "REUSE_EXISTING_VALID_SHARE",
      };
    }
    return { ok: true };
  }
  return { ok: false, code: "UNKNOWN_ACTION" };
}

export function assertShareNotIssuedBeforeProductionVerification(opts = {}) {
  if (opts.shareIssued && !opts.productionVerified) {
    return { ok: false, code: "SHARE_ISSUED_BEFORE_PRODUCTION_VERIFICATION" };
  }
  return { ok: true };
}

export function assertValidShareNotRotated(opts = {}) {
  if (opts.existingValidShare && opts.rotated && !opts.demonstratedReason) {
    return { ok: false, code: "VALID_SHARE_ROTATED_UNNECESSARILY" };
  }
  return { ok: true };
}

/**
 * Persist lifecycle after a verified release step (local durable state only).
 */
export function recordProductionReleaseProgress(propertyId, step, facts = {}) {
  const map = {
    R2: { productionPublished: true },
    R4: { productionVerified: true },
    R6: { externalShareIssued: true },
    R7: { externalShareVerified: true },
    R8: { externalShareRegistered: true, externalShareVerified: true },
  };
  const patchFacts = map[step] || {};
  return saveHotelLifecycleState(propertyId, {
    facts: { ...patchFacts, ...facts },
    authorizations: facts.authorizations || {},
    note: `production_release_${step}`,
  });
}

export function permanentProductionReleaseNegativeFixtures() {
  return [
    {
      id: "NO_PUBLISH_WITHOUT_AUTH",
      run: () =>
        assertProductionReleaseActionAllowed("PUBLISH", {
          authorizePublish: false,
        }).ok === false,
    },
    {
      id: "VERSION_PIN",
      run: () =>
        assertProductionReleaseActionAllowed("PUBLISH", {
          authorizePublish: true,
          expectedStagedVersionId: "staged_a",
          authorizedStagedVersionId: "staged_b",
        }).ok === false,
    },
    {
      id: "NO_SHARE_BEFORE_VERIFY",
      run: () =>
        assertShareNotIssuedBeforeProductionVerification({
          shareIssued: true,
          productionVerified: false,
        }).ok === false,
    },
    {
      id: "NO_UNNECESSARY_ROTATION",
      run: () =>
        assertValidShareNotRotated({
          existingValidShare: true,
          rotated: true,
          demonstratedReason: false,
        }).ok === false,
    },
    {
      id: "PREPARE_ONLY",
      run: () => {
        const m = prepareProductionReleaseManifest({
          propertyId: "adp_test",
          stagedVersionId: "staged_x",
          pdfSha256: "abc",
          founderPdfDesignAccepted: true,
        });
        return m.executed === false && m.authorizationsRequired.length === 3;
      },
    },
    {
      id: "LOCALHOST_URL",
      run: () =>
        assertExternalShareUrlIsNotLocalhost(
          "http://127.0.0.1:8080/x"
        ).ok === false,
    },
  ];
}

export function runPermanentProductionReleaseNegativeTests() {
  const results = permanentProductionReleaseNegativeFixtures().map((f) => ({
    id: f.id,
    pass: Boolean(f.run()),
  }));
  return {
    contract: ADP_FULL_E2E_PRODUCTION_RELEASE_WORKFLOW_V1,
    ok: results.every((r) => r.pass),
    results,
  };
}

export function resolvePostCertificationOperationalState(input = {}) {
  return resolveHotelLifecycleState({
    founderReviewReady: true,
    customerReportStaged: true,
    executivePdfCertified: Boolean(input.executivePdfCertified),
    aiAdminRegistered: Boolean(input.aiAdminRegistered),
    productionPublished: Boolean(input.productionPublished),
    productionVerified: Boolean(input.productionVerified),
    externalShareIssued: Boolean(input.externalShareIssued),
    externalShareVerified: Boolean(input.externalShareVerified),
    externalShareRegistered: Boolean(input.externalShareRegistered),
    authorizations: {
      productionPublication: Boolean(input.authorizePublish),
      externalClientShare: Boolean(input.authorizeExternalClientLinks),
    },
  });
}

export { resolveExternalShareLifecycleState };
