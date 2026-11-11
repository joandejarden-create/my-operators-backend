/**
 * ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1
 *
 * State machine for commissioned Full ADP external client shares.
 * Orchestrator prepares the workflow after certification; never auto-publishes,
 * deploys, or mints live shares without explicit founder authorization.
 */

export const ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1 =
  "ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1";

export const EXTERNAL_SHARE_LIFECYCLE_STATES = Object.freeze([
  "NOT_READY",
  "STAGED_CERTIFIED",
  "FOUNDER_APPROVED_FOR_PUBLICATION",
  "PRODUCTION_PUBLISHED",
  "EXTERNAL_SHARE_ISSUED",
  "EXTERNAL_SHARE_VERIFIED",
  "EXTERNAL_SHARE_REGISTERED",
]);

export const EXTERNAL_SHARE_AUTH_GATES = Object.freeze({
  R1_PUBLICATION: "FOUNDER_APPROVED_FOR_PUBLICATION",
  R3_DEPLOY: "PRODUCTION_DEPLOY_AUTHORIZED",
  R5_EXTERNAL_SHARE: "EXTERNAL_CLIENT_SHARE_AUTHORIZED",
});

const LOCALHOST_RE = /localhost|127\.0\.0\.1/i;
const NON_PRODUCTION_LABEL_RE =
  /LOCAL_TEST|neg-legacy|LOCAL_TEST_ONLY/i;

/**
 * @param {object} input
 * @returns {string} one of EXTERNAL_SHARE_LIFECYCLE_STATES
 */
export function resolveExternalShareLifecycleState(input = {}) {
  if (input.externalShareRegistered && input.externalShareVerified) {
    return "EXTERNAL_SHARE_REGISTERED";
  }
  if (input.externalShareVerified) return "EXTERNAL_SHARE_VERIFIED";
  if (input.externalShareIssued) return "EXTERNAL_SHARE_ISSUED";
  if (input.productionPublished) return "PRODUCTION_PUBLISHED";
  if (input.founderApprovedForPublication) {
    return "FOUNDER_APPROVED_FOR_PUBLICATION";
  }
  if (input.stagedCertified) return "STAGED_CERTIFIED";
  return "NOT_READY";
}

/**
 * Prepare (do not execute) the production-share workflow after certification.
 * Safe to call from Full ADP E2E S12 — never mints or publishes.
 */
export function prepareExternalShareWorkflowAfterCertification(input = {}) {
  const stagedCertified = Boolean(input.stagedCertified);
  const founderPdfDesignAccepted = Boolean(input.founderPdfDesignAccepted);
  const state = resolveExternalShareLifecycleState({
    stagedCertified,
    founderApprovedForPublication: false,
    productionPublished: false,
    externalShareIssued: false,
    externalShareVerified: false,
    externalShareRegistered: false,
  });

  return {
    contract: ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1,
    ok: true,
    executed: false,
    lifecycleState: state,
    founderPdfDesignAccepted,
    note:
      "PDF design acceptance is not publication or external-share authorization.",
    nextRequiredGates: stagedCertified
      ? [
          EXTERNAL_SHARE_AUTH_GATES.R1_PUBLICATION,
          EXTERNAL_SHARE_AUTH_GATES.R3_DEPLOY,
          EXTERNAL_SHARE_AUTH_GATES.R5_EXTERNAL_SHARE,
        ]
      : ["STAGED_CERTIFICATION"],
    aiAdminDisplayRules: {
      inventExternalUrlForUnpublished: false,
      labelMissingLinkAsComplete: false,
      showLifecycleState: true,
    },
  };
}

export function isProductionOrientedShareLabel(label) {
  const s = String(label || "");
  if (!s || NON_PRODUCTION_LABEL_RE.test(s)) return false;
  return (
    /^production-distribution:|^client-ready:|^hilton-client|^renaissance-client|^founder-review:adp_bethesda/i.test(
      s
    ) || s.includes("— client share") || s.includes("client share")
  );
}

export function assertExternalShareUrlIsNotLocalhost(url) {
  if (!url) return { ok: false, code: "URL_MISSING" };
  if (LOCALHOST_RE.test(String(url))) {
    return { ok: false, code: "LOCALHOST_FORBIDDEN_AS_EXTERNAL" };
  }
  return { ok: true };
}

export function assertShareNotIssuedWithoutAuthorization(opts = {}) {
  if (opts.issueAttempted && !opts.authorizeExternalClientLinks) {
    return {
      ok: false,
      code: "SHARE_ISSUE_WITHOUT_FOUNDER_AUTHORIZATION",
    };
  }
  return { ok: true };
}

export function assertStagedNotRepresentedAsPublished(opts = {}) {
  if (opts.stagedOnly && opts.representedAsPublished) {
    return { ok: false, code: "STAGED_REPRESENTED_AS_PUBLISHED" };
  }
  return { ok: true };
}

export function assertNoTokenLeakInAuditText(text) {
  const s = String(text || "");
  if (/adpshare\.v1\.eyJ/i.test(s)) {
    return { ok: false, code: "LIVE_TOKEN_EXPOSED_IN_AUDIT" };
  }
  return { ok: true };
}

export function permanentExternalShareLifecycleNegativeFixtures() {
  return [
    {
      id: "LOCALHOST_AS_EXTERNAL",
      run: () =>
        assertExternalShareUrlIsNotLocalhost(
          "http://localhost:8080/owner-ai-demand-share.html?share=x"
        ).ok === false,
    },
    {
      id: "ISSUE_WITHOUT_AUTH",
      run: () =>
        assertShareNotIssuedWithoutAuthorization({
          issueAttempted: true,
          authorizeExternalClientLinks: false,
        }).ok === false,
    },
    {
      id: "STAGED_AS_PUBLISHED",
      run: () =>
        assertStagedNotRepresentedAsPublished({
          stagedOnly: true,
          representedAsPublished: true,
        }).ok === false,
    },
    {
      id: "TOKEN_LEAK",
      run: () =>
        assertNoTokenLeakInAuditText(
          "adpshare.v1.eyJ0ZXN0IjoiYmFkIn0.sig"
        ).ok === false,
    },
    {
      id: "LIFECYCLE_ORDER",
      run: () =>
        resolveExternalShareLifecycleState({ stagedCertified: true }) ===
        "STAGED_CERTIFIED",
    },
    {
      id: "PREPARE_NO_EXECUTE",
      run: () => {
        const p = prepareExternalShareWorkflowAfterCertification({
          stagedCertified: true,
          founderPdfDesignAccepted: true,
        });
        return p.executed === false && p.lifecycleState === "STAGED_CERTIFIED";
      },
    },
  ];
}

export function runPermanentExternalShareLifecycleNegativeTests() {
  const fixtures = permanentExternalShareLifecycleNegativeFixtures();
  const results = fixtures.map((f) => ({
    id: f.id,
    pass: Boolean(f.run()),
  }));
  return {
    contract: ADP_FULL_E2E_EXTERNAL_CLIENT_SHARE_LIFECYCLE_V1,
    ok: results.every((r) => r.pass),
    results,
  };
}
