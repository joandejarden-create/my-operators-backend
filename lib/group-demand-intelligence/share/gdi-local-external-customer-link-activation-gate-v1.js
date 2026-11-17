/**
 * Permanent hotel-agnostic gate: reject customer-link activation when local staged
 * customer presentation and simulated external share inventory diverge.
 *
 * Does not write CURRENT, issue tokens, or call paid providers.
 */
import {
  assessAuthSharePackageParity,
  COMMERCIAL_DRAWER_PARITY_FIELDS,
} from "./gdi-auth-share-package-parity-v1.js";

export const GDI_LOCAL_EXTERNAL_CUSTOMER_LINK_ACTIVATION_GATE_V1 =
  "gdi-local-external-customer-link-activation-gate-v1";

/**
 * @param {string} hotelId
 * @param {{ expectedFingerprint?: string, expectedCounts?: { all?: number, hotelMarket?: number, cmi?: number }, root?: string, env?: object }} [opts]
 */
export function evaluateLocalExternalCustomerLinkActivationGate(hotelId, opts = {}) {
  const assessment = assessAuthSharePackageParity(hotelId, opts);
  const fails = [];
  const sim = assessment?.simulatedShareFromStaged || {};
  const auth = assessment?.auth || {};

  if (!(auth.ok === true)) {
    fails.push({
      code: "AUTH_STAGED_UNAVAILABLE",
      detail: assessment?.auth || null,
    });
  }
  if (!(sim.parityOk === true && sim.ok === true)) {
    fails.push({
      code: "SIMULATED_SHARE_PARITY_FAILED",
      missingInSim: sim.missingInSim || [],
      extraInSim: sim.extraInSim || [],
      fieldMismatchCount: sim.fieldMismatchCount || 0,
      detailFailCount: sim.detailFailCount || 0,
      sampleFieldMismatches: (sim.fieldMismatches || []).slice(0, 5),
    });
  }

  const fp = auth.fingerprint || sim.fingerprint || null;
  if (opts.expectedFingerprint) {
    if (String(fp || "").toLowerCase() !== String(opts.expectedFingerprint).toLowerCase()) {
      fails.push({
        code: "FINGERPRINT_MISMATCH",
        expected: opts.expectedFingerprint,
        actual: fp,
      });
    }
  }

  const inv = auth.inventory || sim.inventory || {};
  const expected = opts.expectedCounts || {};
  for (const key of ["all", "hotelMarket", "cmi"]) {
    if (expected[key] != null && Number(inv[key]) !== Number(expected[key])) {
      fails.push({
        code: `COUNT_${key.toUpperCase()}_MISMATCH`,
        expected: expected[key],
        actual: inv[key],
      });
    }
  }

  if ((sim.missingInSim || []).length || (sim.extraInSim || []).length) {
    fails.push({
      code: "OPPORTUNITY_ID_SET_MISMATCH",
      missingInSim: sim.missingInSim || [],
      extraInSim: sim.extraInSim || [],
    });
  }

  return {
    contract: GDI_LOCAL_EXTERNAL_CUSTOMER_LINK_ACTIVATION_GATE_V1,
    hotelId,
    ok: fails.length === 0,
    fingerprint: fp,
    inventory: inv,
    commercialFieldsCompared: COMMERCIAL_DRAWER_PARITY_FIELDS,
    assessmentSummary: {
      authOk: auth.ok === true,
      authCount: auth.opportunityIds?.length ?? null,
      simulatedShareParityOk: sim.parityOk === true,
      simulatedShareCount: sim.inventory?.all ?? null,
      liveShareCurrentAvailable:
        assessment?.liveShare?.ok === true &&
        assessment?.liveShare?.mode === "PACKAGE",
      productionExternalReady: assessment?.productionExternalReady === true,
      founderApprovalRequired: assessment?.founderApprovalRequired !== false,
    },
    fails,
    approvedCapabilityDifferences: [
      {
        id: "auth_vs_share_access",
        note: "Local preview may use founder auth + preview=staged; external uses share token + CURRENT only",
      },
      {
        id: "surfe_on_click",
        note: "Authenticated Surfe remains founder-gated on-click; external must not silently enable paid Surfe",
      },
      {
        id: "feedback_capabilities",
        note: "CAN_VALIDATE / action / outcome require explicit capability grant — not default on issue",
      },
    ],
  };
}

export function assertLocalExternalCustomerLinkActivationGate(hotelId, opts = {}) {
  return evaluateLocalExternalCustomerLinkActivationGate(hotelId, opts);
}
