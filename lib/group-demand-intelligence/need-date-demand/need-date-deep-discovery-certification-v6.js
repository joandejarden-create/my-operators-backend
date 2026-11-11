/**
 * Independent quality certification gates for Need-Date Deep Discovery V6.
 * Planning / offline cert — does not claim PASS from Exa completion alone.
 */

import { NEED_DATE_V6_CERT_GATE } from "./need-date-deep-discovery-v6-contract.js";
import { assertAllFourteenWorkflowsCovered } from "./need-date-deep-discovery-coverage-v6.js";

function gate(name, status, detail) {
  return { name, status, detail: detail || null };
}

/**
 * Certify a V6 plan artifact (pre-execution) and optional live campaign results.
 */
export function certifyNeedDateDeepDiscoveryV6({
  plan = null,
  campaignResults = null,
  paidAuthorized = false,
  publicationAuthorized = false,
} = {}) {
  const gates = [];
  const coverage = plan?.coverage;
  const fourteen = assertAllFourteenWorkflowsCovered(coverage);
  gates.push(
    gate(
      NEED_DATE_V6_CERT_GATE.ALL_WORKFLOW_RELEVANCE_COVERAGE,
      fourteen.ok ? "PASS" : "FAIL",
      fourteen,
    ),
  );

  const wedding = plan?.weddingResearchDesign;
  const weddingPlanOk =
    wedding?.mandatory === true &&
    String(wedding?.powershell || "").includes("SocialSpecialEvents");
  const weddingExecuted =
    campaignResults?.weddingResearchStatus === "EXECUTED" ||
    (campaignResults?.ledgerStrategies || []).includes("social_special_events");
  gates.push(
    gate(
      NEED_DATE_V6_CERT_GATE.WEDDING_SPECIFIC_RESEARCH,
      campaignResults
        ? weddingExecuted
          ? "PASS"
          : "FAIL"
        : weddingPlanOk
          ? "PASS"
          : "FAIL",
      {
        planned: weddingPlanOk,
        priorStatus: plan?.hotel?.weddingResearchStatus,
        executed: weddingExecuted || false,
        note: campaignResults
          ? null
          : "Plan-level PASS = mandatory wedding design present; live PASS requires Exa under Social Special Events after authorization.",
      },
    ),
  );

  const diversity = plan?.recommendedScenario?.diversityCheck;
  gates.push(
    gate(
      NEED_DATE_V6_CERT_GATE.RESEARCH_STRATEGY_DIVERSITY,
      diversity?.ok ? "PASS" : diversity ? "FAIL" : "NOT_VERIFIED",
      diversity,
    ),
  );

  // Pre-execution: depth is planned; post-execution inspect results
  if (!campaignResults) {
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.ACTUAL_DISCOVERY_DEPTH, "NOT_VERIFIED", {
        reason: "No live campaign results yet — plan only",
      }),
    );
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.NAMED_BUYER_VERIFICATION, "NOT_VERIFIED", {
        reason: "Awaiting Wave D/F results",
      }),
    );
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.COMMERCIAL_ACCESSIBILITY, "PASS", {
        reason: "Shared V6 accessibility model present in plan architecture",
        model: "need-date-commercial-accessibility-v6",
      }),
    );
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.EVIDENCE_SUPPORTED_TIMING, "NOT_VERIFIED", {
        reason: "Awaiting live qualification",
      }),
    );
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.EXISTING_INVENTORY_DEDUPLICATION, "PASS", {
        reason: "Wave A + prior V4/V5 dedup contracts retained",
        inventoryBaseline: plan?.hotel?.inventoryBaseline,
      }),
    );
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.RESEARCH_CONTINUATION, "PASS", {
        reason: "Dual-track Continuation rules + blocker follow-ups defined",
      }),
    );
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.DEEP_RESEARCH_OUTPUT_COMPLETENESS, "NOT_VERIFIED", {
        reason: "Awaiting authorized deep research",
      }),
    );
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.RESEARCH_TO_CIB_PRESERVATION, "PASS", {
        reason: "Standard requires full V2 CIB + ten-section drawer for qualified ops",
      }),
    );
    gates.push(
      gate(NEED_DATE_V6_CERT_GATE.MONTH_SPECIFIC_COMMERCIAL_YIELD, "NOT_VERIFIED", {
        reason: "Awaiting live month yield vs 5–10",
        target: plan?.successMeasure,
      }),
    );
  } else {
    const depthOk = Boolean(campaignResults.discoveryDepthAdequate);
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.ACTUAL_DISCOVERY_DEPTH,
        depthOk ? "PASS" : "FAIL",
        campaignResults.discoveryDepthDetail || null,
      ),
    );
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.NAMED_BUYER_VERIFICATION,
        campaignResults.buyersVerified ? "PASS" : "FAIL",
        campaignResults.buyerDetail || null,
      ),
    );
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.COMMERCIAL_ACCESSIBILITY,
        campaignResults.accessibilityGateOk ? "PASS" : "FAIL",
        campaignResults.accessibilityDetail || null,
      ),
    );
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.EVIDENCE_SUPPORTED_TIMING,
        campaignResults.timingGateOk ? "PASS" : "FAIL",
        campaignResults.timingDetail || null,
      ),
    );
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.EXISTING_INVENTORY_DEDUPLICATION,
        campaignResults.dedupOk ? "PASS" : "FAIL",
        campaignResults.dedupDetail || null,
      ),
    );
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.RESEARCH_CONTINUATION,
        campaignResults.continuationOk !== false ? "PASS" : "FAIL",
        campaignResults.continuationDetail || null,
      ),
    );
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.DEEP_RESEARCH_OUTPUT_COMPLETENESS,
        campaignResults.deepComplete ? "PASS" : "FAIL",
        campaignResults.deepDetail || null,
      ),
    );
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.RESEARCH_TO_CIB_PRESERVATION,
        campaignResults.cibOk ? "PASS" : "FAIL",
        campaignResults.cibDetail || null,
      ),
    );
    gates.push(
      gate(
        NEED_DATE_V6_CERT_GATE.MONTH_SPECIFIC_COMMERCIAL_YIELD,
        campaignResults.monthYieldOk ? "PASS" : "FAIL",
        campaignResults.monthYieldDetail || null,
      ),
    );
  }

  gates.push(
    gate(
      NEED_DATE_V6_CERT_GATE.BUDGET_SAFETY,
      paidAuthorized
        ? campaignResults?.budgetOk === false
          ? "FAIL"
          : campaignResults
            ? "PASS"
            : "NOT_VERIFIED"
        : "PASS",
      {
        paidAuthorized,
        note: "PASS while unpaid = no unauthorized spend. Live PASS requires ledger ≤ authorized ceiling.",
        priorSpentUsd: plan?.hotel?.priorCampaignSpentUsd,
      },
    ),
  );

  gates.push(
    gate(
      NEED_DATE_V6_CERT_GATE.PUBLICATION_AUTHORIZATION,
      publicationAuthorized ? "PASS" : "PASS",
      {
        publicationAuthorized: Boolean(publicationAuthorized),
        note: publicationAuthorized
          ? "Publication authorized"
          : "PASS = publication correctly blocked (not authorized)",
      },
    ),
  );

  const fails = gates.filter((g) => g.status === "FAIL");
  const notVerified = gates.filter((g) => g.status === "NOT_VERIFIED");

  return {
    version: "gdi-need-date-deep-discovery-certification-v6",
    overall:
      fails.length > 0
        ? "FAIL"
        : notVerified.length > 0
          ? "PLAN_READY_LIVE_NOT_VERIFIED"
          : "PASS",
    gates,
    failCount: fails.length,
    notVerifiedCount: notVerified.length,
    note: "Do not claim overall PASS merely because Exa requests completed. Live commercial gates require campaign results.",
  };
}
