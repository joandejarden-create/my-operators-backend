/**
 * Operator Fit Engine v2 API — feature-flagged, read-only evaluation.
 * Does not overwrite legacy OAS scores or write Airtable.
 */

import {
  isOperatorFitEngineV2Enabled,
  isOperatorFitEngineV2ShadowEnabled,
  getOperatorFitEngineFlagState,
} from "../lib/operator-fit/feature-flag.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { fetchDealScoringContext } from "./my-deals.js";
import {
  loadActiveOperatorCandidatesForAlignment,
} from "../lib/operator-alignment-company-utils.js";
import {
  buildPrefillObjectFromNewBaseRows,
} from "./lib/operator-setup-new-base-read.js";

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

/**
 * GET /api/operator-fit/v2/flag
 */
export async function getOperatorFitV2Flag(req, res) {
  try {
    return json(res, 200, {
      success: true,
      ...getOperatorFitEngineFlagState(),
      targetListOperatorSupport: false,
      targetListNote:
        "Target List is brand-only; operator shortlist save remains disabled in Phase 1–2.",
    });
  } catch (err) {
    console.error("[operator-fit-v2] flag error", err);
    return json(res, 500, { success: false, error: "Flag status unavailable" });
  }
}

/**
 * GET /api/operator-fit/v2/:dealId/top5
 * Requires OPERATOR_FIT_ENGINE_V2=1 for owner-facing payload.
 * Shadow mode (OPERATOR_FIT_ENGINE_V2_SHADOW=1) returns diagnostics-oriented payload
 * even when primary flag is off (still no Airtable writes).
 */
export async function getOperatorFitV2Top5(req, res) {
  try {
    const dealId = String(req.params.dealId || "").trim();
    if (!dealId.startsWith("rec")) {
      return json(res, 400, { success: false, error: "Valid deal ID required" });
    }

    const enabled = isOperatorFitEngineV2Enabled();
    const shadow = isOperatorFitEngineV2ShadowEnabled();
    if (!enabled && !shadow) {
      return json(res, 404, {
        success: false,
        error: "Operator Fit Engine v2 is not enabled",
        flag: getOperatorFitEngineFlagState(),
        ownerMessage: "Feature unavailable because the flag is off.",
      });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return json(res, 500, { success: false, error: "Airtable credentials not configured" });
    }

    const ctx = await fetchDealScoringContext(baseId, apiKey, dealId);
    if (!ctx) {
      return json(res, 404, { success: false, error: "Deal not found" });
    }

    const { candidates } = await loadActiveOperatorCandidatesForAlignment();
    const operatorPrefills = [];
    for (const c of candidates || []) {
      try {
        const prefill =
          c.prefill ||
          buildPrefillObjectFromNewBaseRows(
            c.master,
            c.profile,
            c.platform,
            c.commercial,
            c.governance
          );
        operatorPrefills.push({
          operatorId: c.operatorId,
          companyName: c.companyName,
          prefill: {
            ...(prefill || {}),
            submission_status: "Active",
            companyName: c.companyName,
          },
        });
      } catch (err) {
        console.error("[operator-fit-v2] prefill failed", c.operatorId, err?.message || err);
      }
    }

    // Optional brand-managed candidates from preferred brands + registry-style hints on SI
    const si = ctx.siData || {};
    const preferred = [].concat(si["Preferred Brands"] || []).filter(Boolean);
    const brandManagedCandidates = [];
    const operatingModel = String(si["Operating Model"] || "");
    const mgmt = [].concat(si["Preferred Management Structure"] || []);
    const openToBrandManaged =
      /brand-managed|brand managed/i.test(operatingModel) ||
      mgmt.some((m) => /brand-managed|brand managed/i.test(String(m)));
    if (openToBrandManaged) {
      for (const brandName of preferred.slice(0, 3)) {
        const name = typeof brandName === "string" ? brandName : String(brandName);
        brandManagedCandidates.push({
          brandName: name,
          // Preference alone — not confirmed (enrichment founder 2.3)
          offersBrandManagement: false,
          offersBrandManagementConfirmed: false,
          markets: ctx.locationData?.Country ? [ctx.locationData.Country] : [],
          scales: ctx.locationData?.["Hotel Chain Scale"]
            ? [ctx.locationData["Hotel Chain Scale"]]
            : [],
        });
      }
    }

    const result = evaluateOperatorFitForDeal({
      dealId,
      dealFields: ctx.dealFields,
      locationData: ctx.locationData,
      mpData: ctx.mpData,
      siData: ctx.siData,
      operatorPrefills,
      brandManagedCandidates,
    });

    return json(res, 200, {
      success: true,
      ownerFacing: enabled,
      shadowOnly: !enabled && shadow,
      ...result,
      saveToTargetListEnabled: false,
    });
  } catch (err) {
    console.error("[operator-fit-v2] top5 error", err);
    return json(res, 500, {
      success: false,
      error: "Operator Fit evaluation failed",
      detail: process.env.NODE_ENV === "development" ? String(err?.message || err) : undefined,
    });
  }
}
