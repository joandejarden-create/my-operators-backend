/**
 * Customer evidence UI delivery integrity — DATA → API → ACTION → DRAWER.
 *
 * Principle: CUSTOMER_ACTION_MUST_DELIVER_GOVERNED_OUTPUT
 * Gates:
 *   POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY
 *   MISSING_EVIDENCE_UI_DELIVERY_INTEGRITY
 *   POSITIVE_EVIDENCE_ACTION_HAS_RENDERABLE_EVIDENCE
 */

import { getPublishedEvidenceResponse } from "../published-read-service.js";
import { loadPublishedReport } from "../published-snapshot.js";
import { filterPositiveEvidencePool } from "../customer/positive-evidence-v1.js";
import { filterMissingEvidencePool } from "../customer/missing-evidence-v1.js";
import { loadLatestCustomerPeriod, loadPropertyProfile } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";

export const CUSTOMER_ACTION_MUST_DELIVER_GOVERNED_OUTPUT =
  "CUSTOMER_ACTION_MUST_DELIVER_GOVERNED_OUTPUT";
export const POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY =
  "POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY";
export const MISSING_EVIDENCE_UI_DELIVERY_INTEGRITY =
  "MISSING_EVIDENCE_UI_DELIVERY_INTEGRITY";
export const POSITIVE_EVIDENCE_ACTION_HAS_RENDERABLE_EVIDENCE =
  "POSITIVE_EVIDENCE_ACTION_HAS_RENDERABLE_EVIDENCE";

export const DEFECT_FALSE_POSITIVE_EVIDENCE_ACTION = "FALSE_POSITIVE_EVIDENCE_ACTION";
export const DEFECT_EMPTY_DRAWER_WITH_ELIGIBLE_EVIDENCE =
  "EMPTY_DRAWER_WITH_ELIGIBLE_EVIDENCE";
export const DEFECT_MISSING_COUNT_UI_API_MISMATCH = "MISSING_COUNT_UI_API_MISMATCH";
export const DEFECT_STALE_PUBLISHED_EVIDENCE_INDEX_PATH =
  "STALE_PUBLISHED_EVIDENCE_INDEX_PATH";

/** Gold case from founder screenshot-class failure. */
export const CAMBRIDGE_GEMINI_POSITIVE_GOLD = Object.freeze({
  propertyId: "adp_cambridge_beaches_bermuda",
  provider: "gemini",
  surface: "provider_presence",
});

/**
 * Simulate Provider Presence / Demand Territory evidence click params → API.
 */
export async function runEvidenceUiDeliveryIntegrity(propertyId) {
  const defects = [];
  const actions = [];
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestCustomerPeriod(propertyId);
  const published = loadPublishedReport(propertyId);
  const scenarios = buildScenarioUniverse(profile);

  if (!profile || !period || !published) {
    return {
      gate: POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY,
      status: "FAIL",
      defects: [{ code: "NO_PUBLISHED_PAYLOAD", detail: propertyId }],
      actions: [],
    };
  }

  // --- Provider Presence actions (Overall provider scope — no territory) ---
  for (const p of published.evidence?.providers || []) {
    const denom = p.comparable != null ? p.comparable : p.total;
    const mentioned = Number(p.mentioned) || 0;
    const missing = Math.max(0, Number(denom) - mentioned);
    const unavailable = p.presenceUnavailable === true || p.presence == null;

    const showPositive = !unavailable && mentioned > 0;
    const showMissing = !unavailable && missing > 0;

    const { pool: posPool } = filterPositiveEvidencePool({
      period,
      scenarios,
      propertyProfile: profile,
      provider: p.provider,
    });
    const { pool: missPool } = filterMissingEvidencePool({
      period,
      scenarios,
      provider: p.provider,
    });

    if (showPositive) {
      const api = await getPublishedEvidenceResponse(propertyId, {
        type: "present",
        mode: "positive",
        provider: p.provider,
        limit: 5,
        offset: 0,
      });
      const rendered = api.evidence || [];
      const row = {
        surface: "provider_presence",
        filter: "provider",
        provider: p.provider,
        visibleMentioned: mentioned,
        eligiblePositive: posPool.length,
        apiCount: rendered.length,
        apiTotal: api.totalQualifying ?? api.total,
        apiSource: api.source,
        actionShown: true,
        pass: rendered.length > 0 && api.source !== "published_snapshot",
      };
      actions.push({ ...row, type: "positive" });

      if (posPool.length > 0 && rendered.length === 0) {
        defects.push({
          code: DEFECT_EMPTY_DRAWER_WITH_ELIGIBLE_EVIDENCE,
          gate: POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY,
          detail: `${propertyId} provider=${p.provider} eligible=${posPool.length} rendered=0 source=${api.source}`,
        });
      }
      if (posPool.length === 0 && showPositive) {
        defects.push({
          code: DEFECT_FALSE_POSITIVE_EVIDENCE_ACTION,
          gate: POSITIVE_EVIDENCE_ACTION_HAS_RENDERABLE_EVIDENCE,
          detail: `${propertyId} provider=${p.provider} UI shows View examples but eligible=0`,
        });
      }
      if (api.source === "published_snapshot" && showPositive) {
        defects.push({
          code: DEFECT_STALE_PUBLISHED_EVIDENCE_INDEX_PATH,
          gate: POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY,
          detail: `${propertyId} provider=${p.provider} still on published_snapshot path`,
        });
      }
      const expectedSample = Math.min(5, posPool.length);
      if (posPool.length > 0 && rendered.length !== expectedSample && rendered.length > 0) {
        // allow fewer only if pool < 5 (already handled by expectedSample)
        if (rendered.length < expectedSample) {
          defects.push({
            code: DEFECT_EMPTY_DRAWER_WITH_ELIGIBLE_EVIDENCE,
            gate: POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY,
            detail: `${propertyId} provider=${p.provider} expected sample ${expectedSample} got ${rendered.length}`,
          });
        }
      }
    }

    if (showMissing) {
      const api = await getPublishedEvidenceResponse(propertyId, {
        type: "missing",
        mode: "missing",
        provider: p.provider,
        limit: 100,
        offset: 0,
      });
      const total = api.totalQualifying ?? api.total ?? 0;
      const row = {
        surface: "provider_presence",
        filter: "provider",
        provider: p.provider,
        visibleMissing: missing,
        eligibleMissing: missPool.length,
        apiTotal: total,
        apiSource: api.source,
        actionShown: true,
        pass: total === missPool.length && total === missing && api.source === "missing_evidence_v1",
      };
      actions.push({ ...row, type: "missing" });

      if (total !== missPool.length || total !== missing) {
        defects.push({
          code: DEFECT_MISSING_COUNT_UI_API_MISMATCH,
          gate: MISSING_EVIDENCE_UI_DELIVERY_INTEGRITY,
          detail: `${propertyId} provider=${p.provider} visible=${missing} eligible=${missPool.length} api=${total}`,
        });
      }
      if (api.source === "published_snapshot") {
        defects.push({
          code: DEFECT_STALE_PUBLISHED_EVIDENCE_INDEX_PATH,
          gate: MISSING_EVIDENCE_UI_DELIVERY_INTEGRITY,
          detail: `${propertyId} provider=${p.provider} missing still on published_snapshot`,
        });
      }
    }
  }

  // --- Demand Territory actions ---
  for (const [intent, data] of Object.entries(published.demandCapture?.byIntent || {})) {
    const missingScenarios = Number(data.total) - Number(data.captured);
    const showPositive = Number(data.captured) > 0;
    const showMissing = missingScenarios > 0;

    const { pool: posPool } = filterPositiveEvidencePool({
      period,
      scenarios,
      propertyProfile: profile,
      intent,
    });
    const { pool: missPool } = filterMissingEvidencePool({ period, scenarios, intent });

    if (showPositive) {
      const api = await getPublishedEvidenceResponse(propertyId, {
        type: "present",
        mode: "positive",
        intent,
        limit: 5,
      });
      const rendered = api.evidence || [];
      actions.push({
        type: "positive",
        surface: "demand_territory",
        filter: "intent",
        intent,
        eligiblePositive: posPool.length,
        apiCount: rendered.length,
        apiSource: api.source,
        pass: rendered.length > 0 && api.source === "positive_evidence_v1",
      });
      if (posPool.length > 0 && rendered.length === 0) {
        defects.push({
          code: DEFECT_EMPTY_DRAWER_WITH_ELIGIBLE_EVIDENCE,
          gate: POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY,
          detail: `${propertyId} intent=${intent} eligible=${posPool.length} rendered=0`,
        });
      }
    }

    if (showMissing) {
      const api = await getPublishedEvidenceResponse(propertyId, {
        type: "missing",
        mode: "missing",
        intent,
        limit: 100,
      });
      const total = api.totalQualifying ?? api.total ?? 0;
      actions.push({
        type: "missing",
        surface: "demand_territory",
        filter: "intent",
        intent,
        visibleMissingScenarios: missingScenarios,
        eligibleMissingObs: missPool.length,
        apiTotal: total,
        apiSource: api.source,
        // Territory "Missing" column is scenario-grain; drawer is observation-grain.
        pass: total === missPool.length && api.source === "missing_evidence_v1",
      });
      if (total !== missPool.length) {
        defects.push({
          code: DEFECT_MISSING_COUNT_UI_API_MISMATCH,
          gate: MISSING_EVIDENCE_UI_DELIVERY_INTEGRITY,
          detail: `${propertyId} intent=${intent} eligibleObs=${missPool.length} api=${total}`,
        });
      }
    }
  }

  return {
    principle: CUSTOMER_ACTION_MUST_DELIVER_GOVERNED_OUTPUT,
    gates: [
      POSITIVE_EVIDENCE_UI_DELIVERY_INTEGRITY,
      MISSING_EVIDENCE_UI_DELIVERY_INTEGRITY,
      POSITIVE_EVIDENCE_ACTION_HAS_RENDERABLE_EVIDENCE,
    ],
    propertyId,
    periodId: period.periodId,
    status: defects.length ? "FAIL" : "PASS",
    defects,
    actions,
  };
}

export async function runCambridgeGeminiPositiveGoldCase() {
  const { propertyId, provider } = CAMBRIDGE_GEMINI_POSITIVE_GOLD;
  const api = await getPublishedEvidenceResponse(propertyId, {
    type: "present",
    mode: "positive",
    provider,
    limit: 5,
  });
  const rendered = api.evidence || [];
  const failEmptyCopy =
    rendered.length === 0
      ? "Evidence unavailable for this observation. No valid provider evidence was captured for this filter."
      : null;
  return {
    gold: CAMBRIDGE_GEMINI_POSITIVE_GOLD,
    status: rendered.length > 0 && api.source === "positive_evidence_v1" ? "PASS" : "FAIL",
    apiSource: api.source,
    renderedCount: rendered.length,
    totalQualifying: api.totalQualifying,
    failEmptyCopy,
  };
}
