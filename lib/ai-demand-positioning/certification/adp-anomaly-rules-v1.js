/**
 * Global ADP anomaly detection + explanation helpers (Parts 14, 21–23, 34).
 * Surprising results are allowed; unexplained results are not certifiable.
 */

import { PROVIDERS } from "../data-model.js";
import { evaluateAdpComparability, ADP_COMPARABILITY_OUTCOMES } from "./adp-comparability-engine-v1.js";
import { scenarioIdsFromPeriod } from "./adp-scenario-universe-contract-v1.js";

export const ADP_ANOMALY_RULES_VERSION = "adp_anomaly_rules_v1";

function pct(n, d) {
  if (!d) return null;
  return (n / d) * 100;
}

function relativeChange(current, prior) {
  if (current == null || prior == null || prior === 0) return null;
  return (current - prior) / Math.abs(prior);
}

/**
 * Provider zero-presence forensic (Part 14).
 */
export function forensicProviderZeroPresence(period, provider) {
  const rows = (period?.observations || []).filter((o) => o.provider === provider);
  const mentions = rows.filter((o) => o.mentioned === true || o.subjectMentioned === true);
  if (mentions.length > 0) {
    return { provider, zeroPresence: false, explanation: null };
  }

  const successful = rows.filter(
    (o) => !o.error && !o.providerError && (o.rawResponse || o.parsed || o.responseText)
  );
  const nonempty = successful.filter((o) => {
    const text = o.rawResponse || o.responseText || o.parsed?.text || "";
    return String(text).trim().length > 20;
  });
  const timedOut = rows.filter((o) => o.timeout || /timeout/i.test(String(o.error || "")));
  const rateLimited = rows.filter((o) =>
    /rate.?limit|429|quota/i.test(String(o.error || o.httpStatus || ""))
  );
  const refusals = nonempty.filter((o) =>
    /i can'?t|cannot assist|unable to provide/i.test(
      String(o.rawResponse || o.responseText || "").slice(0, 400)
    )
  );

  const checks = {
    providerExecutionSuccessful: successful.length > 0,
    responsesNonempty: nonempty.length > 0,
    entityParserLikelyFunctioning: rows.some((o) => o.parsed != null || o.mentioned != null),
    aliasMatchingLikelyFunctioning: null, // filled by caller via canary
    wrongHotelConfusion: null,
    rateLimitOrRefusalPattern: rateLimited.length > 0 || refusals.length > 0,
    timedOutCount: timedOut.length,
    attempted: rows.length,
    successful: successful.length,
    nonempty: nonempty.length,
  };

  let explanationClass = "TRUE_ABSENCE_CANDIDATE";
  if (!checks.providerExecutionSuccessful) explanationClass = "PROVIDER_EXECUTION_FAILURE";
  else if (!checks.responsesNonempty) explanationClass = "EMPTY_RESPONSES";
  else if (checks.rateLimitOrRefusalPattern) explanationClass = "RATE_LIMIT_OR_REFUSAL";
  else if (!checks.entityParserLikelyFunctioning) explanationClass = "PARSER_SUSPECT";

  return {
    provider,
    zeroPresence: true,
    explanationClass,
    checks,
    unexplained: explanationClass === "TRUE_ABSENCE_CANDIDATE",
    note:
      explanationClass === "TRUE_ABSENCE_CANDIDATE"
        ? "Zero may be valid but requires identity/alias canary confirmation before CERTIFIED."
        : `Zero presence explained as ${explanationClass}`,
  };
}

/**
 * Cross-metric consistency (Part 23).
 */
export function auditCrossMetricConsistency(metrics = {}) {
  const hardFailures = [];
  const {
    scenarioPresence,
    considerationNumerator,
    considerationDenominator,
    top1Rate,
    top3Rate,
    rankEligibleResponses,
    recognizedAttributes,
    trackedAttributes,
  } = metrics;

  if (scenarioPresence != null && scenarioPresence > 100 + 1e-6) {
    hardFailures.push({ code: "SCENARIO_PRESENCE_GT_100", value: scenarioPresence });
  }
  if (
    considerationNumerator != null &&
    considerationDenominator != null &&
    considerationNumerator > considerationDenominator
  ) {
    hardFailures.push({
      code: "CONSIDERATION_NUMERATOR_GT_DENOMINATOR",
      numerator: considerationNumerator,
      denominator: considerationDenominator,
    });
  }
  if ((top1Rate != null || top3Rate != null) && !(rankEligibleResponses > 0)) {
    if ((top1Rate || 0) > 0 || (top3Rate || 0) > 0) {
      hardFailures.push({ code: "RANK_RATE_WITHOUT_RANK_ELIGIBLE_RESPONSES" });
    }
  }
  if (
    recognizedAttributes != null &&
    trackedAttributes != null &&
    recognizedAttributes > trackedAttributes
  ) {
    hardFailures.push({
      code: "RECOGNIZED_ATTRIBUTES_GT_TRACKED",
      recognizedAttributes,
      trackedAttributes,
    });
  }
  return { hardFailures };
}

/**
 * Extreme / new-hotel anomaly detection (Parts 21–22).
 */
export function detectAdpAnomalies({
  period,
  priorPeriod = null,
  propertyProfile = null,
  recomputed = null,
  stored = null,
  identityCanary = null,
  isFirstComparableRun = false,
} = {}) {
  const reviewFlags = [];
  const hardFailures = [];
  const warnings = [];
  const explanations = [];

  const scenarioCount = scenarioIdsFromPeriod(period).length || period?.scenarioCount || 0;

  // Provider zero presence
  for (const provider of PROVIDERS) {
    const rows = (period?.observations || []).filter((o) => o.provider === provider);
    if (!rows.length) continue;
    const mentions = rows.filter((o) => o.mentioned === true || o.subjectMentioned === true);
    if (mentions.length === 0 && rows.length >= Math.max(5, Math.floor(scenarioCount * 0.5))) {
      const forensic = forensicProviderZeroPresence(period, provider);
      if (identityCanary) {
        forensic.checks.aliasMatchingLikelyFunctioning = identityCanary.pass;
      }
      explanations.push(
        explainAdpAnomaly({
          type: "PROVIDER_ZERO_PRESENCE",
          provider,
          forensic,
        })
      );
      if (forensic.unexplained && identityCanary && !identityCanary.pass) {
        hardFailures.push({
          code: "ZERO_PRESENCE_WITH_IDENTITY_CANARY_FAIL",
          provider,
        });
      } else if (
        forensic.checks?.providerExecutionSuccessful &&
        forensic.checks?.responsesNonempty &&
        identityCanary?.pass &&
        forensic.explanationClass === "TRUE_ABSENCE_CANDIDATE"
      ) {
        // Confirmed real zero after identity + execution forensic — surprising but certifiable.
        warnings.push({
          code: "PROVIDER_ZERO_PRESENCE_CONFIRMED_REAL",
          severity: "DISCLOSURE",
          provider,
          forensic,
        });
      } else {
        reviewFlags.push({
          code: "PROVIDER_ZERO_PRESENCE",
          provider,
          forensic,
        });
      }
    }
  }

  // Raw vs stored mismatch handled by caller; soft note here
  if (recomputed && stored) {
    for (const key of Object.keys(recomputed)) {
      if (stored[key] == null || recomputed[key] == null) continue;
      if (Number(stored[key]) !== Number(recomputed[key])) {
        // integer-derived exactness
        if (Number.isInteger(Number(recomputed[key])) && Number.isInteger(Number(stored[key]))) {
          hardFailures.push({
            code: "RAW_STORED_METRIC_MISMATCH",
            metric: key,
            stored: stored[key],
            recomputed: recomputed[key],
          });
        }
      }
    }
  }

  if (priorPeriod) {
    const comp = evaluateAdpComparability(period, priorPeriod);
    if (comp.outcome === ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE) {
      reviewFlags.push({
        code: "PRIOR_NOT_FORMALLY_COMPARABLE",
        comparability: comp.outcome,
        reasons: comp.reasons,
      });
      explanations.push(
        explainAdpAnomaly({
          type: "NOT_COMPARABLE",
          reasons: comp.reasons,
          scenarioCountA: comp.scenarioCountA,
          scenarioCountB: comp.scenarioCountB,
        })
      );
    } else {
      const curCons = stored?.aiConsideration ?? recomputed?.aiConsideration;
      const priCons =
        priorPeriod?.summaryMetrics?.aiConsideration ??
        priorPeriod?.executiveMetrics?.considerationRate?.value;
      const rel = relativeChange(curCons, priCons);
      if (rel != null && Math.abs(rel) > 0.5) {
        reviewFlags.push({
          code: "CONSIDERATION_RELATIVE_SHIFT_GT_50PCT",
          current: curCons,
          prior: priCons,
          relativeChange: rel,
        });
        explanations.push(
          explainAdpAnomaly({
            type: "CONSIDERATION_SHIFT",
            current: curCons,
            prior: priCons,
            relativeChange: rel,
            scenarioDelta: comp.scenarioCountA - comp.scenarioCountB,
          })
        );
      }

      const curSc = scenarioIdsFromPeriod(period).length;
      const priSc = scenarioIdsFromPeriod(priorPeriod).length;
      if (priSc > 0 && Math.abs(curSc - priSc) / priSc > 0.15) {
        reviewFlags.push({
          code: "SCENARIO_COUNT_CHANGE",
          current: curSc,
          prior: priSc,
        });
      }
    }
  }

  if (isFirstComparableRun || !priorPeriod) {
    if (scenarioCount > 0) {
      const allZero = PROVIDERS.every((p) => {
        const rows = (period?.observations || []).filter((o) => o.provider === p);
        if (!rows.length) return true;
        return !rows.some((o) => o.mentioned === true);
      });
      if (allZero) {
        reviewFlags.push({ code: "NEW_HOTEL_ALL_PROVIDERS_ZERO" });
      }
    }
    if (identityCanary && !identityCanary.pass) {
      hardFailures.push({ code: "NEW_HOTEL_IDENTITY_CANARY_FAIL", failed: identityCanary.failed });
    }
  }

  void propertyProfile;
  void pct;

  return {
    rulesVersion: ADP_ANOMALY_RULES_VERSION,
    hardFailures,
    reviewFlags,
    warnings,
    explanations,
  };
}

/**
 * Internal QA explanation helper (Part 34).
 */
export function explainAdpAnomaly(input = {}) {
  const type = input.type;
  if (type === "CONSIDERATION_SHIFT") {
    if (input.scenarioDelta && input.scenarioDelta !== 0) {
      return `Consideration moved (${input.prior} → ${input.current}) alongside a scenario-universe delta of ${input.scenarioDelta}.`;
    }
    return `Consideration changed from ${input.prior} to ${input.current} (relative ${(input.relativeChange * 100).toFixed(1)}%).`;
  }
  if (type === "PROVIDER_ZERO_PRESENCE") {
    return `Provider ${input.provider} returned 0 valid mentions (${input.forensic?.explanationClass || "unexplained"}).`;
  }
  if (type === "NOT_COMPARABLE") {
    return `Peer/period comparison not formally comparable (${(input.reasons || []).join(", ") || "measurement-universe differences"}). Scenario counts: ${input.scenarioCountA} vs ${input.scenarioCountB}.`;
  }
  if (type === "ALIAS_COVERAGE") {
    return "Alias coverage changed; identity canary results differ from prior period.";
  }
  if (type === "OWNED_SOURCE_MAPPING") {
    return "Owned source mapping changed or owned-source share moved unexpectedly.";
  }
  return input.message || `Anomaly: ${type || "UNKNOWN"}`;
}
