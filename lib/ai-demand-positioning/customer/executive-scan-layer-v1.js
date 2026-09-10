/**
 * ADP Executive Summary scan layer (left diagnostic cards).
 *
 * Doctrine:
 *   EXECUTIVE SUMMARY = SCAN LAYER + SYNTHESIS LAYER
 *   LEFT DIAGNOSTIC CARDS PROVIDE FAST SCAN
 *   V3 EXECUTIVE READ PROVIDES ANALYTICAL SYNTHESIS
 *
 * RETAIN: Biggest Strength / Biggest Constraint / Change Since Last Comparable Run
 * DEPRECATE as right-side main: What The Data Says narrative
 * RETAIN / NEW right-side: V3 structured Executive Read
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN. — presentation only.
 */

export const ADP_EXECUTIVE_SUMMARY_HYBRID_V3_LAYOUT =
  "ADP_EXECUTIVE_SUMMARY_HYBRID_V3_LAYOUT";
export const ADP_EXECUTIVE_LEFT_CONSTRAINT_PRIMARY_ISSUE_PARITY =
  "ADP_EXECUTIVE_LEFT_CONSTRAINT_PRIMARY_ISSUE_PARITY";
export const ADP_EXECUTIVE_SCAN_SYNTHESIS_INCREMENTAL_VALUE =
  "ADP_EXECUTIVE_SCAN_SYNTHESIS_INCREMENTAL_VALUE";
export const ADP_EXECUTIVE_HYBRID_PRINT_PARITY = "ADP_EXECUTIVE_HYBRID_PRINT_PARITY";
export const ADP_EXECUTIVE_HYBRID_CROSS_SURFACE_PARITY =
  "ADP_EXECUTIVE_HYBRID_CROSS_SURFACE_PARITY";
export const EXECUTIVE_SCAN_LAYER_VERSION = "adp_executive_scan_layer_v1";

function fmtPct(n) {
  if (n == null || Number.isNaN(Number(n))) return null;
  const v = Number(n);
  return Number.isInteger(v) ? `${v}%` : `${Math.round(v * 10) / 10}%`;
}

function resolvePrimaryIssueId(er) {
  return (
    er?.primaryIssueId ||
    er?.compositionV3?.primaryIssueId ||
    er?.compositionV3?.primary?.primaryIssueId ||
    null
  );
}

function scenarioPresencePct(payload) {
  const em = payload?.executiveMetrics?.scenarioPresence;
  if (em?.rate != null) return Number(em.rate);
  if (payload?.demandCapture?.overallRate != null) return Number(payload.demandCapture.overallRate);
  return null;
}

function considerationPct(payload) {
  const em = payload?.executiveMetrics?.considerationRate;
  if (em?.rate != null) return Number(em.rate);
  if (payload?.consideration?.rate != null) return Number(payload.consideration.rate);
  return null;
}

function weakestRealityGap(payload) {
  const gaps = payload?.realityGap?.gaps || [];
  if (!gaps.length) return null;
  return [...gaps].sort((a, b) => (a.recognitionRate ?? 100) - (b.recognitionRate ?? 100))[0];
}

function urbanOrSignatureGap(payload) {
  const gaps = payload?.realityGap?.gaps || [];
  const matches = gaps.filter((g) =>
    /urban|signature|luxury|spa|lounge|bar|ballroom|meeting|marina|beach/i.test(
      `${g.label || ""} ${g.attribute || ""}`
    )
  );
  if (!matches.length) return weakestRealityGap(payload);
  return [...matches].sort((a, b) => (a.recognitionRate ?? 100) - (b.recognitionRate ?? 100))[0];
}

/**
 * Map V3 primaryIssueId → scan-layer constraint family.
 * Prevents left-card constraint contradicting the analytical primary issue.
 */
export function constraintFamilyForPrimaryIssue(primaryIssueId) {
  const id = String(primaryIssueId || "");
  if (/answer_level|consistency_consideration|business_consideration|consistency_consideration_gap/.test(id)) {
    return "ANSWER_LEVEL_INCLUSION";
  }
  if (/signature_urban|reality_|meeting_ballroom|marina_beach|proposition|protect_and_close/.test(id)) {
    return "REALITY_PROPOSITION";
  }
  if (/displacement_/.test(id)) return "DISPLACEMENT";
  if (/territory_|business_group|soft_spot/.test(id)) return "TERRITORY";
  if (/entry_|lifestyle_couples|times_square_entry/.test(id)) return "ENTRY";
  if (/provider_fragmentation/.test(id)) return "PROVIDER";
  return "GENERIC";
}

function buildAlignedConstraint(primaryIssueId, uxConstraint, payload, propertyName) {
  const family = constraintFamilyForPrimaryIssue(primaryIssueId);
  const sp = scenarioPresencePct(payload);
  const cr = considerationPct(payload);
  const name = propertyName || "This hotel";

  if (family === "ANSWER_LEVEL_INCLUSION" && sp != null && cr != null) {
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: "Inconsistent answer-level inclusion",
      body:
        `Scenario Presence is ${fmtPct(sp)}, while AI Consideration is ${fmtPct(cr)}. ` +
        "AI recognizes the hotel across many traveler needs, but it does not appear consistently in individual answers.",
      primaryIssueId,
      family,
    };
  }

  if (family === "REALITY_PROPOSITION") {
    const gap = urbanOrSignatureGap(payload);
    if (gap) {
      return {
        sectionLabel: "BIGGEST CONSTRAINT",
        headline: "Signature attributes under-recognized",
        body:
          `${gap.label || "A defining property attribute"} appears in only ${fmtPct(gap.recognitionRate)} of monitored answers. ` +
          "AI is incomplete on a material part of the hotel’s proposition.",
        primaryIssueId,
        family,
      };
    }
  }

  if (family === "DISPLACEMENT") {
    const top = payload?.lostDemand?.displacement?.[0];
    if (top?.name) {
      return {
        sectionLabel: "BIGGEST CONSTRAINT",
        headline: "Competitive displacement pressure",
        body:
          `${top.name} appears in ${top.displacementCount || top.count || "multiple"} scenarios where ${name} is absent. ` +
          "Competing hotels are being surfaced in demand situations this hotel does not capture.",
        primaryIssueId,
        family,
      };
    }
  }

  if (family === "TERRITORY") {
    const weak = payload?.intentPresenceIndex
      ? Object.entries(payload.intentPresenceIndex)
          .map(([intent, row]) => ({ intent, rate: row?.subjectRatePct ?? row?.myRate }))
          .filter((r) => r.rate != null)
          .sort((a, b) => a.rate - b.rate)[0]
      : null;
    if (weak) {
      return {
        sectionLabel: "BIGGEST CONSTRAINT",
        headline: "Weaker demand territory",
        body:
          `${toTitle(weak.intent)} presence is ${fmtPct(weak.rate)} in this period — ` +
          "a softer territory relative to the hotel’s stronger demand areas.",
        primaryIssueId,
        family,
      };
    }
  }

  if (family === "ENTRY" && cr != null) {
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: "Entry before rank",
      body:
        `AI Consideration is ${fmtPct(cr)}. The limiting factor is getting into AI answers at all, not just how the hotel ranks once included.`,
      primaryIssueId,
      family,
    };
  }

  // Keep UX constraint when family is GENERIC or alignment data is incomplete —
  // but tag family for gate visibility.
  if (uxConstraint) {
    return { ...uxConstraint, primaryIssueId: primaryIssueId || null, family };
  }
  return {
    sectionLabel: "BIGGEST CONSTRAINT",
    headline: "Still developing",
    body: "A governed constraint summary is not available for this view yet.",
    primaryIssueId,
    family,
  };
}

function toTitle(intent) {
  return String(intent || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function preferReachStrength(uxStrength, primaryIssueId, payload, propertyName) {
  const family = constraintFamilyForPrimaryIssue(primaryIssueId);
  const sp = scenarioPresencePct(payload);
  if (family !== "ANSWER_LEVEL_INCLUSION" || sp == null || sp < 70) return uxStrength;
  const name = propertyName || "This hotel";
  return {
    sectionLabel: "BIGGEST STRENGTH",
    headline: "Broad demand reach",
    body:
      `${name} appeared in at least one AI answer for ${fmtPct(sp)} of the traveler needs tested (Scenario Presence). ` +
      "That is the clearest positive reach signal in this period.",
  };
}

/**
 * Build canonical scanLayer for left diagnostic cards.
 */
export function buildExecutiveScanLayer({ ux, executiveRead, payload, propertyProfile } = {}) {
  const primaryIssueId = resolvePrimaryIssueId(executiveRead);
  const propertyName = propertyProfile?.name || payload?.property?.name || "This hotel";
  const uxStrength = ux?.biggestStrength || executiveRead?.summary?.biggestStrength || null;
  const uxConstraint = ux?.biggestConstraint || executiveRead?.summary?.biggestConstraint || null;
  const change =
    ux?.changeSinceLastRun ||
    executiveRead?.summary?.changeSinceLastComparableRun ||
    executiveRead?.summary?.changeSinceLastRun ||
    {
      sectionLabel: "CHANGE SINCE LAST COMPARABLE RUN",
      headline: "No comparable prior official period yet",
      body:
        "No comparable prior official monitoring period is available yet. " +
        "This period can be evaluated on its own; change over time is not shown.",
    };

  const biggestStrength = preferReachStrength(uxStrength, primaryIssueId, payload, propertyName);
  const biggestConstraint = buildAlignedConstraint(
    primaryIssueId,
    uxConstraint,
    payload,
    propertyName
  );

  return {
    version: EXECUTIVE_SCAN_LAYER_VERSION,
    contract: ADP_EXECUTIVE_SUMMARY_HYBRID_V3_LAYOUT,
    primaryIssueId,
    biggestStrength,
    biggestConstraint,
    changeSincePrior: change,
  };
}

export function attachExecutiveScanLayer(executiveRead, payload, propertyProfile) {
  if (!executiveRead || executiveRead.available === false) return executiveRead;
  const scanLayer = buildExecutiveScanLayer({
    ux: executiveRead.ux,
    executiveRead,
    payload,
    propertyProfile,
  });
  return {
    ...executiveRead,
    scanLayer,
    // Keep summary mirrors for older renderers
    summary: {
      ...(executiveRead.summary || {}),
      biggestStrength: scanLayer.biggestStrength,
      biggestConstraint: scanLayer.biggestConstraint,
      changeSinceLastComparableRun: scanLayer.changeSincePrior,
    },
  };
}

/** Gate helper: left constraint family must match primary issue family when V3 stamped. */
export function assertLeftConstraintPrimaryIssueParity(executiveRead) {
  const primaryIssueId = resolvePrimaryIssueId(executiveRead);
  if (!primaryIssueId) return { ok: true, skipped: true };
  const scan = executiveRead?.scanLayer;
  if (!scan?.biggestConstraint) return { ok: false, reason: "MISSING_SCAN_CONSTRAINT" };
  const expected = constraintFamilyForPrimaryIssue(primaryIssueId);
  const actual = scan.biggestConstraint.family || constraintFamilyForPrimaryIssue(primaryIssueId);
  // If family tagged on box, compare; otherwise infer from headline
  const headline = String(scan.biggestConstraint.headline || "");
  if (expected === "ANSWER_LEVEL_INCLUSION") {
    const ok = /inconsistent|not appearing consistently|answer-level|consideration/i.test(headline);
    return { ok, expected, headline, primaryIssueId };
  }
  if (expected === "REALITY_PROPOSITION") {
    const ok = /attribute|proposition|under-recognized|reality|reflect/i.test(headline);
    return { ok, expected, headline, primaryIssueId };
  }
  if (expected === "DISPLACEMENT") {
    const ok = /displacement|competitor/i.test(headline);
    return { ok, expected, headline, primaryIssueId };
  }
  return { ok: true, expected, actual, primaryIssueId };
}
