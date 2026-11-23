/**
 * Canonical Full E2E research economics ledger V1.
 *
 * Captures honesty metrics for a run. Does not invent spend or commercial value.
 * Zero-yield research must be reported honestly.
 */

export const GDI_RESEARCH_ECONOMICS_LEDGER_V1 =
  "gdi-research-economics-ledger-v1";

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Build economics summary from chain artifacts already produced by Slice 1.
 */
export function buildResearchEconomicsLedger({
  discoveryYieldCounts = {},
  retainedCandidates = [],
  preDeepScreening = null,
  selectionRegister = null,
  processed = [],
  segmentCoverage = null,
  spentUsd = 0,
  hardCeilingUsd = null,
} = {}) {
  const discovered = num(retainedCandidates.length);
  const liveNew = num(discoveryYieldCounts.LIVE_NEW);
  const historical = num(discoveryYieldCounts.HISTORICAL_REPLAY);
  const fixture = num(discoveryYieldCounts.FIXTURE);
  const inventoryReopen = num(discoveryYieldCounts.INVENTORY_REOPEN);
  const previouslyKnown = historical + inventoryReopen;

  const considered =
    num(preDeepScreening?.totalCandidates) ||
    num(selectionRegister?.register?.length) ||
    discovered;
  const selected = num(
    selectionRegister?.selectedCount != null
      ? selectionRegister.selectedCount
      : processed.length,
  );
  const deferred = num(selectionRegister?.deferredCount);
  const skippedReasons = {};
  for (const row of selectionRegister?.register || []) {
    if (row.selectionDecision && String(row.selectionDecision).startsWith("SELECTED")) {
      continue;
    }
    const reason = text(row.decisionReason || row.selectionDecision) || "UNKNOWN";
    skippedReasons[reason] = (skippedReasons[reason] || 0) + 1;
  }

  const contactsGained = processed.filter(
    (p) => text(p.primaryContactName) || num(p.salesReadyContacts) > 0,
  ).length;
  // Intermediate research-complete flags must not count as actionable when final
  // disposition is EXCLUDE (Portfolio Cert V1.1 Bethesda scorecard contradiction).
  const actionable = processed.filter((p) => {
    const disp = text(p.disposition || p.internalDisposition).toUpperCase();
    if (/EXCLUDE|INELIGIBLE/.test(disp)) return false;
    return (
      p.commercialResearchComplete === true ||
      disp.includes("SALES") ||
      p.publicationReviewReady === true ||
      /WATCHLIST|RESEARCH_CONTINUATION|ACCOUNT_DEVELOPMENT|PRE_SOURCING|ACTIVE/.test(
        disp,
      )
    );
  }).length;
  const salesReadyCanonical = processed.filter((p) => {
    const disp = text(p.disposition || p.internalDisposition).toUpperCase();
    return (
      p.salesReady === true ||
      /^(SALES_READY|ACTIVE_OPPORTUNITY)$/.test(disp) ||
      disp === "ACTIVE_BUYING_OPPORTUNITIES"
    );
  }).length;
  const researchContinuationCount = processed.filter(
    (p) =>
      p.researchContinuationRequired === true ||
      text(p.commercialEligibilityStatus).toUpperCase() === "RESEARCH_CONTINUATION" ||
      /WATCHLIST|RESEARCH_CONTINUATION/.test(
        text(p.disposition || p.internalDisposition).toUpperCase(),
      ),
  ).length;
  const stoppedOrMonitored = processed.filter(
    (p) =>
      p.skipped === true ||
      p.blocked === true ||
      p.intakeBlocksSalesReady === true ||
      /MONITOR|TOO_EARLY|SPECULATIVE|COMPETITOR/i.test(
        text(p.commercialDispositionAtIntake || p.disposition),
      ),
  ).length;
  const synthetic = processed.filter((p) => p.corpusSynthetic === true).length;

  const cost = num(spentUsd);
  const incrementalActionable = actionable;
  const costPerActionable =
    incrementalActionable > 0 ? Number((cost / incrementalActionable).toFixed(6)) : null;

  const zeroYield =
    selected === 0 ||
    (liveNew === 0 && actionable === 0 && contactsGained === 0);

  return {
    version: GDI_RESEARCH_ECONOMICS_LEDGER_V1,
    at: new Date().toISOString(),
    candidatesDiscovered: discovered,
    newUniqueCandidates: liveNew,
    previouslyKnownCandidates: previouslyKnown,
    fixtureCandidates: fixture,
    candidatesConsideredForDeeperResearch: considered,
    candidatesSelected: selected,
    candidatesSkipped: deferred,
    skipReasons: skippedReasons,
    researchCostUsdTotal: cost,
    hardCeilingUsd:
      hardCeilingUsd == null || !Number.isFinite(Number(hardCeilingUsd))
        ? null
        : Number(hardCeilingUsd),
    researchCostUsdPerSelected:
      selected > 0 ? Number((cost / selected).toFixed(6)) : null,
    researchCostUsdPerIncrementalActionable: costPerActionable,
    verifiedCommercialContactsGained: contactsGained,
    opportunitiesBecomingActionable: actionable,
    canonicalSalesReadyCount: salesReadyCanonical,
    researchContinuationCount,
    candidatesStoppedMonitoredOrExhausted: stoppedOrMonitored,
    evidenceReusedFromPreviousRuns: previouslyKnown,
    syntheticCorpusCount: synthetic,
    segmentCoverageStatus: segmentCoverage?.coverageStatus || null,
    zeroYieldResearch: zeroYield,
    honestyNotes: [
      "Cheaper runs are not better unless coverage and commercial quality are also considered.",
      "Zero-yield research is reported explicitly when selected/actionable/live-new are empty.",
      "Economics do not invent incremental quality from drawer length.",
      "verifiedCommercialContactsGained counts named primaryContactName / salesReadyContacts on processed rows — not Sales Ready and not verified personal email.",
      "opportunitiesBecomingActionable excludes final EXCLUDE disposition even when commercialResearchComplete is true.",
      "canonicalSalesReadyCount is the only economics field that reflects Sales Ready eligibility.",
      "Portfolio certification must prefer commercialEligibilityStatus + reconciled disposition over intermediate economics counters.",
    ],
  };
}

export default {
  GDI_RESEARCH_ECONOMICS_LEDGER_V1,
  buildResearchEconomicsLedger,
};
