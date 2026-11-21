/**
 * HIF V6 — Permanent research-gap visibility (not market-coverage % without denominator).
 */

export const HIF_V6_RESEARCH_GAP_VISIBILITY = "hif-v6-research-gap-visibility-v6";

export const GAP_KIND = Object.freeze({
  COVERAGE_GAP: "COVERAGE_GAP",
  GEOGRAPHIC_VERIFICATION_GAP: "GEOGRAPHIC_VERIFICATION_GAP",
  TIMING_GAP: "TIMING_GAP",
  AGREEMENT_CLASSIFICATION_GAP: "AGREEMENT_CLASSIFICATION_GAP",
  CURRENT_STATUS_GAP: "CURRENT_STATUS_GAP",
  COMMERCIAL_QUALIFICATION_GAP: "COMMERCIAL_QUALIFICATION_GAP",
});

/**
 * Summarize gaps from audited commercial rows. Does not invent universe denominators.
 */
export function summarizeResearchGaps(rows = [], opts = {}) {
  const byCountry = {};
  const ensure = (c) => {
    if (!byCountry[c]) {
      byCountry[c] = {
        country: c,
        discovered: 0,
        geoVerified: 0,
        geoReviewRequired: 0,
        geoUnresolved: 0,
        timingUnknown: 0,
        agreementUnresolved: 0,
        currentStatusUnchecked: 0,
        commercialQualificationGap: 0,
        externalUniverseEstimate: opts.universeByCountry?.[c] ?? null,
        coverageGapNote:
          opts.universeByCountry?.[c] != null
            ? "DENOMINATOR_DOCUMENTED"
            : "NO_CREDIBLE_UNIVERSE_DENOMINATOR — coverage % not computed",
      };
    }
    return byCountry[c];
  };

  for (const r of rows) {
    const c = r.physicalCountry || r.hotelCountry || "UNATTRIBUTED";
    const b = ensure(c);
    b.discovered += 1;
    if (r.geographicClass === "VERIFIED") b.geoVerified += 1;
    else if (r.geographicClass === "UNRESOLVED") b.geoUnresolved += 1;
    else b.geoReviewRequired += 1;
    if (r.timingHorizon === "UNKNOWN" || r.decisionWindowKind === "TIMING_UNKNOWN") b.timingUnknown += 1;
    if (
      r.agreementTypeResolved === false ||
      /UNRESOLVED|UNKNOWN|COMBINED/i.test(String(r.agreementType || r.commercialList || ""))
    ) {
      b.agreementUnresolved += 1;
    }
    if (
      r.currentStatusConfidence === "EXPIRATION_UNCHECKED_FOR_RENEWAL" ||
      r.evidenceFreshness === "SUPERSESSION_RISK_HIGH" ||
      r.evidenceFreshness === "SOURCE_AGED_UNCHECKED" ||
      !r.currentStatusVerified
    ) {
      b.currentStatusUnchecked += 1;
    }
    if (
      r.commercialPriority === "PRIORITY_B" ||
      r.commercialPriority === "HOLD" ||
      r.decisionWindowKind === "EXPIRATION_ONLY_PROXY"
    ) {
      b.commercialQualificationGap += 1;
    }
  }

  const totals = {
    [GAP_KIND.GEOGRAPHIC_VERIFICATION_GAP]: rows.filter((r) => r.geographicClass !== "VERIFIED").length,
    [GAP_KIND.TIMING_GAP]: rows.filter(
      (r) => r.timingHorizon === "UNKNOWN" || r.decisionWindowKind === "TIMING_UNKNOWN",
    ).length,
    [GAP_KIND.AGREEMENT_CLASSIFICATION_GAP]: rows.filter(
      (r) =>
        r.agreementTypeResolved === false ||
        /UNRESOLVED|UNKNOWN|COMBINED/i.test(String(r.agreementType || r.commercialList || "")),
    ).length,
    [GAP_KIND.CURRENT_STATUS_GAP]: rows.filter(
      (r) =>
        r.currentStatusConfidence === "EXPIRATION_UNCHECKED_FOR_RENEWAL" ||
        r.evidenceFreshness === "SUPERSESSION_RISK_HIGH" ||
        !r.currentStatusVerified,
    ).length,
    [GAP_KIND.COMMERCIAL_QUALIFICATION_GAP]: rows.filter(
      (r) =>
        r.commercialPriority === "PRIORITY_B" ||
        r.commercialPriority === "HOLD" ||
        r.decisionWindowKind === "EXPIRATION_ONLY_PROXY",
    ).length,
    [GAP_KIND.COVERAGE_GAP]:
      "NOT_COMPUTED_WITHOUT_CREDIBLE_DOCUMENTED_HOTEL_UNIVERSE_DENOMINATOR",
  };

  return {
    policy: "Absence of records ≠ absence of opportunity; coverage % requires documented denominator",
    totals,
    byCountry,
  };
}
