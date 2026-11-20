/**
 * Research yield prioritization — prefer filings that disclose hotel-level intel.
 */

export const YIELD_PRIORITIZATION_VERSION = "sec-edgar-yield-prioritization-v1";

/**
 * @param {Array<{
 *   companyKey:string,
 *   companyName:string,
 *   cik:string,
 *   filingsProcessed:number,
 *   namedHotels:number,
 *   verifiedMilestones:number,
 *   relevantAgreements:number,
 *   estimatedCostUsd:number,
 *   failedAttributionReasons?:string[]
 * }>} companyStats
 */
export function buildYieldPrioritizationReport(companyStats = []) {
  const rows = companyStats.map((c) => {
    const filings = Math.max(1, Number(c.filingsProcessed) || 0);
    const agreementsPer10 = (Number(c.relevantAgreements) || 0) * (10 / filings);
    const hotelsPerFiling = (Number(c.namedHotels) || 0) / filings;
    const milestonesPerFiling = (Number(c.verifiedMilestones) || 0) / filings;
    const costPerHotel =
      Number(c.namedHotels) > 0
        ? (Number(c.estimatedCostUsd) || 0) / Number(c.namedHotels)
        : null;
    const score =
      hotelsPerFiling * 4 +
      milestonesPerFiling * 6 +
      agreementsPer10 * 0.5 -
      (costPerHotel || 0) * 0.1;

    let recommendation = "CONTINUE";
    if (c.namedHotels === 0 && c.verifiedMilestones === 0) {
      recommendation = "DEPRIORITIZE_BROAD_CORPORATE";
    } else if (hotelsPerFiling >= 0.5 || milestonesPerFiling > 0) {
      recommendation = "PRIORITIZE_EXHIBITS_AND_SCHEDULES";
    } else if (c.namedHotels > 0 && c.verifiedMilestones === 0) {
      recommendation = "PRIORITIZE_EX_10_PROPERTY_AGREEMENTS";
    }

    return {
      companyKey: c.companyKey,
      companyName: c.companyName,
      cik: c.cik,
      namedHotels: c.namedHotels || 0,
      verifiedContractualMilestones: c.verifiedMilestones || 0,
      relevantAgreementsPer10Filings: Number(agreementsPer10.toFixed(2)),
      hotelLevelExtractionCostUsd: Number((c.estimatedCostUsd || 0).toFixed(4)),
      costPerNamedHotelUsd: costPerHotel == null ? null : Number(costPerHotel.toFixed(4)),
      failedAttributionReasons: [...new Set(c.failedAttributionReasons || [])],
      yieldScore: Number(score.toFixed(3)),
      recommendation,
    };
  });

  rows.sort((a, b) => b.yieldScore - a.yieldScore);

  return {
    version: YIELD_PRIORITIZATION_VERSION,
    ranked: rows,
    nextCohortGuidance:
      rows.length === 0
        ? "No company yield data"
        : rows[0].recommendation === "DEPRIORITIZE_BROAD_CORPORATE"
          ? "Shift cohort toward REIT property schedules and EX-10 hotel agreements; avoid broad brand-corporate 10-K narrative-only loops"
          : `Next research focus: ${rows[0].companyKey} — ${rows[0].recommendation}`,
  };
}
