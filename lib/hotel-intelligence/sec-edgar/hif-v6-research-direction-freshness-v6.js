/**
 * HIF V6 — Research direction & source freshness (founder 2026-11-21).
 * Separates document publication date, discovery date, contract date, last verification.
 * Does not invent renewals or notice periods.
 */

export const HIF_V6_RESEARCH_DIRECTION_FRESHNESS =
  "hif-v6-research-direction-freshness-v6";

export const RESEARCH_DIRECTION = Object.freeze({
  HISTORICAL_FORWARD: "HISTORICAL_FORWARD",
  CURRENT_BACKWARD: "CURRENT_BACKWARD",
  CONTRACT_WINDOW_TARGETED: "CONTRACT_WINDOW_TARGETED",
  OTHER_UNKNOWN: "OTHER_UNKNOWN",
});

export const SOURCE_VINTAGE = Object.freeze({
  BEFORE_2015: "BEFORE_2015",
  Y2015_2019: "2015_2019",
  Y2020_2023: "2020_2023",
  Y2024_2026: "2024_2026",
  LATER: "LATER",
  UNKNOWN: "UNKNOWN",
});

export const EVIDENCE_FRESHNESS = Object.freeze({
  CURRENT_STATUS_CORROBORATED: "CURRENT_STATUS_CORROBORATED",
  SOURCE_RECENT_UNCHECKED: "SOURCE_RECENT_UNCHECKED",
  SOURCE_AGED_UNCHECKED: "SOURCE_AGED_UNCHECKED",
  SUPERSESSION_RISK_HIGH: "SUPERSESSION_RISK_HIGH",
  UNKNOWN: "UNKNOWN",
});

export function parseYear(dateLike) {
  if (!dateLike) return null;
  const m = String(dateLike).match(/(20\d{2}|19\d{2})/);
  return m ? Number(m[1]) : null;
}

export function classifySourceVintage(publicationDate) {
  const y = parseYear(publicationDate);
  if (!y) return SOURCE_VINTAGE.UNKNOWN;
  if (y < 2015) return SOURCE_VINTAGE.BEFORE_2015;
  if (y <= 2019) return SOURCE_VINTAGE.Y2015_2019;
  if (y <= 2023) return SOURCE_VINTAGE.Y2020_2023;
  if (y <= 2026) return SOURCE_VINTAGE.Y2024_2026;
  return SOURCE_VINTAGE.LATER;
}

/**
 * Infer research direction from provenance / wave / discoveryMethod — never invent.
 */
export function classifyResearchDirection(row = {}) {
  const blob = [
    row.discoveryMethod,
    row.wave,
    row.sourceFamily,
    row.provenance,
    ...(Array.isArray(row.provenance) ? row.provenance : []),
    row.queryId,
    row.track,
  ]
    .flat()
    .filter(Boolean)
    .join(" ")
    .toUpperCase();

  if (/MGMT_DISCOVERY|EIGHT_K|8K|CURRENT_BACKWARD|OPERATOR_CHANGE|REFLAG|CONVERSION/.test(blob)) {
    return {
      researchDirection: RESEARCH_DIRECTION.CURRENT_BACKWARD,
      directionConfidence: "INFERRED_FROM_PROVENANCE",
      note: "Contemporary commercial-event / 8-K style discovery path",
    };
  }
  if (/CONTRACT_WINDOW|PRIORITY_B_RECOVERY|NOTICE_DEADLINE|WINDOW_TARGETED/.test(blob)) {
    return {
      researchDirection: RESEARCH_DIRECTION.CONTRACT_WINDOW_TARGETED,
      directionConfidence: "INFERRED_FROM_PROVENANCE",
      note: "Targeted from upcoming contractual decision window",
    };
  }
  if (
    /NAME_FREE|CMBS|SCALEUP|CONTINUOUS|SIA_|WAVE2|WAVE34|424B2|424H|HISTORICAL_FORWARD|SEC_CMBS/.test(
      blob,
    )
  ) {
    return {
      researchDirection: RESEARCH_DIRECTION.HISTORICAL_FORWARD,
      directionConfidence: "INFERRED_FROM_PROVENANCE",
      note: "CMBS / Related License schedule harvest progressing from financing disclosures",
    };
  }
  return {
    researchDirection: RESEARCH_DIRECTION.OTHER_UNKNOWN,
    directionConfidence: "UNKNOWN",
    note: "Provenance insufficient to classify research direction",
  };
}

/**
 * Extract accession/file year from SEC URL when fileDate absent.
 * e.g. .../000153949725001873/... → often year embedded in accession mid segment.
 */
export function inferPublicationDateFromSource(row = {}) {
  if (row.fileDate) return String(row.fileDate).slice(0, 10);
  if (row.sourcePublicationDate) return String(row.sourcePublicationDate).slice(0, 10);
  if (row.filingDate) return String(row.filingDate).slice(0, 10);
  const url = String(row.sourceUrl || "");
  // Accession pattern: 0001539497-25-001873 → year 2025
  const acc = url.match(/(\d{10})-(\d{2})-(\d{6})/);
  if (acc) {
    const yy = Number(acc[2]);
    const year = yy >= 90 ? 1900 + yy : 2000 + yy;
    return `${year}-01-01`; // year-precision only
  }
  // Compact accession in path: 000153949725001873
  const compact = url.match(/\/(000\d{7})(\d{2})(\d{6})\//);
  if (compact) {
    const yy = Number(compact[2]);
    const year = yy >= 90 ? 1900 + yy : 2000 + yy;
    return `${year}-01-01`;
  }
  return null;
}

export function classifyEvidenceFreshness(row = {}, asOf, publicationDate) {
  const pubYear = parseYear(publicationDate);
  const asOfYear = parseYear(asOf) || new Date().getUTCFullYear();
  if (row.currentStatusVerified === true || row.evidenceFreshness === "CURRENT_STATUS_CORROBORATED") {
    return {
      evidenceFreshness: EVIDENCE_FRESHNESS.CURRENT_STATUS_CORROBORATED,
      amendmentsChecked: Boolean(row.amendmentsChecked),
      brandCurrentChecked: Boolean(row.brandCurrentChecked),
      managerCurrentChecked: Boolean(row.managerCurrentChecked),
      note: "Current status explicitly corroborated",
    };
  }
  if (!pubYear) {
    return {
      evidenceFreshness: EVIDENCE_FRESHNESS.UNKNOWN,
      amendmentsChecked: false,
      brandCurrentChecked: false,
      managerCurrentChecked: false,
      note: "Publication date unknown — freshness unclassified",
    };
  }
  const age = asOfYear - pubYear;
  if (age <= 2) {
    return {
      evidenceFreshness: EVIDENCE_FRESHNESS.SOURCE_RECENT_UNCHECKED,
      amendmentsChecked: false,
      brandCurrentChecked: false,
      managerCurrentChecked: false,
      note: "Recent source; subsequent renewal/reflag not yet verified",
    };
  }
  if (age >= 5) {
    return {
      evidenceFreshness: EVIDENCE_FRESHNESS.SUPERSESSION_RISK_HIGH,
      amendmentsChecked: false,
      brandCurrentChecked: false,
      managerCurrentChecked: false,
      note: "Aged source — high risk of amendment/renewal/reflag supersession; absence of renewal evidence ≠ no renewal",
    };
  }
  return {
    evidenceFreshness: EVIDENCE_FRESHNESS.SOURCE_AGED_UNCHECKED,
    amendmentsChecked: false,
    brandCurrentChecked: false,
    managerCurrentChecked: false,
    note: "Aged source without current-status corroboration",
  };
}

export function annotateResearchFreshness(row = {}, asOf) {
  const publicationDate = inferPublicationDateFromSource(row);
  const direction = classifyResearchDirection(row);
  const vintage = classifySourceVintage(publicationDate);
  const freshness = classifyEvidenceFreshness(row, asOf, publicationDate);
  return {
    sourcePublicationDate: publicationDate,
    sourcePublicationPrecision: publicationDate && String(publicationDate).endsWith("-01-01") ? "YEAR" : publicationDate ? "DAY_OR_UNKNOWN" : null,
    contractCommencementDate: row.commencementDate || row.agreementStartDate || null,
    originalExpirationDate: row.originalExpirationDate || row.expirationDate || null,
    currentEffectiveExpiration: row.currentEffectiveExpiration || row.expirationDate || null,
    noticeOrRenewalDecisionDate: row.noticeDeadline || row.nonrenewalNoticeDeadline || null,
    eventDiscoveryTimestamp: row.discoveredAt || row.wave || null,
    mostRecentEvidenceVerificationDate: row.lastVerifiedAt || null,
    researchRunTimestamp: asOf,
    sourceVintage: vintage,
    ...direction,
    ...freshness,
  };
}
