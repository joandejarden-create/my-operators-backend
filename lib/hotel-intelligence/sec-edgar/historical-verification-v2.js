/**
 * Historical-to-current applicability classification for SEC-disclosed contracts.
 */

import { HISTORICAL_APPLICABILITY, VERIFICATION_CLASS } from "./contract.js";

export const HISTORICAL_VERIFICATION_V2 = "sec-edgar-historical-verification-v2";

/**
 * Classify a finding's current applicability given filing date and later evidence.
 *
 * @param {object} finding
 * @param {object} [ctx]
 * @param {string} [ctx.asOfDate] — ISO date for "current" (default now)
 * @param {Array} [ctx.laterFindings] — subsequent filings about same hotel/field
 */
export function classifyHistoricalApplicability(finding, ctx = {}) {
  const asOf = ctx.asOfDate ? new Date(ctx.asOfDate) : new Date();
  const filingDate = finding?.source?.filingDate ? new Date(finding.source.filingDate) : null;
  const cls = String(finding?.verificationClass || "");
  const later = ctx.laterFindings || [];

  // Explicit lifecycle from later filings
  if (later.some((f) => f.verificationClass === VERIFICATION_CLASS.VERIFIED_TERMINATION)) {
    return {
      historicalApplicability: HISTORICAL_APPLICABILITY.TERMINATED,
      currentlyVerifiedExpiration: null,
      note: "Later filing evidences termination",
    };
  }
  if (later.some((f) => f.verificationClass === VERIFICATION_CLASS.VERIFIED_AMENDMENT)) {
    const latest = later
      .filter((f) => f.verificationClass === VERIFICATION_CLASS.VERIFIED_AMENDMENT)
      .sort((a, b) => String(b.source?.filingDate || "").localeCompare(String(a.source?.filingDate || "")))[0];
    return {
      historicalApplicability: HISTORICAL_APPLICABILITY.AMENDED,
      currentlyVerifiedExpiration: latest?.primaryDate || null,
      note: "Amended by subsequent filing — use latest milestone",
    };
  }
  if (
    later.some(
      (f) =>
        f.verificationClass === VERIFICATION_CLASS.VERIFIED_EXPIRATION &&
        f.primaryDate &&
        f.primaryDate !== finding.primaryDate,
    )
  ) {
    return {
      historicalApplicability: HISTORICAL_APPLICABILITY.SUPERSEDED,
      currentlyVerifiedExpiration: later.find((f) => f.verificationClass === VERIFICATION_CLASS.VERIFIED_EXPIRATION)
        ?.primaryDate,
      note: "Later expiration disclosure supersedes earlier date",
    };
  }

  // Original finding classifications
  if (cls === VERIFICATION_CLASS.VERIFIED_TERMINATION) {
    return {
      historicalApplicability: HISTORICAL_APPLICABILITY.TERMINATED,
      currentlyVerifiedExpiration: null,
      note: "Termination disclosed in source filing",
    };
  }

  if (cls === VERIFICATION_CLASS.VERIFIED_EXPIRATION && finding.primaryDate) {
    const year = extractYear(finding.primaryDate);
    // If we only have the original disclosure and no later confirmation:
    if (!later.length) {
      if (year && year < asOf.getFullYear()) {
        return {
          historicalApplicability: HISTORICAL_APPLICABILITY.CURRENT_STATUS_UNCONFIRMED,
          currentlyVerifiedExpiration: null,
          historicallyVerifiedExpiration: finding.primaryDate,
          note: "Historical expiration year has passed; current status unconfirmed without later evidence",
        };
      }
      return {
        historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
        currentlyVerifiedExpiration: null,
        historicallyVerifiedExpiration: finding.primaryDate,
        note: "Historically verified from filing; not promoted to CURRENTLY_VERIFIED without subsequent confirmation",
      };
    }
  }

  if (cls.startsWith("VERIFIED_") || cls === VERIFICATION_CLASS.HISTORICAL_AGREEMENT) {
    return {
      historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
      currentlyVerifiedExpiration: null,
      note: "Historically verified disclosure",
      filingDate: filingDate ? filingDate.toISOString().slice(0, 10) : null,
    };
  }

  return {
    historicalApplicability: HISTORICAL_APPLICABILITY.REQUIRES_FURTHER_RESEARCH,
    currentlyVerifiedExpiration: null,
    note: "Insufficient evidence for historical applicability",
  };
}

function extractYear(token) {
  const m = String(token || "").match(/(20\d{2}|19\d{2})/);
  return m ? Number(m[1]) : null;
}

/**
 * Apply applicability classification across findings for the same hotel.
 */
export function applyHistoricalApplicability(findings = [], opts = {}) {
  const byHotel = new Map();
  for (const f of findings) {
    const key =
      f.attributedHotelName ||
      f.hotelNameHints?.[0] ||
      f.source?.accessionNumber ||
      "unknown";
    if (!byHotel.has(key)) byHotel.set(key, []);
    byHotel.get(key).push(f);
  }

  const out = [];
  for (const [, group] of byHotel) {
    const sorted = [...group].sort((a, b) =>
      String(a.source?.filingDate || "").localeCompare(String(b.source?.filingDate || "")),
    );
    for (let i = 0; i < sorted.length; i++) {
      const f = sorted[i];
      const later = sorted.slice(i + 1);
      const applicability = classifyHistoricalApplicability(f, {
        asOfDate: opts.asOfDate,
        laterFindings: later,
      });
      out.push({
        ...f,
        ...applicability,
        historicalApplicability:
          f.historicalApplicability &&
          f.historicalApplicability !== HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED
            ? f.historicalApplicability
            : applicability.historicalApplicability,
      });
    }
  }
  return {
    version: HISTORICAL_VERIFICATION_V2,
    count: out.length,
    findings: out,
  };
}

/**
 * Never promote historical → currently verified without later evidence.
 */
export function assertNotPromotedWithoutEvidence(finding) {
  if (
    finding.historicalApplicability === HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED &&
    !finding._laterEvidenceConfirmed
  ) {
    return {
      ok: false,
      error: "PROMOTION_WITHOUT_EVIDENCE",
      message: "CURRENTLY_VERIFIED requires subsequent confirming evidence",
    };
  }
  return { ok: true };
}
