/**
 * Verification gate — never promote estimates or unattributed portfolio claims.
 */

import {
  VERIFICATION_CLASS,
  NON_VERIFIED_CLASSES,
  PERSISTENCE_CLASS,
  isPersistableVerifiedClass,
} from "./contract.js";
import { isUnsupportedExpirationClaim } from "./contract-extractor-v1.js";

export const VERIFICATION_VERSION = "sec-edgar-verification-v1";

/**
 * @param {object} finding
 * @param {object} [ctx]
 */
export function verifyFinding(finding, ctx = {}) {
  const reasons = [];
  let cls = finding.verificationClass || VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE;

  if (isUnsupportedExpirationClaim(finding.snippet)) {
    reasons.push("speculative_or_estimated_language");
    cls = VERIFICATION_CLASS.ESTIMATED_RENEWAL_WINDOW;
  }

  if (finding.portfolioWide || !finding.hotelAttributed) {
    reasons.push("no_hotel_level_attribution");
    if (isPersistableVerifiedClass(cls)) {
      cls = VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE;
    }
  }

  if (!finding.primaryDate && cls.startsWith("VERIFIED_")) {
    reasons.push("no_date_in_snippet");
    cls = VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE;
  }

  if (!finding.source?.accessionNumber || !finding.source?.documentUrl) {
    reasons.push("incomplete_source_provenance");
    cls = VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE;
  }

  // Subsequent filing supersession: if ctx.laterFilings contradict, mark historical.
  if (ctx.supersededByLaterFiling === true && cls === VERIFICATION_CLASS.VERIFIED_EXPIRATION) {
    cls = VERIFICATION_CLASS.HISTORICAL_AGREEMENT;
    reasons.push("later_filing_may_amend_expiration");
  }

  const persistable = isPersistableVerifiedClass(cls) && finding.hotelAttributed === true;
  let persistenceClass = PERSISTENCE_CLASS.STAGED_LOCAL;
  if (NON_VERIFIED_CLASSES.includes(cls)) {
    persistenceClass = PERSISTENCE_CLASS.REJECTED_ESTIMATE;
  } else if (!persistable) {
    persistenceClass = PERSISTENCE_CLASS.QUARANTINED;
  }

  return {
    version: VERIFICATION_VERSION,
    verificationClass: cls,
    persistable,
    persistenceClass,
    reasons,
    requiresCurrentApplicabilitySearch:
      cls === VERIFICATION_CLASS.VERIFIED_EXPIRATION ||
      cls === VERIFICATION_CLASS.HISTORICAL_AGREEMENT,
    finding: {
      ...finding,
      verificationClass: cls,
      verifiedAt: new Date().toISOString(),
    },
  };
}

export function verifyFindings(findings = [], ctx = {}) {
  const verified = findings.map((f) => verifyFinding(f, ctx));
  return {
    version: VERIFICATION_VERSION,
    total: verified.length,
    persistable: verified.filter((v) => v.persistable).length,
    rejectedEstimates: verified.filter(
      (v) => v.persistenceClass === PERSISTENCE_CLASS.REJECTED_ESTIMATE,
    ).length,
    quarantined: verified.filter((v) => v.persistenceClass === PERSISTENCE_CLASS.QUARANTINED)
      .length,
    results: verified,
  };
}
