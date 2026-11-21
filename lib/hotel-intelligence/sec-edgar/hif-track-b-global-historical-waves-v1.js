/**
 * HIF Track B — global historical contractual wave rules.
 * Explicit instruments only. Loan maturity is not a hotel expiration.
 * Combined franchise/management columns stay unresolved.
 * A missing newer filing does not prove an old agreement is still effective.
 * CUSTOMER_BLOCKED.
 */

export const HIF_TRACK_B_GLOBAL_WAVES = "hif-track-b-global-historical-waves-v1";

export const RESOLVED_AGREEMENT_TYPES = Object.freeze(["FRANCHISE", "MANAGEMENT", "LEASE", "LICENSE", "OTHER"]);

export function lackOfNewerFilingProvesCurrent() {
  return false;
}

export function isPortfolioOrArtifactName(name) {
  const n = String(name || "").trim();
  if (n.length < 8) return true;
  if (/\b(portfolio|mortgage loan|related mortgage|borrower|system-wide|approximately \d+ hotels)\b/i.test(n)) {
    return true;
  }
  if (/^hotel indigo$/i.test(n)) return true;
  if (n.split(/\s+/).length > 16) return true;
  return false;
}

/**
 * Resolve agreement type only from explicit instrument words in one clause.
 * Combined columns and dual labels stay unresolved.
 */
export function resolveExplicitAgreementType(clause) {
  const t = String(clause || "");
  const loanLike = /\b(loan maturity|maturity date|mortgage loan|cut-off date|appraised value)\b/i.test(t);
  const franchise = /\bfranchise agreements?\b/i.test(t);
  const management = /\bmanagement agreements?\b/i.test(t);
  const license = /\blicense agreements?\b/i.test(t) && !/\bfranchise agreements?\b/i.test(t);
  const lease = /\b(lease agreements?|contrato de arrendamiento|arrendamiento)\b/i.test(t);
  const concession = /\b(concession agreements?|contrato de concesi[oó]n)\b/i.test(t);
  const hits = [
    franchise ? "FRANCHISE" : null,
    management ? "MANAGEMENT" : null,
    license ? "LICENSE" : null,
    lease ? "LEASE" : null,
    concession ? "CONCESSION" : null,
  ].filter(Boolean);

  if (loanLike && hits.length === 0) {
    return {
      agreementType: null,
      resolved: false,
      reject: "LOAN_OR_CMBS_MATURITY_NOT_HOTEL_CONTRACT",
    };
  }
  if (hits.length === 1 && hits[0] === "CONCESSION") {
    return { agreementType: "OTHER", resolved: true, instrument: "CONCESSION", reject: null };
  }
  if (hits.length === 1) {
    return { agreementType: hits[0], resolved: true, instrument: hits[0], reject: null };
  }
  if (hits.length > 1) {
    return {
      agreementType: null,
      resolved: false,
      reject: null,
      reason: "COMBINED_OR_AMBIGUOUS_INSTRUMENT",
    };
  }
  return { agreementType: null, resolved: false, reason: "INSTRUMENT_NOT_EXPLICIT", reject: null };
}

export function timingBucket(isoDate, asOf) {
  const note = "Timing bucket is an observed date class, not commercial availability";
  if (!isoDate || !/^\d{4}-\d{2}-\d{2}$/.test(String(isoDate))) {
    return { bucket: "UNDATED_OR_IMPRECISE", note };
  }
  const exp = new Date(`${isoDate}T00:00:00Z`);
  const now = new Date(`${asOf}T00:00:00Z`);
  const days = (exp.getTime() - now.getTime()) / 86400000;
  if (days < 0) return { bucket: "HISTORICAL_DATE", note };
  if (days <= 730) return { bucket: "WITHIN_24_MONTHS", note };
  if (days <= 1825) return { bucket: "MONTHS_24_TO_60", note };
  return { bucket: "BEYOND_60_MONTHS", note };
}

export function contractPersistAllowed(dossier) {
  if (!dossier?.persistEligible) return false;
  if (dossier.contractEligible === false) return false;
  if (dossier.identityClass !== "VERIFIED_PROPERTY") return false;
  if (dossier.dateConflict === true) return false;
  const t = dossier.agreementType;
  return RESOLVED_AGREEMENT_TYPES.includes(t);
}
