/**
 * Contract Timing V3 — current-status classification beyond historical verification.
 * Absence of a later amendment is NOT proof the contract remains active.
 */

export const CONTRACT_TIMING_V3_STATUS_V1 = "contract-timing-v3-status-v1";

export const CONTRACT_CURRENT_STATUS = Object.freeze({
  HISTORICALLY_VERIFIED: "HISTORICALLY_VERIFIED",
  CURRENTLY_CORROBORATED: "CURRENTLY_CORROBORATED",
  SUPERSEDED: "SUPERSEDED",
  EXPIRED_STATUS_UNCONFIRMED: "EXPIRED_STATUS_UNCONFIRMED",
  CONFLICTED: "CONFLICTED",
  REQUIRES_REVALIDATION: "REQUIRES_REVALIDATION",
});

/**
 * @param {object} finding
 * @param {object} [ctx]
 * @param {Date|string} [ctx.now]
 * @param {Array} [ctx.laterEvidence] — subsequent disclosures (SEC or Exa leads verified)
 */
export function classifyContractCurrentStatus(finding, ctx = {}) {
  const now = ctx.now ? new Date(ctx.now) : new Date();
  const later = ctx.laterEvidence || [];
  const primary = finding?.primaryDate ? new Date(finding.primaryDate) : null;
  const year = primary && !Number.isNaN(primary.getTime()) ? primary.getUTCFullYear() : null;

  const hasTermination = later.some((e) =>
    /TERMINAT|REFLAG|CONVERSION|CANCEL/i.test(String(e.kind || e.dateMeaning || e.snippet || "")),
  );
  const hasSupersession = later.some((e) =>
    /SUPERSED|AMEND|RENEW|EXTEND|REPLAC/i.test(String(e.kind || e.dateMeaning || e.snippet || "")),
  );
  const hasCorroboration = later.some((e) =>
    /CORROBORAT|SAME_DATE|STILL_ACTIVE|AFFIRM/i.test(String(e.kind || "")),
  );
  const conflictingDates = later.filter(
    (e) =>
      e.primaryDate &&
      finding.primaryDate &&
      String(e.primaryDate).slice(0, 10) !== String(finding.primaryDate).slice(0, 10) &&
      e.dateMeaning === finding.dateMeaning,
  );

  if (conflictingDates.length && hasTermination) {
    return {
      status: CONTRACT_CURRENT_STATUS.CONFLICTED,
      note: "Conflicting later dates and termination language present",
      laterCount: later.length,
    };
  }
  if (hasTermination) {
    return {
      status: CONTRACT_CURRENT_STATUS.SUPERSEDED,
      note: "Later disclosure indicates termination / reflag / replacement",
      laterCount: later.length,
    };
  }
  if (hasSupersession) {
    return {
      status: CONTRACT_CURRENT_STATUS.SUPERSEDED,
      note: "Later amendment / renewal / extension supersedes earlier milestone",
      laterCount: later.length,
      successorDate: later.find((e) => e.primaryDate)?.primaryDate || null,
    };
  }
  if (conflictingDates.length) {
    return {
      status: CONTRACT_CURRENT_STATUS.CONFLICTED,
      note: "Later disclosures show different date for same agreement type",
      laterCount: later.length,
    };
  }
  if (hasCorroboration) {
    return {
      status: CONTRACT_CURRENT_STATUS.CURRENTLY_CORROBORATED,
      note: "Later independent disclosure corroborates the contractual date",
      laterCount: later.length,
    };
  }

  if (primary && !Number.isNaN(primary.getTime()) && primary.getTime() < now.getTime()) {
    return {
      status: CONTRACT_CURRENT_STATUS.EXPIRED_STATUS_UNCONFIRMED,
      note: "Contractual date is in the past; no later evidence found — not proof of current inactivity or renewal",
      laterCount: later.length,
    };
  }

  if (year != null && year >= now.getUTCFullYear() && year <= now.getUTCFullYear() + 12) {
    return {
      status: CONTRACT_CURRENT_STATUS.HISTORICALLY_VERIFIED,
      note: "Future contractual date from filing; no later corroboration or supersession located yet",
      laterCount: later.length,
      commerciallyActionableWindow: year >= 2027 && year <= 2035,
    };
  }

  return {
    status: CONTRACT_CURRENT_STATUS.REQUIRES_REVALIDATION,
    note: "Insufficient later evidence to classify current commercial applicability",
    laterCount: later.length,
  };
}

export function isCommerciallyFutureWindow(isoOrYear, opts = {}) {
  const start = opts.startYear ?? 2027;
  const end = opts.endYear ?? 2035;
  const y =
    typeof isoOrYear === "number"
      ? isoOrYear
      : Number(String(isoOrYear || "").slice(0, 4));
  return Number.isFinite(y) && y >= start && y <= end;
}

export default {
  classifyContractCurrentStatus,
  isCommerciallyFutureWindow,
  CONTRACT_CURRENT_STATUS,
};
