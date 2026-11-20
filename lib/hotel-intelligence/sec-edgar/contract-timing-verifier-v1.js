/**
 * Verify hotel-level contract timing findings — identity match + date-meaning gates.
 */

import { classifyHotelIdentity, IDENTITY_CLASS } from "./hotel-identity-quality-v2.js";
import { matchHotelHintToHpc } from "./hotel-attribution-v1.js";
import {
  DATE_MEANING,
  isHotelContractDateMeaning,
  isLoanMaturityMeaning,
  classifyDateHorizon,
} from "./contract-timing-types-v1.js";
import { HISTORICAL_APPLICABILITY, VERIFICATION_CLASS } from "./contract.js";

export const CONTRACT_TIMING_VERIFIER_V1 = "contract-timing-verifier-v1";

/**
 * @param {object[]} hotelContractFindings — from CMBS/REIT extractors
 * @param {object} opts
 * @param {Array} [opts.hpcCandidates]
 * @param {Array} [opts.discoveryInventoryHotels] — filesystem/Airtable discovery rows
 * @param {Date|string} [opts.now]
 */
export function verifyContractTimingFindings(hotelContractFindings = [], opts = {}) {
  const now = opts.now ? new Date(opts.now) : new Date();
  const verified = [];
  const probable = [];
  const rejected = [];
  const loanConfused = [];
  const historicalNeedRevalidation = [];
  const horizons = { 6: [], 12: [], 24: [], 36: [] };

  const discovery = opts.discoveryInventoryHotels || [];

  for (const f of hotelContractFindings) {
    if (isLoanMaturityMeaning(f.dateMeaning) || f.isLoanMaturity) {
      loanConfused.push({
        ...f,
        rejectReason: "loan_maturity_must_not_enter_hotel_contract_verified_set",
      });
      continue;
    }
    if (!isHotelContractDateMeaning(f.dateMeaning)) {
      rejected.push({ ...f, rejectReason: "date_meaning_not_hotel_contract" });
      continue;
    }
    if (f.portfolioWide || f.rejectReason === "multi_hotel_row") {
      rejected.push({ ...f, rejectReason: "portfolio_or_multi_hotel" });
      continue;
    }

    const identity = classifyHotelIdentity({
      hotelName: f.hotelName,
      city: f.city,
      stateRegion: f.stateRegion,
      discoveryMethod: f.discoveryMethod || "HTML_PROPERTY_TABLE",
      contractFindings: [
        {
          verificationClass: f.verificationClass,
          primaryDate: f.primaryDate,
        },
      ],
    });

    if (
      identity.identityClass === IDENTITY_CLASS.EXTRACTION_ARTIFACT ||
      identity.identityClass === IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY ||
      identity.identityClass === IDENTITY_CLASS.MULTIPLE_PROPERTIES
    ) {
      rejected.push({
        ...f,
        identityClass: identity.identityClass,
        rejectReason: `identity_${identity.identityClass}`,
        identityReasons: identity.reasons,
      });
      continue;
    }

    // Match discovery inventory
    const discHit = discovery.find((h) => {
      const n = String(h.normalizedName || h.hotelName || "").toLowerCase();
      const fn = String(identity.normalizedName || f.hotelName || "").toLowerCase();
      if (!n || !fn) return false;
      const sameCity =
        !f.city ||
        !h.city ||
        String(h.city).toLowerCase() === String(f.city).toLowerCase();
      return (n === fn || n.includes(fn) || fn.includes(n)) && sameCity;
    });

    // Match HPC
    let hpcMatch = { status: "NO_MATCH", hpcId: null };
    if (opts.hpcCandidates?.length) {
      const hint = [identity.normalizedName || f.hotelName, f.city].filter(Boolean).join(" ");
      hpcMatch = matchHotelHintToHpc(hint, opts.hpcCandidates, { minScore: 0.55 });
    }

    const horizon = classifyDateHorizon(f.primaryDate, now);
    const row = {
      ...f,
      hotelName: identity.normalizedName || f.hotelName,
      identityClass: identity.identityClass,
      identityReasons: identity.reasons,
      discoveryPropertyId: discHit?.propertyId || discHit?.discoveryId || null,
      hpcMatchStatus: hpcMatch.status,
      hpcRecordId: hpcMatch.hpcId || null,
      horizon,
      persistable:
        identity.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY &&
        f.verificationClass === VERIFICATION_CLASS.VERIFIED_EXPIRATION &&
        isHotelContractDateMeaning(f.dateMeaning),
    };

    if (horizon.historical) {
      row.historicalApplicability = HISTORICAL_APPLICABILITY.REQUIRES_FURTHER_RESEARCH;
      row.notes = [...(row.notes || []), "HISTORICAL_DATE_REQUIRES_REVALIDATION"];
      historicalNeedRevalidation.push(row);
    }

    if (row.persistable && !horizon.historical) {
      verified.push(row);
      for (const b of horizon.buckets || []) {
        horizons[b].push(row);
      }
    } else if (
      identity.identityClass === IDENTITY_CLASS.PROBABLE_PROPERTY ||
      identity.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY
    ) {
      probable.push(row);
      if (!horizon.historical) {
        for (const b of horizon.buckets || []) horizons[b].push(row);
      }
    } else {
      rejected.push({ ...row, rejectReason: "identity_not_verified_or_probable" });
    }
  }

  const uniqueHotels = new Set(
    [...verified, ...probable].map(
      (r) => `${String(r.hotelName || "").toLowerCase()}|${r.city}|${r.stateRegion}`,
    ),
  );

  return {
    version: CONTRACT_TIMING_VERIFIER_V1,
    counts: {
      input: hotelContractFindings.length,
      verifiedHotelContractDates: verified.length,
      probable: probable.length,
      rejected: rejected.length,
      loanMaturityExcluded: loanConfused.length,
      historicalNeedRevalidation: historicalNeedRevalidation.length,
      uniqueHotels: uniqueHotels.size,
      horizon6: horizons[6].length,
      horizon12: horizons[12].length,
      horizon24: horizons[24].length,
      horizon36: horizons[36].length,
      franchise: [...verified, ...probable].filter(
        (r) => r.dateMeaning === DATE_MEANING.FRANCHISE_EXPIRATION,
      ).length,
      management: [...verified, ...probable].filter(
        (r) => r.dateMeaning === DATE_MEANING.MANAGEMENT_EXPIRATION,
      ).length,
      terminationRights: [...verified, ...probable].filter(
        (r) => r.dateMeaning === DATE_MEANING.TERMINATION_RIGHT_DATE,
      ).length,
      hpcMatched: [...verified, ...probable].filter((r) => r.hpcMatchStatus === "MATCHED").length,
    },
    verified,
    probable,
    rejected,
    loanConfused,
    historicalNeedRevalidation,
    horizons,
  };
}

export default { verifyContractTimingFindings, CONTRACT_TIMING_VERIFIER_V1 };
