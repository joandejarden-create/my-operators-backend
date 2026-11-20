/**
 * Bridge franchise-schedule-extractor-v2 rows into contract-timing findings.
 */

import {
  DATE_MEANING,
  SOURCE_FAMILY,
  isHotelContractDateMeaning,
} from "./contract-timing-types-v1.js";
import { AGREEMENT_TYPE } from "./contract.js";

export const CONTRACT_TIMING_FRANCHISE_BRIDGE_V1 = "contract-timing-franchise-bridge-v1";

function agreementToMeaning(agreementType) {
  const a = String(agreementType || "").toUpperCase();
  if (a.includes("FRANCHISE") || a === "FRANCHISE") return DATE_MEANING.FRANCHISE_EXPIRATION;
  if (a.includes("MANAGEMENT") || a === "MANAGEMENT") return DATE_MEANING.MANAGEMENT_EXPIRATION;
  if (a.includes("LICENSE") || a === "LICENSE") return DATE_MEANING.LICENSE_EXPIRATION;
  if (a.includes("LEASE")) return DATE_MEANING.AMBIGUOUS_DATE;
  return DATE_MEANING.AMBIGUOUS_DATE;
}

function toIso(finding) {
  if (finding.primaryDate && /^\d{4}-\d{2}-\d{2}/.test(String(finding.primaryDate))) {
    return String(finding.primaryDate).slice(0, 10);
  }
  if (finding.primaryDate && /^\d{4}$/.test(String(finding.primaryDate))) {
    return `${finding.primaryDate}-12-31`;
  }
  const named = String(finding.primaryDate || finding.initialExpiration || "").match(
    /(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})/i,
  );
  if (named) {
    const months = {
      january: 1, february: 2, march: 3, april: 4, may: 5, june: 6,
      july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
    };
    const m = months[String(finding.primaryDate || finding.initialExpiration).match(/^[A-Za-z]+/)?.[0]?.toLowerCase()];
    if (m) {
      return `${named[2]}-${String(m).padStart(2, "0")}-${String(Number(named[1])).padStart(2, "0")}`;
    }
  }
  if (finding.initialExpirationYear) {
    return `${finding.initialExpirationYear}-12-31`;
  }
  const yearOnly = String(finding.primaryDate || "").match(/\b(20\d{2})\b/);
  if (yearOnly) return `${yearOnly[1]}-12-31`;
  return null;
}

/**
 * @param {object} scheduleResult — extractFranchiseExpirationSchedule return
 * @param {object} meta
 */
export function bridgeFranchiseScheduleToContractTiming(scheduleResult, meta = {}) {
  const findings = [];
  const rejected = [];

  // Prefer hotel-grained rows (carry city/state); fall back to flat findings.
  const pairs = [];
  for (const h of scheduleResult?.hotels || []) {
    for (const f of h.contractFindings || []) {
      pairs.push({
        hotelName: h.hotelName,
        city: h.city,
        stateRegion: h.stateRegion,
        brandFamily: h.brandFamily || f.brandFamily,
        brandLabel: h.historicalBrand || f.historicalBrand,
        f,
      });
    }
  }
  if (!pairs.length) {
    for (const f of scheduleResult?.findings || []) {
      pairs.push({
        hotelName: f.hotelNameHints?.[0] || f.attributedHotelName,
        city: f.city || null,
        stateRegion: f.stateRegion || null,
        brandFamily: f.brandFamily,
        brandLabel: f.historicalBrand || f.brandLabel,
        f,
      });
    }
  }

  for (const { hotelName, city, stateRegion, brandFamily, brandLabel, f } of pairs) {
    const meaning = agreementToMeaning(f.agreementType);
    const iso = toIso(f);
    if (!iso) {
      rejected.push({ reason: "no_date", hotelName });
      continue;
    }
    if (!isHotelContractDateMeaning(meaning)) {
      rejected.push({ reason: "non_hotel_agreement_type", agreementType: f.agreementType, hotelName });
      continue;
    }
    if (!hotelName) {
      rejected.push({ reason: "no_hotel_name" });
      continue;
    }
    findings.push({
      findingId: f.findingId || f.discoveryId || null,
      hotelName,
      city: city || null,
      stateRegion: stateRegion || null,
      brandFamily: brandFamily || null,
      brandLabel: brandLabel || null,
      dateMeaning: meaning,
      agreementType: f.agreementType || AGREEMENT_TYPE.UNKNOWN,
      primaryDate: iso,
      datePrecision: /^\d{4}$/.test(String(f.primaryDate || f.initialExpirationYear || ""))
        ? "YEAR"
        : "DAY",
      dateRaw: String(f.primaryDate || f.initialExpirationYear || iso),
      isHotelContractDate: true,
      isLoanMaturity: false,
      verificationClass: f.verificationClass,
      historicalApplicability: f.historicalApplicability,
      hotelAttributed: Boolean(f.hotelAttributed ?? hotelName),
      portfolioWide: Boolean(f.portfolioWide),
      counterparty: f.disclosedManager || f.franchisor || null,
      snippet: f.snippet || null,
      source: {
        ...(f.source || {}),
        cik: meta.cik || f.source?.cik || null,
        companyName: meta.companyName || f.source?.companyName || null,
        accessionNumber: meta.accessionNumber || f.source?.accessionNumber || null,
        formType: meta.form || f.source?.formType || null,
        filingDate: meta.filingDate || f.source?.filingDate || null,
        documentUrl: meta.documentUrl || f.source?.documentUrl || null,
        sourceFamily: meta.sourceFamily || SOURCE_FAMILY.HOTEL_REIT_SCHEDULE,
      },
      notes: ["BRIDGED_FROM_FRANCHISE_SCHEDULE_EXTRACTOR_V2"],
    });
  }
  return {
    ok: true,
    version: CONTRACT_TIMING_FRANCHISE_BRIDGE_V1,
    hotelContractFindings: findings,
    rejected,
    counts: { hotelContractDates: findings.length, rejected: rejected.length },
  };
}

export default {
  bridgeFranchiseScheduleToContractTiming,
  CONTRACT_TIMING_FRANCHISE_BRIDGE_V1,
};
