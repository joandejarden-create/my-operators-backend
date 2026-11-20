/**
 * Conversion / management-contract opportunity discovery from verified milestones.
 * Expiration is a research trigger — never proof of owner intent.
 */

import { VERIFICATION_CLASS } from "./contract.js";

export const OPPORTUNITY_DISCOVERY_VERSION = "sec-edgar-opportunity-discovery-v1";

export const OPPORTUNITY_WINDOWS = Object.freeze([
  { id: "0_6_MONTHS", minDays: 0, maxDays: 182 },
  { id: "6_12_MONTHS", minDays: 183, maxDays: 365 },
  { id: "12_24_MONTHS", minDays: 366, maxDays: 730 },
  { id: "24_36_MONTHS", minDays: 731, maxDays: 1095 },
]);

function parseLooseDate(s) {
  if (!s) return null;
  const t = Date.parse(String(s));
  if (Number.isFinite(t)) return new Date(t);
  const m = String(s).match(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})\b/i,
  );
  if (m) return new Date(`${m[1]} ${m[2]}, ${m[3]}`);
  return null;
}

function windowForDate(date, now = new Date()) {
  if (!date) return null;
  const days = Math.round((date.getTime() - now.getTime()) / 86400000);
  if (days < 0) return { id: "PAST_OR_HISTORICAL", days };
  for (const w of OPPORTUNITY_WINDOWS) {
    if (days >= w.minDays && days <= w.maxDays) return { id: w.id, days };
  }
  return { id: "BEYOND_36_MONTHS", days };
}

/**
 * Score separates evidence confidence from commercial attractiveness.
 */
export function scoreOpportunity(input = {}) {
  const evidence =
    input.verificationClass === VERIFICATION_CLASS.VERIFIED_EXPIRATION
      ? 0.8
      : input.verificationClass === VERIFICATION_CLASS.VERIFIED_TERMINATION
        ? 0.85
        : input.verificationClass === VERIFICATION_CLASS.VERIFIED_RENEWAL_OPTION
          ? 0.55
          : 0.3;
  const timing =
    input.windowId === "0_6_MONTHS"
      ? 1
      : input.windowId === "6_12_MONTHS"
        ? 0.85
        : input.windowId === "12_24_MONTHS"
          ? 0.65
          : input.windowId === "24_36_MONTHS"
            ? 0.4
            : 0.15;
  const commercial = Math.min(1, timing * 0.7 + (input.hasOwnerRoute ? 0.2 : 0) + (input.roomsKnown ? 0.1 : 0));
  return {
    evidenceConfidence: Number(evidence.toFixed(3)),
    commercialAttractiveness: Number(commercial.toFixed(3)),
    note: "High commercial score must never upgrade weak evidence.",
  };
}

export function buildOpportunityDossiers(attributedVerified = [], opts = {}) {
  const now = opts.now || new Date();
  const dossiers = [];
  for (const row of attributedVerified) {
    const f = row.finding || {};
    if (!row.persistable && !opts.includeNonPersistable) continue;
    const interesting = [
      VERIFICATION_CLASS.VERIFIED_EXPIRATION,
      VERIFICATION_CLASS.VERIFIED_TERMINATION,
      VERIFICATION_CLASS.VERIFIED_RENEWAL_OPTION,
      VERIFICATION_CLASS.VERIFIED_RENEWAL_NOTICE_DEADLINE,
      VERIFICATION_CLASS.CONVERSION_SIGNAL,
    ].includes(f.verificationClass);
    if (!interesting) continue;

    const dt = parseLooseDate(f.primaryDate);
    const win = windowForDate(dt, now);
    const scores = scoreOpportunity({
      verificationClass: f.verificationClass,
      windowId: win?.id,
      hasOwnerRoute: Boolean(opts.ownerRouteByHpc?.[f.hpcHotelId]),
      roomsKnown: Boolean(opts.roomsByHpc?.[f.hpcHotelId]),
    });

    dossiers.push({
      hotelName: f.attributedHotelName || f.hotelNameHints?.[0] || null,
      hpcHotelId: f.hpcHotelId || null,
      city: opts.cityByHpc?.[f.hpcHotelId] || null,
      country: opts.countryByHpc?.[f.hpcHotelId] || null,
      currentBrand: opts.brandByHpc?.[f.hpcHotelId] || null,
      rooms: opts.roomsByHpc?.[f.hpcHotelId] || null,
      owner: opts.ownerByHpc?.[f.hpcHotelId] || null,
      managementCompany: opts.operatorByHpc?.[f.hpcHotelId] || null,
      contractType: f.agreementType,
      milestones: [
        {
          class: f.verificationClass,
          date: f.primaryDate,
          fieldName: f.evidenceFieldName,
        },
      ],
      evidence: {
        url: f.source?.documentUrl,
        accession: f.source?.accessionNumber,
        form: f.source?.formType,
        snippet: String(f.snippet || "").slice(0, 400),
      },
      window: win,
      scores,
      conversionThesis:
        "Contract milestone is a research trigger only — not evidence the owner seeks a new brand or manager.",
      recommendedNextAction:
        "Confirm current applicability in subsequent filings; resolve decision-maker route on Contact Intelligence; do not outreach on expiration alone.",
      publication: "INTERNAL_ONLY",
    });
  }

  return {
    version: OPPORTUNITY_DISCOVERY_VERSION,
    count: dossiers.length,
    dossiers,
  };
}
