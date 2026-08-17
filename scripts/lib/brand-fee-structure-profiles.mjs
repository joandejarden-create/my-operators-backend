/**
 * Brand Setup - Fee Structure profiles for all Brand Basics rows.
 * Resolve: brand override → Choice FDD → parent template → default.
 * Percent fields are Airtable decimals (0–1). Dollar fees are numbers.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  CHOICE_FEE_FDD_FILE,
  CHOICE_FEE_TIER,
  CHOICE_FEE_OVERRIDES,
} from "./choice-fee-structure-profiles.mjs";
import { BRAND_TO_CHOICE_FDD_KEY } from "./brand-deal-terms-profiles.mjs";
import { readFddText, parseChoiceFddItem6Fees } from "./parse-choice-fdd-item6-fees.mjs";
import { FDD_FIELD_DISCLAIMER } from "../../lib/external-owner-copy.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const D = FDD_FIELD_DISCLAIMER;
const DIR =
  "Directionally accurate brand-typical estimate for matching—not a property-specific quote. Confirm against current brand documents.";

function pctPointsToDecimal(p) {
  if (p == null || Number.isNaN(Number(p))) return null;
  return Math.round(Number(p) * 1000) / 100000;
}

function baseFranchiseFees(o = {}) {
  const isMgmt = o.cohort === "luxury-mgmt" || o.dealModel === "management";
  return {
    sourceTier: "directional",
    cohort: o.cohort || "generic-franchise",
    dealModel: o.dealModel || (isMgmt ? "management" : "franchise"),
    appMin: o.appMin ?? 50000,
    appMax: o.appMax ?? 100000,
    appBasis: o.appBasis ?? "Per Application",
    appNotes: o.appNotes ?? DIR,
    appPerRoom: o.appPerRoom ?? null,
    appThreshold: o.appThreshold ?? null,
    royaltyMin: o.royaltyMin ?? 0.05,
    royaltyMax: o.royaltyMax ?? 0.06,
    royaltyBasis: o.royaltyBasis ?? "% of Rooms Revenue",
    royaltyNotes: o.royaltyNotes ?? DIR,
    marketingMin: o.marketingMin ?? 0.03,
    marketingMax: o.marketingMax ?? 0.04,
    marketingBasis: o.marketingBasis ?? "% of Rooms Revenue",
    marketingNotes: o.marketingNotes ?? DIR,
    techMin: o.techMin ?? 10,
    techMax: o.techMax ?? 15,
    techBasis: o.techBasis ?? "Per Room / Month",
    techNotes: o.techNotes ?? DIR,
    loyaltyMin: o.loyaltyMin ?? 0.04,
    loyaltyMax: o.loyaltyMax ?? 0.055,
    loyaltyBasis: o.loyaltyBasis ?? "% of Gross Revenue",
    loyaltyNotes: o.loyaltyNotes ?? DIR,
    reservationMin: o.reservationMin ?? null,
    reservationMax: o.reservationMax ?? null,
    reservationBasis: o.reservationBasis ?? null,
    reservationNotes: o.reservationNotes ?? null,
    trainingMin: o.trainingMin ?? 5000,
    trainingMax: o.trainingMax ?? 25000,
    trainingBasis: o.trainingBasis ?? "One-Time",
    trainingNotes: o.trainingNotes ?? DIR,
    incentives: o.incentives ?? "Negotiated case-by-case (conversions, multi-unit, strategic markets).",
    earlyTerm: o.earlyTerm ?? "Sometimes",
    earlyTermNotes: o.earlyTermNotes ?? "Cure periods and liquidated damages vary—confirm franchise agreement.",
    termStruct: o.termStruct ?? "Case-by-Case",
    termStructNotes: o.termStructNotes ?? "Liquidated damages / unrealized-fee formulas are agreement-specific.",
    perfTerm: o.perfTerm ?? "Mutual",
    keyMoney: o.keyMoney ?? "Only In Select Deals",
    reserves: o.reserves ?? "FF&E / PIP reserves per brand standards—underwrite from deal docs.",
    capReimb: o.capReimb ?? "No",
    auditRights: o.auditRights ?? "Yes",
    feeBand: o.feeBand ?? "In Line with Market",
    pipBand: o.pipBand ?? "Moderate",

    // Extended Meta columns (not on Brand Setup HTML form today)
    otherMin: o.otherMin ?? (isMgmt ? null : 2000),
    techFeeRangeMax: o.techFeeRangeMax ?? (isMgmt ? null : 12000),
    techFeeRangeBasis: o.techFeeRangeBasis ?? (isMgmt ? null : "One-Time Fee"),
    otherProgramMin: o.otherProgramMin ?? (isMgmt ? null : 2500),
    otherProgramMax: o.otherProgramMax ?? (isMgmt ? null : 15000),
    otherProgramBasis: o.otherProgramBasis ?? (isMgmt ? null : "Per Program"),
    feeDescription:
      o.feeDescription ??
      (isMgmt
        ? `Management agreements may include base management, incentive, centralized services, and owner-funded reserves. ${DIR}`
        : `Other required fees may include PIP / QA inspections, opening process services, and mandatory system installs—confirm FDD Items 5–7. ${DIR}`),

    mgmtMin: o.mgmtMin ?? (isMgmt ? 0.02 : null),
    mgmtMax: o.mgmtMax ?? (isMgmt ? 0.03 : null),
    mgmtBasis: o.mgmtBasis ?? (isMgmt ? "% of Total Revenue" : null),
    incentiveMin: o.incentiveMin ?? (isMgmt ? "10%" : null),
    incentiveMax: o.incentiveMax ?? (isMgmt ? "20%" : null),
    incentiveBasis: o.incentiveBasis ?? (isMgmt ? "% of Net Operating Income" : null),
    incentiveNotes:
      o.incentiveNotes ??
      (isMgmt
        ? `Incentive / promote typically paid above an agreed NOI or preferred-return hurdle. ${DIR}`
        : null),
    incentiveExcessMin: o.incentiveExcessMin ?? (isMgmt ? 0.1 : null),
    incentiveExcessMax: o.incentiveExcessMax ?? (isMgmt ? 0.2 : null),
    incentiveExcessBasis: o.incentiveExcessBasis ?? (isMgmt ? "% of NOI above hurdle" : null),
    incentiveExcessNotes:
      o.incentiveExcessNotes ??
      (isMgmt ? `Applies to NOI (or cash flow) above the negotiated hurdle. ${DIR}` : null),

    ...o.extra,
  };
}

function membershipFees(o = {}) {
  return baseFranchiseFees({
    cohort: "membership",
    dealModel: "membership",
    sourceTier: "directional",
    appMin: o.appMin ?? 5000,
    appMax: o.appMax ?? 25000,
    appBasis: "One-Time",
    appNotes: `Membership / referral network joining fee (directional). ${DIR}`,
    royaltyMin: o.royaltyMin ?? 0.02,
    royaltyMax: o.royaltyMax ?? 0.04,
    royaltyBasis: "% of Rooms Revenue",
    royaltyNotes: `Membership / referral commission or dues-like assessment (directional). ${DIR}`,
    marketingMin: null,
    marketingMax: null,
    marketingBasis: null,
    techMin: null,
    techMax: null,
    techBasis: null,
    loyaltyMin: null,
    loyaltyMax: null,
    loyaltyBasis: null,
    reservationMin: null,
    reservationMax: null,
    trainingMin: 0,
    trainingMax: 5000,
    trainingNotes: `Limited onboarding; not a classic franchise training package. ${DIR}`,
    incentives: "Membership networks rarely offer key money; commercial terms are deal-specific.",
    earlyTerm: "Yes",
    termStruct: "Typically None",
    termStructNotes: `Membership exit fees (if any) are network-specific. ${DIR}`,
    perfTerm: "Rarely Exercised / Case-by-Case",
    keyMoney: "No",
    feeBand: "Below Market / Flexible",
    pipBand: "Low",
    techFeeRangeMax: null,
    techFeeRangeBasis: null,
    otherProgramMin: 500,
    otherProgramMax: 5000,
    otherProgramBasis: "Per Program",
    feeDescription: `Membership may charge annual dues, marketing co-op, or inspection fees outside classic franchise Item 6 schedules. ${DIR}`,
    mgmtMin: null,
    mgmtMax: null,
    mgmtBasis: null,
    incentiveMin: null,
    incentiveMax: null,
    incentiveBasis: null,
    incentiveNotes: null,
    incentiveExcessMin: null,
    incentiveExcessMax: null,
    incentiveExcessBasis: null,
    incentiveExcessNotes: null,
    ...o,
  });
}

function buildChoiceProfile(basicsName, fddKey) {
  const tier = CHOICE_FEE_TIER[fddKey];
  if (!tier) {
    return baseFranchiseFees({
      sourceTier: "directional",
      cohort: "choice",
      appMin: 5000,
      appMax: 75000,
      appBasis: "Base + Per Room Over Threshold",
      appPerRoom: 500,
      appThreshold: 100,
      royaltyMin: 0.05,
      royaltyMax: 0.06,
      marketingMin: 0.025,
      marketingMax: 0.035,
      marketingNotes: `Choice-style combined marketing/reservation assessment (directional). ${DIR}`,
      techMin: 9,
      techMax: 11,
      loyaltyMin: 0.045,
      loyaltyMax: 0.055,
      // Reservation intentionally omitted — often combined into marketing in Choice FDD Item 6
      trainingMin: 3345,
      trainingMax: 5295,
      feeBand: "In Line with Market",
      pipBand: "Moderate",
    });
  }

  const fddFile = CHOICE_FEE_FDD_FILE[fddKey];
  const text = fddFile ? readFddText(fddFile) : null;
  let parsed = text ? parseChoiceFddItem6Fees(text) : {};
  const ov = CHOICE_FEE_OVERRIDES[fddKey];
  if (ov) {
    parsed = {
      ...parsed,
      royaltyMin: ov.royaltyMin ?? parsed.royaltyMin,
      royaltyMax: ov.royaltyMax ?? parsed.royaltyMax,
      marketingReservationPct: ov.marketingReservationPct ?? parsed.marketingReservationPct,
      loyaltyMin: ov.loyaltyMin ?? parsed.loyaltyMin,
      loyaltyMax: ov.loyaltyMax ?? parsed.loyaltyMax,
      techPerRoomMonthly: ov.techPerRoomMonthly ?? parsed.techPerRoomMonthly,
    };
  }

  const roy = parsed.royaltyMin;
  const royMax = parsed.royaltyMax ?? parsed.royaltyMin;
  const mr = parsed.marketingReservationPct;
  let lMin = parsed.loyaltyMin;
  let lMax = parsed.loyaltyMax;
  if (lMin == null && lMax == null) {
    lMin = 4.5;
    lMax = 5.5;
  }
  const tech = parsed.techPerRoomMonthly ?? tier.techFallback;

  return {
    sourceTier: "fdd",
    cohort: "choice",
    fddFile,
    appMin: tier.appMin,
    appMax: tier.appMax,
    appBasis: "Base + Per Room Over Threshold",
    appPerRoom: tier.appPerRoom,
    appThreshold: tier.appThresholdRooms,
    appNotes: D,
    royaltyMin: roy != null ? pctPointsToDecimal(roy) : 0.05,
    royaltyMax: royMax != null ? pctPointsToDecimal(royMax) : 0.06,
    royaltyBasis: "% of Rooms Revenue",
    royaltyNotes: D,
    marketingMin: mr != null ? pctPointsToDecimal(mr) : 0.03,
    marketingMax: mr != null ? pctPointsToDecimal(mr) : 0.03,
    marketingBasis: "% of Rooms Revenue",
    marketingNotes:
      "Choice Item 6 lists a combined Marketing and Reservation Fee; the same percentage is recorded here. Reservation / distribution line left blank unless separately stated in the FDD.",
    techMin: tech,
    techMax: tech,
    techBasis: "Per Room / Month",
    techNotes: D,
    loyaltyMin: lMin != null ? pctPointsToDecimal(lMin) : 0.045,
    loyaltyMax: lMax != null ? pctPointsToDecimal(lMax) : 0.055,
    loyaltyBasis: "% of Gross Revenue",
    loyaltyNotes: D,
    // omit reservation min/max/basis
    reservationMin: null,
    reservationMax: null,
    reservationBasis: null,
    trainingMin: 3345,
    trainingMax: 5295,
    trainingBasis: "One-Time",
    trainingNotes:
      "Immersion + HOST pre-opening training (Choice Item 5 template); excludes travel. Confirm current tuition in FDD Item 11.",
    incentives:
      "Negotiated case-by-case (conversions, multi-unit, strategic markets). Confirm active Choice development incentives at signing.",
    earlyTerm: "Sometimes",
    earlyTermNotes:
      "Cure periods and liquidated damages vary by agreement generation—review FDD Item 17 and franchise agreement.",
    termStruct: "Case-by-Case",
    termStructNotes:
      "Liquidated damages and unrealized-fee formulas are agreement-specific; model using FDD Item 17 and legal review.",
    perfTerm: "Mutual",
    keyMoney: "Only In Select Deals",
    reserves:
      "FF&E reserves, PIP reserves, and replacement cycles per brand standards and PIP schedule—underwrite from FDD Item 7 and architecture.",
    capReimb: "No",
    auditRights: "Yes",
    feeBand: tier.feeBand,
    pipBand: tier.pipBand,
    brandLabel: basicsName,
    techFeeRangeMax: 8000,
    techFeeRangeBasis: "One-Time Fee",
    otherProgramMin: 1500,
    otherProgramMax: 12000,
    otherProgramBasis: "Per Program",
    feeDescription: `Choice Item 5/6 may also require PIP inspections, opening process fees, and system installs beyond ongoing royalty/marketing. Reservation fees are often combined into the marketing line. ${D}`,
    otherMin: 2000,
    mgmtMin: null,
    mgmtMax: null,
    mgmtBasis: null,
    incentiveMin: null,
    incentiveMax: null,
    incentiveBasis: null,
  };
}

function loadKimptonFromFixture() {
  const p = path.join(__dirname, "..", "..", "fixtures", "kimpton-fdd-economics.json");
  if (!fs.existsSync(p)) return null;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const patch = j.feeStructurePatch || j.dealTermsPatch;
  if (!patch || !j.parsed) return null;
  const i5 = j.parsed.item5 || {};
  const i6 = j.parsed.item6 || {};
  return {
    sourceTier: "fdd",
    cohort: "ihg",
    appMin: patch["Min - Typical Application Fee"] ?? 100000,
    appMax: patch["Max - Typical Application Fee"] ?? 150000,
    appBasis: patch["Basis - Typical Application Fee"] ?? "Per Application",
    appNotes: patch["Additional Notes - Typical Application Fee"] ?? D,
    royaltyMin: i6.royaltyRoomPct ?? 0.06,
    royaltyMax: i6.royaltyRoomPct ?? 0.06,
    royaltyBasis: "% of Gross Revenue",
    royaltyNotes: patch["Additional Notes - Typical Royalty Fee Range"] ?? D,
    marketingMin: i6.servicesContributionPct ?? 0.03,
    marketingMax: i6.servicesContributionPct ?? 0.03,
    marketingBasis: "% of Gross Revenue",
    marketingNotes: patch["Additional Notes - Typical Marketing Fee Range"] ?? D,
    techMin: i6.technologyPerRoomMonthly ?? 17,
    techMax: i6.technologyPerRoomMonthly ?? 17,
    techBasis: "Per Room / Month",
    techNotes: patch["Additional Notes - Typical Tech"] ?? D,
    loyaltyMin: i6.loyaltyRoomMeetingPct ?? 0.01365,
    loyaltyMax: i6.loyaltyFullFolioPct ?? 0.0455,
    loyaltyBasis: "% of Gross Revenue",
    loyaltyNotes: patch["Additional Notes - Typical Loyalty Program Fee"] ?? D,
    reservationMin: i6.gdsPerReservation ?? 6.4,
    reservationMax: i6.gdsPerReservation ?? 6.4,
    reservationBasis: "Per Reservation / Per Booking",
    reservationNotes: patch["Additional Notes - Typical Reservation / Distribution Fee"] ?? D,
    trainingMin: patch["Min - Typical Training Fee"] ?? 20000,
    trainingMax: patch["Max - Typical Training Fee"] ?? 60000,
    trainingBasis: "One-Time",
    trainingNotes: patch["Additional Notes - Typical Training Fee"] ?? D,
    incentives: patch["Typical Incentives Offered"],
    termStruct: "Case-by-Case",
    termStructNotes: patch["Typical Termination Fee Structure (if any) Text"] ?? D,
    perfTerm: "Mutual",
    keyMoney: "Only In Select Deals",
    reserves: patch["Typical Expectations for Owner-Funded Reserves"],
    earlyTerm: "Sometimes",
    earlyTermNotes: "See FDD Item 17 cure / LD framework.",
    capReimb: "No",
    auditRights: "Yes",
    feeBand: "Premium",
    pipBand: "High",
    techFeeRangeMax: 25000,
    techFeeRangeBasis: "One-Time Fee",
    otherProgramMin: 5500,
    otherProgramMax: 35000,
    otherProgramBasis: "Per Program",
    feeDescription: `IHG Kimpton: additional Item 5 fees may include PIP inspection, pre-opening support, and learning/certification programs beyond ongoing royalty/services. ${D}`,
    mgmtMin: null,
    mgmtMax: null,
    mgmtBasis: null,
  };
}

function loadCurioFromFixture() {
  const p = path.join(__dirname, "..", "..", "fixtures", "curio-fdd-economics.json");
  if (!fs.existsSync(p)) return null;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const i5 = j.parsed?.item5 || {};
  const i6 = j.parsed?.item6 || {};
  return {
    sourceTier: "fdd",
    cohort: "hilton",
    appMin: i5.applicationFeeNewDevBase ?? 85000,
    appMax: Math.max(i5.applicationFeeChangeOfOwnership ?? 150000, 150000),
    appBasis: "Base + Per Room Over Threshold",
    appPerRoom: i5.applicationFeePerRoomOver250 ?? 400,
    appThreshold: 250,
    appNotes: `Curio FDD: base application fee with per-room over threshold; change-of-ownership higher. ${D}`,
    royaltyMin: i6.royaltyRoomPct ?? 0.05,
    royaltyMax: i6.royaltyRoomPct ?? 0.05,
    royaltyBasis: "% of Rooms Revenue",
    royaltyNotes: D,
    marketingMin: i6.programFeePct ?? 0.04,
    marketingMax: i6.programFeePct ?? 0.04,
    marketingBasis: "% of Rooms Revenue",
    marketingNotes: `Program fee (marketing/system). Ramp may apply in early years. ${D}`,
    techMin: null,
    techMax: null,
    techBasis: null,
    techNotes: `ONQ / Hilton systems fees are multi-component (initial + monthly)—see FDD Item 6; not collapsed to a single $/room/month here. ${D}`,
    loyaltyMin: i6.honorsEligibleFolioPct ?? 0.04,
    loyaltyMax: i6.honorsEligibleFolioPct ?? 0.04,
    loyaltyBasis: "% of Gross Revenue",
    loyaltyNotes: `Hilton Honors assessment on eligible folio. ${D}`,
    reservationMin: i6.thirdPartyReservationPerStay ?? 6.05,
    reservationMax: i6.thirdPartyReservationPerStay ?? 6.05,
    reservationBasis: "Per Reservation / Per Booking",
    reservationNotes: `Third-party reservation fee per stay; digital advance % may also apply. ${D}`,
    trainingMin: i5.trainingMin ?? 5000,
    trainingMax: i5.trainingMax ?? 15000,
    trainingBasis: "One-Time",
    trainingNotes: D,
    incentives: "Key money and fee ramps may be available for strategic Curio conversions—deal-specific.",
    earlyTerm: "Sometimes",
    earlyTermNotes: D,
    termStruct: "Case-by-Case",
    termStructNotes: D,
    perfTerm: "Mutual",
    keyMoney: "Only In Select Deals",
    reserves: "FF&E / PIP reserves per Hilton soft-brand standards.",
    capReimb: "No",
    auditRights: "Yes",
    feeBand: "Premium",
    pipBand: "High",
    techFeeRangeMax: i5.onqInitialMax ?? 218800,
    techFeeRangeBasis: "One-Time Fee",
    otherProgramMin: i5.pipFee ?? 10000,
    otherProgramMax: (i5.pipFee ?? 10000) + (i5.openingProcessServicesFee ?? 20000),
    otherProgramBasis: "Per Program",
    otherMin: i5.openingProcessServicesFee ?? 20000,
    feeDescription: `Curio FDD Item 5/6: ONQ initial systems, PIP fee, opening process services, and other Hilton program fees may apply in addition to royalty/program/Honors. ${D}`,
    mgmtMin: null,
    mgmtMax: null,
    mgmtBasis: null,
  };
}
export const BRAND_FEE_OVERRIDES = Object.freeze({
  "Kimpton Hotels": loadKimptonFromFixture() || baseFranchiseFees({ sourceTier: "fdd", cohort: "ihg", feeBand: "Premium", pipBand: "High" }),
  "Curio Collection by Hilton": loadCurioFromFixture() || baseFranchiseFees({ sourceTier: "fdd", cohort: "hilton", feeBand: "Premium" }),
  "Tapestry Collection by Hilton": baseFranchiseFees({
    sourceTier: "directional",
    cohort: "hilton",
    appMin: 75000,
    appMax: 150000,
    royaltyMin: 0.05,
    royaltyMax: 0.05,
    marketingMin: 0.035,
    marketingMax: 0.04,
    loyaltyMin: 0.04,
    loyaltyMax: 0.04,
    reservationMin: 5,
    reservationMax: 7,
    reservationBasis: "Per Reservation / Per Booking",
    feeBand: "Premium",
    pipBand: "High",
    royaltyNotes: `Hilton soft-brand directional economics (Tapestry). ${DIR}`,
  }),
  "Hotel Indigo": baseFranchiseFees({
    sourceTier: "directional",
    cohort: "ihg",
    appMin: 75000,
    appMax: 125000,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.03,
    marketingMax: 0.035,
    techMin: 15,
    techMax: 20,
    loyaltyMin: 0.02,
    loyaltyMax: 0.05,
    reservationMin: 5,
    reservationMax: 8,
    reservationBasis: "Per Reservation / Per Booking",
    trainingMin: 15000,
    trainingMax: 45000,
    feeBand: "Premium",
    pipBand: "High",
  }),
  "Vignette Collection": baseFranchiseFees({
    sourceTier: "directional",
    cohort: "ihg",
    appMin: 75000,
    appMax: 150000,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.03,
    marketingMax: 0.04,
    feeBand: "Premium",
    pipBand: "High",
  }),
  "Handwritten Collection": baseFranchiseFees({
    sourceTier: "directional",
    cohort: "ihg",
    appMin: 50000,
    appMax: 100000,
    royaltyMin: 0.04,
    royaltyMax: 0.055,
    marketingMin: 0.025,
    marketingMax: 0.035,
    feeBand: "In Line with Market",
    pipBand: "Moderate",
  }),
  "Autograph Collection": baseFranchiseFees({
    sourceTier: "directional",
    cohort: "marriott",
    appMin: 85000,
    appMax: 150000,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.03,
    marketingMax: 0.04,
    loyaltyMin: 0.045,
    loyaltyMax: 0.055,
    feeBand: "Premium",
    pipBand: "High",
  }),
  "Tribute Portfolio": baseFranchiseFees({
    sourceTier: "directional",
    cohort: "marriott",
    appMin: 75000,
    appMax: 140000,
    royaltyMin: 0.05,
    royaltyMax: 0.055,
    marketingMin: 0.03,
    marketingMax: 0.04,
    feeBand: "Premium",
    pipBand: "Moderate",
  }),
  "Design Hotels": membershipFees({
    appMin: 10000,
    appMax: 40000,
    royaltyMin: 0.02,
    royaltyMax: 0.035,
    feeBand: "Below Market / Flexible",
    pipBand: "Low",
    royaltyNotes: `Design Hotels affiliation / membership-style economics (directional). ${DIR}`,
  }),
  "BW Premier Collection": membershipFees({
    appMin: 5000,
    appMax: 20000,
    royaltyMin: 0.03,
    royaltyMax: 0.045,
    feeBand: "Below Market / Flexible",
    pipBand: "Low",
  }),
  "BW Signature Collection": membershipFees({
    appMin: 5000,
    appMax: 20000,
    royaltyMin: 0.025,
    royaltyMax: 0.04,
    feeBand: "Below Market / Flexible",
    pipBand: "Low",
  }),
  "MGallery Collection": baseFranchiseFees({
    sourceTier: "directional",
    cohort: "accor",
    appMin: 60000,
    appMax: 120000,
    royaltyMin: 0.045,
    royaltyMax: 0.055,
    marketingMin: 0.025,
    marketingMax: 0.035,
    feeBand: "Premium",
    pipBand: "Moderate",
  }),
  "Preferred Hotels & Resorts": membershipFees({
    appMin: 5000,
    appMax: 30000,
    royaltyMin: 0.02,
    royaltyMax: 0.04,
  }),
  "Small Luxury Hotels of the World": membershipFees({
    appMin: 5000,
    appMax: 25000,
    royaltyMin: 0.02,
    royaltyMax: 0.035,
  }),
  "The Leading Hotels of the World": membershipFees({
    appMin: 10000,
    appMax: 40000,
    royaltyMin: 0.025,
    royaltyMax: 0.04,
  }),
  "Mr & Mrs Smith": membershipFees({
    appMin: 3000,
    appMax: 15000,
    royaltyMin: 0.15,
    royaltyMax: 0.2,
    royaltyBasis: "% of Rooms Revenue",
    royaltyNotes: `Mr & Mrs Smith commission-style economics on booked stays (directional; model carefully). ${DIR}`,
    feeBand: "Performance-Linked / Flexible",
  }),
});

export const PARENT_FEE_TEMPLATES = Object.freeze({
  "Choice Hotels International": baseFranchiseFees({
    cohort: "choice",
    appMin: 5000,
    appMax: 75000,
    appBasis: "Base + Per Room Over Threshold",
    appPerRoom: 500,
    appThreshold: 100,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.025,
    marketingMax: 0.035,
    marketingNotes: `Choice-style combined marketing/reservation (directional). ${DIR}`,
    techMin: 8,
    techMax: 12,
    loyaltyMin: 0.045,
    loyaltyMax: 0.055,
    trainingMin: 3000,
    trainingMax: 6000,
    feeBand: "In Line with Market",
  }),
  "Hilton Worldwide": baseFranchiseFees({
    cohort: "hilton",
    appMin: 75000,
    appMax: 150000,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.035,
    marketingMax: 0.04,
    loyaltyMin: 0.04,
    loyaltyMax: 0.045,
    reservationMin: 5,
    reservationMax: 8,
    reservationBasis: "Per Reservation / Per Booking",
    techMin: 12,
    techMax: 20,
    feeBand: "In Line with Market",
    pipBand: "Moderate",
  }),
  "Marriott International, Inc.": baseFranchiseFees({
    cohort: "marriott",
    appMin: 85000,
    appMax: 150000,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.03,
    marketingMax: 0.045,
    loyaltyMin: 0.045,
    loyaltyMax: 0.055,
    techMin: 12,
    techMax: 22,
    feeBand: "Premium",
    pipBand: "High",
  }),
  "InterContinental Hotels Group": baseFranchiseFees({
    cohort: "ihg",
    appMin: 75000,
    appMax: 125000,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.03,
    marketingMax: 0.035,
    loyaltyMin: 0.02,
    loyaltyMax: 0.05,
    techMin: 15,
    techMax: 20,
    reservationMin: 5,
    reservationMax: 8,
    reservationBasis: "Per Reservation / Per Booking",
    feeBand: "In Line with Market",
    pipBand: "Moderate",
  }),
  "Hyatt Hotels Corporation": baseFranchiseFees({
    cohort: "hyatt",
    appMin: 75000,
    appMax: 150000,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.03,
    marketingMax: 0.04,
    loyaltyMin: 0.04,
    loyaltyMax: 0.05,
    feeBand: "Premium",
  }),
  "Hyatt Vacation Ownership": baseFranchiseFees({
    cohort: "hyatt",
    appMin: 50000,
    appMax: 100000,
    royaltyMin: 0.04,
    royaltyMax: 0.06,
  }),
  "Wyndham Hotels & Resorts": baseFranchiseFees({
    cohort: "wyndham",
    appMin: 35000,
    appMax: 75000,
    royaltyMin: 0.045,
    royaltyMax: 0.055,
    marketingMin: 0.03,
    marketingMax: 0.04,
    loyaltyMin: 0.035,
    loyaltyMax: 0.05,
    techMin: 8,
    techMax: 14,
    feeBand: "In Line with Market",
  }),
  AccorHotels: baseFranchiseFees({
    cohort: "accor",
    appMin: 40000,
    appMax: 100000,
    royaltyMin: 0.04,
    royaltyMax: 0.055,
    marketingMin: 0.025,
    marketingMax: 0.035,
    feeBand: "In Line with Market",
  }),
  "BWH Hotels": membershipFees({
    appMin: 5000,
    appMax: 25000,
    royaltyMin: 0.03,
    royaltyMax: 0.05,
  }),
  "Sonesta International Hotels Corporation": baseFranchiseFees({
    cohort: "sonesta",
    appMin: 40000,
    appMax: 90000,
    royaltyMin: 0.045,
    royaltyMax: 0.055,
    marketingMin: 0.025,
    marketingMax: 0.035,
  }),
  "Radisson Hotel Group": baseFranchiseFees({
    cohort: "radisson-legacy",
    appMin: 40000,
    appMax: 90000,
    royaltyMin: 0.05,
    royaltyMax: 0.06,
    marketingMin: 0.025,
    marketingMax: 0.035,
  }),
  "Red Roof Franchise, UK": baseFranchiseFees({
    cohort: "red-roof",
    appMin: 15000,
    appMax: 45000,
    royaltyMin: 0.045,
    royaltyMax: 0.055,
    marketingMin: 0.025,
    marketingMax: 0.035,
    feeBand: "In Line with Market",
    pipBand: "Low",
  }),
  "Minor Hotel Group Limited": baseFranchiseFees({
    cohort: "minor",
    appMin: 50000,
    appMax: 120000,
    royaltyMin: 0.045,
    royaltyMax: 0.06,
  }),
  "Dovetail + Co": membershipFees({
    appMin: 5000,
    appMax: 20000,
    royaltyMin: 0.02,
    royaltyMax: 0.04,
  }),
  "Staycity Ltd": baseFranchiseFees({
    cohort: "staycity",
    appMin: 30000,
    appMax: 80000,
    royaltyMin: 0.04,
    royaltyMax: 0.05,
  }),
  "Banyan Tree Hotels & Resorts": baseFranchiseFees({
    cohort: "banyan",
    appMin: 100000,
    appMax: 200000,
    royaltyMin: 0.04,
    royaltyMax: 0.06,
    feeBand: "Premium",
    pipBand: "High",
  }),
  "Iberostar Hotels & Resorts": baseFranchiseFees({
    cohort: "iberostar",
    appMin: 75000,
    appMax: 150000,
    royaltyMin: 0.04,
    royaltyMax: 0.06,
    feeBand: "Premium",
  }),
  "Prem Group": baseFranchiseFees({
    cohort: "prem",
    appMin: 40000,
    appMax: 100000,
    royaltyMin: 0.04,
    royaltyMax: 0.055,
  }),
  "Four Seasons Hotels and Resorts": baseFranchiseFees({
    cohort: "luxury-mgmt",
    dealModel: "management",
    appMin: 0,
    appMax: 0,
    royaltyMin: 0.03,
    royaltyMax: 0.05,
    royaltyNotes: `Four Seasons is typically management-agreement economics (base + incentive), not classic franchise royalty. Directional for matching only. ${DIR}`,
    marketingMin: null,
    marketingMax: null,
    feeBand: "Premium",
    pipBand: "High",
    keyMoney: "No",
    mgmtMin: 0.02,
    mgmtMax: 0.03,
    mgmtBasis: "% of Total Revenue",
    incentiveMin: "10%",
    incentiveMax: "20%",
    incentiveBasis: "% of Net Operating Income",
    techFeeRangeMax: null,
    otherProgramMin: null,
    otherProgramMax: null,
  }),
  "Rosewood Hotel Group": baseFranchiseFees({
    cohort: "luxury-mgmt",
    royaltyMin: 0.03,
    royaltyMax: 0.05,
    feeBand: "Premium",
    pipBand: "High",
  }),
  "Shangri-La Hotels and Resorts": baseFranchiseFees({
    cohort: "luxury-mgmt",
    royaltyMin: 0.03,
    royaltyMax: 0.05,
    feeBand: "Premium",
  }),
  "Mandarin Oriental Hotel Group": baseFranchiseFees({
    cohort: "luxury-mgmt",
    royaltyMin: 0.03,
    royaltyMax: 0.05,
    feeBand: "Premium",
  }),
  "The Peninsula Hotels": baseFranchiseFees({
    cohort: "luxury-mgmt",
    royaltyMin: 0.03,
    royaltyMax: 0.05,
    feeBand: "Premium",
  }),
  "Oetker Hotels": baseFranchiseFees({
    cohort: "luxury-mgmt",
    royaltyMin: 0.03,
    royaltyMax: 0.05,
    feeBand: "Premium",
  }),
  "Aman Group": baseFranchiseFees({
    cohort: "luxury-mgmt",
    royaltyMin: 0.03,
    royaltyMax: 0.05,
    feeBand: "Premium",
    pipBand: "High",
  }),
  "Leading Hotels of the World": membershipFees({}),
  "Preferred Hotels & Resorts": membershipFees({}),
  "Small Luxury Hotels of the World": membershipFees({}),
  "AmeriVu Inn and Suites": baseFranchiseFees({
    appMin: 10000,
    appMax: 35000,
    royaltyMin: 0.04,
    royaltyMax: 0.05,
    pipBand: "Low",
  }),
  "Northland Properties": baseFranchiseFees({
    appMin: 20000,
    appMax: 50000,
    royaltyMin: 0.04,
    royaltyMax: 0.05,
  }),
  "Coast Hotels Limited": baseFranchiseFees({
    appMin: 25000,
    appMax: 60000,
    royaltyMin: 0.04,
    royaltyMax: 0.055,
  }),
  "Edyn Limited": baseFranchiseFees({
    appMin: 30000,
    appMax: 70000,
    royaltyMin: 0.04,
    royaltyMax: 0.05,
  }),
});

const DEFAULT_FEE = baseFranchiseFees({ cohort: "unknown-parent" });

/**
 * @returns {{ profile: object, resolveSource: string }}
 */
export function getFeeStructureProfile(brandName, parentCompany = "") {
  const name = String(brandName || "").trim();
  const parent = String(parentCompany || "").trim();

  if (BRAND_FEE_OVERRIDES[name]) {
    return { profile: { ...BRAND_FEE_OVERRIDES[name] }, resolveSource: "brand-override" };
  }

  const fddKey = BRAND_TO_CHOICE_FDD_KEY[name];
  if (fddKey) {
    return {
      profile: buildChoiceProfile(name, fddKey),
      resolveSource: `choice-fdd:${fddKey}`,
    };
  }

  if (parent && PARENT_FEE_TEMPLATES[parent]) {
    return {
      profile: { ...PARENT_FEE_TEMPLATES[parent] },
      resolveSource: `parent:${parent}`,
    };
  }

  return { profile: { ...DEFAULT_FEE }, resolveSource: "default" };
}
