/**
 * Track B V4 — Decision-window extraction dossiers (DRH/SVC/RHP deep dive).
 * CUSTOMER_BLOCKED. Year/month precision preserved; no invented ISO days.
 * Does not re-issue coverage-audit APLE/RHP IDs (tb_gsca_*).
 */
import { classifyCurrentStatus } from "./hif-track-b-historical-foundation-v1.js";

export const DRH_10K =
  "https://www.sec.gov/Archives/edgar/data/1298946/000129894626000013/drh-20251231.htm";
export const SVC_10K =
  "https://www.sec.gov/Archives/edgar/data/945394/000094539426000012/svc-20251231.htm";
export const RHP_10K =
  "https://www.sec.gov/Archives/edgar/data/1040829/000110465926019035/rhp-20251231x10k.htm";

/** A–E commercial decision-window classes (Track A qualifies E). */
export const WINDOW_CLASS = Object.freeze({
  A_HISTORICAL_COMPLETED: "A_HISTORICAL_COMPLETED",
  B_ACTIVE_NO_NEAR_TERM: "B_ACTIVE_NO_NEAR_TERM",
  C_FORWARD_CONTRACTUAL_WATCH: "C_FORWARD_CONTRACTUAL_WATCH",
  D_POTENTIAL_DECISION_WINDOW: "D_POTENTIAL_DECISION_WINDOW",
  E_VERIFIED_OPEN_OPPORTUNITY: "E_VERIFIED_OPEN_OPPORTUNITY",
});

function slug(v) {
  return String(v || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function hotel(row) {
  return {
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    persistEligible: true,
    contractEligible: row.contractEligible !== false,
    discoveryMethod: "TRACK_B_DECISION_WINDOW_V4",
    hpcMatchStatus: "PENDING_HPC_SEARCH",
    wave: "B25",
    publicationState: "CUSTOMER_BLOCKED",
    reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
    ...row,
  };
}

function parseMonthYear(mmYyyy) {
  const m = String(mmYyyy || "").match(/^(\d{1,2})\/(20\d{2})$/);
  if (!m) return { expirationDate: null, expirationYear: null, datePrecision: "UNKNOWN", impreciseDates: [] };
  const month = Number(m[1]);
  const year = Number(m[2]);
  return {
    expirationDate: null,
    expirationYear: year,
    expirationMonth: month,
    datePrecision: "MONTH",
    impreciseDates: [`${year}-${String(month).padStart(2, "0")}`],
    milestoneDate: null,
  };
}

/** Near-term DRH franchise expirations (month precision). */
export const DRH_NEAR_FRANCHISE = [
  {
    hotelName: "Westin Boston Seaport District",
    city: "Boston",
    stateProvince: "Massachusetts",
    brand: "Westin",
    mmYyyy: "12/2026",
    feeNote: "6%/2% F&B; fees increase Jan 2026 to 7%/3%",
    windowClass: WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW,
  },
  {
    hotelName: "Courtyard Denver Downtown",
    city: "Denver",
    stateProvince: "Colorado",
    brand: "Courtyard",
    mmYyyy: "10/2027",
    feeNote: "5.5% of gross room sales",
    windowClass: WINDOW_CLASS.C_FORWARD_CONTRACTUAL_WATCH,
  },
];

/** Near-term DRH HMA current-term ends (≤2026) that are also at-will. */
export const DRH_NEAR_HMA = [
  { hotelName: "Hotel Clio", city: "Denver", stateProvince: "Colorado", manager: "Sage Hospitality", mmYyyy: "5/2026", renewals: "One five-year period", terminable: "At will with no fee" },
  { hotelName: "The Gwen Hotel", city: "Chicago", stateProvince: "Illinois", manager: "HEI Hotels & Resorts", mmYyyy: "6/2026", renewals: "None", terminable: "At will with no fee" },
  { hotelName: "Bourbon Orleans Hotel", city: "New Orleans", stateProvince: "Louisiana", manager: "Aimbridge Hospitality", mmYyyy: "7/2026", renewals: "Month-to-month", terminable: "At will with no fee" },
  { hotelName: "Courtyard Denver Downtown", city: "Denver", stateProvince: "Colorado", manager: "Sage Hospitality", mmYyyy: "7/2026", renewals: "One five-year period", terminable: "At will with no fee" },
  { hotelName: "Henderson Park Inn", city: "Destin", stateProvince: "Florida", manager: "Aimbridge Hospitality", mmYyyy: "7/2026", renewals: "Month-to-month", terminable: "At will with no fee" },
  { hotelName: "Westin Boston Seaport District", city: "Boston", stateProvince: "Massachusetts", manager: "Aimbridge Hospitality", mmYyyy: "12/2026", renewals: "Month-to-month", terminable: "At will with no fee" },
];

/** Historical completed manager replacements (exact day). */
export const DRH_HISTORICAL_HIGHGATE = [
  {
    hotelName: "Courtyard New York Manhattan/Fifth Avenue",
    city: "New York",
    stateProvince: "New York",
    priorManager: "Prior manager unnamed (table shows Highgate as of post-2026-02-03)",
    newManager: "Highgate Hotels",
    effectiveDate: "2026-02-03",
    footnote: 2,
    note: "HMA table row marked (2): Effective 2026-02-03, new Highgate HMA expires February 2036 with one five-year renewal option; terminable at will with no fee. Prior manager name not disclosed.",
  },
  {
    hotelName: "Hilton Garden Inn New York/Times Square Central",
    city: "New York",
    stateProvince: "New York",
    priorManager: "Prior manager unnamed (table shows Highgate as of post-2026-02-03)",
    newManager: "Highgate Hotels",
    effectiveDate: "2026-02-03",
    footnote: 2,
    note: "HMA table row marked (2)/(4): Effective 2026-02-03, new Highgate HMA expires February 2036 with one five-year renewal option; terminable at will with no fee. Prior manager name not disclosed.",
  },
  {
    hotelName: "Courtyard New York Manhattan/Midtown East",
    city: "New York",
    stateProvince: "New York",
    priorManager: "HEI Hotels & Resorts",
    newManager: "Highgate Hotels",
    effectiveDate: "2026-02-03",
    footnote: 3,
    note: "HMA table row marked (3): Effective 2026-02-03, HEI HMA terminated; Highgate HMA expires February 2036 with one five-year renewal option; terminable at will with no fee.",
  },
];

function drhFranchiseDossiers() {
  return DRH_NEAR_FRANCHISE.map((h) => {
    const dates = parseMonthYear(h.mmYyyy);
    const key = slug(h.hotelName);
    const isD = h.windowClass === WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW;
    return hotel({
      discoveryPropertyId: `disc_tb_v4_drh_${key}`,
      findingId: `find_tb_v4_drh_${key}_franchise_${dates.expirationYear}${String(dates.expirationMonth).padStart(2, "0")}`,
      contractId: `ctr_tb_v4_drh_${key}_franchise_${dates.expirationYear}${String(dates.expirationMonth).padStart(2, "0")}`,
      hotelName: h.hotelName,
      hotelNameNormalized: h.hotelName.toLowerCase(),
      city: h.city,
      stateProvince: h.stateProvince,
      physicalCountry: "US",
      currentBrand: h.brand,
      owner: "DiamondRock Hospitality Company",
      counterpartyName: "Franchisor (brand company per affiliation)",
      agreementType: "FRANCHISE",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "BRAND_AFFILIATION",
      findingType: "BRAND_AFFILIATION",
      ...dates,
      sourceUrl: DRH_10K,
      sourcePublicationDate: "2026-02-27",
      sourceVintage: "2025",
      evidenceSnippet:
        `DRH YE2025 10-K Hotel Franchise Agreements table: ${h.hotelName} franchise expiration ${h.mmYyyy}. ${h.feeNote}. ` +
        `Month precision only — day not disclosed; not stored as ISO milestone. ` +
        (isD
          ? "Near-term franchise expiry may create a brand decision window; renewal/notice mechanics and owner intent NOT disclosed — class D requiring Track A validation, NOT verified open BCT."
          : "Forward contractual watch — expiry alone is not a verified conversion opportunity."),
      unresolvedTerms: ["Exact expiration day", "Nonrenewal notice window", "Owner renewal intent", "PIP / relicensing conditions"],
      windowClass: h.windowClass,
      hpcQuery: h.hotelName,
      trackA: isD ? "POTENTIAL_BCT_WINDOW_NEEDS_VALIDATION" : "CONTRACTUAL_WATCH_NOT_OPEN",
      trackAPayload: {
        hotelName: h.hotelName,
        owner: "DiamondRock Hospitality",
        signal: "FRANCHISE_EXPIRATION_MONTH",
        month: dates.impreciseDates[0],
        windowClass: h.windowClass,
        limitation: "Expiry alone ≠ verified open BCT; notice/renewal/owner intent unknown",
        sourceUrl: DRH_10K,
      },
    });
  });
}

function drhHmaDossiers() {
  return DRH_NEAR_HMA.map((h) => {
    const dates = parseMonthYear(h.mmYyyy);
    const key = slug(`${h.hotelName}_hma`);
    return hotel({
      discoveryPropertyId: `disc_tb_v4_drh_${slug(h.hotelName)}`,
      findingId: `find_tb_v4_drh_${key}_${dates.expirationYear}${String(dates.expirationMonth).padStart(2, "0")}`,
      contractId: `ctr_tb_v4_drh_${key}_${dates.expirationYear}${String(dates.expirationMonth).padStart(2, "0")}`,
      hotelName: h.hotelName,
      hotelNameNormalized: h.hotelName.toLowerCase(),
      city: h.city,
      stateProvince: h.stateProvince,
      physicalCountry: "US",
      currentOperator: h.manager,
      counterpartyName: h.manager,
      owner: "DiamondRock Hospitality Company",
      agreementType: "MANAGEMENT",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      ...dates,
      sourceUrl: DRH_10K,
      sourcePublicationDate: "2026-02-27",
      sourceVintage: "2025",
      evidenceSnippet:
        `DRH YE2025 10-K Hotel Management Agreements table: ${h.hotelName} — manager ${h.manager}; terminable ${h.terminable}; current term expiration ${h.mmYyyy}; renewals ${h.renewals}. ` +
        `At-will + near-term term end = structural MCT flexibility. Do NOT treat as verified open partner-selection opportunity without evidence owner is evaluating alternatives. Performance-test existence alone does not prove terminability beyond the stated at-will right.`,
      renewalSummary: h.renewals,
      terminationSummary: h.terminable,
      unresolvedTerms: ["Exact term-end day", "Whether owner is actively evaluating managers", "Any informal exclusivity"],
      windowClass: WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW,
      hpcQuery: h.hotelName,
      trackA: "POTENTIAL_MCT_WINDOW_AT_WILL_NEAR_TERM",
      trackAPayload: {
        hotelName: h.hotelName,
        owner: "DiamondRock Hospitality",
        signal: "HMA_AT_WILL_PLUS_NEAR_TERM_EXPIRY",
        manager: h.manager,
        month: dates.impreciseDates[0],
        windowClass: WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW,
        limitation: "At-will ≠ verified open MCT outreach opportunity without owner process evidence",
        sourceUrl: DRH_10K,
      },
    });
  });
}

function drhHistoricalDossiers() {
  return DRH_HISTORICAL_HIGHGATE.map((h) => {
    const key = slug(h.hotelName);
    return hotel({
      discoveryPropertyId: `disc_tb_v4_drh_${key}`,
      findingId: `find_tb_v4_drh_${key}_highgate_20260203`,
      contractId: `ctr_tb_v4_drh_${key}_highgate_20260203`,
      hotelName: h.hotelName,
      hotelNameNormalized: h.hotelName.toLowerCase(),
      city: h.city,
      stateProvince: h.stateProvince,
      physicalCountry: "US",
      priorOperator: h.priorManager,
      currentOperator: h.newManager,
      counterpartyName: h.newManager,
      owner: "DiamondRock Hospitality Company",
      agreementType: "MANAGEMENT",
      agreementRole: "TERMINATION_INSTRUMENT",
      contractStatus: "TERMINATED_VERIFIED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      expirationDate: null,
      milestoneId: `ms_tb_v4_drh_${key}_highgate_${h.effectiveDate.replace(/-/g, "")}`,
      milestoneDate: h.effectiveDate,
      milestoneType: h.footnote === 3 ? "TERMINATION" : "COMMENCEMENT",
      datePrecision: "DAY",
      sourceUrl: DRH_10K,
      sourcePublicationDate: "2026-02-27",
      evidenceSnippet: `DRH YE2025 10-K management footnotes: ${h.note} HISTORICAL completed management replacement — not an open MCT.`,
      windowClass: WINDOW_CLASS.A_HISTORICAL_COMPLETED,
      hpcQuery: h.hotelName,
      trackA: "HISTORICAL_MGMT_REPLACEMENT_CLOSED",
      trackAPayload: {
        hotelName: h.hotelName,
        signal: "COMPLETED_MANAGER_TRANSITION",
        effectiveDate: h.effectiveDate,
        priorOperator: h.priorManager,
        currentOperator: h.newManager,
        windowClass: WINDOW_CLASS.A_HISTORICAL_COMPLETED,
        limitation: "Closed historical event",
        sourceUrl: DRH_10K,
      },
    });
  });
}

function svcDossiers() {
  return [
    hotel({
      discoveryPropertyId: "disc_tb_v4_svc_crowne_plaza_ihg",
      findingId: "find_tb_v4_svc_crowne_plaza_ihg_hma_20270131",
      contractId: "ctr_tb_v4_svc_crowne_plaza_ihg_hma_20270131",
      hotelName: "Crowne Plaza (SVC / IHG-managed, 495 keys)",
      hotelNameNormalized: "crowne plaza svc ihg 495 keys",
      physicalCountry: "US",
      currentBrand: "Crowne Plaza",
      currentOperator: "InterContinental Hotels Group (IHG)",
      counterpartyName: "IHG",
      owner: "Service Properties Trust",
      rooms: 495,
      agreementType: "MANAGEMENT",
      agreementRole: "RENEWAL_EXTENSION",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      expirationDate: "2027-01-31",
      milestoneId: "ms_tb_v4_svc_crowne_plaza_ihg_hma_20270131",
      milestoneDate: "2027-01-31",
      milestoneType: "OTHER",
      datePrecision: "DAY",
      sourceUrl: SVC_10K,
      sourcePublicationDate: "2026-02-25",
      evidenceSnippet:
        `SVC YE2025 10-K: In December 2025, SVC exercised the option to extend the term of its management agreement with IHG for one hotel by up to one year to January 31, 2027. ` +
        `Brand affiliation table lists one Crowne Plaza® (IHG Full Service Upscale) with 495 guest rooms — identity match is brand+keys only; city/property name not disclosed in the IHG agreement paragraph. ` +
        `Extension already exercised → commitment through 2027-01-31. Class C/D watch for post-2027 window; NOT a verified open MCT today.`,
      unresolvedTerms: ["Exact Crowne Plaza property name/city", "Whether further extension options remain", "Notice windows before 2027-01-31"],
      windowClass: WINDOW_CLASS.C_FORWARD_CONTRACTUAL_WATCH,
      hpcMatchStatus: "DISCOVERED_NOT_IN_HPC",
      hpcQuery: "Crowne Plaza 495",
      trackA: "EXTENDED_HMA_WATCH_IDENTITY_GAP",
      trackAPayload: {
        hotelName: "Crowne Plaza (SVC IHG 495 keys)",
        owner: "Service Properties Trust",
        signal: "IHG_HMA_EXTENDED_TO_2027-01-31",
        windowClass: WINDOW_CLASS.C_FORWARD_CONTRACTUAL_WATCH,
        limitation: "Hotel city/name unresolved — Track C; not open MCT while extended through Jan 2027",
        sourceUrl: SVC_10K,
      },
    }),
    hotel({
      discoveryPropertyId: "disc_tb_v4_svc_hyatt_17_portfolio",
      findingId: "find_tb_v4_svc_hyatt_17_hma_20310331",
      contractId: null,
      contractEligible: false,
      persistEligible: false,
      hotelName: "SVC Hyatt select-service portfolio (17 hotels)",
      hotelNameNormalized: "svc hyatt 17 hotel portfolio",
      physicalCountry: "US",
      currentOperator: "Hyatt",
      owner: "Service Properties Trust",
      agreementType: "MANAGEMENT",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      expirationDate: "2031-03-31",
      milestoneDate: null,
      datePrecision: "DAY",
      sourceUrl: SVC_10K,
      evidenceSnippet:
        `SVC YE2025 10-K: Hyatt managed 17 select-service hotels under a portfolio management agreement that expires on March 31, 2031. Hotels not named in this passage. Portfolio-level watch only — do not invent hotel-level opportunities.`,
      windowClass: WINDOW_CLASS.B_ACTIVE_NO_NEAR_TERM,
      trackA: "PORTFOLIO_HMA_HOTELS_UNNAMED",
      trackAPayload: {
        owner: "Service Properties Trust",
        signal: "HYATT_PORTFOLIO_HMA_EXPIRES_2031-03-31",
        windowClass: WINDOW_CLASS.B_ACTIVE_NO_NEAR_TERM,
        limitation: "17 hotels unnamed — no hotel-level opportunity without schedule",
        sourceUrl: SVC_10K,
      },
    }),
    hotel({
      discoveryPropertyId: "disc_tb_v4_svc_radisson_7_portfolio",
      findingId: "find_tb_v4_svc_radisson_7_hma_20310731",
      contractId: null,
      contractEligible: false,
      persistEligible: false,
      hotelName: "SVC Radisson full-service portfolio (7 hotels)",
      hotelNameNormalized: "svc radisson 7 hotel portfolio",
      physicalCountry: "US",
      currentOperator: "Radisson",
      owner: "Service Properties Trust",
      agreementType: "MANAGEMENT",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      expirationDate: "2031-07-31",
      milestoneDate: null,
      datePrecision: "DAY",
      sourceUrl: SVC_10K,
      evidenceSnippet:
        `SVC YE2025 10-K: Radisson managed seven full-service hotels under a portfolio management agreement that expires on July 31, 2031. Hotels not named. Portfolio-level watch only.`,
      windowClass: WINDOW_CLASS.B_ACTIVE_NO_NEAR_TERM,
      trackA: "PORTFOLIO_HMA_HOTELS_UNNAMED",
    }),
  ];
}

/** Desert Ridge deep dive — analysis-only (contract already persisted in coverage audit). */
function desertRidgeDeepDive() {
  return hotel({
    discoveryPropertyId: "disc_tb_v4_rhp_jw_desert_ridge_window_analysis",
    findingId: "find_tb_v4_rhp_jw_desert_ridge_2027_window_analysis",
    contractId: null,
    contractEligible: false,
    persistEligible: false,
    hotelName: "JW Marriott Phoenix Desert Ridge Resort & Spa",
    hotelNameNormalized: "jw marriott phoenix desert ridge resort spa",
    city: "Phoenix",
    stateProvince: "Arizona",
    physicalCountry: "US",
    currentBrand: "JW Marriott",
    currentOperator: "Marriott International",
    owner: "Ryman Hospitality Properties, Inc.",
    agreementType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
    findingType: "MANAGEMENT",
    expirationYear: 2027,
    datePrecision: "YEAR_ONLY",
    sourceUrl: RHP_10K,
    evidenceSnippet:
      `RHP YE2025 10-K: JW Marriott Desert Ridge HMA expires in 2027 with five automatic 10-year renewal periods if the hotel has met certain performance thresholds. ` +
      `Base fee ~3% of gross revenues; incentive 20% of available cash flow; owner's priority $34.1M (+11.5% of certain non-routine capex). ` +
      `Purchased June 10, 2025 (950 rooms). Performance threshold definitions, notice windows, and owner non-renewal rights are NOT disclosed. ` +
      `Automatic renewals mean 2027 is NOT automatically an open MCT. Classification: D (potential window requiring validation of performance/renewal path), NOT E.`,
    unresolvedTerms: [
      "Exact 2027 expiration month/day",
      "Performance threshold definitions",
      "Whether automatic renewal is unilateral to Marriott if thresholds met",
      "Owner notice / non-renewal rights if any",
    ],
    windowClass: WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW,
    trackA: "DESERT_RIDGE_2027_AUTO_RENEWAL_VALIDATION",
    trackAPayload: {
      hotelName: "JW Marriott Phoenix Desert Ridge Resort & Spa",
      owner: "Ryman Hospitality Properties",
      signal: "HMA_YEAR_2027_WITH_FIVE_AUTO_10YR_RENEWALS",
      windowClass: WINDOW_CLASS.D_POTENTIAL_DECISION_WINDOW,
      decisionMayStillBeAvailable: "Only if performance thresholds fail OR owner has undisclosed non-renewal rights — neither verified",
      whoControls: "Marriott appears to control automatic renewal if thresholds met; owner control unverified",
      whyCompete: "Large meetings resort; only if renewal path actually opens",
      limitation: "Do not promote to Verified open MCT/BCT from year+auto-renewal alone",
      priorTrackBContractId: "ctr_tb_gsca_rhp_jw_marriott_phoenix_desert_ridge_resort_spa_hma_2027",
      sourceUrl: RHP_10K,
    },
  });
}

/** Portfolio context — APLE flexibility (already assessed; analysis only). */
function apleContext() {
  return hotel({
    discoveryPropertyId: "disc_tb_v4_aple_flexibility_context",
    findingId: "find_tb_v4_aple_flexibility_context_not_eligible",
    contractId: null,
    contractEligible: false,
    persistEligible: false,
    hotelName: "Apple Hospitality REIT — portfolio HMA flexibility context",
    hotelNameNormalized: "apple hospitality reit portfolio flexibility context",
    physicalCountry: "US",
    owner: "Apple Hospitality REIT, Inc.",
    agreementType: null,
    commercialCategory: "MANAGEMENT",
    findingType: "MANAGEMENT",
    sourceUrl: "https://www.sec.gov/Archives/edgar/data/1418121/000119312526064008/aple-20251231.htm",
    evidenceSnippet:
      `APLE YE2025 context only: ~81% short-term variable HMAs. Not automatic conversion eligibility for any named hotel. Prior V1 audit persisted 9 completed Jan 2026 Marriott→third-party transitions separately.`,
    windowClass: WINDOW_CLASS.B_ACTIVE_NO_NEAR_TERM,
    trackA: "PORTFOLIO_CONTEXT_NOT_HOTEL_ELIGIBLE",
  });
}

export function buildDecisionWindowV4Dossiers() {
  return [
    ...drhFranchiseDossiers(),
    ...drhHmaDossiers(),
    ...drhHistoricalDossiers(),
    ...svcDossiers(),
    desertRidgeDeepDive(),
    apleContext(),
  ];
}

export function summarizeWindowClasses(dossiers = buildDecisionWindowV4Dossiers()) {
  return dossiers.reduce((acc, d) => {
    const k = d.windowClass || "UNCLASSIFIED";
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, {});
}
