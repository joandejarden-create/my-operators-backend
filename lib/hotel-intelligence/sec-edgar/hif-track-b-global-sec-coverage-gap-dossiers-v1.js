/**
 * Controlled incremental discovery from Global SEC Coverage Audit V1.
 * Sources: APLE YE2025 10-K (Marriott→third-party HMA transitions Jan 2026);
 * RHP YE2025 10-K (named Gaylord/JW Marriott HMA year expirations).
 * CUSTOMER_BLOCKED. Year-only dates not stored as ISO milestones.
 */
import { classifyCurrentStatus } from "./hif-track-b-historical-foundation-v1.js";
import { yearOnlyExpirationPolicy } from "./hif-track-b-international-source-registry-v1.js";

export const APLE_10K =
  "https://www.sec.gov/Archives/edgar/data/1418121/000119312526064008/aple-20251231.htm";
export const RHP_10K =
  "https://www.sec.gov/Archives/edgar/data/1040829/000110465926019035/rhp-20251231x10k.htm";

function hotel(row) {
  return {
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    persistEligible: true,
    contractEligible: row.contractEligible !== false,
    discoveryMethod: "TRACK_B_GLOBAL_SEC_COVERAGE_AUDIT_V1",
    hpcMatchStatus: "PENDING_HPC_SEARCH",
    wave: "B24",
    reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
    ...row,
  };
}

function slug(v) {
  return String(v || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

/** Nine APLE hotels that left Marriott management in January 2026. */
export const APLE_MARRIOTT_EXIT_JAN2026 = [
  { hotelName: "Residence Inn Burbank", city: "Burbank", stateProvince: "California", brand: "Residence Inn", rooms: 166, newManager: "Huntington Hotel Group, LP", footnote: 4 },
  { hotelName: "SpringHill Suites Burbank", city: "Burbank", stateProvince: "California", brand: "SpringHill Suites", rooms: 170, newManager: "Huntington Hotel Group, LP", footnote: 4 },
  { hotelName: "Courtyard Oceanside", city: "Oceanside", stateProvince: "California", brand: "Courtyard", rooms: 142, newManager: "Huntington Hotel Group, LP", footnote: 4 },
  { hotelName: "Residence Inn Oceanside", city: "Oceanside", stateProvince: "California", brand: "Residence Inn", rooms: 125, newManager: "Huntington Hotel Group, LP", footnote: 4 },
  { hotelName: "Fairfield Inn & Suites Tustin", city: "Tustin", stateProvince: "California", brand: "Fairfield", rooms: 145, newManager: "Huntington Hotel Group, LP", footnote: 4 },
  { hotelName: "Residence Inn Tustin", city: "Tustin", stateProvince: "California", brand: "Residence Inn", rooms: 149, newManager: "Huntington Hotel Group, LP", footnote: 4 },
  { hotelName: "Residence Inn San Juan Capistrano", city: "San Juan Capistrano", stateProvince: "California", brand: "Residence Inn", rooms: 130, newManager: "Dimension", footnote: 5 },
  { hotelName: "Fairfield Inn & Suites Orlando", city: "Orlando", stateProvince: "Florida", brand: "Fairfield", rooms: 200, newManager: "Crestline Hotels & Resorts, LLC", footnote: 6 },
  { hotelName: "SpringHill Suites Orlando", city: "Orlando", stateProvince: "Florida", brand: "SpringHill Suites", rooms: 200, newManager: "Crestline Hotels & Resorts, LLC", footnote: 6 },
];

function apleTransitionDossiers() {
  return APLE_MARRIOTT_EXIT_JAN2026.map((h) => {
    const key = slug(`${h.hotelName}_${h.city}`);
    return hotel({
      discoveryPropertyId: `disc_tb_gsca_aple_${key}`,
      findingId: `find_tb_gsca_aple_${key}_marriott_exit_202601`,
      contractId: `ctr_tb_gsca_aple_${key}_mgmt_transition_202601`,
      hotelName: h.hotelName,
      hotelNameNormalized: h.hotelName.toLowerCase(),
      city: h.city,
      stateProvince: h.stateProvince,
      physicalCountry: "US",
      currentBrand: h.brand,
      priorOperator: "Marriott International (affiliate)",
      currentOperator: h.newManager,
      counterpartyName: h.newManager,
      owner: "Apple Hospitality REIT, Inc.",
      agreementType: "MANAGEMENT",
      agreementRole: "TERMINATION_INSTRUMENT",
      contractStatus: "TERMINATED_VERIFIED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      expirationDate: null,
      milestoneDate: null,
      datePrecision: "MONTH",
      impreciseDates: ["management-transition:2026-01"],
      rooms: h.rooms,
      sourceUrl: APLE_10K,
      sourcePublicationDate: "2026-02-23",
      sourceVintage: "2025",
      evidenceSnippet:
        `APLE Form 10-K YE2025 property schedule footnotes (${h.footnote}): Manager noted as of 2025-12-31 was Marriott; in January 2026 management responsibility transferred from Marriott to ${h.newManager}. ` +
        `Portfolio narrative: nine Marriott-managed hotels as of YE2025 all transitioned in January 2026 to managers not affiliated with Marriott, Hilton or Hyatt. ` +
        `HISTORICAL completed management replacement — not an open MCT. Franchise with ${h.brand} continues separately (franchise expirations not disclosed hotel-by-hotel). Day-of-month not stated.`,
      terminationSummary: `Marriott affiliate HMA ended January 2026; successor manager ${h.newManager}. Exact day unknown.`,
      unresolvedTerms: ["Exact January 2026 transition day", "Termination fees if any", "New HMA term length for this hotel"],
      hpcQuery: h.hotelName,
      trackA: "HISTORICAL_MGMT_REPLACEMENT_CLOSED",
      trackAPayload: {
        hotelName: h.hotelName,
        owner: "Apple Hospitality REIT",
        signal: "COMPLETED_MANAGER_TRANSITION",
        month: "2026-01",
        priorOperator: "Marriott affiliate",
        currentOperator: h.newManager,
        limitation: "Closed historical event; franchise schedule still portfolio-only 10–30 years",
        sourceUrl: APLE_10K,
      },
    });
  });
}

function aplePortfolioStructure() {
  return hotel({
    discoveryPropertyId: "disc_tb_gsca_aple_portfolio_hma_structure",
    findingId: "find_tb_gsca_aple_variable_hma_81pct_short_term",
    contractId: null,
    contractEligible: false,
    persistEligible: false,
    hotelName: "Apple Hospitality REIT — portfolio HMA/franchise structure",
    hotelNameNormalized: "apple hospitality reit portfolio hma structure",
    physicalCountry: "US",
    owner: "Apple Hospitality REIT, Inc.",
    agreementType: null,
    commercialCategory: "MANAGEMENT",
    findingType: "MANAGEMENT",
    sourceUrl: APLE_10K,
    sourcePublicationDate: "2026-02-23",
    evidenceSnippet:
      `APLE YE2025 10-K: 217 hotels; ~81% under variable management fee agreements with average initial term ~1–2 years; HMAs terminable for performance, default, sale, or without cause. Franchise agreements generally 10–30 years; no hotel-level franchise expiry table. ` +
      `FORWARD-LOOKING structural flexibility for MCT engagement at portfolio level — do not invent hotel-specific HMA expirations. Franchise BCT still requires hotel-level schedules (research gap).`,
    trackA: "PORTFOLIO_MCT_FLEXIBILITY_NO_HOTEL_SCHEDULE",
    trackAPayload: {
      owner: "Apple Hospitality REIT",
      signal: "SHORT_TERM_VARIABLE_HMA_STRUCTURE",
      limitation: "Portfolio structure only; not Verified open BCT/MCT for a named hotel without hotel-specific evidence",
      sourceUrl: APLE_10K,
    },
  });
}

/** RHP named HMA year expirations. */
export const RHP_HMA_ROWS = [
  { hotelName: "Gaylord Opryland Resort & Convention Center", city: "Nashville", stateProvince: "Tennessee", year: 2047, renewals: "three automatic 10-year periods if performance thresholds met", fee: "base ~2% + pooled incentive" },
  { hotelName: "Gaylord Palms Resort & Convention Center", city: "Kissimmee", stateProvince: "Florida", year: 2047, renewals: "three automatic 10-year periods if performance thresholds met", fee: "base ~2% + pooled incentive" },
  { hotelName: "Gaylord Texan Resort & Convention Center", city: "Grapevine", stateProvince: "Texas", year: 2047, renewals: "three automatic 10-year periods if performance thresholds met", fee: "base ~2% + pooled incentive" },
  { hotelName: "Gaylord National Resort & Convention Center", city: "National Harbor", stateProvince: "Maryland", year: 2047, renewals: "three automatic 10-year periods if performance thresholds met", fee: "base ~2% + pooled incentive" },
  { hotelName: "Gaylord Rockies Resort & Convention Center", city: "Aurora", stateProvince: "Colorado", year: 2053, renewals: "two automatic 20-year periods if performance thresholds met", fee: "base ~3% + 20% incentive" },
  { hotelName: "JW Marriott San Antonio Hill Country Resort & Spa", city: "San Antonio", stateProvince: "Texas", year: 2060, renewals: "three automatic 10-year periods if performance thresholds met", fee: "base ~3.5% + 20% incentive" },
  { hotelName: "JW Marriott Phoenix Desert Ridge Resort & Spa", city: "Phoenix", stateProvince: "Arizona", year: 2027, renewals: "five automatic 10-year periods if performance thresholds met", fee: "base ~3% + 20% incentive", nearTerm: true },
  { hotelName: "Inn at Opryland", city: "Nashville", stateProvince: "Tennessee", year: 2034, renewals: "five five-year renewal options so long as neither party terminates", fee: "base ~2% + incentive" },
  { hotelName: "AC Hotel National Harbor", city: "National Harbor", stateProvince: "Maryland", year: 2040, renewals: "two ten-year renewal options so long as neither party terminates", fee: "base 7% + incentive" },
];

function rhpDossiers() {
  return RHP_HMA_ROWS.map((h) => {
    const key = slug(h.hotelName);
    const yp = yearOnlyExpirationPolicy(h.year);
    return hotel({
      discoveryPropertyId: `disc_tb_gsca_rhp_${key}`,
      findingId: `find_tb_gsca_rhp_${key}_hma_${h.year}`,
      contractId: `ctr_tb_gsca_rhp_${key}_hma_${h.year}`,
      hotelName: h.hotelName,
      hotelNameNormalized: h.hotelName.toLowerCase(),
      city: h.city,
      stateProvince: h.stateProvince,
      physicalCountry: "US",
      currentBrand: h.hotelName.split(" ")[0] === "JW" ? "JW Marriott" : h.hotelName.split(" ")[0],
      currentOperator: "Marriott International",
      counterpartyName: "Marriott International",
      owner: "Ryman Hospitality Properties, Inc.",
      agreementType: "MANAGEMENT",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      expirationDate: null,
      milestoneDate: null,
      expirationYear: h.year,
      datePrecision: "YEAR_ONLY",
      sourceUrl: RHP_10K,
      sourcePublicationDate: "2026-02-24",
      sourceVintage: "2025",
      evidenceSnippet:
        `RHP Form 10-K YE2025 Management Agreements: ${h.hotelName} — Marriott HMA expires in ${h.year} (${yp.notes}). Renewals: ${h.renewals}. Fees: ${h.fee}. ` +
        `Sale/brand-competitor transfer restrictions apply to pooled Gaylord hotels. Day/month not disclosed. ` +
        (h.nearTerm
          ? "NEAR-TERM year 2027 expiration with automatic renewals if performance thresholds met — Track A may assess as decision window, not auto-Verified MCT."
          : "Distant expiration — contractual fact only, not a near-term commercial opportunity."),
      renewalSummary: h.renewals,
      unresolvedTerms: ["Exact expiration month/day", "Performance threshold definitions", "Notice windows before renewal/non-renewal"],
      hpcQuery: h.hotelName.replace(/Resort & Convention Center|Resort & Spa/gi, "").trim().slice(0, 40),
      trackA: h.nearTerm ? "DECISION_WINDOW_YEAR_ONLY_WITH_AUTO_RENEWAL" : "NOT_QUALIFIED_DISTANT_YEAR_ONLY",
      trackAPayload: h.nearTerm
        ? {
            hotelName: h.hotelName,
            owner: "Ryman Hospitality Properties",
            signal: "HMA_EXPIRATION_YEAR",
            year: 2027,
            limitation: "YEAR_ONLY; five automatic 10-year renewals if performance thresholds met — not auto-Verified open MCT",
            sourceUrl: RHP_10K,
          }
        : undefined,
    });
  });
}

export function buildGlobalSecCoverageGapDossiers() {
  return [...apleTransitionDossiers(), aplePortfolioStructure(), ...rhpDossiers()];
}
