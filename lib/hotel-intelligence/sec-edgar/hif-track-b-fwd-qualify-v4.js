/**
 * Track B V4 — Forward-looking decision-window qualification (Mexico residual + prior SEC).
 * Research / handoff only. persistEligible=false until founder authorizes expanded writes.
 * CUSTOMER_BLOCKED. Does not re-apply CALA / coverage-audit / DRH-SVC ledgers.
 */
export const QUAL_CLASS = Object.freeze({
  HISTORICAL_COMPLETED: "HISTORICAL_COMPLETED",
  COMMITTED_NOT_OPEN: "COMMITTED_NOT_OPEN",
  CONTRACT_WATCH: "CONTRACT_WATCH",
  POTENTIAL_DECISION_WINDOW: "POTENTIAL_DECISION_WINDOW",
  VERIFIED_OPEN_OPPORTUNITY: "VERIFIED_OPEN_OPPORTUNITY",
});

export const SOURCES = Object.freeze({
  FINN_GOV_2025:
    "https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf",
  FINN_3T2026:
    "https://fibrainn.mx/storage/docs/fibra-inn-anuncia-resultados-del-tercer-trimestre-2026.pdf",
  FINN_ASG_2025:
    "https://fibrainn.mx/assets/docs/informe-asg-2025-fibra-inn-alta_69f93d46d78d3.pdf",
  FINN_NOTAS_4T2025:
    "https://fibrainn.mx/assets/docs/f1616-fibra-inn-notas-ingles-4t2025_6a875905dc66b.pdf",
  FINN_INTERNALIZE_28:
    "https://fibrainn.mx/storage/docs/fibra-inn-internaliza-la-operacion-en-28-de-sus-hoteles.pdf",
  FINN_INTERNALIZE_DONE:
    "https://fibrainn.mx/storage/docs/fibra-inn-finaliza-proceso-de-internalizacion-de-la-operacion-en-28-hoteles.pdf",
  FINN_ASG_2025_ALT: "https://fibrainn.mx/assets/docs/esg-report-2025-fibra-inn-hg_69f93dd0a2b90.pdf",
  AIMBRIDGE_PORTFOLIO: "https://www.aimbridgelatam.com/portfolio.htm",
  FIHO_HR_2026: "tmp/fiho-hr-2026.txt",
  FIHO_Q2_EVENT: "tmp/fiho-eventfid-1574632.txt",
  DRH_10K: "https://www.sec.gov/Archives/edgar/data/1298946/000129894626000013/drh-20251231.htm",
  SVC_10K: "https://www.sec.gov/Archives/edgar/data/945394/000094539426000012/svc-20251231.htm",
  RHP_10K: "https://www.sec.gov/Archives/edgar/data/1040829/000110465926019035/rhp-20251231x10k.htm",
});

function q(row) {
  return {
    publicationState: "CUSTOMER_BLOCKED",
    persistEligible: false,
    researchCostUsd: 0,
    wave: "B25_FWD_QUALIFY",
    discoveryMethod: "TRACK_B_FWD_QUALIFY_V4",
    ...row,
  };
}

/** Seven Track A questions answered per finding. */
function answers(a) {
  return {
    whatDecisionRemains: a.whatDecisionRemains,
    whoControls: a.whoControls,
    whenCouldItOccur: a.whenCouldItOccur,
    contractualEventCreatingOpening: a.contractualEventCreatingOpening,
    canNewBrandOrOperatorCompete: a.canNewBrandOrOperatorCompete,
    alreadyExecuted: a.alreadyExecuted,
    evidenceStillMissing: a.evidenceStillMissing,
  };
}

export function buildFwdQualifyFindings() {
  return [
    q({
      findingId: "qual_tb_fwd_westin_mty_aimbridge_hma",
      hotelName: "The Westin Monterrey Valle",
      city: "San Pedro Garza García",
      stateProvince: "Nuevo León",
      physicalCountry: "MX",
      rooms: 174,
      agreementType: "MANAGEMENT",
      currentBrand: "Westin",
      currentOperator: "Aimbridge Latam",
      owner: "Fibra Inn co-owned (Hotel Factory / FID vehicle)",
      legalOwnerNote: "Copropiedad per ASG 2025; debt vehicle references Trust CIB/3096",
      expirationDate: null,
      datePrecision: "UNKNOWN",
      renewalMechanics: "NOT_DISCLOSED",
      nonrenewalNotice: "NOT_DISCLOSED",
      performanceTermination: "NOT_DISCLOSED",
      changeOfControl: "NOT_DISCLOSED — co-owner consent likely material; not published",
      qualClass: QUAL_CLASS.POTENTIAL_DECISION_WINDOW,
      trackAStatus: "EMERGING",
      evidenceSnippet:
        `ASG 2025: Westin Monterrey Valle — Copropiedad — Bajo acuerdo de gestión — 174 keys. ` +
        `FINN 3T26: Aimbridge Latam 2 hotels / 424 rooms (Westin 174 + JW 250). ` +
        `Gov letters (xix): upon expiration of existing third-party HMAs, hotels fold into internal operating structure (Servicios Hoteleros FINN). ` +
        `(xx) early Aimbridge termination authorized only for 100%-owned hotels — co-owned residuals excluded from 28-hotel buyout. ` +
        `HMA term/expiry/notice NOT published. Aimbridge Latam portfolio page still lists The Westin Monterrey Valle.`,
      sourceUrls: [SOURCES.FINN_ASG_2025, SOURCES.FINN_GOV_2025, SOURCES.AIMBRIDGE_PORTFOLIO],
      existingRecordRefs: {
        calaFindingId: "find_tb_cala_fu_westin_mty_aimbridge_hma_still_active_20260930",
        calaContractId: "ctr_tb_cala_fu_westin_mty_aimbridge_hma_active",
      },
      answers: answers({
        whatDecisionRemains: "Future manager selection at HMA expiry OR earlier termination/buyout if co-owners agree",
        whoControls: "Fibra Inn Technical Committee + co-owner(s); Aimbridge is current operator; brand (Marriott/Westin) separate",
        whenCouldItOccur: "UNKNOWN — expiry not disclosed",
        contractualEventCreatingOpening: "HMA expiration (if any competitive tender) or co-owner-approved early exit",
        canNewBrandOrOperatorCompete:
          "Operator: constrained — portfolio policy (xix) prefers internalization at expiry. Brand/franchise: separate instrument, not assessed as open BCT here.",
        alreadyExecuted: false,
        evidenceStillMissing: [
          "Exact HMA expiration date / notice window",
          "Co-owner identity and manager-change consent rights",
          "Whether policy (xix) binds co-owned Hotel Factory assets the same as 100%-owned",
        ],
      }),
    }),
    q({
      findingId: "qual_tb_fwd_jw_mty_aimbridge_hma",
      hotelName: "JW Marriott Monterrey (Arboleda / Valle naming conflict)",
      preferredCanonicalName: "JW Marriott Monterrey Arboleda",
      aliasNames: ["JW Marriott Monterrey Valle", "JW Marriott Hotel Monterrey Valle"],
      city: "San Pedro Garza García",
      stateProvince: "Nuevo León",
      physicalCountry: "MX",
      rooms: 250,
      agreementType: "MANAGEMENT",
      currentBrand: "JW Marriott",
      currentOperator: "Aimbridge Latam",
      owner: "Fibra Inn co-owned (Hotel Factory / FID vehicle)",
      legalOwnerNote: "ASG table uses Arboleda; debt notes and Aimbridge portfolio use Valle — Track C must resolve single HPC",
      expirationDate: null,
      datePrecision: "UNKNOWN",
      renewalMechanics: "NOT_DISCLOSED",
      nonrenewalNotice: "NOT_DISCLOSED",
      performanceTermination: "NOT_DISCLOSED",
      changeOfControl: "NOT_DISCLOSED — co-owner consent likely material",
      qualClass: QUAL_CLASS.POTENTIAL_DECISION_WINDOW,
      trackAStatus: "EMERGING",
      evidenceSnippet:
        `ASG 2025 property table: JW Marriott Monterrey Arboleda — Copropiedad — Bajo acuerdo de gestión — 250 keys. ` +
        `FINN 3T26 / debt: JW Marriott Monterrey Valle financing language. Aimbridge portfolio lists JW Marriott Monterrey Valle 250 keys. ` +
        `Same Aimbridge residual + gov (xix)/(xx) mechanics as Westin. Expiration unknown. Identity alias is a Track C blocker before any customer signal.`,
      sourceUrls: [SOURCES.FINN_ASG_2025, SOURCES.FINN_GOV_2025, SOURCES.AIMBRIDGE_PORTFOLIO],
      existingRecordRefs: {
        calaFindingId: "find_tb_cala_fu_jw_mty_aimbridge_hma_still_active_20260930",
        calaContractId: "ctr_tb_cala_fu_jw_mty_aimbridge_hma_active",
        note: "Prior CALA used Valle name; ASG canonical table uses Arboleda",
      },
      answers: answers({
        whatDecisionRemains: "Future manager selection at HMA expiry / co-owner-approved change",
        whoControls: "Fibra Inn + co-owner(s); Aimbridge current operator",
        whenCouldItOccur: "UNKNOWN",
        contractualEventCreatingOpening: "HMA expiration or negotiated early termination",
        canNewBrandOrOperatorCompete:
          "Operator competition constrained by internalization-at-expiry policy (xix). Brand open status not evidenced.",
        alreadyExecuted: false,
        evidenceStillMissing: [
          "Canonical HPC name (Arboleda vs Valle)",
          "HMA expiry / notice",
          "Co-owner consent mechanics",
        ],
      }),
    }),
    q({
      findingId: "qual_tb_fwd_hi_mty_franchise_renewed",
      hotelName: "Holiday Inn Monterrey Valle",
      agreementType: "FRANCHISE",
      currentBrand: "Holiday Inn",
      currentOperator: "Servicios Hoteleros FINN, S.C. (internalized ops)",
      owner: "Fibra Inn (FINN13) — owned",
      qualClass: QUAL_CLASS.COMMITTED_NOT_OPEN,
      trackAStatus: "COMMITTED",
      evidenceSnippet:
        `Gov (xiii): approved execution of new Holiday Inn franchise after prior agreement expired. Ops internalized 2025-09-01. New franchise term end UNKNOWN. Not an open BCT.`,
      sourceUrls: [SOURCES.FINN_GOV_2025, SOURCES.FINN_NOTAS_4T2025],
      existingRecordRefs: { calaFindingId: "find_tb_cala_fu_hi_monterrey_valle_franchise_renewed_2025" },
      answers: answers({
        whatDecisionRemains: "None for brand selection — franchise already renewed",
        whoControls: "Fibra Inn / IHG (executed)",
        whenCouldItOccur: "N/A — executed FY2025",
        contractualEventCreatingOpening: "Prior franchise expiry (already addressed by renewal)",
        canNewBrandOrOperatorCompete: false,
        alreadyExecuted: true,
        evidenceStillMissing: ["New franchise expiration date", "Notice windows of new instrument"],
      }),
    }),
    q({
      findingId: "qual_tb_fwd_courtyard_gdl_conversion",
      hotelName: "Wyndham Garden Guadalajara Acueducto → Courtyard by Marriott (Andares)",
      agreementType: "FRANCHISE",
      currentBrand: "Courtyard (committed conversion)",
      historicalBrand: "Wyndham Garden",
      owner: "Fibra Inn (FINN13)",
      qualClass: QUAL_CLASS.COMMITTED_NOT_OPEN,
      trackAStatus: "COMMITTED",
      evidenceSnippet:
        `Gov (xvi): conversion Wyndham Garden Guadalajara Acueducto (GDLAN) → Courtyard by Marriott (GDLCA) approved. ` +
        `ASG timeline: 1 Dec announcement as Courtyard Guadalajara Andares. ASG table shows Courtyard Guadalajara Andares — Operación Internalizada — 186 keys. ` +
        `Committed brand decision. New Courtyard franchise expiry UNKNOWN. Do not pitch competing brands.`,
      sourceUrls: [SOURCES.FINN_GOV_2025, SOURCES.FINN_ASG_2025],
      existingRecordRefs: { calaFindingId: "find_tb_cala_fu_wyndham_gdl_courtyard_conversion_approved_2025" },
      answers: answers({
        whatDecisionRemains: "None for brand — conversion approved/committed",
        whoControls: "Fibra Inn / Marriott (executed approval)",
        whenCouldItOccur: "N/A — committed",
        contractualEventCreatingOpening: "Owner-approved conversion (already done)",
        canNewBrandOrOperatorCompete: false,
        alreadyExecuted: true,
        evidenceStillMissing: [
          "Whether Acueducto and Andares are one physical asset or alias",
          "Courtyard franchise term end",
        ],
      }),
    }),
    q({
      findingId: "qual_tb_fwd_fiho_fairfield_villahermosa_lease",
      hotelName: "Fairfield Inn & Suites Villahermosa → InHouse lease",
      city: "Villahermosa",
      stateProvince: "Tabasco",
      physicalCountry: "MX",
      rooms: 134,
      agreementType: "LEASE",
      priorBrand: "Fairfield Inn / Marriott International",
      currentBrand: "InHouse",
      currentOperator: "InHouse Hotels (lessee)",
      owner: "FibraHotel (FIHO12)",
      commencementDate: "2026-07-01",
      datePrecision: "DAY",
      qualClass: QUAL_CLASS.COMMITTED_NOT_OPEN,
      trackAStatus: "COMMITTED",
      evidenceSnippet:
        `HR Ratings 2026: Servicio Limitado Villahermosa (Marriott) ended ops+franchise; from 2026-07-01 fixed-rent arrendamiento under InHouse, 134 rooms. ` +
        `FIHO portfolio lists Fairfield Inn & Suites Villahermosa 134 (Marriott) separately from Fiesta Inn Villahermosa 159 (Posadas) — do not conflate. Lessee legal entity beyond InHouse brand not fully named.`,
      sourceUrls: [SOURCES.FIHO_HR_2026, SOURCES.FIHO_Q2_EVENT],
      existingRecordRefs: { calaFindingId: "find_tb_cala_fu_villahermosa_lease" },
      answers: answers({
        whatDecisionRemains: "None for brand/operator selection — lease+InHouse committed",
        whoControls: "FibraHotel / InHouse lessee (executed)",
        whenCouldItOccur: "N/A — commenced 2026-07-01",
        contractualEventCreatingOpening: "Prior franchise/HMA end → lease conversion (executed)",
        canNewBrandOrOperatorCompete: false,
        alreadyExecuted: true,
        evidenceStillMissing: ["Lease term end / renewal", "Lessee legal name if distinct from brand"],
      }),
    }),
    q({
      findingId: "qual_tb_fwd_fiho_cd_carmen_lease",
      hotelName: "InHouse Ciudad del Carmen (ex-Fiesta Inn)",
      city: "Ciudad del Carmen",
      stateProvince: "Campeche",
      physicalCountry: "MX",
      rooms: 253,
      agreementType: "LEASE",
      priorBrand: "Fiesta Inn",
      currentBrand: "InHouse",
      currentOperator: "InHouse Hotels (lessee)",
      owner: "FibraHotel (FIHO12)",
      commencementDate: "2026-04-01",
      datePrecision: "DAY",
      qualClass: QUAL_CLASS.COMMITTED_NOT_OPEN,
      trackAStatus: "COMMITTED",
      evidenceSnippet:
        `HR / FIHO Q2: Fiesta Inn Ciudad del Carmen → fixed-rent lease; InHouse ops from 2026-04-01 (announce 2026-03-04). Portfolio row InHouse Ciudad del Carmen 253 keys.`,
      sourceUrls: [SOURCES.FIHO_HR_2026, SOURCES.FIHO_Q2_EVENT],
      answers: answers({
        whatDecisionRemains: "None — lease transition executed",
        whoControls: "FibraHotel / InHouse",
        whenCouldItOccur: "N/A — commenced 2026-04-01",
        contractualEventCreatingOpening: "Executed lease conversion",
        canNewBrandOrOperatorCompete: false,
        alreadyExecuted: true,
        evidenceStillMissing: ["Lease expiry", "Whether Posadas exit fees published"],
      }),
    }),
    q({
      findingId: "qual_tb_fwd_fiho_yucatan_lease_emerging",
      hotelName: "The Yucatán Resort Playa del Carmen",
      city: "Playa del Carmen",
      stateProvince: "Quintana Roo",
      physicalCountry: "MX",
      rooms: 60,
      agreementType: "LEASE",
      currentBrand: null,
      currentOperator: null,
      owner: "FibraHotel (FIHO12)",
      opsSuspendedDate: "2026-04-30",
      datePrecision: "DAY",
      qualClass: QUAL_CLASS.POTENTIAL_DECISION_WINDOW,
      trackAStatus: "EMERGING",
      evidenceSnippet:
        `FIHO: Yucatán Resort suspended 2026-04-30 to enter lease portfolio. Counterparty and brand for the lease NOT named in tested Q2/HR disclosures. ` +
        `Do not confuse with Mexicali→Holiday Inn/Aimbridge repositioning (different asset). Lease lessee selection may still be open — unverified.`,
      sourceUrls: [SOURCES.FIHO_HR_2026, SOURCES.FIHO_Q2_EVENT],
      answers: answers({
        whatDecisionRemains: "Lease operator/brand selection for arrendamiento model (if not already privately contracted)",
        whoControls: "FibraHotel",
        whenCouldItOccur: "From 2026-04-30 suspension onward; timing of lease execution unknown",
        contractualEventCreatingOpening: "Ops suspension to enter lease portfolio",
        canNewBrandOrOperatorCompete: "POSSIBLE for lease operators/brands IF no executed lease yet — not verified",
        alreadyExecuted: false,
        evidenceStillMissing: ["Lessee name", "Brand", "Lease term", "Whether lease already signed offline"],
      }),
    }),
    q({
      findingId: "qual_tb_fwd_santander_watch",
      hotelName: "Departamento de Santander — concesión (hotel identity TBD)",
      physicalCountry: "CO",
      city: "Bucaramanga",
      agreementType: null,
      qualClass: QUAL_CLASS.CONTRACT_WATCH,
      trackAStatus: "WATCHLIST",
      evidenceSnippet:
        `Track A CALA discovery: SECOP II 20-year ops/admin concesión — hotel identity not confirmed. No HPC create. Not a hotel opportunity until pliego confirms lodging asset.`,
      sourceUrls: [],
      answers: answers({
        whatDecisionRemains: "Unknown until asset type confirmed",
        whoControls: "Departamento de Santander",
        whenCouldItOccur: "Tender close ~2026-11-26 per Track A (unverified here)",
        contractualEventCreatingOpening: "Public concesión tender (if hotel)",
        canNewBrandOrOperatorCompete: "UNKNOWN — identity TBD",
        alreadyExecuted: false,
        evidenceStillMissing: ["Confirm hotel vs other inmueble", "Pliego terms"],
      }),
    }),
    // Prior SEC V4 windows — cross-reference only
    q({
      findingId: "qual_tb_fwd_drh_westin_boston_franchise_202612",
      hotelName: "Westin Boston Seaport District",
      agreementType: "FRANCHISE",
      expirationMonth: "2026-12",
      datePrecision: "MONTH",
      owner: "DiamondRock Hospitality",
      qualClass: QUAL_CLASS.POTENTIAL_DECISION_WINDOW,
      trackAStatus: "EMERGING",
      evidenceSnippet: "DRH YE2025 franchise table: Westin Boston Seaport District expires 12/2026. Notice/renewal intent unknown. Class D not E.",
      sourceUrls: [SOURCES.DRH_10K],
      existingRecordRefs: { v4FindingId: "find_tb_v4_drh_westin_boston_seaport_district_franchise_202612" },
      answers: answers({
        whatDecisionRemains: "Franchise renewal vs reflag",
        whoControls: "DiamondRock / franchisor",
        whenCouldItOccur: "Around Dec 2026 (month only)",
        contractualEventCreatingOpening: "Franchise expiration 12/2026",
        canNewBrandOrOperatorCompete: "POSSIBLE if owner elects reflag — owner intent unverified",
        alreadyExecuted: false,
        evidenceStillMissing: ["Notice window", "Owner renewal intent", "Exact day"],
      }),
    }),
    q({
      findingId: "qual_tb_fwd_rhp_desert_ridge_2027",
      hotelName: "JW Marriott Phoenix Desert Ridge Resort & Spa",
      agreementType: "MANAGEMENT",
      expirationYear: 2027,
      datePrecision: "YEAR_ONLY",
      owner: "Ryman Hospitality Properties",
      currentOperator: "Marriott International",
      qualClass: QUAL_CLASS.POTENTIAL_DECISION_WINDOW,
      trackAStatus: "EMERGING",
      evidenceSnippet:
        `RHP YE2025: HMA expires 2027 with five automatic 10-year renewals if performance thresholds met. Auto-renewal means 2027 ≠ verified open MCT.`,
      sourceUrls: [SOURCES.RHP_10K],
      existingRecordRefs: {
        gscaContractId: "ctr_tb_gsca_rhp_jw_marriott_phoenix_desert_ridge_resort_spa_hma_2027",
        v4AnalysisId: "find_tb_v4_rhp_jw_desert_ridge_2027_window_analysis",
      },
      answers: answers({
        whatDecisionRemains: "Only if performance thresholds fail or undisclosed owner non-renewal rights exist",
        whoControls: "Marriott appears to control auto-renewal if thresholds met; owner rights unverified",
        whenCouldItOccur: "2027 (year only)",
        contractualEventCreatingOpening: "HMA year expiration subject to auto-renewal",
        canNewBrandOrOperatorCompete: false,
        alreadyExecuted: false,
        evidenceStillMissing: ["Performance thresholds", "Owner non-renewal rights", "Exact day"],
      }),
    }),
  ];
}

export function summarizeQualClasses(findings = buildFwdQualifyFindings()) {
  return findings.reduce((a, f) => {
    a[f.qualClass] = (a[f.qualClass] || 0) + 1;
    return a;
  }, {});
}

export function buildTrackCIdentityHandoff() {
  return [
    {
      issue: "JW_MARRIOTT_MONTERREY_ARBOLEDA_VS_VALLE",
      preferredNameFromAsgTable: "JW Marriott Monterrey Arboleda",
      aliases: ["JW Marriott Monterrey Valle", "JW Marriott Hotel Monterrey Valle"],
      rooms: 250,
      city: "San Pedro Garza García",
      country: "MX",
      evidence:
        "ASG 2025 property table uses Arboleda; Aimbridge portfolio + FINN debt language use Valle; ASG also prints Valle in photo captions. Resolve one HPC; do not create duplicates.",
      action: "MATCH_EXISTING_OR_ALIAS — no speculative create",
      priorCalaId: "disc_tb_cala_fu_jw_marriott_monterrey_valle",
    },
    {
      issue: "COURTYARD_GDL_ACUEDUCTO_VS_ANDARES",
      committedBrand: "Courtyard by Marriott",
      priorName: "Wyndham Garden Guadalajara Acueducto (GDLAN)",
      asgName: "Courtyard by Marriott Guadalajara Andares",
      rooms: 186,
      evidence: "Gov conversion GDLAN→GDLCA; ASG Dec announcement Andares; table shows Andares internalized 186 keys.",
      action: "Confirm single physical asset alias before HPC write",
    },
    {
      issue: "VILLAHERMOSA_FAIRFIELD_VS_FIESTA_INN",
      leaseHotel: "Fairfield Inn & Suites Villahermosa (134) → InHouse lease",
      distinctHotel: "Fiesta Inn Villahermosa (159) Grupo Posadas — still listed separately",
      action: "Do not merge identities; lease finding attaches only to 134-key Fairfield/InHouse asset",
    },
    {
      issue: "CIUDAD_DEL_CARMEN_INHOUSE",
      hotelName: "InHouse Ciudad del Carmen",
      priorBrand: "Fiesta Inn",
      rooms: 253,
      action: "Match/alias prior Fiesta Inn Cd Carmen HPC if exists; preserve historical brand",
    },
    {
      issue: "YUCATAN_RESORT_PLAYA_DEL_CARMEN",
      hotelName: "The Yucatán Resort Playa del Carmen",
      rooms: 60,
      status: "Ops suspended 2026-04-30 for lease portfolio; brand/lessee unnamed",
      action: "Match existing HPC only; no create from weak evidence",
    },
    {
      issue: "SANTANDER_BUCARAMANGA",
      status: "Hotel identity TBD — SECOP II concesión",
      action: "NO HPC CREATE until pliego confirms lodging asset",
    },
  ];
}
