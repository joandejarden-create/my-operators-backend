#!/usr/bin/env node
/**
 * Track B — Historical contractual + commercial research batch + current-state reconciliation.
 * Writes research dossiers + Country×Timing×Gap matrix. Does not write Airtable (see persist script).
 * CUSTOMER_BLOCKED. $0 metered. Avoids Track A Lakeville IDs.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CURRENT_STATUS_CLASS,
  HISTORICAL_EVENT_TYPE,
  TRACK_A_RESERVED_DISCOVERY_IDS,
  classifyCurrentStatus,
  lackOfNewerFilingImpliesCurrent,
  buildCountryTimingMatrixRow,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-historical-foundation");
fs.mkdirSync(OUT, { recursive: true });

const universe = JSON.parse(fs.readFileSync(path.join(OUT, "HOTEL_UNIVERSE_COVERAGE.json"), "utf8"));
const hpcProbe = JSON.parse(fs.readFileSync(path.join(OUT, "hpc-country-probe.json"), "utf8"));
const canonicalPath = path.join(ROOT, "reports/sec-edgar-hif-v6-canonical-inventory/CANONICAL_SUMMARY.json");
const canonical = fs.existsSync(canonicalPath) ? JSON.parse(fs.readFileSync(canonicalPath, "utf8")) : null;

/** Verified research dossiers — hotel-specific or explicitly portfolio-scoped. */
const dossiers = [
  {
    discoveryPropertyId: "disc_tb_yorkville_royal_sonesta_toronto_2025",
    hotelName: "The Yorkville Royal Sonesta",
    hotelNameNormalized: "yorkville royal sonesta",
    city: "Toronto",
    stateProvince: "ON",
    physicalCountry: "CA",
    address: "220 Bloor Street West",
    currentBrand: "Royal Sonesta",
    brandFamily: "OTHER",
    operator: "Sonesta Toronto ULC",
    ownerLessee: "HPT TRS IHG-2, Inc.",
    rooms: null,
    hpcMatchStatus: "DISCOVERED_NOT_IN_HPC",
    hpcRecordId: null,
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    historicalEvents: [
      {
        type: HISTORICAL_EVENT_TYPE.CONTRACT_AMENDMENT,
        label: "New Retained Hotel management agreement effective Aug 1, 2025 (SVC/SVC Sonesta package)",
        agreementType: "MANAGEMENT",
        effectiveDate: "2025-08-01",
        originalExpiration: "2040-07-31",
        sourceUrl:
          "https://www.sec.gov/Archives/edgar/data/945394/000094539425000108/R14.htm",
        note: "SVC disclosed new Retained Hotel Sonesta HMAs expire Jul 31, 2040 with two 10-year renewals at Sonesta option.",
      },
      {
        type: HISTORICAL_EVENT_TYPE.CONTRACT_AMENDMENT,
        label: "Omnibus amendment extends HMA term to Jun 30, 2043",
        agreementType: "MANAGEMENT",
        effectiveDate: "2026-09-25",
        amendedExpiration: "2043-06-30",
        sourceUrl:
          "https://contracts.justia.com/companies/hospitality-properties-trust-660/contract/1390933/",
        note: "EX-10.3 Omnibus Amendment Schedule 1 item 41 names The Yorkville Royal Sonesta; operator Sonesta Toronto ULC.",
      },
    ],
    reconciliation: classifyCurrentStatus({
      historicallyVerified: true,
      currentStatusCorroborated: true,
      agreementExtendedOrAmended: true,
    }),
    persistEligible: true,
    findingId: "find_tb_yorkville_sonesta_hma_amend_2043",
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
  },
  {
    discoveryPropertyId: "disc_tb_sonesta_es_toronto_thornhill_2025",
    hotelName: "Sonesta ES Suites Toronto Thornhill",
    hotelNameNormalized: "sonesta es suites toronto thornhill",
    city: "Thornhill",
    stateProvince: "ON",
    physicalCountry: "CA",
    currentBrand: "Sonesta ES Suites",
    brandFamily: "OTHER",
    operator: "Sonesta Canada ULC",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    hpcMatchStatus: "DISCOVERED_NOT_IN_HPC",
    historicalEvents: [
      {
        type: HISTORICAL_EVENT_TYPE.CONTRACT_AMENDMENT,
        label: "HMA dated Aug 1, 2025; omnibus amendment extends term to Jun 30, 2043",
        agreementType: "MANAGEMENT",
        effectiveDate: "2025-08-01",
        amendedExpiration: "2043-06-30",
        sourceUrl:
          "https://contracts.justia.com/companies/hospitality-properties-trust-660/contract/1390933/",
        note: "Schedule 1 item 42 — Sonesta ES Suites Toronto Thornhill; operator Sonesta Canada ULC.",
      },
    ],
    reconciliation: classifyCurrentStatus({
      historicallyVerified: true,
      currentStatusCorroborated: true,
      agreementExtendedOrAmended: true,
    }),
    persistEligible: true,
    findingId: "find_tb_thornhill_sonesta_hma_amend_2043",
    findingType: "MANAGEMENT",
    commercialCategory: "MANAGEMENT",
  },
  {
    discoveryPropertyId: "disc_tb_choice_canada_mfa_transition_2025",
    hotelName: "Choice Hotels Canada (portfolio MFA)",
    hotelNameNormalized: "choice hotels canada portfolio mfa",
    city: null,
    stateProvince: null,
    physicalCountry: "CA",
    currentBrand: "Choice (multi-brand)",
    brandFamily: "CHOICE",
    identityClass: "GENERIC_BRAND_OR_COMPANY",
    evidenceConfidence: "HIGH",
    hpcMatchStatus: "NOT_APPLICABLE_PORTFOLIO",
    scope: "PORTFOLIO_MASTER_FRANCHISE",
    portfolioHotelsApprox: 350,
    portfolioRoomsApprox: 30000,
    historicalEvents: [
      {
        type: HISTORICAL_EVENT_TYPE.PORTFOLIO_MASTER_FRANCHISE,
        label: "Choice acquires remaining 50% of Choice Hotels Canada; MFA amended; shift to direct franchising",
        agreementType: "MASTER_FRANCHISE_AMENDED",
        effectiveDate: "2025-07-02",
        sourceUrl:
          "https://www.sec.gov/Archives/edgar/data/1046311/000104631126000008/R26.htm",
        corroborationUrl:
          "https://media.choicehotels.com/2025-08-06-Choice-Hotels-International-Continues-Global-Expansion-Through-Consolidation-of-Investment-in-Choice-Hotels-Canada",
        note: "Portfolio-level MFA amendment — NOT hotel-level franchise expirations for ~350 hotels. Do not auto-create 350 hotel inventory rows.",
      },
    ],
    reconciliation: classifyCurrentStatus({
      historicallyVerified: true,
      currentStatusCorroborated: true,
      agreementExtendedOrAmended: true,
      superseded: false,
    }),
    persistEligible: false,
    persistReason: "PORTFOLIO_SCOPE_STAGED_NOT_HOTEL_INVENTORY",
    findingId: "find_tb_choice_canada_mfa_amend_20250702",
    findingType: "BRAND_AFFILIATION",
    commercialCategory: "PORTFOLIO_MASTER_FRANCHISE",
  },
  {
    discoveryPropertyId: "disc_tb_hgi_sp_reboucas_grand_mercure_2026",
    hotelName: "Hilton Garden Inn São Paulo Rebouças / Grand Mercure São Paulo Rebouças",
    hotelNameNormalized: "hilton garden inn sao paulo reboucas",
    city: "São Paulo",
    stateProvince: "SP",
    physicalCountry: "BR",
    address: "Avenida Rebouças, Pinheiros (Helbor Wide)",
    rooms: 170,
    historicalBrand: "Hilton Garden Inn",
    currentBrand: "Grand Mercure (conversion announced; reopen scheduled Jul 2026)",
    brandFamily: "ACCOR",
    priorOperator: "Atlantica Hospitality International",
    currentOperator: "Atrio Hotel Management (Accor franchisee)",
    owner: "CVPAR Capital (acquired from HBR Realty 2025, ~R$110m)",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    hpcMatchStatus: "PENDING_HPC_SEARCH",
    historicalEvents: [
      {
        type: HISTORICAL_EVENT_TYPE.HOTEL_TRANSACTION,
        label: "HBR Realty sold 170-key Helbor Wide hotel to CVPAR (~R$110m)",
        effectiveDate: "2025",
        sourceUrl: "https://hoteliernews.com.br/dobradinha-entre-atrio-e-accor-faz-conversao-relevante-em-sp/",
      },
      {
        type: HISTORICAL_EVENT_TYPE.BRAND_CONVERSION,
        label: "Hilton Garden Inn → Grand Mercure conversion (Accor franchise)",
        agreementType: "FRANCHISE",
        effectiveDate: "2026-05",
        sourceUrl: "https://valorinternational.globo.com/business/news/2026/05/20/atrio-enters-sao-paulo-with-accor-premium-hotel-deal.ghtml",
        note: "Conversion announced; reopen scheduled July 2026 after ~R$5m renovation. Distinguish historical conversion from currently available opportunity.",
      },
      {
        type: HISTORICAL_EVENT_TYPE.MANAGEMENT_REPLACEMENT,
        label: "Operator change Atlantica → Atrio",
        effectiveDate: "2026-05",
        sourceUrl: "https://hoteliernews.com.br/dobradinha-entre-atrio-e-accor-faz-conversao-relevante-em-sp/",
      },
    ],
    reconciliation: classifyCurrentStatus({
      historicallyVerified: true,
      brandChanged: true,
      operatorChanged: true,
      currentStatusCorroborated: true,
    }),
    persistEligible: true,
    findingId: "find_tb_reboucas_hgi_to_grand_mercure_2026",
    findingType: "BRAND_AFFILIATION",
    commercialCategory: "BRAND_CONVERSION",
  },
  {
    discoveryPropertyId: "disc_tb_norte19_city_express_brand_migration_2025",
    hotelName: "Norte 19 City Express portfolio brand migrations (MX)",
    hotelNameNormalized: "norte 19 city express brand migration",
    physicalCountry: "MX",
    brandFamily: "MARRIOTT",
    identityClass: "GENERIC_BRAND_OR_COMPANY",
    evidenceConfidence: "MEDIUM",
    scope: "PORTFOLIO_BRAND_MIGRATION",
    historicalEvents: [
      {
        type: HISTORICAL_EVENT_TYPE.BRAND_CONVERSION,
        label: "City Express Suites consolidated into City Express / Plus at 7 properties; evaluating Plus→Courtyard/Aloft/Moxy/AC migrations",
        effectiveDate: "2025-Q3",
        sourceUrl: "https://miranda-newswire.com/wp-content/uploads/2025/10/N19-Resultados-3T25-ENG-vf.pdf",
        note: "Portfolio announcement — hotel-level agreement terms not disclosed. HOLD hotel-level inventory until property names + agreements verified.",
      },
    ],
    reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
    persistEligible: false,
    persistReason: "PORTFOLIO_WITHOUT_HOTEL_LEVEL_IDS",
    findingId: "find_tb_norte19_city_express_migration_2025q3",
    findingType: "BRAND_AFFILIATION",
    commercialCategory: "BRAND_CONVERSION",
  },
];

// Guard: never reuse Track A IDs
for (const d of dossiers) {
  if (TRACK_A_RESERVED_DISCOVERY_IDS.includes(d.discoveryPropertyId) || TRACK_A_RESERVED_DISCOVERY_IDS.includes(d.findingId)) {
    throw new Error(`TRACK_A_COLLISION: ${d.discoveryPropertyId}`);
  }
}

const reconciliationSummary = {
  CURRENTLY_CORROBORATED: 0,
  AGREEMENT_EXTENDED_OR_AMENDED: 0,
  BRAND_CHANGED: 0,
  OPERATOR_CHANGED: 0,
  HISTORICALLY_VERIFIED_CURRENT_UNKNOWN: 0,
  SUPERSEDED: 0,
  RESEARCH_REQUIRED: 0,
  CONFLICTED: 0,
  HOTEL_CLOSED_OR_GONE: 0,
};
for (const d of dossiers) {
  const cls = d.reconciliation.currentStatusClass;
  reconciliationSummary[cls] = (reconciliationSummary[cls] || 0) + 1;
}

const allCountries = Object.fromEntries(hpcProbe.countries || []);
const priorityCodes = ["US", "MX", "BR", "GB", "CA"];
const countryMatrix = {};

for (const code of priorityCodes) {
  const u = universe.priorityMarkets[code] || {};
  const contractualFromCanonical =
    code === "US" ? canonical?.exactCanonical?.byCountry?.US || 0 : 0;
  const dossierEvents = dossiers.filter((d) => d.physicalCountry === code);
  const franchiseResolved = dossierEvents.filter((d) =>
    d.historicalEvents?.some((e) => e.agreementType === "FRANCHISE"),
  ).length;
  const managementResolved = dossierEvents.filter((d) =>
    d.historicalEvents?.some((e) => e.agreementType === "MANAGEMENT"),
  ).length;
  const corroborated = dossierEvents.filter(
    (d) =>
      d.reconciliation.currentStatusClass === CURRENT_STATUS_CLASS.CURRENTLY_CORROBORATED ||
      d.reconciliation.currentStatusClass === CURRENT_STATUS_CLASS.AGREEMENT_EXTENDED_OR_AMENDED ||
      d.reconciliation.currentStatusClass === CURRENT_STATUS_CLASS.BRAND_CHANGED,
  ).length;

  countryMatrix[code] = buildCountryTimingMatrixRow({
    country: code,
    hpcHotels: u.hpcVerifiedHotels || 0,
    contractualEvents: contractualFromCanonical + dossierEvents.length,
    franchiseResolved: (code === "US" ? canonical?.exactCanonical?.verifiedFranchiseResolved || 0 : 0) + franchiseResolved,
    managementResolved: (code === "US" ? canonical?.exactCanonical?.verifiedManagementResolved || 0 : 0) + managementResolved,
    unresolvedAgreementType: code === "US" ? canonical?.exactCanonical?.agreementTypeUnresolvedAmongToward1000 || 0 : 0,
    currentStatusCorroborated: corroborated,
    researchCoverageLabel: u.researchCoverageLabel || "UNDER_RESEARCHED",
    brandedDenominatorStatus: u.coverage?.denominatorStatus || "UNKNOWN",
  });
  countryMatrix[code].external = u.external || null;
  countryMatrix[code].batchDossiers = dossierEvents.length;
  countryMatrix[code].criticalMassAssessment =
    code === "US"
      ? "CONTRACTUAL_CRITICAL_MASS_APPROACHING — 1004 toward-1000 US events; HPC hotel-universe coverage critically thin (15 HPC)"
      : code === "MX" || code === "BR"
        ? "HOTEL_UNIVERSE_SUBSTANTIAL — contractual intelligence sparse relative to HPC; approaching universe critical mass, not contractual"
        : "UNDER_RESEARCHED — both hotel-universe and contractual thin";
}

const worldwideCountries = (hpcProbe.countries || []).map(([country, count]) => ({
  country,
  hpcHotels: count,
  priority: ["United States", "Mexico", "Brazil", "United Kingdom", "Canada"].includes(country),
}));

const economics = {
  researchCostUsd: 0,
  meteredResearch: false,
  uniqueHotelsResearched: dossiers.filter((d) => d.identityClass === "VERIFIED_PROPERTY").length,
  portfolioScopedFindings: dossiers.filter((d) => d.scope).length,
  newCanonicalHotelIdentities: dossiers.filter((d) => d.persistEligible && d.identityClass === "VERIFIED_PROPERTY").length,
  newVerifiedContractualEvents: dossiers.filter((d) =>
    d.historicalEvents?.some((e) => e.agreementType === "MANAGEMENT" || e.agreementType === "FRANCHISE"),
  ).length,
  historicalAgreementsReconciled: dossiers.filter((d) =>
    [
      CURRENT_STATUS_CLASS.AGREEMENT_EXTENDED_OR_AMENDED,
      CURRENT_STATUS_CLASS.CURRENTLY_CORROBORATED,
      CURRENT_STATUS_CLASS.BRAND_CHANGED,
    ].includes(d.reconciliation.currentStatusClass),
  ).length,
  agreementTypeResolutions: dossiers.filter((d) =>
    d.historicalEvents?.some((e) => e.agreementType === "MANAGEMENT" || e.agreementType === "FRANCHISE"),
  ).length,
  countriesTouched: [...new Set(dossiers.map((d) => d.physicalCountry))],
  lackOfNewerFilingImpliesCurrent: lackOfNewerFilingImpliesCurrent(),
};

const weeklyPrep = {
  schedulerActivated: false,
  incrementalHooksDesigned: [
    "NEW_HOTEL_IDENTITY",
    "NEW_CONTRACT",
    "CONTRACT_AMENDMENT",
    "BRAND_CHANGE",
    "OPERATOR_CHANGE",
    "COMMERCIAL_EVENT",
    "TIMING_CHANGE",
    "SUPERSEDED_RECORD",
    "SOURCE_FRESHNESS",
    "GEOGRAPHIC_COVERAGE_CHANGE",
  ],
  note: "Do not activate unattended scheduling until historical foundation reconciled/validated.",
};

const out = {
  asOf: new Date().toISOString(),
  publicationState: "CUSTOMER_BLOCKED",
  stream: "TRACK_B",
  dossiers,
  reconciliationSummary,
  countryMatrix,
  worldwideHpcCountries: worldwideCountries,
  economics,
  weeklyPrep,
  trackACoordination: {
    reservedIds: TRACK_A_RESERVED_DISCOVERY_IDS,
    note: "Track B must not recreate Lakeville discovery/finding IDs; coordinate overlapping records.",
  },
  canonicalInventorySnapshot: canonical?.exactCanonical || null,
};

fs.writeFileSync(path.join(OUT, "HISTORICAL_RESEARCH_BATCH.json"), JSON.stringify(out, null, 2));
fs.writeFileSync(path.join(OUT, "MATRIX_COUNTRY_TIMING_GAP.json"), JSON.stringify({ asOf: out.asOf, priority: countryMatrix, worldwideHpcCountries: worldwideCountries }, null, 2));
console.log(
  JSON.stringify(
    {
      ok: true,
      dossiers: dossiers.length,
      persistEligible: dossiers.filter((d) => d.persistEligible).length,
      staged: dossiers.filter((d) => !d.persistEligible).length,
      reconciliationSummary,
      economics,
    },
    null,
    2,
  ),
);
