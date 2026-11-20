/**
 * International expansion readiness — architecture hooks only (no paid sources).
 */

export const INTL_READINESS_VERSION = "sec-edgar-intl-readiness-v1";

export const FUTURE_JURISDICTION_SOURCES = Object.freeze([
  {
    jurisdiction: "GB",
    label: "United Kingdom",
    sources: ["UK Companies House", "LSE filings"],
    agreementNotes: "Distinguish management agreements vs leases under English law terminology.",
  },
  {
    jurisdiction: "ES",
    label: "Spain",
    sources: ["CNMV filings", "Registro Mercantil"],
    agreementNotes: "Contrato de gestión hotelera vs franquicia vs arrendamiento.",
  },
  {
    jurisdiction: "PT",
    label: "Portugal",
    sources: ["CMVM", "IRN commercial registry"],
    agreementNotes: "Preserve original Portuguese date formats and agreement labels.",
  },
  {
    jurisdiction: "MX",
    label: "Mexico",
    sources: ["BMV filings", "CNBV", "public deed disclosures"],
    agreementNotes: "Contrato de operación / franquicia; notarial ownership deeds.",
  },
  {
    jurisdiction: "DO",
    label: "Dominican Republic",
    sources: ["Government corporate disclosures", "transaction announcements"],
    agreementNotes: "CALA priority; often announcement-led rather than registry-rich.",
  },
  {
    jurisdiction: "CALA",
    label: "Wider Caribbean & Latin America",
    sources: ["Exchange filings", "hotel transaction announcements"],
    agreementNotes: "Reuse HI graph destinations; jurisdiction field mandatory.",
  },
  {
    jurisdiction: "ME",
    label: "Middle East",
    sources: ["Exchange disclosures", "government ownership announcements"],
    agreementNotes: "Owner/operator structures often layered; do not collapse PropCo/OpCo.",
  },
  {
    jurisdiction: "APAC",
    label: "Asia-Pacific",
    sources: ["Local exchange filings", "corporate registries"],
    agreementNotes: "Franchise vs management terminology varies by market.",
  },
]);

export function internationalSourceAdapterContract() {
  return {
    version: INTL_READINESS_VERSION,
    requiredCapabilities: [
      "fetch_filings_or_disclosures",
      "normalize_to_contract_finding",
      "preserve_jurisdiction_and_original_dates",
      "attribute_to_hpc_hotel",
      "persist_via_hi_graph_evidence",
      "change_detect_against_prior_state",
    ],
    sharedDestinations: [
      "HI Graph Evidence",
      "HI Hotel–Org Relationships",
      "HI Organizations",
      "HI Ownership Research Runs",
    ],
    paidSourcesAuthorizedThisPhase: false,
    jurisdictions: FUTURE_JURISDICTION_SOURCES,
  };
}
