/**
 * Track B international / capital source registry.
 * Incorporates DealSignal Track A Global Source Expansion V1 productivity ranking
 * (owner schedules ≫ IR ≫ SEC ≫ owner-operator ≫ transactions ≫ international ≫ CapEx ≫ CMBS ≫ planning).
 * CUSTOMER_BLOCKED. No paid providers. No parallel repositories.
 */

export const TRACK_A_SOURCE_PRODUCTIVITY_RANK = Object.freeze([
  "A_owner_contractual_schedules",
  "B_owner_investor_relations",
  "C_contractual_filings_sec",
  "G_owner_operator_relationship",
  "F_ownership_transaction",
  "I_international_disclosures",
  "E_capex_pip",
  "D_cmbs_debt",
  "H_property_planning_development",
]);

export const EVIDENCE_CLASS = Object.freeze({
  PUBLIC_CONTRACT_INSTRUMENT: "PUBLIC_CONTRACT_INSTRUMENT",
  CORPORATE_CONTRACTUAL_DISCLOSURE: "CORPORATE_CONTRACTUAL_DISCLOSURE",
  SECONDARY_REPORTING: "SECONDARY_REPORTING",
  INFERRED_COMMERCIAL_MILESTONE: "INFERRED_COMMERCIAL_MILESTONE",
  CAPITAL_CONTEXT_NOT_CONVERSION: "CAPITAL_CONTEXT_NOT_CONVERSION",
});

export const SOURCE_FAMILIES = Object.freeze({
  OWNER_FRANCHISE_HMA_SCHEDULE: {
    id: "OWNER_FRANCHISE_HMA_SCHEDULE",
    trackAMap: "A_owner_contractual_schedules",
    priority: 1,
    regions: ["US", "GLOBAL"],
    languages: ["en"],
    reuseExisting: ["SEC 10-K/10-Q property schedules", "Track B EX-10 named schedules"],
  },
  OWNER_IR_EARNINGS: {
    id: "OWNER_IR_EARNINGS",
    trackAMap: "B_owner_investor_relations",
    priority: 2,
    regions: ["US", "MX", "BR", "CA", "GB", "GLOBAL"],
    languages: ["en", "es", "pt"],
  },
  SEC_EXHIBITS_8K_10K: {
    id: "SEC_EXHIBITS_8K_10K",
    trackAMap: "C_contractual_filings_sec",
    priority: 3,
    regions: ["US"],
    languages: ["en"],
    reuseExisting: ["hif-v6-management-discovery", "Sonesta EX-10", "Sotherly EX-10"],
  },
  MX_BMV_EMISNET: {
    id: "MX_BMV_EMISNET",
    trackAMap: "I_international_disclosures",
    priority: 4,
    regions: ["MX", "CALA"],
    languages: ["es"],
    spanishTerms: [
      "contrato de administración",
      "contrato de operación",
      "franquicia",
      "rescisión",
      "cambio de marca",
      "arrendamiento",
      "internalización",
    ],
    accessNotes: "BMV eventfid PDFs intermittently 503; issuer IR mirrors often reachable.",
  },
  BR_CVM: {
    id: "BR_CVM",
    trackAMap: "I_international_disclosures",
    priority: 5,
    regions: ["BR", "CALA"],
    languages: ["pt"],
    portugueseTerms: ["gestão hoteleira", "franquia", "troca de bandeira", "fato relevante"],
    accessNotes: "CVM open data reachable; hotel-issuer structured extract still thin.",
    hardGate: "Do not classify administração condominial / multipropriedade as HMA.",
  },
  CMBS_DEBT_CAPITAL: {
    id: "CMBS_DEBT_CAPITAL",
    trackAMap: "D_cmbs_debt",
    priority: 8,
    regions: ["US"],
    languages: ["en"],
    rule: "Debt maturity alone is never proof of hotel conversion.",
    reuseExisting: ["CMBS Related License schedules (contractual columns only)"],
  },
  CAPEX_PIP_HOTEL_SPECIFIC: {
    id: "CAPEX_PIP_HOTEL_SPECIFIC",
    trackAMap: "E_capex_pip",
    priority: 7,
    regions: ["US", "CALA", "GLOBAL"],
    languages: ["en", "es", "pt"],
    rule: "Never apply generic brand PIP templates to a hotel without hotel-specific evidence.",
  },
  PUBLIC_CONCESSION_AWARD: {
    id: "PUBLIC_CONCESSION_AWARD",
    trackAMap: "F_ownership_transaction",
    priority: 6,
    regions: ["GLOBAL", "CALA"],
    languages: ["en", "es", "pt"],
  },
});

/** Year-only expirations are contractual facts but not exact-day milestones. */
export function yearOnlyExpirationPolicy(year) {
  const y = Number(year);
  if (!Number.isFinite(y) || y < 1990 || y > 2100) {
    return { ok: false, reason: "INVALID_YEAR" };
  }
  return {
    ok: true,
    expirationDate: null,
    milestoneDate: null,
    datePrecision: "YEAR_ONLY",
    notes: `Expiration year ${y} disclosed without month or day. Not stored in ISO date fields.`,
  };
}

export function classifyEvidenceAuthority(sourceFamilyId, isIssuerPrimary) {
  if (sourceFamilyId === "SEC_EXHIBITS_8K_10K" && isIssuerPrimary) {
    return EVIDENCE_CLASS.PUBLIC_CONTRACT_INSTRUMENT;
  }
  if (
    ["OWNER_FRANCHISE_HMA_SCHEDULE", "OWNER_IR_EARNINGS", "MX_BMV_EMISNET", "BR_CVM"].includes(sourceFamilyId) &&
    isIssuerPrimary
  ) {
    return EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE;
  }
  if (sourceFamilyId === "CMBS_DEBT_CAPITAL") {
    return EVIDENCE_CLASS.CAPITAL_CONTEXT_NOT_CONVERSION;
  }
  return EVIDENCE_CLASS.SECONDARY_REPORTING;
}
