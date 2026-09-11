/**
 * Packet 2.6B / 2.6B-R — Full Hotel Intelligence Investigation dossier statuses.
 * Customer-facing investigation product (not ownership-research-dossier-v1).
 */

export const DOSSIER_TYPE = "FULL_HOTEL_INTELLIGENCE_INVESTIGATION";

/** Packet 2.6C-R2 — follow-up Research Addendum (reuses dossier reader + R9 PDF). */
export const DOSSIER_TYPE_RESEARCH_ADDENDUM = "RESEARCH_ADDENDUM";

export const DOSSIER_TITLE = "Full Hotel Intelligence Investigation";

export const DOSSIER_TITLE_RESEARCH_ADDENDUM = "Research Addendum";

export const DOSSIER_STATUSES = Object.freeze([
  "NOT_STARTED",
  "QUEUED",
  "IN_PROGRESS",
  "COMPLETED",
  "COMPLETED_WITH_OPEN_QUESTIONS",
  "PARTIAL",
  "FAILED",
  "CANCELLED",
]);

export const FINDING_STATUSES = Object.freeze([
  "VERIFIED_INTELLIGENCE",
  "RESEARCH_FINDING",
  "UNRESOLVED",
  "CONTRADICTORY_EVIDENCE",
  "SUPERSEDED_FINDING",
]);

export const FINDING_STATUS_LABELS = Object.freeze({
  VERIFIED_INTELLIGENCE: "Verified Intelligence",
  RESEARCH_FINDING: "Research Finding",
  UNRESOLVED: "Unresolved",
  CONTRADICTORY_EVIDENCE: "Contradictory Evidence",
  SUPERSEDED_FINDING: "Superseded Finding",
});

/** Institutional report chapters — Packet 2.6B-R */
export const DOSSIER_SECTION_IDS = Object.freeze([
  "property_identity",
  "ownership_chain_propco",
  "operator_management",
  "brand_reflag",
  "property_history",
  "organization_portfolio",
  "people_decision_authority",
  "transactions_capital",
  "commercial_pursuit",
  "open_questions",
  "sources_evidence",
  "research_reconciliation",
  "contacts_appendix",
  // Legacy 2.6B section ids (summarizing adapter) — still valid for validation.
  "ownership_corporate_structure",
  "corporate_structure_trace",
  "ownership_history_transactions",
  "brand_operator_history",
  "people_decision_makers",
  "asset_capital_development",
  "development_pursuit_implications",
  "unresolved_questions",
  "research_notes",
  // Packet 2.6C-R2 Research Addendum sections
  "executive_answer",
  "research_question",
  "key_findings",
  "investigation",
  "opportunity_thesis",
  "why_now",
  "recent_change_triggers",
  "asset_product_risk",
  "product_investment_capex",
  "operating_quality",
  "operating_quality_management_risk",
  "operator_management_stability",
  "what_could_derail",
  "deal_risk_flags",
  "what_looks_stable",
  "what_to_verify_next",
  "decision_map",
  "authority_evidence",
  "professional_profiles",
  "contact_paths",
  "current_structure",
  "agreement_evidence",
  "brand_history",
  "operator_history",
  "change_signals",
  "current_ownership",
  "recent_ownership_events",
  "financing_capital_events",
  "transaction_signals",
  "entity_control_changes",
  "current_product_position",
  "renovation_capex",
  "development_expansion",
  "repositioning_signals",
  "owner_organization",
  "portfolio",
  "owned_controlled_assets",
  "pipeline_development",
  "portfolio_change_signals",
]);

export const DOSSIER_SECTION_TITLES = Object.freeze({
  property_identity: "1. Property Identity & Research Resolution",
  ownership_chain_propco: "2. Ownership Chain, PropCo & Beneficial Ownership",
  operator_management: "3. Operator & Management Structure",
  brand_reflag: "4. Brand & Reflag Investigation",
  property_history: "5. Property Development & Asset History",
  organization_portfolio: "6. Organization & Portfolio Intelligence",
  people_decision_authority: "7. People & Decision Authority",
  transactions_capital: "8. Transactions, Capital & Corporate Events",
  commercial_pursuit: "9. Commercial & Pursuit Intelligence",
  open_questions: "10. Open Questions & Research Priorities",
  sources_evidence: "11. Sources & Evidence",
  research_reconciliation: "Appendix A. Evidence Reconciliation",
  contacts_appendix: "Appendix B. Contact Intelligence",
  ownership_corporate_structure: "2. Ownership & Corporate Structure",
  corporate_structure_trace: "3. Corporate Structure Trace",
  ownership_history_transactions: "4. Ownership History & Transactions",
  brand_operator_history: "5. Brand & Operator History",
  people_decision_makers: "7. People & Decision-Maker Research",
  asset_capital_development: "8. Asset / Capital / Development Intelligence",
  development_pursuit_implications: "9. Development / Pursuit Implications",
  unresolved_questions: "10. Unresolved Questions",
  research_notes: "Research Notes",
  executive_answer: "Executive Answer",
  research_question: "Research Question",
  key_findings: "Key Findings",
  investigation: "Investigation",
  opportunity_thesis: "Opportunity Thesis — Why This Hotel May Matter Now",
  why_now: "Why Now",
  recent_change_triggers: "Recent Change & Triggers",
  asset_product_risk: "Asset & Product Risk",
  product_investment_capex: "Product Investment / CapEx",
  operating_quality: "Operating Quality",
  operating_quality_management_risk: "Operating Quality & Management Risk",
  operator_management_stability: "Management / Operator Stability",
  what_could_derail: "What Could Derail the Opportunity",
  deal_risk_flags: "Deal Risk Flags",
  what_looks_stable: "What Looks Stable",
  what_to_verify_next: "What to Verify Next",
  decision_map: "Decision Map",
  authority_evidence: "Authority Evidence",
  professional_profiles: "Professional Profiles",
  contact_paths: "Contact Paths",
  current_structure: "Current Structure",
  agreement_evidence: "Agreement / Relationship Evidence",
  brand_history: "Brand History",
  operator_history: "Operator History",
  change_signals: "Change Signals",
  current_ownership: "Current Ownership",
  recent_ownership_events: "Recent Ownership Events",
  financing_capital_events: "Financing / Capital Events",
  transaction_signals: "Transaction Signals",
  entity_control_changes: "Entity / Control Changes",
  current_product_position: "Current Product Position",
  renovation_capex: "Renovation / CapEx",
  development_expansion: "Development / Expansion",
  repositioning_signals: "Repositioning Signals",
  owner_organization: "Owner / Organization",
  portfolio: "Portfolio",
  owned_controlled_assets: "Owned / Controlled Assets",
  pipeline_development: "Pipeline / Development",
  portfolio_change_signals: "Portfolio Change Signals",
});

export function isValidDossierType(type) {
  const t = String(type || "").toUpperCase();
  return t === DOSSIER_TYPE || t === DOSSIER_TYPE_RESEARCH_ADDENDUM;
}

export function isValidDossierStatus(status) {
  return DOSSIER_STATUSES.includes(String(status || "").toUpperCase());
}

export function isValidFindingStatus(status) {
  return FINDING_STATUSES.includes(String(status || "").toUpperCase());
}
