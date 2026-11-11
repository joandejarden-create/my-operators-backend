/**
 * Shared Research → Local-Shadow customer delivery contract V1.
 *
 * Weekly / PowerShell #15 Need-Date campaigns must land in the hotel-scoped
 * local-shadow inventory with stable IDs before founder browser review.
 * Zero-paid staging only — never mint Airtable production IDs, never publish.
 *
 * Contract: GDI_RESEARCH_TO_LOCAL_SHADOW_DELIVERY_V1
 */
export const RESEARCH_TO_LOCAL_SHADOW_DELIVERY_V1 =
  "gdi-research-to-local-shadow-delivery-v1";

/**
 * Required steps after a Need-Date / multi-workflow research yield is final.
 * Implementations (Waterstone Stage 2, Chapultepec, Condesa, Nobu) must satisfy
 * every gate — hotel-specific mappers are allowed; alternate drawer engines are not.
 */
export const RESEARCH_TO_LOCAL_SHADOW_DELIVERY_STEPS = Object.freeze([
  "SNAPSHOT_EXISTING_CUSTOMER_VISIBLE_IDS",
  "RECONCILE_UNIQUE_ACCOUNTS_DEDUPED",
  "ASSIGN_DETERMINISTIC_LOCAL_SHADOW_IDS",
  "PRESERVE_RESEARCH_RUN_IDS_AND_CIB",
  "MAP_NAMED_CONTACTS_FROM_BUYER_CANDIDATES",
  "CLASSIFY_REVENUE_STREAMS_EVIDENCE_BASED",
  "BUILD_RESEARCH_TO_DRAWER_V2_HOTEL_CONTEXT",
  "MERGE_VIA_LOCAL_SHADOW_COMMERCIAL_PORTFOLIO",
  "CERTIFY_LIST_DTO_REVENUE_ICONS_AND_CONTACTS",
  "BROWSER_VERIFY_LOCALHOST_HOTEL_URL",
]);

export function researchToLocalShadowDeliveryChecklist(result = {}) {
  const required = [...RESEARCH_TO_LOCAL_SHADOW_DELIVERY_STEPS];
  const completed = Array.isArray(result.completedSteps)
    ? result.completedSteps
    : [];
  const missing = required.filter((s) => !completed.includes(s));
  return {
    version: RESEARCH_TO_LOCAL_SHADOW_DELIVERY_V1,
    pass: missing.length === 0,
    missing,
    required,
    completed,
    boundaries: {
      paidResearch: false,
      airtableOpportunityWrites: false,
      externalPublication: false,
      liveSurfe: false,
    },
  };
}
