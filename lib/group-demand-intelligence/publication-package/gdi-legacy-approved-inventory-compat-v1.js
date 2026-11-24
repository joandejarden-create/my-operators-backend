/**
 * Legacy approved inventory compatibility V1
 *
 * Strict FQO remains required for *new* Qualified Opportunity candidacy.
 * Previously approved STAGED/CURRENT publication packages must not be silently
 * emptied by a post-hoc runtime FQO filter (schema / projection mismatches).
 *
 * Soft safety only: strip exhibitor placeholders and explicit founder rejects.
 * Does not invent contacts. Does not write pointers.
 */
import {
  evaluateFinalizedQualifiedOpportunity,
  GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
} from "../workflow/gdi-finalized-qualified-opportunity-contract-v1.js";

export const GDI_LEGACY_APPROVED_INVENTORY_COMPAT_V1 =
  "gdi-legacy-approved-inventory-compat-v1";

const EXHIBITOR_PLACEHOLDER_RE =
  /\bEXHIBITOR\s+BLOCK\b|CONTACT_RESEARCH_REQUIRED|BUYER NOT YET|ROLE NOT YET|PLACEHOLDER/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * True when the resolved package is an activated STAGED/CURRENT inventory
 * (not a non-activated hotel-wide / paid candidate preview).
 */
export function isLegacyApprovedPublicationPreview(meta = {}) {
  const src = String(meta?.previewSource || "").toUpperCase();
  if (src !== "STAGED" && src !== "CURRENT") return false;
  if (meta?.pointerActivated === false) return false;
  if (src === "CANDIDATE_CERTIFIED_NOT_ACTIVATED") return false;
  const ver = String(
    meta?.publicationVersion || meta?.version || meta?.packageVersion || "",
  );
  // New FQO candidacy packages are never "legacy approved" even if mis-tagged.
  if (/hotel_wide_fqo|paid_research_v1_20261124/i.test(ver) && src !== "STAGED" && src !== "CURRENT") {
    return false;
  }
  return true;
}

/**
 * Soft safety filter for legacy approved packages — not full FQO.
 */
export function filterLegacyApprovedPackageOpportunities(
  opportunities = [],
  opts = {},
) {
  const rows = Array.isArray(opportunities) ? opportunities : [];
  const kept = [];
  const removed = [];
  for (const o of rows) {
    const title = `${text(o?.title)} ${text(o?.displayTitle)}`;
    const id = text(o?.id || o?.opportunityId);
    if (
      o?.disqualified === true ||
      o?.founderRejected === true ||
      /^(DISQUALIFIED|REJECTED)$/i.test(String(o?.status || ""))
    ) {
      removed.push({
        opportunityId: id,
        classification: "FOUNDER_REJECTED",
        reasons: ["EXPLICITLY_REJECTED_OR_DISQUALIFIED"],
        primaryName: text(o?.primaryContact?.name),
        channel:
          text(o?.primaryContact?.email) ||
          text(o?.primaryContact?.phone) ||
          text(o?.primaryContact?.linkedinUrl) ||
          null,
      });
      continue;
    }
    if (EXHIBITOR_PLACEHOLDER_RE.test(title)) {
      removed.push({
        opportunityId: id,
        classification: "GENUINELY_INELIGIBLE",
        reasons: ["EXHIBITOR_PLACEHOLDER_OR_REJECTED"],
        primaryName: text(o?.primaryContact?.name),
        channel: null,
      });
      continue;
    }
    kept.push(o);
  }
  return {
    contract: GDI_LEGACY_APPROVED_INVENTORY_COMPAT_V1,
    fqoContract: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    mode: "LEGACY_APPROVED_SOFT_SAFETY",
    beforeCount: rows.length,
    afterCount: kept.length,
    droppedCount: removed.length,
    opportunities: kept,
    removed,
    note:
      opts.note ||
      "Legacy approved STAGED/CURRENT inventory — FQO not applied at read time; soft safety only.",
  };
}

/**
 * Classify why a legacy row would fail *new* FQO (diagnostic only).
 */
export function classifyLegacyRowAgainstNewFqo(opportunity = {}) {
  const evaluation = evaluateFinalizedQualifiedOpportunity(opportunity);
  if (evaluation.finalizedQualified === true) {
    return {
      classification: "STILL_VISIBLE_UNDER_NEW_FQO",
      reasons: [],
      evaluation,
    };
  }
  const reasons = evaluation.reasons || [];
  let classification = "FILTERED_BY_NEW_FQO";
  if (reasons.some((r) => /PERSON_SPECIFIC|NAMED_COMMERCIAL/.test(r))) {
    classification = "CONTACT_FIELD_MISMATCH";
  }
  if (reasons.some((r) => /MISSING_EVIDENCE/.test(r))) {
    classification = "MISSING_SOURCE_EVIDENCE";
  }
  if (reasons.some((r) => /HISTORICAL|FUTURE_SOURCING|NO_VALID_FUTURE/.test(r))) {
    classification = "EXPIRED_OR_HISTORICAL";
  }
  if (evaluation.status === "FOUNDER_REJECTED") {
    classification = "FOUNDER_REJECTED";
  }
  return { classification, reasons, evaluation };
}
