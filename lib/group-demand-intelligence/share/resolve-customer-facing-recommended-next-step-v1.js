/**
 * Prefer certified / published recommendedNextStep over generic association
 * procurement boilerplate that events/scaling drawer builders may invent when
 * recommendedAction is empty.
 */
export const RESOLVE_CUSTOMER_FACING_RECOMMENDED_NEXT_STEP_V1 =
  "gdi-resolve-customer-facing-recommended-next-step-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

/** Generic association/housing template — must not replace published next steps. */
export function isGenericAssociationProcurementNextStep(value) {
  return /meetings\s*\/\s*congress\s*\/\s*operations route|association staff\s*vs\s*PCO|PCO vs housing|Organizer TBD|host\/overflow decision is still open|host or overflow decision is still open/i.test(
    String(value || ""),
  );
}

/**
 * Resolve the customer-facing recommended next-step body.
 * Order:
 * 1. Certified recommendedNextStep when action is missing or generic
 * 2. Non-generic recommendedAction / recommendedNextAction / suggestedAction
 * 3. Remaining certified next step
 */
export function resolveCustomerFacingRecommendedNextStep(opportunity = {}) {
  const certified = text(opportunity.recommendedNextStep);
  const action = text(
    opportunity.recommendedAction ||
      opportunity.recommendedNextAction ||
      opportunity.suggestedAction ||
      opportunity.salesActionRecommended,
  );
  if (certified && (!action || isGenericAssociationProcurementNextStep(action))) {
    return certified;
  }
  if (action && !isGenericAssociationProcurementNextStep(action)) {
    return action;
  }
  return certified || action || "";
}

/**
 * Stamp presentation aliases so list DTO + sanitize + UI all see the same body.
 */
export function stampCustomerFacingRecommendedNextStepAliases(opportunity = {}) {
  const next = resolveCustomerFacingRecommendedNextStep(opportunity);
  if (!next) return opportunity;
  const out = { ...opportunity };
  if (
    !text(out.recommendedNextStep) ||
    isGenericAssociationProcurementNextStep(out.recommendedNextStep)
  ) {
    out.recommendedNextStep = next;
  }
  out.recommendedAction = next;
  out.recommendedNextAction = next;
  if (
    !text(out.suggestedAction) ||
    isGenericAssociationProcurementNextStep(out.suggestedAction)
  ) {
    out.suggestedAction = next;
  }
  return out;
}
