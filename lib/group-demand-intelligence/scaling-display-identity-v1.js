/**
 * Scaling customer-facing company identity — display name vs legal entity.
 * Shared across hotels/segments; no company-specific hardcoding beyond patterns.
 */
export const SCALING_DISPLAY_IDENTITY_V1 = "gdi-scaling-display-identity-v1";

/**
 * Customer-facing classification on tiles and drawers.
 * Do NOT replace with the internal filter label "Scaling Company".
 * Filter dropdown may still say Scaling Company via opportunity-type-filter-v1.
 */
export const SCALING_CUSTOMER_CLASSIFICATION_LABEL =
  "Corporate Account Relationship";

/** @deprecated Use SCALING_CUSTOMER_CLASSIFICATION_LABEL — kept as alias for imports. */
export const SCALING_SEGMENT_DISPLAY_LABEL =
  SCALING_CUSTOMER_CLASSIFICATION_LABEL;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const LEGAL_SUFFIX_RE =
  /,?\s*(S\.?\s*C\.?|S\.?\s*A\.?\s*(?:de\s*)?C\.?\s*V\.?|S\.?\s*de\s*R\.?\s*L\.?\s*(?:de\s*)?C\.?\s*V\.?|SAPI\s*(?:de\s*CV)?(?:\s*,?\s*SOFOM(?:\s*,?\s*ENR)?)?|Inc\.?|LLC|Ltd\.?|Corp\.?|GmbH|PLC)\s*$/i;

/**
 * Strip trailing legal-entity suffixes for a short brand / trading name.
 */
export function stripLegalEntitySuffix(name = "") {
  let s = text(name);
  // Peel repeated suffixes (e.g. "Foo, S.A. de C.V.")
  for (let i = 0; i < 3; i += 1) {
    const next = s.replace(LEGAL_SUFFIX_RE, "").trim();
    if (next === s) break;
    s = next;
  }
  return s.replace(/[,;]\s*$/, "").trim();
}

/**
 * Derive a recognizable customer-facing company name from research identity.
 */
export function deriveDisplayCompanyName(rawName = "", opts = {}) {
  if (text(opts.displayCompanyName)) return text(opts.displayCompanyName);
  const raw = text(rawName);
  if (!raw) return "";

  const trading = raw.match(/\(\s*(?:trading|doing business|d\/?b\/?a)\s+as\s+([^)]+)\)/i);
  if (trading?.[1]) {
    return stripLegalEntitySuffix(trading[1]);
  }

  // "Clip — PayClip, S. de R.L. …" / "Mendel — Mexico platforms…"
  if (/[—–]/.test(raw) || /\s-\s/.test(raw)) {
    const left = raw.split(/\s*[—–]\s*|\s+-\s+/)[0].trim();
    if (left && left.length <= 48) {
      return stripLegalEntitySuffix(left);
    }
  }

  // Prefer short brand before semicolon lists of entities
  if (raw.includes(";") && raw.indexOf(";") < 40) {
    return stripLegalEntitySuffix(raw.split(";")[0]);
  }

  return stripLegalEntitySuffix(raw);
}

/**
 * Full identity pack for Scaling opportunities.
 */
export function buildScalingDisplayIdentity(opportunity = {}, opts = {}) {
  const legal =
    text(opts.legalEntityName) ||
    text(opportunity.legalEntityName) ||
    text(opportunity.canonicalCompanyIdentity) ||
    text(opportunity.organizationName) ||
    text(opportunity.companyName) ||
    text(opportunity.company) ||
    "";
  const display =
    text(opportunity.displayCompanyName) ||
    deriveDisplayCompanyName(legal, {
      displayCompanyName: opts.displayCompanyName,
    });
  // Customer classification is always Corporate Account Relationship for Scaling.
  // Ignore stale "Scaling Company" values that Stage F briefly wrote as segment labels.
  const rawSegment = text(opportunity.segmentDisplayLabel);
  const rawType = text(opportunity.opportunityTypeLabel);
  const isScaling = isScalingOpportunity(opportunity);
  const looksLikeFilterOnly =
    /^scaling company$/i.test(rawSegment) || /^scaling company$/i.test(rawType);
  const segmentLabel = isScaling
    ? SCALING_CUSTOMER_CLASSIFICATION_LABEL
    : looksLikeFilterOnly
      ? SCALING_CUSTOMER_CLASSIFICATION_LABEL
      : rawSegment ||
        rawType ||
        text(opportunity.segment) ||
        null;

  return {
    version: SCALING_DISPLAY_IDENTITY_V1,
    displayCompanyName: display || legal || "Company",
    legalEntityName: legal || null,
    segmentDisplayLabel: segmentLabel,
    opportunityTypeLabel: isScaling
      ? SCALING_CUSTOMER_CLASSIFICATION_LABEL
      : rawType || segmentLabel,
    canonicalCompanyIdentity: legal || display || null,
    drawerTitle: display || legal || "Opportunity",
    listTitle: display || legal || null,
  };
}

export function isScalingOpportunity(opportunity = {}) {
  return (
    opportunity.researchSegment === "scaling_companies" ||
    /scaling/i.test(String(opportunity.segment || "")) ||
    opportunity.demandFamily === "SCALING_COMPANY" ||
    /SCALING_COMPANY/i.test(String(opportunity.demandSignalType || ""))
  );
}

/**
 * Apply identity fields onto an opportunity (non-destructive of legal name).
 */
export function applyScalingDisplayIdentity(opportunity = {}, opts = {}) {
  const id = buildScalingDisplayIdentity(opportunity, opts);
  return {
    ...opportunity,
    displayCompanyName: id.displayCompanyName,
    legalEntityName: id.legalEntityName,
    segmentDisplayLabel: id.segmentDisplayLabel,
    opportunityTypeLabel: id.opportunityTypeLabel || opportunity.opportunityTypeLabel,
    opportunityType:
      opportunity.opportunityType ||
      (isScalingOpportunity(opportunity)
        ? "CORPORATE_ACCOUNT_RELATIONSHIP"
        : opportunity.opportunityType),
    canonicalCompanyIdentity: id.canonicalCompanyIdentity,
    // Customer list/drawer titles — never legal-entity dump
    title: id.drawerTitle,
    displayTitle: id.drawerTitle,
    organizationName:
      // Keep legal on organizationName for audit when distinct; UI prefers displayCompanyName
      id.legalEntityName && id.legalEntityName !== id.displayCompanyName
        ? id.legalEntityName
        : id.displayCompanyName,
  };
}
