/**
 * GDI Field Authority Contract V1
 *
 * Machine-readable authority for customer-facing presentation fields.
 * Authenticated local GDI (founder-approved freeze / live master) is the
 * presentation reference. Publication freezes approved display values;
 * share/auth renderers must not re-select research classifications.
 *
 * Contact semantics (V2 tile footer — from universal-tile-contact-preview-v2):
 *   A. professionalTitle  — verified workplace / assigned activity title
 *   B. opportunityRole    — commercial category (Event Organizer, Group Travel…)
 *   C. contactRelevance   — buyerEligibility status (not tile subtitle)
 *   D. buyerEligibleForGate — buyer vs intermediary gate
 *   E. cardContactSubtitle / displayRole — FINAL tile subtitle
 *
 * Priority for E: A if present → else B → else "Role Not Yet Verified".
 * Never substitute B for E when A is present.
 */
export const GDI_FIELD_AUTHORITY_CONTRACT_V1 = "gdi-field-authority-contract-v1";

export const CONTACT_SEMANTIC_LAYER = Object.freeze({
  PROFESSIONAL_TITLE: "A_professionalTitle",
  OPPORTUNITY_ROLE: "B_opportunityRole",
  CONTACT_RELEVANCE: "C_contactRelevance",
  BUYER_VS_INTERMEDIARY: "D_buyerEligibleForGate",
  CARD_SUBTITLE: "E_cardContactSubtitle",
});

/**
 * @typedef {object} FieldAuthorityRow
 * @property {string} field
 * @property {string} definition
 * @property {string} authoritativeSource
 * @property {string} evidenceRequirements
 * @property {string} transformationRules
 * @property {string} approvedDisplayField
 * @property {string[]} allowedFallbacks
 * @property {string[]} neverSubstituteWith
 * @property {string} frontendComponent
 * @property {string} publicationSerialization
 */

/** @type {FieldAuthorityRow[]} */
export const GDI_FIELD_AUTHORITY_MATRIX_V1 = Object.freeze([
  {
    field: "cardContactSubtitle",
    definition:
      "Final customer-facing contact subtitle under the person name on the opportunity card/list footer.",
    authoritativeSource:
      "tileContactPreview.displayRole (frozen from local master) or cardContactSubtitle on the publication row",
    evidenceRequirements:
      "Named person required. Prefer verified professional/activity title; else approved opportunity-role enum; else Role Not Yet Verified.",
    transformationRules:
      "displayRoleKind=professional_title → use displayRole verbatim (after shared short/proper-case display choke-point only). Never remap to opportunityRole.",
    approvedDisplayField: "canonicalCustomerPresentation.contact.cardContactSubtitle",
    allowedFallbacks: [
      "tileContactPreview.displayRole",
      "tileContactPreview.roleLabel",
      "tileContactRoleLabel",
      "primaryContact.professionalTitle",
      "primaryContact.title",
    ],
    neverSubstituteWith: [
      "tileContactPreview.opportunityRole",
      "opportunityRole",
      "buyerRole",
      "contactRelevance",
      "buyerEligibility.status",
      "TILE_OPPORTUNITY_ROLE_LABEL.GROUP_TRAVEL when displayRoleKind is professional_title",
    ],
    frontendComponent:
      "dealality-gdi-ui.js → resolveTileContactPreviewBlock → .brand-card__contact-line--role",
    publicationSerialization:
      "Freeze tileContactPreview.displayRole + cardContactSubtitle; attach must preserve frozen bytes",
  },
  {
    field: "professionalTitle",
    definition:
      "Verified workplace title or assigned activity-specific contact description (e.g. RIC Conference Contact).",
    authoritativeSource: "primaryContact.title / professionalTitle / tileContactPreview.professionalTitle",
    evidenceRequirements: "Source-backed named individual; not an eligibility enum",
    transformationRules: "May feed displayRole when displayRoleKind=professional_title",
    approvedDisplayField: "tileContactPreview.professionalTitle",
    allowedFallbacks: ["primaryContact.jobTitle", "primaryContact.title"],
    neverSubstituteWith: ["opportunityRole", "buyerEligibility"],
    frontendComponent: "universal-tile-contact-preview-v1.js → extractVerifiedProfessionalTitle",
    publicationSerialization: "Preserve primaryContact.title and tileContactPreview.professionalTitle",
  },
  {
    field: "opportunityRole",
    definition:
      "Commercial category of the contact for this opportunity (Event Organizer, Meeting Planner, Group Travel Coordinator, Corporate Travel Contact, Referral Contact).",
    authoritativeSource: "tileContactPreview.opportunityRole / mapEvidenceToOpportunityRoleLabel",
    evidenceRequirements: "Derived from contact classification / sourcing evidence — internal + eligibility",
    transformationRules:
      "May become displayRole ONLY when no professional title exists (displayRoleKind=opportunity_role)",
    approvedDisplayField: "tileContactPreview.opportunityRole (internal; card only if no title)",
    allowedFallbacks: [],
    neverSubstituteWith: [],
    frontendComponent: "universal-tile-contact-preview-v1.js → mapEvidenceToOpportunityRoleLabel",
    publicationSerialization: "Freeze alongside displayRole; must not overwrite displayRole",
  },
  {
    field: "contactRelevance",
    definition: "Buyer/intermediary relevance status for gates — not customer tile subtitle.",
    authoritativeSource: "tileContactPreview.buyerEligibility / primaryContact.contactRelevanceLabel",
    evidenceRequirements: "Eligibility assessment separate from title",
    transformationRules: "Never render as card subtitle",
    approvedDisplayField: "canonicalCustomerPresentation.contact.relevance (drawer/diagnostics)",
    allowedFallbacks: ["buyerRole"],
    neverSubstituteWith: ["displayRole"],
    frontendComponent: "drawer Who-to-Contact / eligibility badges only",
    publicationSerialization: "Preserve buyerEligibility object; do not promote to displayRole",
  },
  {
    field: "buyerEligibleForGate",
    definition: "Boolean: named contact satisfies buyer/sourcing gate vs intermediary-only.",
    authoritativeSource: "tileContactPreview.buyerEligibleForGate",
    evidenceRequirements: "verified_sourcing_or_planning_role or explicit buyer evidence",
    transformationRules: "Independent of displayRole text",
    approvedDisplayField: "canonicalCustomerPresentation.contact.buyerEligibleForGate",
    allowedFallbacks: [],
    neverSubstituteWith: ["displayRole", "opportunityRole"],
    frontendComponent: "capability-gated Surfe / reveal paths",
    publicationSerialization: "Freeze buyerEligibleForGate on tileContactPreview",
  },
  {
    field: "commercialAction",
    definition: "Customer sales-action pill (Build Account, Monitor, Qualify, Build Venue Partnership…).",
    authoritativeSource: "salesActionPillLabel / tileBadgePlan.salesActionPillLabel (local master)",
    evidenceRequirements: "Founder/local commercial presentation — not recomputed from raw stage",
    transformationRules: "Do not replace with commercialStagePillLabel or legacy Pursue Now labels",
    approvedDisplayField: "canonicalCustomerPresentation.commercialAction.pillLabel",
    allowedFallbacks: ["salesActivationLabel"],
    neverSubstituteWith: [
      "commercialStagePillLabel",
      "raw salesActivation from research",
      "inventory classification labels",
    ],
    frontendComponent: "dealality-gdi-ui.js → tileSalesActionPill",
    publicationSerialization: "Freeze salesActionKey + salesActionPillLabel",
  },
  {
    field: "priority",
    definition: "Priority pill (High / Medium / Low).",
    authoritativeSource: "priorityPillLabel / priorityFilterKey",
    evidenceRequirements: "Local master priority assignment",
    transformationRules: "Display pillLabel; filter uses key",
    approvedDisplayField: "canonicalCustomerPresentation.priority.pillLabel",
    allowedFallbacks: ["priority"],
    neverSubstituteWith: ["maturity badges", "readiness state"],
    frontendComponent: "dealality-gdi-ui.js priority pill",
    publicationSerialization: "Freeze priorityFilterKey + priorityPillLabel",
  },
  {
    field: "revenueStreamIcons",
    definition: "Guest Rooms / Local Catering SVG active|muted states.",
    authoritativeSource: "revenueStreamIcons + revenueStreamClass frozen on publication row",
    evidenceRequirements: "guestroomThesis / eventFnbThesis evidence; no keyword invention",
    transformationRules:
      "Package attach preserves frozen icons; demote COMBINED only when icons were not frozen",
    approvedDisplayField: "canonicalCustomerPresentation.icons",
    allowedFallbacks: [],
    neverSubstituteWith: [
      "live re-derivation that mutes ACTIVE when thesis scrubbed",
      "inferred COMBINED without eventFnbThesis when icons frozen",
    ],
    frontendComponent: "dealality-gdi-ui.js revenue stream SVG",
    publicationSerialization: "Freeze revenueStreamClass + revenueStreamIcons bytes",
  },
  {
    field: "displayTitle",
    definition: "Customer opportunity title on card and drawer header.",
    authoritativeSource: "displayTitle / title from local master",
    evidenceRequirements: "Scrub research commentary; proper-case display",
    transformationRules: "Use displayTitle when present",
    approvedDisplayField: "canonicalCustomerPresentation.display.title",
    allowedFallbacks: ["title", "name"],
    neverSubstituteWith: ["internal research titles with audit notes"],
    frontendComponent: ".brand-card__name",
    publicationSerialization: "Freeze displayTitle",
  },
  {
    field: "customerSummary",
    definition: "Card/list summary prose (what is happening).",
    authoritativeSource: "summaryWhat / whatIsHappening / customerSummary",
    evidenceRequirements: "Customer-safe prose; no internal infra leakage",
    transformationRules: "Prefer customer summary fields over research brief dumps",
    approvedDisplayField: "canonicalCustomerPresentation.display.summary",
    allowedFallbacks: ["summary", "commercialSummary"],
    neverSubstituteWith: ["researchToDrawer raw dumps", "internal audit notes"],
    frontendComponent: "card body / drawer What Is Happening",
    publicationSerialization: "Freeze whatIsHappening / summaryWhat",
  },
  {
    field: "hotelSourcingStatus",
    definition: "Hotel sourcing status label on card meta.",
    authoritativeSource: "hotelSourcingStatusLabel / hotelSourcingStatusNormalized",
    evidenceRequirements: "Evidence-backed sourcing posture",
    transformationRules: "Locked presentation labels — do not invent Confirmed from weak evidence",
    approvedDisplayField: "hotelSourcingStatusLabel",
    allowedFallbacks: ["hotelSourcingStatusNormalized"],
    neverSubstituteWith: ["generic Unknown from share projection"],
    frontendComponent: "card meta line",
    publicationSerialization: "Freeze hotelSourcingStatusLabel",
  },
  {
    field: "needDateMatch",
    definition: "Need-Date Match indicator and preview on card.",
    authoritativeSource: "needDateMatchV1 / needDateTilePreview",
    evidenceRequirements: "Hotel need periods vs opportunity dates",
    transformationRules: "Display frozen need-date presentation; do not recompute on share",
    approvedDisplayField: "canonicalCustomerPresentation.icons.needDateLabel",
    allowedFallbacks: ["needDateStatus"],
    neverSubstituteWith: ["live recomputed mismatch that differs from freeze"],
    frontendComponent: "need-date card indicator",
    publicationSerialization: "Freeze needDateMatchV1",
  },
  {
    field: "drawerSections",
    definition:
      "Opportunity Details: Overview, Status, Timing, Location, Hotel sourcing, Room demand, Need-Date, What Is Happening, Potential Business, Why This Hotel, Why Act Now, Facts/Uncertainties, Contact, Evidence, Recommended Next Step, Hotel Validation/Action/Outcome.",
    authoritativeSource: "Frozen package drawer models (sharedTenSectionDrawer / events / scaling)",
    evidenceRequirements: "Per-section evidence; scaling drawers must be complete when present",
    transformationRules: "Serve frozen models; do not rebuild from lean list DTO",
    approvedDisplayField: "approvedCustomerFacingDetail / drawer models on package row",
    allowedFallbacks: [],
    neverSubstituteWith: ["generic section templates", "empty scalingDrawerModel shells"],
    frontendComponent: "dealality-gdi-ui.js drawer",
    publicationSerialization: "Allowlist full drawer models into publication package",
  },
]);

export function getFieldAuthority(fieldName) {
  return (
    GDI_FIELD_AUTHORITY_MATRIX_V1.find((r) => r.field === fieldName) || null
  );
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Resolve the authoritative card contact subtitle from a row.
 * Enforces: never use opportunityRole when a professional/activity displayRole exists.
 */
export function resolveAuthoritativeCardContactSubtitle(opportunity = {}) {
  const o = opportunity && typeof opportunity === "object" ? opportunity : {};
  const preview = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  const kind = text(preview.displayRoleKind).toLowerCase();
  const displayRole = text(
    preview.displayRole || preview.roleLabel || o.tileContactRoleLabel || o.cardContactSubtitle,
  );
  const professionalTitle = text(
    preview.professionalTitle ||
      pc.professionalTitle ||
      pc.jobTitle ||
      pc.title,
  );
  const opportunityRole = text(preview.opportunityRole || o.opportunityRole);

  if (displayRole && kind === "professional_title") {
    return {
      subtitle: displayRole,
      layer: CONTACT_SEMANTIC_LAYER.CARD_SUBTITLE,
      source: "displayRole.professional_title",
      rejectedOpportunityRole:
        opportunityRole && opportunityRole !== displayRole ? opportunityRole : null,
    };
  }
  if (displayRole && kind !== "opportunity_role") {
    // Frozen freeform displayRole without kind — still authoritative over opportunityRole.
    if (!opportunityRole || opportunityRole !== displayRole) {
      return {
        subtitle: displayRole,
        layer: CONTACT_SEMANTIC_LAYER.CARD_SUBTITLE,
        source: "displayRole.frozen",
        rejectedOpportunityRole:
          opportunityRole && opportunityRole !== displayRole ? opportunityRole : null,
      };
    }
  }
  if (professionalTitle) {
    return {
      subtitle: professionalTitle,
      layer: CONTACT_SEMANTIC_LAYER.PROFESSIONAL_TITLE,
      source: "professionalTitle",
      rejectedOpportunityRole:
        opportunityRole && opportunityRole !== professionalTitle
          ? opportunityRole
          : null,
    };
  }
  if (displayRole) {
    return {
      subtitle: displayRole,
      layer: CONTACT_SEMANTIC_LAYER.CARD_SUBTITLE,
      source: "displayRole",
      rejectedOpportunityRole: null,
    };
  }
  if (opportunityRole) {
    return {
      subtitle: opportunityRole,
      layer: CONTACT_SEMANTIC_LAYER.OPPORTUNITY_ROLE,
      source: "opportunityRole.fallback",
      rejectedOpportunityRole: null,
    };
  }
  return {
    subtitle: "Role Not Yet Verified",
    layer: CONTACT_SEMANTIC_LAYER.CARD_SUBTITLE,
    source: "unverified",
    rejectedOpportunityRole: null,
  };
}

/**
 * Detect illegal substitution of opportunityRole (or other never-fields) for card subtitle.
 */
export function detectContactSubtitleSubstitution(opportunity = {}, renderedSubtitle = null) {
  const resolved = resolveAuthoritativeCardContactSubtitle(opportunity);
  const rendered = text(renderedSubtitle);
  const issues = [];
  const preview = opportunity.tileContactPreview || {};
  const opportunityRole = text(preview.opportunityRole || opportunity.opportunityRole);
  const displayRole = text(preview.displayRole || preview.roleLabel);

  if (
    displayRole &&
    opportunityRole &&
    displayRole !== opportunityRole &&
    rendered &&
    rendered === opportunityRole
  ) {
    issues.push({
      code: "OPPORTUNITY_ROLE_SUBSTITUTED_FOR_DISPLAY_ROLE",
      expected: displayRole,
      actual: rendered,
      opportunityRole,
    });
  }
  if (rendered && resolved.subtitle && rendered !== resolved.subtitle) {
    // Truncation/proper-case may differ; only flag when rendered equals a never-substitute field.
    if (opportunityRole && rendered === opportunityRole && resolved.subtitle !== opportunityRole) {
      issues.push({
        code: "RENDERED_EQUALS_REJECTED_OPPORTUNITY_ROLE",
        expected: resolved.subtitle,
        actual: rendered,
      });
    }
  }
  return {
    ok: issues.length === 0,
    resolved,
    issues,
  };
}

/**
 * Apply authoritative contact subtitle onto canonical contract contact block.
 */
export function applyFieldAuthorityToCanonicalContact(contract = {}, opportunity = {}) {
  const c =
    contract && typeof contract === "object" ? { ...contract } : {};
  const contact = { ...(c.contact || {}) };
  const resolved = resolveAuthoritativeCardContactSubtitle(opportunity);
  contact.cardContactSubtitle = resolved.subtitle;
  contact.role = resolved.subtitle;
  contact.activityRole =
    contact.activityRole ||
    text(opportunity.tileContactPreview?.opportunityRole) ||
    null;
  contact.fieldAuthority = {
    version: GDI_FIELD_AUTHORITY_CONTRACT_V1,
    subtitleSource: resolved.source,
    layer: resolved.layer,
    rejectedOpportunityRole: resolved.rejectedOpportunityRole,
  };
  c.contact = contact;
  return c;
}
