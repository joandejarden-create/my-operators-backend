/**
 * GDI Canonical Customer Presentation Contract V1
 *
 * Single customer-facing field contract for approved publication rows.
 * Auth PACKAGE preview and external share must expose the same values for
 * the same opportunity identity. Commercial facts are never recomputed here —
 * this module only normalizes/attaches a stable read model from the row.
 *
 * Contract: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1
 */
export const GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1 =
  "GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1";

export const CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_VERSION =
  "gdi-canonical-customer-presentation-contract-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function pick(...vals) {
  for (const v of vals) {
    const t = text(v);
    if (t) return t;
  }
  return null;
}

/**
 * Build the customer-facing presentation contract from an opportunity row.
 * Does not invent commercial recommendations or contact titles.
 *
 * @param {object} opportunity
 * @param {object} [meta]
 * @returns {object}
 */
export function buildCanonicalCustomerPresentationContract(
  opportunity = {},
  meta = {},
) {
  const o = opportunity && typeof opportunity === "object" ? opportunity : {};
  const preview = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  const icons = o.revenueStreamIcons || {};

  return {
    version: CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_VERSION,
    contractId: GDI_CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_V1,
    opportunityId: pick(o.id, o.opportunityId),
    hotelId: pick(o.hotelId, meta.hotelId),
    publicationVersion: pick(
      meta.publicationVersion,
      o.publicationVersion,
      o.publication?.publicationVersion,
    ),
    contentFingerprintSha256: pick(
      meta.contentFingerprintSha256,
      o.contentFingerprintSha256,
    ),
    listScope: pick(meta.listScope, o.listScope),
    marketScope: {
      hotelEligible: o.hotelMarketScopeEligible !== false,
      competitiveEligible: o.competitiveMarketScopeEligible === true,
      inventoryPopulations: Array.isArray(o.inventoryPopulations)
        ? o.inventoryPopulations.slice()
        : null,
      inventoryPopulationPrimary: pick(o.inventoryPopulationPrimary),
    },
    display: {
      title: pick(o.displayTitle, o.title, o.name),
      drawerHeaderTitle: pick(o.drawerHeaderTitle, o.displayTitle, o.title),
      subtitle: pick(
        o.cardWhyNowLine,
        o.segmentDisplayLabel,
        o.opportunityTypeFilterLabel,
      ),
      summary: pick(
        o.summaryWhat,
        o.customerSummary,
        o.summary,
        o.commercialSummary,
      ),
      organizationName: pick(
        o.displayCompanyName,
        o.organizationName,
        pc.organizationName,
        pc.organization,
      ),
      opportunityTypeLabel: pick(
        o.opportunityTypeFilterLabel,
        o.demandFamilyLabel,
        o.segmentDisplayLabel,
      ),
      timingLabel: pick(
        o.datePillDisplayLabel,
        o.eventDateDisplay,
        o.canonicalTiming,
      ),
    },
    priority: {
      key: pick(o.priorityFilterKey, o.priority),
      pillLabel: pick(o.priorityPillLabel, o.priority),
      show: o.showPriorityPill !== false,
    },
    commercialAction: {
      key: pick(o.salesActionKey, o.salesActivationKey),
      pillLabel: pick(
        o.salesActionPillLabel,
        o.salesActivationLabel,
        o.commercialStagePillLabel,
      ),
      stageKey: pick(o.commercialStageKey, o.commercialStageFilterKey),
      stagePillLabel: pick(o.commercialStagePillLabel),
      recommendedNextStep: pick(
        o.recommendedNextAction,
        o.recommendedAction,
      ),
      show: o.showSalesActionPill !== false,
    },
    contact: {
      name: pick(
        preview.name,
        preview.displayName,
        o.tileContactName,
        pc.name,
      ),
      // Frozen card subtitle — never opportunityRole / referral remapping.
      role: pick(
        preview.displayRole,
        preview.roleLabel,
        o.tileContactRoleLabel,
        o.cardContactSubtitle,
        pc.professionalTitle,
        pc.jobTitle,
        pc.title,
      ),
      cardContactSubtitle: pick(
        o.cardContactSubtitle,
        preview.displayRole,
        preview.roleLabel,
        o.tileContactRoleLabel,
        pc.professionalTitle,
        pc.jobTitle,
        pc.title,
      ),
      activityRole: pick(preview.opportunityRole, o.opportunityRole),
      professionalTitle: pick(
        preview.professionalTitle,
        pc.professionalTitle,
        pc.jobTitle,
        pc.title,
      ),
      roleKind: pick(preview.displayRoleKind),
      organization: pick(
        preview.organization,
        pc.organizationName,
        pc.organization,
      ),
      relevance: pick(
        preview.buyerEligibility?.status,
        pc.contactRelevanceLabel,
        o.buyerRole,
      ),
      buyerEligibleForGate: preview.buyerEligibleForGate === true,
      kind: pick(preview.kind, preview.name || pc.name ? "named" : "none"),
    },
    icons: {
      revenueStreamClass: pick(o.revenueStreamClass),
      guestrooms: pick(icons.guestrooms) || "muted",
      eventFnb: pick(icons.eventFnb) || "muted",
      needDateState: pick(
        o.needDateMatchV1?.displayState,
        o.needDateTilePreview?.state,
        o.needDateStatus,
      ),
      needDateLabel: pick(
        o.needDateMatchV1?.displayLabel,
        o.needDateTilePreview?.label,
      ),
    },
    needDateMatch: o.needDateMatchV1 || o.needDateAssociations || null,
    evidencePresent: Boolean(
      o.evidenceSummary ||
        o.supportingEvidence ||
        o.customerEvidence ||
        (Array.isArray(o.evidence) && o.evidence.length),
    ),
  };
}

/**
 * Attach contract onto opportunity (idempotent).
 */
export function attachCanonicalCustomerPresentationContract(
  opportunity = {},
  meta = {},
) {
  const o =
    opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const contract = buildCanonicalCustomerPresentationContract(o, meta);
  o.canonicalCustomerPresentation = contract;
  o.canonicalCustomerPresentationVersion =
    CANONICAL_CUSTOMER_PRESENTATION_CONTRACT_VERSION;
  return o;
}

/**
 * Stable comparison fingerprint for parity tests (excludes mutable feedback).
 */
export function canonicalPresentationCompareKey(contract = {}) {
  const c = contract || {};
  return JSON.stringify({
    opportunityId: c.opportunityId || null,
    title: c.display?.title || null,
    timingLabel: c.display?.timingLabel || null,
    priority: c.priority?.pillLabel || null,
    commercialAction: c.commercialAction?.pillLabel || null,
    recommendedNextStep: c.commercialAction?.recommendedNextStep || null,
    contactName: c.contact?.name || null,
    contactRole: c.contact?.role || null,
    contactKind: c.contact?.kind || null,
    guestrooms: c.icons?.guestrooms || null,
    eventFnb: c.icons?.eventFnb || null,
    needDateState: c.icons?.needDateState || null,
  });
}

/**
 * Diff two contracts; returns array of { field, left, right }.
 */
export function diffCanonicalCustomerPresentation(a = {}, b = {}) {
  const left = typeof a === "string" ? null : a;
  const right = typeof b === "string" ? null : b;
  if (!left || !right) {
    return [
      {
        field: "(root)",
        left: left || null,
        right: right || null,
      },
    ];
  }
  const ka = JSON.parse(canonicalPresentationCompareKey(left));
  const kb = JSON.parse(canonicalPresentationCompareKey(right));
  const diffs = [];
  for (const k of Object.keys({ ...ka, ...kb }).sort()) {
    if (JSON.stringify(ka[k]) !== JSON.stringify(kb[k])) {
      diffs.push({ field: k, left: ka[k], right: kb[k] });
    }
  }
  return diffs;
}
