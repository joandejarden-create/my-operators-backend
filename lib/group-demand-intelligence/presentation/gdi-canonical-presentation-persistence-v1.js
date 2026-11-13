/**
 * Canonical customer presentation persistence contract V1.
 *
 * Ensures Airtable opportunityPayloadJson retains the semantic fields required
 * to reconstruct founder-approved customer cards/drawers without loading the
 * immutable publication package at reconstruction time.
 *
 * Publication packages remain the customer-content master when CUSTOMER_PUBLISHED.
 * This contract makes the research registry able to regenerate that presentation.
 */

export const GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1 =
  "gdi-canonical-presentation-persistence-v1";

export const GDI_PRESENTATION_SCHEMA_VERSION = "gdi-presentation-schema-v1";
export const GDI_PRESENTATION_DERIVATION_RULE_VERSION =
  "gdi-presentation-derivation-frozen-v2-bethesda-local-master";

/**
 * Keys that must survive Airtable compaction / minimal payload floors.
 * Nested drawer models included — rebuild-at-serve is not a substitute for
 * durable storage of approved historical presentation.
 */
export const REQUIRED_CUSTOMER_PRESENTATION_KEYS = Object.freeze([
  "displayTitle",
  "title",
  "drawerHeaderTitle",
  "organizationName",
  "displayCompanyName",
  "salesActionKey",
  "salesActionPillLabel",
  "commercialStageKey",
  "commercialStagePillLabel",
  "datePillDisplayLabel",
  "eventDateDisplay",
  "tileEventStartDate",
  "tileEventEndDate",
  "whatIsHappening",
  "summaryWhat",
  "cardWhyNowLine",
  "cardContactSubtitle",
  "tileContactPreview",
  "primaryContact",
  "secondaryContact",
  "needDateMatchV1",
  "needDateIndicator",
  "tileNeedDatePreview",
  "needDateRelevanceClass",
  "revenueStreamClass",
  "revenueStreamIcons",
  "priority",
  "priorityPillLabel",
  "priorityFilterKey",
  "opportunityType",
  "opportunityTypeLabel",
  "opportunityTypeFilterLabel",
  "opportunityTypeFilterCategory",
  "demandFamily",
  "demandFamilyLabel",
  "recommendedAction",
  "recommendedNextAction",
  "recommendedNextStep",
  "hotelSourcingStatusLabel",
  "sharedTenSectionDrawer",
  "scalingDrawerModel",
  "researchToDrawerV2",
  "segmentOverviewModel",
  "eventsOpportunityOverview",
  "canonicalCustomerPresentation",
  "canonicalCustomerPresentationVersion",
  "tileBadgePlan",
  "tileClassificationLine",
  "salesOpportunityQuickFilterKeys",
  "inventoryPopulations",
  "inventoryPopulationPrimary",
  "hotelMarketScopeEligible",
  "competitiveMarketScopeEligible",
  "competitiveStatus",
  "competitiveStatusLabel",
  "watchCardCurrentStatus",
  "cmiVerified",
  "commercialCategory",
  "segmentDisplayLabel",
]);

export const PRESENTATION_PROVENANCE_KEY = "canonicalPresentationProvenance";

function text(v) {
  return String(v == null ? "" : v).trim();
}

function nonempty(v) {
  if (v == null) return false;
  if (typeof v === "string") return text(v).length > 0;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return true;
}

/**
 * Resolve frozen card contact subtitle from presentation sources.
 */
export function resolveCardContactSubtitle(opp = {}) {
  if (nonempty(opp.cardContactSubtitle)) return text(opp.cardContactSubtitle);
  const preview = opp.tileContactPreview || {};
  if (nonempty(preview.displayRole)) return text(preview.displayRole);
  if (nonempty(preview.roleLabel)) return text(preview.roleLabel);
  if (nonempty(preview.professionalTitle)) return text(preview.professionalTitle);
  const pc = opp.primaryContact || {};
  if (nonempty(pc.title)) return text(pc.title);
  if (nonempty(pc.professionalTitle)) return text(pc.professionalTitle);
  if (nonempty(pc.role) && !/group travel coordinator/i.test(String(pc.role))) {
    return text(pc.role);
  }
  return null;
}

/**
 * Extract the presentation slice that must be durable in Airtable.
 */
export function extractCanonicalPresentationSlice(opp = {}) {
  const out = {};
  for (const key of REQUIRED_CUSTOMER_PRESENTATION_KEYS) {
    if (opp[key] !== undefined) out[key] = opp[key];
  }
  const subtitle = resolveCardContactSubtitle({ ...opp, ...out });
  if (subtitle) out.cardContactSubtitle = subtitle;
  out.presentationSchemaVersion = GDI_PRESENTATION_SCHEMA_VERSION;
  out.presentationDerivationRuleVersion =
    opp.presentationDerivationRuleVersion ||
    GDI_PRESENTATION_DERIVATION_RULE_VERSION;
  return out;
}

/**
 * Merge presentation onto an existing research opportunity without wiping
 * research evidence / history. Presentation keys from `presentation` win when
 * explicitly provided (including null for explicit absent need-date).
 */
export function mergeCanonicalPresentationIntoOpportunity(
  existing = {},
  presentation = {},
  provenance = {},
) {
  const next = { ...(existing && typeof existing === "object" ? existing : {}) };
  const slice = extractCanonicalPresentationSlice(presentation);
  for (const [k, v] of Object.entries(slice)) {
    if (v !== undefined) next[k] = v;
  }
  // Explicit absent need-date from approved publication
  if (
    Object.prototype.hasOwnProperty.call(presentation, "needDateMatchV1") &&
    presentation.needDateMatchV1 == null
  ) {
    next.needDateMatchV1 = null;
    next.canonicalNeedDateMatchStatus =
      "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION";
  } else if (nonempty(presentation.needDateMatchV1)) {
    next.canonicalNeedDateMatchStatus = "PRESENT";
  }
  next[PRESENTATION_PROVENANCE_KEY] = {
    schema: GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1,
    presentationSchemaVersion: GDI_PRESENTATION_SCHEMA_VERSION,
    presentationDerivationRuleVersion:
      slice.presentationDerivationRuleVersion ||
      GDI_PRESENTATION_DERIVATION_RULE_VERSION,
    ...provenance,
    mergedAt: new Date().toISOString(),
  };
  return next;
}

/**
 * After compaction, re-attach required presentation keys from the pre-compact
 * opportunity so emergency floors cannot strip customer reconstruction fields.
 */
export function reattachRequiredPresentationKeys(compacted = {}, original = {}) {
  const next =
    compacted && typeof compacted === "object" ? { ...compacted } : {};
  const slice = extractCanonicalPresentationSlice(original);
  for (const key of REQUIRED_CUSTOMER_PRESENTATION_KEYS) {
    if (slice[key] !== undefined && slice[key] !== null) {
      next[key] = slice[key];
    } else if (
      key === "needDateMatchV1" &&
      Object.prototype.hasOwnProperty.call(original, "needDateMatchV1")
    ) {
      next.needDateMatchV1 = original.needDateMatchV1;
    }
  }
  if (original[PRESENTATION_PROVENANCE_KEY]) {
    next[PRESENTATION_PROVENANCE_KEY] = original[PRESENTATION_PROVENANCE_KEY];
  }
  if (original.canonicalNeedDateMatchStatus) {
    next.canonicalNeedDateMatchStatus = original.canonicalNeedDateMatchStatus;
  }
  if (original.presentationSchemaVersion) {
    next.presentationSchemaVersion = original.presentationSchemaVersion;
  }
  if (original.presentationDerivationRuleVersion) {
    next.presentationDerivationRuleVersion =
      original.presentationDerivationRuleVersion;
  }
  return next;
}

/**
 * Validate required presentation fields for customer reconstruction.
 */
export function validateRequiredPresentationFields(opp = {}) {
  const missing = [];
  const requiredAlways = [
    "displayTitle",
    "salesActionKey",
    "salesActionPillLabel",
    "datePillDisplayLabel",
    "whatIsHappening",
    "revenueStreamClass",
    "primaryContact",
  ];
  for (const key of requiredAlways) {
    if (!nonempty(opp[key]) && !(key === "displayTitle" && nonempty(opp.title))) {
      missing.push(key);
    }
  }
  if (!resolveCardContactSubtitle(opp)) missing.push("cardContactSubtitle");
  // needDateMatchV1 may be explicitly null
  if (
    opp.needDateMatchV1 == null &&
    opp.canonicalNeedDateMatchStatus !==
      "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION" &&
    !nonempty(opp.needDateMatchV1)
  ) {
    // Only flag if status not stamped
    if (!opp.canonicalNeedDateMatchStatus) {
      missing.push("needDateMatchV1_or_explicit_absent_status");
    }
  }
  return {
    ok: missing.length === 0,
    missing,
    contract: GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1,
  };
}

/**
 * Field-level presentation parity (customer-facing). Ignores research-only blobs.
 */
export const PRESENTATION_PARITY_COMPARE_KEYS = Object.freeze([
  "displayTitle",
  "salesActionKey",
  "salesActionPillLabel",
  "datePillDisplayLabel",
  "whatIsHappening",
  "cardContactSubtitle",
  "revenueStreamClass",
  "priority",
  "recommendedAction",
  "organizationName",
  "opportunityType",
  "primaryContact.name",
  "primaryContact.title",
  "primaryContact.email",
  "needDateMatchV1.needPeriodId",
  "needDateMatchV1.tileIndicatorState",
  "needDateMatchV1.timingOverlapStatus",
  "tileContactPreview.displayRole",
  "scalingDrawerModel",
  "sharedTenSectionDrawer",
  "researchToDrawerV2",
]);

function pickPath(obj, dotted) {
  const parts = String(dotted).split(".");
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function norm(v) {
  if (v == null) return "";
  if (typeof v === "object") {
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  }
  return text(v);
}

export function comparePresentationParity(airtableOpp = {}, approvedOpp = {}) {
  const a = {
    ...airtableOpp,
    cardContactSubtitle: resolveCardContactSubtitle(airtableOpp),
  };
  const p = {
    ...approvedOpp,
    cardContactSubtitle: resolveCardContactSubtitle(approvedOpp),
  };
  const diffs = [];
  for (const key of PRESENTATION_PARITY_COMPARE_KEYS) {
    const av = pickPath(a, key);
    const pv = pickPath(p, key);
    // Approved publication absent need-date: airtable may store null + status
    if (
      key.startsWith("needDateMatchV1.") &&
      !nonempty(approvedOpp.needDateMatchV1)
    ) {
      if (
        a.canonicalNeedDateMatchStatus ===
          "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION" ||
        a.needDateMatchV1 == null
      ) {
        continue;
      }
    }
    // Drawer models: only compare when approved has them
    if (
      (key === "scalingDrawerModel" ||
        key === "sharedTenSectionDrawer" ||
        key === "researchToDrawerV2") &&
      !nonempty(pv)
    ) {
      continue;
    }
    if (!nonempty(pv) && !nonempty(av)) continue;
    if (!nonempty(av) && nonempty(pv)) {
      diffs.push({ key, status: "MISSING_IN_AIRTABLE", package: norm(pv).slice(0, 200) });
      continue;
    }
    if (nonempty(av) && nonempty(pv) && norm(av) !== norm(pv)) {
      // Soft title match
      if (key === "displayTitle" || key === "primaryContact.name") {
        if (norm(av).toLowerCase() === norm(pv).toLowerCase()) continue;
      }
      diffs.push({
        key,
        status: "MISMATCH",
        airtable: norm(av).slice(0, 200),
        package: norm(pv).slice(0, 200),
      });
    }
  }
  return {
    ok: diffs.length === 0,
    diffCount: diffs.length,
    diffs,
  };
}
