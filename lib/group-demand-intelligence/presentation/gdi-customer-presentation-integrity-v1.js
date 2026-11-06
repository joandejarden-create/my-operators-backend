/**
 * GDI customer presentation integrity — blocking validation for one
 * authoritative customer-facing presentation object.
 *
 * Catches contradictions that tile/drawer field-subset parity missed:
 * duplicate contacts, named contact classified as unidentified, action /
 * sourcing / demand-stage inconsistency, missing frozen presentation body.
 */
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const GDI_CUSTOMER_PRESENTATION_INTEGRITY_V1 =
  "gdi-customer-presentation-integrity-v1";

const UNIDENTIFIED_CEILING_RE =
  /named event contact not publicly identified|buyer not yet identified|office operations contact not yet identified/i;

const INTERNAL_LABEL_RE =
  /\b(CONTACT_RELEVANT|INELIGIBLE_UNSPECIFIED|CONTRACTING_AUTHORITY_VERIFIED|NO_CONTACT|GENERIC_ONLY|NAMED_DIRECT|ORGANIZATION_PATH|FUNCTIONAL_CONTACT)\b/;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normName(v) {
  return text(v)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function contactName(c) {
  if (!c || typeof c !== "object") return "";
  return text(c.name || c.personIdentity || c.displayName);
}

/**
 * Deduplicate secondary / backup contacts against primary and each other.
 * Identity is unique within one opportunity unless roles genuinely differ
 * (different title + organization). Same person+role → keep first only.
 */
export function dedupeOpportunityContacts(opportunity = {}) {
  const o = { ...opportunity };
  const primary = o.primaryContact && typeof o.primaryContact === "object"
    ? { ...o.primaryContact }
    : null;
  const primaryKey = normName(contactName(primary));

  function roleKey(c) {
    return [
      normName(contactName(c)),
      normName(c?.role || c?.title),
      normName(c?.organization),
    ].join("|");
  }

  const seen = new Set();
  if (primaryKey) seen.add(roleKey(primary));

  let secondary = o.secondaryContact && typeof o.secondaryContact === "object"
    ? { ...o.secondaryContact }
    : null;
  if (secondary) {
    const key = roleKey(secondary);
    if (
      !contactName(secondary) ||
      (primaryKey && normName(contactName(secondary)) === primaryKey) ||
      seen.has(key)
    ) {
      secondary = null;
    } else {
      seen.add(key);
    }
  }

  const backups = Array.isArray(o.backupContacts)
    ? o.backupContacts.filter((b) => {
        if (!contactName(b)) return false;
        const key = roleKey(b);
        if (primaryKey && normName(contactName(b)) === primaryKey) return false;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
    : [];

  const next = {
    ...o,
    primaryContact: primary,
    secondaryContact: secondary,
    backupContacts: backups,
  };
  if (primary && looksLikePersonName(contactName(primary))) {
    if (!looksLikePersonName(next.contactName)) {
      next.contactName = contactName(primary);
    }
    if (!text(next.contactRole)) {
      next.contactRole = text(primary.role || primary.title) || null;
    }
  }
  return next;
}

/**
 * Blocking integrity check for a customer-facing opportunity / snapshot.
 * @returns {{ ok: boolean, fails: string[], warnings: string[] }}
 */
export function validateCustomerPresentationIntegrity(opportunity = {}) {
  const fails = [];
  const warnings = [];
  const o = opportunity || {};
  const id = text(o.id || o.opportunityId) || "(missing-id)";

  const primary = o.primaryContact;
  const secondary = o.secondaryContact;
  const pName = contactName(primary);
  const sName = contactName(secondary);
  const two = o.scalingTwoContactPresentation || null;

  if (pName && sName && normName(pName) === normName(sName)) {
    const pRole = normName(primary?.role || primary?.title);
    const sRole = normName(secondary?.role || secondary?.title);
    if (!pRole || !sRole || pRole === sRole) {
      fails.push("contacts:duplicate_primary_secondary_same_identity");
    }
  }

  const backupNames = (Array.isArray(o.backupContacts) ? o.backupContacts : [])
    .map(contactName)
    .filter(Boolean);
  if (sName && backupNames.some((n) => normName(n) === normName(sName))) {
    fails.push("contacts:duplicate_secondary_in_backupContacts");
  }
  const seenBackup = new Set();
  for (const n of backupNames) {
    const k = normName(n);
    if (seenBackup.has(k)) {
      fails.push("contacts:duplicate_backup_identity");
      break;
    }
    seenBackup.add(k);
  }

  if (two?.secondary?.customerFacingNamed && sName) {
    const twoSec = contactName(two.secondary);
    // Two-contact slot + top-level secondary must not disagree on identity.
    if (twoSec && normName(twoSec) !== normName(sName)) {
      fails.push("contacts:scaling_secondary_diverges_from_secondaryContact");
    }
  }

  const ceiling =
    text(o.customerContactDrawer?.publicCeilingCopy) ||
    text(two?.primary?.researchGapCopy) ||
    "";
  if (
    pName &&
    looksLikePersonName(pName) &&
    UNIDENTIFIED_CEILING_RE.test(ceiling)
  ) {
    fails.push(
      "contacts:named_primary_present_but_classified_unidentified",
    );
  }
  if (
    two?.primary &&
    two.primary.customerFacingNamed === false &&
    pName &&
    looksLikePersonName(pName)
  ) {
    fails.push(
      "contacts:scaling_primary_gap_despite_verified_named_primaryContact",
    );
  }

  const action = text(
    o.salesActionKey || o.salesActionPillLabel || o.recommendedNextStep,
  );
  const sourcing = text(
    o.hotelSourcingStatusNormalized ||
      o.hotelSourcingStatusLabel ||
      o.hotelSourcingStatus ||
      o.commercialStageKey,
  ).toUpperCase();
  if (
    /FORECAST|HOLD_INVENTORY|COMMIT_ROOMS/i.test(action) &&
    /UNCONFIRMED|NOT_CONFIRMED|UNKNOWN|ACCOUNT_DEVELOPMENT/i.test(sourcing)
  ) {
    fails.push("action:inconsistent_with_sourcing_status");
  }

  const demandStage = text(
    o.demandStage || o.commercialDemandStage || o.commercialStageKey || "",
  ).toUpperCase();
  const inventoryClass = text(
    o.commercialInventoryClass || o.commercialClassification || "",
  ).toUpperCase();
  if (
    inventoryClass === "ACCOUNT_DEVELOPMENT" &&
    /CONFIRMED_DEMAND|ACTIVE_HOTEL_SOURCING|LIVE_BRIEF|ROOM_BLOCK_CONFIRMED/.test(
      demandStage,
    )
  ) {
    fails.push("demand_stage:inflated_vs_account_development");
  }

  const tilePriority = text(o.priorityFilterKey || o.priority || "").toUpperCase();
  const drawerPriority = text(
    o.drawerPriorityFilterKey || o.sharedTenSectionDrawer?.priority || "",
  ).toUpperCase();
  if (
    tilePriority &&
    drawerPriority &&
    tilePriority !== drawerPriority &&
    !drawerPriority.includes(tilePriority) &&
    !tilePriority.includes(drawerPriority)
  ) {
    fails.push("priority:tile_drawer_mismatch");
  }

  const narratives = [
    o.whatIsHappening,
    o.recommendedNextStep,
    o.whyThisHotel,
    o.whyActNow,
    primary?.whyThisContact,
    secondary?.whyThisContact,
  ];
  for (const n of narratives) {
    if (INTERNAL_LABEL_RE.test(text(n))) {
      fails.push("copy:internal_only_label_in_customer_narrative");
      break;
    }
  }

  if (o.publishedPresentationLocked === true) {
    if (!two || !two.primary) {
      warnings.push(
        "freeze:scalingTwoContactPresentation_missing_on_locked_snapshot",
      );
    }
    if (!text(o.customerFacingPresentationHash) && !text(o.contentHash)) {
      warnings.push("freeze:presentation_hash_missing");
    }
  }

  if (!pName && looksLikePersonName(text(o.contactName))) {
    fails.push("contacts:contactName_present_but_primaryContact_missing");
  }

  return {
    version: GDI_CUSTOMER_PRESENTATION_INTEGRITY_V1,
    opportunityId: id,
    ok: fails.length === 0,
    fails,
    warnings,
  };
}

/**
 * Normalize contacts then validate — used before publish and in Full E2E.
 */
export function prepareAndValidateCustomerPresentation(opportunity = {}) {
  const normalized = dedupeOpportunityContacts(opportunity);
  const gate = validateCustomerPresentationIntegrity(normalized);
  return { opportunity: normalized, gate };
}
