/**
 * GDI full-field presentation parity — compares the entire customer-facing
 * presentation object across local / admin / published channels.
 *
 * Partial tile or drawer field subsets must NOT return FULL_PARITY_PASS.
 * Label those explicitly as TILE_DRAWER_FIELD_SUBSET_PASS or similar.
 */
import { createHash } from "node:crypto";
import { buildCanonicalPresentationObject } from "../contracts/gdi-canonical-opportunity-contract-v1.js";
import {
  dedupeOpportunityContacts,
  validateCustomerPresentationIntegrity,
} from "./gdi-customer-presentation-integrity-v1.js";

export const GDI_FULL_PRESENTATION_PARITY_V1 =
  "gdi-full-presentation-parity-v1";

export const PARITY_RESULT = Object.freeze({
  FULL_PARITY_PASS: "FULL_PARITY_PASS",
  TILE_DRAWER_FIELD_SUBSET_PASS: "TILE_DRAWER_FIELD_SUBSET_PASS",
  FULL_PARITY_FAIL: "FULL_PARITY_FAIL",
  INTEGRITY_BLOCKED: "INTEGRITY_BLOCKED",
});

/** Customer-visible paths that must match exactly (semantic). */
export const FULL_PRESENTATION_COMPARE_PATHS = Object.freeze([
  "identity.opportunityId",
  "identity.hotelId",
  "identity.canonicalTitle",
  "identity.organization",
  "identity.opportunityType",
  "commercialClassification.inventoryClass",
  "commercialClassification.priority",
  "commercialClassification.salesRelevance",
  "commercialClassification.recommendedAction",
  "commercialClassification.hotelSourcingStatus",
  "commercialClassification.demandTiming",
  "tile.title",
  "tile.commercialCategory",
  "tile.relevantDates",
  "tile.description",
  "tile.primaryContact.name",
  "tile.primaryContact.role",
  "tile.primaryContact.title",
  "tile.contactRole",
  "tile.badges.priority",
  "tile.badges.salesAction",
  "tile.badges.inventoryClass",
  "drawer.opportunityOverview",
  "drawer.whatIsHappening",
  "drawer.potentialBusinessForThisHotel",
  "drawer.whyThisHotel",
  "drawer.advantages",
  "drawer.limitationsAndTradeOffs",
  "drawer.whyActNow",
  "drawer.establishedFacts",
  "drawer.uncertainties",
  "drawer.primaryContact.name",
  "drawer.primaryContact.role",
  "drawer.primaryContact.title",
  "drawer.secondaryContacts",
  "drawer.contactRelevance",
  "drawer.recommendedNextStep",
  "drawer.qualificationQuestions",
  "drawer.supportingEvidence",
  "opportunity.salesActionKey",
  "opportunity.salesActionPillLabel",
  "opportunity.priorityFilterKey",
  "opportunity.priority",
  "opportunity.hotelSourcingStatusNormalized",
  "opportunity.hotelSourcingStatusLabel",
  "opportunity.salesOpportunityQuickFilterKeys",
  "opportunity.primaryContact",
  "opportunity.secondaryContact",
  "opportunity.scalingTwoContactPresentation",
  "opportunity.recommendedNextStep",
  "opportunity.whatIsHappening",
  "opportunity.whyThisHotel",
  "opportunity.whyActNow",
  "opportunity.establishedFacts",
  "opportunity.uncertainties",
  "opportunity.qualificationQuestions",
  "opportunity.sources",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function getPath(obj, path) {
  return String(path)
    .split(".")
    .reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

/**
 * Deterministic normalization for non-semantic noise only
 * (whitespace, key order via JSON stringify of sorted structures).
 */
export function normalizeForParityCompare(value) {
  if (value == null) return null;
  if (typeof value === "string") return text(value);
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) {
    return value.map((v) => normalizeForParityCompare(v));
  }
  if (typeof value === "object") {
    const out = {};
    for (const k of Object.keys(value).sort()) {
      // Ignore publication timestamps / hashes that differ by channel.
      if (
        /^(contentHash|customerFacingPresentationHash|publicationTimestamp|publishedAt|publishedVersion|_)/i.test(
          k,
        )
      ) {
        continue;
      }
      out[k] = normalizeForParityCompare(value[k]);
    }
    return out;
  }
  return value;
}

export function stablePresentationHash(presentation = {}) {
  const normalized = normalizeForParityCompare(presentation);
  return createHash("sha256")
    .update(JSON.stringify(normalized))
    .digest("hex");
}

/**
 * Build the authoritative customer presentation object used for freeze + compare.
 */
export function buildAuthoritativeCustomerPresentation(
  opportunity = {},
  opts = {},
) {
  const deduped = dedupeOpportunityContacts(opportunity);
  const integrity = validateCustomerPresentationIntegrity(deduped);
  const presentation = buildCanonicalPresentationObject(deduped, {
    hotelId: opts.hotelId || deduped.hotelId,
    lock: opts.lock === true || deduped.publishedPresentationLocked === true,
    publicationVersion: opts.publicationVersion || null,
    publicationTimestamp: opts.publicationTimestamp || null,
  });
  const hash = stablePresentationHash({
    identity: presentation.identity,
    commercialClassification: presentation.commercialClassification,
    tile: presentation.tile,
    drawer: presentation.drawer,
    opportunity: {
      salesActionKey: presentation.opportunity?.salesActionKey,
      salesActionPillLabel: presentation.opportunity?.salesActionPillLabel,
      priorityFilterKey: presentation.opportunity?.priorityFilterKey,
      priority: presentation.opportunity?.priority,
      hotelSourcingStatusNormalized:
        presentation.opportunity?.hotelSourcingStatusNormalized,
      hotelSourcingStatusLabel: presentation.opportunity?.hotelSourcingStatusLabel,
      salesOpportunityQuickFilterKeys:
        presentation.opportunity?.salesOpportunityQuickFilterKeys,
      primaryContact: presentation.opportunity?.primaryContact,
      secondaryContact: presentation.opportunity?.secondaryContact,
      scalingTwoContactPresentation:
        presentation.opportunity?.scalingTwoContactPresentation,
      recommendedNextStep: presentation.opportunity?.recommendedNextStep,
      whatIsHappening: presentation.opportunity?.whatIsHappening,
      whyThisHotel: presentation.opportunity?.whyThisHotel,
      whyActNow: presentation.opportunity?.whyActNow,
      establishedFacts: presentation.opportunity?.establishedFacts,
      uncertainties: presentation.opportunity?.uncertainties,
      qualificationQuestions: presentation.opportunity?.qualificationQuestions,
      sources: presentation.opportunity?.sources,
    },
  });
  return {
    version: GDI_FULL_PRESENTATION_PARITY_V1,
    integrity,
    presentation: {
      ...presentation,
      publicationMetadata: {
        ...presentation.publicationMetadata,
        contentHash: hash,
      },
    },
    contentHash: hash,
    opportunity: {
      ...deduped,
      customerFacingPresentationHash: hash,
      publishedPresentationLocked:
        opts.lock === true || deduped.publishedPresentationLocked === true,
    },
  };
}

function comparePresentations(left, right, paths = FULL_PRESENTATION_COMPARE_PATHS) {
  const diffs = [];
  for (const path of paths) {
    const a = normalizeForParityCompare(getPath(left, path));
    const b = normalizeForParityCompare(getPath(right, path));
    if (JSON.stringify(a) !== JSON.stringify(b)) {
      diffs.push({
        path,
        left: a,
        right: b,
      });
    }
  }
  return diffs;
}

/**
 * Full-field parity between two opportunity payloads (local vs published, etc.).
 */
export function compareFullCustomerPresentations(
  leftOpportunity = {},
  rightOpportunity = {},
  opts = {},
) {
  const left = buildAuthoritativeCustomerPresentation(leftOpportunity, opts);
  const right = buildAuthoritativeCustomerPresentation(rightOpportunity, opts);

  if (!left.integrity.ok || !right.integrity.ok) {
    return {
      version: GDI_FULL_PRESENTATION_PARITY_V1,
      result: PARITY_RESULT.INTEGRITY_BLOCKED,
      ok: false,
      leftHash: left.contentHash,
      rightHash: right.contentHash,
      leftIntegrity: left.integrity,
      rightIntegrity: right.integrity,
      diffs: [],
      scope: "full_presentation_object",
    };
  }

  const diffs = comparePresentations(left.presentation, right.presentation);
  const hashMatch = left.contentHash === right.contentHash;
  const ok = diffs.length === 0 && hashMatch;

  return {
    version: GDI_FULL_PRESENTATION_PARITY_V1,
    result: ok
      ? PARITY_RESULT.FULL_PARITY_PASS
      : PARITY_RESULT.FULL_PARITY_FAIL,
    ok,
    leftHash: left.contentHash,
    rightHash: right.contentHash,
    leftIntegrity: left.integrity,
    rightIntegrity: right.integrity,
    diffs,
    scope: "full_presentation_object",
    note: ok
      ? "Full customer presentation object matched."
      : "Full-field differences detected — do not label as FULL_PARITY_PASS from tile/drawer subset checks.",
  };
}

/**
 * Label for older subset comparisons — never upgrades to FULL_PARITY_PASS.
 */
export function labelSubsetParityPass(subsetName = "tile_drawer") {
  return {
    result: PARITY_RESULT.TILE_DRAWER_FIELD_SUBSET_PASS,
    scope: subsetName,
    upgradesToFullParity: false,
    note: "Selected-field match only. Full presentation parity not certified.",
  };
}
