/**
 * GDI 2.0 Phase 2G — buyer qualification alignment (shadow).
 *
 * Compares legacy soft-pass paths vs hotel-commercial aligned evaluation.
 * Does NOT mutate hotelFacingEligible, canonical records, or customer visibility.
 */

import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { evaluateBuyerEvidenceGate } from "./buyer-evidence-gate-v1.js";
import { isCommerciallyRelevantScalingBuyer } from "./scaling-publication-qualification-v1.js";
import { classifyHotelCommercialRemit } from "./hotel-commercial-responsibility-v1.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import { isCustomerFacingOpportunity } from "./customer-visibility.js";

export const BUYER_QUALIFICATION_ALIGNMENT_V1 = "gdi-buyer-qualification-alignment-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function extractPerson(opportunity = {}) {
  const name =
    text(opportunity.primaryContact?.name) ||
    text(opportunity.contactName) ||
    text(opportunity.buyerName) ||
    "";
  const role =
    text(opportunity.primaryContact?.role) ||
    text(opportunity.primaryContact?.title) ||
    text(opportunity.contactRole) ||
    text(opportunity.buyerRole) ||
    "";
  return { name, role, title: role };
}

/**
 * Shadow evaluation: what sales-ready would be under hotel-commercial alignment.
 * Live hotelFacingEligible / customer visibility are reported unchanged.
 */
export function evaluateBuyerQualificationAlignment(opportunity = {}, opts = {}) {
  const person = extractPerson(opportunity);
  const visibilityBefore = isCustomerFacingOpportunity(opportunity);
  const hotelFacingBefore = opportunity.hotelFacingEligible === true;

  const legacyShared = evaluateSharedCommercialEligibility(opportunity, {
    ...opts,
    hotelCommercialAlignment: false,
  });
  const alignedShared = evaluateSharedCommercialEligibility(opportunity, {
    ...opts,
    hotelCommercialAlignment: true,
  });

  const scalingSoft = person.name
    ? isCommerciallyRelevantScalingBuyer(person, opportunity)
    : { ok: false, reason: "no_person" };
  const remit = classifyHotelCommercialRemit(person.role);
  const gate = evaluateBuyerEvidenceGate(person, opportunity, {
    contactDataOrigin: "PUBLIC_SOURCE",
    affiliationVerified: true,
  });

  const legacyWouldSalesReady = legacyShared.salesReady === true;
  const alignedWouldSalesReady = alignedShared.salesReady === true;
  const disagreement =
    legacyWouldSalesReady !== alignedWouldSalesReady
      ? legacyWouldSalesReady && !alignedWouldSalesReady
        ? "LEGACY_SOFT_PASS_REJECTED_BY_ALIGNMENT"
        : "ALIGNMENT_STRICTER_PASS_UNEXPECTED"
      : "AGREE";

  return {
    version: BUYER_QUALIFICATION_ALIGNMENT_V1,
    opportunityId: opportunity.id || null,
    organizationName: text(opportunity.organizationName) || null,
    person: person.name ? { name: person.name, role: person.role || null } : null,
    legacy: {
      sharedStatus: legacyShared.status,
      salesReady: legacyWouldSalesReady,
      gaps: legacyShared.gaps || [],
      scalingSoftPass: scalingSoft,
    },
    aligned: {
      sharedStatus: alignedShared.status,
      salesReady: alignedWouldSalesReady,
      gaps: alignedShared.gaps || [],
      hotelCommercialRemit: remit,
      buyerEvidenceGate: {
        buyerEligible: gate.buyerEligible,
        completedStages: gate.completedStages,
        notes: gate.notes,
      },
    },
    disagreement,
    liveProtections: {
      hotelFacingEligibleUnchanged: true,
      hotelFacingEligibleValue: hotelFacingBefore,
      customerVisibilityUnchanged: true,
      customerVisibilityValue: visibilityBefore,
      note: "Shadow only — does not write hotelFacingEligible or visibility.",
    },
    namedPersonLooksValid: Boolean(person.name && looksLikePersonName(person.name)),
  };
}

/**
 * Batch shadow audit for a cohort (offline).
 */
export function auditBuyerQualificationAlignment(opportunities = [], opts = {}) {
  const rows = opportunities.map((o) => evaluateBuyerQualificationAlignment(o, opts));
  const disagreed = rows.filter((r) => r.disagreement !== "AGREE");
  return {
    version: BUYER_QUALIFICATION_ALIGNMENT_V1,
    generatedAt: new Date().toISOString(),
    cohortSize: rows.length,
    agreementCount: rows.length - disagreed.length,
    disagreementCount: disagreed.length,
    legacySoftPassRejectedByAlignment: disagreed.filter(
      (r) => r.disagreement === "LEGACY_SOFT_PASS_REJECTED_BY_ALIGNMENT",
    ).length,
    liveVisibilityMutations: 0,
    liveHotelFacingMutations: 0,
    rows: opts.includeRows === false ? undefined : rows,
    disagreedSample: disagreed.slice(0, 25).map((r) => ({
      id: r.opportunityId,
      org: r.organizationName,
      person: r.person,
      disagreement: r.disagreement,
      legacyGaps: r.legacy.gaps,
      alignedGaps: r.aligned.gaps,
      remit: r.aligned.hotelCommercialRemit?.reason,
    })),
  };
}
