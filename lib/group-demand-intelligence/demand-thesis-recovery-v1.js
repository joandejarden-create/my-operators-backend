/**
 * GDI 2.0 Phase 2D — offline demand-thesis evidence recovery.
 *
 * Recovers hotel demand thesis from existing opportunity fields, evidence[],
 * sources, and investigation notes. Does NOT invent attendance, room nights,
 * spend, or buying intent. Does NOT call providers. Does NOT mutate storage.
 */

import { evaluateRealisticHotelDemandThesis } from "./scaling-publication-qualification-v1.js";

export const DEMAND_THESIS_RECOVERY_V1 = "gdi-demand-thesis-recovery-v1";

const MECHANISM_RE =
  /guestroom|room.?block|room.?night|transient|overnight|lodging|accommodation|housing|hospedaje|alojamiento|project.?team|visiting|fly-?in|meeting|meetings|training|onboarding|workshop|catering|f&b|food.?and.?beverage|banquet|banquetes|corporate travel|overflow|delegate|delegados|speaker housing|room list/i;

const HOTEL_FIT_RE =
  /hotel|marriott|wyndham|waterstone|renaissance|fiesta|bayside|property|airport|theater district|times square|walking distance|near (?:the )?office|meeting room|ballroom|this hotel|our hotel/i;

const INVENTED_VOLUME_RE =
  /will book\s+\d+|guaranteed\s+\d+\s*room|definitely\s+\d+\s*room.?night|projected revenue\s*\$\d/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function claimKind(raw) {
  const k = text(raw?.claimKind || raw?.estimatedAttendanceClaimKind || "").toUpperCase();
  if (k === "FACT" || k === "PUBLISHED") return "supported_fact";
  if (k === "ESTIMATED" || k === "INFERRED") return "reasonable_inference";
  if (k === "UNKNOWN") return "unknown";
  return null;
}

/**
 * Collect candidate demand text from existing records (no invention).
 */
export function collectDemandEvidenceCandidates(opportunity = {}) {
  const fields = [
    { field: "potentialHotelBusiness", text: text(opportunity.potentialHotelBusiness) },
    { field: "proposedHotelUse", text: text(opportunity.proposedHotelUse) },
    { field: "hotelDemandThesis", text: text(opportunity.hotelDemandThesis) },
    { field: "hotelOpportunityThesis", text: text(opportunity.hotelOpportunityThesis) },
    { field: "summaryWhyHotel", text: text(opportunity.summaryWhyHotel) },
    { field: "summaryWhyMatters", text: text(opportunity.summaryWhyMatters) },
    { field: "whyThisHotel", text: text(opportunity.whyThisHotel) },
    { field: "fitExplanation", text: text(opportunity.fitExplanation) },
    { field: "roomDemandRationale", text: text(opportunity.roomDemandRationale) },
    { field: "bethesdaWinThesis", text: text(opportunity.bethesdaWinThesis) },
    { field: "reactivationThesis", text: text(opportunity.reactivationThesis) },
    { field: "localConnection", text: text(opportunity.localConnection) },
    { field: "whatIsHappening", text: text(opportunity.whatIsHappening) },
    { field: "activityHeadline", text: text(opportunity.activityHeadline) },
    { field: "demandMechanism", text: text(opportunity.demandMechanism) },
    {
      field: "salesBrief.potentialBusinessForThisHotel",
      text: text(opportunity.salesBrief?.potentialBusinessForThisHotel),
    },
    {
      field: "salesBrief.whyThisHotel",
      text: text(opportunity.salesBrief?.whyThisHotel),
    },
  ];

  if (Array.isArray(opportunity.inferredHotelDemand)) {
    for (const [i, row] of opportunity.inferredHotelDemand.entries()) {
      fields.push({
        field: `inferredHotelDemand[${i}]`,
        text: text(typeof row === "string" ? row : row?.text || row?.thesis || ""),
        claimKind: "reasonable_inference",
      });
    }
  }

  for (const e of opportunity.evidence || []) {
    const t = text(e?.extractedText || e?.value || e?.quote || "");
    if (!t) continue;
    fields.push({
      field: `evidence.${e.field || e.id || "row"}`,
      text: t,
      claimKind: claimKind(e) || "supported_fact",
      sourceUrl: e.sourceUrl || null,
    });
  }

  const out = [];
  for (const f of fields) {
    if (!f.text) continue;
    const hasMechanism = MECHANISM_RE.test(f.text);
    const hasHotelFit = HOTEL_FIT_RE.test(f.text);
    const inventsVolume = INVENTED_VOLUME_RE.test(f.text);
    out.push({
      ...f,
      hasMechanism,
      hasHotelFit,
      inventsVolume,
      commerciallyUseful: hasMechanism && !inventsVolume,
    });
  }
  return out;
}

function publishedDemandFacts(opportunity = {}) {
  const facts = [];
  if (opportunity.publishedAttendance != null) {
    facts.push({
      kind: "published_attendance",
      value: opportunity.publishedAttendance,
      note: "Published figure only — not used as invented sales volume.",
    });
  }
  if (opportunity.publishedPeakRooms != null) {
    facts.push({
      kind: "published_peak_rooms",
      value: opportunity.publishedPeakRooms,
    });
  }
  if (opportunity.publishedRoomNights != null) {
    facts.push({
      kind: "published_room_nights",
      value: opportunity.publishedRoomNights,
    });
  }
  // Estimated figures are unknowns for sales-ready — surface but do not promote
  if (
    opportunity.estimatedAttendance != null &&
    String(opportunity.estimatedAttendanceClaimKind || "").toUpperCase() !== "FACT"
  ) {
    facts.push({
      kind: "estimated_attendance_not_sales_ready_proof",
      value: opportunity.estimatedAttendance,
      claimKind: opportunity.estimatedAttendanceClaimKind || "ESTIMATED",
    });
  }
  return facts;
}

/**
 * Recover a hotel-specific demand thesis overlay from existing evidence.
 */
export function recoverDemandThesis(opportunity = {}, opts = {}) {
  const before = evaluateRealisticHotelDemandThesis(opportunity);
  const candidates = collectDemandEvidenceCandidates(opportunity);
  const useful = candidates.filter((c) => c.commerciallyUseful);
  const hotelTied = useful.filter((c) => c.hasHotelFit || text(opportunity.whyThisHotel));
  const contradictory = candidates.filter((c) => c.inventsVolume);
  const published = publishedDemandFacts(opportunity);

  const missingFacts = [];
  if (!useful.length) {
    missingFacts.push("no_guestroom_meetings_or_catering_mechanism_in_existing_evidence");
  }
  if (useful.length && !hotelTied.length && !text(opportunity.proposedHotelUse)) {
    missingFacts.push("mechanism_present_but_not_tied_to_this_hotel");
  }
  if (
    /PRIMARY_VENUE_SELECTED_NO_OVERFLOW/i.test(String(opportunity.venueSourcingStatus || "")) &&
    opportunity.externalRoomBlocksAvailable !== true
  ) {
    missingFacts.push("overflow_or_external_room_block_unproven");
  }

  const best = hotelTied[0] || useful[0] || null;
  const supportedFacts = useful
    .filter((c) => c.claimKind === "supported_fact" || !c.claimKind)
    .map((c) => ({ field: c.field, excerpt: c.text.slice(0, 240), sourceUrl: c.sourceUrl || null }));
  const inferences = useful
    .filter((c) => c.claimKind === "reasonable_inference")
    .map((c) => ({ field: c.field, excerpt: c.text.slice(0, 240) }));

  const recoveredThesis = best
    ? text(
        [
          best.text,
          text(opportunity.whyThisHotel) && best.field !== "whyThisHotel"
            ? `Hotel fit: ${text(opportunity.whyThisHotel)}`
            : "",
        ]
          .filter(Boolean)
          .join(" — "),
      )
    : "";

  const canSupport =
    Boolean(recoveredThesis) &&
    MECHANISM_RE.test(recoveredThesis) &&
    contradictory.length === 0 &&
    (HOTEL_FIT_RE.test(recoveredThesis) ||
      Boolean(text(opportunity.whyThisHotel) || text(opportunity.proposedHotelUse)));

  const overlay = canSupport
    ? {
        hotelDemandThesis: recoveredThesis.slice(0, 600),
        proposedHotelUse:
          text(opportunity.proposedHotelUse) ||
          (MECHANISM_RE.test(recoveredThesis) ? recoveredThesis.slice(0, 280) : ""),
        potentialHotelBusiness:
          text(opportunity.potentialHotelBusiness) || recoveredThesis.slice(0, 280),
        whyThisHotel:
          text(opportunity.whyThisHotel) ||
          text(opportunity.summaryWhyHotel) ||
          text(opportunity.hotelOpportunityThesis).slice(0, 280) ||
          "",
        gdi2DemandThesisRecoveryV1: {
          recoveredFromField: best?.field || null,
          recoveredAt: new Date().toISOString(),
          offlineOnly: true,
        },
      }
    : null;

  const afterProbe = overlay
    ? evaluateRealisticHotelDemandThesis({ ...opportunity, ...overlay })
    : before;

  let category = "C_NEW_RESEARCH_REQUIRED";
  if (before.ok) category = "A_ALREADY_SUPPORTED";
  else if (canSupport && best && !opts.forceNewResearch) {
    category =
      best.field.startsWith("evidence.") || best.claimKind === "supported_fact"
        ? "A_EXISTING_EVIDENCE_RECOVERED"
        : "B_DETERMINISTIC_MAPPING_CORRECTED";
  } else if (!candidates.length && !(opportunity.sources || []).length) {
    category = "E_UNABLE_TO_EVALUATE_MISSING_SOURCE_MATERIAL";
  } else if (!canSupport && useful.length === 0) {
    category = "D_COMMERCIALLY_UNSUPPORTED";
  }

  return {
    version: DEMAND_THESIS_RECOVERY_V1,
    offlineOnly: true,
    doesNotInventVolumeOrIntent: true,
    doesNotMutateStorage: true,
    organizationName: text(opportunity.organizationName) || null,
    activity:
      text(opportunity.eventName) ||
      text(opportunity.title) ||
      text(opportunity.activityHeadline) ||
      text(opportunity.whatIsHappening) ||
      null,
    beforeDemandOk: before.ok === true,
    afterDemandOk: afterProbe.ok === true,
    category,
    supportedFacts,
    reasonableInferences: inferences,
    unknowns: missingFacts,
    contradictoryEvidence: contradictory.map((c) => ({
      field: c.field,
      excerpt: c.text.slice(0, 200),
      reason: "invented_or_guaranteed_volume_language",
    })),
    publishedFactsSurfaced: published,
    recoveredThesis: recoveredThesis || null,
    overlay,
    evidenceLinks: useful
      .filter((c) => c.sourceUrl)
      .map((c) => ({ field: c.field, url: c.sourceUrl }))
      .slice(0, 8),
    missingFacts,
    note: "AI-plausible narrative alone is insufficient — recovery requires existing mechanism text tied to the hotel.",
  };
}

/**
 * Apply demand recovery overlay onto a clone (evaluation only).
 */
export function applyDemandThesisRecoveryOverlay(opportunity = {}, recovery = null) {
  const rec = recovery || recoverDemandThesis(opportunity);
  if (!rec.overlay) {
    return {
      opportunity: { ...opportunity },
      recovery: rec,
      applied: false,
    };
  }
  return {
    opportunity: {
      ...opportunity,
      ...rec.overlay,
    },
    recovery: rec,
    applied: true,
  };
}
