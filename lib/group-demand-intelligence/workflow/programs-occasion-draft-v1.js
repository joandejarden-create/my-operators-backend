/**
 * Close programs→occasion discovery when fitted generators have no signals.
 * Produces substantive draft cards from Demand Program evidence for human review.
 * Does not auto-publish.
 */

import { createHash } from "node:crypto";
import { evaluateDrawerContentCompleteness } from "./drawer-content-contract-v1.js";

export const PROGRAMS_OCCASION_DRAFT_VERSION = "gdi-programs-occasion-draft-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function draftId(hotelId, programId, year) {
  const seed = `${hotelId}|${programId || ""}|${year || "undated"}`;
  return `gdi_dgp_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * Build draft occasion cards from Demand Programs lacking linked signals.
 */
export function buildOccasionDraftsFromPrograms({
  hotelId,
  hotelLabel = null,
  programs = [],
  signals = [],
  fits = [],
  hotelConfig = null,
  maxDrafts = 12,
} = {}) {
  const id = text(hotelId);
  const signalProgramIds = new Set(
    (signals || []).map((s) => text(s.programId)).filter(Boolean)
  );
  const fitByGen = new Map();
  for (const f of fits || []) {
    if (f.demandGeneratorId) fitByGen.set(String(f.demandGeneratorId), f);
  }

  const territory =
    hotelConfig?.demandTerritory?.label ||
    hotelConfig?.city ||
    hotelLabel ||
    "the hotel market";
  const rooms = hotelConfig?.capabilityProfile?.totalGuestrooms || null;

  const drafts = [];
  for (const prog of programs || []) {
    if (drafts.length >= maxDrafts) break;
    const pid = text(prog.programId);
    if (pid && signalProgramIds.has(pid)) continue; // already has signal coverage

    const programName = text(prog.programName) || "Demand program";
    const org =
      text(prog.organizationName) ||
      text(fitByGen.get(String(prog.demandGeneratorId || ""))?.organizationName) ||
      programName;
    const yearHint =
      String(prog.typicalTiming || "").match(/20\d{2}/)?.[0] ||
      String(new Date().getFullYear() + 1);
    const timing =
      text(prog.typicalTiming) ||
      `Typical cycle timing not fully dated — research ${yearHint} edition`;
    const market = text(prog.market) || territory;
    const fit = fitByGen.get(String(prog.demandGeneratorId || ""));
    const thesisParts = [
      `${org}'s ${programName} is a recurring demand program`,
      market ? `with geography around ${market}` : null,
      `that can create group lodging need near ${territory}`,
      rooms ? `(hotel capacity ${rooms} keys is a fit ceiling, not demand proof)` : null,
      fit?.fitRationale ? `Hotel fit note: ${fit.fitRationale}` : null,
    ].filter(Boolean);
    const thesis = thesisParts.join(". ") + ".";
    const whyNow = `Confirm the next dated cycle (${yearHint} window) and whether housing/room-block procurement is open — program recurrence is evidence of occasion shape, not a confirmed booking.`;
    const nextAction = `Verify ${yearHint} dates + official source, then identify the lodging / housing coordinator contact route (email, phone, or form — not homepage-only).`;
    const caveats = [
      "Demand Program present without a dated Demand Generator Signal — draft requires human confirmation of the next occasion.",
      "Do not treat typical timing as a confirmed event start date.",
    ];

    const draft = {
      id: draftId(id, pid || programName, yearHint),
      hotelId: id,
      organizationName: org,
      company: org,
      title: `${programName} (${yearHint} cycle research)`,
      opportunityName: `${programName} (${yearHint} cycle research)`,
      eventStartDate: null,
      timingNote: timing,
      location: market,
      travelingCohort:
        "Expected traveling attendees / planners for a recurring program — size not established until cycle evidence",
      hotelOpportunityThesis: thesis,
      fitExplanation: thesis,
      summaryWhyHotel: thesis,
      summaryWhat: `${programName} — recurring demand program under research for lodging capture near ${territory}.`,
      whyNow,
      recommendedAction: nextAction,
      materialLimitations: caveats,
      sources: [],
      demandGeneratorId: prog.demandGeneratorId || null,
      programId: pid || null,
      programType: prog.programType || null,
      draftSource: "demand_program_occasion_synthesis",
      prospectPublicationBasis: "program_draft_pending_human_review",
      prospectReviewSection: "EXPLORATORY",
      prospectHotelFacing: false,
      prospectEnrichmentQueue: true,
      customerVisible: false,
      hotelFitScore: null,
      hotelFitScoreNote:
        "Qualitative draft from Demand Program reuse — no unexplained numeric fit score",
      draftNote:
        "Evidence-grounded program→occasion draft — human review approves/corrects/rejects; not auto-published",
    };
    const drawer = evaluateDrawerContentCompleteness(draft);
    drafts.push({
      ...draft,
      draftDrawerOk: drawer.ok,
      draftDrawerFailed: drawer.failed,
      draftDrawerWarnings: drawer.warnings,
    });
  }

  return {
    version: PROGRAMS_OCCASION_DRAFT_VERSION,
    hotelId: id,
    asOf: new Date().toISOString(),
    programCount: (programs || []).length,
    signalCount: (signals || []).length,
    draftCount: drafts.length,
    drafts,
    note: "Drafts close the programs→occasion gap when signals are missing; human review required",
  };
}
