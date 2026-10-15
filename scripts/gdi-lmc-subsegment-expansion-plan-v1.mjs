#!/usr/bin/env node
/**
 * LMC subsegment expansion — three-hotel applicability + coverage plan.
 *
 * Does not invent prospects or spend paid budget. Emits hotel-by-hotel
 * applicability, queries, existing LMC inventory classification, and
 * scorecard-ready coverage shells for the standard coordinator.
 *
 *   node scripts/gdi-lmc-subsegment-expansion-plan-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadHotelConfig,
  buildHotelSegmentResearchPlan,
  RESEARCH_SEGMENT,
} from "../lib/group-demand-intelligence/workflow/hotel-segment-research-plan-v1.js";
import { buildHotelSegmentResearchBrief } from "../lib/group-demand-intelligence/workflow/hotel-segment-research-brief-v1.js";
import {
  assessLocalMeetingsCateringApplicability,
  buildLocalMeetingsCateringCoverageSnapshot,
  buildLocalMeetingsCateringResearchQueries,
  isLocalMeetingsCateringOpportunity,
  LMC_SUBSEGMENT,
  LMC_SUBSEGMENT_LABEL,
  LOCAL_MEETINGS_CATERING_RESEARCH_VERSION,
} from "../lib/group-demand-intelligence/workflow/local-meetings-catering-research-v1.js";
import {
  createEmptyScorecardRow,
  attachLmcSubsegmentCoverage,
  EXECUTION_MODE,
} from "../lib/group-demand-intelligence/workflow/research-scorecard-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "gdi-lmc-subsegment-expansion");

const HOTELS = [
  { hotelId: "rec0eTEnCqlsGJJt9", short: "Mayflower" },
  { hotelId: "recnPB3Q02NO8Ma1o", short: "SanDiego" },
  { hotelId: "recLuxvwwxID7U2B8", short: "Bethesda" },
];

function loadOpportunities(hotelId) {
  const candidates = [
    path.join(
      ROOT,
      "data",
      "group-demand-intelligence",
      "hotels",
      hotelId,
      "opportunities.json"
    ),
    // Sibling deploy/workspace mirror when local hotel store is empty
    path.join(
      ROOT,
      "..",
      "gdi-release-main",
      "data",
      "group-demand-intelligence",
      "hotels",
      hotelId,
      "opportunities.json"
    ),
  ];
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      const opps = Array.isArray(raw) ? raw : raw.opportunities || [];
      if (opps.length) return opps;
    } catch {
      /* try next */
    }
  }
  return [];
}

function classifyExistingLmc(opps = []) {
  const lmc = opps.filter((o) => isLocalMeetingsCateringOpportunity(o));
  const pe = opps.filter((o) => {
    const seg = String(o.researchSegment || o.segment || "");
    const type = String(o.opportunityType || "");
    return (
      Boolean(o.peVenueId) ||
      /private.?event|wedding|venue.?partner|preferred.?lodging/i.test(seg) ||
      /VENUE_PARTNERSHIP|VENUE_REFERRAL|PREFERRED_LODGING/i.test(type) ||
      o.relationshipGrain === "VENUE_REFERRAL"
    );
  });

  function bucket(list, pred) {
    return list.filter(pred);
  }

  return {
    lmcTotal: lmc.length,
    publishedFacing: lmc.filter(
      (o) => o.customerVisible !== false && o.prospectHotelFacing !== false
    ).length,
    cateringProspects: bucket(
      lmc,
      (o) =>
        o.cateringOnly === true ||
        o.guestroomStatus === "NOT_REQUIRED" ||
        /catering|banquet|gala|fundrais|lunch|dinner/i.test(
          [o.title, o.functionType, o.lmcSubsegmentLabel, o.summaryWhat].join(" ")
        )
    ).map(summarizeOpp),
    meetingSpaceProspects: bucket(
      lmc,
      (o) =>
        /meeting|training|workshop|board|room.?rental|chapter/i.test(
          [o.title, o.functionType, o.lmcSubsegmentLabel, o.summaryWhat].join(" ")
        )
    ).map(summarizeOpp),
    socialOrPlannerProspects: bucket(
      lmc,
      (o) =>
        o.lmcSubsegment === LMC_SUBSEGMENT.SOCIAL_CELEBRATIONS_CATERING ||
        o.lmcSubsegment ===
          LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES ||
        /planner|mitzvah|wedding|social|quince|anniversary/i.test(
          [o.title, o.functionType, o.lmcSubsegmentLabel].join(" ")
        )
    ).map(summarizeOpp),
    outsideVenueLodgingPartnerships: pe.slice(0, 40).map(summarizeOpp),
    namedContacts: lmc.filter((o) => Boolean(o.contactName)).length,
    fallbackRoutes: lmc.filter(
      (o) => !o.contactName && (o.contactEmail || o.contactPhone || o.contactUrl)
    ).length,
    datedOccasions: lmc.filter(
      (o) => o.relationshipGrain === "DATED_OCCASION" || o.eventStartDate
    ).length,
    standingRelationships: lmc.filter(
      (o) =>
        o.relationshipGrain !== "DATED_OCCASION" &&
        (o.standingOpportunity !== false || !o.eventStartDate)
    ).length,
    allLmc: lmc.map(summarizeOpp),
  };
}

function summarizeOpp(o) {
  return {
    id: o.id || o.opportunityId,
    title: o.title || o.opportunityName || o.organizationName,
    organizationName: o.organizationName || o.company,
    lmcSubsegment: o.lmcSubsegment || null,
    guestroomStatus: o.guestroomStatus || null,
    relationshipGrain: o.relationshipGrain || null,
    customerVisible: o.customerVisible,
    contactName: o.contactName || null,
    contactEmail: o.contactEmail || null,
    contactRoute: o.contactEmail || o.contactPhone || o.contactUrl || null,
  };
}

function hotelPlan(hotelMeta) {
  const cfg = loadHotelConfig(hotelMeta.hotelId);
  if (!cfg) {
    return {
      hotelId: hotelMeta.hotelId,
      short: hotelMeta.short,
      error: "hotel_config_missing",
    };
  }
  const opps = loadOpportunities(hotelMeta.hotelId);
  const plan = buildHotelSegmentResearchPlan({
    hotelId: hotelMeta.hotelId,
    hotelConfig: cfg,
    opportunities: opps,
  });
  const lmcRow = (plan.segments || []).find(
    (s) => s.segmentId === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING
  );
  const app = assessLocalMeetingsCateringApplicability(cfg);
  const coverage = buildLocalMeetingsCateringCoverageSnapshot(cfg);
  const queries = buildLocalMeetingsCateringResearchQueries(cfg);
  const brief = buildHotelSegmentResearchBrief({
    hotelId: hotelMeta.hotelId,
    segmentId: RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
    hotelConfig: cfg,
    segmentPlanRow: lmcRow,
  });
  const existing = classifyExistingLmc(opps);
  const scorecard = createEmptyScorecardRow({
    hotelId: hotelMeta.hotelId,
    segmentId: RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
    mode: EXECUTION_MODE.PLAN_ONLY,
    objective: brief.objective,
  });
  attachLmcSubsegmentCoverage(scorecard, coverage);

  function coveredByExisting(id) {
    if (existing.allLmc.some((o) => o.lmcSubsegment === id)) return true;
    const blob = existing.allLmc
      .map((o) => [o.title, o.organizationName].join(" "))
      .join(" ")
      .toLowerCase();
    if (
      id === LMC_SUBSEGMENT.ASSOCIATION_LUNCHES_CHAPTER_AWARDS &&
      /chapter|association|asid|acec|boma|cmaa|mba|partnership|urban league|arc of/i.test(
        blob
      )
    ) {
      return true;
    }
    if (
      id === LMC_SUBSEGMENT.NONPROFIT_FUNDRAISERS_GALAS_RECEPTIONS &&
      /urban league|arc of|nonprofit|foundation|aero club/i.test(blob)
    ) {
      return true;
    }
    return false;
  }

  const remainingGaps = [
    ...(app.constraints?.notesForVerification || []),
    ...coverage.applicableSubsegmentIds
      .filter((id) => !coveredByExisting(id))
      .map((id) => `subsegment_research_gap:${id}`),
    existing.namedContacts < existing.lmcTotal
      ? `named_contact_gaps:${existing.lmcTotal - existing.namedContacts}_of_${existing.lmcTotal}_use_org_fallback`
      : null,
    "Existing LMC cards lack lmcSubsegment tags until next enrich/refresh pass",
    "Do not force equal quotas or invent mitzvah/social counts",
    "Continue unpaid research; paid spend stays within consolidated $11 proposal LMC line ($2) — no silent increase",
  ].filter(Boolean);

  return {
    hotelId: hotelMeta.hotelId,
    short: hotelMeta.short,
    displayName: cfg.displayName,
    city: cfg.city || null,
    lmcApplicable: app.applicable,
    needsProfileEnrichment: app.needsProfileEnrichment,
    rationale: app.rationale,
    hotelMeetingSnapshot: app.hotelMeetingSnapshot,
    constraints: app.constraints,
    applicableSubsegmentIds: app.applicableSubsegmentIds,
    subsegments: Object.entries(app.subsegments || {}).map(([id, row]) => ({
      id,
      label: LMC_SUBSEGMENT_LABEL[id],
      applicable: row.applicable,
      rationale: row.rationale,
    })),
    researchQueries: queries.filter((q) => q.query),
    existingInventory: existing,
    remainingGaps,
    pePathNote: coverage.pePathNote,
    briefId: brief.briefId,
    scorecardShell: {
      version: scorecard.version,
      segmentCoverageStatus: scorecard.scope.segmentCoverageStatus,
      lmcSubsegmentCoverageTotals: coverage.totals,
      coverageGaps: scorecard.scope.coverageGaps,
    },
    budgetNote:
      "LMC subsegment expansion is included in the existing three-hotel consolidated paid-research proposal. Do not silently increase budget or strip other applicable segments.",
  };
}

function mdReport(results) {
  const lines = [
    "# Local Meetings & Catering — subsegment expansion (three-hotel)",
    "",
    `Version: \`${LOCAL_MEETINGS_CATERING_RESEARCH_VERSION}\``,
    "",
    "Extension of the standard GDI coordinator LMC stage — not a separate pipeline.",
    "Preferred lodging with outside venues remains on the Private Events path.",
    "",
    "## Budget",
    "",
    "Included in the current three-hotel research plan / consolidated paid-research proposal.",
    "No silent budget increase. No equal subsegment quotas. No invented prospects.",
    "",
  ];

  for (const h of results) {
    lines.push(`## ${h.displayName || h.short} (\`${h.hotelId}\`)`);
    lines.push("");
    if (h.error) {
      lines.push(`**Error:** ${h.error}`);
      lines.push("");
      continue;
    }
    const snap = h.hotelMeetingSnapshot || {};
    lines.push("### Verified meeting inventory");
    lines.push("");
    lines.push(
      `- Rooms: ${snap.meetingRoomCount ?? "unknown"} · Sq ft: ${snap.totalMeetingSpaceSqFt ?? "unknown"} · Reception: ${snap.largestReceptionCapacity ?? "unknown"} · Ballroom sq ft: ${snap.largestBallroomSqFt ?? "unknown"}`
    );
    lines.push(`- Applicable: **${h.lmcApplicable}** — ${h.rationale}`);
    lines.push("");
    lines.push("### Constraint unknowns (do not invent)");
    lines.push("");
    const c = h.constraints || {};
    lines.push(
      `- Outside catering: \`${c.outsideCateringAllowed}\` · Certified kosher: \`${c.certifiedKosherCatering}\` · AV: \`${c.audiovisualSupport}\` · Accessibility: \`${c.accessibilityVerified}\` · Parking: \`${c.parkingAccess}\``
    );
    lines.push(`- ${c.kosherInferencePolicy || ""}`);
    lines.push("");
    lines.push("### Applicable subsegments");
    lines.push("");
    for (const s of h.subsegments || []) {
      lines.push(
        `- ${s.applicable ? "✓" : "·"} **${s.label}** — ${s.rationale}`
      );
    }
    lines.push("");
    lines.push("### Existing saved / visible inventory");
    lines.push("");
    const inv = h.existingInventory || {};
    lines.push(
      `- LMC total: **${inv.lmcTotal}** (facing-ish: ${inv.publishedFacing}) · Named contacts: ${inv.namedContacts} · Fallback routes: ${inv.fallbackRoutes}`
    );
    lines.push(
      `- Dated occasions: ${inv.datedOccasions} · Standing relationships: ${inv.standingRelationships}`
    );
    lines.push("");
    lines.push("#### Local catering prospects");
    for (const o of (inv.cateringProspects || []).slice(0, 12)) {
      lines.push(
        `- \`${o.id}\` ${o.title} — guestroom=${o.guestroomStatus || "?"} · contact=${o.contactRoute || "gap"}`
      );
    }
    if (!(inv.cateringProspects || []).length) lines.push("- _(none classified yet)_");
    lines.push("");
    lines.push("#### Meeting-space prospects");
    for (const o of (inv.meetingSpaceProspects || []).slice(0, 12)) {
      lines.push(`- \`${o.id}\` ${o.title}`);
    }
    if (!(inv.meetingSpaceProspects || []).length) {
      lines.push("- _(none classified yet)_");
    }
    lines.push("");
    lines.push("#### Social-event / planner prospects");
    for (const o of (inv.socialOrPlannerProspects || []).slice(0, 12)) {
      lines.push(`- \`${o.id}\` ${o.title}`);
    }
    if (!(inv.socialOrPlannerProspects || []).length) {
      lines.push("- _(none tagged yet — research pending; no quota)_");
    }
    lines.push("");
    lines.push("#### Outside-venue lodging partnerships (Private Events — separate)");
    for (const o of (inv.outsideVenueLodgingPartnerships || []).slice(0, 8)) {
      lines.push(`- \`${o.id}\` ${o.title}`);
    }
    if (!(inv.outsideVenueLodgingPartnerships || []).length) {
      lines.push("- _(none in local store)_");
    }
    lines.push("");
    lines.push("### Remaining evidence & contact gaps");
    lines.push("");
    for (const g of h.remainingGaps || []) lines.push(`- ${g}`);
    lines.push("");
    lines.push("### Research queries (applicable subsegments only)");
    lines.push("");
    for (const q of h.researchQueries || []) {
      lines.push(`- **${q.intent}** (\`${q.subsegment || ""}\`): ${q.query}`);
    }
    lines.push("");
  }

  lines.push("## Coordinator notes");
  lines.push("");
  lines.push("- Persist buyers/planners via Demand Generator / Program / Signal / Hotel Fit + Group Demand Opportunity writer.");
  lines.push("- Only actual venues belong in Private Event Venues.");
  lines.push("- Weekly refresh: check new programs/dates/contacts/buying signals; do not re-label standing accounts as new.");
  lines.push("- Publish qualified no-guestroom LMC under existing three-hotel authorization.");
  lines.push("");

  return lines.join("\n");
}

fs.mkdirSync(OUT_DIR, { recursive: true });
const results = HOTELS.map(hotelPlan);
const payload = {
  version: LOCAL_MEETINGS_CATERING_RESEARCH_VERSION,
  generatedAt: new Date().toISOString(),
  hotels: results,
  budgetNote:
    "Include LMC expansion in existing three-hotel consolidated paid-research proposal — no silent budget increase.",
};

fs.writeFileSync(
  path.join(OUT_DIR, "three-hotel-lmc-subsegment-plan.json"),
  JSON.stringify(payload, null, 2)
);
fs.writeFileSync(path.join(OUT_DIR, "FOUNDER_REPORT.md"), mdReport(results));

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT_DIR,
      hotels: results.map((h) => ({
        hotelId: h.hotelId,
        displayName: h.displayName,
        applicable: h.lmcApplicable,
        subsegments: (h.applicableSubsegmentIds || []).length,
        existingLmc: h.existingInventory?.lmcTotal ?? 0,
      })),
    },
    null,
    2
  )
);
