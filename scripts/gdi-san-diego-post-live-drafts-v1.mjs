/**
 * Post identity-lock live: quarantine check on program drafts, PE hotel-specific
 * assessment (no Bethesda fixtures), draft cards from accepted Parallel candidates.
 * No San Diego publication.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildPrivateEventIntelligence } from "../lib/group-demand-intelligence/workflow/private-event-intelligence-v1.js";
import { buildDraftReviewPackage } from "../lib/group-demand-intelligence/workflow/draft-review-package-v1.js";
import {
  quarantineWrongMarketCandidates,
  assessGeographicRelevance,
} from "../lib/group-demand-intelligence/workflow/geographic-relevance-gate-v1.js";
import { assessIndependentHotelFit } from "../lib/group-demand-intelligence/workflow/independent-hotel-fit-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const OUT = path.join(
  ROOT,
  "reports/gdi-operational-baseline/san-diego-post-live-drafts"
);

delete process.env.GDI_DATA_ROOT;

const hotelConfig = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, `config/group-demand-intelligence/hotels/${HOTEL_ID}.json`),
    "utf8"
  )
);

const live = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/gdi-operational-baseline/san-diego-parallel-identity-lock-live/identity-lock-live-result.json"
    ),
    "utf8"
  )
);

const programDraftsPath = path.join(
  ROOT,
  "reports/gdi-operational-baseline/san-diego-parallel-live-v2/runs/gdi_workflow_recnPB3Q_2026-10-14T22-05-42-720Z_234335/reuse_prior_intelligence/programs-occasion-drafts.json"
);
const programDoc = JSON.parse(fs.readFileSync(programDraftsPath, "utf8"));

fs.mkdirSync(OUT, { recursive: true });

// --- Program draft contamination check ---
const BETHESDA_THEME =
  /\bbethesda\b|\bnih\b|\bmaryland\b|\bdmv\b|\bwalter\s*reed\b|\bmedxellence\b|\bclinical education\b|\bvisiting programs\b|\bmilitary medical\b/i;

const programAudit = (programDoc.drafts || []).map((d) => {
  const blob = [
    d.title,
    d.organizationName,
    d.hotelOpportunityThesis,
    d.summaryWhat,
    d.location,
  ].join(" | ");
  const geo = assessGeographicRelevance(
    {
      title: d.title,
      organizationName: d.organizationName,
      hotelOpportunityThesis: d.hotelOpportunityThesis,
      location: d.location,
      summaryWhat: d.summaryWhat,
    },
    hotelConfig,
    { hotelId: HOTEL_ID }
  );
  const thematicContamination = BETHESDA_THEME.test(blob);
  return {
    id: d.id,
    title: d.title,
    organizationName: d.organizationName,
    location: d.location,
    geoDisposition: geo.disposition,
    thematicContamination,
    disposition: thematicContamination
      ? "QUARANTINED_THEMATIC_CONTAMINATION"
      : geo.quarantine
        ? "QUARANTINED_GEO"
        : "KEEP_AS_SAN_DIEGO_DRAFT_QUEUE",
    note: thematicContamination
      ? "Program label/theme looks Bethesda/NIH-corridor reused — not counted as San Diego prospect"
      : null,
  };
});

const programKept = programAudit.filter(
  (p) => p.disposition === "KEEP_AS_SAN_DIEGO_DRAFT_QUEUE"
);
const programQuarantined = programAudit.filter(
  (p) => p.disposition !== "KEEP_AS_SAN_DIEGO_DRAFT_QUEUE"
);

// --- Parallel live → draft cards ---
const liveCandidates = Array.isArray(live.candidates) ? live.candidates : [];
const geoLive = quarantineWrongMarketCandidates(liveCandidates, hotelConfig, {
  hotelId: HOTEL_ID,
});

const enriched = geoLive.accepted.map((c) => {
  const fit = assessIndependentHotelFit(c, HOTEL_ID);
  const contactRoute =
    c.primaryContact?.email ||
    c.primaryContact?.phone ||
    c.officialSource ||
    (Array.isArray(c.evidenceLinks) && c.evidenceLinks[0]) ||
    null;
  const gaps = {
    missingResearchEvidence: !(
      c.officialSource ||
      (Array.isArray(c.evidenceLinks) && c.evidenceLinks.length)
    ),
    missingContactRoute: !c.primaryContact?.email && !c.primaryContact?.phone,
    missingNarrativeSynthesis: !c.hotelOpportunityThesis,
    internalReviewerDecision: true,
    hotelSalesValidationAfterPublication: true,
  };
  const completeEnough =
    !gaps.missingResearchEvidence &&
    !gaps.missingNarrativeSynthesis &&
    Boolean(c.eventStartDate || c.title);
  return {
    ...c,
    independentHotelFit: fit,
    contactRoutePresent: Boolean(contactRoute),
    contactRouteNote: contactRoute
      ? "Official source / housing channel URL qualifies as intermediary route under shared contact policy — not booking authority"
      : "No named email/phone yet — housing bureau or org form still needed",
    gapClassification: gaps,
    draftStatus: completeEnough
      ? "COMPLETE_DRAFT_CARD_INTERNAL"
      : "INTERNAL_RESEARCH_QUEUE",
    publicationBlocked: true,
    publicationStatus: "not_published_san_diego",
  };
});

const draftPkg = buildDraftReviewPackage({
  hotelId: HOTEL_ID,
  hotelKey: "san_diego_bayside",
  hotelLabel: hotelConfig.displayName,
  opportunities: enriched,
  fitAssessments: enriched.map((c) => ({
    opportunityId: c.candidateId || c.id,
    ...c.independentHotelFit,
  })),
  outDir: path.join(OUT, "draft_review_package"),
  maxCards: 20,
});

// --- PE hotel-specific (live Airtable read; no fixture) ---
const pe = await buildPrivateEventIntelligence(HOTEL_ID, {
  hotelConfig,
  allowFixtureFallback: false,
  maxVenues: 50,
  maxFits: 50,
  maxSignals: 50,
});

const peHotelSpecific = {
  source: pe.source,
  liveAirtable: pe.liveAirtable || null,
  fixtureFallback: pe.fixtureFallback || null,
  venueCount: (pe.venues || pe.candidates || []).length,
  note:
    pe.source === "airtable"
      ? "Reading shared PE corpus is reuse until hotel-specific fit/rationale is written below"
      : pe.fixtureFallback?.used
        ? "BLOCKED — fixture used (should not happen in operational mode)"
        : "No live PE records or blocked",
  hotelAttributesUsed: {
    displayName: hotelConfig.displayName,
    city: hotelConfig.city,
    territory: hotelConfig.demandTerritory?.label,
    guestrooms: hotelConfig.capabilityProfile?.totalGuestrooms,
    meetingRooms: hotelConfig.capabilityProfile?.meetingRoomCount,
    meetingSqFt: hotelConfig.capabilityProfile?.totalMeetingSpaceSqFt,
    classification: hotelConfig.capabilityProfile?.classification,
    geo: hotelConfig.geo,
  },
  localVenueAssessments: (pe.dispositions || pe.candidates || pe.venues || [])
    .slice(0, 12)
    .map((v) => {
      const name = v.venueName || v.name || v.organizationName || "unknown venue";
      const rel = v.relationship || v.disposition || v.qualification || null;
      return {
        venue: name,
        whyMattersToThisHotel:
          v.rationale ||
          v.whyRelevantToHotel ||
          `Assess against ${hotelConfig.displayName} bayfront 600-key / 15-meeting-room capacity near Convention Center`,
        complementaryVsCompeting:
          /compet/i.test(String(rel)) || /compet/i.test(String(v.rationale || ""))
            ? "competing_or_substitute_meeting_inventory"
            : /partner|referral|complement|overflow|lodging/i.test(
                  `${rel} ${v.rationale || ""}`
                )
              ? "complementary_overnight_or_referral"
              : "unresolved_relationship",
        travelingCohortOrReferral:
          v.travelingCohort ||
          v.referralHypothesis ||
          "Unresolved — need event calendar / planner path for this venue",
        timingEvidence: v.eventStartDate || v.timingNote || v.signalDate || null,
        contactRoute: v.contact || v.primaryContact || null,
        disposition:
          v.disposition ||
          v.qualification?.status ||
          (v.internalKeep ? "background_intelligence" : "unresolved"),
        label: "venue_partnership_prospect_not_confirmed_room_block",
      };
    }),
  dispositionsSummary: pe.summary || pe.counts || null,
};

const report = {
  asOf: new Date().toISOString(),
  committedBuild: "f911ff9ae4b5d7d0c83c0e94e0400f1a74271bfd",
  hotelId: HOTEL_ID,
  providerRunId: live.providerRunId,
  programDraftAudit: {
    total: programAudit.length,
    keptAsSdQueue: programKept.length,
    quarantined: programQuarantined.length,
    rows: programAudit,
  },
  parallelDrafts: {
    acceptedInTerritory: geoLive.acceptedCount,
    quarantined: geoLive.quarantinedCount,
    completeDraftCards: enriched.filter(
      (c) => c.draftStatus === "COMPLETE_DRAFT_CARD_INTERNAL"
    ).length,
    researchQueue: enriched.filter(
      (c) => c.draftStatus === "INTERNAL_RESEARCH_QUEUE"
    ).length,
    cards: enriched.map((c) => ({
      id: c.candidateId || c.id,
      title: c.title,
      organizationName: c.organizationName,
      location: c.location,
      eventStartDate: c.eventStartDate,
      draftStatus: c.draftStatus,
      gapClassification: c.gapClassification,
      contactRouteNote: c.contactRouteNote,
      thesis: c.hotelOpportunityThesis,
      sources: c.evidenceLinks || [],
      fit: c.independentHotelFit?.outcome,
    })),
  },
  privateEvents: peHotelSpecific,
  publication: "none_san_diego",
};

fs.writeFileSync(
  path.join(OUT, "san-diego-post-live-report.json"),
  JSON.stringify(report, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(OUT, "draft-review-package.json"),
  JSON.stringify(draftPkg, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(OUT, "private-event-intelligence.json"),
  JSON.stringify(pe, null, 2) + "\n"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      programKept: programKept.length,
      programQuarantined: programQuarantined.length,
      parallelCompleteDrafts: report.parallelDrafts.completeDraftCards,
      parallelResearchQueue: report.parallelDrafts.researchQueue,
      peSource: peHotelSpecific.source,
      peVenueAssessments: peHotelSpecific.localVenueAssessments.length,
      sampleCards: report.parallelDrafts.cards.slice(0, 4),
    },
    null,
    2
  )
);
