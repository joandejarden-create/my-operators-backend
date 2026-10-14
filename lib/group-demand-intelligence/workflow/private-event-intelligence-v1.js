/**
 * Private-event intelligence reuse for the integrated GDI workflow.
 * Reads venues, hotel–venue fits, and signals before external research.
 * Does not auto-promote to customer-visible opportunities.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listVenuesFromAirtable,
} from "../private-events/airtable-venue-store.js";
import {
  isPeAirtableConfigured,
  getPeBase,
  parseJson,
} from "../private-events/airtable-client.js";
import {
  HOTEL_VENUE_FIT_TABLE_NAME,
  MAP_HOTEL_VENUE_FIT,
  MAP_PE_SIGNAL,
  PE_SIGNALS_TABLE_NAME,
} from "../private-events/airtable-field-map.js";
import {
  buildHotelContext,
  buildHotelVenueRelationship,
} from "../private-events/hotel-context.js";
import {
  qualifySpecificEvent,
  qualifyVenuePartnership,
} from "../private-events/qualify.js";
import { buildGdiOpportunityFromPrivateEvent } from "../private-events/promote-to-gdi.js";
import {
  evaluateDrawerContentCompleteness,
} from "./drawer-content-contract-v1.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { normalizeOrgIdentityKey } from "./reuse-prior-intelligence-v1.js";

export const PRIVATE_EVENT_INTELLIGENCE_VERSION = "gdi-private-event-intelligence-v1";

export const PE_DISPOSITION = Object.freeze({
  VENUE_REFERRAL_PARTNERSHIP: "venue_referral_partnership_account",
  SPECIFIC_EVENT_LODGING: "specific_event_related_lodging_prospect",
  MEETING_ANCILLARY: "potential_meeting_or_ancillary_hospitality",
  BACKGROUND_ONLY: "background_market_intelligence_only",
  REJECTED_INSUFFICIENT: "rejected_insufficient_travel_or_procurement",
  INTERNAL_ONLY: "internal_only_not_customer_facing",
  ALREADY_LINKED_OPPORTUNITY: "already_linked_gdi_opportunity",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadHotelConfig(hotelId) {
  const p = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${String(hotelId || "").trim()}.json`
  );
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Frozen Bethesda PE fixtures used when Airtable is unavailable (local replay).
 * Not fabricated prospects — sourced from prior PE audits / snapshots.
 */
export function loadFrozenPeFixtureCorpus(hotelId) {
  const id = text(hotelId);
  if (id !== "recLuxvwwxID7U2B8") {
    return { ok: false, reason: "no_frozen_fixture_for_hotel", venues: [], fits: [], signals: [] };
  }
  const auditPath = path.join(
    ROOT,
    "reports/group-demand-intelligence/private-events-test-cleanup/AUDIT.json"
  );
  const wcPath = path.join(
    ROOT,
    "reports/group-demand-intelligence/private-events-v1-6/WOMANS_CLUB_DETAIL_SNAPSHOT.json"
  );
  const auxPath = path.join(
    ROOT,
    "reports/gdi-existing-integrations-audit/_aux-airtable-sample.json"
  );
  if (!fs.existsSync(auditPath)) {
    return { ok: false, reason: "fixture_missing", venues: [], fits: [], signals: [] };
  }
  const audit = JSON.parse(fs.readFileSync(auditPath, "utf8"));
  const wc = fs.existsSync(wcPath)
    ? JSON.parse(fs.readFileSync(wcPath, "utf8"))
    : null;
  const aux = fs.existsSync(auxPath)
    ? JSON.parse(fs.readFileSync(auxPath, "utf8"))
    : null;

  const venues = (audit.venueAudit?.rows || []).map((v) => ({
    venueId: v.venueId,
    venueName: v.venueName,
    website: v.website,
    primarySourceUrl: v.website,
    sourceUrls: v.website ? [v.website] : [],
    onSiteLodgingStatus: "NO_LODGING",
    privateEventsAdvertised: true,
    weddingsAdvertised: /wedding|mansion|club/i.test(v.venueName || ""),
    venueType: /club/i.test(v.venueName || "") ? "PRIVATE_CLUB" : "HISTORIC_ESTATE",
    city: "Bethesda",
    region: "MD",
    classification: v.classification,
  }));

  // Enrich Woman's Club from snapshot
  const wcVenue = venues.find((v) => v.venueId === "pev_d7991905b5642dc4");
  if (wcVenue && wc.after) {
    /* snapshot is wc.after */
  }
  if (wcVenue && wc?.after) {
    wcVenue.address = wc.after.venueAddress;
    wcVenue.lat = null;
    wcVenue.longitude = null;
    wcVenue.fixtureDistanceMiles = wc.after.distanceMiles;
    wcVenue.fixtureDriveMinutes = wc.after.driveTimeMinutes;
    wcVenue.fixtureThesis = wc.after.thesis;
    wcVenue.fixtureAction = wc.after.action;
    wcVenue.fixtureSummary = wc.after.summaryWhat;
    wcVenue.eventContactName = wc.after.contactName;
    wcVenue.primarySourceUrl = wc.after.sources?.[0]?.url || wcVenue.website;
    wcVenue.sourceUrls = (wc.after.sources || []).map((s) => s.url).filter(Boolean);
  }

  const fits = [
    {
      fitId: "hvf_2e7744ffe743dd",
      hotelId: id,
      venueId: "pev_d7991905b5642dc4",
      venueName: "Woman's Club of Bethesda",
      distanceMiles: wc?.after?.distanceMiles ?? 1.4,
      driveTimeMinutes: wc?.after?.driveTimeMinutes ?? 4,
      lodgingCatchmentFit: "CORE",
      productFit: "STRONG",
      partnershipPotential: "HIGH",
      lodgingCapturePotential: "HIGH",
      fitRationale: wc?.after?.thesis || null,
      whyHotelCouldWin: wc?.after?.thesis || null,
      currentGdiOpportunityId: "gdi_pe_781f12393f8117e7",
      evidenceUrls: (wc?.after?.sources || []).map((s) => s.url).filter(Boolean),
      source: "frozen_audit_fixture",
    },
  ];

  const signals = (aux?.privateEventSignals?.sample || [])
    .filter((s) => s.signalId && s.title)
    .map((s) => ({
      signalId: s.signalId,
      airtableRecordId: s.id,
      eventName: s.title,
      venueName: /glenview/i.test(s.title || "")
        ? "Glenview Mansion"
        : /wedding/i.test(s.title || "")
          ? "Glenview Mansion"
          : null,
      venueId: /glenview|wedding|mary vickerman/i.test(s.title || "")
        ? "pev_5d6cb3406b270ba5"
        : null,
      eventStartDate: null,
      sourceUrl: null,
      roomDemandStatus: "UNKNOWN",
      lodgingMentioned: false,
      roomBlockMentioned: false,
      hotelMentioned: false,
      demandSignalType: "PRIVATE_EVENT",
      source: "aux_airtable_sample_fixture",
      keys: s.keys || [],
    }));

  return {
    ok: true,
    reason: "frozen_fixture",
    venues,
    fits,
    signals,
    linkedOpportunity: audit.opportunityAudit?.rows?.[0] || null,
  };
}

async function listHotelVenueFitsFromAirtable(hotelId, { maxRecords = 100 } = {}) {
  if (!isPeAirtableConfigured()) return [];
  const table = getPeBase()(HOTEL_VENUE_FIT_TABLE_NAME);
  const id = text(hotelId).replace(/"/g, '\\"');
  const rows = [];
  await table
    .select({
      filterByFormula: `{${MAP_HOTEL_VENUE_FIT.hotelId}} = "${id}"`,
      maxRecords,
    })
    .eachPage((page, next) => {
      for (const r of page) {
        const f = r.fields || {};
        rows.push({
          airtableRecordId: r.id,
          fitId: f[MAP_HOTEL_VENUE_FIT.fitId] || null,
          hotelId: f[MAP_HOTEL_VENUE_FIT.hotelId] || hotelId,
          venueId: f[MAP_HOTEL_VENUE_FIT.venueId] || null,
          venueName: f[MAP_HOTEL_VENUE_FIT.venueName] || null,
          distanceMiles: f[MAP_HOTEL_VENUE_FIT.distanceMiles] ?? null,
          driveTimeMinutes: f[MAP_HOTEL_VENUE_FIT.driveTimeMinutes] ?? null,
          lodgingCatchmentFit: f[MAP_HOTEL_VENUE_FIT.lodgingCatchmentFit] || null,
          productFit: f[MAP_HOTEL_VENUE_FIT.productFit] || null,
          partnershipPotential: f[MAP_HOTEL_VENUE_FIT.partnershipPotential] || null,
          lodgingCapturePotential: f[MAP_HOTEL_VENUE_FIT.lodgingCapturePotential] || null,
          fitRationale: f[MAP_HOTEL_VENUE_FIT.fitRationale] || null,
          whyHotelCouldWin: f[MAP_HOTEL_VENUE_FIT.whyHotelCouldWin] || null,
          currentGdiOpportunityId: f[MAP_HOTEL_VENUE_FIT.currentGdiOpportunityId] || null,
          evidenceUrls: f[MAP_HOTEL_VENUE_FIT.evidenceUrls] || null,
          fitPayload: parseJson(f[MAP_HOTEL_VENUE_FIT.fitPayloadJson], null),
        });
      }
      next();
    });
  return rows;
}

async function listPrivateEventSignalsFromAirtable({ maxRecords = 100 } = {}) {
  if (!isPeAirtableConfigured()) return [];
  const table = getPeBase()(PE_SIGNALS_TABLE_NAME);
  const rows = [];
  await table
    .select({ maxRecords, pageSize: 50 })
    .eachPage((page, next) => {
      for (const r of page) {
        const f = r.fields || {};
        rows.push({
          airtableRecordId: r.id,
          signalId: f[MAP_PE_SIGNAL.signalId] || null,
          venueId: f[MAP_PE_SIGNAL.venueId] || null,
          venueName: f[MAP_PE_SIGNAL.venueName] || null,
          eventName: f[MAP_PE_SIGNAL.eventName] || null,
          eventType: f[MAP_PE_SIGNAL.eventType] || null,
          eventStartDate: f[MAP_PE_SIGNAL.eventStartDate] || null,
          eventEndDate: f[MAP_PE_SIGNAL.eventEndDate] || null,
          estimatedAttendance: f[MAP_PE_SIGNAL.estimatedAttendance] ?? null,
          attendanceStatus: f[MAP_PE_SIGNAL.attendanceStatus] || null,
          potentialRoomsLow: f[MAP_PE_SIGNAL.potentialRoomsLow] ?? null,
          potentialRoomsHigh: f[MAP_PE_SIGNAL.potentialRoomsHigh] ?? null,
          roomDemandStatus: f[MAP_PE_SIGNAL.roomDemandStatus] || null,
          lodgingMentioned: Boolean(f[MAP_PE_SIGNAL.lodgingMentioned]),
          roomBlockMentioned: Boolean(f[MAP_PE_SIGNAL.roomBlockMentioned]),
          hotelMentioned: Boolean(f[MAP_PE_SIGNAL.hotelMentioned]),
          sourceUrl: f[MAP_PE_SIGNAL.sourceUrl] || null,
          demandSignalType: f[MAP_PE_SIGNAL.demandSignalType] || null,
          hotelDemandThesis: f[MAP_PE_SIGNAL.hotelDemandThesis] || null,
          signalPayload: parseJson(f[MAP_PE_SIGNAL.signalPayloadJson], null),
        });
      }
      next();
    });
  return rows;
}

/**
 * Classify a venue+fit (+optional signal) into PE disposition.
 * Nearby event ≠ automatic traveling guests / open procurement.
 */
export function classifyPrivateEventDisposition({
  hotelId,
  venue,
  fit,
  signal = null,
  hotelCtx = null,
} = {}) {
  const cfg = loadHotelConfig(hotelId);
  const ctx =
    hotelCtx ||
    buildHotelContext({
      hotelId,
      displayName: cfg?.displayName,
      roomCount: cfg?.capabilityProfile?.totalGuestrooms,
      meetingSpace: cfg?.capabilityProfile?.meetingRoomCount,
      urbanSuburbanResort: /urban|downtown/i.test(cfg?.demandTerritory?.label || "")
        ? "urban"
        : "suburban",
      practicalCatchment: { coreMiles: 5, stretchMiles: 15 },
      lat: cfg?.geo?.lat,
      long: cfg?.geo?.lng,
    });

  const distance =
    fit?.distanceMiles ??
    venue?.fixtureDistanceMiles ??
    null;
  const relationship =
    fit && fit.lodgingCatchmentFit
      ? {
          lodgingCatchmentFit: fit.lodgingCatchmentFit,
          productFit: fit.productFit,
          potentialPartnershipFit:
            fit.partnershipPotential === "HIGH"
              ? "HIGH"
              : fit.partnershipPotential === "MEDIUM"
                ? "MODERATE"
                : "LOW",
          distanceMiles: distance,
        }
      : buildHotelVenueRelationship(ctx, {
          ...venue,
          lat: venue.lat ?? venue.latitude,
          long: venue.longitude ?? venue.lng,
        }, { distanceMiles: distance });

  const caveats = [
    "An event at a nearby venue does not automatically imply traveling guests, available room demand, or an open hotel procurement opportunity.",
    "Hotel total rooms are capacity, not evidence of a group block.",
  ];

  if (fit?.currentGdiOpportunityId) {
    return {
      disposition: PE_DISPOSITION.ALREADY_LINKED_OPPORTUNITY,
      customerFacingCandidate: false,
      promote: false,
      internalKeep: true,
      rationale: `Already linked to GDI opportunity ${fit.currentGdiOpportunityId}`,
      relationship,
      caveats,
      qualification: null,
    };
  }

  if (signal) {
    const q = qualifySpecificEvent({
      signal: {
        ...signal,
        eventDate: signal.eventStartDate || signal.eventDate,
        sourceUrl: signal.sourceUrl || venue?.primarySourceUrl,
      },
      venue,
      hotelCtx: ctx,
      relationship,
    });
    if (
      q.qualification === "FALSE" ||
      (q.rejectReasons || q.reject || []).length
    ) {
      return {
        disposition: PE_DISPOSITION.REJECTED_INSUFFICIENT,
        customerFacingCandidate: false,
        promote: false,
        internalKeep: true,
        rationale:
          (q.rejectReasons || q.reject || []).join(";") ||
          "Signal lacks future dated evidence, travel cohort, or open procurement proof",
        relationship,
        caveats,
        qualification: q,
      };
    }
    if (
      !signal.lodgingMentioned &&
      !signal.roomBlockMentioned &&
      !signal.hotelMentioned &&
      signal.roomDemandStatus === "UNKNOWN"
    ) {
      return {
        disposition: PE_DISPOSITION.BACKGROUND_ONLY,
        customerFacingCandidate: false,
        promote: false,
        internalKeep: true,
        rationale:
          "Event signal without lodging/travel/procurement evidence — background market intelligence only",
        relationship,
        caveats,
        qualification: q,
      };
    }
    return {
      disposition: PE_DISPOSITION.SPECIFIC_EVENT_LODGING,
      customerFacingCandidate: q.actionable === true,
      promote: false,
      internalKeep: true,
      rationale: q.whyOpportunityExists || q.whyHotelCouldWin || "Specific event lodging candidate pending human review",
      relationship,
      caveats,
      qualification: q,
    };
  }

  // Partnership / venue account path
  const pq = qualifyVenuePartnership({
    venue: {
      ...venue,
      eventActivityEvidenceStatus: venue.privateEventsAdvertised
        ? "STRONG_REPEATED_ACTIVITY"
        : "UNKNOWN",
    },
    hotelCtx: ctx,
    relationship,
  });

  if (
    relationship.lodgingCatchmentFit === "OUTSIDE" ||
    (pq.qualification === "FALSE" && (pq.rejectReasons || []).length && !venue?.fixtureThesis)
  ) {
    return {
      disposition: PE_DISPOSITION.REJECTED_INSUFFICIENT,
      customerFacingCandidate: false,
      promote: false,
      internalKeep: true,
      rationale:
        relationship.lodgingCatchmentFit === "OUTSIDE"
          ? "Venue outside practical lodging catchment for this hotel"
          : (pq.rejectReasons || []).join(";") || "Partnership gate failed",
      relationship,
      caveats,
      qualification: pq,
    };
  }

  if (
    relationship.potentialPartnershipFit === "HIGH" ||
    pq.actionable === true ||
    pq.qualification === "WATCH" ||
    venue?.fixtureThesis
  ) {
    return {
      disposition: PE_DISPOSITION.VENUE_REFERRAL_PARTNERSHIP,
      customerFacingCandidate: false, // still requires human/eligibility — not auto
      promote: false,
      internalKeep: true,
      rationale:
        venue?.fixtureThesis ||
        pq.partnershipThesis ||
        pq.whyOpportunityExists ||
        "Venue referral/partnership account — lodging capture plausible but not auto-published",
      relationship,
      caveats,
      qualification: pq,
    };
  }

  if (relationship.lodgingCatchmentFit === "CORE" || relationship.lodgingCatchmentFit === "COMPETITIVE") {
    return {
      disposition: PE_DISPOSITION.MEETING_ANCILLARY,
      customerFacingCandidate: false,
      promote: false,
      internalKeep: true,
      rationale:
        "Nearby venue with complementary facilities — potential meeting/ancillary hospitality, travel cohort unproven",
      relationship,
      caveats,
      qualification: pq,
    };
  }

  return {
    disposition: PE_DISPOSITION.BACKGROUND_ONLY,
    customerFacingCandidate: false,
    promote: false,
    internalKeep: true,
    rationale: "Venue retained as background market intelligence only",
    relationship,
    caveats,
    qualification: pq,
  };
}

/**
 * Build PE intelligence pack for a hotel (read-only).
 */
export async function buildPrivateEventIntelligence(hotelId, opts = {}) {
  const id = text(hotelId);
  const cfg = opts.hotelConfig || loadHotelConfig(id);
  let source = "none";
  let venues = [];
  let fits = [];
  let signals = [];
  let linkedOpportunity = null;
  let airtableOk = false;
  let airtableReason = null;

  let liveAirtable = {
    attempted: false,
    ok: false,
    reason: null,
    venues: 0,
    fits: 0,
    signals: 0,
  };
  let fixtureFallback = {
    used: false,
    reason: null,
    corpusHotelId: null,
  };

  if (opts.skipAirtable === true) {
    airtableReason = "skip_airtable_explicit";
    liveAirtable.reason = airtableReason;
  } else if (opts.skipAirtable !== true && isPeAirtableConfigured()) {
    liveAirtable.attempted = true;
    try {
      venues = await listVenuesFromAirtable({ maxRecords: opts.maxVenues || 200 });
      fits = await listHotelVenueFitsFromAirtable(id, {
        maxRecords: opts.maxFits || 100,
      });
      signals = await listPrivateEventSignalsFromAirtable({
        maxRecords: opts.maxSignals || 100,
      });
      airtableOk = true;
      source = "airtable";
      liveAirtable = {
        attempted: true,
        ok: true,
        reason:
          venues.length || fits.length || signals.length
            ? "live_read"
            : "empty_success_zero_records",
        venues: venues.length,
        fits: fits.length,
        signals: signals.length,
      };
    } catch (err) {
      airtableReason = err?.message || String(err);
      airtableOk = false;
      liveAirtable = {
        attempted: true,
        ok: false,
        reason: airtableReason,
        venues: 0,
        fits: 0,
        signals: 0,
      };
    }
  } else if (!isPeAirtableConfigured()) {
    airtableReason = "airtable_not_configured";
    liveAirtable.reason = airtableReason;
  }

  // Fixtures ONLY under explicit test/replay mode — never automatic operational fallback.
  // Unavailable live data is blocked/empty, not replaced by another hotel's evidence.
  const explicitFixtureMode =
    opts.allowFixtureFallback === true ||
    opts.fixtureMode === "replay" ||
    opts.fixtureMode === "test" ||
    String(process.env.GDI_PE_FIXTURE_MODE || "").toLowerCase() === "replay" ||
    String(process.env.GDI_PE_FIXTURE_MODE || "").toLowerCase() === "test";
  if (explicitFixtureMode) {
    const frozen = loadFrozenPeFixtureCorpus(id);
    if (frozen.ok) {
      venues = frozen.venues;
      fits = frozen.fits;
      signals = frozen.signals;
      linkedOpportunity = frozen.linkedOpportunity;
      source = "frozen_fixture";
      fixtureFallback = {
        used: true,
        reason: "explicit_test_or_replay_mode",
        corpusHotelId: id,
      };
    }
  } else if (!airtableOk) {
    source = "none";
    fixtureFallback = {
      used: false,
      reason: "blocked_no_automatic_fixture_fallback",
      corpusHotelId: null,
      airtableReason: airtableReason || liveAirtable.reason,
    };
  }

  const hotelCtx = buildHotelContext({
    hotelId: id,
    displayName: cfg?.displayName,
    roomCount: cfg?.capabilityProfile?.totalGuestrooms,
    meetingSpace: cfg?.capabilityProfile?.meetingRoomCount,
    urbanSuburbanResort: /downtown|urban|times.?square/i.test(
      `${cfg?.demandTerritory?.label || ""} ${cfg?.city || ""}`
    )
      ? "urban"
      : "suburban",
    practicalCatchment: { coreMiles: 5, stretchMiles: 15 },
  });

  const venueById = new Map(venues.map((v) => [v.venueId, v]));
  const rows = [];

  for (const fit of fits) {
    const venue = venueById.get(fit.venueId) || {
      venueId: fit.venueId,
      venueName: fit.venueName,
      onSiteLodgingStatus: "NO_LODGING",
      privateEventsAdvertised: true,
    };
    const classified = classifyPrivateEventDisposition({
      hotelId: id,
      venue,
      fit,
      hotelCtx,
    });
    const draftOpp = buildGdiOpportunityFromPrivateEvent({
      hotel: { hotelId: id, displayName: cfg?.displayName },
      venue,
      fit,
      signal: null,
      qualification: {
        ...(classified.qualification || {}),
        whyHotelCouldWin:
          classified.rationale || fit.whyHotelCouldWin || venue.fixtureThesis,
        whyOpportunityExists: venue.fixtureSummary || classified.rationale,
        recommendedAction: venue.fixtureAction || classified.qualification?.recommendedAction,
        actionable: false,
        isTestData: source.includes("fixture"),
      },
    });
    // Force non-customer until human package — preserve IDs
    draftOpp.customerVisible = false;
    draftOpp.publicationBlocked = true;
    draftOpp.peDisposition = classified.disposition;
    draftOpp.hotelOpportunityThesis =
      venue.fixtureThesis || fit.whyHotelCouldWin || draftOpp.hotelWinThesis;
    draftOpp.recommendedAction = venue.fixtureAction || draftOpp.recommendedAction;
    draftOpp.summaryWhat = venue.fixtureSummary || draftOpp.whyNow;
    draftOpp.primaryContact = {
      name: venue.eventContactName || null,
      email: venue.eventContactEmail || null,
      phone: venue.eventContactPhone || null,
    };
    draftOpp.officialSource = venue.primarySourceUrl || venue.website || null;
    draftOpp.evidenceLinks = venue.sourceUrls || fit.evidenceUrls || [];
    draftOpp.sourcingStatus =
      "Venue partnership / referral path — lodging capture plausible; open procurement not confirmed";
    draftOpp.materialLimitations = classified.caveats;
    draftOpp.eventStartDate = null;
    draftOpp.opportunityQualification = "WATCH";
    draftOpp.venueSourcingStatus = "UNCONFIRMED";
    draftOpp.whyNow = draftOpp.summaryWhat || draftOpp.whyNow;
    draftOpp.hotelFitScore = 60;
    draftOpp.customerEnrichmentVersion = "gdi_customer_enrichment_v1";
    draftOpp.discoverySource = `gdi_pe_reuse_${source}`;

    const elig = evaluateProspectPublicationEligibility(draftOpp);
    const drawer = evaluateDrawerContentCompleteness(draftOpp);

    rows.push({
      sourceType: "hotel_venue_fit",
      sourceRecordId: fit.fitId || fit.airtableRecordId,
      peVenueId: venue.venueId,
      peSignalId: null,
      hotelVenueFitId: fit.fitId,
      organizationName: venue.venueName,
      organizationIdentityKey: normalizeOrgIdentityKey(venue.venueName),
      disposition: classified.disposition,
      rationale: classified.rationale,
      relationship: classified.relationship,
      linkedOpportunityId: fit.currentGdiOpportunityId || linkedOpportunity?.opportunityId || null,
      draftOpportunityId: draftOpp.id,
      customerFacingCandidate: false,
      promote: false,
      internalKeep: classified.internalKeep,
      eligibility: elig.eligibility,
      drawerOk: drawer.ok,
      drawerFailed: drawer.failed,
      caveats: classified.caveats,
      preserveIds: {
        peVenueId: venue.venueId,
        hotelVenueFitId: fit.fitId,
        peSignalId: null,
        gdiOpportunityId: fit.currentGdiOpportunityId || draftOpp.id,
      },
    });
  }

  // Signals not already covered as venue partnership
  const fitVenueIds = new Set(fits.map((f) => f.venueId));
  for (const signal of signals) {
    const venue =
      venueById.get(signal.venueId) ||
      venues.find((v) => v.venueName && signal.venueName && normEq(v.venueName, signal.venueName)) ||
      {
        venueId: signal.venueId || `pev_signal_${signal.signalId}`,
        venueName: signal.venueName || "Unknown venue",
        onSiteLodgingStatus: "UNKNOWN",
      };
    // Prefer evaluating signals for venues in hotel fit set OR Glenview-style Bethesda fixtures
    const inFit = fitVenueIds.has(venue.venueId) || Boolean(signal.venueId);
    if (!inFit && source.includes("fixture") && !signal.venueId) continue;

    const fit =
      fits.find((f) => f.venueId === venue.venueId) ||
      (venue.venueId
        ? {
            fitId: null,
            hotelId: id,
            venueId: venue.venueId,
            lodgingCatchmentFit: "UNKNOWN",
          }
        : null);

    const classified = classifyPrivateEventDisposition({
      hotelId: id,
      venue,
      fit,
      signal,
      hotelCtx,
    });

    rows.push({
      sourceType: "private_event_signal",
      sourceRecordId: signal.signalId || signal.airtableRecordId,
      peVenueId: venue.venueId,
      peSignalId: signal.signalId,
      hotelVenueFitId: fit?.fitId || null,
      organizationName: venue.venueName || signal.plannerCompany,
      organizationIdentityKey: normalizeOrgIdentityKey(
        venue.venueName || signal.eventName
      ),
      occasionIdentityKey: `${normalizeOrgIdentityKey(signal.eventName)}|${String(signal.eventStartDate || "").slice(0, 10) || "undated"}`,
      eventName: signal.eventName,
      disposition: classified.disposition,
      rationale: classified.rationale,
      relationship: classified.relationship,
      customerFacingCandidate: false,
      promote: false,
      internalKeep: true,
      caveats: classified.caveats,
      preserveIds: {
        peVenueId: venue.venueId,
        hotelVenueFitId: fit?.fitId || null,
        peSignalId: signal.signalId,
      },
    });
  }

  // Venues with no fit edge — still record as background if PE path relevant
  for (const venue of venues) {
    if (fits.some((f) => f.venueId === venue.venueId)) continue;
    if (rows.some((r) => r.peVenueId === venue.venueId)) continue;
    const classified = classifyPrivateEventDisposition({
      hotelId: id,
      venue,
      fit: null,
      hotelCtx,
    });
    rows.push({
      sourceType: "private_event_venue",
      sourceRecordId: venue.venueId,
      peVenueId: venue.venueId,
      peSignalId: null,
      hotelVenueFitId: null,
      organizationName: venue.venueName,
      organizationIdentityKey: normalizeOrgIdentityKey(venue.venueName),
      disposition: classified.disposition,
      rationale: classified.rationale,
      relationship: classified.relationship,
      customerFacingCandidate: false,
      promote: false,
      internalKeep: true,
      caveats: classified.caveats,
      preserveIds: { peVenueId: venue.venueId },
    });
  }

  const counts = {};
  for (const d of Object.values(PE_DISPOSITION)) counts[d] = 0;
  for (const r of rows) counts[r.disposition] = (counts[r.disposition] || 0) + 1;

  return {
    version: PRIVATE_EVENT_INTELLIGENCE_VERSION,
    hotelId: id,
    asOf: new Date().toISOString(),
    source,
    airtableOk,
    airtableReason,
    liveAirtable,
    fixtureFallback,
    venues: venues.map((v) => ({
      venueId: v.venueId,
      venueName: v.venueName,
      lat: v.lat ?? v.latitude ?? null,
      lng: v.lng ?? v.longitude ?? null,
      onSiteLodgingStatus: v.onSiteLodgingStatus || null,
      city: v.city || null,
      market: v.market || null,
    })),
    fits: fits.map((f) => ({
      fitId: f.fitId || f.hotelVenueFitId,
      venueId: f.venueId,
      venueName: f.venueName,
    })),
    counts: {
      venues: venues.length,
      fits: fits.length,
      signals: signals.length,
      rows: rows.length,
      byDisposition: counts,
    },
    rows,
    notes: [
      "PE graph is read before external research.",
      "Venue nearby ≠ traveling guests or open hotel procurement.",
      "IDs (peVenueId / hotelVenueFitId / peSignalId) preserved; no auto customer publish.",
      "Internal-only and rejected dispositions are first-class outcomes.",
      "Fixture fallback is never reported as live Airtable success.",
    ],
  };
}

/**
 * Map supported PE dispositions into draft opportunity shapes for review packages.
 * Does not publish; human review checkpoint remains required.
 */
export function peSupportedCandidatesToDraftOpportunities(peDoc, hotelId) {
  const id = text(hotelId || peDoc?.hotelId);
  const supported = new Set([
    PE_DISPOSITION.SPECIFIC_EVENT_LODGING,
    PE_DISPOSITION.VENUE_REFERRAL_PARTNERSHIP,
  ]);
  const out = [];
  for (const r of peDoc?.rows || []) {
    if (!supported.has(r.disposition)) continue;
    const org = r.organizationName || r.venueName || "Private event venue";
    const cardSeed = `${r.peVenueId || ""}|${r.peSignalId || ""}|${r.disposition}`;
    out.push({
      id:
        r.preserveIds?.currentGdiOpportunityId ||
        `gdi_pe_draft_${String(cardSeed)
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "_")
          .slice(0, 48)}`,
      hotelId: id,
      organizationName: org,
      company: org,
      title: r.eventName || r.title || org,
      opportunityName: r.eventName || r.title || org,
      eventStartDate: r.eventStartDate || null,
      eventEndDate: r.eventEndDate || null,
      hotelOpportunityThesis: r.rationale || null,
      fitExplanation: r.rationale || null,
      summaryWhyHotel: r.rationale || null,
      sources: (r.sourceUrls || r.sources || [])
        .map((s) => (typeof s === "string" ? { url: s } : s))
        .filter((s) => s?.url),
      officialSource: r.sourceUrl || null,
      peVenueId: r.peVenueId || null,
      peSignalId: r.peSignalId || null,
      hotelVenueFitId: r.hotelVenueFitId || null,
      peDisposition: r.disposition,
      prospectPublicationBasis: "private_event_draft_pending_human_review",
      prospectReviewSection: "EXPLORATORY",
      prospectHotelFacing: false,
      prospectEnrichmentQueue: true,
      customerVisible: false,
      draftSource: "private_event_intelligence",
      draftNote:
        "Supported PE candidate carried into draft package — human review required; not auto-published",
    });
  }
  return out;
}

function normEq(a, b) {
  return normalizeOrgIdentityKey(a) === normalizeOrgIdentityKey(b);
}

export function peIntelligenceToCsv(doc) {
  const header = [
    "hotelId",
    "sourceType",
    "sourceRecordId",
    "organizationName",
    "disposition",
    "peVenueId",
    "hotelVenueFitId",
    "peSignalId",
    "promote",
    "internalKeep",
    "rationale",
  ];
  const lines = [header.join(",")];
  const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  for (const r of doc.rows || []) {
    lines.push(
      [
        doc.hotelId,
        r.sourceType,
        r.sourceRecordId,
        esc(r.organizationName),
        r.disposition,
        r.peVenueId || "",
        r.hotelVenueFitId || "",
        r.peSignalId || "",
        r.promote,
        r.internalKeep,
        esc(r.rationale),
      ].join(",")
    );
  }
  return lines.join("\n") + "\n";
}
