/**
 * Canonical Private Events persistence → Hotel Venue Fit → GDI opportunity link.
 * Venue = shared intelligence. Fit + GDI opportunity = hotel-specific.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

import { OPPORTUNITY_TYPE } from "../claim-types.js";
import { DEMAND_FAMILY, DEMAND_SIGNAL_TYPE } from "../demand-signal-types.js";
import {
  upsertOpportunity,
  findOpportunityRecordById,
  listOpportunitiesForHotel,
} from "../airtable-opportunity-store.js";
import { invalidateGdiHotelReadCache } from "../read-cache.js";
import {
  REVIEWED_PROSPECT_PUBLICATION_BASIS,
} from "../reviewed-prospect-publication-v1.js";
import { upsertVenue, listVenuesFromAirtable } from "./airtable-venue-store.js";
import { upsertHotelVenueFit, computeFitId } from "./airtable-fit-store.js";
import { upsertPrivateEventSignal, computeSignalId } from "./airtable-signal-store.js";
import { getPeBase, isPeAirtableConfigured } from "./airtable-client.js";
import {
  HOTEL_VENUE_FIT_TABLE_NAME,
  MAP_HOTEL_VENUE_FIT,
} from "./airtable-field-map.js";
import { buildVenueEntity, computeVenueId } from "./venue-entity.js";
import { buildHotelContext } from "./hotel-context.js";
import { PE_CUSTOMER_LABELS } from "./customer-detail-enrichment.js";
import { resolvePeContactConsistency } from "./pe-contact-consistency-v1.js";
import { adaptPeCandidatesBatch } from "../workflow/pe-provider-to-canonical-adapter-v1.js";
import { assertPublicationWriteReadiness } from "../workflow/full-e2e-publication-quality-v1.js";

export { resolvePeContactConsistency };

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function nowIso() {
  return new Date().toISOString();
}

export function buildVenuePartnershipOpportunityPayload({
  hotel,
  venue,
  fit,
  signal = null,
  packageRow = {},
  existingOpportunityId = null,
} = {}) {
  const hotelId = hotel.hotelId || hotel.id;
  const contact = resolvePeContactConsistency(
    {
      contactEmail: venue.eventContactEmail,
      contactPhone: venue.eventContactPhone,
      contactUrl:
        venue.organizationPath || packageRow.contactUrl || venue.website,
      contactName: venue.eventContactName,
      contactRole: venue.eventContactRole,
    },
    venue
  );

  const title =
    packageRow.title || `${venue.venueName} — Preferred Lodging Partnership`;
  const whyMatters =
    packageRow.whyThisMatters ||
    packageRow.hotelWinThesis ||
    `No on-site lodging at ${venue.venueName}; overnight guests need nearby hotels.`;
  const whyNow =
    packageRow.whyNow ||
    "Standing private-event venue with published event activity; partnership / lodging referral (not a dated room block).";
  const recommendedAction =
    packageRow.recommendedAction ||
    "Contact the venue events path to discuss preferred / nearby lodging referral for private events.";

  const partnerStatus = venue.partnerStatus || "NO_PUBLIC_PARTNER_FOUND";
  const id =
    existingOpportunityId ||
    packageRow.opportunityId ||
    `gdi_pe_${createHash("sha256")
      .update(`partnership|${hotelId}|${venue.venueId}`)
      .digest("hex")
      .slice(0, 16)}`;

  const activityEvidence = Array.isArray(venue.activityEvidence)
    ? venue.activityEvidence
    : [];

  return {
    id,
    opportunityId: id,
    hotelId,
    title,
    opportunityName: title,
    organizationName: venue.venueName,
    opportunityType: OPPORTUNITY_TYPE.VENUE_PARTNERSHIP,
    demandFamily: DEMAND_FAMILY.PRIVATE_EVENTS,
    demandSignalType: DEMAND_SIGNAL_TYPE.VENUE_PARTNERSHIP,
    peVenueId: venue.venueId,
    peSignalId: signal?.signalId || null,
    hotelVenueFitId: fit?.fitId || computeFitId(hotelId, venue.venueId),
    detailLayout: "VENUE_PARTNERSHIP",
    standingOpportunity: true,
    eventDateDisplay: "Ongoing partnership opportunity",
    priority: packageRow.priority || "MEDIUM_PRIORITY",
    opportunityQualification: packageRow.qualification || "MODERATE",
    qualification: packageRow.qualification || "MODERATE",
    customerFacingState: packageRow.publish
      ? "ACTIONABLE_NOW"
      : "INTERNAL_ONLY",
    lifecycle: packageRow.publish ? "ACTIONABLE_NOW" : "INTERNAL_ONLY",
    qualificationGate: packageRow.publish ? "TRUE" : "HOLD",
    actionable: Boolean(packageRow.publish),
    customerVisible: packageRow.publish !== false,
    publicationState: packageRow.publish ? "ACTIONABLE_NOW" : "INTERNAL_ONLY",
    partnershipConfidence: packageRow.partnershipConfidence || "MODERATE",
    evidenceConfidence: packageRow.evidenceConfidence ?? 70,
    newnessStatus: "NEW_TO_GDI",
    weeklyDeltaState: "NEW",
    eventActivityEvidenceStatus:
      venue.eventActivityEvidenceStatus || "MODERATE_ACTIVITY",
    partnerStatus,
    partnerStatusLabel:
      PE_CUSTOMER_LABELS.partnerStatus[partnerStatus] || partnerStatus,
    partnerResearchScope:
      packageRow.partnerResearchScope || venue.partnerResearchScope || null,
    onSiteLodgingStatus: venue.onSiteLodgingStatus || "NO_LODGING",
    lodgingCatchmentFit: fit?.lodgingCatchmentFit || null,
    productFit: fit?.productFit || null,
    partnershipPotential: fit?.partnershipPotential || null,
    lodgingCapturePotential: fit?.lodgingCapturePotential || null,
    distanceMiles: fit?.distanceMiles ?? packageRow.distanceMiles ?? null,
    distanceMethod: packageRow.distanceMethod || "haversine_estimate",
    maxCapacity: venue.maxCapacity ?? null,
    venueAddress: venue.address || null,
    venueCity: venue.city || null,
    venueRegion: venue.region || null,
    venueType: venue.venueType || null,
    summaryWhat: packageRow.summaryWhat || null,
    whyNow,
    summaryWhyMatters: whyMatters,
    whyThisMatters: whyMatters,
    // Eligibility hotel_fit gate reads these fields (not only hotelOpportunityThesis)
    hotelOpportunityThesis: whyMatters,
    hotelWinThesis: whyMatters,
    fitExplanation: whyMatters,
    fitRationale: whyMatters,
    summaryWhyHotel: whyMatters,
    recommendedAction,
    recommendedNextStep: recommendedAction,
    // Reviewed-prospect allowlist hotels need this stamp for customer list visibility
    // without lowering Ready / inventing housing control.
    ...(packageRow.publish !== false
      ? {
          prospectPublicationBasis: REVIEWED_PROSPECT_PUBLICATION_BASIS,
          prospectHotelFacing: true,
          prospectReviewSection:
            packageRow.prospectReviewSection ||
            packageRow.reviewSection ||
            "PRIORITY",
          prospectCommercialLabel: "Venue partnership",
          prospectCustomerLabel: "Venue partnership",
        }
      : {
          prospectHotelFacing: false,
          customerFacingState: "INTERNAL_ONLY",
        }),
    ...contact,
    activityEvidence,
    sources: [
      ...(venue.sourceUrls || []).map((url) => ({
        url,
        type: "official_venue",
        title: "Official venue source",
        authority: "verified",
      })),
      ...activityEvidence
        .filter((e) => e?.sourceUrl)
        .slice(0, 8)
        .map((e) => ({
          url: e.sourceUrl,
          type: e.evidenceType || "activity_evidence",
          title: e.evidenceText || e.evidenceType,
          authority: e.official ? "verified" : "estimated",
        })),
    ],
    venueSourcingStatus: "OPEN_UNRESOLVED",
    roomDemandStatus: "UNKNOWN",
    segment: "Private Events",
    schemaVersion: "gdi_private_events_canonical_persist_v1",
    researchVersion: packageRow.researchVersion || "pe_canonical_persist_v1",
    firstSeenAt: nowIso(),
    updatedAt: nowIso(),
    lastVerifiedAt: nowIso(),
    doNotEstimateAnnualRevenue: true,
  };
}

function matchExistingOpportunity(hotelOpps, { venue, packageRow, proposedId }) {
  const nameRe = packageRow.matchNameRe
    ? new RegExp(packageRow.matchNameRe, "i")
    : null;
  const exact =
    hotelOpps.find((o) => o.id === proposedId || o.opportunityId === proposedId) ||
    (packageRow.existingOpportunityId
      ? hotelOpps.find(
          (o) =>
            o.id === packageRow.existingOpportunityId ||
            o.opportunityId === packageRow.existingOpportunityId
        )
      : null);
  if (exact) return { mode: "UPDATE", opp: exact };

  const byVenue = hotelOpps.find(
    (o) =>
      o.peVenueId === venue.venueId &&
      (o.opportunityType === "VENUE_PARTNERSHIP" ||
        o.opportunityType === "VENUE_REFERRAL_PARTNERSHIP")
  );
  if (byVenue) return { mode: "UPDATE_EXISTING_EQUIVALENT", opp: byVenue };

  if (nameRe) {
    const byName = hotelOpps.find((o) => {
      const blob = `${o.title || ""} ${o.organizationName || ""}`;
      return (
        (/VENUE|PRIVATE_EVENTS/i.test(
          `${o.opportunityType || ""} ${o.demandFamily || ""}`
        ) ||
          o.opportunityType === "VENUE_REFERRAL_PARTNERSHIP") &&
        nameRe.test(blob)
      );
    });
    if (byName) return { mode: "UPDATE_EXISTING_EQUIVALENT", opp: byName };
  }
  return { mode: "CREATE", opp: null };
}

export async function persistVenuePartnershipPackage(pkg, { dryRun = true } = {}) {
  const hotel = pkg.hotel;
  const hotelId = hotel.hotelId || hotel.id;
  if (!hotelId) throw new Error("hotel_id_required");

  const report = {
    hotelId,
    venueName: pkg.venue?.venueName,
    dryRun,
    steps: {},
  };

  const venueEntity = buildVenueEntity(pkg.venue);
  report.venueId = venueEntity.venueId;

  let existingCache = null;
  if (isPeAirtableConfigured()) {
    existingCache = await listVenuesFromAirtable({ maxRecords: 500 });
  }

  const venueUpsert = await upsertVenue(pkg.venue, { dryRun, existingCache });
  report.steps.venue = {
    action: venueUpsert.action,
    venueId: venueUpsert.venueId || venueEntity.venueId,
    reused: Boolean(venueUpsert.reused),
    needsReview: Boolean(venueUpsert.needsReview),
    recordId: venueUpsert.recordId || venueUpsert.airtableRecordId || null,
    ok: venueUpsert.ok !== false,
  };

  if (venueUpsert.needsReview) {
    report.verdict = "HOLD_NEEDS_REVIEW";
    report.error = "venue_match_needs_review";
    return report;
  }

  const resolvedVenue = {
    ...venueEntity,
    ...pkg.venue,
    venueId: venueUpsert.venueId || venueEntity.venueId,
    airtableRecordId:
      venueUpsert.recordId ||
      venueUpsert.airtableRecordId ||
      venueEntity.airtableRecordId ||
      null,
  };

  const hotelForFit = {
    hotelId,
    displayName: hotel.displayName || hotel.name,
    lat: hotel.lat ?? hotel.latitude,
    lng: hotel.lng ?? hotel.longitude,
    long: hotel.long ?? hotel.lng ?? hotel.longitude,
    roomCount: hotel.roomCount,
    meetingSpace: hotel.meetingSpace,
    urbanSuburbanResort: hotel.urbanSuburbanResort || hotel.setting || "urban",
    serviceLevel: hotel.serviceLevel,
    practicalCatchment: hotel.practicalCatchment || {
      coreMiles: hotel.coreMiles || 5,
      stretchMiles: hotel.stretchMiles || 15,
    },
  };

  const fitUpsert = await upsertHotelVenueFit({
    hotel: hotelForFit,
    venue: resolvedVenue,
    dryRun,
    meta: {
      venueAirtableRecordId: resolvedVenue.airtableRecordId,
      whyHotelCouldWin: pkg.whyThisMatters || pkg.hotelWinThesis,
      currentGdiOpportunityId: pkg.existingOpportunityId || null,
      confidence: pkg.evidenceConfidence ?? 70,
    },
  });
  report.steps.fit = {
    action: fitUpsert.created ? "create" : dryRun ? "dry_run" : "update",
    fitId: fitUpsert.fitId,
    lodgingCatchmentFit: fitUpsert.lodgingCatchmentFit,
    partnershipPotential: fitUpsert.partnershipPotential,
    recordId: fitUpsert.recordId || fitUpsert.airtableRecordId || null,
    ok: fitUpsert.ok !== false,
  };

  const signalResults = [];
  for (const sig of pkg.signals || []) {
    if (!sig || sig.artificial === true) continue;
    if (!sig.eventName && !sig.sourceUrl && !sig.evidenceText) continue;
    const signalPayload = {
      ...sig,
      venueId: resolvedVenue.venueId,
      venueName: resolvedVenue.venueName,
    };
    const s = await upsertPrivateEventSignal(signalPayload, {
      dryRun,
      meta: {
        venueAirtableRecordId: resolvedVenue.airtableRecordId,
        hotelVenueFitId: fitUpsert.fitId,
        promotedGdiOpportunityId: pkg.existingOpportunityId || null,
        hotelDemandThesis: pkg.whyThisMatters,
      },
    });
    signalResults.push({
      signalId: s.signalId || computeSignalId(signalPayload),
      action: s.created ? "create" : dryRun ? "dry_run" : "update",
      recordId: s.recordId || null,
      ok: s.ok !== false,
    });
  }
  report.steps.signals = signalResults;

  const hotelOpps =
    dryRun && !isPeAirtableConfigured()
      ? []
      : await listOpportunitiesForHotel(hotelId);
  const proposedId =
    pkg.existingOpportunityId ||
    `gdi_pe_${createHash("sha256")
      .update(`partnership|${hotelId}|${resolvedVenue.venueId}`)
      .digest("hex")
      .slice(0, 16)}`;
  const match = matchExistingOpportunity(hotelOpps, {
    venue: resolvedVenue,
    packageRow: pkg,
    proposedId,
  });

  const fitView = {
    fitId: fitUpsert.fitId,
    lodgingCatchmentFit: fitUpsert.lodgingCatchmentFit,
    productFit: fitUpsert.fields?.[MAP_HOTEL_VENUE_FIT.productFit] || null,
    partnershipPotential: fitUpsert.partnershipPotential,
    lodgingCapturePotential:
      fitUpsert.fields?.[MAP_HOTEL_VENUE_FIT.lodgingCapturePotential] || null,
    distanceMiles:
      fitUpsert.fields?.[MAP_HOTEL_VENUE_FIT.distanceMiles] ??
      pkg.distanceMiles ??
      null,
  };

  const opp = buildVenuePartnershipOpportunityPayload({
    hotel: hotelForFit,
    venue: resolvedVenue,
    fit: fitView,
    signal: signalResults[0] ? { signalId: signalResults[0].signalId } : null,
    packageRow: {
      ...pkg,
      opportunityId: match.opp?.id || match.opp?.opportunityId || proposedId,
    },
    existingOpportunityId: match.opp?.id || match.opp?.opportunityId || null,
  });

  if (match.opp) {
    for (const key of [
      "decisionId",
      "customerValidation",
      "validationStatus",
      "actionStatus",
      "salesActionStatus",
      "outcomeStatus",
      "feedback",
      "firstSeenAt",
      "createdAt",
      "prospectReviewCardId",
      "prospectReviewSection",
      "contactPackage",
      "primaryContact",
      "salesOwner",
      "owner",
    ]) {
      if (match.opp[key] != null && opp[key] == null) opp[key] = match.opp[key];
    }
    if (match.opp.firstSeenAt) opp.firstSeenAt = match.opp.firstSeenAt;
    // Never drop an existing reviewed-publication stamp on update
    if (match.opp.prospectPublicationBasis) {
      opp.prospectPublicationBasis = match.opp.prospectPublicationBasis;
    }
    if (match.opp.prospectHotelFacing === true) {
      opp.prospectHotelFacing = true;
    }
  }

  report.steps.opportunityMatch = {
    mode: match.mode,
    existingId: match.opp?.id || match.opp?.opportunityId || null,
    targetId: opp.id,
  };

  // New customer-facing promotion must pass shared readiness (fit + drawer + eligibility).
  // Enrichment of an already-facing row may preserve publication metadata without re-promoting.
  const alreadyFacing = match.opp?.prospectHotelFacing === true;
  const promotingFacing =
    (pkg.publish === true || opp.prospectHotelFacing === true) && !alreadyFacing;
  if (promotingFacing) {
    const ready = assertPublicationWriteReadiness(opp);
    report.steps.publicationWriteReadiness = {
      ok: ready.ok,
      incomplete: ready.incomplete === true,
      unknown: ready.unknown === true,
      blockers: ready.blockers,
      reasons: ready.reasons,
    };
    if (!ready.ok) {
      report.ok = false;
      report.failReasons = (ready.blockers || []).map((b) => b.code);
      report.warnings = ready.reasons || [];
      report.publicationWriteReady = false;
      return report;
    }
  }

  if (!dryRun) {
    const write = await upsertOpportunity(opp, {
      hotelId,
      runId: pkg.researchVersion || "pe_canonical_persist_v1",
    });
    invalidateGdiHotelReadCache(hotelId);
    try {
      const { appendHotelActivityEvent, ACTIVITY_KIND } = await import(
        "../hotel-activity-history-v1.js"
      );
      appendHotelActivityEvent(hotelId, {
        kind: ACTIVITY_KIND.CANONICAL_PUBLICATION,
        status: "completed",
        at: new Date().toISOString(),
        runId: pkg.researchVersion || "pe_canonical_persist_v1",
        source: "pe-canonical-persist-and-link-v1",
        provenance: "live_write",
        note: opp.id || opp.opportunityId || null,
      });
    } catch (actErr) {
      console.warn("[pe-canonical] activity ledger:", actErr?.message || actErr);
    }
    report.steps.opportunityWrite = {
      ok: true,
      created: Boolean(write?.created),
      updated: Boolean(write?.updated || !write?.created),
      airtableRecordId: write?.id || write?.recordId || null,
    };

    const fitRecId = fitUpsert.recordId || fitUpsert.airtableRecordId;
    if (fitRecId) {
      try {
        await getPeBase()(HOTEL_VENUE_FIT_TABLE_NAME).update(fitRecId, {
          [MAP_HOTEL_VENUE_FIT.currentGdiOpportunityId]: opp.id,
          [MAP_HOTEL_VENUE_FIT.currentGdiOpportunityStatus]: pkg.publish
            ? "PROMOTED"
            : "LINKED_INTERNAL",
        });
        report.steps.fitPointerUpdated = true;
      } catch (err) {
        report.steps.fitPointerUpdated = false;
        report.steps.fitPointerError = String(err?.message || err);
      }
    }

    const readback = await findOpportunityRecordById(opp.id);
    report.steps.readback = readback
      ? {
          PASS: true,
          peVenueId: readback.fields?.peVenueId || null,
          hotelVenueFitId: readback.fields?.hotelVenueFitId || null,
          peSignalId: readback.fields?.peSignalId || null,
          opportunityType: readback.fields?.opportunityType || null,
          demandFamily: readback.fields?.demandFamily || null,
        }
      : { PASS: false };
  } else {
    report.steps.opportunityWrite = { ok: false, dryRun: true };
    report.payloadPreview = {
      id: opp.id,
      title: opp.title,
      opportunityType: opp.opportunityType,
      peVenueId: opp.peVenueId,
      hotelVenueFitId: opp.hotelVenueFitId,
      contactGrade: opp.contactGrade,
      commercialContactPath: opp.commercialContactPath,
      publish: pkg.publish !== false,
    };
  }

  report.opportunityId = opp.id;
  report.published = pkg.publish !== false;
  report.verdict = dryRun
    ? "DRY_RUN"
    : report.steps.readback?.PASS
      ? "PERSISTED_AND_LINKED"
      : "WRITE_UNCERTAIN";
  return report;
}

export async function persistVenuePartnershipPackages(
  packages,
  { dryRun = true, outDir = null } = {}
) {
  const results = [];
  for (const pkg of packages) {
    results.push(await persistVenuePartnershipPackage(pkg, { dryRun }));
  }
  const summary = {
    generatedAt: nowIso(),
    dryRun,
    count: results.length,
    byVerdict: results.reduce((acc, r) => {
      acc[r.verdict] = (acc[r.verdict] || 0) + 1;
      return acc;
    }, {}),
    results,
  };
  if (outDir) {
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, dryRun ? "dry-run-report.json" : "apply-report.json"),
      JSON.stringify(summary, null, 2) + "\n"
    );
  }
  return summary;
}

/**
 * Load PE packages and adapt provider schema variants → canonical shape.
 * Nested Parallel packages (venue.name) and flat dispositions are accepted.
 * Malformed rows are omitted from the return list; callers can pass
 * `{ includeReviewQueue: true }` to also receive the review queue.
 */
export function loadVenuePartnershipPackagesFromFile(
  filePath,
  { includeReviewQueue = false, hotelId = null } = {}
) {
  const abs = path.isAbsolute(filePath) ? filePath : path.join(ROOT, filePath);
  const doc = JSON.parse(fs.readFileSync(abs, "utf8"));
  const raw = Array.isArray(doc) ? doc : doc.packages || [];
  const filtered = hotelId
    ? raw.filter(
        (p) =>
          (p.hotel?.hotelId || p.hotel?.id || p.hotelId || p.package?.hotelId) ===
          hotelId
      )
    : raw;
  // Fast path: already canonical (venue.venueName present on all)
  const allCanonical = filtered.every((p) => p?.venue?.venueName && p?.hotel);
  if (allCanonical) {
    if (includeReviewQueue) {
      return {
        packages: filtered,
        reviewQueue: [],
        packageCount: filtered.length,
        reviewCount: 0,
        sharedDomainVenues: [],
      };
    }
    return filtered;
  }
  const adapted = adaptPeCandidatesBatch(filtered, { hotelId });
  if (includeReviewQueue) {
    return adapted;
  }
  return adapted.packages;
}

export { computeVenueId, buildHotelContext };
