/**
 * Private Events V1.6 — enrich GDI opportunity for customer detail.
 * Resolves venue/fit graph fields + normalizes sources/evidence/contact
 * so VENUE_PARTNERSHIP and SPECIFIC_PRIVATE_EVENT render cleanly.
 */

import { escapeAirtableFormulaValue } from "../../airtable-utils.js";
import { OPPORTUNITY_TYPE, OPPORTUNITY_TYPE_LABEL } from "../claim-types.js";
import { DEMAND_FAMILY, DEMAND_FAMILY_LABEL } from "../demand-signal-types.js";
import {
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  MAP_PE_VENUE,
  MAP_HOTEL_VENUE_FIT,
} from "./airtable-field-map.js";
import {
  getPeBase,
  isPeAirtableConfigured,
} from "./airtable-client.js";
import { airtableRecordToVenue } from "./airtable-venue-store.js";

export const PE_CUSTOMER_LABELS = Object.freeze({
  opportunityType: {
    VENUE_PARTNERSHIP: "Venue Partnership",
    SPECIFIC_PRIVATE_EVENT: "Specific Private Event",
  },
  demandFamily: {
    PRIVATE_EVENTS: "Private Events",
  },
  eventActivityEvidenceStatus: {
    CONFIRMED_VOLUME: "Confirmed event volume",
    STRONG_REPEATED_ACTIVITY: "Strong repeated private-event activity",
    MODERATE_ACTIVITY: "Moderate private-event activity",
    LIMITED_ACTIVITY: "Limited private-event activity",
    UNKNOWN: "Event activity unknown",
  },
  partnerStatus: {
    EXCLUSIVE_PARTNER_FOUND: "Exclusive hotel partner identified",
    PREFERRED_PARTNER_FOUND: "Preferred hotel partner identified",
    NONEXCLUSIVE_PARTNERS_FOUND: "Non-exclusive hotel partners identified",
    NO_PUBLIC_PARTNER_FOUND: "No public preferred hotel partner identified",
    UNKNOWN: "Partner status unknown",
  },
  onSiteLodgingStatus: {
    NO_LODGING: "No on-site lodging",
    LIMITED_LODGING: "Limited on-site lodging",
    ADEQUATE_LODGING: "Adequate on-site lodging",
    UNKNOWN: "On-site lodging unknown",
  },
  lodgingCatchmentFit: {
    CORE: "Core catchment",
    COMPETITIVE: "Competitive catchment",
    STRETCH: "Stretch catchment",
    OUTSIDE: "Outside catchment",
    UNKNOWN: "Catchment unknown",
  },
  productFit: {
    STRONG: "Strong",
    MODERATE: "Moderate",
    WEAK: "Weak",
    POOR: "Poor",
    UNKNOWN: "Unknown",
  },
  partnershipPotential: {
    HIGH: "High",
    MEDIUM: "Medium",
    LOW: "Low",
    NONE: "None",
    UNKNOWN: "Unknown",
  },
  lodgingCapturePotential: {
    HIGH: "High",
    MODERATE: "Moderate",
    LOW: "Low",
    UNKNOWN: "Unknown",
  },
  commercialContactPath: {
    NAMED_DIRECT: "Named contact",
    NAMED_PARTIAL: "Named contact (partial)",
    FUNCTIONAL: "Functional contact (events / sales)",
    VENUE_EVENT_DIRECTOR: "Venue events director path",
    VENUE_SALES: "Venue sales path",
    EVENT_TEAM: "Venue events team",
    ORGANIZATION_PATH: "Venue / organization contact path",
    NO_CONTACT: "No contact path identified",
  },
});

export function isPrivateEventOpportunity(opp = {}) {
  const type = String(opp.opportunityType || "");
  const fam = String(opp.demandFamily || "");
  return (
    type === OPPORTUNITY_TYPE.VENUE_PARTNERSHIP ||
    type === OPPORTUNITY_TYPE.SPECIFIC_PRIVATE_EVENT ||
    fam === DEMAND_FAMILY.PRIVATE_EVENTS ||
    Boolean(opp.peVenueId)
  );
}

export function isVenuePartnership(opp = {}) {
  return String(opp.opportunityType || "") === OPPORTUNITY_TYPE.VENUE_PARTNERSHIP;
}

function labelOf(map, value, fallback) {
  if (value == null || value === "") return fallback || null;
  return map[value] || fallback || String(value);
}

function hostOf(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function supportsFactFromType(type) {
  const t = String(type || "").toUpperCase();
  if (/WEDDING|PRIVATE_EVENT|EVENT_CALENDAR|DATED_EVENT|FUTURE_EVENT/.test(t)) {
    return "Private-event / wedding activity";
  }
  if (/LODGING|HOTEL|PARTNER/.test(t)) return "Hotel partnership / lodging status";
  if (/CAPACITY|VENUE/.test(t)) return "Venue identity / capacity";
  if (/BOOKING|INQUIRY|CONTACT/.test(t)) return "Commercial contact / inquiry path";
  if (/OFFICIAL/.test(t)) return "Official venue source";
  return "Venue evidence";
}

/**
 * Normalize PE / mixed source shapes into customer source DTOs.
 */
export function normalizePeCustomerSources(sources = [], { officialDomain = "" } = {}) {
  const preferredHost = String(officialDomain || "")
    .replace(/^www\./i, "")
    .toLowerCase();
  const seen = new Set();
  const mapped = [];
  for (const raw of sources || []) {
    if (!raw || typeof raw !== "object") continue;
    const url = String(raw.url || raw.sourceUrl || "").trim();
    if (!url || !/^https?:\/\//i.test(url)) continue;
    const key = url.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) continue;
    seen.add(key);
    const title =
      String(raw.title || raw.name || raw.sourceTitle || "").trim() ||
      "Source";
    const sourceType = String(
      raw.sourceType || raw.type || raw.authority || "venue"
    );
    mapped.push({
      name: title,
      title,
      url,
      sourceType,
      authority:
        raw.authority ||
        raw.sourceAuthority ||
        (/official/i.test(sourceType) ? "OFFICIAL_VENUE" : null),
      date: raw.date || raw.sourceDate || null,
      whatItSupports: raw.whatItSupports || raw.supportsFact || supportsFactFromType(sourceType),
      supportsFact: raw.supportsFact || supportsFactFromType(sourceType),
      claimKind: raw.claimKind || "FACT",
      officialHostMatch: preferredHost
        ? hostOf(url) === preferredHost || hostOf(url).endsWith(`.${preferredHost}`)
        : null,
    });
  }

  // Prefer official-domain matches first for customer clarity
  mapped.sort((a, b) => {
    if (a.officialHostMatch === b.officialHostMatch) return 0;
    if (a.officialHostMatch) return -1;
    if (b.officialHostMatch) return 1;
    return 0;
  });

  // If we have preferred-host sources, drop clearly foreign "official" noise
  const preferred = mapped.filter((s) => s.officialHostMatch);
  if (preferredHost && preferred.length > 0) {
    return preferred.slice(0, 12);
  }
  return mapped.slice(0, 12);
}

function parseJsonMaybe(raw, fallback) {
  if (raw == null) return fallback;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return fallback;
  }
}

async function loadVenueById(venueId) {
  if (!venueId || !isPeAirtableConfigured()) return null;
  const rows = await getPeBase()(PE_VENUES_TABLE_NAME)
    .select({
      filterByFormula: `{${MAP_PE_VENUE.venueId}}='${escapeAirtableFormulaValue(venueId)}'`,
      maxRecords: 1,
    })
    .firstPage();
  return rows[0] ? airtableRecordToVenue(rows[0]) : null;
}

async function loadFitById(fitId) {
  if (!fitId || !isPeAirtableConfigured()) return null;
  const rows = await getPeBase()(HOTEL_VENUE_FIT_TABLE_NAME)
    .select({
      filterByFormula: `{${MAP_HOTEL_VENUE_FIT.fitId}}='${escapeAirtableFormulaValue(fitId)}'`,
      maxRecords: 1,
    })
    .firstPage();
  if (!rows[0]) return null;
  const f = rows[0].fields || {};
  return {
    fitId: f[MAP_HOTEL_VENUE_FIT.fitId],
    hotelId: f[MAP_HOTEL_VENUE_FIT.hotelId],
    venueId: f[MAP_HOTEL_VENUE_FIT.venueId],
    venueName: f[MAP_HOTEL_VENUE_FIT.venueName],
    distanceMiles: f[MAP_HOTEL_VENUE_FIT.distanceMiles],
    distanceKm: f[MAP_HOTEL_VENUE_FIT.distanceKm],
    driveTimeMinutes: f[MAP_HOTEL_VENUE_FIT.driveTimeMinutes],
    lodgingCatchmentFit: f[MAP_HOTEL_VENUE_FIT.lodgingCatchmentFit],
    productFit: f[MAP_HOTEL_VENUE_FIT.productFit],
    partnershipPotential: f[MAP_HOTEL_VENUE_FIT.partnershipPotential],
    lodgingCapturePotential: f[MAP_HOTEL_VENUE_FIT.lodgingCapturePotential],
    fitRationale: f[MAP_HOTEL_VENUE_FIT.fitRationale],
    whyHotelCouldWin: f[MAP_HOTEL_VENUE_FIT.whyHotelCouldWin],
    evidenceUrls: String(f[MAP_HOTEL_VENUE_FIT.evidenceUrls] || "")
      .split(/\n+/)
      .map((s) => s.trim())
      .filter(Boolean),
  };
}

function buildPeKnownVsEstimated({ opp, venue, fit }) {
  const verified = [];
  const estimated = [];
  const inferred = [];

  const venueName = venue?.venueName || opp.venueName || opp.organizationName;
  if (venueName) {
    verified.push({ field: "Venue identity", value: venueName, status: "Verified" });
  }
  const address = [venue?.address || opp.venueAddress, venue?.city || opp.venueCity, venue?.region || opp.venueRegion]
    .filter(Boolean)
    .join(", ");
  if (address) {
    verified.push({ field: "Venue address", value: address, status: "Verified" });
  }
  const lodging = venue?.onSiteLodgingStatus || opp.onSiteLodgingStatus || opp.venueLodgingStatus;
  if (lodging) {
    verified.push({
      field: "On-site lodging",
      value: labelOf(PE_CUSTOMER_LABELS.onSiteLodgingStatus, lodging, lodging),
      status: "Verified",
    });
  }
  const activity =
    venue?.eventActivityEvidenceStatus || opp.eventActivityEvidenceStatus;
  if (activity) {
    verified.push({
      field: "Private-event activity",
      value: labelOf(PE_CUSTOMER_LABELS.eventActivityEvidenceStatus, activity, activity),
      status: "Verified",
    });
  }
  const distance = fit?.distanceMiles ?? opp.distanceMiles;
  if (distance != null) {
    verified.push({
      field: "Distance from hotel",
      value: `${distance} miles`,
      status: "Verified",
    });
  }
  if ((opp.sources || []).some((s) => s.url && /official|OFFICIAL/i.test(s.sourceType || s.type || ""))) {
    verified.push({
      field: "Official source",
      value: "Official venue web presence",
      status: "Verified",
    });
  }

  const cap = venue?.maxCapacity ?? opp.maxCapacity ?? opp.venueCapacity;
  if (cap != null) {
    estimated.push({
      field: "Venue capacity",
      value: String(cap),
      status: "Estimated / published",
    });
  }

  const catchment = fit?.lodgingCatchmentFit || opp.lodgingCatchmentFit;
  if (catchment) {
    inferred.push({
      field: "Lodging catchment",
      value: labelOf(PE_CUSTOMER_LABELS.lodgingCatchmentFit, catchment, catchment),
      status: "Inferred",
    });
  }
  const partnership =
    fit?.partnershipPotential || opp.partnershipPotential;
  if (partnership) {
    inferred.push({
      field: "Partnership opportunity",
      value: labelOf(PE_CUSTOMER_LABELS.partnershipPotential, partnership, partnership),
      status: "Inferred",
    });
  }
  const capture =
    fit?.lodgingCapturePotential || opp.lodgingCapturePotential;
  if (capture) {
    inferred.push({
      field: "Lodging capture potential",
      value: labelOf(PE_CUSTOMER_LABELS.lodgingCapturePotential, capture, capture),
      status: "Inferred",
    });
  }

  return { verified, estimated, inferred };
}

function buildPeEvidenceExplanation(known, sources, score) {
  const v = known.verified.length;
  const e = known.estimated.length;
  const i = known.inferred.length;
  const official = (sources || []).filter(
    (s) =>
      s.officialHostMatch ||
      /official|OFFICIAL_VENUE/i.test(String(s.authority || s.sourceType || ""))
  ).length;
  const secondary = Math.max(0, (sources || []).length - official);
  const band =
    score >= 75 ? "High" : score >= 50 ? "Moderate" : score != null ? "Lower" : null;
  const parts = [
    band ? `${band} evidence confidence` : "Evidence confidence",
    `${v} verified field${v === 1 ? "" : "s"}`,
    `${e} estimated`,
    `${i} inferred`,
    `${official} official source${official === 1 ? "" : "s"}`,
  ];
  if (secondary > 0) parts.push(`${secondary} secondary`);
  return parts.join(" · ");
}

function buildOrganizationContact(opp, venue) {
  const path = opp.commercialContactPath || venue?.commercialContactPath || "ORGANIZATION_PATH";
  const website =
    venue?.website ||
    opp.officialSourceUrls?.[0] ||
    (opp.sources || []).find((s) => s.url)?.url ||
    null;
  const name =
    venue?.eventContactName ||
    opp.contactName ||
    opp.primaryContact?.name ||
    null;
  const role =
    venue?.eventContactRole ||
    opp.contactRole ||
    opp.primaryContact?.role ||
    null;
  const email =
    venue?.eventContactEmail ||
    opp.contactEmail ||
    opp.primaryContact?.email ||
    null;
  const phone =
    venue?.eventContactPhone ||
    opp.contactPhone ||
    opp.primaryContact?.phone ||
    null;

  if (name || email || phone) {
    return {
      commercialContactPath: path,
      commercialContactPathLabel: labelOf(
        PE_CUSTOMER_LABELS.commercialContactPath,
        path,
        path
      ),
      contactDisplayMode: name ? (email || phone ? "NAMED_DIRECT" : "NAMED_PARTIAL") : "FUNCTIONAL",
      primaryContact: {
        name: name || "Venue Events Team",
        role: role && role !== "UNKNOWN" ? role : "Events / venue sales",
        email: email || null,
        phone: phone || null,
        contactKind: name ? "NAMED" : "FUNCTIONAL",
        functionalEntity: !name,
        whyThisContact:
          "Primary commercial path for preferred lodging / room-block partnership discussion.",
      },
      contactOfficialUrl: website,
    };
  }

  // ORGANIZATION_PATH without named person — still a valid commercial path
  return {
    commercialContactPath: path,
    commercialContactPathLabel: labelOf(
      PE_CUSTOMER_LABELS.commercialContactPath,
      path,
      "Venue / organization contact path"
    ),
    contactDisplayMode: "ORGANIZATION_PATH",
    primaryContact: {
      name: "Venue Events Team",
      role: "Organization contact",
      email: null,
      phone: null,
      contactKind: "FUNCTIONAL",
      functionalEntity: true,
      whyThisContact:
        "No named individual published; contact the venue's events / organization path via the official venue site.",
      officialContactUrl: website,
    },
    contactOfficialUrl: website,
    whoPrimaryReason:
      "Organization contact path — reach the venue events team through the official venue contact / inquiry channel.",
  };
}

function shortPartnershipSummary(opp, venue, fit) {
  if (opp.summaryWhat && !/preferred lodging partnership/i.test(opp.summaryWhat)) {
    return opp.summaryWhat;
  }
  const lodging = labelOf(
    PE_CUSTOMER_LABELS.onSiteLodgingStatus,
    venue?.onSiteLodgingStatus || opp.onSiteLodgingStatus,
    "no on-site lodging"
  ).toLowerCase();
  const partner = labelOf(
    PE_CUSTOMER_LABELS.partnerStatus,
    venue?.partnerStatus || opp.partnerStatus,
    "no public preferred hotel partner identified"
  ).toLowerCase();
  const catchment = labelOf(
    PE_CUSTOMER_LABELS.lodgingCatchmentFit,
    fit?.lodgingCatchmentFit || opp.lodgingCatchmentFit,
    "core catchment"
  ).toLowerCase();
  return `Private-event venue within the hotel's ${catchment} with ${lodging} and ${partner}.`;
}

/**
 * Enrich a PE opportunity for customer auth/share detail.
 * Safe no-op for non-PE opportunities.
 */
export async function enrichPrivateEventOpportunityDetail(opp, { hydrate = true } = {}) {
  if (!opp || !isPrivateEventOpportunity(opp)) {
    return { opportunity: opp, peEnriched: false };
  }

  let venue = null;
  let fit = null;
  if (hydrate) {
    try {
      if (opp.peVenueId) venue = await loadVenueById(opp.peVenueId);
    } catch (err) {
      console.error("[gdi-pe] venue hydrate failed", err?.message || err);
    }
    try {
      if (opp.hotelVenueFitId) fit = await loadFitById(opp.hotelVenueFitId);
    } catch (err) {
      console.error("[gdi-pe] fit hydrate failed", err?.message || err);
    }
  }

  const activityEvidence = parseJsonMaybe(
    venue?.activityEvidenceJson || opp.activityEvidence,
    opp.activityEvidence || []
  );

  const rawSources = [
    ...(Array.isArray(opp.sources) ? opp.sources : []),
    ...(venue?.sourceUrls || []).map((url) => ({
      url,
      type: "official_venue",
      title: "Official venue source",
    })),
    ...(fit?.evidenceUrls || []).map((url) => ({
      url,
      type: "fit_evidence",
      title: "Fit evidence",
    })),
  ];

  const officialDomain = venue?.officialDomain || opp.officialDomain || "";
  const sources = normalizePeCustomerSources(rawSources, { officialDomain });
  const officialSourceUrls = sources
    .filter((s) => s.officialHostMatch || /official/i.test(s.sourceType))
    .map((s) => s.url);

  const contactBlock = buildOrganizationContact(opp, venue);
  const knownVsEstimated = buildPeKnownVsEstimated({ opp, venue, fit });

  const scoreRaw = opp.evidenceConfidence;
  let evidenceConfidence =
    scoreRaw != null && Number.isFinite(Number(scoreRaw)) ? Number(scoreRaw) : null;
  // Fail-closed: never show high numeric confidence with zero verified facts
  if (
    evidenceConfidence != null &&
    evidenceConfidence >= 50 &&
    knownVsEstimated.verified.length === 0 &&
    sources.length === 0
  ) {
    evidenceConfidence = null;
  }
  const evidenceConfidenceExplanation = buildPeEvidenceExplanation(
    knownVsEstimated,
    sources,
    evidenceConfidence
  );

  const partnership = isVenuePartnership(opp);
  const venueAddress = venue?.address || opp.venueAddress || null;
  const venueCity = venue?.city || opp.venueCity || null;
  const venueRegion = venue?.region || opp.venueRegion || null;
  const distanceMiles = fit?.distanceMiles ?? opp.distanceMiles ?? null;
  const distanceKm = fit?.distanceKm ?? opp.distanceKm ?? null;
  const driveTimeMinutes =
    fit?.driveTimeMinutes ?? opp.driveTimeMinutes ?? null;
  const lodgingCatchmentFit =
    fit?.lodgingCatchmentFit || opp.lodgingCatchmentFit || null;
  const productFit = fit?.productFit || opp.productFit || null;
  const partnershipPotential =
    fit?.partnershipPotential || opp.partnershipPotential || null;
  const lodgingCapturePotential =
    fit?.lodgingCapturePotential || opp.lodgingCapturePotential || null;
  const onSiteLodgingStatus =
    venue?.onSiteLodgingStatus || opp.onSiteLodgingStatus || null;
  const partnerStatus = venue?.partnerStatus || opp.partnerStatus || null;
  const eventActivityEvidenceStatus =
    venue?.eventActivityEvidenceStatus || opp.eventActivityEvidenceStatus || null;

  const locationSummary = [
    venue?.venueName || opp.organizationName,
    [venueAddress, venueCity, venueRegion].filter(Boolean).join(", "),
    distanceMiles != null ? `${distanceMiles} mi from hotel` : null,
    driveTimeMinutes != null ? `~${driveTimeMinutes} min drive` : null,
    lodgingCatchmentFit
      ? labelOf(PE_CUSTOMER_LABELS.lodgingCatchmentFit, lodgingCatchmentFit)
      : null,
  ]
    .filter(Boolean)
    .join(" · ");

  const whyMatters =
    opp.summaryWhyMatters ||
    opp.whyThisMatters ||
    opp.hotelOpportunityThesis ||
    opp.hotelWinThesis ||
    null;

  const enriched = {
    ...opp,
    opportunityTypeLabel:
      opp.opportunityTypeLabel ||
      PE_CUSTOMER_LABELS.opportunityType[opp.opportunityType] ||
      OPPORTUNITY_TYPE_LABEL[opp.opportunityType] ||
      opp.opportunityType,
    demandFamilyLabel:
      opp.demandFamilyLabel ||
      PE_CUSTOMER_LABELS.demandFamily[opp.demandFamily] ||
      DEMAND_FAMILY_LABEL[opp.demandFamily] ||
      opp.demandFamily,
    demandSignalTypeLabel:
      opp.demandSignalTypeLabel ||
      PE_CUSTOMER_LABELS.opportunityType[opp.demandSignalType] ||
      opp.demandSignalType,

    venueId: opp.peVenueId || venue?.venueId || null,
    venueName: venue?.venueName || opp.venueName || opp.organizationName || null,
    venueAddress,
    venueCity,
    venueRegion,
    venueType: venue?.venueType || opp.venueType || null,
    venueCapacity: venue?.maxCapacity ?? opp.maxCapacity ?? null,
    maxCapacity: venue?.maxCapacity ?? opp.maxCapacity ?? null,
    minCapacity: venue?.minCapacity ?? opp.minCapacity ?? null,

    distanceMiles,
    distanceKm,
    driveTimeMinutes,

    venueLodgingStatus: onSiteLodgingStatus,
    onSiteLodgingStatus,
    onSiteLodgingStatusLabel: labelOf(
      PE_CUSTOMER_LABELS.onSiteLodgingStatus,
      onSiteLodgingStatus
    ),
    onSiteGuestrooms: venue?.onSiteGuestrooms ?? opp.onSiteGuestrooms ?? null,

    eventActivityEvidenceStatus,
    eventActivityEvidenceStatusLabel: labelOf(
      PE_CUSTOMER_LABELS.eventActivityEvidenceStatus,
      eventActivityEvidenceStatus
    ),
    partnerStatus,
    partnerStatusLabel: labelOf(PE_CUSTOMER_LABELS.partnerStatus, partnerStatus),

    productFit,
    productFitLabel: labelOf(PE_CUSTOMER_LABELS.productFit, productFit),
    lodgingCatchmentFit,
    lodgingCatchmentFitLabel: labelOf(
      PE_CUSTOMER_LABELS.lodgingCatchmentFit,
      lodgingCatchmentFit
    ),
    partnershipPotential,
    partnershipPotentialLabel: labelOf(
      PE_CUSTOMER_LABELS.partnershipPotential,
      partnershipPotential
    ),
    lodgingCapturePotential,
    lodgingCapturePotentialLabel: labelOf(
      PE_CUSTOMER_LABELS.lodgingCapturePotential,
      lodgingCapturePotential
    ),
    fitRationale: fit?.fitRationale || opp.fitRationale || null,

    sources,
    sourceCount: sources.length,
    officialSourceUrls,
    activityEvidence,
    lodgingEvidence: opp.lodgingEvidence || null,
    partnerEvidence: opp.partnerEvidence || null,

    ...contactBlock,

    // Location for partnership → venue location (not blank event location)
    eventLocationStatus: partnership ? "VENUE_PARTNERSHIP_LOCATION" : opp.eventLocationStatus,
    eventLocationStatusLabel: partnership
      ? "Venue location"
      : opp.eventLocationStatusLabel || opp.eventLocationStatus,
    eventLocationSummary: locationSummary || opp.eventLocationSummary || null,
    demandTerritoryFit: lodgingCatchmentFit || opp.demandTerritoryFit,
    demandTerritoryFitLabel:
      labelOf(PE_CUSTOMER_LABELS.lodgingCatchmentFit, lodgingCatchmentFit) ||
      opp.demandTerritoryFitLabel,

    // Standing partnership — no TBD event dates
    eventDateDisplay: partnership
      ? "Ongoing partnership opportunity"
      : opp.eventDateDisplay,
    eventStartDate: partnership ? null : opp.eventStartDate,
    eventEndDate: partnership ? null : opp.eventEndDate,
    standingOpportunity: partnership,

    // Partnership status instead of OPEN_UNRESOLVED sourcing theatre
    venueSourcingStatusLabel: partnership
      ? labelOf(PE_CUSTOMER_LABELS.partnerStatus, partnerStatus) ||
        opp.venueSourcingStatusLabel
      : opp.venueSourcingStatusLabel,
    partnershipStatusDisplay: partnership
      ? {
          partnerStatus,
          partnerStatusLabel: labelOf(PE_CUSTOMER_LABELS.partnerStatus, partnerStatus),
          partnershipPotential,
          partnershipPotentialLabel: labelOf(
            PE_CUSTOMER_LABELS.partnershipPotential,
            partnershipPotential
          ),
          venueLodgingStatus: onSiteLodgingStatus,
          venueLodgingStatusLabel: labelOf(
            PE_CUSTOMER_LABELS.onSiteLodgingStatus,
            onSiteLodgingStatus
          ),
        }
      : null,

    // Content hierarchy — short summary; thesis once; action only
    summaryWhat: partnership
      ? shortPartnershipSummary(opp, venue, fit)
      : opp.summaryWhat || opp.title,
    summaryWhyMatters: whyMatters,
    hotelOpportunityThesis: whyMatters,
    // Clear duplicate surfaces for UI (keep whyNow distinct)
    summaryWhyHotel: null,
    bethesdaWinThesis: null,
    hotelWinThesis: null,
    whyThisMatters: whyMatters,
    recommendedAction: opp.recommendedAction || opp.recommendedNextStep || null,

    knownVsEstimated,
    evidenceConfidence,
    evidenceConfidenceExplanation,
    evidenceConfidenceTooltip:
      "Based on verified venue facts, official sources, and inferred lodging / partnership fit — not a room-block forecast.",

    detailLayout: partnership
      ? "VENUE_PARTNERSHIP"
      : String(opp.opportunityType) === OPPORTUNITY_TYPE.SPECIFIC_PRIVATE_EVENT
        ? "SPECIFIC_PRIVATE_EVENT"
        : "PRIVATE_EVENTS",

    peDetailHydrated: true,
  };

  return {
    opportunity: enriched,
    peEnriched: true,
    sourceCount: sources.length,
    hasOfficialSource: officialSourceUrls.length > 0 || sources.length > 0,
    venueLoaded: Boolean(venue),
    fitLoaded: Boolean(fit),
  };
}
