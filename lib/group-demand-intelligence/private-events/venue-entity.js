/**
 * Canonical reusable event venue entity — global identity, not hotel-scoped.
 */

import { createHash } from "node:crypto";
import {
  ON_SITE_LODGING_STATUS,
  VENUE_TYPE,
} from "./constants.js";

function clean(s) {
  return String(s || "").trim();
}

function normKey(s) {
  return clean(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function domainFromUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.hostname.replace(/^www\./i, "").toLowerCase() || null;
  } catch {
    return null;
  }
}

/**
 * Stable venueId from official domain + normalized name (or name+city+region).
 * Never hotel-prefixed.
 */
export function computeVenueId({
  venueName,
  officialDomain,
  website,
  city,
  region,
  country,
} = {}) {
  const domain = clean(officialDomain) || domainFromUrl(website) || "";
  const name = normKey(venueName);
  const geo = [normKey(city), normKey(region), normKey(country || "US")]
    .filter(Boolean)
    .join("|");
  const seed = domain ? `d:${domain}|n:${name}` : `n:${name}|g:${geo}`;
  const hash = createHash("sha256").update(seed).digest("hex").slice(0, 16);
  return `pev_${hash}`;
}

export function normalizeOnSiteLodgingStatus({
  onSiteGuestrooms,
  onSiteLodgingStatus,
} = {}) {
  const explicit = clean(onSiteLodgingStatus).toUpperCase();
  if (Object.values(ON_SITE_LODGING_STATUS).includes(explicit)) return explicit;
  const rooms = Number(onSiteGuestrooms);
  if (!Number.isFinite(rooms) || rooms < 0) return ON_SITE_LODGING_STATUS.UNKNOWN;
  if (rooms === 0) return ON_SITE_LODGING_STATUS.NO_LODGING;
  if (rooms > 0 && rooms < 40) return ON_SITE_LODGING_STATUS.LIMITED_LODGING;
  return ON_SITE_LODGING_STATUS.ADEQUATE_LODGING;
}

export function normalizeVenueType(raw) {
  const t = clean(raw).toUpperCase().replace(/[\s-]+/g, "_");
  if (VENUE_TYPE[t]) return VENUE_TYPE[t];
  if (Object.values(VENUE_TYPE).includes(t)) return t;
  const blob = clean(raw).toLowerCase();
  if (/country\s*club/.test(blob)) return VENUE_TYPE.COUNTRY_CLUB;
  if (/museum/.test(blob)) return VENUE_TYPE.MUSEUM;
  if (/estate|manor|mansion/.test(blob)) return VENUE_TYPE.HISTORIC_ESTATE;
  if (/garden|arboretum/.test(blob)) return VENUE_TYPE.GARDEN;
  if (/winery|vineyard/.test(blob)) return VENUE_TYPE.WINERY;
  if (/banquet|ballroom/.test(blob)) return VENUE_TYPE.BANQUET_HALL;
  if (/church|synagogue|temple|mosque|chapel/.test(blob)) {
    return VENUE_TYPE.RELIGIOUS_VENUE;
  }
  if (/private\s*club|social\s*club/.test(blob)) return VENUE_TYPE.PRIVATE_CLUB;
  if (/conference|event\s*center/.test(blob)) return VENUE_TYPE.CONFERENCE_EVENT_CENTER;
  if (/wedding/.test(blob)) return VENUE_TYPE.WEDDING_VENUE;
  if (/rooftop|event\s*space/.test(blob)) return VENUE_TYPE.SOCIAL_EVENT_SPACE;
  return VENUE_TYPE.OTHER;
}

/**
 * Build / merge a canonical venue record.
 */
export function buildVenueEntity(raw = {}, prior = null) {
  const base = prior && typeof prior === "object" ? { ...prior } : {};
  const venueName = clean(raw.venueName || base.venueName);
  const website = clean(raw.website || base.website) || null;
  const officialDomain =
    clean(raw.officialDomain || base.officialDomain) || domainFromUrl(website);
  const city = clean(raw.city || base.city) || null;
  const region = clean(raw.region || base.region) || null;
  const country = clean(raw.country || base.country) || "US";
  const venueId =
    clean(raw.venueId || base.venueId) ||
    computeVenueId({
      venueName,
      officialDomain,
      website,
      city,
      region,
      country,
    });

  const aliases = [
    ...new Set(
      [...(base.venueAliases || []), ...(raw.venueAliases || [])]
        .map(clean)
        .filter(Boolean)
    ),
  ];

  const sourceUrls = [
    ...new Set(
      [...(base.sourceUrls || []), ...(raw.sourceUrls || [])]
        .map(clean)
        .filter(Boolean)
    ),
  ];

  const hotelPartners = [
    ...new Set(
      [...(base.hotelPartners || []), ...(raw.hotelPartners || [])]
        .map(clean)
        .filter(Boolean)
    ),
  ];

  const onSiteGuestrooms =
    raw.onSiteGuestrooms != null
      ? Number(raw.onSiteGuestrooms)
      : base.onSiteGuestrooms != null
        ? Number(base.onSiteGuestrooms)
        : null;

  const entity = {
    venueId,
    venueName,
    venueAliases: aliases,
    address: clean(raw.address || base.address) || null,
    city,
    region,
    country,
    lat:
      raw.lat != null
        ? Number(raw.lat)
        : base.lat != null
          ? Number(base.lat)
          : null,
    long:
      raw.long != null
        ? Number(raw.long)
        : base.long != null
          ? Number(base.long)
          : null,
    website,
    officialDomain: officialDomain || null,
    venueType: normalizeVenueType(raw.venueType || base.venueType),
    maxCapacity:
      raw.maxCapacity != null
        ? Number(raw.maxCapacity)
        : base.maxCapacity != null
          ? Number(base.maxCapacity)
          : null,
    minCapacity:
      raw.minCapacity != null
        ? Number(raw.minCapacity)
        : base.minCapacity != null
          ? Number(base.minCapacity)
          : null,
    onSiteGuestrooms: Number.isFinite(onSiteGuestrooms) ? onSiteGuestrooms : null,
    onSiteLodgingStatus: normalizeOnSiteLodgingStatus({
      onSiteGuestrooms,
      onSiteLodgingStatus: raw.onSiteLodgingStatus || base.onSiteLodgingStatus,
    }),
    weddingsAdvertised: Boolean(
      raw.weddingsAdvertised ?? base.weddingsAdvertised ?? false
    ),
    privateEventsAdvertised: Boolean(
      raw.privateEventsAdvertised ?? base.privateEventsAdvertised ?? false
    ),
    estimatedAnnualPrivateEvents:
      raw.estimatedAnnualPrivateEvents != null
        ? Number(raw.estimatedAnnualPrivateEvents)
        : base.estimatedAnnualPrivateEvents != null
          ? Number(base.estimatedAnnualPrivateEvents)
          : null,
    preferredHotelListed: Boolean(
      raw.preferredHotelListed ?? base.preferredHotelListed ?? false
    ),
    exclusiveHotelRelationship: Boolean(
      raw.exclusiveHotelRelationship ?? base.exclusiveHotelRelationship ?? false
    ),
    hotelPartners,
    eventContact: clean(raw.eventContact || base.eventContact) || null,
    eventContactRole: clean(raw.eventContactRole || base.eventContactRole) || null,
    eventContactName: clean(raw.eventContactName || base.eventContactName) || null,
    eventContactEmail:
      clean(raw.eventContactEmail || base.eventContactEmail) || null,
    sourceUrls,
    annualEventVolumeStatus:
      clean(raw.annualEventVolumeStatus || base.annualEventVolumeStatus) || null,
    eventActivityEvidenceStatus:
      clean(raw.eventActivityEvidenceStatus || base.eventActivityEvidenceStatus) ||
      null,
    activityEvidence: Array.isArray(raw.activityEvidence)
      ? raw.activityEvidence
      : Array.isArray(base.activityEvidence)
        ? base.activityEvidence
        : [],
    activityDecisionReason:
      clean(raw.activityDecisionReason || base.activityDecisionReason) || null,
    partnerStatus: clean(raw.partnerStatus || base.partnerStatus) || null,
    partnerResearchComplete: Boolean(
      raw.partnerResearchComplete ?? base.partnerResearchComplete ?? false
    ),
    hasPublicInquiryForm: Boolean(
      raw.hasPublicInquiryForm ?? base.hasPublicInquiryForm ?? false
    ),
    publicBookingLanguage: Boolean(
      raw.publicBookingLanguage ?? base.publicBookingLanguage ?? false
    ),
    organizationPath: clean(raw.organizationPath || base.organizationPath) || null,
    commercialContactPath:
      clean(raw.commercialContactPath || base.commercialContactPath) || null,
    lastVerifiedAt:
      clean(raw.lastVerifiedAt || base.lastVerifiedAt) || new Date().toISOString(),
  };

  return entity;
}

/** In-memory venue graph with global reuse (no hotel-scoped IDs). */
export function createVenueGraph(seedVenues = []) {
  const byId = new Map();
  for (const v of seedVenues) {
    const entity = buildVenueEntity(v);
    byId.set(entity.venueId, entity);
  }

  return {
    get(venueId) {
      return byId.get(venueId) || null;
    },
    upsert(raw) {
      const prior = raw.venueId ? byId.get(raw.venueId) : null;
      const entity = buildVenueEntity(raw, prior);
      // Domain+name collision reuse
      if (!prior) {
        for (const existing of byId.values()) {
          if (
            existing.officialDomain &&
            entity.officialDomain &&
            existing.officialDomain === entity.officialDomain &&
            normKey(existing.venueName) === normKey(entity.venueName)
          ) {
            const merged = buildVenueEntity(raw, existing);
            byId.set(merged.venueId, merged);
            return { venue: merged, reused: true };
          }
        }
      }
      byId.set(entity.venueId, entity);
      return { venue: entity, reused: Boolean(prior) };
    },
    list() {
      return [...byId.values()];
    },
    size() {
      return byId.size;
    },
  };
}
