/**
 * Private Event Venues — global Airtable upsert + dedupe.
 * One venue once; never hotel-scoped identity.
 */

import { createHash } from "node:crypto";
import {
  PE_VENUES_TABLE_NAME,
  MAP_PE_VENUE as F,
  VAL_PE_VENUE_TYPE,
  VAL_ON_SITE_LODGING,
  VAL_RESEARCH_STATUS,
  VAL_ANNUAL_EVENT_VOLUME_STATUS,
  VAL_CANONICAL_STATUS,
  VAL_EVENT_ACTIVITY_EVIDENCE_STATUS,
  VAL_PARTNER_STATUS,
  toAirtableVenueType,
} from "./airtable-field-map.js";
import {
  getPeBase,
  isPeAirtableConfigured,
  omitEmpty,
  asSelect,
  jsonText,
  nowIso,
  schemaVersion,
  findByField,
  createOrUpdate,
} from "./airtable-client.js";
import { buildVenueEntity, computeVenueId } from "./venue-entity.js";

function venuesTable() {
  return getPeBase()(PE_VENUES_TABLE_NAME);
}

function normName(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function domainOf(v) {
  return String(v?.officialDomain || "")
    .toLowerCase()
    .replace(/^www\./, "")
    .trim();
}

function haversineMiles(a, b) {
  const lat1 = Number(a?.lat ?? a?.latitude);
  const lon1 = Number(a?.long ?? a?.longitude ?? a?.lng);
  const lat2 = Number(b?.lat ?? b?.latitude);
  const lon2 = Number(b?.long ?? b?.longitude ?? b?.lng);
  if (![lat1, lon1, lat2, lon2].every(Number.isFinite)) return null;
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 3958.7613;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x));
}

/**
 * Deterministic + fuzzy match against an in-memory venue list (and optional Airtable hits).
 */
export function findVenueMatch(candidate, existingList = []) {
  const entity = buildVenueEntity(candidate);
  const cDomain = domainOf(entity);
  const cName = normName(entity.venueName);
  const cId = entity.venueId;

  for (const ex of existingList) {
    if (ex.venueId && ex.venueId === cId) {
      return {
        match: ex,
        matchedBy: "VENUE_ID",
        matchConfidence: "HIGH",
        mergeReason: "Identical stable Venue ID",
      };
    }
  }

  for (const ex of existingList) {
    const eDomain = domainOf(ex);
    if (cDomain && eDomain && cDomain === eDomain && cName && normName(ex.venueName) === cName) {
      return {
        match: ex,
        matchedBy: "DOMAIN_NAME",
        matchConfidence: "HIGH",
        mergeReason: "Same official domain + normalized name",
      };
    }
  }

  for (const ex of existingList) {
    const eDomain = domainOf(ex);
    if (cDomain && eDomain && cDomain === eDomain) {
      return {
        match: ex,
        matchedBy: "DOMAIN",
        matchConfidence: "MEDIUM",
        mergeReason: "Same official domain",
      };
    }
  }

  for (const ex of existingList) {
    const aliases = [ex.venueName, ...(ex.venueAliases || [])].map(normName);
    if (cName && aliases.includes(cName)) {
      const miles = haversineMiles(entity, ex);
      if (miles == null || miles <= 0.5) {
        return {
          match: ex,
          matchedBy: miles != null ? "NAME_ALIAS_GEO" : "NAME_ALIAS",
          matchConfidence: miles != null ? "HIGH" : "MEDIUM",
          mergeReason: "Alias/name match within geo tolerance",
        };
      }
    }
  }

  for (const ex of existingList) {
    const miles = haversineMiles(entity, ex);
    if (
      miles != null &&
      miles <= 0.15 &&
      cName &&
      normName(ex.venueName).slice(0, 12) === cName.slice(0, 12)
    ) {
      return {
        match: ex,
        matchedBy: "GEO_NAME_PREFIX",
        matchConfidence: "MEDIUM",
        mergeReason: "Near-identical coordinates + name prefix",
      };
    }
  }

  // Uncertain near-collision → NEEDS_REVIEW rather than insert
  for (const ex of existingList) {
    const miles = haversineMiles(entity, ex);
    if (miles != null && miles <= 0.05 && cName && normName(ex.venueName) !== cName) {
      return {
        match: null,
        needsReview: true,
        conflictWith: ex,
        matchedBy: "GEO_COLLISION_UNCERTAIN",
        matchConfidence: "LOW",
        mergeReason: "Coordinates collide with differently named venue",
      };
    }
  }

  return { match: null, matchedBy: null, matchConfidence: null };
}

export function venueToAirtableFields(venue, meta = {}) {
  const entity = buildVenueEntity(venue);
  const aliases = (entity.venueAliases || []).join("; ");
  const partners = (entity.hotelPartners || []).join("; ");
  const sources = (entity.sourceUrls || []).join("\n");
  const primarySource = entity.sourceUrls?.[0] || entity.website || null;
  const annualStatus =
    entity.estimatedAnnualPrivateEvents != null
      ? "ESTIMATED"
      : "UNKNOWN";

  return omitEmpty({
    [F.venueId]: entity.venueId,
    [F.venueName]: entity.venueName,
    [F.venueAliases]: aliases || undefined,
    [F.venueType]: asSelect(toAirtableVenueType(entity.venueType), VAL_PE_VENUE_TYPE),
    [F.address]: entity.address,
    [F.city]: entity.city,
    [F.region]: entity.region,
    [F.postalCode]: venue.postalCode || null,
    [F.country]: entity.country,
    [F.latitude]: entity.lat,
    [F.longitude]: entity.long,
    [F.website]: entity.website,
    [F.officialDomain]: entity.officialDomain,
    [F.minCapacity]: entity.minCapacity,
    [F.maxCapacity]: entity.maxCapacity,
    [F.onSiteLodgingStatus]: asSelect(
      entity.onSiteLodgingStatus,
      VAL_ON_SITE_LODGING
    ),
    [F.onSiteGuestrooms]: entity.onSiteGuestrooms,
    [F.weddingsAdvertised]: Boolean(entity.weddingsAdvertised),
    [F.privateEventsAdvertised]: Boolean(entity.privateEventsAdvertised),
    [F.estimatedAnnualPrivateEvents]: entity.estimatedAnnualPrivateEvents,
    [F.annualEventVolumeStatus]: asSelect(
      venue.annualEventVolumeStatus || annualStatus,
      VAL_ANNUAL_EVENT_VOLUME_STATUS
    ),
    [F.preferredHotelListed]: Boolean(entity.preferredHotelListed),
    [F.exclusiveHotelRelationship]: Boolean(entity.exclusiveHotelRelationship),
    [F.knownHotelPartners]: partners || undefined,
    [F.eventActivityEvidenceStatus]: asSelect(
      venue.eventActivityEvidenceStatus,
      VAL_EVENT_ACTIVITY_EVIDENCE_STATUS
    ),
    [F.activityEvidenceJson]:
      venue.activityEvidence != null
        ? jsonText(venue.activityEvidence)
        : undefined,
    [F.partnerStatus]: asSelect(venue.partnerStatus, VAL_PARTNER_STATUS),
    [F.eventContactName]: venue.eventContactName || null,
    [F.eventContactRole]: entity.eventContactRole,
    [F.eventContactEmail]: entity.eventContact?.includes("@")
      ? entity.eventContact
      : venue.eventContactEmail || null,
    [F.eventContactPhone]: venue.eventContactPhone || null,
    [F.sourceUrls]: sources || undefined,
    [F.primarySourceUrl]: primarySource,
    [F.sourceAuthority]: venue.sourceAuthority || "OFFICIAL",
    [F.researchStatus]: asSelect(
      meta.researchStatus || venue.researchStatus || "DISCOVERED",
      VAL_RESEARCH_STATUS
    ),
    [F.confidence]: venue.confidence != null ? Number(venue.confidence) : undefined,
    [F.firstSeen]: meta.firstSeen || venue.firstSeen || nowIso(),
    [F.lastSeen]: nowIso(),
    [F.lastVerified]: entity.lastVerifiedAt || nowIso(),
    [F.canonicalStatus]: asSelect(
      meta.canonicalStatus || venue.canonicalStatus || "ACTIVE",
      VAL_CANONICAL_STATUS
    ),
    [F.matchedBy]: meta.matchedBy || undefined,
    [F.matchConfidence]: meta.matchConfidence || undefined,
    [F.mergeReason]: meta.mergeReason || undefined,
    [F.schemaVersion]: schemaVersion(),
    [F.venuePayloadJson]: jsonText(entity),
  });
}

export function airtableRecordToVenue(rec) {
  if (!rec) return null;
  const f = rec.fields || {};
  return {
    airtableRecordId: rec.id,
    venueId: f[F.venueId],
    venueName: f[F.venueName],
    venueAliases: String(f[F.venueAliases] || "")
      .split(";")
      .map((s) => s.trim())
      .filter(Boolean),
    venueType: f[F.venueType],
    address: f[F.address],
    city: f[F.city],
    region: f[F.region],
    postalCode: f[F.postalCode],
    country: f[F.country],
    lat: f[F.latitude],
    long: f[F.longitude],
    website: f[F.website],
    officialDomain: f[F.officialDomain],
    minCapacity: f[F.minCapacity],
    maxCapacity: f[F.maxCapacity],
    onSiteLodgingStatus: f[F.onSiteLodgingStatus],
    onSiteGuestrooms: f[F.onSiteGuestrooms],
    weddingsAdvertised: f[F.weddingsAdvertised],
    privateEventsAdvertised: f[F.privateEventsAdvertised],
    estimatedAnnualPrivateEvents: f[F.estimatedAnnualPrivateEvents],
    preferredHotelListed: f[F.preferredHotelListed],
    exclusiveHotelRelationship: f[F.exclusiveHotelRelationship],
    hotelPartners: String(f[F.knownHotelPartners] || "")
      .split(";")
      .map((s) => s.trim())
      .filter(Boolean),
    eventActivityEvidenceStatus: f[F.eventActivityEvidenceStatus] || null,
    activityEvidence: (() => {
      try {
        const raw = f[F.activityEvidenceJson];
        if (!raw) return [];
        const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        return Array.isArray(parsed) ? parsed : [];
      } catch {
        return [];
      }
    })(),
    partnerStatus: f[F.partnerStatus] || null,
    eventContact: f[F.eventContactEmail] || f[F.eventContactName],
    eventContactName: f[F.eventContactName],
    eventContactRole: f[F.eventContactRole],
    eventContactEmail: f[F.eventContactEmail],
    eventContactPhone: f[F.eventContactPhone],
    sourceUrls: String(f[F.sourceUrls] || "")
      .split(/\n+/)
      .map((s) => s.trim())
      .filter(Boolean),
    researchStatus: f[F.researchStatus],
    firstSeen: f[F.firstSeen],
    lastSeen: f[F.lastSeen],
    lastVerifiedAt: f[F.lastVerified],
    canonicalStatus: f[F.canonicalStatus],
  };
}

export async function listVenuesFromAirtable({ maxRecords = 500 } = {}) {
  if (!isPeAirtableConfigured()) return [];
  const table = venuesTable();
  const rows = [];
  await table
    .select({ pageSize: 100, maxRecords })
    .eachPage((page, next) => {
      rows.push(...page);
      next();
    });
  return rows.map(airtableRecordToVenue);
}

/**
 * Upsert venue with dedupe. Fail-closed to NEEDS_REVIEW on uncertain collision.
 */
export async function upsertVenue(rawVenue, { dryRun = true, existingCache = null } = {}) {
  const entity = buildVenueEntity(rawVenue);
  if (!entity.venueName) {
    return {
      ok: false,
      error: "missing_venue_name",
      venueId: null,
    };
  }

  let existingList = existingCache;
  if (!existingList) {
    existingList = dryRun && !isPeAirtableConfigured()
      ? []
      : await listVenuesFromAirtable();
  }

  // Also probe by Venue ID / domain when live
  if (isPeAirtableConfigured() && !dryRun) {
    const byId = await findByField(venuesTable(), F.venueId, entity.venueId);
    for (const rec of byId) {
      const v = airtableRecordToVenue(rec);
      if (!existingList.find((e) => e.venueId === v.venueId)) existingList.push(v);
    }
  }

  const matchResult = findVenueMatch(entity, existingList);

  if (matchResult.needsReview) {
    const fields = venueToAirtableFields(entity, {
      researchStatus: "NEEDS_REVIEW",
      canonicalStatus: "NEEDS_REVIEW",
      matchedBy: matchResult.matchedBy,
      matchConfidence: matchResult.matchConfidence,
      mergeReason: matchResult.mergeReason,
    });
    return {
      ok: true,
      action: "needs_review",
      duplicatesPrevented: 0,
      needsReview: true,
      venueId: entity.venueId,
      fields,
      dryRun,
      conflictWith: matchResult.conflictWith?.venueId || null,
    };
  }

  if (matchResult.match) {
    const prior = matchResult.match;
    const merged = buildVenueEntity(rawVenue, prior);
    const fields = venueToAirtableFields(merged, {
      firstSeen: prior.firstSeen || nowIso(),
      researchStatus: prior.researchStatus || "PARTIALLY_RESEARCHED",
      matchedBy: matchResult.matchedBy,
      matchConfidence: matchResult.matchConfidence,
      mergeReason: matchResult.mergeReason,
    });
    // Preserve firstSeen — strip if update would reset? already set via meta
    const result = await createOrUpdate(dryRun ? null : venuesTable(), {
      recordId: prior.airtableRecordId || null,
      fields,
      dryRun,
    });
    // If dry-run without record id, still count as update when matched
    return {
      ok: true,
      action: "update",
      duplicatesPrevented: 1,
      needsReview: false,
      venueId: merged.venueId,
      priorVenueId: prior.venueId,
      matchedBy: matchResult.matchedBy,
      matchConfidence: matchResult.matchConfidence,
      fields,
      ...result,
      reused: true,
    };
  }

  const fields = venueToAirtableFields(entity, {
    researchStatus: "DISCOVERED",
    firstSeen: nowIso(),
  });
  const result = await createOrUpdate(dryRun ? null : venuesTable(), {
    recordId: null,
    fields,
    dryRun,
  });
  if (!dryRun && result.record) {
    entity.airtableRecordId = result.recordId;
  }
  return {
    ok: true,
    action: "create",
    duplicatesPrevented: 0,
    needsReview: false,
    venueId: entity.venueId,
    fields,
    reused: false,
    ...result,
  };
}

export function computeStableVenueId(input) {
  return computeVenueId(input);
}

/** Offline identity hash for tests without crypto collision risk. */
export function venueIdentityFingerprint(v) {
  const seed = [
    domainOf(v) || "",
    normName(v.venueName),
    String(v.lat ?? ""),
    String(v.long ?? ""),
  ].join("|");
  return createHash("sha256").update(seed).digest("hex").slice(0, 12);
}

export { PE_VENUES_TABLE_NAME, F as PE_VENUE_FIELDS };
