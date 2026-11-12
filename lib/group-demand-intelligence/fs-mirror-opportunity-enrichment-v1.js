/**
 * FS mirror enrichment for Airtable-primary GDI reads.
 *
 * Airtable opportunityPayloadJson can lag filesystem repairs (named contacts,
 * revenue-stream class, guestroom/event theses, commercial titles). When the
 * FS mirror holds stronger verified presentation fields, hydrate them at read
 * time — hotel-agnostic, never invents contacts or demand.
 *
 * Does not write Airtable or opportunities.json.
 */

import * as fsRepo from "./repository.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import { HOTEL_REVENUE_STREAM_CLASS } from "./multi-revenue-qualification-v1.js";

export const FS_MIRROR_OPPORTUNITY_ENRICHMENT_V1 =
  "gdi-fs-mirror-opportunity-enrichment-v1";

const PLACEHOLDER_NAME_RE =
  /^(unknown|n\/?a|tbd|none|null|buyer not yet identified|contact research required|organization contact desk|tournaments?\s+desk|meetings?\s+desk|housing\s+desk)$/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function oppId(o) {
  return text(o?.id || o?.opportunityId);
}

export function isNamedVerifiedContact(contact) {
  if (!contact || typeof contact !== "object") return false;
  const name = text(contact.name || contact.contactName);
  if (!name || PLACEHOLDER_NAME_RE.test(name)) return false;
  if (!looksLikePersonName(name)) return false;
  return true;
}

function contactScore(contact) {
  if (!isNamedVerifiedContact(contact)) return 0;
  let score = 10;
  if (text(contact.email)) score += 3;
  if (text(contact.phone)) score += 1;
  if (text(contact.role || contact.title || contact.professionalTitle)) score += 2;
  if (text(contact.whyRelevant || contact.whyThisContact)) score += 1;
  return score;
}

function hasConfirmedRevenueStream(cls) {
  const c = text(cls);
  if (!c || c === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED) {
    return false;
  }
  return (
    c === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY ||
    c === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY ||
    c === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB ||
    c === "GUESTROOMS_ONLY" ||
    c === "EVENT_FNB_ONLY" ||
    c === "COMBINED_GUESTROOMS_EVENT_FNB"
  );
}

function titleLooksLikeResearchNote(title) {
  const t = text(title);
  if (!t) return true;
  return (
    /next date TBD|destination TBD|recommended-hotel|hotel information forth|recurring Bethesda on-site pattern|monitor for|parents weekend demand —|unconfirmed this pass|exact next dates|verified via|hotel not named/i.test(
      t,
    ) || t.length > 90
  );
}

function preferTitle(airtableTitle, fsTitle) {
  const at = text(airtableTitle);
  const fs = text(fsTitle);
  if (!fs) return null;
  if (!at) return fs;
  if (titleLooksLikeResearchNote(at) && !titleLooksLikeResearchNote(fs)) {
    return fs;
  }
  if (fs.length + 12 < at.length && !titleLooksLikeResearchNote(fs)) {
    return fs;
  }
  return null;
}

/**
 * Merge one Airtable (or canonical) row with its FS mirror peer.
 * @returns {{ opportunity: object, patches: string[] }}
 */
export function mergeFsMirrorFieldsIntoOpportunity(airtableOpp, fsOpp) {
  if (!airtableOpp || typeof airtableOpp !== "object") {
    return { opportunity: airtableOpp, patches: [] };
  }
  if (!fsOpp || typeof fsOpp !== "object") {
    return { opportunity: airtableOpp, patches: [] };
  }

  const next = { ...airtableOpp };
  const patches = [];

  const atContact = airtableOpp.primaryContact;
  const fsContact = fsOpp.primaryContact;
  if (contactScore(fsContact) > contactScore(atContact)) {
    next.primaryContact = { ...fsContact };
    if (fsOpp.primaryContactName) next.primaryContactName = fsOpp.primaryContactName;
    if (fsOpp.primaryContactRole) next.primaryContactRole = fsOpp.primaryContactRole;
    if (fsOpp.primaryContactEmail) next.primaryContactEmail = fsOpp.primaryContactEmail;
    if (fsOpp.contactQuality) next.contactQuality = fsOpp.contactQuality;
    patches.push("primaryContact");
  }

  if (
    Array.isArray(fsOpp.backupContacts) &&
    fsOpp.backupContacts.length > 0 &&
    (!Array.isArray(airtableOpp.backupContacts) ||
      airtableOpp.backupContacts.length === 0)
  ) {
    next.backupContacts = fsOpp.backupContacts.map((c) =>
      c && typeof c === "object" ? { ...c } : c,
    );
    patches.push("backupContacts");
  }

  if (
    hasConfirmedRevenueStream(fsOpp.revenueStreamClass) &&
    !hasConfirmedRevenueStream(airtableOpp.revenueStreamClass)
  ) {
    next.revenueStreamClass = fsOpp.revenueStreamClass;
    patches.push("revenueStreamClass");
  }

  if (text(fsOpp.guestroomThesis) && !text(airtableOpp.guestroomThesis)) {
    next.guestroomThesis = fsOpp.guestroomThesis;
    patches.push("guestroomThesis");
  }
  if (text(fsOpp.eventFnBThesis) && !text(airtableOpp.eventFnBThesis)) {
    next.eventFnBThesis = fsOpp.eventFnBThesis;
    patches.push("eventFnBThesis");
  }
  if (
    text(fsOpp.localCateringThesis) &&
    !text(airtableOpp.localCateringThesis)
  ) {
    next.localCateringThesis = fsOpp.localCateringThesis;
    patches.push("localCateringThesis");
  }

  const betterTitle = preferTitle(airtableOpp.title, fsOpp.title);
  if (betterTitle) {
    next.title = betterTitle;
    patches.push("title");
  }
  const betterDisplay = preferTitle(
    airtableOpp.displayTitle || airtableOpp.title,
    fsOpp.displayTitle || fsOpp.title,
  );
  if (betterDisplay && betterDisplay !== text(next.displayTitle)) {
    next.displayTitle = betterDisplay;
    patches.push("displayTitle");
  }

  if (patches.length) {
    next.fsMirrorEnrichmentV1 = {
      version: FS_MIRROR_OPPORTUNITY_ENRICHMENT_V1,
      patches,
      enrichedAt: new Date().toISOString(),
    };
  }

  return { opportunity: next, patches };
}

/**
 * Hydrate an opportunity list from the hotel FS mirror when present.
 */
export function enrichOpportunitiesFromFsMirror(hotelId, opportunities = []) {
  const id = text(hotelId);
  if (!id) {
    return {
      opportunities: opportunities || [],
      enrichedCount: 0,
      patchCounts: {},
    };
  }

  let fsDoc;
  try {
    fsDoc = fsRepo.loadOpportunities(id);
  } catch {
    return {
      opportunities: opportunities || [],
      enrichedCount: 0,
      patchCounts: {},
      fsMissing: true,
    };
  }

  const byId = new Map();
  for (const row of fsDoc?.opportunities || []) {
    const oid = oppId(row);
    if (oid) byId.set(oid, row);
  }
  if (byId.size === 0) {
    return {
      opportunities: opportunities || [],
      enrichedCount: 0,
      patchCounts: {},
      fsEmpty: true,
    };
  }

  const patchCounts = {};
  let enrichedCount = 0;
  const next = (opportunities || []).map((row) => {
    const fsRow = byId.get(oppId(row));
    if (!fsRow) return row;
    const merged = mergeFsMirrorFieldsIntoOpportunity(row, fsRow);
    if (merged.patches.length) {
      enrichedCount += 1;
      for (const p of merged.patches) {
        patchCounts[p] = (patchCounts[p] || 0) + 1;
      }
    }
    return merged.opportunity;
  });

  return { opportunities: next, enrichedCount, patchCounts };
}
