/**
 * Canonical Channel Resolver V1 — shared GDI + Hotel Explorer SoT reader.
 *
 * Reads eligible verified channels from HI Contact Channels via HI Org Contacts.
 * Never invents emails. Never promotes Surfe / switchboard / org mailboxes.
 * Does not convert organizational channels into personal direct channels.
 */

import {
  readHiGraphForHotel,
  listOrgContactsForOrganizations,
  mapHiContactChannelRecord,
  listContactChannelsByIds,
} from "../hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";
import {
  resolveCanonicalPersonId,
  gdiPersonIdentityKey,
  loadPersonIdMap,
  upsertPersonIdMapEntry,
} from "./canonical-person-identity-v1.js";
import { SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1 } from "./architecture-constants-v1.js";

export const CANONICAL_CHANNEL_RESOLVER_V1 = "canonical-channel-resolver-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function extractLinkedInFromNotes(notes) {
  const m = String(notes || "").match(
    /https?:\/\/(?:www\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i,
  );
  return m ? m[0].replace(/\/$/, "") + "/" : null;
}

function preferOrgContactRows(contacts = []) {
  const byName = new Map();
  for (const p of contacts || []) {
    if (!p?.displayName) continue;
    const key = text(p.displayName).toLowerCase();
    const rank =
      (String(p.verificationState || "").toLowerCase() === "validated" ? 2 : 0) +
      (p.organizationId ? 1 : 0) +
      (extractLinkedInFromNotes(p.notes) ? 1 : 0) +
      (Array.isArray(p.channels) && p.channels.length ? 2 : 0);
    const prev = byName.get(key);
    if (!prev || rank > prev.rank) byName.set(key, { p, rank });
  }
  return [...byName.values()].map((x) => x.p);
}

/**
 * Normalize a hydrate-safe channel DTO for both products.
 */
export function toSharedChannelDto(raw, person) {
  if (!raw?.value) return null;
  const kind = text(raw.kind);
  let channelType = null;
  if (/^email$/i.test(kind)) channelType = "Email";
  else if (/^telephone$/i.test(kind) || /phone/i.test(kind)) channelType = "Telephone";
  else if (/linkedin/i.test(kind)) channelType = "LinkedIn";
  else return null;

  return {
    canonicalPersonId: person?.personId || person?.canonicalPersonId || null,
    channelType,
    channelValue: raw.value,
    provider: raw.provider || null,
    evidence: {
      sourceName: raw.sourceName || null,
      sourceUrl: raw.sourceUrl || null,
      source: raw.source || "HI_CONTACT_CHANNELS",
    },
    verificationStatus: raw.verificationStatus || null,
    verificationDate: raw.lastCheckedAt || raw.observedAt || null,
    classification: raw.classification || null,
    retentionReuseEligible: true,
    licenseRetentionClass: raw.licenseRetentionClass || null,
    canonicalAirtableRecordId: raw.airtableId || null,
    sessionOnly: false,
    surfeDerived: false,
  };
}

function personSharedDto(p, opts = {}) {
  const linkedInNotes = extractLinkedInFromNotes(p.notes);
  const channels = [];
  const seen = new Set();
  for (const raw of p.channels || []) {
    const dto = toSharedChannelDto(raw, p);
    if (!dto) continue;
    const key = `${dto.channelType}|${dto.channelValue.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    channels.push(dto);
  }
  if (linkedInNotes && !channels.some((c) => c.channelType === "LinkedIn")) {
    channels.push({
      canonicalPersonId: p.personId || null,
      channelType: "LinkedIn",
      channelValue: linkedInNotes,
      provider: null,
      evidence: {
        sourceName: "HI_ORG_CONTACTS_NOTES",
        sourceUrl: linkedInNotes,
        source: "HI_ORG_CONTACTS_NOTES",
      },
      verificationStatus: "Public Listed",
      verificationDate: null,
      classification: "Public",
      retentionReuseEligible: true,
      licenseRetentionClass: null,
      canonicalAirtableRecordId: p.airtableId || null,
      sessionOnly: false,
      surfeDerived: false,
      notesFallback: true,
    });
  }

  const email = channels.find((c) => c.channelType === "Email") || null;
  const phone = channels.find((c) => c.channelType === "Telephone") || null;
  const linkedin = channels.find((c) => c.channelType === "LinkedIn") || null;
  const gdiKey = gdiPersonIdentityKey({
    name: p.displayName,
    organization: opts.organizationName || p.organizationId,
  });

  return {
    version: CANONICAL_CHANNEL_RESOLVER_V1,
    canonicalPersonId: p.personId || null,
    hiAirtableId: p.airtableId || null,
    displayName: p.displayName || null,
    title: p.title || null,
    organizationId: p.organizationId || null,
    organizationName: opts.organizationName || null,
    functionRole: p.functionRole || null,
    hotelResponsibility: p.hotelResponsibility || null,
    verificationState: p.verificationState || null,
    gdiPersonIdentityKey: gdiKey,
    channels,
    email: email?.channelValue || null,
    phone: phone?.channelValue || null,
    linkedinUrl: linkedin?.channelValue || null,
    directlyContactable: Boolean(email || phone),
    sourceOfTruth: "HI_ORG_CONTACTS_PLUS_HI_CONTACT_CHANNELS",
  };
}

/**
 * Resolve eligible channels for one canonical Person ID (or identity probe).
 */
export async function resolveCanonicalPersonChannels(probe = {}, opts = {}) {
  const identity = resolveCanonicalPersonId(probe, {
    map: opts.map || loadPersonIdMap(opts.mapPath),
    candidates: opts.candidates,
  });

  if (opts.fixturePeople) {
    const people = preferOrgContactRows(opts.fixturePeople).map((p) =>
      personSharedDto(p, { organizationName: p.organizationName }),
    );
    const match = people.find(
      (p) =>
        (identity.canonicalPersonId &&
          p.canonicalPersonId === identity.canonicalPersonId) ||
        (probe.hiAirtableId && p.hiAirtableId === probe.hiAirtableId) ||
        (probe.displayName &&
          text(p.displayName).toLowerCase() === text(probe.displayName).toLowerCase()),
    );
    if (!match) {
      return {
        ok: false,
        status: "NOT_FOUND",
        identity,
        person: null,
        version: CANONICAL_CHANNEL_RESOLVER_V1,
      };
    }
    return {
      ok: true,
      status: "RESOLVED",
      identity,
      person: match,
      version: CANONICAL_CHANNEL_RESOLVER_V1,
      airtableLive: false,
    };
  }

  const token =
    opts.token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || null;
  if (!token) {
    return {
      ok: false,
      status: "BLOCKED",
      reason: "NO_AIRTABLE_CREDENTIALS",
      identity,
      person: null,
      version: CANONICAL_CHANNEL_RESOLVER_V1,
    };
  }

  // Hotel-scoped resolve is preferred when hotelId provided.
  if (opts.hotelId || probe.hotelId) {
    const hotelId = opts.hotelId || probe.hotelId;
    const hotelResult = await resolveCanonicalHotelContacts(hotelId, opts);
    if (!hotelResult.ok) return { ...hotelResult, identity };
    const match = hotelResult.people.find(
      (p) =>
        (identity.canonicalPersonId &&
          p.canonicalPersonId === identity.canonicalPersonId) ||
        (probe.hiAirtableId && p.hiAirtableId === probe.hiAirtableId) ||
        (probe.displayName &&
          text(p.displayName).toLowerCase() ===
            text(probe.displayName).toLowerCase() &&
          (!probe.organizationName ||
            text(p.organizationName).toLowerCase() ===
              text(probe.organizationName).toLowerCase())),
    );
    if (!match) {
      return {
        ok: false,
        status: "NOT_FOUND",
        identity,
        person: null,
        hotelId,
        version: CANONICAL_CHANNEL_RESOLVER_V1,
        airtableLive: hotelResult.airtableLive,
      };
    }
    return {
      ok: true,
      status: "RESOLVED",
      identity,
      person: match,
      hotelId,
      version: CANONICAL_CHANNEL_RESOLVER_V1,
      airtableLive: true,
    };
  }

  return {
    ok: false,
    status: "BLOCKED",
    reason: "HOTEL_OR_FIXTURE_REQUIRED",
    identity,
    person: null,
    version: CANONICAL_CHANNEL_RESOLVER_V1,
  };
}

/**
 * Resolve all preferred unique people + eligible channels for a hotel.
 */
export async function resolveCanonicalHotelContacts(hotelId, opts = {}) {
  const hpcHotelId = text(hotelId);
  if (!hpcHotelId) {
    return {
      ok: false,
      status: "BLOCKED",
      reason: "HOTEL_ID_REQUIRED",
      people: [],
      version: CANONICAL_CHANNEL_RESOLVER_V1,
    };
  }

  if (opts.fixturePeople) {
    const people = preferOrgContactRows(opts.fixturePeople).map((p) =>
      personSharedDto(p, { organizationName: p.organizationName }),
    );
    return {
      ok: true,
      status: "RESOLVED",
      hotelId: hpcHotelId,
      people,
      duplicatePersonRowsCollapsed: Math.max(
        0,
        (opts.fixturePeople || []).length - people.length,
      ),
      version: CANONICAL_CHANNEL_RESOLVER_V1,
      sharedContract: SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
      airtableLive: false,
      sourceOfTruth: "FIXTURE",
    };
  }

  const token =
    opts.token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || null;
  if (!token) {
    return {
      ok: false,
      status: "BLOCKED",
      reason: "NO_AIRTABLE_CREDENTIALS",
      hotelId: hpcHotelId,
      people: [],
      version: CANONICAL_CHANNEL_RESOLVER_V1,
    };
  }

  const graph = await readHiGraphForHotel(hpcHotelId, {
    includeContacts: true,
    includePortfolio: false,
    token,
    env: opts.env,
  });
  if (!graph?.ok) {
    return {
      ok: false,
      status: graph?.error === "NO_AIRTABLE_CREDENTIALS" ? "BLOCKED" : "NOT_TESTED",
      reason: graph?.error || "HI_GRAPH_READ_FAILED",
      hotelId: hpcHotelId,
      people: [],
      version: CANONICAL_CHANNEL_RESOLVER_V1,
      airtableLive: false,
    };
  }

  const orgByCanonical = new Map(
    (graph.organizations || []).map((o) => [String(o.orgId), o]),
  );
  const preferred = preferOrgContactRows(graph.orgContacts || []);
  const people = preferred.map((p) => {
    const org = orgByCanonical.get(String(p.organizationId || "")) || null;
    return personSharedDto(p, {
      organizationName: org
        ? org.tradingName || org.legalName || p.organizationId
        : p.organizationId,
    });
  });

  if (opts.updateIdentityMap) {
    for (const person of people) {
      if (!person.canonicalPersonId) continue;
      upsertPersonIdMapEntry(
        {
          hiPersonId: person.canonicalPersonId,
          hiAirtableId: person.hiAirtableId,
          displayName: person.displayName,
          organizationName: person.organizationName,
          organizationId: person.organizationId,
          gdiPersonIdentityKey: person.gdiPersonIdentityKey,
          linkedInUrl: person.linkedinUrl,
          evidence: ["HI_ORG_CONTACTS_READ"],
        },
        { mapPath: opts.mapPath, write: true },
      );
    }
  }

  return {
    ok: true,
    status: "RESOLVED",
    hotelId: hpcHotelId,
    people,
    rawOrgContactCount: (graph.orgContacts || []).length,
    duplicatePersonRowsCollapsed: Math.max(
      0,
      (graph.orgContacts || []).length - people.length,
    ),
    relationshipCount: graph.relationshipCount || 0,
    version: CANONICAL_CHANNEL_RESOLVER_V1,
    sharedContract: SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
    airtableLive: true,
    sourceOfTruth: "HI_ORG_CONTACTS_PLUS_HI_CONTACT_CHANNELS",
  };
}

/**
 * GDI consumer shape — same channels as HE, separate UI fields.
 */
export function toGdiContactPresentation(person) {
  if (!person) return null;
  return {
    canonicalPersonId: person.canonicalPersonId,
    whoToContact: person.displayName,
    contactDetails: {
      name: person.displayName,
      title: person.title,
      organization: person.organizationName,
      email: person.email,
      phone: person.phone,
      linkedinUrl: person.linkedinUrl,
    },
    primaryContactEmail: person.email,
    primaryContactPhone: person.phone,
    verifiedLinkedIn: person.linkedinUrl,
    channels: person.channels,
    sourceOfTruth: person.sourceOfTruth,
    sessionEnrichmentSeparated: true,
  };
}

/**
 * HE five-column Contact Intelligence row shape.
 */
export function toHeContactDirectoryRow(person) {
  if (!person) return null;
  return {
    personId: person.canonicalPersonId,
    name: person.displayName,
    title: person.title,
    organization: person.organizationName,
    email: person.email,
    phone: person.phone,
    linkedin: person.linkedinUrl,
    channels: person.channels,
    sourceOfTruth: person.sourceOfTruth,
  };
}

/**
 * Cross-product parity: same eligible channel values for same Person ID.
 */
export function assertGdiHeChannelParity(hePerson, gdiPerson) {
  if (!hePerson || !gdiPerson) {
    return { ok: false, status: "FAIL", reason: "MISSING_PERSON" };
  }
  if (
    hePerson.canonicalPersonId &&
    gdiPerson.canonicalPersonId &&
    hePerson.canonicalPersonId !== gdiPerson.canonicalPersonId
  ) {
    return { ok: false, status: "FAIL", reason: "PERSON_ID_MISMATCH" };
  }
  const fields = ["email", "phone", "linkedinUrl"];
  const mismatches = [];
  for (const f of fields) {
    const a = text(hePerson[f]).toLowerCase();
    const b = text(gdiPerson[f]).toLowerCase();
    if (a !== b) mismatches.push({ field: f, he: hePerson[f] || null, gdi: gdiPerson[f] || null });
  }
  if (mismatches.length) {
    return { ok: false, status: "FAIL", reason: "CHANNEL_VALUE_MISMATCH", mismatches };
  }
  return { ok: true, status: "PASS", canonicalPersonId: hePerson.canonicalPersonId };
}

export {
  mapHiContactChannelRecord,
  listContactChannelsByIds,
  listOrgContactsForOrganizations,
  preferOrgContactRows,
};

export default {
  CANONICAL_CHANNEL_RESOLVER_V1,
  resolveCanonicalPersonChannels,
  resolveCanonicalHotelContacts,
  toGdiContactPresentation,
  toHeContactDirectoryRow,
  assertGdiHeChannelParity,
  toSharedChannelDto,
};
