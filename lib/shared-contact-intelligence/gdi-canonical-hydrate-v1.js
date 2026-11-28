/**
 * Hydrate GDI primaryContact projections from the shared canonical resolver.
 * Does not invent channels. Does not overwrite stronger verified values with weaker ones.
 */

import {
  resolveCanonicalPersonChannels,
  toGdiContactPresentation,
} from "./canonical-channel-resolver-v1.js";
import { resolveCanonicalPersonId, loadPersonIdMap } from "./canonical-person-identity-v1.js";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function strength(email) {
  if (!email) return 0;
  if (/^(info|contact|sales|hello|admin)@/i.test(email)) return 1;
  return 3;
}

/**
 * Merge canonical durable channels onto a GDI primaryContact object.
 */
export async function hydrateGdiPrimaryContact(primaryContact = {}, opts = {}) {
  const name = text(primaryContact.name || primaryContact.fullName);
  const org = text(
    primaryContact.organization ||
      primaryContact.organizationName ||
      opts.organizationName,
  );
  const map = opts.map || loadPersonIdMap(opts.mapPath);
  const identity = resolveCanonicalPersonId(
    {
      canonicalPersonId: primaryContact.canonicalPersonId || primaryContact.personId,
      displayName: name,
      organizationName: org,
      gdiPersonIdentityKey: primaryContact.personIdentityKey,
      linkedInUrl:
        primaryContact.linkedinUrl ||
        primaryContact.verifiedLinkedIn ||
        primaryContact.linkedInUrl,
    },
    { map },
  );

  const resolved = await resolveCanonicalPersonChannels(
    {
      canonicalPersonId: identity.canonicalPersonId,
      displayName: name,
      organizationName: org,
      hotelId: opts.hotelId,
      hiAirtableId: primaryContact.hiAirtableId,
    },
    { hotelId: opts.hotelId, fixturePeople: opts.fixturePeople, map },
  );

  if (!resolved.ok || !resolved.person) {
    return {
      ok: false,
      primaryContact: {
        ...primaryContact,
        canonicalPersonId: identity.canonicalPersonId || null,
        sharedHydration: {
          status: resolved.status || "UNRESOLVED",
          reason: resolved.reason || identity.reason || null,
        },
      },
      resolved,
    };
  }

  const presentation = toGdiContactPresentation(resolved.person);
  const existingEmail = text(primaryContact.email || primaryContact.primaryContactEmail);
  const canonicalEmail = text(presentation.primaryContactEmail);
  const chooseEmail =
    strength(canonicalEmail) >= strength(existingEmail) ? canonicalEmail || existingEmail : existingEmail;

  const next = {
    ...primaryContact,
    canonicalPersonId: resolved.person.canonicalPersonId,
    personIdentityKey: resolved.person.gdiPersonIdentityKey,
    name: primaryContact.name || presentation.whoToContact,
    role: primaryContact.role || primaryContact.title || resolved.person.title,
    title: primaryContact.title || resolved.person.title,
    email: chooseEmail || null,
    phone:
      text(presentation.primaryContactPhone) ||
      text(primaryContact.phone) ||
      null,
    linkedinUrl:
      text(presentation.verifiedLinkedIn) ||
      text(primaryContact.linkedinUrl || primaryContact.verifiedLinkedIn) ||
      null,
    verifiedLinkedIn:
      text(presentation.verifiedLinkedIn) ||
      text(primaryContact.verifiedLinkedIn) ||
      null,
    sharedHydration: {
      status: "HYDRATED",
      sourceOfTruth: "HI_ORG_CONTACTS_PLUS_HI_CONTACT_CHANNELS",
      airtableLive: Boolean(resolved.airtableLive),
      sessionEnrichmentSeparated: true,
    },
  };

  return { ok: true, primaryContact: next, resolved, presentation };
}

export default { hydrateGdiPrimaryContact };
