/**
 * Durable contact intelligence store (V2.2) — fixture / local-file only.
 *
 * LIVE Airtable writes remain DISABLED. This store exists so cross-week reuse,
 * employer-change, expiry, correction, and deletion can be tested without
 * mutating production schema or storing unauthorized provider payloads.
 *
 * Distinguishes:
 *   PROCESS_LOCAL_CACHE — in-memory workflow Map (cleared each process)
 *   DURABLE_FIXTURE_STORE — file-backed person index (tests / authorized sims)
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { publicContactPersonKey } from "./gdi-public-contact-verification-v1.js";
import { GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED } from "./gdi-public-contact-persistence-readiness-v1.js";
import {
  buildIdentityDisambiguationFingerprint,
  decideDurablePersonUpsert,
  durablePersonRecordKey,
  IDENTITY_MATCH,
} from "./gdi-person-identity-collision-v1.js";

export const GDI_DURABLE_CONTACT_INTELLIGENCE_STORE_V1 =
  "gdi-durable-contact-intelligence-store-v1";

/** Hard off for any path that would write live Airtable contact rows. */
export const GDI_DURABLE_CONTACT_LIVE_AIRTABLE_WRITE_ENABLED = false;

export const CHANNEL_KIND = Object.freeze({
  VERIFIED_DIRECT_WORK_EMAIL: "VERIFIED_DIRECT_WORK_EMAIL",
  VERIFIED_DIRECT_BUSINESS_PHONE: "VERIFIED_DIRECT_BUSINESS_PHONE",
  VERIFIED_ORGANIZATION_EMAIL_ROUTE: "VERIFIED_ORGANIZATION_EMAIL_ROUTE",
  VERIFIED_ORGANIZATION_PHONE_ROUTE: "VERIFIED_ORGANIZATION_PHONE_ROUTE",
});

export const REUSE_LAYER = Object.freeze({
  PROCESS_LOCAL_CACHE: "PROCESS_LOCAL_CACHE",
  DURABLE_FIXTURE_STORE: "DURABLE_FIXTURE_STORE",
  NONE: "NONE",
});

const PROVIDER_ORIGIN_RE =
  /APOLLO|PROSPEO|PDL|PEOPLE.?DATA.?LABS|SURFE|FULLENRICH|FINDYMAIL|PROVIDER_ENRICHED|PROVIDER_EPHEMERAL/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function emptyDoc() {
  return {
    version: GDI_DURABLE_CONTACT_INTELLIGENCE_STORE_V1,
    liveAirtableWrites: false,
    people: {},
    auditLog: [],
  };
}

export function createDurableContactStore(opts = {}) {
  const storePath = opts.storePath || null;
  let doc = emptyDoc();

  if (storePath && fs.existsSync(storePath) && opts.load !== false) {
    try {
      doc = { ...emptyDoc(), ...JSON.parse(fs.readFileSync(storePath, "utf8")) };
      doc.people = doc.people || {};
      doc.auditLog = Array.isArray(doc.auditLog) ? doc.auditLog : [];
    } catch {
      doc = emptyDoc();
    }
  }

  function persist() {
    if (!storePath || opts.persist === false) return;
    fs.mkdirSync(path.dirname(storePath), { recursive: true });
    fs.writeFileSync(storePath, JSON.stringify(doc, null, 2));
  }

  function audit(action, detail = {}) {
    doc.auditLog.push({
      at: nowIso(),
      action,
      ...detail,
    });
  }

  function assertNoLiveAirtable() {
    if (
      GDI_DURABLE_CONTACT_LIVE_AIRTABLE_WRITE_ENABLED === true ||
      GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED === true ||
      opts.authorizeLiveAirtableWrite === true
    ) {
      throw new Error(
        "LIVE_AIRTABLE_CONTACT_WRITE_REFUSED — durable store is fixture-only until founder authorizes",
      );
    }
  }

  function refuseProviderOrigin(origin) {
    return PROVIDER_ORIGIN_RE.test(text(origin));
  }

  /**
   * Upsert a verified public-source person record (fixture store only).
   */
  function upsertPerson(input = {}) {
    assertNoLiveAirtable();
    const name = text(input.name);
    const organization = text(input.organization);
    const groupingKey =
      text(input.personIdentityKey) ||
      publicContactPersonKey({ name, organization });
    if (!groupingKey || !name || !organization) {
      return { ok: false, reason: "MISSING_IDENTITY", personIdentityKey: null };
    }
    if (refuseProviderOrigin(input.origin || input.contactDataOrigin)) {
      return {
        ok: false,
        reason: "PROVIDER_DATA_PERSISTENCE_PROHIBITED",
        personIdentityKey: groupingKey,
      };
    }
    if (!input.permissionToRetain) {
      return {
        ok: false,
        reason: "RETENTION_NOT_PERMITTED",
        personIdentityKey: groupingKey,
      };
    }

    const fingerprint =
      text(input.disambiguationFingerprint) ||
      buildIdentityDisambiguationFingerprint({
        professionalProfileUrl: input.professionalProfileUrl,
        organizationalProfileUrl: input.organizationalProfileUrl,
        personSpecificSourceUrl: input.sourceUrl || input.personSpecificSourceUrl,
        employerDomain: input.organizationDomain,
        verifiedRole: input.role,
        externalStableId: input.externalStableId,
      });

    const incomingIdentity = {
      name,
      organization,
      professionalProfileUrl: input.professionalProfileUrl || null,
      personSpecificSourceUrl: input.sourceUrl || input.personSpecificSourceUrl || null,
      disambiguationFingerprint: fingerprint,
      organizationDomain: input.organizationDomain || null,
      verifiedRole: input.role || null,
    };

    // Collision gate: never auto-merge same name+org without evidence.
    const sameGroupRecords = Object.values(doc.people).filter(
      (p) =>
        p.groupingKey === groupingKey ||
        p.personIdentityKey === groupingKey ||
        String(p.personIdentityKey || "").startsWith(`${groupingKey}::`),
    );
    let forceRecordKey = null;
    for (const existingCandidate of sameGroupRecords) {
      const decision = decideDurablePersonUpsert(
        {
          ...existingCandidate,
          sourceUrl: existingCandidate.evidence?.sourceUrl,
          personSpecificSourceUrl: existingCandidate.evidence?.sourceUrl,
        },
        incomingIdentity,
      );
      if (
        decision.action === "UPDATE_SAME_PERSON" &&
        decision.mergeAllowed
      ) {
        forceRecordKey = existingCandidate.personIdentityKey;
        break;
      }
      if (
        decision.action === "CREATE_COLLISION_BRANCH" &&
        decision.match?.match === IDENTITY_MATCH.DISTINCT_PERSONS
      ) {
        // Distinct person under same grouping key — continue to branched key.
        continue;
      }
      if (decision.action === "REFUSE_AMBIGUOUS") {
        // If this is the sole same-group record and incoming carries no new
        // conflicting evidence, allow in-place update of that record only when
        // caller passes allowAmbiguousSelfUpdate + matching record key.
        if (
          input.allowAmbiguousSelfUpdate === true &&
          text(input.existingRecordKey) ===
            existingCandidate.personIdentityKey
        ) {
          forceRecordKey = existingCandidate.personIdentityKey;
          break;
        }
        return {
          ok: false,
          reason: "AMBIGUOUS_IDENTITY_UNRESOLVED",
          personIdentityKey: groupingKey,
          match: decision.match,
        };
      }
    }

    const personKey =
      forceRecordKey ||
      durablePersonRecordKey({
        name,
        organization,
        disambiguationFingerprint: fingerprint,
        professionalProfileUrl: input.professionalProfileUrl,
        personSpecificSourceUrl: input.sourceUrl,
        employerDomain: input.organizationDomain,
        verifiedRole: input.role,
        externalStableId: input.externalStableId,
      });

    const existing = doc.people[personKey] || null;
    const channels = {
      ...(existing?.channels || {}),
      ...(input.channels || {}),
    };
    // Never keep provider-marked channels
    for (const [k, ch] of Object.entries(channels)) {
      if (refuseProviderOrigin(ch?.origin)) delete channels[k];
    }

    const record = {
      personIdentityKey: personKey,
      groupingKey,
      disambiguationFingerprint: fingerprint || null,
      name,
      organization,
      role: text(input.role) || existing?.role || null,
      organizationDomain:
        text(input.organizationDomain) || existing?.organizationDomain || null,
      professionalProfileUrl:
        text(input.professionalProfileUrl) ||
        existing?.professionalProfileUrl ||
        null,
      channels,
      evidence: {
        sourceUrl: text(input.sourceUrl) || existing?.evidence?.sourceUrl || null,
        verifiedAt:
          text(input.verifiedAt) || existing?.evidence?.verifiedAt || nowIso(),
        origin: text(input.origin) || "PUBLIC_SOURCE",
      },
      rights: {
        permissionToRetain: input.permissionToRetain === true,
        permissionToDisplayToHotelClients:
          input.permissionToDisplayToHotelClients === true,
        storageAuthorizationClass:
          text(input.storageAuthorizationClass) ||
          existing?.rights?.storageAuthorizationClass ||
          "INTERNAL_RESTRICTED_PUBLIC_SOURCE",
        retentionPolicyDays: Number(input.retentionPolicyDays || 365),
        revalidationRequiredAfterDays: Number(
          input.revalidationRequiredAfterDays || 365,
        ),
      },
      associations: {
        opportunityIds: [
          ...new Set(
            [
              ...(existing?.associations?.opportunityIds || []),
              ...(input.opportunityIds || []),
            ].filter(Boolean),
          ),
        ],
        hotelIds: [
          ...new Set(
            [
              ...(existing?.associations?.hotelIds || []),
              ...(input.hotelIds || []),
            ].filter(Boolean),
          ),
        ],
      },
      employmentHistory: Array.isArray(existing?.employmentHistory)
        ? [...existing.employmentHistory]
        : [],
      invalid: existing?.invalid === true ? true : false,
      invalidReason: existing?.invalidReason || null,
      createdAt: existing?.createdAt || nowIso(),
      updatedAt: nowIso(),
    };

    const created = !existing;
    doc.people[personKey] = record;
    audit(created ? "CREATE" : "UPDATE", {
      personIdentityKey: personKey,
      groupingKey,
      opportunityIds: record.associations.opportunityIds,
      hotelIds: record.associations.hotelIds,
    });
    persist();
    return {
      ok: true,
      created,
      personIdentityKey: personKey,
      groupingKey,
      record,
    };
  }

  function getPerson(personKey) {
    const key = text(personKey);
    if (!key) return null;
    if (doc.people[key]) return doc.people[key];
    // Allow lookup by grouping key when exactly one record exists (incl. invalid)
    const groupMatches = Object.values(doc.people).filter(
      (p) =>
        p.groupingKey === key ||
        p.personIdentityKey === key ||
        String(p.personIdentityKey || "").startsWith(`${key}::`),
    );
    if (groupMatches.length === 1) return groupMatches[0];
    // Prefer the sole non-invalid match when multiple history rows share a group
    const live = groupMatches.filter((p) => !p.invalid);
    if (live.length === 1) return live[0];
    return null;
  }

  function findByNameOrg(name, organization) {
    const key = publicContactPersonKey({ name, organization });
    return key ? getPerson(key) : null;
  }

  /**
   * Record employer change: prior key stays as history; new key for new org.
   */
  function recordEmployerChange({
    priorPersonIdentityKey,
    name,
    newOrganization,
    role,
    sourceUrl,
    permissionToRetain = true,
    permissionToDisplayToHotelClients = false,
  } = {}) {
    assertNoLiveAirtable();
    const prior = getPerson(priorPersonIdentityKey);
    if (!prior) return { ok: false, reason: "PRIOR_PERSON_NOT_FOUND" };
    const newKey = publicContactPersonKey({
      name: name || prior.name,
      organization: newOrganization,
    });
    if (!newKey) return { ok: false, reason: "NEW_KEY_FAILED" };
    if (newKey === priorPersonIdentityKey) {
      return { ok: false, reason: "ORGANIZATION_UNCHANGED", personIdentityKey: newKey };
    }

    prior.employmentHistory = [
      ...(prior.employmentHistory || []),
      {
        at: nowIso(),
        event: "EMPLOYER_CHANGE_SUPERSEDED",
        fromOrganization: prior.organization,
        toOrganization: text(newOrganization),
        successorPersonIdentityKey: newKey,
      },
    ];
    prior.invalid = true;
    prior.invalidReason = "EMPLOYER_CHANGED";
    prior.updatedAt = nowIso();
    doc.people[priorPersonIdentityKey] = prior;
    audit("EMPLOYER_CHANGE_INVALIDATE", {
      personIdentityKey: priorPersonIdentityKey,
      successorPersonIdentityKey: newKey,
    });

    const created = upsertPerson({
      personIdentityKey: newKey,
      name: name || prior.name,
      organization: newOrganization,
      role: role || prior.role,
      sourceUrl: sourceUrl || prior.evidence?.sourceUrl,
      origin: "PUBLIC_SOURCE",
      permissionToRetain,
      permissionToDisplayToHotelClients,
      channels: {}, // channels must be re-verified under new employer
      opportunityIds: [],
      hotelIds: [],
      retentionPolicyDays: prior.rights?.retentionPolicyDays,
    });
    if (created.ok && created.record) {
      created.record.employmentHistory = [
        {
          at: nowIso(),
          event: "EMPLOYER_CHANGE_SUCCESSOR",
          fromOrganization: prior.organization,
          priorPersonIdentityKey,
        },
      ];
      doc.people[newKey] = created.record;
      persist();
    }
    return {
      ok: true,
      priorPersonIdentityKey,
      newPersonIdentityKey: newKey,
      priorInvalidated: true,
      channelsCleared: true,
      upsert: created,
    };
  }

  function isFresh(person, asOf = new Date()) {
    if (!person || person.invalid) return false;
    const verifiedAt = person.evidence?.verifiedAt;
    if (!verifiedAt) return false;
    const days = Number(person.rights?.revalidationRequiredAfterDays || 365);
    const verifiedMs = Date.parse(verifiedAt);
    if (!Number.isFinite(verifiedMs)) return false;
    const ageDays = (asOf.getTime() - verifiedMs) / (86400 * 1000);
    return ageDays <= days;
  }

  function expireStale(asOf = new Date()) {
    assertNoLiveAirtable();
    let expired = 0;
    for (const [key, person] of Object.entries(doc.people)) {
      if (person.invalid) continue;
      if (!isFresh(person, asOf)) {
        person.invalid = true;
        person.invalidReason = "EVIDENCE_STALE_REVALIDATION_REQUIRED";
        person.updatedAt = nowIso();
        doc.people[key] = person;
        audit("EXPIRE", { personIdentityKey: key });
        expired += 1;
      }
    }
    persist();
    return { ok: true, expired };
  }

  function correctChannel(personKey, channelKind, nextChannel) {
    assertNoLiveAirtable();
    const person = getPerson(personKey);
    if (!person) return { ok: false, reason: "NOT_FOUND" };
    const recordKey = person.personIdentityKey;
    if (refuseProviderOrigin(nextChannel?.origin)) {
      return { ok: false, reason: "PROVIDER_DATA_PERSISTENCE_PROHIBITED" };
    }
    const prev = person.channels?.[channelKind] || null;
    person.channels = { ...(person.channels || {}), [channelKind]: nextChannel };
    person.updatedAt = nowIso();
    doc.people[recordKey] = person;
    audit("CORRECT_CHANNEL", {
      personIdentityKey: recordKey,
      channelKind,
      previousValuePresent: Boolean(prev?.value),
    });
    persist();
    return { ok: true, record: person };
  }

  function deletePerson(personKey, reason = "FOUNDER_OR_OPERATOR_DELETE") {
    assertNoLiveAirtable();
    const person = getPerson(personKey);
    if (!person) return { ok: false, reason: "NOT_FOUND" };
    const recordKey = person.personIdentityKey;
    delete doc.people[recordKey];
    audit("DELETE", { personIdentityKey: recordKey, reason });
    persist();
    return { ok: true, deleted: true, personIdentityKey: recordKey };
  }

  function associateOpportunity(personKey, { opportunityId, hotelId } = {}) {
    assertNoLiveAirtable();
    const person = getPerson(personKey);
    if (!person) return { ok: false, reason: "NOT_FOUND" };
    const recordKey = person.personIdentityKey;
    if (opportunityId && !person.associations.opportunityIds.includes(opportunityId)) {
      person.associations.opportunityIds.push(opportunityId);
    }
    if (hotelId && !person.associations.hotelIds.includes(hotelId)) {
      person.associations.hotelIds.push(hotelId);
    }
    person.updatedAt = nowIso();
    doc.people[recordKey] = person;
    audit("ASSOCIATE", {
      personIdentityKey: recordKey,
      opportunityId: opportunityId || null,
      hotelId: hotelId || null,
    });
    persist();
    return { ok: true, record: person };
  }

  function listPeople() {
    return Object.values(doc.people);
  }

  function getAuditLog() {
    return [...doc.auditLog];
  }

  function snapshot() {
    return JSON.parse(JSON.stringify(doc));
  }

  return {
    version: GDI_DURABLE_CONTACT_INTELLIGENCE_STORE_V1,
    upsertPerson,
    getPerson,
    findByNameOrg,
    recordEmployerChange,
    isFresh,
    expireStale,
    correctChannel,
    deletePerson,
    associateOpportunity,
    listPeople,
    getAuditLog,
    snapshot,
    assertNoLiveAirtable,
  };
}

export function hashNonPiiChannelPresence(channels = {}) {
  const flags = Object.keys(channels || {})
    .sort()
    .map((k) => `${k}:${channels[k]?.kind || k}:${Boolean(channels[k]?.value)}`)
    .join("|");
  return createHash("sha256").update(flags).digest("hex").slice(0, 12);
}

export default {
  GDI_DURABLE_CONTACT_INTELLIGENCE_STORE_V1,
  GDI_DURABLE_CONTACT_LIVE_AIRTABLE_WRITE_ENABLED,
  CHANNEL_KIND,
  REUSE_LAYER,
  createDurableContactStore,
  hashNonPiiChannelPresence,
};
