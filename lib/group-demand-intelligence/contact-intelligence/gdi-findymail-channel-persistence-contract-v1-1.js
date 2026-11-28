/**
 * Findymail Contact Intelligence V1.1 — channel persistence + identity contract.
 *
 * Design-only / zero-write by default. No Findymail API. No contact record writes.
 * Reuses HI Org Contacts + HI Contact Channels; preserves GDI personIdentityKey.
 * Surfe remains session-only (out of scope for durable channel writes).
 *
 * Airtable schema (provider-agnostic usage ledger + HI channel licensing fields)
 * was created under gdi-contact-enrichment-airtable-schema-v1 — see
 * `./gdi-contact-enrichment-airtable-schema-v1.js` for live table/field IDs.
 */

import { createHash } from "node:crypto";
import { personIdentityKey } from "../workflow/contact-field-provenance-v1.js";
import {
  GDI_OPS_BASE,
  HI_BASE,
  USAGE_EVENTS_TABLE_NAME,
  USAGE_EVENTS_TABLE_ID_LIVE,
  MAP_USAGE_EVENT,
  MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1,
  mapUsageOutcomeToAirtable,
  mapProviderToAirtable,
  resolveUsageEventsTableId,
} from "./gdi-contact-enrichment-airtable-schema-v1.js";

export const GDI_FINDYMAIL_CHANNEL_PERSISTENCE_V1_1 =
  "gdi-findymail-channel-persistence-contract-v1-1";

/** Live Airtable destinations (schema created; writes still dry-run / inactive). */
export const FINDYMAIL_AIRTABLE_DESTINATIONS_V1_1 = Object.freeze({
  usageEvents: {
    baseId: GDI_OPS_BASE.baseId,
    tableName: USAGE_EVENTS_TABLE_NAME,
    tableId: USAGE_EVENTS_TABLE_ID_LIVE,
    fieldMap: MAP_USAGE_EVENT,
  },
  hiContactChannels: {
    baseId: HI_BASE.baseId,
    tableId: HI_BASE.contactChannelsTableId,
    enrichmentFieldMap: MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1,
  },
  findymailApiEnabled: false,
  surfeDurableEmailStorage: false,
  dryRunDefault: true,
});

export const LICENSE_RETENTION_CLASS = Object.freeze({
  PUBLIC_LISTED: "PUBLIC_LISTED",
  PROVIDER_LICENSED_INTERNAL: "PROVIDER_LICENSED_INTERNAL",
  PROVIDER_LICENSED_CUSTOMER_DURABLE: "PROVIDER_LICENSED_CUSTOMER_DURABLE",
  PROVIDER_LICENSED_EXPORT_OK: "PROVIDER_LICENSED_EXPORT_OK",
  SUPPRESSED: "SUPPRESSED",
  OPT_OUT: "OPT_OUT",
  EXPIRED: "EXPIRED",
});

export const USAGE_OUTCOMES = Object.freeze({
  REUSE_HIT: "REUSE_HIT",
  LOOKUP_FOUND: "LOOKUP_FOUND",
  LOOKUP_MISS: "LOOKUP_MISS",
  LOOKUP_SUPPRESSED: "LOOKUP_SUPPRESSED",
  LOOKUP_STALE_REFRESH: "LOOKUP_STALE_REFRESH",
  LOOKUP_BLOCKED_LICENSE: "LOOKUP_BLOCKED_LICENSE",
  LOOKUP_BLOCKED_DUPLICATE_WINDOW: "LOOKUP_BLOCKED_DUPLICATE_WINDOW",
  LOOKUP_BLOCKED_CUSTOMER_ACCESS: "LOOKUP_BLOCKED_CUSTOMER_ACCESS",
});

/** Default freshness window for licensed provider emails (days). */
export const DEFAULT_FRESHNESS_DAYS = 90;

/** Provider dedupe window to avoid re-billing the same identity (days). */
export const DEFAULT_PROVIDER_DEDUPE_DAYS = 30;

function text(v) {
  return String(v == null ? "" : v)
    .replace(/\s+/g, " ")
    .trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

export function normalizeEmail(value) {
  return lower(value);
}

export function isGenericMailbox(email) {
  const local = normalizeEmail(email).split("@")[0] || "";
  return /^(info|contact|sales|reservations|booking|hello|admin|office|support|enquiries|inquiry|mail|front.?desk)$/i.test(
    local,
  );
}

export function emailDomain(email) {
  const parts = normalizeEmail(email).split("@");
  return parts.length === 2 ? parts[1] : "";
}

/**
 * Channel dedupe: subject + kind + normalized value (matches HI graph pattern).
 */
export function channelDedupeKey({ subjectId, kind = "Email", value } = {}) {
  const s = lower(subjectId);
  const k = lower(kind);
  const v = normalizeEmail(value);
  if (!s || !v) return null;
  return createHash("sha256").update(`${s}|${k}|${v}`).digest("hex").slice(0, 24);
}

/**
 * Affiliation dedupe for HI Org Contacts (person × demand org — not hotel).
 * Never invent hotel ownership affiliation.
 */
export function affiliationDedupeKey({ displayName, organizationId, orgDedupeKey } = {}) {
  const n = lower(displayName);
  const o = lower(organizationId || orgDedupeKey);
  if (!n || !o) return null;
  return createHash("sha256").update(`aff|${n}|${o}`).digest("hex").slice(0, 24);
}

/**
 * Resolve GDI stakeholder identity without requiring hotel ownership.
 * Demand-org affiliation is allowed; hotel–org relationship is NOT required
 * and must not be fabricated.
 */
export function resolveGdiStakeholderIdentity(input = {}) {
  const name = text(input.name || input.displayName);
  const organization = text(input.organization || input.organizationName);
  const domain = lower(input.domain || input.companyDomain);
  if (!name) {
    return {
      ok: false,
      code: "MISSING_PERSON_NAME",
      personIdentityKey: null,
    };
  }
  if (!organization && !domain) {
    return {
      ok: false,
      code: "MISSING_ORG_OR_DOMAIN",
      personIdentityKey: null,
      note: "GDI-only stakeholders still need a demand organization or domain — not a hotel affiliation.",
    };
  }
  const pik = personIdentityKey({ name, organization, domain });
  const orgDedupe = createHash("sha256")
    .update(`org|${lower(organization || domain)}`)
    .digest("hex")
    .slice(0, 16);
  const affKey = affiliationDedupeKey({
    displayName: name,
    organizationId: orgDedupe,
  });
  return {
    ok: true,
    personIdentityKey: pik,
    displayName: name,
    demandOrganizationName: organization || null,
    demandOrganizationDomain: domain || null,
    demandOrganizationDedupeKey: orgDedupe,
    affiliationDedupeKey: affKey,
    personDedupeKey: createHash("sha256")
      .update(`person|${lower(name)}`)
      .digest("hex")
      .slice(0, 16),
    hotelOwnershipRequired: false,
    hotelOrgRelationshipRequired: false,
    hiOrgContactCompatible: Boolean(affKey),
    note:
      "Persist as HI Org Contact affiliated to the DEMAND organization (type Other/Government/Association). Do not create HI Hotel–Org Relationships for GDI event stakeholders.",
  };
}

function daysBetween(isoA, isoB = new Date().toISOString()) {
  const a = Date.parse(isoA);
  const b = Date.parse(isoB);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return Infinity;
  return Math.abs(b - a) / (1000 * 60 * 60 * 24);
}

/**
 * Decide whether an existing channel may be reused (0 provider credits).
 */
export function evaluateChannelReuse(channel = {}, opts = {}) {
  const freshnessDays = Number(opts.freshnessDays ?? DEFAULT_FRESHNESS_DAYS);
  const now = opts.now || new Date().toISOString();
  const license =
    channel.licenseRetentionClass ||
    channel.accessRestriction ||
    LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL;
  const publicationState = channel.publicationState || "INTERNAL_ONLY";
  const verification = channel.verificationStatus || "Unverified";
  const observedAt = channel.lastCheckedAt || channel.observedAt;

  if (
    license === LICENSE_RETENTION_CLASS.SUPPRESSED ||
    license === LICENSE_RETENTION_CLASS.OPT_OUT ||
    verification === "Do Not Publish"
  ) {
    return { reuse: false, reason: "SUPPRESSED_OR_OPT_OUT", license };
  }
  if (license === LICENSE_RETENTION_CLASS.EXPIRED) {
    return { reuse: false, reason: "LICENSE_EXPIRED", license };
  }
  if (opts.requireCustomerAccess === true) {
    const customerOk =
      license === LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_CUSTOMER_DURABLE ||
      license === LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_EXPORT_OK ||
      license === LICENSE_RETENTION_CLASS.PUBLIC_LISTED;
    if (!customerOk || publicationState === "CUSTOMER_BLOCKED") {
      return { reuse: false, reason: "CUSTOMER_ACCESS_RESTRICTED", license };
    }
  }
  if (!channel.channelValue || isGenericMailbox(channel.channelValue)) {
    return { reuse: false, reason: "MISSING_OR_GENERIC_EMAIL", license };
  }
  if (observedAt && daysBetween(observedAt, now) > freshnessDays) {
    return {
      reuse: false,
      reason: "STALE_NEEDS_REFRESH",
      license,
      ageDays: daysBetween(observedAt, now),
    };
  }
  return {
    reuse: true,
    reason: "REUSE_PERMITTED",
    license,
    verification,
    channelValue: normalizeEmail(channel.channelValue),
    credits: 0,
  };
}

/**
 * Idempotent email-channel upsert planner (dry). No provider calls.
 */
export function planEmailChannelUpsert(input = {}, store = {}, opts = {}) {
  const identity = resolveGdiStakeholderIdentity(input);
  if (!identity.ok) {
    return { ok: false, stage: "RESOLVE_IDENTITY", identity, ops: [] };
  }

  const email = normalizeEmail(input.email || input.channelValue);
  const domain = identity.demandOrganizationDomain || emailDomain(email);
  if (email && domain && input.requireEmployerDomainMatch !== false) {
    const claimed = lower(input.expectedDomain || domain);
    if (claimed && emailDomain(email) && emailDomain(email) !== claimed) {
      return {
        ok: false,
        stage: "VERIFY_EMPLOYER_DOMAIN",
        code: "DOMAIN_MISMATCH",
        identity,
        ops: [],
      };
    }
  }

  const subjectId =
    store.existingPersonSubjectId ||
    identity.affiliationDedupeKey ||
    identity.personIdentityKey;
  const dedupe = channelDedupeKey({
    subjectId,
    kind: "Email",
    value: email || input.lookupEmailHint,
  });

  const existingChannels = Array.isArray(store.channels) ? store.channels : [];
  const byDedupe = existingChannels.find((c) => c.dedupeKey === dedupe);
  const byPersonKind = existingChannels.find(
    (c) =>
      c.subjectId === subjectId &&
      lower(c.channelKind) === "email" &&
      c.channelValue,
  );

  const candidate = byDedupe || byPersonKind || null;
  if (candidate) {
    const reuse = evaluateChannelReuse(candidate, opts);
    if (reuse.reuse) {
      return {
        ok: true,
        stage: "REUSE_EXISTING_CHANNEL",
        identity,
        reuse,
        usageEvent: {
          outcome: USAGE_OUTCOMES.REUSE_HIT,
          provider: candidate.provider || "existing",
          creditsCharged: 0,
          personIdentityKeyHash: identity.personIdentityKey,
          hotelId: input.hotelId || null,
          opportunityId: input.opportunityId || null,
        },
        ops: [
          {
            op: "LINK_OPPORTUNITY",
            opportunityId: input.opportunityId || null,
            hotelId: input.hotelId || null,
            channelDedupeKey: candidate.dedupeKey,
          },
        ],
        salesReadyPromotion: false,
      };
    }
    if (reuse.reason === "STALE_NEEDS_REFRESH") {
      // Fall through to authorized lookup / refresh path
    } else if (
      reuse.reason === "SUPPRESSED_OR_OPT_OUT" ||
      reuse.reason === "CUSTOMER_ACCESS_RESTRICTED" ||
      reuse.reason === "LICENSE_EXPIRED"
    ) {
      return {
        ok: false,
        stage: "LICENSE_OR_PRIVACY_GATE",
        code: reuse.reason,
        identity,
        reuse,
        ops: [],
        usageEvent: {
          outcome:
            reuse.reason === "CUSTOMER_ACCESS_RESTRICTED"
              ? USAGE_OUTCOMES.LOOKUP_BLOCKED_CUSTOMER_ACCESS
              : USAGE_OUTCOMES.LOOKUP_SUPPRESSED,
          provider: "none",
          creditsCharged: 0,
          personIdentityKeyHash: identity.personIdentityKey,
        },
        salesReadyPromotion: false,
      };
    }
  }

  // Duplicate lookup suppression window
  const usage = Array.isArray(store.usageEvents) ? store.usageEvents : [];
  const windowDays = Number(opts.providerDedupeDays ?? DEFAULT_PROVIDER_DEDUPE_DAYS);
  const recent = usage.find((u) => {
    if (u.personIdentityKeyHash !== identity.personIdentityKey) return false;
    if (lower(u.provider) !== lower(opts.provider || "findymail")) return false;
    if (!u.at) return false;
    return daysBetween(u.at, opts.now || new Date().toISOString()) <= windowDays;
  });
  if (recent && opts.authorizeProviderLookup === true) {
    return {
      ok: false,
      stage: "DUPLICATE_LOOKUP_SUPPRESSION",
      code: "LOOKUP_BLOCKED_DUPLICATE_WINDOW",
      identity,
      prior: recent,
      ops: [],
      usageEvent: {
        outcome: USAGE_OUTCOMES.LOOKUP_BLOCKED_DUPLICATE_WINDOW,
        provider: opts.provider || "findymail",
        creditsCharged: 0,
        personIdentityKeyHash: identity.personIdentityKey,
      },
      salesReadyPromotion: false,
    };
  }

  if (opts.authorizeProviderLookup !== true) {
    return {
      ok: true,
      stage: "PREPARE_AUTHORIZED_LOOKUP",
      identity,
      ops: [],
      pendingProviderLookup: {
        provider: opts.provider || "findymail",
        name: identity.displayName,
        domain: identity.demandOrganizationDomain,
        personIdentityKey: identity.personIdentityKey,
        authorized: false,
      },
      salesReadyPromotion: false,
      note: "Findymail remains inactive until founder activates subscription + credit ceiling.",
    };
  }

  // Authorized path with a validated provider result (fixture / future live)
  const providerResult = opts.providerResult || null;
  if (!providerResult) {
    return {
      ok: true,
      stage: "AWAITING_PROVIDER_RESULT",
      identity,
      ops: [],
      salesReadyPromotion: false,
    };
  }

  if (providerResult.outcome === "MISS" || !providerResult.email) {
    return {
      ok: true,
      stage: "PROVIDER_MISS",
      identity,
      ops: [],
      usageEvent: {
        outcome: USAGE_OUTCOMES.LOOKUP_MISS,
        provider: opts.provider || "findymail",
        creditsCharged: Number(providerResult.creditsCharged || 0),
        personIdentityKeyHash: identity.personIdentityKey,
        hotelId: input.hotelId || null,
        opportunityId: input.opportunityId || null,
      },
      salesReadyPromotion: false,
      namedPersonPreserved: true,
    };
  }

  const foundEmail = normalizeEmail(providerResult.email);
  if (isGenericMailbox(foundEmail)) {
    return {
      ok: false,
      stage: "VALIDATE_PROVIDER_RESULT",
      code: "GENERIC_MAILBOX_REJECTED",
      identity,
      ops: [],
      salesReadyPromotion: false,
    };
  }

  const channelKey =
    channelDedupeKey({ subjectId, kind: "Email", value: foundEmail }) || dedupe;
  const licenseClass =
    opts.licenseRetentionClass ||
    LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL;

  const ops = [
    {
      op: "UPSERT_HI_ORGANIZATION",
      dryRun: true,
      record: {
        dedupeKey: identity.demandOrganizationDedupeKey,
        legalName: identity.demandOrganizationName,
        organizationType: input.organizationType || "Other",
        officialWebsite: domain ? `https://${domain}` : null,
        publicationState: "INTERNAL_ONLY",
        note: "Demand generator org — NOT hotel ownership",
      },
    },
    {
      op: "UPSERT_HI_ORG_CONTACT",
      dryRun: true,
      record: {
        displayName: identity.displayName,
        organizationDedupeKey: identity.demandOrganizationDedupeKey,
        affiliationDedupeKey: identity.affiliationDedupeKey,
        personDedupeKey: identity.personDedupeKey,
        title: input.title || null,
        affiliationVerified: Boolean(input.affiliationVerified),
        publicationState: "INTERNAL_ONLY",
        gdiPersonIdentityKey: identity.personIdentityKey,
        note: "No HI Hotel–Org Relationship created",
      },
    },
    {
      op: "UPSERT_HI_CONTACT_CHANNEL",
      dryRun: true,
      record: {
        subjectType: "Person",
        subjectId,
        channelKind: "Email",
        channelValue: foundEmail,
        classification: "Direct Personal",
        verificationStatus: "Verified Direct",
        sourceName: "Findymail",
        provider: "Findymail",
        observedAt: opts.now || new Date().toISOString(),
        lastCheckedAt: opts.now || new Date().toISOString(),
        providerRequestId: providerResult.requestId || null,
        licenseRetentionClass: licenseClass,
        accessRestriction: licenseClass,
        publicationState: "CUSTOMER_BLOCKED",
        dedupeKey: channelKey,
        usageProvenance: `findymail:${providerResult.requestId || "unknown"}`,
      },
    },
    {
      op: "LINK_OPPORTUNITY",
      dryRun: true,
      opportunityId: input.opportunityId || null,
      hotelId: input.hotelId || null,
      channelDedupeKey: channelKey,
      projectToGdi: {
        primaryContactEmail: foundEmail,
        preservePrimaryContactName: true,
        customerFacingNamedUnchanged: true,
        salesReadyUnchanged: true,
      },
    },
    {
      op: "RECORD_USAGE_EVENT",
      dryRun: true,
      record: {
        outcome: candidate ? USAGE_OUTCOMES.LOOKUP_STALE_REFRESH : USAGE_OUTCOMES.LOOKUP_FOUND,
        provider: "findymail",
        creditsCharged: Number(providerResult.creditsCharged ?? 1),
        personIdentityKeyHash: identity.personIdentityKey,
        hotelId: input.hotelId || null,
        opportunityId: input.opportunityId || null,
        // no email / name / phone
      },
    },
  ];

  return {
    ok: true,
    stage: "PREPARE_CHANNEL_UPSERT",
    identity,
    ops,
    salesReadyPromotion: false,
    namedPersonPreserved: true,
  };
}

/**
 * Project a verified email into GDI opportunity presentation fields.
 * Missing email must never clear an authorized verified name.
 */
export function projectVerifiedEmailOntoOpportunity(opportunity = {}, channel = {}, opts = {}) {
  const name =
    opportunity.primaryContactName ||
    opportunity.primaryContact?.name ||
    opportunity.tileContactName ||
    null;
  const customerFacingNamed =
    opportunity.primaryContact?.customerFacingNamed === true ||
    opportunity.customerFacingNamed === true ||
    (Boolean(name) && opportunity.primaryContact?.customerFacingNamed !== false);

  const email = normalizeEmail(channel.channelValue || channel.email);
  const customerMaySeeEmail =
    opts.customerDisplayPermission === true &&
    evaluateChannelReuse(channel, { requireCustomerAccess: true }).reuse === true;

  const next = {
    ...opportunity,
    primaryContactName: name,
    primaryContact: {
      ...(opportunity.primaryContact || {}),
      name: name || opportunity.primaryContact?.name || null,
      customerFacingNamed,
      email: email || opportunity.primaryContact?.email || null,
      emailCustomerVerified: Boolean(email) && channel.verificationStatus === "Verified Direct",
      emailProvider: channel.provider || null,
    },
    primaryContactEmail: email || opportunity.primaryContactEmail || null,
    // Tile / drawer: name visibility independent of email
    tileContactName: customerFacingNamed && name ? name : opportunity.tileContactName,
    emailCustomerVisible: customerMaySeeEmail ? email : null,
    salesReady: opportunity.salesReady === true, // never auto-promote
    salesReadyUnchanged: true,
  };

  if (!email && customerFacingNamed && name) {
    next.contactDisplayNote = "Authorized named person retained without email";
  }
  return next;
}

/**
 * Provider-agnostic usage ledger row (no PII).
 * Maps to Airtable GDI Contact Enrichment Usage Events when sync is enabled.
 */
export function buildUsageLedgerEvent(partial = {}) {
  const at = partial.at || new Date().toISOString();
  const provider = partial.provider || null;
  const outcome = partial.outcome || null;
  const lookupEventId =
    partial.lookupEventId ||
    createHash("sha256")
      .update(
        [
          provider || "",
          partial.personIdentityKeyHash || "",
          partial.opportunityId || "",
          at,
          outcome || "",
        ].join("|"),
      )
      .digest("hex")
      .slice(0, 24);
  return {
    schema: "gdi-contact-enrichment-usage-event-v1",
    lookupEventId,
    at,
    provider,
    outcome,
    creditsCharged: Number(partial.creditsCharged || 0),
    costEstimateUsd: partial.costEstimateUsd ?? null,
    personIdentityKeyHash: partial.personIdentityKeyHash || null,
    hotelId: partial.hotelId || null,
    opportunityId: partial.opportunityId || null,
    dataEnvironment: partial.dataEnvironment || "local",
    requestOrigin: partial.requestOrigin || "GDI Opportunity",
    providerRequestReference: partial.providerRequestReference || null,
    // Explicitly omit email/name/phone/raw payload
    airtable: {
      baseId: GDI_OPS_BASE.baseId,
      tableId: resolveUsageEventsTableId(),
      tableName: USAGE_EVENTS_TABLE_NAME,
      fields: {
        [MAP_USAGE_EVENT.lookupEventId]: lookupEventId,
        [MAP_USAGE_EVENT.provider]: mapProviderToAirtable(provider),
        [MAP_USAGE_EVENT.lookupOutcome]: mapUsageOutcomeToAirtable(outcome),
        [MAP_USAGE_EVENT.creditsCharged]: Number(partial.creditsCharged || 0),
        [MAP_USAGE_EVENT.costEstimateUsd]: partial.costEstimateUsd ?? null,
        [MAP_USAGE_EVENT.personIdentityHash]:
          partial.personIdentityKeyHash || null,
        [MAP_USAGE_EVENT.hotelId]: partial.hotelId || null,
        [MAP_USAGE_EVENT.opportunityId]: partial.opportunityId || null,
        [MAP_USAGE_EVENT.requestOrigin]:
          partial.requestOrigin || "GDI Opportunity",
        [MAP_USAGE_EVENT.eventAt]: at,
        [MAP_USAGE_EVENT.dataEnvironment]: mapDataEnvironment(
          partial.dataEnvironment,
        ),
        [MAP_USAGE_EVENT.schemaVersion]:
          partial.schemaVersion || GDI_FINDYMAIL_CHANNEL_PERSISTENCE_V1_1,
        [MAP_USAGE_EVENT.providerRequestReference]:
          partial.providerRequestReference || null,
      },
    },
  };
}

function mapDataEnvironment(env) {
  const e = String(env || "").toLowerCase();
  if (e === "production" || e === "prod") return "Production";
  if (e === "staging" || e === "stage") return "Staging";
  return "Development";
}

export default {
  GDI_FINDYMAIL_CHANNEL_PERSISTENCE_V1_1,
  FINDYMAIL_AIRTABLE_DESTINATIONS_V1_1,
  LICENSE_RETENTION_CLASS,
  USAGE_OUTCOMES,
  resolveGdiStakeholderIdentity,
  evaluateChannelReuse,
  planEmailChannelUpsert,
  projectVerifiedEmailOntoOpportunity,
  buildUsageLedgerEvent,
  channelDedupeKey,
  affiliationDedupeKey,
  normalizeEmail,
  isGenericMailbox,
};
