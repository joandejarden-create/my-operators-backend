/**
 * HE Contact Intelligence V2 — hybrid Findymail (durable) + Surfe (session fallback offer).
 *
 * Flow on user click:
 * 1. Recheck canonical Airtable via shared resolver
 * 2. Findymail email / phone only for missing eligible channels (authorized + budget)
 * 3. Persist eligible Findymail results to HI Contact Channels + read back
 * 4. Offer Surfe fallback flags — never auto-call Surfe
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  resolveCanonicalPersonChannels,
  resolveCanonicalHotelContacts,
} from "../../shared-contact-intelligence/index.js";
import {
  findymailFindEmailFromName,
  findymailFindPhoneFromLinkedIn,
  appendFindymailUsageLedger,
  describeFindymailKeyPresence,
  readFindymailUsageLedger,
  FINDYMAIL_CREDIT_COSTS,
} from "../../group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import { normalizePersonKey } from "../../group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import {
  MAP_HI_CONTACT_CHANNEL,
  MAP_HI_ORG_CONTACT,
  HI_GRAPH_TABLES,
  resolveGraphBaseId,
  assertCanonicalGraphBase,
} from "../research-engine-v1/airtable-graph-contract-v1.js";
import { LICENSE_RETENTION_CLASS } from "../../group-demand-intelligence/contact-intelligence/gdi-findymail-channel-persistence-contract-v1-1.js";
import {
  computeMissingContactChannels,
  HE_CONTACT_MISSING_CHANNEL_V3,
  MISSING_CHANNEL_ENRICHMENT_V3,
} from "../../shared-contact-intelligence/missing-channel-enrichment-v3.js";

export const HE_CONTACT_HYBRID_ENRICH_V1 = "he-contact-hybrid-enrich-v1";
export { HE_CONTACT_MISSING_CHANNEL_V3 };

export const HE50_PHONE_AUTH =
  "FOUNDER_GO_HE50_CONTACT_PHONE_HYBRID_20261128";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const FINDYMAIL_USD_PER_CREDIT = Number(
  process.env.FINDYMAIL_USD_PER_CREDIT || 0.05,
);
const HE50_CAP_USD = 50;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadRegistry() {
  return JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json",
      ),
      "utf8",
    ),
  );
}

export function loadHe50SpendLedger() {
  const p = path.join(
    ROOT,
    "reports/hotel-explorer/he-5-0-pilot/HE50-RESEARCH-SPEND-LEDGER.json",
  );
  const data = JSON.parse(fs.readFileSync(p, "utf8"));
  return {
    path: p,
    data,
    remainingAuthorizedUsd: Number(data.remainingAuthorizedUsd ?? HE50_CAP_USD),
    confirmedActualUsd: Number(data.confirmedActualUsd || 0),
  };
}

function channelDedupeKey(subjectId, kind, value) {
  return createHash("sha256")
    .update(
      `${String(subjectId).toLowerCase()}|${String(kind).toLowerCase()}|${String(value).toLowerCase()}`,
    )
    .digest("hex")
    .slice(0, 24);
}

function makeChannelId() {
  return `ch_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

async function atRequest(method, tableId, body, opts = {}) {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = resolveGraphBaseId(process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) throw new Error("BASE_GATE");
  const url =
    method === "PATCH" && opts.recordId
      ? `https://api.airtable.com/v0/${baseId}/${tableId}/${opts.recordId}`
      : method === "GET" && opts.recordId
        ? `https://api.airtable.com/v0/${baseId}/${tableId}/${opts.recordId}`
        : `https://api.airtable.com/v0/${baseId}/${tableId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(`Airtable ${method} ${res.status}`);
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

async function persistChannel({
  person,
  kind,
  value,
  providerRequestId = null,
  dryRun = false,
}) {
  const registry = loadRegistry();
  const tableId = registry.tableIds[HI_GRAPH_TABLES.contactChannels];
  const contactTable = registry.tableIds[HI_GRAPH_TABLES.orgContacts];
  const dedupe = channelDedupeKey(person.canonicalPersonId || person.personId, kind, value);
  const fields = {
    [MAP_HI_CONTACT_CHANNEL.channelId]: makeChannelId(),
    [MAP_HI_CONTACT_CHANNEL.subjectType]: "Person",
    [MAP_HI_CONTACT_CHANNEL.subjectId]: person.canonicalPersonId || person.personId,
    [MAP_HI_CONTACT_CHANNEL.personLink]: person.hiAirtableId
      ? [person.hiAirtableId]
      : undefined,
    [MAP_HI_CONTACT_CHANNEL.channelKind]: kind,
    [MAP_HI_CONTACT_CHANNEL.channelValue]: value,
    [MAP_HI_CONTACT_CHANNEL.classification]: "Direct Personal",
    [MAP_HI_CONTACT_CHANNEL.verificationStatus]: "Verified Direct",
    [MAP_HI_CONTACT_CHANNEL.provider]: "Findymail",
    [MAP_HI_CONTACT_CHANNEL.sourceName]: "Findymail",
    [MAP_HI_CONTACT_CHANNEL.publicationState]: "STAGED",
    [MAP_HI_CONTACT_CHANNEL.accessRestriction]: "APPROVED_INTERNAL",
    [MAP_HI_CONTACT_CHANNEL.dedupeKey]: dedupe,
    [MAP_HI_CONTACT_CHANNEL.licenseRetentionClass]:
      LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_CUSTOMER_DURABLE,
    [MAP_HI_CONTACT_CHANNEL.lastCheckedAt]: new Date().toISOString().slice(0, 10),
    [MAP_HI_CONTACT_CHANNEL.observedAt]: new Date().toISOString().slice(0, 10),
  };
  if (providerRequestId) {
    fields[MAP_HI_CONTACT_CHANNEL.providerRequestId] = String(providerRequestId);
  }
  Object.keys(fields).forEach((k) => {
    if (fields[k] == null) delete fields[k];
  });
  if (dryRun) {
    return { dryRun: true, dedupe, fields };
  }
  const created = await atRequest("POST", tableId, { fields });
  const channelRecId = created.id;
  if (person.hiAirtableId && channelRecId) {
    const existing = await atRequest("GET", contactTable, null, {
      recordId: person.hiAirtableId,
    });
    const prev = existing.fields?.[MAP_HI_ORG_CONTACT.contactChannelsLink] || [];
    const next = [...new Set([...(Array.isArray(prev) ? prev : []), channelRecId])];
    await atRequest(
      "PATCH",
      contactTable,
      { fields: { [MAP_HI_ORG_CONTACT.contactChannelsLink]: next } },
      { recordId: person.hiAirtableId },
    );
  }
  const readBack = await atRequest("GET", tableId, null, { recordId: channelRecId });
  return {
    dryRun: false,
    recordId: channelRecId,
    dedupe,
    readBack: {
      ok: true,
      value: readBack.fields?.[MAP_HI_CONTACT_CHANNEL.channelValue] || null,
      kind: readBack.fields?.[MAP_HI_CONTACT_CHANNEL.channelKind] || null,
    },
  };
}

function employerDomain(orgName, explicit) {
  if (explicit) return String(explicit).replace(/^www\./i, "").toLowerCase();
  if (/mullen/i.test(orgName || "")) return "mullencap.com";
  if (/marriott/i.test(orgName || "")) return "marriott.com";
  return null;
}

/**
 * @param {object} opts
 */
export async function runHybridContactEnrich(opts = {}) {
  const hotelId = text(opts.hotelId);
  const personId = text(opts.personId);
  const dryRun = opts.dryRun === true;
  const allowPaid = opts.allowPaidFindymail === true && !dryRun;
  const authId = opts.authorizationId || HE50_PHONE_AUTH;

  const hotel = await resolveCanonicalHotelContacts(hotelId, {
    updateIdentityMap: false,
  });
  if (!hotel.ok) {
    return {
      ok: false,
      version: HE_CONTACT_HYBRID_ENRICH_V1,
      error: hotel.reason || hotel.status || "AIRTABLE_UNAVAILABLE",
      surfeFallbackAvailable: false,
      paidCallsExecuted: false,
    };
  }

  let person =
    (hotel.people || []).find(
      (p) =>
        String(p.canonicalPersonId || "") === personId ||
        String(p.hiAirtableId || "") === personId,
    ) ||
    (hotel.people || []).find(
      (p) =>
        text(p.displayName).toLowerCase() ===
        text(opts.personName).toLowerCase(),
    );

  if (!person && opts.personName) {
    const resolved = await resolveCanonicalPersonChannels(
      {
        canonicalPersonId: personId || undefined,
        displayName: opts.personName,
        organizationName: opts.organization,
        hotelId,
      },
      { hotelId },
    );
    person = resolved.person;
  }

  if (!person) {
    return {
      ok: false,
      version: HE_CONTACT_HYBRID_ENRICH_V1,
      error: "PERSON_NOT_FOUND",
      surfeFallbackAvailable: false,
      paidCallsExecuted: false,
    };
  }

  const steps = [];
  let missingState = computeMissingContactChannels(person);
  steps.push({
    step: "AIRTABLE_RECHECK",
    email: person.email || null,
    phone: person.phone || null,
    linkedinUrl: person.linkedinUrl || null,
    missingChannelState: missingState,
  });

  const missingEmail = missingState.needsEmail === true;
  const missingPhone = missingState.needsPhone === true;
  if (!missingEmail && !missingPhone) {
    return {
      ok: true,
      version: HE_CONTACT_HYBRID_ENRICH_V1,
      missingChannelVersion: HE_CONTACT_MISSING_CHANNEL_V3,
      message: "Verified email and phone already on file.",
      email: person.email,
      phone: person.phone,
      linkedinUrl: person.linkedinUrl,
      channels: person.channels,
      missingChannelState: missingState,
      surfeFallbackAvailable: false,
      paidCallsExecuted: false,
      findymail: { skipped: true, reason: "CHANNELS_COMPLETE" },
      steps,
    };
  }

  const spend = loadHe50SpendLedger();
  const keyPresence = describeFindymailKeyPresence();
  const ledger = readFindymailUsageLedger();
  const personKey = normalizePersonKey(
    person.displayName,
    person.organizationName,
  );
  let creditsDebited = 0;
  let estimatedUsd = 0;
  const findymail = {
    email: null,
    phone: null,
    blocked: null,
  };

  const domain =
    employerDomain(person.organizationName, opts.companyDomain) ||
    employerDomain(opts.organization);

  if (!allowPaid) {
    findymail.blocked = dryRun
      ? "DRY_RUN"
      : "PAID_NOT_ENABLED";
  } else if (!keyPresence.present) {
    findymail.blocked = "FINDYMAIL_CREDENTIALS_MISSING";
  } else if (spend.remainingAuthorizedUsd < FINDYMAIL_USD_PER_CREDIT) {
    findymail.blocked = "HE50_BUDGET_EXHAUSTED";
  }

  // Email via Findymail
  if (missingEmail && !findymail.blocked && domain) {
    try {
      const emailRes = await findymailFindEmailFromName(
        { name: person.displayName, domain },
        { dryRun: false },
      );
      findymail.email = {
        disposition: emailRes.disposition,
        credits: emailRes.creditsDebitedEstimate,
      };
      creditsDebited += Number(emailRes.creditsDebitedEstimate || 0);
      if (emailRes.email) {
        const persisted = await persistChannel({
          person,
          kind: "Email",
          value: emailRes.email,
          dryRun: false,
        });
        person.email = emailRes.email;
        person.channels = [
          ...(person.channels || []),
          {
            channelType: "Email",
            channelValue: emailRes.email,
            provider: "Findymail",
            canonicalAirtableRecordId: persisted.recordId,
            verificationStatus: "Verified Direct",
          },
        ];
        findymail.email.persisted = persisted;
        findymail.email.value = emailRes.email;
      }
      appendFindymailUsageLedger({
        authorizationId: authId,
        hotelId,
        personKey,
        organizationName: person.organizationName,
        operation: "FIND_FROM_NAME",
        creditsDebited: emailRes.creditsDebitedEstimate,
        resultDisposition: emailRes.disposition,
        emailFound: Boolean(emailRes.email),
        piiPersisted: Boolean(emailRes.email),
      });
    } catch (err) {
      findymail.email = {
        error: String(err?.code || err?.message || err),
      };
    }
  } else if (missingEmail) {
    findymail.email = {
      skipped: true,
      reason: findymail.blocked || (!domain ? "DOMAIN_UNRESOLVED" : "SKIPPED"),
    };
  }

  // Phone via Findymail (requires LinkedIn; 10 credits when found)
  const linkedin = person.linkedinUrl || text(opts.linkedinUrl);
  const phoneBudgetOk =
    spend.remainingAuthorizedUsd - creditsDebited * FINDYMAIL_USD_PER_CREDIT >=
    10 * FINDYMAIL_USD_PER_CREDIT;

  if (missingPhone && !findymail.blocked && linkedin && phoneBudgetOk) {
    try {
      const phoneRes = await findymailFindPhoneFromLinkedIn(
        { linkedinUrl: linkedin },
        { dryRun: false },
      );
      findymail.phone = {
        disposition: phoneRes.disposition,
        credits: phoneRes.creditsDebitedEstimate,
        lineType: phoneRes.lineType,
      };
      creditsDebited += Number(phoneRes.creditsDebitedEstimate || 0);
      if (phoneRes.phone) {
        const persisted = await persistChannel({
          person,
          kind: "Telephone",
          value: phoneRes.phone,
          dryRun: false,
        });
        person.phone = phoneRes.phone;
        person.channels = [
          ...(person.channels || []),
          {
            channelType: "Telephone",
            channelValue: phoneRes.phone,
            provider: "Findymail",
            canonicalAirtableRecordId: persisted.recordId,
            verificationStatus: "Verified Direct",
          },
        ];
        findymail.phone.persisted = persisted;
        findymail.phone.value = phoneRes.phone;
      }
      appendFindymailUsageLedger({
        authorizationId: authId,
        hotelId,
        personKey,
        organizationName: person.organizationName,
        operation: "FIND_PHONE",
        creditsDebited: phoneRes.creditsDebitedEstimate,
        resultDisposition: phoneRes.disposition,
        phoneFound: Boolean(phoneRes.phone),
        piiPersisted: Boolean(phoneRes.phone),
      });
    } catch (err) {
      findymail.phone = {
        error: String(err?.code || err?.message || err),
      };
    }
  } else if (missingPhone) {
    findymail.phone = {
      skipped: true,
      reason: findymail.blocked
        ? findymail.blocked
        : !linkedin
          ? "LINKEDIN_REQUIRED"
          : !phoneBudgetOk
            ? "HE50_BUDGET_INSUFFICIENT_FOR_PHONE"
            : "SKIPPED",
    };
  }

  estimatedUsd = Number((creditsDebited * FINDYMAIL_USD_PER_CREDIT).toFixed(4));

  // Update HE50 ledger when spend occurred
  if (creditsDebited > 0 && allowPaid) {
    const he50 = loadHe50SpendLedger();
    he50.data.confirmedActualUsd = Number(
      (Number(he50.data.confirmedActualUsd || 0) + estimatedUsd).toFixed(4),
    );
    he50.data.remainingAuthorizedUsd = Number(
      (HE50_CAP_USD - he50.data.confirmedActualUsd).toFixed(4),
    );
    he50.data.byProvider = he50.data.byProvider || [];
    he50.data.byProvider.push({
      provider: "findymail",
      credits: creditsDebited,
      usd: estimatedUsd,
      at: new Date().toISOString(),
      authorizationId: authId,
      channel: "hybrid_email_phone",
    });
    he50.data.entries = he50.data.entries || [];
    he50.data.entries.push({
      run_id: authId,
      operation_id: `he_hybrid_enrich_${hotelId}_${person.canonicalPersonId || personId}`,
      hotel_id: hotelId,
      research_category: "ownership_stakeholders",
      provider: "findymail",
      request_type: "hybrid_email_phone",
      timestamp: new Date().toISOString(),
      actual_usd: estimatedUsd,
      credits_debited: creditsDebited,
      status: "CONFIRMED",
    });
    fs.writeFileSync(he50.path, JSON.stringify(he50.data, null, 2) + "\n", "utf8");
  }

  // Fresh read-back
  const after = await resolveCanonicalPersonChannels(
    {
      canonicalPersonId: person.canonicalPersonId,
      displayName: person.displayName,
      hotelId,
    },
    { hotelId },
  );
  const finalPerson = after.person || person;
  missingState = computeMissingContactChannels(finalPerson);
  const stillMissingEmail = missingState.needsEmail === true;
  const stillMissingPhone = missingState.needsPhone === true;
  steps.push({
    step: "AIRTABLE_RECHECK_AFTER",
    missingChannelState: missingState,
  });

  return {
    ok: true,
    version: HE_CONTACT_HYBRID_ENRICH_V1,
    missingChannelVersion: HE_CONTACT_MISSING_CHANNEL_V3,
    message: stillMissingEmail && stillMissingPhone
      ? "No durable channels found via Findymail. Surfe fallback available (session only)."
      : stillMissingPhone || stillMissingEmail
        ? "Partial durable channels updated. Surfe fallback available for remaining gaps (session only)."
        : "Canonical channels updated from Airtable.",
    email: finalPerson.email || null,
    phone: finalPerson.phone || null,
    linkedinUrl: finalPerson.linkedinUrl || null,
    channels: finalPerson.channels || [],
    canonicalPersonId: finalPerson.canonicalPersonId,
    missingChannelState: missingState,
    requestChannel: missingState.requestChannel,
    surfeFallbackAvailable: stillMissingEmail || stillMissingPhone,
    surfeIncludeSuggested: missingState.surfeInclude,
    paidCallsExecuted: creditsDebited > 0,
    creditsDebited,
    estimatedUsd,
    he50RemainingUsd: loadHe50SpendLedger().remainingAuthorizedUsd,
    findymail,
    steps,
    surfePolicy: {
      autoInvoke: false,
      persist: false,
      sessionOnly: true,
    },
  };
}

export default {
  HE_CONTACT_HYBRID_ENRICH_V1,
  HE50_PHONE_AUTH,
  runHybridContactEnrich,
  loadHe50SpendLedger,
};
