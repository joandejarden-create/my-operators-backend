/**
 * HE Factory ↔ shared GDI Findymail Stage 4 bridge.
 *
 * Permanent rules:
 * - Reuse HI graph / prior ledger before any paid call.
 * - Never invent a parallel HE enrichment engine.
 * - Surfe is forbidden here (explicit-click UI only).
 * - Live spend requires authorizeFindymail=true; dryRun default true.
 * - Airtable persistence of returned emails requires airtableWriteAuthorized=true.
 * - Generic org mailboxes / switchboards are never written as person-direct channels.
 */
import {
  assessFindymailEmailEligibility,
  enrichPersonEmailWithFindymail,
  normalizePersonKey,
  FINDYMAIL_STAGE4_AUTH_TOKEN,
  GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
} from "../../group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import {
  describeFindymailKeyPresence,
  readFindymailUsageLedger,
} from "../../group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import {
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
} from "../contact-intelligence/he-contactability-metrics-v1.js";

export const HE_FACTORY_FINDYMAIL_BRIDGE_V1 = "he-factory-findymail-bridge-v1";

const GENERIC_LOCAL =
  /^(info|contact|sales|reservations|front[_-]?desk|hello|admin|office|enquiry|inquiry)$/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function employerDomainHint(person) {
  const org = text(person.organization || person.organization_name || person.company);
  if (/mullen/i.test(org)) return "mullencap.com";
  if (/marriott/i.test(org)) return "marriott.com";
  return null;
}

function hasDirectEmail(person) {
  const channels = Array.isArray(person.channels) ? person.channels : [];
  return channels.some((c) => {
    const kind = text(c.channel_kind || c.kind || c.type);
    const val = text(c.channel_value || c.value || c.email);
    if (!/email/i.test(kind) || !val.includes("@")) return false;
    if (c.ephemeral === true || c.surfeDerived === true) return false;
    const local = val.split("@")[0];
    if (GENERIC_LOCAL.test(local)) return false;
    if (/org|switchboard|general/i.test(text(c.classification))) return false;
    return true;
  });
}

/**
 * Build opportunity-shaped domain sources for Stage 4 eligibility.
 */
export function buildHeFindymailOpportunity(person = {}) {
  const domain = employerDomainHint(person);
  const org = text(person.organization || person.organization_name || person.company);
  return {
    organizationName: org,
    sources: domain ? [{ url: `https://www.${domain}/` }] : [],
  };
}

/**
 * Audit + optional execute Findymail for HE recovered people.
 */
export async function runHeFactoryFindymailBridge({
  hotelId,
  people = [],
  authorizeFindymail = false,
  dryRun = true,
  airtableWriteAuthorized = false,
  creditHardCap = 15,
  authorizationId = FINDYMAIL_STAGE4_AUTH_TOKEN,
  ledgerPath,
} = {}) {
  const key = describeFindymailKeyPresence();
  const ledger = readFindymailUsageLedger(
    ledgerPath ? { ledgerPath } : undefined,
  );
  const budget = { used: 0, hardCap: creditHardCap };

  const rows = [];
  let executed = 0;
  let found = 0;
  let plannedPersists = [];

  for (const person of people) {
    const name = text(person.display_name || person.name || person.fullName);
    const org = text(
      person.organization || person.organization_name || person.company,
    );
    const opportunity = buildHeFindymailOpportunity(person);
    const alreadyEmail = hasDirectEmail(person);
    const elig = alreadyEmail
      ? {
          eligible: false,
          reason: "EMAIL_ALREADY_PRESENT",
          personKey: normalizePersonKey(name, org),
          name,
          org,
          domain: employerDomainHint(person),
        }
      : assessFindymailEmailEligibility(
          { name, organization: org },
          opportunity,
          { ledger },
        );

    const row = {
      name,
      organization: org,
      personKey: elig.personKey || normalizePersonKey(name, org),
      eligible: elig.eligible === true,
      eligibilityReason: elig.reason,
      domain: elig.domain || null,
      findymailCalled: false,
      verifiedResult: null,
      savedToAirtable: false,
      outcome: "NOT_ATTEMPTED",
    };

    if (!elig.eligible) {
      row.outcome = `SKIPPED_${elig.reason}`;
      rows.push(row);
      continue;
    }

    if (!authorizeFindymail) {
      row.outcome = "BLOCKED_NOT_AUTHORIZED";
      rows.push(row);
      continue;
    }

    const enrich = await enrichPersonEmailWithFindymail(
      { name, organization: org, fullName: name },
      opportunity,
      {
        allowFindymail: true,
        authorizeSurfe: false,
        dryRun: dryRun === true,
        hotelId,
        authorizationId,
        findymailCreditBudget: budget,
        creditHardCap,
        ledger,
      },
    );

    row.findymailCalled = enrich.findymailUsed === true || enrich.dryRun === true;
    row.outcome = enrich.outcome;
    executed += row.findymailCalled ? 1 : 0;

    const email =
      enrich?.result?.email ||
      enrich?.person?.channels?.find?.((c) => c.channelType === "PROFESSIONAL_EMAIL")
        ?.value ||
      null;

    if (email && !GENERIC_LOCAL.test(String(email).split("@")[0])) {
      found += 1;
      row.verifiedResult = email;
      const persistPlan = {
        op: "CREATE",
        table: "HI Contact Channels",
        personAirtableId: person.airtableId || null,
        personName: name,
        channel: {
          kind: "email",
          value: email,
          classification: "Person Direct",
          verification_status: "Provider Returned — Unverified",
          provider: "FINDYMAIL",
          access_restriction: "INTERNAL_ONLY",
          subject_id: person.personId || elig.personKey,
        },
      };
      plannedPersists.push(persistPlan);
      if (airtableWriteAuthorized && dryRun !== true) {
        // Persistence apply is intentionally separate — bridge returns the plan.
        // Callers with write auth must use the HI graph upsert apply path.
        row.savedToAirtable = false;
        row.persistPlanReady = true;
      } else {
        row.persistBlockedReason = airtableWriteAuthorized
          ? dryRun
            ? "DRY_RUN"
            : null
          : "AIRTABLE_WRITE_NOT_AUTHORIZED";
      }
    }

    rows.push(row);
  }

  const afterPeople = people.map((p) => {
    const hit = rows.find(
      (r) =>
        text(r.name).toLowerCase() ===
        text(p.display_name || p.name).toLowerCase(),
    );
    const channels = [...(Array.isArray(p.channels) ? p.channels : [])];
    if (hit?.verifiedResult) {
      channels.push({
        kind: "email",
        value: hit.verifiedResult,
        provider: "FINDYMAIL",
        ephemeral: false,
      });
    }
    return {
      ...p,
      name: p.display_name || p.name,
      channels,
    };
  });

  const metricsAfter = computeContactabilityMetrics(afterPeople);
  const metricsBefore = computeContactabilityMetrics(
    people.map((p) => ({ ...p, name: p.display_name || p.name })),
  );

  return {
    version: HE_FACTORY_FINDYMAIL_BRIDGE_V1,
    sharedEngine: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
    hotelId: hotelId || null,
    authorizeFindymail: authorizeFindymail === true,
    dryRun: dryRun === true,
    airtableWriteAuthorized: airtableWriteAuthorized === true,
    findymailKey: key,
    creditsUsed: budget.used,
    creditHardCap,
    executedLookups: executed,
    emailsFound: found,
    surfeAuto: false,
    rows,
    plannedPersists,
    contactabilityBefore: metricsBefore,
    contactabilityAfter: metricsAfter,
    factoryFieldsAfter: formatContactabilityFactoryFields(metricsAfter),
    gates: {
      HE_CONTACT_IDENTITY_VERIFIED:
        people.length > 0 &&
        people.every((p) => text(p.display_name || p.name).split(/\s+/).length >= 2)
          ? "PASS"
          : "FAIL",
      HE_DIRECT_CHANNEL_RESEARCH_ATTEMPTED:
        authorizeFindymail && executed > 0
          ? "EXECUTED"
          : authorizeFindymail
            ? "ATTEMPTED_NO_ELIGIBLE"
            : "BLOCKED",
      HE_FINDYMAIL_EXECUTED: executed > 0 ? (dryRun ? "DRY_RUN" : "EXECUTED") : "SKIPPED",
      HE_PROVIDER_RETURNED_USABLE_DATA: found > 0 ? "YES" : "NO",
      HE_PERSISTENCE_AUTHORIZED: airtableWriteAuthorized ? "YES" : "NO",
      HE_AIRTABLE_WRITE_SUCCEEDED: "NOT_RUN",
      HE_INDEPENDENT_READBACK_SUCCEEDED: "NOT_RUN",
      HE_API_HYDRATED_CONTACT: "NOT_RUN",
      HE_CONTACT_DETAILS_CELL_CORRECT: "NOT_RUN",
      HE_CONTACTABILITY_COMPLETE:
        metricsAfter.contactabilityComplete === true ? "YES" : "NO",
    },
  };
}

export default {
  HE_FACTORY_FINDYMAIL_BRIDGE_V1,
  runHeFactoryFindymailBridge,
  buildHeFindymailOpportunity,
};
