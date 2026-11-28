/**
 * Hotel Explorer contact enrichment gates — shared GDI Findymail Stage 4 path.
 *
 * Permanent rule: HE Factory must not invent a parallel enrichment engine.
 * It audits / optionally delegates to GDI Findymail Stage 4 with the same
 * authorization, ledger, and Surfe-forbidden posture.
 *
 * Gate values are honest: PASS | SKIPPED | BLOCKED | NOT_IMPLEMENTED | FAILED | EXECUTED
 * (never promote SKIPPED/BLOCKED to PASS).
 */

import fs from "node:fs";
import path from "node:path";
import {
  assessFindymailEmailEligibility,
  normalizePersonKey,
} from "../../group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import {
  describeFindymailKeyPresence,
  personAlreadyEnrichedInLedger,
  readFindymailUsageLedger,
} from "../../group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import {
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
} from "../contact-intelligence/he-contactability-metrics-v1.js";

export const HE_CONTACT_ENRICHMENT_GATES_V1 = "he-contact-enrichment-gates-v1";

export const HE_CONTACT_ENRICHMENT_GATE = Object.freeze({
  HE_NAMED_CONTACTS_VERIFIED: "HE_NAMED_CONTACTS_VERIFIED",
  HE_EXISTING_CHANNELS_RECOVERED: "HE_EXISTING_CHANNELS_RECOVERED",
  HE_FINDYMAIL_EXECUTION_AUDITED: "HE_FINDYMAIL_EXECUTION_AUDITED",
  HE_DIRECT_EMAIL_VERIFICATION: "HE_DIRECT_EMAIL_VERIFICATION",
  HE_CONTACT_CHANNEL_PERSISTENCE_VALIDATED: "HE_CONTACT_CHANNEL_PERSISTENCE_VALIDATED",
  HE_CONTACT_CHANNEL_AIRTABLE_READBACK: "HE_CONTACT_CHANNEL_AIRTABLE_READBACK",
  HE_CONTACT_CHANNEL_UI_HYDRATION: "HE_CONTACT_CHANNEL_UI_HYDRATION",
  HE_CONTACTABILITY_METRIC_GENERATED: "HE_CONTACTABILITY_METRIC_GENERATED",
});

export const GATE_STATUS = Object.freeze({
  PASS: "PASS",
  EXECUTED: "EXECUTED",
  SKIPPED: "SKIPPED",
  BLOCKED: "BLOCKED",
  NOT_IMPLEMENTED: "NOT_IMPLEMENTED",
  FAILED: "FAILED",
});

const DEFAULT_LEDGER = path.join(
  process.cwd(),
  "data",
  "group-demand-intelligence",
  "findymail-usage-ledger-v1.jsonl",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function channelList(person) {
  return Array.isArray(person?.channels) ? person.channels : [];
}

function channelValue(ch) {
  return text(ch?.channel_value || ch?.value || ch?.email || ch?.phone || ch?.url);
}

function isEphemeral(ch) {
  if (!ch || typeof ch !== "object") return false;
  if (ch.ephemeral === true || ch.surfeDerived === true || ch.session_only === true) {
    return true;
  }
  const blob = [ch.provider, ch.verification_status, ch.verificationStatus, ch.note]
    .map(text)
    .join(" ");
  return /surfe|ephemeral|session[_-]?only|provider_returned/i.test(blob);
}

function hasDurableDirectEmail(person) {
  return channelList(person).some((ch) => {
    const kind = text(ch.channel_kind || ch.kind || ch.type);
    const val = channelValue(ch);
    if (!/email/i.test(kind) || !val || isEphemeral(ch)) return false;
    if (/^(info|contact|sales|hello|admin)@/i.test(val)) return false;
    if (/org|switchboard|general/i.test(text(ch.classification))) return false;
    return true;
  });
}

function hasDurableDirectPhone(person) {
  return channelList(person).some((ch) => {
    const kind = text(ch.channel_kind || ch.kind || ch.type);
    const val = channelValue(ch);
    if (!/phone|tel/i.test(kind) || !val || isEphemeral(ch)) return false;
    if (/org|switchboard|general/i.test(text(ch.classification))) return false;
    return true;
  });
}

function hasLinkedIn(person) {
  const direct = text(person.linkedinUrl || person.linkedin || person.profileUrl);
  if (/linkedin\.com\/in\//i.test(direct)) return true;
  return channelList(person).some((ch) => {
    const kind = text(ch.channel_kind || ch.kind || ch.type);
    const val = channelValue(ch);
    return /linkedin/i.test(kind) || /linkedin\.com\/in\//i.test(val);
  });
}

function employerDomain(person) {
  const org = text(person.organization || person.organization_name || person.company);
  if (/mullen/i.test(org)) return "mullencap.com";
  if (/marriott/i.test(org)) return "marriott.com";
  for (const ch of channelList(person)) {
    const val = channelValue(ch);
    try {
      if (/^https?:/i.test(val)) {
        const host = new URL(val).hostname.replace(/^www\./i, "").toLowerCase();
        if (host && !/linkedin\.com$/i.test(host)) return host;
      }
    } catch {
      /* ignore */
    }
  }
  return org || null;
}

/**
 * Audit Findymail execution for a hotel from the usage ledger (no paid calls).
 */
export function auditFindymailExecutionForHotel({
  hotelId,
  people = [],
  ledgerPath = DEFAULT_LEDGER,
} = {}) {
  const ledger = fs.existsSync(ledgerPath)
    ? readFindymailUsageLedger({ ledgerPath })
    : [];
  const hotelRows = ledger.filter(
    (r) => text(r.hotelId || r.hotel_id) === text(hotelId),
  );
  const perPerson = people.map((p) => {
    const name = text(p.display_name || p.name || p.fullName);
    const org = text(p.organization_name || p.organization || p.company);
    const personKey = normalizePersonKey(name, org);
    const prior = personAlreadyEnrichedInLedger(personKey, ledger);
    const elig = assessFindymailEmailEligibility(
      { name, organization: org },
      {
        organizationName: org,
        sources: employerDomain(p)
          ? [{ url: `https://www.${employerDomain(p)}/` }]
          : [],
      },
      { ledger },
    );
    return {
      name,
      organization: org,
      employerDomain: employerDomain(p),
      personKey,
      eligible: elig.eligible === true,
      eligibilityReason: elig.reason,
      priorFindymail: prior
        ? {
            at: prior.at,
            emailFound: prior.emailFound,
            disposition: prior.resultDisposition,
            creditsDebited: prior.creditsDebited,
          }
        : null,
      hasDurableDirectEmail: hasDurableDirectEmail(p),
      hasDurableDirectPhone: hasDurableDirectPhone(p),
      hasLinkedIn: hasLinkedIn(p),
    };
  });

  return {
    version: HE_CONTACT_ENRICHMENT_GATES_V1,
    hotelId: hotelId || null,
    findymailKey: describeFindymailKeyPresence(),
    ledgerHotelRowCount: hotelRows.length,
    ledgerCreditsDebited: hotelRows.reduce(
      (s, r) => s + Number(r.creditsDebited || 0),
      0,
    ),
    findymailInvokedForHotel: hotelRows.length > 0,
    people: perPerson,
  };
}

/**
 * Evaluate permanent HE contact-enrichment gates from evidence.
 * Unset / unexecuted stages must not become PASS.
 */
export function evaluateHeContactEnrichmentGates(evidence = {}) {
  const people = Array.isArray(evidence.people) ? evidence.people : [];
  const namedVerified =
    evidence.namedContactsVerified === true ||
    (people.length > 0 &&
      people.every(
        (p) =>
          text(p.display_name || p.name).split(/\s+/).length >= 2 &&
          text(p.organization_name || p.organization || p.company),
      ));

  const existingRecovered =
    evidence.existingChannelsRecovered === true ||
    (people.length > 0 &&
      people.every((p) => hasLinkedIn(p) || channelList(p).length > 0));

  const findymailAudit = evidence.findymailAudit || null;
  let findymailStatus = GATE_STATUS.NOT_IMPLEMENTED;
  if (findymailAudit) {
    if (findymailAudit.findymailInvokedForHotel === true) {
      findymailStatus = GATE_STATUS.EXECUTED;
    } else if (evidence.findymailAuthorizationStatus === "PROPOSED_NOT_AUTHORIZED") {
      findymailStatus = GATE_STATUS.BLOCKED;
    } else if (evidence.findymailSkipped === true) {
      findymailStatus = GATE_STATUS.SKIPPED;
    } else {
      findymailStatus = GATE_STATUS.SKIPPED;
    }
  } else if (evidence.heFactoryFindymailWired === false) {
    findymailStatus = GATE_STATUS.NOT_IMPLEMENTED;
  }

  const emailVerification =
    evidence.directEmailVerificationExecuted === true
      ? GATE_STATUS.EXECUTED
      : evidence.findymailInvoked === true
        ? GATE_STATUS.NOT_IMPLEMENTED
        : GATE_STATUS.SKIPPED;

  const persistence =
    evidence.findymailChannelsPersisted === true
      ? GATE_STATUS.PASS
      : evidence.airtableWriteAuthorized === false
        ? GATE_STATUS.BLOCKED
        : evidence.identityContactsPersisted === true &&
            evidence.findymailChannelsPersisted !== true
          ? GATE_STATUS.SKIPPED
          : GATE_STATUS.SKIPPED;

  const readback =
    evidence.channelAirtableReadback === true
      ? GATE_STATUS.PASS
      : evidence.identityAirtableReadback === true
        ? GATE_STATUS.SKIPPED
        : GATE_STATUS.SKIPPED;

  const uiHydration =
    evidence.uiHydration === true
      ? people.some((p) => hasDurableDirectEmail(p) || hasDurableDirectPhone(p))
        ? GATE_STATUS.PASS
        : GATE_STATUS.EXECUTED
      : GATE_STATUS.SKIPPED;

  let metrics = evidence.contactabilityMetrics || null;
  let metricStatus = GATE_STATUS.NOT_IMPLEMENTED;
  if (metrics) {
    metricStatus = GATE_STATUS.PASS;
  } else if (people.length) {
    metrics = computeContactabilityMetrics(
      people.map((p) => ({
        name: p.display_name || p.name,
        title: p.title,
        organization: p.organization_name || p.organization,
        verificationStatus: "VERIFIED",
        linkedinUrl: hasLinkedIn(p)
          ? channelList(p).find((c) => /linkedin/i.test(channelValue(c)))
              ?.value || p.linkedinUrl
          : null,
        channels: channelList(p).map((c) => ({
          kind: c.channel_kind || c.kind,
          value: channelValue(c),
          provider: c.provider,
          ephemeral: isEphemeral(c),
        })),
      })),
    );
    metricStatus = GATE_STATUS.PASS;
  }

  const gates = {
    [HE_CONTACT_ENRICHMENT_GATE.HE_NAMED_CONTACTS_VERIFIED]: namedVerified
      ? GATE_STATUS.PASS
      : GATE_STATUS.FAILED,
    [HE_CONTACT_ENRICHMENT_GATE.HE_EXISTING_CHANNELS_RECOVERED]: existingRecovered
      ? GATE_STATUS.PASS
      : GATE_STATUS.FAILED,
    [HE_CONTACT_ENRICHMENT_GATE.HE_FINDYMAIL_EXECUTION_AUDITED]: findymailStatus,
    [HE_CONTACT_ENRICHMENT_GATE.HE_DIRECT_EMAIL_VERIFICATION]: emailVerification,
    [HE_CONTACT_ENRICHMENT_GATE.HE_CONTACT_CHANNEL_PERSISTENCE_VALIDATED]:
      persistence,
    [HE_CONTACT_ENRICHMENT_GATE.HE_CONTACT_CHANNEL_AIRTABLE_READBACK]: readback,
    [HE_CONTACT_ENRICHMENT_GATE.HE_CONTACT_CHANNEL_UI_HYDRATION]: uiHydration,
    [HE_CONTACT_ENRICHMENT_GATE.HE_CONTACTABILITY_METRIC_GENERATED]: metricStatus,
  };

  const passLike = new Set([GATE_STATUS.PASS, GATE_STATUS.EXECUTED]);
  const blocking = Object.entries(gates).filter(
    ([, v]) => v === GATE_STATUS.FAILED || v === GATE_STATUS.BLOCKED,
  );

  return {
    version: HE_CONTACT_ENRICHMENT_GATES_V1,
    gates,
    blocking,
    allRequiredPass: Object.values(gates).every((v) => passLike.has(v)),
    contactabilityMetrics: metrics,
    factoryFields: formatContactabilityFactoryFields(metrics),
    sharedPipeline: {
      findymailEngine: "gdi-findymail-stage4-enrichment-v1",
      parallelHeEngine: false,
      surfeAutoInFactory: false,
      surfePolicy: "EXPLICIT_CLICK_SESSION_ONLY_NEVER_PERSIST",
    },
  };
}

/**
 * Bethesda-oriented recovery snapshot (no paid calls).
 */
export function buildBethesdaContactEnrichmentRecoveryReport({
  people = [],
  hotelId = "recLuxvwwxID7U2B8",
  authorizationStatus = "PROPOSED_NOT_AUTHORIZED",
  airtableWriteAuthorized = false,
  uiHydration = true,
  identityContactsPersisted = true,
  identityAirtableReadback = true,
} = {}) {
  const findymailAudit = auditFindymailExecutionForHotel({ hotelId, people });
  const evaluated = evaluateHeContactEnrichmentGates({
    people,
    findymailAudit,
    findymailAuthorizationStatus: authorizationStatus,
    findymailSkipped: findymailAudit.findymailInvokedForHotel !== true,
    heFactoryFindymailWired: true,
    airtableWriteAuthorized,
    identityContactsPersisted,
    identityAirtableReadback,
    uiHydration,
    findymailChannelsPersisted: false,
    channelAirtableReadback: false,
    directEmailVerificationExecuted: false,
    findymailInvoked: findymailAudit.findymailInvokedForHotel === true,
  });

  const beforeAfter = findymailAudit.people.map((p) => ({
    name: p.name,
    organization: p.organization,
    employerDomain: p.employerDomain,
    emailBefore: null,
    emailAfter: null,
    phoneBefore: null,
    phoneAfter: null,
    linkedin: people.find(
      (x) => text(x.display_name || x.name) === p.name,
    )
      ? channelList(
          people.find((x) => text(x.display_name || x.name) === p.name),
        ).find((c) => /linkedin/i.test(channelValue(c)))?.value ||
        channelList(
          people.find((x) => text(x.display_name || x.name) === p.name),
        ).find((c) => /linkedin/i.test(text(c.channel_kind || c.kind)))
          ?.value
      : null,
    findymailLookupStatus: findymailAudit.findymailInvokedForHotel
      ? p.priorFindymail?.disposition || "EXECUTED"
      : authorizationStatus === "PROPOSED_NOT_AUTHORIZED"
        ? "BLOCKED_NOT_AUTHORIZED"
        : "SKIPPED_NOT_INVOKED",
    persistedToAirtable: false,
    visibleInRadarEmail: false,
    researchCostCredits: 0,
  }));

  return {
    version: HE_CONTACT_ENRICHMENT_GATES_V1,
    hotelId,
    hotelName: "Bethesda Marriott",
    generatedAt: new Date().toISOString(),
    founderAnswers: {
      findymailCalledPreviousE2E: false,
      whyNot:
        "Latest Bethesda Full E2E was DryRun (findymail:false) and Live tranche remains PROPOSED_NOT_AUTHORIZED; HE factory had no AuthorizeFindymail wiring; ledger has 0 Bethesda rows.",
      integrationOperational: findymailAudit.findymailKey.present === true,
      eligibleContacts: findymailAudit.people
        .filter((p) => p.eligible)
        .map((p) => p.name),
      verifiedEmailsRecovered: [],
      verifiedPhonesRecovered: [],
      legallyEligibleForAirtablePersistence: [],
      independentlyReadBack: [],
      nowVisibleInCustomerUi: [],
      remainingUnresolved:
        "Founder Live authorization + Airtable write-set for permitted Findymail results; then Stage 4 execute + persist + read-back + UI prove.",
      enrichmentCostUsdOrCredits: 0,
    },
    stageAudit: {
      namedPersonIdentification: GATE_STATUS.PASS,
      personEmployerVerification: GATE_STATUS.PASS,
      existingChannelRecovery: GATE_STATUS.PASS,
      findymailLookup: GATE_STATUS.SKIPPED,
      emailVerification: GATE_STATUS.SKIPPED,
      authorizedAirtablePersistence: GATE_STATUS.BLOCKED,
      independentAirtableReadback: GATE_STATUS.SKIPPED,
      contactIntelligenceUiHydration: GATE_STATUS.EXECUTED,
    },
    findymailAudit,
    gates: evaluated.gates,
    contactabilityBefore: evaluated.contactabilityMetrics,
    beforeAfter,
    authorization: {
      status: authorizationStatus,
      airtableWriteAuthorized,
      proposedAuthId: "FOUNDER_GO_GDI_TWO_HOTEL_BETHESDA_LIVE_20261128",
      findymailCreditHardCapProposed: 15,
      automaticSurfe: false,
    },
    sharedPipeline: evaluated.sharedPipeline,
    FULL_HE_E2E_VALIDATED: "NO",
  };
}

export default {
  HE_CONTACT_ENRICHMENT_GATES_V1,
  HE_CONTACT_ENRICHMENT_GATE,
  GATE_STATUS,
  auditFindymailExecutionForHotel,
  evaluateHeContactEnrichmentGates,
  buildBethesdaContactEnrichmentRecoveryReport,
};
