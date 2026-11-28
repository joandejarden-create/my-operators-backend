/**
 * GDI Contact Channel Evidence Recovery V1
 *
 * Hydrates `contactChannelResearch` researched-flags on package contacts from
 * evidence that already exists (Findymail usage ledger, prior public-research
 * artifacts). It exists because ledger-recorded research never reached the
 * package flags, so channels stayed NOT_YET_RESEARCHED and failed
 * `evaluateContactBestEffortCertificationGate`.
 *
 * Contract: GDI_CONTACT_CHANNEL_EVIDENCE_RECOVERY_V1
 *
 * Hard rules:
 *  - Flags only. NEVER writes email / phone / LinkedIn values.
 *  - Never spends credits or calls providers (reads the repo-local ledger only).
 *  - Pure / in-memory: returns new objects; inputs are not mutated.
 *  - Display names are never mutated unless `mutateDisplayName` is explicitly set.
 */
import {
  normalizePersonKey,
  normalizePersonNameForEnrichment,
} from "./gdi-findymail-stage4-enrichment-v1.js";
import { readFindymailUsageLedger } from "../providers/gdi-findymail-client-v1.js";

export { normalizePersonNameForEnrichment };

export const GDI_CONTACT_CHANNEL_EVIDENCE_RECOVERY_V1 =
  "gdi-contact-channel-evidence-recovery-v1";

const RESEARCHED_ENUMS = new Set([
  "FOUND",
  "FOUND_VERIFICATION_REQUIRED",
  "NOT_FOUND_OR_UNAVAILABLE",
  "RESEARCH_COMPLETED_NOT_FOUND",
  "UNAVAILABLE",
]);

const CHANNEL_KEYS = Object.freeze({
  email: { enumKey: "professionalEmail", boolKey: "emailResearched", valueKeys: ["email"] },
  telephone: {
    enumKey: "professionalTelephone",
    boolKey: "phoneResearched",
    valueKeys: ["phone"],
  },
  linkedin: {
    enumKey: "personalLinkedIn",
    boolKey: "linkedinResearched",
    valueKeys: ["linkedinUrl"],
  },
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function personDisplayName(p) {
  return text(p?.fullName || p?.name);
}

function channelAlreadyResearched(research = {}, channel) {
  const k = CHANNEL_KEYS[channel];
  return (
    research[k.boolKey] === true ||
    RESEARCHED_ENUMS.has(text(research[k.enumKey]).toUpperCase())
  );
}

function channelHasValue(person = {}, channel) {
  return CHANNEL_KEYS[channel].valueKeys.some((k) => text(person[k]));
}

/** Latest usable Findymail FIND_FROM_NAME ledger row for a person key. */
export function findFindymailLedgerEvidence(personKey, ledger = []) {
  const key = text(personKey).toLowerCase();
  if (!key) return null;
  const rows = ledger.filter(
    (r) =>
      text(r.personKey).toLowerCase() === key &&
      (r.operation === "FIND_FROM_NAME" || r.operation === "FIND") &&
      (r.resultDisposition === "FOUND" || r.resultDisposition === "NOT_FOUND"),
  );
  if (!rows.length) return null;
  return rows.reduce((a, b) => (String(b.at) > String(a.at) ? b : a));
}

/**
 * Prior public-research artifact entries (supplied by caller; never fabricated):
 *   { personKey? | name + organization?, channelsResearched: {email?, telephone?, linkedin?},
 *     source: "<artifact id or path>", at?: ISO }
 */
function findArtifactEvidence(person, personKey, artifacts = []) {
  const name = personDisplayName(person).toLowerCase();
  return artifacts.filter((a) => {
    if (a.personKey && text(a.personKey).toLowerCase() === personKey) return true;
    if (a.name && text(a.name).toLowerCase() === name) {
      const org = text(a.organization).toLowerCase();
      const pOrg = text(person.organization).toLowerCase();
      return !org || !pOrg || org === pOrg;
    }
    return false;
  });
}

/**
 * Recover researched flags for one person.
 *
 * @param {object} person
 * @param {object} [opts]
 * @param {string} [opts.organizationName]       fallback organization for personKey
 * @param {object[]} [opts.findymailLedger]       pre-read ledger rows
 * @param {object[]} [opts.priorResearchArtifacts]
 * @param {boolean} [opts.normalizeIdentity]      add `enrichmentName` (display name untouched)
 * @param {boolean} [opts.mutateDisplayName]      explicitly rewrite name/fullName to normalized form
 * @returns {{person: object, changed: boolean, changes: object[], personKey: string,
 *            ledgerEvidence: object|null, artifactEvidence: object[], identity: object|null}}
 */
export function recoverContactChannelResearchFlags(person = {}, opts = {}) {
  const display = personDisplayName(person);
  const org = text(person.organization || opts.organizationName);
  const personKey = normalizePersonKey(display, org);
  const before = person.contactChannelResearch || {};
  const research = { ...before };
  const changes = [];
  const provenance = [...(before.recoveryProvenance || [])];

  const ledger =
    opts.findymailLedger ||
    (opts.skipFindymailLedger === true
      ? []
      : readFindymailUsageLedger({ repoRoot: opts.repoRoot }));

  const ledgerEvidence = display ? findFindymailLedgerEvidence(personKey, ledger) : null;
  if (ledgerEvidence) {
    if (!research.findymailUsed) {
      research.findymailUsed = true;
      changes.push({ field: "contactChannelResearch.findymailUsed", to: true, source: "FINDYMAIL_LEDGER" });
    }
    if (!channelHasValue(person, "email") && !channelAlreadyResearched(research, "email")) {
      research.emailResearched = true;
      research.emailAttempted = true;
      changes.push({ field: "contactChannelResearch.emailResearched", to: true, source: "FINDYMAIL_LEDGER" });
      if (ledgerEvidence.resultDisposition === "NOT_FOUND") {
        research.professionalEmail = "NOT_FOUND_OR_UNAVAILABLE";
        changes.push({
          field: "contactChannelResearch.professionalEmail",
          to: "NOT_FOUND_OR_UNAVAILABLE",
          source: "FINDYMAIL_LEDGER",
        });
      } else {
        // Provider returned an email but it was not persisted on the package
        // (piiPersisted=false). Mark researched; do not invent the value and do
        // not claim NOT_FOUND. Surfaced as a remaining gap by the orchestrator.
        research.emailRecoveryNote = "PROVIDER_FOUND_EMAIL_NOT_PERSISTED_IN_PACKAGE";
        changes.push({
          field: "contactChannelResearch.emailRecoveryNote",
          to: research.emailRecoveryNote,
          source: "FINDYMAIL_LEDGER",
        });
      }
      provenance.push({
        channel: "email",
        source: "FINDYMAIL_LEDGER",
        ledgerAt: ledgerEvidence.at || null,
        disposition: ledgerEvidence.resultDisposition,
        authorizationId: ledgerEvidence.authorizationId || null,
      });
    }
  }

  const artifactEvidence = findArtifactEvidence(person, personKey, opts.priorResearchArtifacts || []);
  for (const art of artifactEvidence) {
    const done = art.channelsResearched || {};
    for (const channel of Object.keys(CHANNEL_KEYS)) {
      if (done[channel] !== true) continue;
      if (channelHasValue(person, channel) || channelAlreadyResearched(research, channel)) continue;
      const k = CHANNEL_KEYS[channel];
      research[k.boolKey] = true;
      changes.push({
        field: `contactChannelResearch.${k.boolKey}`,
        to: true,
        source: text(art.source) || "PRIOR_PUBLIC_RESEARCH_ARTIFACT",
      });
      provenance.push({
        channel,
        source: text(art.source) || "PRIOR_PUBLIC_RESEARCH_ARTIFACT",
        artifactAt: art.at || null,
      });
    }
  }

  let identity = null;
  let next = { ...person };
  if (display && (opts.normalizeIdentity === true || opts.mutateDisplayName === true)) {
    identity = normalizePersonNameForEnrichment(display);
    if (identity.changed && opts.normalizeIdentity === true) {
      next.enrichmentName = identity.normalized;
      changes.push({ field: "enrichmentName", to: identity.normalized, source: "NAME_NORMALIZATION" });
    }
    if (identity.changed && opts.mutateDisplayName === true) {
      next.name = identity.normalized;
      if (person.fullName) next.fullName = identity.normalized;
      changes.push({ field: "name", to: identity.normalized, source: "NAME_NORMALIZATION_EXPLICIT" });
    }
  }

  const researchChanged = changes.some((c) => c.field.startsWith("contactChannelResearch."));
  if (researchChanged) {
    research.recoveryProvenance = provenance;
    research.recoveryVersion = GDI_CONTACT_CHANNEL_EVIDENCE_RECOVERY_V1;
    next.contactChannelResearch = research;
  }

  return {
    person: next,
    changed: changes.length > 0,
    changes,
    personKey,
    ledgerEvidence: ledgerEvidence
      ? {
          at: ledgerEvidence.at || null,
          resultDisposition: ledgerEvidence.resultDisposition,
          emailFound: ledgerEvidence.emailFound === true,
          creditsDebited: ledgerEvidence.creditsDebited ?? null,
        }
      : null,
    artifactEvidence: artifactEvidence.map((a) => ({
      source: text(a.source) || null,
      channelsResearched: a.channelsResearched || {},
    })),
    identity,
  };
}

/**
 * Apply recovery to primary / secondary / alternative-entry contacts of every
 * opportunity. Returns new opportunities + a report.
 */
export function applyEvidenceRecoveryToOpportunities(opportunities = [], opts = {}) {
  const ledger =
    opts.findymailLedger ||
    (opts.skipFindymailLedger === true
      ? []
      : readFindymailUsageLedger({ repoRoot: opts.repoRoot }));
  const rows = [];
  const uniquePersons = new Map();

  const run = (person, opportunity, slot) => {
    if (!person || !personDisplayName(person)) return person;
    const r = recoverContactChannelResearchFlags(person, {
      ...opts,
      findymailLedger: ledger,
      organizationName: opportunity.organizationName || opportunity.displayCompanyName,
    });
    const row = {
      opportunityId: opportunity.id || opportunity.opportunityId || null,
      slot,
      name: personDisplayName(person),
      personKey: r.personKey,
      changed: r.changed,
      changes: r.changes,
      ledgerEvidence: r.ledgerEvidence,
      artifactEvidence: r.artifactEvidence,
    };
    rows.push(row);
    if (r.changed) {
      const u = uniquePersons.get(r.personKey) || {
        personKey: r.personKey,
        name: row.name,
        slots: [],
        opportunityIds: [],
        changes: r.changes,
        ledgerEvidence: r.ledgerEvidence,
      };
      u.slots.push(slot);
      u.opportunityIds.push(row.opportunityId);
      uniquePersons.set(r.personKey, u);
    }
    return r.person;
  };

  const out = opportunities.map((o) => {
    const next = { ...o };
    if (o.primaryContact) next.primaryContact = run(o.primaryContact, o, "PRIMARY");
    if (o.secondaryContact) next.secondaryContact = run(o.secondaryContact, o, "SECONDARY");
    if (Array.isArray(o.alternativeEntryPoints)) {
      next.alternativeEntryPoints = o.alternativeEntryPoints.map((p, i) =>
        run(p, o, `ALTERNATIVE_ENTRY_${i + 1}`),
      );
    }
    return next;
  });

  const changedRows = rows.filter((r) => r.changed);
  return {
    opportunities: out,
    report: {
      version: GDI_CONTACT_CHANNEL_EVIDENCE_RECOVERY_V1,
      contactSlotsExamined: rows.length,
      contactSlotsChanged: changedRows.length,
      uniquePeopleChanged: uniquePersons.size,
      findymailLedgerRows: ledger.length,
      priorArtifactEntries: (opts.priorResearchArtifacts || []).length,
      noValuesInvented: true,
      uniquePeople: [...uniquePersons.values()].map((u) => ({
        ...u,
        slots: [...new Set(u.slots)],
        opportunityIds: [...new Set(u.opportunityIds)],
      })),
      rows: changedRows,
    },
  };
}
