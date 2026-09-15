#!/usr/bin/env node
/**
 * Materialize CURRENT canonical contacts for Bethesda share projection.
 * Reads tier-1 reviewed apply eval — APPLIED fields only (respects rollback).
 * Writes hotel-scoped canonical-contacts.json — does NOT mutate opportunities.json.
 *
 * Usage: node scripts/gdi-materialize-canonical-contacts-for-share.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCanonicalPersonEntry,
  saveCanonicalContacts,
  loadCanonicalContacts,
} from "../lib/group-demand-intelligence/canonical-contacts-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

const APPLY_PATH = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-tier1-reviewed-apply.json"
);
const REACH_PATH = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-contact-reachability-v1.json"
);

const apply = JSON.parse(fs.readFileSync(APPLY_PATH, "utf8"));
const reach = JSON.parse(fs.readFileSync(REACH_PATH, "utf8"));
const hotelId = apply.hotelId || "recLuxvwwxID7U2B8";

const peopleByKey = new Map();
const bindings = [];

function personKey(name, org) {
  return `${String(name || "").toLowerCase()}::${String(org || "").toLowerCase()}`;
}

// Seed people from applied fields only
for (const f of apply.appliedFields || []) {
  if (f.status !== "APPLIED") continue;
  const key = personKey(f.person, f.organization);
  let entry = peopleByKey.get(key);
  if (!entry) {
    entry = {
      displayName: f.person,
      organization: f.organization,
      title: null,
      identityDecision: f.provenance?.identityDecision || null,
      fields: {},
      opportunityId: f.provenance?.opportunityId || null,
    };
    peopleByKey.set(key, entry);
  }
  const kind = f.field === "MOBILE" ? "MOBILE" : f.field;
  entry.fields[kind] = {
    kind,
    value: f.after,
    canonicalStatus: "CANONICAL",
    acceptedAt: apply.generated_at,
    provenance: {
      ...f.provenance,
      provider: f.provenance?.provider || "surfe",
    },
  };
}

// Official emails from reachability hydrate (Jamie/Kelly) — only if person already has applied fields
// OR if reachability before.email exists and identity was ACCEPTED / LIMITED
for (const row of reach.results || []) {
  const key = personKey(row.name, row.organization);
  let entry = peopleByKey.get(key);
  const id = row.identity?.decision;
  const accepted =
    id === "ACCEPTED" || id === "ACCEPTED_WITH_LIMITED_EVIDENCE";
  if (!accepted) continue;

  // Do not create people solely from rolled-back apply history
  const hasApplied = apply.proposals?.some(
    (p) =>
      p.displayName === row.name &&
      p.organization === row.organization &&
      p.status === "APPLIED"
  );
  // Also include if they have appliedFields
  const hasAppliedField = (apply.appliedFields || []).some(
    (f) => f.person === row.name && f.organization === row.organization && f.status === "APPLIED"
  );
  if (!hasApplied && !hasAppliedField) continue;

  if (!entry) {
    entry = {
      displayName: row.name,
      organization: row.organization,
      title: row.role || null,
      identityDecision: id,
      fields: {},
      opportunityId: row.opportunityId,
    };
    peopleByKey.set(key, entry);
  }
  entry.title = entry.title || row.role || null;
  entry.identityDecision = entry.identityDecision || id;
  entry.opportunityId = entry.opportunityId || row.opportunityId;

  // Official before email — only if no APPLIED email and not rolled back
  const rolledBackEmail = (apply.proposals || []).some(
    (p) =>
      p.displayName === row.name &&
      p.fieldType === "EMAIL" &&
      p.status === "ROLLED_BACK"
  );
  if (row.before?.email && !entry.fields.EMAIL && !rolledBackEmail) {
    entry.fields.EMAIL = {
      kind: "EMAIL",
      value: row.before.email,
      canonicalStatus: "CANONICAL",
      provenance: {
        verificationState: "OFFICIAL_SOURCE_VERIFIED",
        confidence: row.whoConfidence,
        opportunityId: row.opportunityId,
        surfeOutcome: null,
        provider: null,
      },
    };
  }
}

// Explicit: Ben Hawkins rolled-back email must NOT appear
const benKey = personKey("Ben Hawkins", "Alexandria Soccer Association");
if (peopleByKey.has(benKey)) {
  const ben = peopleByKey.get(benKey);
  delete ben.fields.EMAIL;
  // If Ben has no remaining fields, drop person entirely from share projection
  if (!Object.keys(ben.fields).length) {
    peopleByKey.delete(benKey);
  }
}

const people = [];
for (const entry of peopleByKey.values()) {
  const person = buildCanonicalPersonEntry({
    displayName: entry.displayName,
    organization: entry.organization,
    title: entry.title,
    fields: entry.fields,
    identityDecision: entry.identityDecision,
  });
  people.push(person);

  const oppId =
    entry.opportunityId ||
    (apply.appliedFields || []).find((f) => f.person === entry.displayName)
      ?.provenance?.opportunityId;
  if (oppId) {
    bindings.push({
      opportunityId: oppId,
      personId: person.personId,
      eventRole: entry.title,
      whyThisContact: `${entry.displayName} (${entry.title || "contact"}) is the primary outreach contact for this opportunity.`,
      active: true,
      preserveGenericAsBackup: true,
      fieldMeta: {},
    });
  }
}

const doc = saveCanonicalContacts(hotelId, {
  version: "gdi_canonical_contacts_v1",
  hotelId,
  source: {
    applyEval: "data/group-demand-intelligence/evals/bethesda-tier1-reviewed-apply.json",
    reachabilityEval: "data/group-demand-intelligence/evals/bethesda-contact-reachability-v1.json",
    note: "Current APPLIED canonical fields only. Rolled-back Ben email excluded.",
  },
  people,
  opportunityBindings: bindings,
});

const loaded = loadCanonicalContacts(hotelId);
const amy = loaded.people.find((p) => p.displayName === "Amy Drow");
const ben = loaded.people.find((p) => p.displayName === "Ben Hawkins");
const jamie = loaded.people.find((p) => p.displayName === "Jamie McCormick");
const kelly = loaded.people.find((p) => p.displayName === "Kelly Frere");

console.log(
  JSON.stringify(
    {
      hotelId,
      people: loaded.people.length,
      bindings: loaded.opportunityBindings.length,
      check: {
        amyEmail: amy?.fields?.EMAIL?.value || null,
        amyMobile: amy?.fields?.MOBILE?.value || null,
        benPresent: Boolean(ben),
        benEmail: ben?.fields?.EMAIL?.value || null,
        jamieEmail: jamie?.fields?.EMAIL?.value || null,
        jamieMobile: jamie?.fields?.MOBILE?.value || null,
        kellyEmail: kelly?.fields?.EMAIL?.value || null,
        kellyMobile: kelly?.fields?.MOBILE?.value || null,
      },
      path: `data/group-demand-intelligence/hotels/${hotelId}/canonical-contacts.json`,
    },
    null,
    2
  )
);
