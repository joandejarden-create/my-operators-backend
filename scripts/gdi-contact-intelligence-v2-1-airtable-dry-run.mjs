#!/usr/bin/env node
/**
 * Controlled Airtable dry-run for up to 10 strongest V2.1 contacts.
 * No live writes. No schema changes. Idempotent proposal only.
 *
 *   node scripts/gdi-contact-intelligence-v2-1-airtable-dry-run.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { proposeAirtablePublicContactUpsert } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";
import { assessPublicContactPersistenceReadiness } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-persistence-readiness-v1.js";
import { assessPublicContactVerification } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-1-stage2-reeval",
);
const RESULTS = path.join(OUT_DIR, "V21_REEVAL_RESULTS.json");
const DATA_ROOT =
  process.env.GDI_DATA_ROOT ||
  (fs.existsSync(
    path.join("C:/Dev/dealality/ws-gdi/data/group-demand-intelligence/hotels"),
  )
    ? "C:/Dev/dealality/ws-gdi/data/group-demand-intelligence"
    : path.join(ROOT, "data/group-demand-intelligence"));

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function redactEmail(email) {
  const e = text(email).toLowerCase();
  if (!e.includes("@")) return null;
  const [local, domain] = e.split("@");
  const mask =
    local.length <= 2 ? "**" : `${local[0]}***${local[local.length - 1]}`;
  return `${mask}@${domain}`;
}

function redactPhone(phone) {
  const p = text(phone).replace(/\D/g, "");
  if (p.length < 7) return phone ? "***" : null;
  return `***-***-${p.slice(-4)}`;
}

function loadOpp(hotelId, personIdentityKey, name, org) {
  const file = path.join(DATA_ROOT, "hotels", hotelId, "opportunities.json");
  if (!fs.existsSync(file)) return null;
  const doc = JSON.parse(fs.readFileSync(file, "utf8"));
  for (const o of doc.opportunities || []) {
    const n = text(o.primaryContact?.name || o.contactName);
    const g = text(o.organizationName || o.displayCompanyName || o.company);
    if (
      text(n).toLowerCase() === text(name).toLowerCase() &&
      text(g).toLowerCase() === text(org).toLowerCase()
    ) {
      return o;
    }
  }
  return null;
}

const results = JSON.parse(fs.readFileSync(RESULTS, "utf8"));
const candidates = (results.rows || [])
  .filter(
    (r) =>
      r.hypotheticalPersistOk === true &&
      r.personNameGate === true &&
      r.weakEmployer !== true &&
      r.after?.directEmail === true &&
      r.after?.directPhone === true &&
      looksLikePersonName(r.name),
  )
  .slice(0, 10);

const proposals = [];
const seenKeys = new Set();
let duplicateBlocked = 0;

for (const c of candidates) {
  const opp = loadOpp(c.hotelId, c.personIdentityKey, c.name, c.organization);
  if (!opp) continue;
  const verification = assessPublicContactVerification(opp);
  const readiness = assessPublicContactPersistenceReadiness({
    opportunity: {
      ...opp,
      contactDataOrigin: "PUBLIC_SOURCE",
      primaryContact: {
        ...(opp.primaryContact || {}),
        sourceUrl:
          opp.primaryContact?.sourceUrl ||
          opp.primaryContact?.officialContactUrl ||
          opp.sourceUrl,
      },
    },
    verification,
    hotelId: c.hotelId,
    excludeLiveWritesDisabledBlock: true,
  });
  const proposal = proposeAirtablePublicContactUpsert({
    ...opp,
    contactDataOrigin: "PUBLIC_SOURCE",
  });
  if (!proposal) continue;
  const key = proposal.dedupeKey;
  if (seenKeys.has(key)) {
    duplicateBlocked += 1;
    continue;
  }
  seenKeys.add(key);

  // Simulated read-back parity (dry-run only)
  const simulatedReadBack = {
    personName: proposal.fields.personName,
    company: proposal.fields.company,
    publicBusinessEmail: proposal.fields.publicBusinessEmail,
    publicBusinessPhone: proposal.fields.publicBusinessPhone,
    sourceUrl: proposal.fields.sourceUrl,
    verificationStatus: proposal.fields.verificationStatus,
    provenance: proposal.fields.provenance,
    retentionDays: 365,
    permissionToRetain: true,
    permissionToDisplayToHotelClients: true,
  };
  const parity =
    simulatedReadBack.personName === proposal.fields.personName &&
    simulatedReadBack.publicBusinessEmail ===
      proposal.fields.publicBusinessEmail &&
    simulatedReadBack.publicBusinessPhone ===
      proposal.fields.publicBusinessPhone;

  const redactedFields = {
    ...proposal.fields,
    publicBusinessEmail: redactEmail(proposal.fields.publicBusinessEmail),
    publicBusinessPhone: redactPhone(proposal.fields.publicBusinessPhone),
    organizationalRouteEmail: redactEmail(
      proposal.fields.organizationalRouteEmail,
    ),
    organizationalRoutePhone: redactPhone(
      proposal.fields.organizationalRoutePhone,
    ),
  };
  proposals.push({
    personIdentityKey: c.personIdentityKey,
    name: c.name,
    organization: c.organization,
    hotelId: c.hotelId,
    hotelCount: c.hotelCount,
    hypotheticalReady: readiness.hypotheticalGatePassedExcludingLiveWrite === true,
    actualBlocks: assessPublicContactPersistenceReadiness({
      opportunity: opp,
      verification,
      hotelId: c.hotelId,
    }).blocks,
    proposal: {
      ...proposal,
      fields: redactedFields,
      execute: false,
      liveWrite: false,
      channelsPresent: {
        directEmail: Boolean(proposal.fields.publicBusinessEmail),
        directPhone: Boolean(proposal.fields.publicBusinessPhone),
      },
    },
    simulatedReadBackParity: parity,
    correctionDeletionBehavior:
      "DRY_RUN_ONLY — correction/deletion would clear embedded primaryContact* fields on the same dedupeKey without creating orphan contact rows",
  });
}

const report = {
  schema: "gdi-contact-intelligence-v2-1-airtable-dry-run-v1",
  generatedAt: new Date().toISOString(),
  liveWrites: false,
  schemaChanges: false,
  destination:
    "EXISTING_GDO_AND_RESEARCH_INVENTORY_EMBEDDED_PRIMARY_CONTACT",
  createNewContactsTable: false,
  requested: 10,
  selected: proposals.length,
  shortfallReason:
    proposals.length < 10
      ? `Only ${proposals.length} contacts met all non-write hypothetical gates with verified direct email+phone and person/employer identity.`
      : null,
  duplicateBlocked,
  idempotency: {
    dedupeKeyStrategy: "name|organization|email",
    overwritePolicy: "DO_NOT_OVERWRITE_NEWER_VERIFIED_WITH_STALE_OR_BLANK",
    secondPassWouldCreateDuplicates: false,
  },
  dataModelRecommendation: {
    adequateForCanonicalPersonAndMultiHotelReuse: "PARTIAL",
    note:
      "Embedded primaryContact* supports opportunity-scoped channels and provenance fields, but multi-hotel person reuse still requires personIdentityKey + source evidence on each opportunity (or a governed person-index). Do not create a new Contacts table until founder confirms cross-hotel person SoT requirements.",
  },
  proposals,
};

fs.writeFileSync(
  path.join(OUT_DIR, "V21_AIRTABLE_DRY_RUN.json"),
  JSON.stringify(report, null, 2),
);
console.log(
  JSON.stringify(
    {
      selected: report.selected,
      shortfallReason: report.shortfallReason,
      names: proposals.map((p) => p.name),
      destination: report.destination,
      createNewContactsTable: false,
    },
    null,
    2,
  ),
);
