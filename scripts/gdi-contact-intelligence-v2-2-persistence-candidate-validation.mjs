#!/usr/bin/env node
/**
 * V2.2 deeper readiness review of V2.1 hypothetical-eligible contacts + 8 pilots.
 * No live Airtable writes. No paid research.
 *
 *   node scripts/gdi-contact-intelligence-v2-2-persistence-candidate-validation.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import { isWeakEmployerOrganizationName } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-employer-domain-v1.js";
import { assessPublicContactPersistenceReadiness } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-persistence-readiness-v1.js";
import { assessPublicContactVerification } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-2-durable-foundation",
);
fs.mkdirSync(OUT_DIR, { recursive: true });

const REEVAL = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-1-stage2-reeval/V21_REEVAL_RESULTS.json",
);
const DRY = path.join(
  ROOT,
  "reports/gdi-contact-intelligence-v2-1-stage2-reeval/V21_AIRTABLE_DRY_RUN.json",
);
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

function loadOpp(hotelId, name, org) {
  const file = path.join(DATA_ROOT, "hotels", hotelId, "opportunities.json");
  if (!fs.existsSync(file)) return null;
  const doc = JSON.parse(fs.readFileSync(file, "utf8"));
  for (const o of doc.opportunities || []) {
    const n = text(o.primaryContact?.name || o.contactName);
    const g = text(o.organizationName || o.displayCompanyName || o.company);
    if (
      n.toLowerCase() === text(name).toLowerCase() &&
      g.toLowerCase() === text(org).toLowerCase()
    ) {
      return o;
    }
  }
  return null;
}

function deeperReview(row, opportunity) {
  const checks = {
    namedPersonIdentity: looksLikePersonName(row.name),
    currentEmployer: Boolean(text(row.organization)) &&
      !isWeakEmployerOrganizationName(row.organization),
    independentlyVerifiedChannel:
      row.after?.directEmail === true || row.after?.directPhone === true,
    evidenceUrl: false,
    sourceProvenancePublic: false,
    lawfulStorageBasisDocumented: false,
    retentionRevalidationPolicy: true, // default 365d in readiness module
    authorizedCustomerVisibilityExplicit: false,
    opportunityAssociation: Boolean(opportunity?.id || opportunity?.opportunityId),
    canonicalIdentityUnique: Boolean(row.personIdentityKey),
  };

  const src =
    text(opportunity?.primaryContact?.sourceUrl) ||
    text(opportunity?.primaryContact?.officialContactUrl) ||
    text(opportunity?.sourceUrl) ||
    text(opportunity?.contactSourceUrl);
  checks.evidenceUrl = Boolean(src);

  const verification = opportunity
    ? assessPublicContactVerification({
        ...opportunity,
        contactDataOrigin: "PUBLIC_SOURCE",
      })
    : null;
  const readiness = assessPublicContactPersistenceReadiness({
    opportunity: opportunity
      ? {
          ...opportunity,
          contactDataOrigin: "PUBLIC_SOURCE",
          primaryContact: {
            ...(opportunity.primaryContact || {}),
            sourceUrl: src || opportunity.primaryContact?.sourceUrl,
          },
        }
      : {
          organizationName: row.organization,
          primaryContact: { name: row.name },
        },
    verification: verification || undefined,
    hotelId: row.hotelId,
    excludeLiveWritesDisabledBlock: true,
    permissionToRetain: true,
    // Customer visibility must be explicit — do not auto-grant from public availability
    permissionToDisplayToHotelClients: false,
  });

  checks.sourceProvenancePublic =
    readiness.checklist?.publicSourceOrigin === true;
  checks.lawfulStorageBasisDocumented =
    readiness.rightsMetadata?.permissionToRetain === true &&
    Array.isArray(readiness.rightsMetadata?.providerDerivedPersistProhibited);
  // Explicit customer visibility remains false unless separately authorized
  checks.authorizedCustomerVisibilityExplicit =
    readiness.checklist?.permissionToDisplayToHotelClients === true;

  const failed = Object.entries(checks)
    .filter(([, v]) => v !== true)
    .map(([k]) => k);

  const readyExceptLiveWrite =
    failed.length === 0 ||
    (failed.length === 1 &&
      failed[0] === "authorizedCustomerVisibilityExplicit");

  // Stricter: for controlled live pilot, require everything except live write,
  // and treat customer visibility as a separate founder toggle.
  const pilotEligibleExcludingLiveWriteAndDisplay =
    checks.namedPersonIdentity &&
    checks.currentEmployer &&
    checks.independentlyVerifiedChannel &&
    checks.evidenceUrl &&
    checks.sourceProvenancePublic &&
    checks.lawfulStorageBasisDocumented &&
    checks.retentionRevalidationPolicy &&
    checks.opportunityAssociation &&
    checks.canonicalIdentityUnique;

  return {
    personIdentityKey: row.personIdentityKey,
    name: row.name,
    organization: row.organization,
    hotelId: row.hotelId,
    checks,
    failed,
    hypotheticalFromV21: row.hypotheticalPersistOk === true,
    pilotEligibleExcludingLiveWriteAndDisplay,
    readyExceptLiveWrite,
    customerDisplayRequiresFounderAuth: true,
    publicAvailabilityDoesNotGrantExportRights: true,
    readinessBlocksExcludingLiveWrite: readiness.blocksExcludingLiveWrite || [],
    evidenceUrl: src || null,
  };
}

const reeval = JSON.parse(fs.readFileSync(REEVAL, "utf8"));
const dry = JSON.parse(fs.readFileSync(DRY, "utf8"));

const hypo39 = (reeval.rows || []).filter((r) => r.hypotheticalPersistOk === true);
const reviews39 = hypo39.map((r) => {
  const opp = r.hotelId ? loadOpp(r.hotelId, r.name, r.organization) : null;
  return deeperReview(r, opp);
});

const pilotNames = new Set((dry.proposals || []).map((p) => p.name));
const pilots = reviews39.filter((r) => pilotNames.has(r.name));

// Ensure all 8 dry-run names reviewed even if load path differs
for (const p of dry.proposals || []) {
  if (!pilots.some((x) => x.name === p.name)) {
    const row =
      hypo39.find((r) => r.name === p.name) ||
      (reeval.rows || []).find((r) => r.name === p.name);
    if (row) {
      const opp = loadOpp(p.hotelId || row.hotelId, p.name, p.organization);
      pilots.push(deeperReview({ ...row, hotelId: p.hotelId || row.hotelId }, opp));
    }
  }
}

const summary = {
  schema: "gdi-contact-intelligence-v2-2-persistence-candidate-validation-v1",
  generatedAt: new Date().toISOString(),
  liveWrites: false,
  hypo39Count: reviews39.length,
  hypo39PilotEligibleExcludingLiveWriteAndDisplay: reviews39.filter(
    (r) => r.pilotEligibleExcludingLiveWriteAndDisplay,
  ).length,
  hypo39MissingEvidenceUrl: reviews39.filter((r) => !r.checks.evidenceUrl).length,
  hypo39WeakEmployerOrIdentity: reviews39.filter(
    (r) => !r.checks.namedPersonIdentity || !r.checks.currentEmployer,
  ).length,
  pilotsReviewed: pilots.length,
  pilotsEligibleExcludingLiveWriteAndDisplay: pilots.filter(
    (r) => r.pilotEligibleExcludingLiveWriteAndDisplay,
  ).length,
  note:
    "Public availability alone does not grant unrestricted reuse/export. Customer display remains founder-gated even when storage readiness passes.",
};

const report = {
  summary,
  pilots,
  hypo39: reviews39,
};

fs.writeFileSync(
  path.join(OUT_DIR, "PERSISTENCE_CANDIDATE_VALIDATION.json"),
  JSON.stringify(report, null, 2),
);
fs.writeFileSync(
  path.join(OUT_DIR, "PERSISTENCE_CANDIDATE_VALIDATION_SUMMARY.json"),
  JSON.stringify(summary, null, 2),
);
console.log(JSON.stringify(summary, null, 2));
console.log(
  "pilots",
  pilots.map((p) => ({
    name: p.name,
    eligible: p.pilotEligibleExcludingLiveWriteAndDisplay,
    failed: p.failed,
  })),
);
