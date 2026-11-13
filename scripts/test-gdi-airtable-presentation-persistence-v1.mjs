#!/usr/bin/env node
/**
 * Gates for GDI Airtable presentation persistence V1.
 * Fails if required presentation can be stripped by compaction or if
 * Bethesda Airtable-only reconstruction ≠ approved V2 (receipts).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  compactOpportunityPayloadForAirtable,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  mergeCanonicalPresentationIntoOpportunity,
  validateRequiredPresentationFields,
  comparePresentationParity,
  reattachRequiredPresentationKeys,
  GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-presentation-persistence-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-airtable-presentation-persistence-v1");
const HOTEL = "recLuxvwwxID7U2B8";
const FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";

function read(name) {
  return JSON.parse(fs.readFileSync(path.join(OUT, name), "utf8"));
}

// Unit: compaction cannot strip required presentation
{
  const base = {
    id: "t1",
    hotelId: HOTEL,
    displayTitle: "Test",
    salesActionKey: "BUILD_ACCOUNT",
    salesActionPillLabel: "Build Account",
    datePillDisplayLabel: "Nov 2026",
    whatIsHappening: "Something",
    revenueStreamClass: "GUESTROOMS_ONLY",
    primaryContact: { name: "Ada", title: "Director" },
    tileContactPreview: { displayRole: "Director" },
    needDateMatchV1: null,
    canonicalNeedDateMatchStatus: "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION",
    supervisorV2: { x: 1 },
    researchAttempts: [{ a: 1 }],
  };
  const merged = mergeCanonicalPresentationIntoOpportunity({}, base, {
    recoveredFrom: "TEST",
  });
  const compacted = compactOpportunityPayloadForAirtable({
    ...merged,
    // force large path
    notes: "n".repeat(100000),
  });
  assert.equal(compacted.salesActionKey, "BUILD_ACCOUNT");
  assert.equal(compacted.salesActionPillLabel, "Build Account");
  assert.equal(compacted.datePillDisplayLabel, "Nov 2026");
  assert.ok(validateRequiredPresentationFields(compacted).ok);
  const re = reattachRequiredPresentationKeys({ id: "x" }, merged);
  assert.equal(re.salesActionPillLabel, "Build Account");
  console.log("PASS compaction_preserves_presentation");
}

// Unit: parity helper
{
  const a = {
    displayTitle: "A",
    salesActionKey: "MONITOR",
    salesActionPillLabel: "Monitor",
    datePillDisplayLabel: "TBD",
    whatIsHappening: "w",
    revenueStreamClass: "GUESTROOMS_ONLY",
    primaryContact: { name: "N", title: "T", email: "e@x.com" },
    tileContactPreview: { displayRole: "T" },
    needDateMatchV1: null,
    canonicalNeedDateMatchStatus: "EXPLICIT_ABSENT_IN_APPROVED_PUBLICATION",
    priority: "WATCHLIST",
    recommendedAction: "x",
    organizationName: "Org",
    opportunityType: "OVERFLOW_HOUSING",
  };
  assert.equal(comparePresentationParity(a, a).ok, true);
  console.log("PASS parity_helper");
}

// Receipts from APPLY
{
  assert.ok(fs.existsSync(path.join(OUT, "06-67_FIELD_PARITY_RESULTS.json")));
  const parity = read("06-67_FIELD_PARITY_RESULTS.json");
  assert.equal(parity.usedPublicationPackageAsFallback, false);
  assert.equal(parity.publishedCount, 67);
  assert.equal(parity.passCount, 67);
  assert.equal(parity.allPass, true);
  const receipts = read("04-BACKFILL_EXECUTION_RECEIPTS.json");
  assert.equal(receipts.mode, "APPLY");
  assert.equal(receipts.errors.length, 0);
  assert.ok(receipts.updates.length >= 67);
  const persistence = read("10-PERSISTENCE_REGRESSION.json");
  assert.equal(persistence.compactionKeepsRequiredPresentation, true);
  assert.equal(persistence.publicationShaUnchanged, true);
  assert.equal(persistence.feedbackShaUnchanged, true);
  assert.equal(persistence.fingerprint, FP);
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL,
    "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2",
    "package.json",
  );
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  assert.equal(pkg.contentFingerprintSha256, FP);
  const ready = fs.readFileSync(
    path.join(OUT, "13-TEMPLATE_CERTIFICATION_READINESS.md"),
    "utf8",
  );
  assert.match(ready, /READY_TO_CERTIFY_AIRTABLE_FIELD_COMPLETENESS/);
  assert.equal(
    GDI_CANONICAL_PRESENTATION_PERSISTENCE_V1,
    "gdi-canonical-presentation-persistence-v1",
  );
  console.log("PASS bethesda_67_airtable_reconstruction_receipts");
}

console.log("\nOK test:gdi-airtable-presentation-persistence-v1");
