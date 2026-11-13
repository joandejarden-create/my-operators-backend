#!/usr/bin/env node
/**
 * Regression gates for GDI research persistence contract + Bethesda reconciliation receipts.
 * Does not write Airtable. Uses frozen report receipts + pure contract helpers.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import {
  assertPublishedIdsCoveredByResearchRegistry,
  buildHistoricalRegistryUpsertCandidate,
  evaluateFullE2eHotelTemplateResearchStorageGate,
  GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
} from "../lib/group-demand-intelligence/research/gdi-research-persistence-contract-v1.js";
import { resolvePublicationPackageRoot } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { verifyPublicationPackageIntegrity } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-research-storage-reconciliation-v1");
const HOTEL = "recLuxvwwxID7U2B8";
const FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";

function read(name) {
  return JSON.parse(fs.readFileSync(path.join(OUT, name), "utf8"));
}

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// Contract: published ⊆ registry
{
  const good = assertPublishedIdsCoveredByResearchRegistry({
    publishedOpportunityIds: ["a", "b"],
    researchRegistryIds: ["a", "b", "c"],
  });
  assert.equal(good.ok, true);
  const bad = assertPublishedIdsCoveredByResearchRegistry({
    publishedOpportunityIds: ["a", "b"],
    researchRegistryIds: ["a"],
  });
  assert.equal(bad.ok, false);
  assert.deepEqual(bad.missingFromRegistry, ["b"]);
  ok("published_subset_gate");
}

// Contract: build candidate quarantines invented fields
{
  const q = buildHistoricalRegistryUpsertCandidate({
    hotelId: HOTEL,
    packageOpportunity: { id: "x", title: "T", primaryContact: { name: "N" } },
    reconstructedOpportunity: {
      id: "x",
      title: "T",
      sources: [{ url: "https://example.com" }],
      inventedFields: ["fake"],
      provenance: { curatedAttestation: true },
    },
  });
  assert.equal(q.ok, false);
  assert.equal(q.quarantine, true);
  ok("quarantine_invented_fields");
}

// Contract: valid candidate
{
  const v = buildHistoricalRegistryUpsertCandidate({
    hotelId: HOTEL,
    packageOpportunity: {
      id: "disp_test",
      title: "Test Org",
      primaryContact: { name: "Ada" },
    },
    reconstructedOpportunity: {
      id: "disp_test",
      title: "Test Org",
      hotelId: HOTEL,
      sources: [{ url: "https://example.com" }],
      provenance: { curatedAttestation: true },
      reconstructionStatus: "RECONSTRUCTED_FROM_VERIFIABLE_SOURCE",
      inventedFields: [],
    },
    publicationFingerprint: FP,
  });
  assert.equal(v.ok, true);
  assert.equal(v.candidate.researchRegistryProvenance.createOnly, true);
  ok("build_historical_candidate");
}

// Template gate fails on incomplete registry
{
  const gate = evaluateFullE2eHotelTemplateResearchStorageGate({
    publishedOpportunityIds: ["p1", "p2"],
    researchRegistryIds: ["p1"],
    requiredPublicationFingerprint: FP,
    actualPublicationFingerprint: FP,
  });
  assert.equal(gate.ok, false);
  assert.equal(gate.fails[0].code, "RESEARCH_REGISTRY_MISSING_PUBLISHED_IDS");
  ok("template_gate_incomplete_registry");
}

// Receipts from live reconciliation (must exist after apply)
{
  assert.ok(fs.existsSync(path.join(OUT, "07-POST_MIGRATION_READBACK.json")));
  const post = read("07-POST_MIGRATION_READBACK.json");
  assert.equal(post.publishedCoveredByAirtable.ok, true);
  assert.equal(post.stillMissingFromAirtable.length, 0);
  assert.ok(post.airtableCountAfter >= 86);
  assert.equal(post.airtableCountBefore, 59);
  assert.equal(post.duplicateOpportunityIds.length, 0);
  assert.equal(post.publicationFingerprintUnchanged, true);
  assert.equal(post.feedbackShaUnchanged, true);
  assert.equal(post.bundledPackageShaUnchanged, true);
  assert.equal(post.airtableOnlyStillPresent, true);
  ok("post_migration_readback_receipts");
}

{
  const receipts = read("06-MIGRATION_RECEIPTS.json");
  assert.equal(receipts.mode, "APPLY");
  assert.equal(receipts.errors.length, 0);
  assert.equal(
    receipts.creates.filter((c) => c.action === "CREATED").length,
    27,
  );
  ok("migration_receipts_27_created");
}

{
  const d19 = read("04-19_RECORD_DISPOSITIONS.json");
  assert.equal(d19.count, 19);
  assert.ok(d19.records.every((r) => r.promoteToCustomerPublication === false));
  assert.ok(d19.records.every((r) => r.preserveInAirtable === true));
  ok("nineteen_dispositions_no_promote");
}

{
  const pointer = read("09-CURRENT_POINTER_AUDIT.json");
  assert.ok(
    pointer.action.type === "QUARANTINE_STALE_DATA_CURRENT" ||
      (pointer.action.type === "NO_QUARANTINE_NEEDED" &&
        pointer.after?.dataCurrentExists === false),
  );
  assert.equal(pointer.after.cannotSilentlyServeSuperseded441773, true);
  assert.equal(pointer.after.loadCustomerVisiblePackage.fingerprint, FP);
  const quarantinePath = path.join(
    ROOT,
    "data/group-demand-intelligence/publication-packages",
    HOTEL,
    "CURRENT.quarantined-stale-441773211494-2026-11-13.json",
  );
  assert.ok(
    fs.existsSync(quarantinePath),
    "stale CURRENT quarantine file must remain for rollback history",
  );
  assert.equal(
    fs.existsSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/publication-packages",
        HOTEL,
        "CURRENT.json",
      ),
    ),
    false,
  );
  ok("stale_current_quarantined");
}

{
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    HOTEL,
    "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2",
    "package.json",
  );
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  assert.equal(pkg.contentFingerprintSha256, FP);
  assert.equal(verifyPublicationPackageIntegrity(pkg).ok, true);
  const root = resolvePublicationPackageRoot(process.env);
  assert.match(
    root.replace(/\\/g, "/"),
    /bundled-publication-packages$/,
  );
  ok("publication_isolation_unchanged");
}

{
  assert.equal(
    GDI_RESEARCH_PERSISTENCE_CONTRACT_V1,
    "gdi-research-persistence-contract-v1",
  );
  const contract = read("11-CROSS_HOTEL_STORAGE_CONTRACT.json");
  assert.equal(contract.templateGate.ok, true);
  ok("cross_hotel_contract_gate");
}

console.log(`\nOK ${passed} checks — test:gdi-research-storage-reconciliation-v1`);
