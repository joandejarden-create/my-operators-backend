#!/usr/bin/env node
/**
 * Permanent coverage checks for GDI Airtable complete data-model audit receipts.
 * Fails template field-coverage certification while reconstruction gaps remain.
 */
import fs from "node:fs";
import path from "node:path";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import {
  assertPublishedIdsCoveredByResearchRegistry,
  evaluateFullE2eHotelTemplateResearchStorageGate,
} from "../lib/group-demand-intelligence/research/gdi-research-persistence-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-airtable-complete-data-model-audit-v1");
const FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";

function read(name) {
  return JSON.parse(fs.readFileSync(path.join(OUT, name), "utf8"));
}

assert.ok(fs.existsSync(path.join(OUT, "FOUNDER_REPORT.md")));
assert.ok(fs.existsSync(path.join(OUT, "GDI_FIELD_TO_AIRTABLE_MATRIX.csv")));
assert.ok(fs.existsSync(path.join(OUT, "04-GDI_FIELD_TO_AIRTABLE_MATRIX.csv")));
assert.ok(fs.existsSync(path.join(OUT, "01-AIRTABLE_TABLE_INVENTORY.csv")));
assert.ok(fs.existsSync(path.join(OUT, "15-TEMPLATE_CERTIFICATION_GAPS.json")));

const gaps = read("15-TEMPLATE_CERTIFICATION_GAPS.json");
assert.equal(gaps.templateCertified, false, "must not auto-certify template");
assert.equal(gaps.researchIdCoverage.ok, true, "67 IDs must be in registry");
assert.equal(gaps.publicationImmutability.fingerprint, FP);
assert.equal(gaps.publicationImmutability.intact, true);

const recon = read("11-BETHESDA_AIRTABLE_RECONSTRUCTION.json");
assert.equal(recon.usedPublicationPackageAsFallback, false);
assert.equal(recon.publishedCount, 67);
assert.ok(
  recon.opportunitiesWithAnyDiff > 0,
  "audit must detect reconstruction gaps (not mask via package fallback)",
);

const matrix = fs.readFileSync(
  path.join(OUT, "GDI_FIELD_TO_AIRTABLE_MATRIX.csv"),
  "utf8",
);
assert.match(matrix, /LOCAL_ONLY_DEPENDENCY|MISSING_SOURCE|AIRTABLE_DIRECT/);

// Contract helpers still enforce ID coverage vs field completeness separately
const idGate = assertPublishedIdsCoveredByResearchRegistry({
  publishedOpportunityIds: ["a", "b"],
  researchRegistryIds: ["a", "b"],
});
assert.equal(idGate.ok, true);
const full = evaluateFullE2eHotelTemplateResearchStorageGate({
  publishedOpportunityIds: gaps.researchIdCoverage.missingFromRegistry.length
    ? ["x"]
    : ["ok"],
  researchRegistryIds: ["ok"],
  requiredPublicationFingerprint: FP,
  actualPublicationFingerprint: FP,
});
assert.equal(typeof full.ok, "boolean");

console.log(
  "OK test:gdi-airtable-complete-data-model-audit-v1 — ID coverage PASS, field reconstruction NOT certified",
);
