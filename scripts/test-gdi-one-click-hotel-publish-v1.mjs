#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  prepareHotelPublicationCandidates,
  publishHotelInventoryOneClick,
  GDI_ONE_CLICK_HOTEL_PUBLISH_V1,
} from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const ENRICHED = path.join(
  ROOT,
  "reports/gdi-production-publication-full-content-parity-v1/enriched-snapshots",
);
const SPICE = "recKRJjcPnb4tVDDS";
const SEVEN = [
  "gdi_s4912_ba7b1cb8a3ab",
  "gdi_s4912_b956aa67a4a2",
  "gdi_s4912_b6d9b1929c05",
  "gdi_s4912_cb2d71ab1b15",
  "gdi_s4912_cc2a8da6ca6e",
  "gdi_s4912_03deda917778",
  "gdi_s4912_3cafec77a21f",
];

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

function loadApproved() {
  const registry = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/published-inventory/registry/current-registry-v1.json",
      ),
      "utf8",
    ),
  );
  return SEVEN.map((id) => {
    const snap = JSON.parse(
      fs.readFileSync(path.join(ENRICHED, `${id}.json`), "utf8"),
    );
    const row = registry.hotels?.[SPICE]?.opportunities?.[id];
    return {
      ...snap,
      hotelId: SPICE,
      commercialInventoryClass: row?.commercialClassification,
      commercialClassification: row?.commercialClassification,
      salesActionPillLabel: snap.salesActionPillLabel || "Build Account",
      salesActionKey: snap.salesActionKey || "BUILD_ACCOUNT",
    };
  });
}

check("contract_version", () => {
  assert.equal(GDI_ONE_CLICK_HOTEL_PUBLISH_V1, "gdi-one-click-hotel-publish-v1");
});

check("prepare_spice_seven", () => {
  const prepared = prepareHotelPublicationCandidates(SPICE, loadApproved(), {
    opportunityIds: SEVEN,
  });
  assert.equal(prepared.ok, true, JSON.stringify(prepared.failures, null, 2));
  assert.equal(prepared.candidates.length, 7);
  assert.equal(prepared.manifest.manifest.classificationCounts.SALES_READY, 1);
  assert.equal(
    prepared.manifest.manifest.classificationCounts.ACCOUNT_DEVELOPMENT,
    6,
  );
  assert.ok(
    prepared.candidates.every((c) => c.snapshot.publishedPresentationLocked === true),
  );
});

check("lean_blocked", () => {
  const lean = [
    {
      id: "gdi_lean_test",
      hotelId: SPICE,
      title: "Acme Corp",
      organizationName: "Acme Corp",
      whatIsHappening: "Acme Corp",
      commercialInventoryClass: "ACCOUNT_DEVELOPMENT",
    },
  ];
  const prepared = prepareHotelPublicationCandidates(SPICE, lean, {
    opportunityIds: ["gdi_lean_test"],
  });
  assert.equal(prepared.ok, false);
});

check("dry_run_default_no_write", () => {
  const out = publishHotelInventoryOneClick({
    hotelId: SPICE,
    opportunities: loadApproved(),
    opportunityIds: SEVEN,
    approval: { authorized: true, authorizedBy: "test" },
    // dryRun defaults true
  });
  assert.equal(out.ok, true);
  assert.equal(out.dryRun, true);
  assert.equal(out.advanced, false);
});

check("isolated_apply_atomic", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-ocp-"));
  const registry = createGdiPublishedInventoryRegistry({ root: tmp });
  const out = publishHotelInventoryOneClick({
    hotelId: SPICE,
    opportunities: loadApproved(),
    opportunityIds: SEVEN,
    approval: {
      authorized: true,
      authorizedBy: "test",
      authorizationPhrase: "TEST_ONE_CLICK",
    },
    dryRun: false,
    registry,
    root: tmp,
  });
  assert.equal(out.ok, true, JSON.stringify(out, null, 2));
  assert.equal(out.advanced, true);
  assert.equal(out.verifiedBeforePointer, true);
  assert.equal(out.pointerAdvanced, true);
  assert.equal(out.writes.length, 7);
  assert.ok(out.pointer?.publicationId);
  assert.equal(registry.getPublishedOpportunityIds(SPICE).length, 7);
  // Second hotel untouched
  assert.equal(registry.getPublishedOpportunityIds("recLuxvwwxID7U2B8").length, 0);
  // Approved event titles preserved through freeze + verify
  const titles = {
    gdi_s4912_ba7b1cb8a3ab:
      "71st CARPHA Annual Health Research Conference — One Health",
    gdi_s4912_03deda917778:
      "Savor 10 Grenada — Full Circle 10th Anniversary Gala Dinner & Dance",
    gdi_s4912_3cafec77a21f:
      "Kenwood Travel — Spice Island Beach Resort Weddings and Vow Renewals",
  };
  for (const [id, title] of Object.entries(titles)) {
    const snap = registry.loadSnapshot(
      SPICE,
      id,
      registry.listPublished(SPICE).find((r) => r.opportunityId === id)
        .publishedVersion,
    );
    const body = snap?.snapshot || snap;
    assert.equal(body.title, title, id);
  }
});

check("apply_requires_approval", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-ocp-deny-"));
  const registry = createGdiPublishedInventoryRegistry({ root: tmp });
  const out = publishHotelInventoryOneClick({
    hotelId: SPICE,
    opportunities: loadApproved(),
    opportunityIds: SEVEN,
    dryRun: false,
    registry,
    root: tmp,
  });
  assert.equal(out.ok, false);
  assert.equal(out.code, "FOUNDER_APPROVAL_REQUIRED");
});

const failed = results.filter((r) => !r.ok);
const report = {
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
};
fs.mkdirSync(
  path.join(ROOT, "reports/gdi-one-click-publication-architecture-v1"),
  { recursive: true },
);
fs.writeFileSync(
  path.join(
    ROOT,
    "reports/gdi-one-click-publication-architecture-v1/02-automated-test-results.json",
  ),
  JSON.stringify(report, null, 2),
);
console.log(JSON.stringify(report, null, 2));
process.exit(failed.length ? 1 : 0);
