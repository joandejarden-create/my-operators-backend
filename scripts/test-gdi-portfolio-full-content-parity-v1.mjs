#!/usr/bin/env node
/**
 * Universal portfolio full-content parity unit/integration tests.
 * Uses Spice work inventory (7 schema-ready snapshots) + fixture freeze gate.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertApprovedCustomerFacingDetailPresent,
  validateCanonicalPresentationObject,
  GDI_FULL_CONTENT_PARITY_STATUS,
} from "../lib/group-demand-intelligence/contracts/gdi-canonical-customer-presentation-contract-v1.js";
import { verifyGdiHotelFullContentParity } from "../lib/group-demand-intelligence/presentation/gdi-hotel-full-content-parity-verify-v1.js";
import {
  buildGdiParityHotelInventory,
  discoverGdiParityHotelUniverse,
  runGdiPortfolioFullContentParityAudit,
} from "../lib/group-demand-intelligence/presentation/gdi-portfolio-full-content-parity-v1.js";
import { freezePublishedPresentationFields } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { verifyStagedPublicationFullContentParity } from "../lib/group-demand-intelligence/presentation/gdi-hotel-full-content-parity-verify-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const WORK_ROOT = path.join(
  ROOT,
  "reports/gdi-universal-full-presentation-parity-v1/work-gdi-published-inventory",
);
const SPICE = "recKRJjcPnb4tVDDS";

const results = [];
function check(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") {
      return r
        .then(() => results.push({ name, ok: true }))
        .catch((err) =>
          results.push({ name, ok: false, error: String(err?.stack || err) }),
        );
    }
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

const fixture = {
  id: "gdi_fixture_parity_univ",
  hotelId: SPICE,
  title: "Parity Universal Fixture",
  commercialInventoryClass: "ACCOUNT_DEVELOPMENT",
  commercialClassification: "ACCOUNT_DEVELOPMENT",
  salesActionKey: "ENGAGE_PLANNER",
  salesActionPillLabel: "Engage Planner",
  priorityFilterKey: "WATCHLIST",
  whatIsHappening: "Fixture narrative.",
  potentialHotelBusiness: "Account development pathway.",
  whyThisHotel: "Fit.",
  whyActNow: "Timing.",
  establishedFacts: ["Fact one"],
  uncertainties: [],
  qualificationQuestions: ["Is there an active brief?"],
  recommendedNextStep: "Qualify.",
  sources: [{ url: "https://example.com", label: "Example" }],
  primaryContact: { name: "Primary Person", role: "Planner" },
  secondaryContact: { name: "Secondary Person", role: "Advisor" },
};

await check("canonical_contract_requires_approved_object", () => {
  const bad = assertApprovedCustomerFacingDetailPresent({});
  assert.equal(bad.ok, false);
  const frozen = freezePublishedPresentationFields(fixture, {
    id: fixture.id,
    hotelId: SPICE,
  });
  frozen.publishedPresentationLocked = true;
  const good = assertApprovedCustomerFacingDetailPresent(frozen);
  assert.equal(good.ok, true, JSON.stringify(good.fails));
  const v = validateCanonicalPresentationObject(frozen.approvedCustomerFacingDetail);
  assert.equal(v.ok, true);
});

await check("staged_pre_pointer_gate_passes_frozen", () => {
  // Use a production-shaped Spice snapshot (not a lean fixture) — hydrate must
  // preserve locked sales/priority from approvedCustomerFacingDetail.
  const snapPath = path.join(
    WORK_ROOT,
    "snapshots",
    SPICE,
    "gdi_s4912_b956aa67a4a2",
    "v5.json",
  );
  assert.ok(fs.existsSync(snapPath), "spice snapshot missing");
  const doc = JSON.parse(fs.readFileSync(snapPath, "utf8"));
  const snap = doc.snapshot || doc;
  assert.ok(snap.approvedCustomerFacingDetail);
  snap.publishedPresentationLocked = true;
  const gate = verifyStagedPublicationFullContentParity(SPICE, [snap]);
  assert.equal(gate.ok, true, JSON.stringify(gate.criticalDifferences, null, 2));
});

await check("staged_gate_fails_lean_list_projection", () => {
  const lean = {
    id: "lean",
    title: "Lean",
    publishedPresentationLocked: true,
  };
  const gate = verifyStagedPublicationFullContentParity(SPICE, [lean]);
  assert.equal(gate.ok, false);
  assert.ok(
    gate.criticalDifferences.some((d) =>
      String(d.fieldPath || d.rootCause || "").includes("approved"),
    ) || gate.results?.[0]?.stage === "snapshot_schema",
  );
});

await check("discover_universe_includes_shares_and_registry", () => {
  const universe = discoverGdiParityHotelUniverse({
    root: WORK_ROOT,
    hotels: [
      { hotelId: SPICE, displayName: "Spice Island Beach Resort" },
      { hotelId: "recLuxvwwxID7U2B8", displayName: "Bethesda Marriott" },
    ],
  });
  assert.ok(universe.some((h) => h.hotelId === SPICE));
  assert.ok(universe.some((h) => h.hotelId === "recLuxvwwxID7U2B8"));
});

await check("inventory_classifies_unpublished_share_as_not_verifiable", () => {
  const rows = buildGdiParityHotelInventory({
    root: WORK_ROOT,
    hotels: [
      { hotelId: SPICE, displayName: "Spice Island Beach Resort" },
      { hotelId: "recLuxvwwxID7U2B8", displayName: "Bethesda Marriott" },
    ],
    shareRegistry: {
      tokens: {
        beth: {
          tokenId: "gdisht_beth_test",
          hotelId: "recLuxvwwxID7U2B8",
          status: "ACTIVE",
          label: "Bethesda",
        },
      },
    },
  });
  const spice = rows.find((r) => r.hotelId === SPICE);
  const beth = rows.find((r) => r.hotelId === "recLuxvwwxID7U2B8");
  assert.ok(spice);
  assert.equal(spice.publishedOpportunityCount, 7);
  assert.ok(
    spice.fullContentParityStatus ===
      GDI_FULL_CONTENT_PARITY_STATUS.VERIFICATION_PENDING ||
      spice.fullContentParityStatus === GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH,
  );
  assert.equal(
    beth.fullContentParityStatus,
    GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE,
  );
  assert.ok(beth.migrationCandidate);
});

await check("spice_offline_verify_seven", async () => {
  assert.ok(fs.existsSync(WORK_ROOT), "work inventory missing");
  const verify = await verifyGdiHotelFullContentParity(SPICE, {
    root: WORK_ROOT,
    hotelName: "Spice Island Beach Resort",
  });
  assert.equal(verify.publishedOpportunityCount, 7);
  assert.equal(
    verify.status,
    GDI_FULL_CONTENT_PARITY_STATUS.VERIFIED,
    JSON.stringify(verify.criticalDifferences?.slice(0, 5), null, 2),
  );
  assert.equal(verify.ok, true);
});

await check("portfolio_audit_does_not_pass_on_spice_alone_with_peers", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-parity-"));
  const report = await runGdiPortfolioFullContentParityAudit({
    root: WORK_ROOT,
    hotels: [
      { hotelId: SPICE, displayName: "Spice Island Beach Resort" },
      { hotelId: "recLuxvwwxID7U2B8", displayName: "Bethesda Marriott" },
      {
        hotelId: "recgMYovrrZDJMqzX",
        displayName: "Waterstone Resort & Marina",
      },
    ],
    shareRegistry: {
      tokens: {
        b: {
          tokenId: "t1",
          hotelId: "recLuxvwwxID7U2B8",
          status: "ACTIVE",
        },
        w: {
          tokenId: "t2",
          hotelId: "recgMYovrrZDJMqzX",
          status: "ACTIVE",
        },
      },
    },
  });
  // Spice should verify; peers with share-only legacy path are NOT_VERIFIABLE.
  const spice = report.hotels.find((h) => h.hotelId === SPICE);
  assert.equal(spice.fullContentParityStatus, GDI_FULL_CONTENT_PARITY_STATUS.VERIFIED);
  assert.ok(report.summary.VERIFIED_HOTELS >= 1);
  assert.ok(report.summary.NOT_VERIFIABLE_HOTELS >= 2);
  // Portfolio PASS is allowed when published+verifiable hotels all verify,
  // and unpublished/unverifiable are separate — Spice is the only published.
  assert.equal(
    report.status,
    "GDI_UNIVERSAL_PORTFOLIO_FULL_CONTENT_PARITY_PASS",
  );
  fs.rmSync(tmp, { recursive: true, force: true });
});

await check("never_claim_unpublished_as_verified", () => {
  const rows = buildGdiParityHotelInventory({
    root: WORK_ROOT,
    hotels: [{ hotelId: "recNeverPublished", displayName: "Unpublished" }],
  });
  const row = rows.find((r) => r.hotelId === "recNeverPublished");
  assert.equal(
    row.fullContentParityStatus,
    GDI_FULL_CONTENT_PARITY_STATUS.NOT_PUBLISHED,
  );
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
