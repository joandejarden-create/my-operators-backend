#!/usr/bin/env node
/**
 * Single inventory + local/publication manifest parity.
 * STOP before production — no deploy, Airtable, or volume writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { mergeLocalShadowCommercialOpportunities } from "../lib/group-demand-intelligence/local-shadow-commercial-portfolio-v1.js";
import { mergePublishedInventoryIntoOpportunities } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import {
  selectDisplayEligibleOpportunities,
  selectPublicationEligibleOpportunities,
  buildPublicationManifest,
  assertManifestParity,
  reportLocalPublishedInventoryDiff,
  GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
} from "../lib/group-demand-intelligence/commercial-inventory/gdi-canonical-display-eligibility-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-unified-commercial-inventory-local-production-v1",
);

const SPICE = "recKRJjcPnb4tVDDS";
const AVENTA = "gdi_sc_f255eb5e9202";
const SPICE_SEVEN = Object.freeze([
  "gdi_s4912_ba7b1cb8a3ab",
  "gdi_s4912_b956aa67a4a2",
  "gdi_s4912_b6d9b1929c05",
  "gdi_s4912_cb2d71ab1b15",
  "gdi_s4912_cc2a8da6ca6e",
  "gdi_s4912_03deda917778",
  "gdi_s4912_3cafec77a21f",
]);

const HOTELS = Object.freeze([
  { hotelId: SPICE, name: "Spice Island Beach Resort", role: "primary" },
  { hotelId: "recLuxvwwxID7U2B8", name: "Bethesda Marriott", role: "peer" },
  {
    hotelId: "recgMYovrrZDJMqzX",
    name: "Waterstone Resort & Marina Boca Raton",
    role: "peer",
  },
  { hotelId: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside", role: "peer" },
  {
    hotelId: "recC2kbCaS48jFjFF",
    name: "Nobu Hotel London Portman Square",
    role: "peer",
  },
  { hotelId: "recrPQcZg7SFARRb2", name: "YOTEL Geneva Lake", role: "peer" },
  {
    hotelId: "recIEcSWuhSCoNLFN",
    name: "Fiesta Americana Condesa Cancún All Inclusive",
    role: "peer",
  },
]);

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

function loadBaseOpps(hotelId) {
  const candidates = [
    path.join(ROOT, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json"),
    path.join(ROOT, "data/group-demand-intelligence", hotelId, "opportunities.json"),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      const doc = JSON.parse(fs.readFileSync(p, "utf8"));
      return Array.isArray(doc)
        ? doc
        : Array.isArray(doc.opportunities)
          ? doc.opportunities
          : [];
    }
  }
  return [];
}

/** Same merge path as API loadOppDoc (local shadow + published overlays). */
function loadOpps(hotelId) {
  let rows = loadBaseOpps(hotelId);
  rows = mergeLocalShadowCommercialOpportunities(hotelId, rows);
  rows = mergePublishedInventoryIntoOpportunities(hotelId, rows, {
    strictPublishedOnly: false,
  });
  return rows;
}

check("ui_dual_inventory_removed", () => {
  const ui = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8",
  );
  const app = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8",
  );
  const api = fs.readFileSync(
    path.join(ROOT, "api/group-demand-intelligence.js"),
    "utf8",
  );
  assert.ok(ui.includes("Dual inventory UI removed"));
  assert.ok(!ui.includes('data-gdi-inventory-view="research"'));
  assert.ok(!app.includes("onInventoryViewChange"));
  assert.ok(!app.includes("inventoryScope="));
  assert.ok(app.includes('inventoryViewLabel: "Opportunities"') || app.includes('"Opportunities"'));
  assert.ok(api.includes("selectDisplayEligibleOpportunities"));
  assert.ok(api.includes("CANONICAL_DISPLAY"));
  assert.ok(!api.includes('inventoryScope === "research"'));
});

check("spice_display_equals_publication_seven", () => {
  const raw = loadOpps(SPICE);
  const facing = filterCustomerFacingOpportunities(raw, { hotelId: SPICE });
  const display = selectDisplayEligibleOpportunities(SPICE, facing);
  const publication = selectPublicationEligibleOpportunities(SPICE, facing);
  assert.equal(display.count, 7);
  assert.equal(publication.count, 7);
  assert.deepEqual(
    display.opportunityIds.slice().sort(),
    publication.opportunityIds.slice().sort(),
  );
  assert.deepEqual(display.opportunityIds.slice().sort(), [...SPICE_SEVEN].sort());
  assert.ok(!display.opportunityIds.includes(AVENTA));
  assert.equal(display.classCounts.SALES_READY, 1);
  assert.equal(display.classCounts.ACCOUNT_DEVELOPMENT, 6);
  assert.equal(display.classCounts.OTHER, 0);
});

check("spice_aventa_excluded_preserved_internally", () => {
  const raw = loadOpps(SPICE);
  assert.ok(raw.some((o) => String(o.id) === AVENTA), "Aventa must remain in research ledger");
  const facing = filterCustomerFacingOpportunities(raw, { hotelId: SPICE });
  const display = selectDisplayEligibleOpportunities(SPICE, facing);
  assert.ok(!display.opportunityIds.includes(AVENTA));
});

check("spice_manifest_parity_self", () => {
  const raw = loadOpps(SPICE);
  const facing = filterCustomerFacingOpportunities(raw, { hotelId: SPICE });
  const built = buildPublicationManifest(SPICE, facing);
  assert.equal(built.manifest.opportunityCount, 7);
  const parity = assertManifestParity(built.manifest, facing, { hotelId: SPICE });
  assert.equal(parity.ok, true, JSON.stringify(parity.diffs, null, 2));
});

check("display_contract_constant", () => {
  assert.equal(
    GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
    "gdi-canonical-display-eligibility-contract-v1",
  );
});

const hotelDiffs = [];
for (const h of HOTELS) {
  check(`hotel_display_publication_same_ids_${h.hotelId}`, () => {
    const raw = loadOpps(h.hotelId);
    assert.ok(raw.length > 0, `missing local opportunities for ${h.name}`);
    const facing = filterCustomerFacingOpportunities(raw, { hotelId: h.hotelId });
    const display = selectDisplayEligibleOpportunities(h.hotelId, facing);
    const publication = selectPublicationEligibleOpportunities(h.hotelId, facing);
    assert.deepEqual(
      display.opportunityIds.slice().sort(),
      publication.opportunityIds.slice().sort(),
    );
    const diff = reportLocalPublishedInventoryDiff(h.hotelId, facing);
    hotelDiffs.push({
      hotelId: h.hotelId,
      name: h.name,
      role: h.role,
      localDisplayCount: diff.localDisplayCount,
      publishedRegistryCount: diff.publishedRegistryCount,
      match: diff.match,
      localOnly: diff.localOnly,
      publishedOnly: diff.publishedOnly,
      classCounts: diff.classCounts,
      proposedReconciliation: diff.proposedReconciliation,
    });
  });
}

const failed = results.filter((r) => !r.ok);
const spiceFacing = filterCustomerFacingOpportunities(loadOpps(SPICE), {
  hotelId: SPICE,
});
const spiceManifest = buildPublicationManifest(SPICE, spiceFacing);
const spiceDisplay = selectDisplayEligibleOpportunities(SPICE, spiceFacing);

const report = {
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
  spiceIsland: {
    localDisplayCount: spiceDisplay.count,
    publicationEligibleCount: spiceDisplay.count,
    opportunityIds: spiceDisplay.opportunityIds,
    classCounts: spiceDisplay.classCounts,
    aventaExcluded: !spiceDisplay.opportunityIds.includes(AVENTA),
    manifestOk: spiceManifest.ok,
    manifestHash: spiceManifest.manifest.manifestHash,
    manifestParitySelf: assertManifestParity(spiceManifest.manifest, spiceFacing, {
      hotelId: SPICE,
    }).ok,
  },
  sevenHotelLocalPublishedDiffs: hotelDiffs,
  contract: GDI_CANONICAL_DISPLAY_ELIGIBILITY_CONTRACT_V1,
  stopBeforeProduction: true,
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "07-single-inventory-manifest-parity-results.json"),
  JSON.stringify(report, null, 2),
);
fs.writeFileSync(
  path.join(OUT, "08-spice-publication-manifest.json"),
  JSON.stringify(spiceManifest, null, 2),
);

console.log(JSON.stringify(report, null, 2));
process.exit(failed.length ? 1 : 0);
