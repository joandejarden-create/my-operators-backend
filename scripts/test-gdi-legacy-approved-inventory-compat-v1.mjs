#!/usr/bin/env node
import assert from "node:assert/strict";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  isLegacyApprovedPublicationPreview,
  filterLegacyApprovedPackageOpportunities,
  GDI_LEGACY_APPROVED_INVENTORY_COMPAT_V1,
} from "../lib/group-demand-intelligence/publication-package/gdi-legacy-approved-inventory-compat-v1.js";
import { filterPackageOpportunitiesToFqo } from "../lib/group-demand-intelligence/publication-package/gdi-founder-fqo-presentation-filter-v1.js";

assert.equal(
  isLegacyApprovedPublicationPreview({ previewSource: "STAGED" }),
  true,
);
assert.equal(
  isLegacyApprovedPublicationPreview({ previewSource: "CURRENT" }),
  true,
);
assert.equal(
  isLegacyApprovedPublicationPreview({
    previewSource: "CANDIDATE_CERTIFIED_NOT_ACTIVATED",
  }),
  false,
);

const soft = filterLegacyApprovedPackageOpportunities([
  {
    id: "keep_1",
    title: "Real conference",
    primaryContact: { name: "Alex Morgan", email: "alex@example.org" },
  },
  {
    id: "drop_exhibitor",
    title: "Something — EXHIBITOR BLOCK",
    primaryContact: { name: "" },
  },
  {
    id: "drop_rejected",
    title: "Rejected deal",
    founderRejected: true,
    primaryContact: { name: "Pat" },
  },
]);
assert.equal(soft.afterCount, 1);
assert.equal(soft.opportunities[0].id, "keep_1");
assert.ok(GDI_LEGACY_APPROVED_INVENTORY_COMPAT_V1.includes("legacy"));

const store = createGdiPublicationPackageStore();
const fixtures = [
  {
    id: "recLuxvwwxID7U2B8",
    name: "Bethesda",
    min: 67,
  },
  {
    id: "recH9h3McgMNFQEla",
    name: "RadissonToronto",
    min: 66,
  },
  {
    id: "recrPQcZg7SFARRb2",
    name: "YOTEL",
    min: 42,
  },
  {
    id: "recXh4NgF8sDbi9cQ",
    name: "WyndhamAlltra",
    min: 27,
  },
];

for (const f of fixtures) {
  const ptr = store.readPointer(f.id, "STAGED") || store.readPointer(f.id, "CURRENT");
  assert.ok(ptr?.publicationVersion, `${f.name} missing STAGED/CURRENT`);
  const loaded = store.loadPackage(f.id, ptr.publicationVersion);
  assert.equal(loaded.ok, true, `${f.name} package load`);
  const opps = loaded.package.opportunities || [];
  assert.ok(opps.length >= f.min, `${f.name} expected >= ${f.min}, got ${opps.length}`);
  const legacy = filterLegacyApprovedPackageOpportunities(opps);
  const fqo = filterPackageOpportunitiesToFqo(opps);
  // Legacy compat must not silently wipe approved inventories the way strict FQO does.
  assert.ok(
    legacy.afterCount >= Math.min(f.min, opps.length) - 5,
    `${f.name} legacy compat lost too many rows: ${legacy.afterCount}`,
  );
  assert.ok(
    fqo.afterCount < legacy.afterCount || fqo.afterCount === 0,
    `${f.name} expected strict FQO to be stricter than legacy compat`,
  );
  console.log(
    `  PASS ${f.name} package=${opps.length} legacy=${legacy.afterCount} strictFqo=${fqo.afterCount}`,
  );
}

// Hilton paid-6 remains FQO-gated as a candidate (not legacy STAGED).
const hiltonPaid = store.loadPackage(
  "rec35fExUxCClpOP6",
  "gdi_hilton_ts_paid_research_v1_20261124",
);
assert.equal(hiltonPaid.ok, true);
assert.equal((hiltonPaid.package.opportunities || []).length, 6);
console.log("  PASS Hilton paid-6 fixture intact");

console.log("\nOK test-gdi-legacy-approved-inventory-compat-v1");
