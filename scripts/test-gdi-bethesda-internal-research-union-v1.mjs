/**
 * Bethesda internal 59+29 read-time union — no store write, no share leak.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import {
  mergeBethesdaInternalResearchOpportunities,
  buildBethesda88UnionDryRunManifest,
  findBethesdaInternalResearchOpportunity,
  BETHESDA_INTERNAL_LIST_SCOPE,
} from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const checks = [];
function check(name, fn) {
  try {
    fn();
    checks.push({ name, ok: true });
  } catch (err) {
    checks.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const oppPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "opportunities.json"
);
const before = crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex");
const doc = await loadOpportunitiesCanonical(HOTEL);
const canonical = doc.opportunities || [];

const union = mergeBethesdaInternalResearchOpportunities(HOTEL, canonical, {
  root: ROOT,
});
const manifest = buildBethesda88UnionDryRunManifest({
  root: ROOT,
  canonicalOpportunities: canonical,
});

check("union_count_88", () => {
  assert.equal(union.applied, true);
  assert.equal(union.opportunities.length, 88);
  assert.equal(union.scope, BETHESDA_INTERNAL_LIST_SCOPE);
  assert.equal(canonical.length, 59);
  assert.equal(union.counts.historicalAdded, 29);
});

check("unique_ids_no_overlap_inflation", () => {
  const ids = union.opportunities.map((o) => o.id || o.opportunityId);
  assert.equal(new Set(ids).size, 88);
  const hist = ids.filter((id) => BETHESDA_29_ATTESTED_IDS.includes(id));
  assert.equal(hist.length, 29);
  assert.equal(manifest.counts.overlap, 0);
  assert.equal(manifest.counts.union, 88);
});

check("historical_not_sales_ready", () => {
  for (const id of BETHESDA_29_ATTESTED_IDS) {
    const row = union.opportunities.find((o) => (o.id || o.opportunityId) === id);
    assert.ok(row, id);
    assert.equal(row.salesReady, false);
    assert.equal(row.customerFacing, false);
    assert.equal(row.customerVisible, false);
    assert.equal(row.bethesdaInternalResearchInventory, true);
    assert.notEqual(String(row.commercialMaturity || ""), "SALES_READY");
  }
});

check("customer_facing_filter_excludes_historical", () => {
  const facing = filterCustomerFacingOpportunities(union.opportunities, {
    hotelId: HOTEL,
  });
  const leaked = facing.filter((o) =>
    BETHESDA_29_ATTESTED_IDS.includes(o.id || o.opportunityId)
  );
  assert.equal(leaked.length, 0, "historical rows must not pass customer-facing filter");
});

check("detail_lookup_finds_historical", () => {
  const sample = BETHESDA_29_ATTESTED_IDS[0];
  const found = findBethesdaInternalResearchOpportunity(HOTEL, sample, {
    root: ROOT,
  });
  assert.ok(found);
  assert.equal(found.id, sample);
});

check("non_bethesda_unchanged", () => {
  const other = mergeBethesdaInternalResearchOpportunities("recOTHER", [
    { id: "a" },
  ]);
  assert.equal(other.applied, false);
  assert.equal(other.opportunities.length, 1);
});

check("store_hash_unchanged_and_no_write_flag", () => {
  const after = crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex");
  assert.equal(after, before);
  assert.equal(
    before,
    "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad"
  );
  assert.equal(manifest.opportunityStore.writesAuthorized, false);
  assert.equal(manifest.opportunityStore.writeApplied, false);
});

check("api_wiring_auth_only", () => {
  const api = fs.readFileSync(
    path.join(ROOT, "api/group-demand-intelligence.js"),
    "utf8"
  );
  assert.match(api, /mergeBethesdaInternalResearchOpportunities/);
  assert.match(api, /BETHESDA_INTERNAL_LIST_SCOPE|bethesdaInternalUnion/);
  // Share handlers must not call the union merger.
  const shareSlice = api.slice(api.indexOf("export async function getGdiShareOpportunities"));
  assert.ok(!/mergeBethesdaInternalResearchOpportunities/.test(shareSlice.slice(0, 2500)));
});

check("share_surfaces_do_not_reference_union", () => {
  const share = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  assert.ok(!/bethesdaInternalUnion|INTERNAL_UNION_88/.test(share));
});

const failed = checks.filter((c) => !c.ok);
const outDir = path.join(ROOT, "reports", "gdi-bethesda-internal-research-union-v1");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "00-dry-run-merge-manifest.json"),
  `${JSON.stringify(manifest, null, 2)}\n`
);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      before,
      unionCount: union.opportunities.length,
      checks,
      manifestPath: "reports/gdi-bethesda-internal-research-union-v1/00-dry-run-merge-manifest.json",
    },
    null,
    2
  )
);
if (failed.length) process.exit(1);
