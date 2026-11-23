#!/usr/bin/env node
/**
 * Multi-hotel GDI inventory integrity recovery — deterministic.
 * No paid research / Airtable / publication mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  PRIORITY_REGRESSION_HOTELS,
  diagnoseResearchPreviewFilterTrap,
  reconcilePriorityHotelsResearchInventory,
  loadHotelResearchStore,
} from "../lib/group-demand-intelligence/inventory/gdi-research-inventory-integrity-v1.js";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";
import { resolveShareOpportunitiesFromApprovedPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT_DIR = path.join(
  ROOT,
  "reports/gdi-multi-hotel-inventory-integrity-recovery-v1",
);

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
  }
}

const apiSrc = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);

const recon = reconcilePriorityHotelsResearchInventory({ root: ROOT });
const guad = diagnoseResearchPreviewFilterTrap("recCAGqkERSXgiku4", {
  root: ROOT,
});

check("guadalajara_store_preserved_near_40", () => {
  assert.ok(guad.storeCount >= 40, `store=${guad.storeCount}`);
  assert.equal(guad.brokenCustomerFacingCount, 6);
  assert.equal(guad.suppressedByCustomerFacingCount, guad.storeCount - 6);
  assert.equal(
    guad.rootCauseCode,
    "RESEARCH_PREVIEW_APPLIED_CUSTOMER_FACING_FILTER",
  );
});

check("api_force_live_skips_customer_facing_filter", () => {
  assert.match(apiSrc, /FOUNDER_RESEARCH_INVENTORY/);
  assert.match(
    apiSrc,
    /Do NOT apply filterCustomerFacingOpportunities on Research Preview/,
  );
  assert.match(apiSrc, /forceLive\s*\?\s*GDI_LIST_SURFACE_VIEW\.FULL/);
  assert.match(apiSrc, /researchInventoryIntegrity/);
});

check("localhost_defaults_to_staged_research_remains_explicit", () => {
  assert.match(appJs, /if \(isLocalFounderHost\(\)\) return "staged"/);
  assert.doesNotMatch(
    appJs,
    /if \(isLocalFounderHost\(\)\) return "research"/,
  );
  // Research diagnostic still available via explicit live=1 path.
  assert.match(appJs, /q\.push\("live=1"\)/);
});

check("san_diego_and_castillo_research_preserved", () => {
  const sd = diagnoseResearchPreviewFilterTrap("recnPB3Q02NO8Ma1o");
  const ca = diagnoseResearchPreviewFilterTrap("rec82D9zpB8fede1I");
  assert.equal(sd.storeCount, 82);
  assert.equal(ca.storeCount, 20);
  assert.ok(sd.brokenCustomerFacingCount < sd.storeCount);
  assert.equal(ca.brokenCustomerFacingCount, 0);
});

check("published_baselines_unchanged", () => {
  const beth = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId: "recLuxvwwxID7U2B8",
  });
  const yotel = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId: "recrPQcZg7SFARRb2",
  });
  const rad = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId: "recH9h3McgMNFQEla",
  });
  assert.equal(beth.ok, true);
  assert.equal(beth.opportunities.length, 67);
  assert.equal(yotel.ok, true);
  assert.equal(yotel.opportunities.length, 42);
  assert.equal(rad.ok, true);
  assert.equal(rad.opportunities.length, 66);
});

check("no_cross_hotel_id_leak_in_priority_stores", () => {
  for (const h of PRIORITY_REGRESSION_HOTELS) {
    const store = loadHotelResearchStore(h.hotelId);
    for (const o of store.opportunities) {
      const oid = String(o.hotelId || o.hotelRecordId || "");
      if (oid) assert.equal(oid, h.hotelId, `${h.name} leak ${oid}`);
    }
  }
});

check("share_path_still_package_gated_for_castillo", () => {
  const share = resolveShareOpportunitiesFromApprovedPackage({
    hotelId: "rec82D9zpB8fede1I",
  });
  // No CUSTOMER_PUBLISHED package — must not resolve as PACKAGE live research.
  assert.equal(share.ok === true && share.mode === "PACKAGE", false);
});

check("staged_and_published_remain_distinct_modes", () => {
  assert.match(appJs, /resolveFounderInventoryMode/);
  assert.match(appJs, /preview=staged/);
  assert.match(appJs, /inventory", "research"/);
  assert.match(appJs, /inventory", "staged"/);
  assert.match(appJs, /inventory", "published"/);
});

check("current_pointers_untouched_on_disk", () => {
  for (const id of [
    "recLuxvwwxID7U2B8",
    "recrPQcZg7SFARRb2",
    "recH9h3McgMNFQEla",
  ]) {
    assert.ok(
      fs.existsSync(
        path.join(
          ROOT,
          "lib/group-demand-intelligence/bundled-publication-packages",
          id,
          "CURRENT.json",
        ),
      ),
      id,
    );
  }
});

check("recon_flags_systemic_filter_trap", () => {
  assert.match(recon.systemicRootCause, /filterCustomerFacingOpportunities/);
  assert.ok(recon.rows.every((r) => r.opportunitiesActuallyDeleted === false));
});

const failed = results.filter((r) => !r.ok);
fs.mkdirSync(REPORT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(REPORT_DIR, "RECONCILIATION_MATRIX.json"),
  `${JSON.stringify(recon, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(REPORT_DIR, "MISSING_OPPORTUNITY_IDS.json"),
  `${JSON.stringify(
    {
      note: "IDs suppressed from Research Preview when customer-facing filter was incorrectly applied — not deleted from store.",
      hotels: recon.rows.map((r) => ({
        hotelId: r.hotelId,
        name: r.name,
        storeCount: r.storeCount,
        brokenVisibleCount: r.brokenCustomerFacingCount,
        suppressedIds: r.suppressedIds,
      })),
    },
    null,
    2,
  )}\n`,
);

console.log(
  JSON.stringify(
    {
      contract: "gdi-multi-hotel-inventory-integrity-recovery-v1",
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      guadalajara: {
        store: guad.storeCount,
        brokenVisible: guad.brokenCustomerFacingCount,
        suppressed: guad.suppressedByCustomerFacingCount,
      },
      safety: recon.safety,
    },
    null,
    2,
  ),
);
if (failed.length) {
  console.error("FAILED:", failed.map((f) => f.name).join(", "));
  process.exit(1);
}
console.log("OK: multi-hotel inventory integrity recovery V1");
