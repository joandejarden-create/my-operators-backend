/**
 * GDI visual SVG icon color regression — Research Preview must restore
 * class+sales-key backed gold icons after thesis scrub (same policy as PACKAGE).
 *
 * Run: npm run test:gdi-visual-svg-icon-color-regression-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import {
  runOpportunityRevenueStreamIconsGate,
  resolveOpportunityRevenueStreamIcons,
  restoreIconsFromPersistedClassAndSalesKeys,
  REVENUE_STREAM_ICON_STATE,
} from "../lib/group-demand-intelligence/opportunity-revenue-stream-icons-v1.js";
import { sanitizeOpportunityForCustomerPresentation } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";
import { HOTEL_REVENUE_STREAM_CLASS } from "../lib/group-demand-intelligence/multi-revenue-qualification-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

check("revenue_stream_icon_gate_includes_thesis_scrub_restore_cases", () => {
  const gate = runOpportunityRevenueStreamIconsGate();
  assert.equal(gate.pass, true, JSON.stringify(gate.failures, null, 2));
  const ids = new Set(gate.results.map((r) => r.id));
  assert.ok(ids.has("persisted_class_sales_keys_without_thesis_restores_icons"));
  assert.ok(ids.has("persisted_combined_sales_keys_without_thesis_restores_both"));
  assert.ok(ids.has("persisted_class_without_sales_keys_stays_muted"));
});

check("restore_helper_requires_sales_key_corroboration", () => {
  const ok = restoreIconsFromPersistedClassAndSalesKeys({
    revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
    salesOpportunityQuickFilterKeys: ["GUEST_ROOMS"],
  });
  assert.ok(ok);
  assert.equal(ok.revenueStreamIcons.guestrooms, REVENUE_STREAM_ICON_STATE.ACTIVE);
  assert.equal(ok.revenueStreamIcons.eventFnb, REVENUE_STREAM_ICON_STATE.MUTED);

  const blocked = restoreIconsFromPersistedClassAndSalesKeys({
    revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
    salesOpportunityQuickFilterKeys: [],
  });
  assert.equal(blocked, null);
});

check("live_sanitize_restores_guadalajara_class_backed_icons", () => {
  const storePath = path.join(
    "data",
    "group-demand-intelligence",
    "hotels",
    "recCAGqkERSXgiku4",
    "opportunities.json",
  );
  assert.ok(fs.existsSync(storePath), "Guadalajara research store missing");
  const ops = JSON.parse(fs.readFileSync(storePath, "utf8")).opportunities || [];
  assert.ok(ops.length >= 40, `expected ~43 research rows, got ${ops.length}`);

  let activeG = 0;
  let activeE = 0;
  let muted = 0;
  for (const raw of ops) {
    const sanitized = sanitizeOpportunityForCustomerPresentation({
      ...raw,
      hotelId: "recCAGqkERSXgiku4",
      title: raw.title || raw.name || raw.organizationName || "Opportunity",
    });
    const icons =
      sanitized.revenueStreamIcons ||
      sanitized.canonicalCustomerPresentation?.icons ||
      {};
    if (icons.guestrooms === "active") activeG += 1;
    if (icons.eventFnb === "active") activeE += 1;
    if (icons.guestrooms !== "active" && icons.eventFnb !== "active") muted += 1;
  }

  // Historical SVG repair baseline for this inventory: 43 guest / 25 catering.
  assert.equal(activeG, 43, `guestroom-active expected 43, got ${activeG}`);
  assert.equal(activeE, 25, `catering-active expected 25, got ${activeE}`);
  assert.equal(muted, 0, `muted-both expected 0, got ${muted}`);
});

check("package_and_live_restore_agree_on_thin_class_sales_row", () => {
  const thin = {
    id: "thin_class_sales",
    title: "Thin Class Sales Row",
    revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB,
    salesOpportunityQuickFilterKeys: ["GUEST_ROOMS", "LOCAL_CATERING", "BOTH"],
    guestroomThesis: "",
    eventFnbThesis: "",
  };
  const live = resolveOpportunityRevenueStreamIcons(thin);
  const pkg = attachPackageCustomerPresentation(thin);
  assert.equal(live.icons.guestrooms, "active");
  assert.equal(live.icons.eventFnb, "active");
  assert.equal(pkg.revenueStreamIcons.guestrooms, "active");
  assert.equal(pkg.revenueStreamIcons.eventFnb, "active");
  assert.equal(
    live.revenueStreamClass,
    HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB,
  );
});

check("css_active_revenue_stream_still_uses_gold_token", () => {
  const css = fs.readFileSync(
    path.join("public", "css", "group-demand-intelligence.css"),
    "utf8",
  );
  assert.match(css, /\.gdi-revenue-stream-icon--active\s*\{[^}]*color:\s*var\(--secondary--color-5\)/s);
  assert.match(css, /--secondary--color-5:\s*#fdb52a/);
  assert.match(css, /\.gdi-revenue-stream-icon--muted/);
});

if (failed) {
  console.error(`\n${failed} failing assertion(s)`);
  process.exit(1);
}
console.log("\nAll GDI visual SVG icon color regression checks passed.");
