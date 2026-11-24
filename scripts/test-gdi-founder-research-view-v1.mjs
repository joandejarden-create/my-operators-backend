#!/usr/bin/env node
/**
 * Permanent $0 CI: Founder Research View R1.
 *   npm run test:gdi-founder-research-view-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { assertFounderResearchAccess } from "../lib/group-demand-intelligence/inventory/gdi-founder-research-access-v1.js";
import { inspectFounderPreviewPublicationAvailability } from "../lib/group-demand-intelligence/publication-package/gdi-founder-preview-availability-v1.js";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = process.env.GDI_AUDIT_BASE || "http://localhost:8080";

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}
async function checkAsync(name, fn) {
  await fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const apiJs = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
const css = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8",
);

const AFFECTED = [
  ["rec35fExUxCClpOP6", "Hilton New York Times Square", 45, 44],
  ["rec2PVBDavppGpenm", "AC Hotel A Coruña", null, null],
  ["recIwaP1etgx2g9nA", "Cambridge Beaches Resort & Spa", null, null],
  ["rec82D9zpB8fede1I", "Castillo Hotel Son Vida", null, null],
  ["recGkME49yYuxQl0u", "NOW NOW NOHO", null, null],
  ["recjhQdAUiSyxfCqE", "Sheraton Mallorca Arabella", null, null],
  ["recFaxTEFF9ILHWC9", "The Westin Grand München", null, null],
];

const STAGED_REGRESSION = [
  ["rece0NdV9FCnaFlVS", "Condesa DF", 4],
  ["recaokE8psIbuVeuH", "LaLuna", 3],
  ["rec1t35e3sPerzxYu", "Memmo Príncipe Real", 11],
  ["recLuxvwwxID7U2B8", "Bethesda Marriott", 67],
];

check("1_ui_contract_missing_staged_research_available", () => {
  assert.match(appJs, /Research available — no finalized package yet/);
  assert.match(appJs, /View Research Opportunities/);
  assert.match(appJs, /data-gdi-recovery="view-research"/);
  assert.match(appJs, /founderInventorySummaryHtml/);
  assert.match(appJs, /founderLifecycleNavHtml/);
  assert.match(appJs, /RESEARCH VIEW — NOT FINALIZED/);
  assert.match(appJs, /Finalized Opportunities/);
  assert.match(appJs, /Research Opportunities/);
  assert.match(css, /\.gdi-inventory-summary/);
  assert.match(css, /\.gdi-lifecycle-nav/);
  assert.match(css, /\.gdi-research-view-banner/);
  assert.match(html, /gdi-founder-research-view-v1/);
  assert.match(appJs, /_gdiLifecycleDelegated/);
  assert.match(appJs, /data-gdi-lifecycle-view/);
  assert.match(appJs, /data-gdi-recovery=\"view-research\"/);
});

check("2_staged_first_default_preserved", () => {
  assert.match(appJs, /if \(isLocalFounderHost\(\)\) return "staged"/);
  assert.match(appJs, /Never as a silent substitute for STAGED|never as a silent substitute for STAGED/i);
});

check("3_no_research_to_staged_or_current_promotion", () => {
  assert.doesNotMatch(appJs, /writePointer\(['\"]STAGED/);
  assert.doesNotMatch(appJs, /writePointer\(['\"]CURRENT/);
  assert.match(
    apiJs,
    /Refusing to fall back to another hotel, CURRENT published inventory, or live research/,
  );
});

check("4_server_research_access_gate", () => {
  assert.match(apiJs, /assertFounderResearchAccess/);
  assert.match(
    apiJs,
    /gdi-founder-research-access-v1\.js/,
  );
  const denied = assertFounderResearchAccess({
    gdiExternalShare: true,
    dealalityUser: { id: "x" },
  });
  assert.equal(denied.ok, false);
  assert.equal(denied.error, "UNAUTHORIZED_RESEARCH_ACCESS");
  const pilotOk = assertFounderResearchAccess({
    gdiPilotUnauthRead: true,
  });
  assert.equal(pilotOk.ok, true);
  const userOk = assertFounderResearchAccess({
    dealalityUser: { id: "founder" },
  });
  assert.equal(userOk.ok, true);
  const bare = assertFounderResearchAccess({});
  assert.equal(bare.ok, false);
  assert.equal(bare.error, "UNAUTHORIZED_RESEARCH_ACCESS");
});

check("5_recovery_includes_view_research_action", () => {
  const avail = inspectFounderPreviewPublicationAvailability({
    hotelId: "rec35fExUxCClpOP6",
    researchSummary: { discovered: 45, qualified: 44, hasResearch: true },
  });
  assert.equal(avail.hasStagedPublication, false);
  assert.ok(avail.recoveryActions.includes("VIEW_RESEARCH_INVENTORY"));
  assert.equal(avail.researchSummary.discovered, 45);
});

check("6_staged_packages_unchanged_counts", () => {
  for (const [id, name, expect] of STAGED_REGRESSION) {
    const pkg = resolvePreviewOpportunitiesFromPublicationPackage({
      hotelId: id,
      forceLive: false,
      preferStaged: true,
      preview: "staged",
    });
    assert.equal(pkg.ok, true, name);
    assert.equal(pkg.mode, "PACKAGE", name);
    assert.equal(
      String(pkg.meta?.previewSource || "").toUpperCase(),
      "STAGED",
      name,
    );
    assert.equal((pkg.opportunities || []).length, expect, name);
  }
});

check("7_export_lifecycle_params_in_ui", () => {
  assert.match(appJs, /inventory=research/);
  assert.match(appJs, /preview=staged/);
  assert.match(apiJs, /RESEARCH_NOT_FINALIZED/);
  assert.match(apiJs, /Finalized export unavailable for the selected lifecycle/);
});

await checkAsync("8_live_api_hilton_research_accessible", async () => {
  const staged = await fetch(
    `${BASE}/api/group-demand-intelligence/hotels/rec35fExUxCClpOP6/opportunities?preview=staged`,
  );
  assert.equal(staged.status, 409);
  const stagedBody = await staged.json();
  assert.equal(stagedBody.error, "STAGED_PUBLICATION_PREVIEW_UNAVAILABLE");
  assert.ok(stagedBody.researchSummary?.hasResearch === true);
  assert.ok(Number(stagedBody.researchSummary?.discovered) >= 45);
  assert.ok(
    (stagedBody.recovery?.recoveryActions || []).includes(
      "VIEW_RESEARCH_INVENTORY",
    ),
  );

  const live = await fetch(
    `${BASE}/api/group-demand-intelligence/hotels/rec35fExUxCClpOP6/opportunities?live=1`,
  );
  assert.equal(live.status, 200);
  const liveBody = await live.json();
  assert.equal(liveBody.listScope, "FOUNDER_RESEARCH_INVENTORY");
  assert.ok(liveBody.count >= 44);
  assert.ok(
    (liveBody.storeIdentity?.opportunityCountInStore || 0) >= 45,
  );
});

await checkAsync("9_seven_hotel_research_visibility_matrix", async () => {
  const rows = [];
  for (const [id, name] of AFFECTED) {
    const staged = await fetch(
      `${BASE}/api/group-demand-intelligence/hotels/${id}/opportunities?preview=staged`,
    );
    const stagedBody = await staged.json().catch(() => ({}));
    const live = await fetch(
      `${BASE}/api/group-demand-intelligence/hotels/${id}/opportunities?live=1`,
    );
    const liveBody = await live.json().catch(() => ({}));
    assert.equal(staged.status, 409, name);
    assert.ok(live.status === 200, name);
    assert.ok((liveBody.count || 0) > 0, `${name} research empty`);
    rows.push({
      hotelId: id,
      hotelName: name,
      stagedStatus: staged.status,
      researchCount: liveBody.count,
      discovered: stagedBody.researchSummary?.discovered ?? null,
      qualified: stagedBody.researchSummary?.qualified ?? null,
    });
  }
  const outDir = path.join(
    ROOT,
    "reports",
    "gdi-founder-research-view-v1",
  );
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "SEVEN_HOTEL_RESEARCH_MATRIX.json"),
    `${JSON.stringify({ at: new Date().toISOString(), rows }, null, 2)}\n`,
  );
});

await checkAsync("10_unauthorized_external_share_rejected_by_gate", async () => {
  const denied = assertFounderResearchAccess({
    gdiExternalShare: true,
    gdiPilotUnauthRead: true,
  });
  assert.equal(denied.ok, false);
  assert.equal(denied.error, "UNAUTHORIZED_RESEARCH_ACCESS");
});

await checkAsync("11_portfolio_selectable_hotels_audited", async () => {
  const hotelsRes = await fetch(
    `${BASE}/api/group-demand-intelligence/hotels`,
  );
  assert.equal(hotelsRes.status, 200);
  const hotelsBody = await hotelsRes.json();
  const hotels = hotelsBody.hotels || [];
  assert.ok(
    hotels.length >= 20,
    `expected a full selectable portfolio (>=20), got ${hotels.length}`,
  );
  const out = [];
  for (const h of hotels) {
    const id = h.hotelId;
    const staged = await fetch(
      `${BASE}/api/group-demand-intelligence/hotels/${encodeURIComponent(id)}/opportunities?preview=staged`,
    );
    const live = await fetch(
      `${BASE}/api/group-demand-intelligence/hotels/${encodeURIComponent(id)}/opportunities?live=1`,
    );
    out.push({
      hotelId: id,
      hotelName: h.hotelName || h.displayName,
      stagedStatus: staged.status,
      liveStatus: live.status,
      liveCount: live.status === 200 ? (await live.json()).count : null,
      stagedOk: staged.status === 200,
    });
  }
  const outDir = path.join(
    ROOT,
    "reports",
    "gdi-founder-research-view-v1",
  );
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "PORTFOLIO_25_AUDIT.json"),
    `${JSON.stringify({ at: new Date().toISOString(), hotelCount: out.length, rows: out }, null, 2)}\n`,
  );
  assert.equal(out.length, hotels.length);
});

console.log(`\nOK gdi-founder-research-view-v1 (${passed} checks)`);
