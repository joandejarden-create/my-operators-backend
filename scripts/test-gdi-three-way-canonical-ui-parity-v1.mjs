#!/usr/bin/env node
/**
 * GDI three-way canonical UI parity.
 *
 *   npm run test:gdi-three-way-canonical-ui-parity
 *
 * Compares independently:
 *   A = local AUTHENTICATED GDI (default PACKAGE customer surface)
 *   A-live = local auth ?live=1 (research browse — documented, not cert baseline)
 *   B = local customer SHARE
 *   C = Railway production SHARE
 *
 * Certificates:
 *   A↔B  (authenticated customer ↔ local share) — REQUIRED for CLIENT READY
 *   B↔C  (local share ↔ production share) — REQUIRED for CLIENT READY
 * Share↔share alone is NEVER sufficient.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { execSync } from "node:child_process";
import {
  buildCanonicalCustomerPresentationContract,
  canonicalPresentationCompareKey,
  diffCanonicalCustomerPresentation,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";
import {
  createDifferenceRecord,
  upsertDifference,
  openMaterialCount,
  RESOLUTION_STATUS,
  saveRegister,
  loadRegister,
} from "../lib/group-demand-intelligence/presentation/gdi-component-difference-register-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-canonical-three-way-parity-v1");
const REGISTER_DIR = path.join(ROOT, "reports/gdi-component-difference-register");
const VISUAL = path.join(OUT, "visual-baselines");
const FIXTURE_VISUAL = path.join(
  ROOT,
  "fixtures/gdi-canonical-ui-visual-baselines",
);
fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });
fs.mkdirSync(VISUAL, { recursive: true });
fs.mkdirSync(FIXTURE_VISUAL, { recursive: true });
fs.mkdirSync(REGISTER_DIR, { recursive: true });

const HOTEL_ID = "recLuxvwwxID7U2B8";
const EXPECTED_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";
const EXPECTED_PKG = "gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1";

const share =
  process.env.GDI_PARITY_SHARE_TOKEN ||
  (fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
    /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
  ) || [])[1]?.trim();
assert.ok(share, "share token required");

const localBase = (
  process.env.GDI_PARITY_LOCAL_URL || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const prodBase = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");
assert.notEqual(localBase, prodBase, "refuse local↔local for B↔C");

function sha256(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
}
function gitIdentity() {
  try {
    return {
      head: execSync("git rev-parse HEAD", { cwd: ROOT }).toString().trim(),
      short: execSync("git rev-parse --short HEAD", { cwd: ROOT })
        .toString()
        .trim(),
      branch: execSync("git rev-parse --abbrev-ref HEAD", { cwd: ROOT })
        .toString()
        .trim(),
    };
  } catch (e) {
    return { error: String(e.message || e) };
  }
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = { _parseError: true, preview: text.slice(0, 200) };
  }
  return { status: res.status, json, url, ok: res.ok };
}

function contractOf(opp, meta = {}) {
  if (opp?.canonicalCustomerPresentation) {
    return opp.canonicalCustomerPresentation;
  }
  return buildCanonicalCustomerPresentationContract(opp, meta);
}

function byId(list) {
  return new Map((list || []).map((o) => [o.id || o.opportunityId, o]));
}

const failures = [];
function fail(code, detail) {
  failures.push({ code, detail });
}

// ── Fetch three environments (+ live research for register) ──
const authPkgUrl = `${localBase}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities`;
const authLiveUrl = `${localBase}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?live=1`;
const shareLocalUrl = `${localBase}/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities?share=${encodeURIComponent(share)}`;
const shareProdUrl = `${prodBase}/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities?share=${encodeURIComponent(share)}`;

let authPkg;
let authLive;
let shareLocal;
let shareProd;
try {
  [authPkg, authLive, shareLocal, shareProd] = await Promise.all([
    fetchJson(authPkgUrl),
    fetchJson(authLiveUrl),
    fetchJson(shareLocalUrl),
    fetchJson(shareProdUrl),
  ]);
} catch (e) {
  fail("NOT_VERIFIED", String(e.message || e));
}

const identity = {
  contract: "GDI_THREE_WAY_CANONICAL_UI_PARITY_V1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL_ID,
  expectedFingerprint: EXPECTED_FP,
  expectedPackage: EXPECTED_PKG,
  git: gitIdentity(),
  environments: {
    A_auth_package: {
      role: "local_authenticated_customer_surface",
      url: authPkgUrl,
      status: authPkg?.status,
      listScope: authPkg?.json?.listScope,
      fingerprint: authPkg?.json?.publication?.contentFingerprintSha256,
      count: authPkg?.json?.opportunities?.length,
    },
    A_live_research: {
      role: "local_authenticated_live_research_browse",
      note: "NOT the customer certification baseline — documents live recompute drift",
      url: authLiveUrl,
      status: authLive?.status,
      listScope: authLive?.json?.listScope,
      count: authLive?.json?.opportunities?.length,
    },
    B_share_local: {
      role: "local_external_share",
      url: shareLocalUrl.split("?")[0],
      status: shareLocal?.status,
      listScope: shareLocal?.json?.listScope,
      fingerprint: shareLocal?.json?.publication?.contentFingerprintSha256,
      count: shareLocal?.json?.opportunities?.length,
    },
    C_share_production: {
      role: "railway_external_share",
      url: shareProdUrl.split("?")[0],
      status: shareProd?.status,
      listScope: shareProd?.json?.listScope,
      fingerprint: shareProd?.json?.publication?.contentFingerprintSha256,
      count: shareProd?.json?.opportunities?.length,
    },
  },
};

if (authPkg?.status !== 200 || shareLocal?.status !== 200 || shareProd?.status !== 200) {
  fail("NOT_VERIFIED", "one or more environments failed HTTP");
}

const aOpps = authPkg?.json?.opportunities || [];
const liveOpps = authLive?.json?.opportunities || [];
const bOpps = shareLocal?.json?.opportunities || [];
const cOpps = shareProd?.json?.opportunities || [];

if (aOpps.length !== 67 || bOpps.length !== 67 || cOpps.length !== 67) {
  fail("PUBLICATION_DATA_DIFFERENCE", `counts A=${aOpps.length} B=${bOpps.length} C=${cOpps.length}`);
}

const fpA = authPkg?.json?.publication?.contentFingerprintSha256;
const fpB = shareLocal?.json?.publication?.contentFingerprintSha256;
const fpC = shareProd?.json?.publication?.contentFingerprintSha256;
if (fpA !== EXPECTED_FP || fpB !== EXPECTED_FP || fpC !== EXPECTED_FP) {
  fail("PUBLICATION_DATA_DIFFERENCE", { fpA, fpB, fpC });
}
if (authPkg?.json?.listScope !== "PUBLICATION_PACKAGE") {
  fail(
    "CONFIGURATION_DIFFERENCE",
    `A listScope=${authPkg?.json?.listScope} — auth customer surface must default to PACKAGE`,
  );
}

const aMap = byId(aOpps);
const liveMap = byId(liveOpps);
const bMap = byId(bOpps);
const cMap = byId(cOpps);

let register = loadRegister(
  path.join(REGISTER_DIR, "COMPONENT_DIFFERENCE_REGISTER.json"),
);

const contactParity = [];
const svgParity = [];
const actionParity = [];
const abMismatches = [];
const bcMismatches = [];
const liveVsPackage = [];

for (const id of [...aMap.keys()].sort()) {
  const a = aMap.get(id);
  const b = bMap.get(id);
  const c = cMap.get(id);
  const live = liveMap.get(id);
  const meta = {
    hotelId: HOTEL_ID,
    publicationVersion: EXPECTED_PKG,
    contentFingerprintSha256: EXPECTED_FP,
    listScope: "PUBLICATION_PACKAGE",
  };
  const ca = contractOf(a, meta);
  const cb = contractOf(b, meta);
  const cc = contractOf(c, meta);

  const abDiffs = diffCanonicalCustomerPresentation(ca, cb);
  const bcDiffs = diffCanonicalCustomerPresentation(cb, cc);

  contactParity.push({
    id,
    title: ca.display?.title,
    A: ca.contact,
    B: cb.contact,
    C: cc.contact,
    abMatch: !abDiffs.some((d) => d.field.startsWith("contact")),
    bcMatch:
      JSON.stringify(cb.contact) === JSON.stringify(cc.contact),
  });
  svgParity.push({
    id,
    title: ca.display?.title,
    A: ca.icons,
    B: cb.icons,
    C: cc.icons,
    abMatch: JSON.stringify(ca.icons) === JSON.stringify(cb.icons),
    bcMatch: JSON.stringify(cb.icons) === JSON.stringify(cc.icons),
  });
  actionParity.push({
    id,
    title: ca.display?.title,
    A: ca.commercialAction,
    B: cb.commercialAction,
    C: cc.commercialAction,
    live: live
      ? {
          pill: live.salesActionPillLabel,
          class: live.revenueStreamClass,
        }
      : null,
    abMatch:
      JSON.stringify(ca.commercialAction) ===
      JSON.stringify(cb.commercialAction),
    bcMatch:
      JSON.stringify(cb.commercialAction) ===
      JSON.stringify(cc.commercialAction),
  });

  if (abDiffs.length) {
    abMismatches.push({ id, title: ca.display?.title, diffs: abDiffs });
    for (const d of abDiffs) {
      const category =
        d.field.includes("contact")
          ? "CONTACT_FIELD_DIFFERENCE"
          : d.field.includes("guest") ||
              d.field.includes("event") ||
              d.field.includes("needDate")
            ? "SVG_STATE_DIFFERENCE"
            : d.field.includes("commercial") ||
                d.field.includes("recommended") ||
                d.field.includes("Action")
              ? "COMMERCIAL_ACTION_DIFFERENCE"
              : "PUBLICATION_DATA_DIFFERENCE";
      register = upsertDifference(
        register,
        createDifferenceRecord({
          differenceId: `AB__${category}__${id}__${d.field}`,
          category,
          componentName: d.field,
          hotelId: HOTEL_ID,
          opportunityId: id,
          authenticatedRoute: authPkgUrl,
          externalShareRoute: shareLocalUrl.split("?")[0],
          publicationVersion: EXPECTED_PKG,
          expectedValue: d.left,
          actualLocalAuth: d.left,
          actualLocalShare: d.right,
          actualProductionShare: cc ? canonicalPresentationCompareKey(cc) : null,
          rootCause:
            "A↔B customer presentation contract mismatch on approved PACKAGE surface",
          owningSourceModule:
            "lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js",
          severity: "MATERIAL",
          resolutionStatus: RESOLUTION_STATUS.OPEN,
          testCoverage: "test:gdi-three-way-canonical-ui-parity",
        }),
      );
      fail("A_B_PARITY", `${id}.${d.field}`);
    }
  }

  if (bcDiffs.length) {
    bcMismatches.push({ id, title: ca.display?.title, diffs: bcDiffs });
    for (const d of bcDiffs) {
      register = upsertDifference(
        register,
        createDifferenceRecord({
          differenceId: `BC__${id}__${d.field}`,
          category: "ASSET_VERSION_DIFFERENCE",
          componentName: d.field,
          hotelId: HOTEL_ID,
          opportunityId: id,
          expectedValue: d.left,
          actualLocalShare: d.left,
          actualProductionShare: d.right,
          rootCause: "Local share vs Railway share diverge — deploy/asset drift",
          severity: "MATERIAL",
          resolutionStatus: RESOLUTION_STATUS.OPEN,
          testCoverage: "test:gdi-three-way-canonical-ui-parity",
        }),
      );
      fail("B_C_PARITY", `${id}.${d.field}`);
    }
  }

  // Document live research vs package (intentional until founder publishes live fields)
  if (live) {
    const liveContract = buildCanonicalCustomerPresentationContract(live, {
      ...meta,
      listScope: "LIVE",
    });
    const liveDiffs = diffCanonicalCustomerPresentation(liveContract, ca);
    if (liveDiffs.length) {
      liveVsPackage.push({
        id,
        title: ca.display?.title,
        diffs: liveDiffs,
      });
      for (const d of liveDiffs) {
        register = upsertDifference(
          register,
          createDifferenceRecord({
            differenceId: `LIVE_VS_PKG__${id}__${d.field}`,
            category: "AUTH_LIVE_RECOMPUTE_DIFFERENCE",
            componentName: d.field,
            hotelId: HOTEL_ID,
            opportunityId: id,
            expectedValue: ca[d.field] ?? d.right,
            actualLocalAuth: d.left,
            actualLocalShare: d.right,
            rootCause:
              "Authenticated ?live=1 recomputes from live store; customer surface uses immutable PACKAGE. Not a share bug.",
            severity: "DOCUMENT",
            resolutionStatus: RESOLUTION_STATUS.INTENTIONAL,
            proposedCorrection:
              "Use default auth (PACKAGE) for customer preview; publish new fingerprint if live evidence must become customer content.",
            testCoverage: "test:gdi-three-way-canonical-ui-parity",
          }),
        );
      }
    }
  }
}

// Mark AB matches as resolved historical share↔share-only false certs when now matching
for (const id of aMap.keys()) {
  const ca = contractOf(aMap.get(id));
  const cb = contractOf(bMap.get(id));
  if (canonicalPresentationCompareKey(ca) === canonicalPresentationCompareKey(cb)) {
    // no-op — open entries remain if previously written with different ids
  }
}

saveRegister(
  path.join(REGISTER_DIR, "COMPONENT_DIFFERENCE_REGISTER.json"),
  register,
);
saveRegister(
  path.join(OUT, "03-COMPONENT_DIFFERENCE_REGISTER.json"),
  register,
);

// ── Browser A / B / C ──
const browserVisited = { A: false, B: false, C: false };
async function extractUi(page, label, url) {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
  await page.waitForSelector("[data-open], .brand-card, #gdiHowToReadReport", {
    timeout: 120000,
  });
  await page.waitForTimeout(1200);
  const reset = page.locator("#gdiResetViewBtn");
  if (await reset.count()) {
    await reset.click().catch(() => {});
    await page.waitForTimeout(600);
  }
  const shotPath = path.join(OUT, "screenshots", `${label}-overview.png`);
  await page.screenshot({ path: shotPath, fullPage: false });
  fs.copyFileSync(shotPath, path.join(VISUAL, `${label}-overview.png`));
  fs.copyFileSync(shotPath, path.join(FIXTURE_VISUAL, `${label}-overview.png`));

  return page.evaluate(() => {
    const norm = (s) => String(s || "").replace(/\s+/g, " ").trim();
    const cards = [];
    const seen = new Set();
    for (const el of document.querySelectorAll("[data-open]")) {
      const id = el.getAttribute("data-open");
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const card = el.closest(".brand-card") || el;
      const contact = card.querySelector(".brand-card__contact");
      const action = card.querySelector(".gdi-tile-pill--sales-action");
      const icons = [...card.querySelectorAll(".gdi-revenue-stream-icon")].map(
        (n) => ({
          className: String(n.className || ""),
          state: n.getAttribute("data-state") ||
            (/--active/.test(n.className) ? "active" : "muted"),
          color: getComputedStyle(n).color,
        }),
      );
      cards.push({
        id,
        title: norm(card.querySelector(".brand-card__name")?.textContent),
        contactText: contact ? norm(contact.innerText) : null,
        actionText: action ? norm(action.textContent) : null,
        icons,
      });
    }
    return {
      cardCount: cards.length,
      cards,
      uiMarker: document
        .querySelector('script[src*="dealality-gdi-ui"]')
        ?.getAttribute("src"),
    };
  });
}

let uiA = null;
let uiB = null;
let uiC = null;
try {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 1,
    locale: "en-US",
    timezoneId: "America/New_York",
  });
  const pageA = await ctx.newPage();
  const pageB = await ctx.newPage();
  const pageC = await ctx.newPage();
  try {
    uiA = await extractUi(
      pageA,
      "A-auth",
      `${localBase}/group-demand-intelligence.html?hotelId=${HOTEL_ID}`,
    );
    browserVisited.A = true;
  } catch (e) {
    fail("NOT_VERIFIED", `A browser: ${e.message || e}`);
  }
  try {
    uiB = await extractUi(
      pageB,
      "B-share-local",
      `${localBase}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
    );
    browserVisited.B = true;
  } catch (e) {
    fail("NOT_VERIFIED", `B browser: ${e.message || e}`);
  }
  try {
    uiC = await extractUi(
      pageC,
      "C-share-prod",
      `${prodBase}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
    );
    browserVisited.C = true;
  } catch (e) {
    fail("NOT_VERIFIED", `C browser: ${e.message || e}`);
  }

  // Focus RIC card screenshots
  for (const [page, label] of [
    [pageA, "A"],
    [pageB, "B"],
    [pageC, "C"],
  ]) {
    try {
      const loc = page
        .locator(".brand-card")
        .filter({ hasText: /Regulatory Information Conference/i })
        .first();
      if (await loc.count()) {
        await loc.scrollIntoViewIfNeeded();
        const box = await loc.boundingBox();
        if (box) {
          const p = path.join(OUT, "screenshots", `${label}-ric-card.png`);
          await page.screenshot({
            path: p,
            clip: {
              x: Math.max(0, box.x - 4),
              y: Math.max(0, box.y - 4),
              width: Math.min(box.width + 8, 700),
              height: Math.min(box.height + 8, 480),
            },
          });
          fs.copyFileSync(p, path.join(FIXTURE_VISUAL, `${label}-ric-card.png`));
        }
      }
    } catch {
      /* ignore focus shot */
    }
  }
  await browser.close();
} catch (e) {
  fail("NOT_VERIFIED", `browser launch: ${e.message || e}`);
}

// UI A↔B / B↔C card text compare when available
const uiAb = [];
const uiBc = [];
if (uiA?.cards && uiB?.cards) {
  const bUi = new Map(uiB.cards.map((c) => [c.id, c]));
  for (const a of uiA.cards) {
    const b = bUi.get(a.id);
    if (!b) continue;
    if (
      a.contactText !== b.contactText ||
      a.actionText !== b.actionText ||
      JSON.stringify(a.icons.map((i) => i.state)) !==
        JSON.stringify(b.icons.map((i) => i.state))
    ) {
      uiAb.push({ id: a.id, title: a.title, A: a, B: b });
      fail("A_B_UI", a.id);
    }
  }
}
if (uiB?.cards && uiC?.cards) {
  const cUi = new Map(uiC.cards.map((c) => [c.id, c]));
  for (const b of uiB.cards) {
    const c = cUi.get(b.id);
    if (!c) continue;
    if (
      b.contactText !== c.contactText ||
      b.actionText !== c.actionText ||
      JSON.stringify(b.icons.map((i) => i.state)) !==
        JSON.stringify(c.icons.map((i) => i.state))
    ) {
      uiBc.push({ id: b.id, title: b.title, B: b, C: c });
      fail("B_C_UI", b.id);
    }
  }
}

const abPass = abMismatches.length === 0 && uiAb.length === 0;
const bcPass = bcMismatches.length === 0 && uiBc.length === 0;
const openMaterial = openMaterialCount(register);
const blockingCodes = new Set([
  "NOT_VERIFIED",
  "A_B_PARITY",
  "B_C_PARITY",
  "A_B_UI",
  "B_C_UI",
  "PUBLICATION_DATA_DIFFERENCE",
  "CONFIGURATION_DIFFERENCE",
]);
const blockingFailures = failures.filter((f) => blockingCodes.has(f.code));
// Auth browser may hit Memberstack wall; API A↔B is authoritative for contract.
// Set GDI_REQUIRE_AUTH_BROWSER=1 to require A DOM as well.
const aBrowserRequired = process.env.GDI_REQUIRE_AUTH_BROWSER === "1";
const finalOk =
  abPass &&
  bcPass &&
  blockingFailures.length === 0 &&
  openMaterial === 0 &&
  fpA === EXPECTED_FP &&
  fpB === EXPECTED_FP &&
  fpC === EXPECTED_FP &&
  authPkg?.json?.listScope === "PUBLICATION_PACKAGE" &&
  browserVisited.B &&
  browserVisited.C &&
  (!aBrowserRequired || browserVisited.A);

const ricA = [...aMap.values()].find((o) =>
  /Regulatory Information Conference/i.test(o.title || ""),
);
const ricLive = [...liveMap.values()].find((o) =>
  /Regulatory Information Conference/i.test(o.title || ""),
);

const certificate = {
  contract: "GDI_THREE_WAY_CANONICAL_UI_PARITY_V1",
  generatedAt: new Date().toISOString(),
  verdict: finalOk ? "CLIENT READY" : "NOT CERTIFIED",
  ok: finalOk,
  note: "CLIENT READY requires A↔B AND B↔C. Share↔share alone is insufficient.",
  certificates: {
    A_to_B_auth_customer_vs_local_share: abPass ? "PASS" : "FAIL",
    B_to_C_local_share_vs_production_share: bcPass ? "PASS" : "FAIL",
    approved_package_fingerprint: fpA === EXPECTED_FP && fpC === EXPECTED_FP ? "PASS" : "FAIL",
  },
  mismatchCounts: {
    abContract: abMismatches.length,
    bcContract: bcMismatches.length,
    abUi: uiAb.length,
    bcUi: uiBc.length,
    liveVsPackageDocumented: liveVsPackage.length,
    openMaterialDifferences: openMaterial,
  },
  lornaKipfer: {
    authPackage: {
      role: ricA?.tileContactRoleLabel || ricA?.tileContactPreview?.displayRole,
      icons: ricA?.revenueStreamIcons,
      sales: ricA?.salesActionPillLabel,
      class: ricA?.revenueStreamClass,
    },
    authLive: {
      role: ricLive?.tileContactRoleLabel || ricLive?.tileContactPreview?.displayRole,
      icons: ricLive?.revenueStreamIcons,
      sales: ricLive?.salesActionPillLabel,
      class: ricLive?.revenueStreamClass,
    },
  },
  browserVisited,
  failures,
  git: identity.git,
};

const files = {
  "01-THREE-ENVIRONMENT-IDENTITY.json": identity,
  "02-SOURCE-AND-RENDERER-MAP.json": {
    sharedUi: "public/js/group-demand-intelligence/dealality-gdi-ui.js",
    authShell: "public/js/group-demand-intelligence/app.js",
    shareShell: "public/js/group-demand-intelligence/share-app.js",
    packageAttach:
      "lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js",
    canonicalContract:
      "lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js",
    differenceRegister:
      "lib/group-demand-intelligence/presentation/gdi-component-difference-register-v1.js",
    authDefault: "PUBLICATION_PACKAGE unless ?live=1",
  },
  "04-CONTACT_FIELD_PARITY.json": {
    compared: contactParity.length,
    abFail: contactParity.filter((r) => !r.abMatch).length,
    bcFail: contactParity.filter((r) => !r.bcMatch).length,
    rows: contactParity,
  },
  "05-SVG_STATE_PARITY.json": {
    compared: svgParity.length,
    abFail: svgParity.filter((r) => !r.abMatch).length,
    bcFail: svgParity.filter((r) => !r.bcMatch).length,
    rows: svgParity,
  },
  "06-COMMERCIAL_ACTION_PARITY.json": {
    compared: actionParity.length,
    abFail: actionParity.filter((r) => !r.abMatch).length,
    bcFail: actionParity.filter((r) => !r.bcMatch).length,
    liveVsPackageSample: liveVsPackage.slice(0, 20),
    rows: actionParity,
  },
  "07-CARD_AND_CSS_PARITY.json": {
    uiAb,
    uiBc,
    markers: {
      A: uiA?.uiMarker || null,
      B: uiB?.uiMarker || null,
      C: uiC?.uiMarker || null,
    },
  },
  "08-DRAWER_PARITY.json": {
    note: "Drawer DTO parity covered via PACKAGE detail attach; sample drawers deferred to complete-release-parity",
    status: abPass && bcPass ? "PACKAGE_CONTRACT_ALIGNED" : "BLOCKED_BY_LIST_DIFFS",
  },
  "09-SHARED_COMPONENT_REFACTOR.json": {
    changes: [
      "Auth Opportunities default to PUBLICATION_PACKAGE (opt-out ?live=1)",
      "Package attach: tile contact + evidence-backed icons + canonical contract",
      "COMBINED without event thesis demoted to GUESTROOMS_ONLY",
      "Permanent three-way test + difference register",
    ],
  },
  "10-THREE_WAY_BROWSER_RESULTS.json": {
    browserVisited,
    uiA: uiA ? { cardCount: uiA.cardCount, marker: uiA.uiMarker } : null,
    uiB: uiB ? { cardCount: uiB.cardCount, marker: uiB.uiMarker } : null,
    uiC: uiC ? { cardCount: uiC.cardCount, marker: uiC.uiMarker } : null,
    uiAbCount: uiAb.length,
    uiBcCount: uiBc.length,
  },
  "11-GITHUB_AND_RAILWAY_RELEASE.json": {
    git: identity.git,
    note: "Filled after PR merge + Railway deploy",
  },
  "12-FINAL_CLIENT_CERTIFICATE.json": certificate,
};

for (const [name, body] of Object.entries(files)) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(body, null, 2));
}

const founder = `# GDI Three-Way Canonical UI Parity V1

**Verdict: ${certificate.verdict}**

## Comparison (mandatory)

| Pair | Result |
|------|--------|
| **A↔B** Auth customer (PACKAGE) ↔ Local share | ${certificate.certificates.A_to_B_auth_customer_vs_local_share} |
| **B↔C** Local share ↔ Railway share | ${certificate.certificates.B_to_C_local_share_vs_production_share} |
| Approved fingerprint | ${certificate.certificates.approved_package_fingerprint} |

Share↔share alone is **not** sufficient for CLIENT READY.

## Architecture

Authenticated GDI Opportunities now default to the **approved publication package** (same attach path as external share). Research browse: \`?live=1\`.

Live-store recomputation differences (e.g. commercial action Build Account vs package Qualify Opportunity; live guestroomThesis) are recorded as **AUTH_LIVE_RECOMPUTE_DIFFERENCE / INTENTIONAL** until a new fingerprint publishes those fields.

## Lorna Kipfer / RIC

| Surface | Role | Icons | Sales |
|---------|------|-------|-------|
| Auth PACKAGE (A) | ${certificate.lornaKipfer.authPackage.role} | ${JSON.stringify(certificate.lornaKipfer.authPackage.icons)} | ${certificate.lornaKipfer.authPackage.sales} |
| Auth LIVE | ${certificate.lornaKipfer.authLive.role} | ${JSON.stringify(certificate.lornaKipfer.authLive.icons)} | ${certificate.lornaKipfer.authLive.sales} |

## Fingerprint

\`${EXPECTED_FP}\` — unchanged.

## Reports

See \`01\`–\`12\` in this directory and \`reports/gdi-component-difference-register/COMPONENT_DIFFERENCE_REGISTER.json\`.

Visual baselines: \`visual-baselines/\` and \`fixtures/gdi-canonical-ui-visual-baselines/\`.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      ok: finalOk,
      verdict: certificate.verdict,
      certificates: certificate.certificates,
      mismatchCounts: certificate.mismatchCounts,
      lorna: certificate.lornaKipfer,
      failureCount: failures.length,
      out: OUT,
    },
    null,
    2,
  ),
);

process.exit(finalOk ? 0 : 1);
