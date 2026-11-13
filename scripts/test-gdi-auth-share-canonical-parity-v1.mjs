#!/usr/bin/env node
/**
 * GDI Auth↔Share Canonical Parity V1
 *
 *   npm run test:gdi-auth-share-canonical-parity
 *
 * Modes:
 *   A = LIVE RESEARCH (?live=1) — NOT the customer certification baseline
 *   B = FOUNDER PUBLICATION PREVIEW (auth + publicationPackage=1)
 *   C = EXTERNAL CUSTOMER SHARE
 *
 * Mandatory customer parity: B ≡ C
 * A≠B differences are recorded as INTENTIONAL_LIVE_RESEARCH unless a defect.
 *
 * NEVER treat share↔share or A↔C as CLIENT READY for authenticated-vs-share.
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
} from "../lib/group-demand-intelligence/presentation/gdi-component-difference-register-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-auth-share-canonical-reconciliation-v1");
fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const HOTEL_ID = "recLuxvwwxID7U2B8";
const EXPECTED_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";

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

const failures = [];
function fail(code, detail) {
  failures.push({ code, detail });
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
    json = null;
  }
  return { status: res.status, json, text, url };
}

async function htmlMarker(base, pagePath) {
  const html = await (
    await fetch(`${base}${pagePath}?cb=${Date.now()}`, {
      headers: { "Cache-Control": "no-cache" },
    })
  ).text();
  return (html.match(/dealality-gdi-ui\.js\?v=([^"']+)/) || [])[1] || null;
}

function oppMap(payload) {
  const m = new Map();
  for (const o of payload?.json?.opportunities || []) {
    const id = o.id || o.opportunityId;
    if (id) m.set(id, o);
  }
  return m;
}

function contractOf(o, meta = {}) {
  if (!o) return null;
  if (o.canonicalCustomerPresentation) return o.canonicalCustomerPresentation;
  return buildCanonicalCustomerPresentationContract(o, meta);
}

function salesOf(o) {
  return (
    o?.salesActionPillLabel ||
    o?.tileBadgePlan?.salesActionPillLabel ||
    o?.canonicalCustomerPresentation?.commercialAction?.pillLabel ||
    null
  );
}

function roleOf(o) {
  return (
    o?.tileContactRoleLabel ||
    o?.tileContactPreview?.displayRole ||
    o?.canonicalCustomerPresentation?.contact?.role ||
    null
  );
}

const git = gitIdentity();
const metaPkg = {
  hotelId: HOTEL_ID,
  contentFingerprintSha256: EXPECTED_FP,
  listScope: "PUBLICATION_PACKAGE",
};

const urlA = `${localBase}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?live=1`;
const urlB = `${localBase}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?publicationPackage=1&surface=customer`;
const urlCLocal = `${localBase}/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities?share=${encodeURIComponent(share)}`;
const urlCProd = `${prodBase}/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities?share=${encodeURIComponent(share)}`;

const [payloadA, payloadB, payloadCLocal, payloadCProd] = await Promise.all([
  fetchJson(urlA),
  fetchJson(urlB),
  fetchJson(urlCLocal),
  fetchJson(urlCProd),
]);

const mapA = oppMap(payloadA);
const mapB = oppMap(payloadB);
const mapC = oppMap(payloadCLocal);
const mapCProd = oppMap(payloadCProd);

const fpB =
  payloadB.json?.contentFingerprintSha256 ||
  payloadB.json?.publication?.contentFingerprintSha256 ||
  null;
const fpC =
  payloadCLocal.json?.contentFingerprintSha256 ||
  payloadCLocal.json?.publication?.contentFingerprintSha256 ||
  null;
const fpCProd =
  payloadCProd.json?.contentFingerprintSha256 ||
  payloadCProd.json?.publication?.contentFingerprintSha256 ||
  null;

if (payloadB.json?.listScope !== "PUBLICATION_PACKAGE") {
  fail("MODE_B_NOT_PACKAGE", payloadB.json?.listScope);
}
if (payloadCLocal.json?.listScope !== "PUBLICATION_PACKAGE") {
  fail("MODE_C_NOT_PACKAGE", payloadCLocal.json?.listScope);
}
if (fpB !== EXPECTED_FP || fpC !== EXPECTED_FP) {
  fail("FINGERPRINT", { fpB, fpC, EXPECTED_FP });
}

let register = { differences: [] };
const bcMismatches = [];
const abLiveDiffs = [];
const commercialConflicts = [];
const contactConflicts = [];
const svgConflicts = [];

const mandatory = [
  {
    id: "gdi_opp_potomac_memorial_2027",
    re: /Potomac Memorial/i,
    field: "commercialAction",
  },
  {
    id: "gdi_opp_bethesda_premier_cup_2026",
    re: /Premier Cup/i,
    field: "commercialAction",
  },
  {
    id: "gdi_opp_regulatory_information_conference_20260930",
    re: /Regulatory Information Conference/i,
    field: "commercialAction",
  },
  {
    id: "gdi_opp_regulatory_information_conference_20260930",
    re: /Regulatory Information Conference/i,
    field: "contactRole",
    name: "Lorna Kipfer",
  },
  {
    id: "gdi_pe_781f12393f8117e7",
    re: /Woman.?s Club of Bethesda/i,
    field: "summary",
  },
];

for (const id of mapB.keys()) {
  const a = mapA.get(id);
  const b = mapB.get(id);
  const c = mapC.get(id);
  if (!b || !c) {
    fail("MISSING_OPP", id);
    continue;
  }
  const cb = contractOf(b, metaPkg);
  const cc = contractOf(c, metaPkg);
  const diffs = diffCanonicalCustomerPresentation(cb, cc);
  if (diffs.length) {
    bcMismatches.push({ id, title: b.title, diffs });
    for (const d of diffs) {
      register = upsertDifference(
        register,
        createDifferenceRecord({
          differenceId: `BC__${id}__${d.field}`,
          category:
            d.field.includes("contact")
              ? "CONTACT_FIELD_DIFFERENCE"
              : d.field.includes("guest") || d.field.includes("event")
                ? "SVG_STATE_DIFFERENCE"
                : "COMMERCIAL_ACTION_DIFFERENCE",
          componentName: d.field,
          hotelId: HOTEL_ID,
          opportunityId: id,
          expectedValue: d.left,
          actualLocalAuth: null,
          actualLocalShare: d.left,
          actualProductionShare: d.right,
          rootCause: "Mode B founder publication preview ≠ Mode C external share",
          severity: "MATERIAL",
          resolutionStatus: RESOLUTION_STATUS.OPEN,
          testCoverage: "test:gdi-auth-share-canonical-parity",
        }),
      );
      fail("B_C_PARITY", `${id}.${d.field}`);
    }
  }

  if (a) {
    const ca = contractOf(a, { ...metaPkg, listScope: "LIVE" });
    const liveDiffs = diffCanonicalCustomerPresentation(ca, cb);
    if (liveDiffs.length) {
      abLiveDiffs.push({
        id,
        title: b.title,
        A_sales: salesOf(a),
        B_sales: salesOf(b),
        A_role: roleOf(a),
        B_role: roleOf(b),
        diffs: liveDiffs,
      });
      for (const d of liveDiffs) {
        register = upsertDifference(
          register,
          createDifferenceRecord({
            differenceId: `A_LIVE_VS_B_PKG__${id}__${d.field}`,
            category: "AUTH_LIVE_RECOMPUTE_DIFFERENCE",
            componentName: d.field,
            hotelId: HOTEL_ID,
            opportunityId: id,
            expectedValue: d.right,
            actualLocalAuth: d.left,
            actualLocalShare: d.right,
            rootCause:
              "Mode A LIVE RESEARCH recomputes from live store; Mode B/C use immutable PUBLICATION_PACKAGE. Not a B↔C defect.",
            severity: "DOCUMENT",
            resolutionStatus: RESOLUTION_STATUS.INTENTIONAL,
            testCoverage: "test:gdi-auth-share-canonical-parity",
          }),
        );
      }
    }
  }
}

// Mandatory screenshot examples
for (const m of mandatory) {
  const b =
    mapB.get(m.id) ||
    [...mapB.values()].find((o) => m.re.test(o.title || ""));
  const c =
    mapC.get(m.id) ||
    [...mapC.values()].find((o) => m.re.test(o.title || ""));
  const a =
    mapA.get(m.id) ||
    [...mapA.values()].find((o) => m.re.test(o.title || ""));
  if (!b || !c) {
    fail("MANDATORY_MISSING", m.id);
    continue;
  }
  if (m.field === "commercialAction") {
    commercialConflicts.push({
      id: b.id,
      title: b.title,
      A: salesOf(a) || a?.tileBadgePlan?.salesActionPillLabel || null,
      B: salesOf(b),
      C: salesOf(c),
      approvedExpected: salesOf(b),
      classification:
        salesOf(a) && salesOf(a) !== salesOf(b)
          ? "INTENTIONAL_LIVE_VS_PACKAGE"
          : salesOf(b) === salesOf(c)
            ? "B_C_MATCH"
            : "B_C_DEFECT",
    });
    if (salesOf(b) !== salesOf(c)) fail("MANDATORY_BC_SALES", b.id);
  }
  if (m.field === "contactRole") {
    contactConflicts.push({
      id: b.id,
      title: b.title,
      name: roleOf(b) && (b.tileContactName || b.tileContactPreview?.name),
      A_displayRole: roleOf(a),
      A_opportunityRole: a?.tileContactPreview?.opportunityRole || null,
      B: roleOf(b),
      C: roleOf(c),
      approvedExpected: "RIC Conference Contact",
      note: "opportunityRole may still say Group Travel Coordinator on LIVE; displayRole must stay RIC Conference Contact on B/C",
      classification:
        roleOf(b) === roleOf(c) && roleOf(b) === "RIC Conference Contact"
          ? "B_C_MATCH"
          : "B_C_DEFECT",
    });
    if (roleOf(b) !== roleOf(c)) fail("MANDATORY_BC_ROLE", b.id);
  }
  if (m.field === "summary") {
    const sb = String(b.summaryWhat || b.summary || "");
    const sc = String(c.summaryWhat || c.summary || "");
    if (sb !== sc) fail("MANDATORY_BC_SUMMARY", b.id);
  }
}

for (const id of mapB.keys()) {
  const b = mapB.get(id);
  const c = mapC.get(id);
  const ib = b?.revenueStreamIcons || {};
  const ic = c?.revenueStreamIcons || {};
  if (
    JSON.stringify(ib) !== JSON.stringify(ic) ||
    String(b?.revenueStreamClass || "") !== String(c?.revenueStreamClass || "")
  ) {
    svgConflicts.push({
      id,
      B: { icons: ib, class: b?.revenueStreamClass },
      C: { icons: ic, class: c?.revenueStreamClass },
    });
    fail("B_C_SVG", id);
  }
}

// Browser B vs C (local share) — never use live as customer baseline
const browser = { B: false, C: false, uiBc: [] };
try {
  const br = await chromium.launch({ headless: true });
  const page = await br.newPage({ viewport: { width: 1440, height: 900 } });
  async function snap(label, url) {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
    await page.waitForSelector("[data-open], .brand-card, #gdiHowToReadReport", {
      timeout: 120000,
    });
    await page.waitForTimeout(1500);
    const reset = page.locator("#gdiResetViewBtn");
    if (await reset.count()) {
      await reset.click().catch(() => {});
      await page.waitForTimeout(600);
    }
    await page.screenshot({
      path: path.join(OUT, "screenshots", `${label}-overview.png`),
      fullPage: false,
    });
    browser[label] = true;
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
            kind: n.getAttribute("data-revenue-stream"),
            state:
              n.getAttribute("data-state") ||
              (/--active/.test(n.className) ? "active" : "muted"),
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
        modeBanner: norm(
          document.querySelector("[data-gdi-presentation-mode]")?.textContent ||
            document.querySelector("#gdiPresentationModeBanner")?.textContent,
        ),
        uiMarker: document
          .querySelector('script[src*="dealality-gdi-ui"]')
          ?.getAttribute("src"),
      };
    });
  }

  const uiB = await snap(
    "B",
    `${localBase}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&publicationPackage=1&surface=customer`,
  );
  const uiC = await snap(
    "C",
    `${localBase}/group-demand-intelligence-share.html?hotelId=${HOTEL_ID}&share=${encodeURIComponent(share)}`,
  );
  const cUi = new Map((uiC?.cards || []).map((c) => [c.id, c]));
  for (const b of uiB?.cards || []) {
    const c = cUi.get(b.id);
    if (!c) continue;
    if (
      b.contactText !== c.contactText ||
      b.actionText !== c.actionText ||
      JSON.stringify(b.icons) !== JSON.stringify(c.icons)
    ) {
      browser.uiBc.push({ id: b.id, B: b, C: c });
      fail("B_C_UI", b.id);
    }
  }
  await br.close();
  browser.uiB = {
    cardCount: uiB?.cardCount,
    marker: uiB?.uiMarker,
    modeBanner: uiB?.modeBanner,
  };
  browser.uiC = {
    cardCount: uiC?.cardCount,
    marker: uiC?.uiMarker,
    modeBanner: uiC?.modeBanner,
  };
} catch (e) {
  fail("NOT_VERIFIED", `browser: ${e.message || e}`);
}

const openMaterial = openMaterialCount(register);
const bcPass = bcMismatches.length === 0 && browser.uiBc.length === 0;
const blocking = failures.filter((f) =>
  [
    "B_C_PARITY",
    "B_C_UI",
    "B_C_SVG",
    "MODE_B_NOT_PACKAGE",
    "MODE_C_NOT_PACKAGE",
    "FINGERPRINT",
    "MANDATORY_BC_SALES",
    "MANDATORY_BC_ROLE",
    "MANDATORY_BC_SUMMARY",
    "NOT_VERIFIED",
  ].includes(f.code),
);

// Explicitly refuse A↔C or share↔share as authenticated parity
const falseCertGuard = {
  rule: "CLIENT READY for authenticated-vs-share requires Mode B ≡ Mode C only",
  shareShareAlone: "INSUFFICIENT",
  liveVsShareAlone: "INVALID — Mode A is LIVE RESEARCH",
};

const ok =
  bcPass &&
  blocking.length === 0 &&
  openMaterial === 0 &&
  fpB === EXPECTED_FP &&
  browser.B &&
  browser.C;

const verdict = ok
  ? "B_C_PUBLICATION_PARITY_PASS"
  : blocking.some((f) => f.code === "NOT_VERIFIED")
    ? "NOT_VERIFIED"
    : "FAILED";

// Never emit CLIENT READY wording that equates Mode A with share
const certificate = {
  contract: "GDI_AUTH_SHARE_CANONICAL_RECONCILIATION_V1",
  generatedAt: new Date().toISOString(),
  verdict,
  ok,
  note: "FOUNDER PUBLICATION PREVIEW (B) ≡ EXTERNAL SHARE (C). Mode A LIVE is intentionally different. Share↔share alone is never authenticated parity.",
  falseCertGuard,
  modes: {
    A_live: {
      url: urlA,
      listScope: payloadA.json?.listScope,
      count: mapA.size,
    },
    B_founder_publication_preview: {
      url: urlB,
      listScope: payloadB.json?.listScope,
      fingerprint: fpB,
      count: mapB.size,
    },
    C_external_share_local: {
      url: urlCLocal.split("?")[0],
      listScope: payloadCLocal.json?.listScope,
      fingerprint: fpC,
      count: mapC.size,
    },
    C_external_share_prod: {
      url: urlCProd.split("?")[0],
      listScope: payloadCProd.json?.listScope,
      fingerprint: fpCProd,
      count: mapCProd.size,
      marker: await htmlMarker(prodBase, "/group-demand-intelligence-share.html"),
    },
  },
  certificates: {
    B_to_C_founder_preview_vs_share: bcPass ? "PASS" : "FAIL",
    A_to_B_live_vs_package: "DOCUMENTED_INTENTIONAL",
    A_to_C_live_vs_share: "NOT_A_CUSTOMER_PARITY_TARGET",
    approved_package_fingerprint: fpB === EXPECTED_FP ? "PASS" : "FAIL",
  },
  mismatchCounts: {
    bcContract: bcMismatches.length,
    bcUi: browser.uiBc.length,
    abLiveDocumented: abLiveDiffs.length,
    openMaterialDifferences: openMaterial,
  },
  failures: blocking,
  git,
};

saveRegister(path.join(OUT, "COMPONENT_DIFFERENCE_REGISTER.json"), register);

function w(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2));
}

w("01-THREE-MODE-SOURCE-MAP.json", {
  modes: certificate.modes,
  htmlMarkers: {
    authLocal: await htmlMarker(localBase, "/group-demand-intelligence.html"),
    shareLocal: await htmlMarker(
      localBase,
      "/group-demand-intelligence-share.html",
    ),
    shareProd: certificate.modes.C_external_share_prod.marker,
  },
  rule: "B and C must use PUBLICATION_PACKAGE + same attach/contract/renderer",
});
w("02-DUPLICATE_RENDERERS.json", {
  sharedCustomerRenderer: "public/js/group-demand-intelligence/dealality-gdi-ui.js",
  authShell: "public/js/group-demand-intelligence/app.js",
  shareShell: "public/js/group-demand-intelligence/share-app.js",
  packageAttach:
    "lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js",
  canonicalContract:
    "lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js",
  eliminatedRisk:
    "Client prefers canonicalCustomerPresentation when present; Mode B/C both attach package",
  remainingIntentional: [
    "Mode A live-store commercial recompute (?live=1)",
    "Auth-only feedback/admin controls",
  ],
});
w("03-COMPONENT_DIFFERENCE_REGISTER.json", register);
w("04-COMMERCIAL_ACTION_CONFLICTS.json", {
  mandatory: commercialConflicts,
  note: "Build Account / Monitor on Mode A vs Qualify Opportunity on B/C is LIVE vs PACKAGE — not a B↔C defect when B=C",
});
w("05-CONTACT_FIELD_CONFLICTS.json", {
  mandatory: contactConflicts,
  lorna: {
    liveOpportunityRoleMayBe: "Group Travel Coordinator",
    approvedDisplayRole: "RIC Conference Contact",
    B: roleOf(mapB.get("gdi_opp_regulatory_information_conference_20260930")),
    C: roleOf(mapC.get("gdi_opp_regulatory_information_conference_20260930")),
  },
});
w("06-SVG_STATE_CONFLICTS.json", {
  bcSvgMismatches: svgConflicts,
  note: "Gold icons on Mode A come from live revenueStreamClass; B/C use package attach/demotion",
});
w("07-CARD_AND_CSS_CONFLICTS.json", {
  browser,
  note: "Compare B vs C under identical viewport; Mode A layout may differ only if live content length differs",
});
w("08-SHARED_COMPONENT_IMPLEMENTATION.json", {
  status: "shared_dealality_gdi_ui_plus_package_attach",
  clientContractPreference: true,
  modeChromeRequired:
    "Authenticated UI must label Publication Preview vs Live Research",
});
w("09-FULL_BROWSER_TEST_RESULTS.json", browser);
w("10-GITHUB_AND_RAILWAY_RELEASE.json", {
  git,
  note: "Update after merge/deploy; refuse CLIENT READY if prod marker lags and B≠C",
});
w("11-FINAL_PUBLICATION_PARITY.json", certificate);

const priorInvalidation = {
  generatedAt: new Date().toISOString(),
  status: "INVALIDATED_FOR_AUTHENTICATED_VS_SHARE",
  reason:
    "Prior CLIENT READY certificates compared share↔share or equated Mode A LIVE with Mode C SHARE. Screenshots showing Build Account/Monitor vs Qualify Opportunity are Mode A vs package — not B↔C failures when B≡C.",
  supersededBy: "reports/gdi-auth-share-canonical-reconciliation-v1/",
  retainedReports: [
    "reports/gdi-component-level-client-parity-v1/",
    "reports/gdi-canonical-three-way-parity-v1/",
    "reports/gdi-canonical-customer-ui-architecture-v1/",
  ],
};
w("00-PRIOR-CERTIFICATE-INVALIDATION.json", priorInvalidation);

const founder = `# GDI Auth↔Share Canonical Reconciliation V1

**Verdict: ${verdict}**

## Why prior certifications failed

They treated **share↔share** or **live auth ↔ share** as proof that the authenticated application matches the customer report.

Founder screenshots compared **Mode A LIVE RESEARCH** to **Mode C EXTERNAL SHARE**:

| Opportunity | Mode A (LIVE) | Mode B/C (PACKAGE) |
|-------------|---------------|---------------------|
| Potomac Memorial | Build Account | Qualify Opportunity |
| Bethesda Premier Cup | Build Account | Qualify Opportunity |
| RIC | Monitor (tileBadgePlan) | Qualify Opportunity |
| Lorna Kipfer | displayRole RIC Conference Contact; opportunityRole may still be Group Travel Coordinator on LIVE | RIC Conference Contact |

**That is not a B↔C defect.** It is live-store recompute vs frozen publication.

## Mandatory parity

**Mode B FOUNDER PUBLICATION PREVIEW ≡ Mode C EXTERNAL CUSTOMER SHARE**

| Check | Result |
|------|--------|
| B↔C contract | ${bcPass ? "PASS" : "FAIL"} |
| Fingerprint | ${fpB === EXPECTED_FP ? "PASS" : "FAIL"} \`${EXPECTED_FP}\` |
| Open MATERIAL B↔C | ${openMaterial} |
| Mode A↔B | ${abLiveDiffs.length} documented INTENTIONAL live diffs |

## Modes

- **A LIVE** — \`?live=1\` — evolving research (not customer baseline)
- **B PREVIEW** — auth \`publicationPackage=1\` — approved package via canonical renderer
- **C SHARE** — external share — same package + same renderer

## Do not declare CLIENT READY for authenticated≡share unless B≡C

Share↔share alone remains **INSUFFICIENT**.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      ok,
      verdict,
      bcPass,
      abLiveDocumented: abLiveDiffs.length,
      openMaterial,
      failureCount: blocking.length,
      out: OUT,
    },
    null,
    2,
  ),
);
process.exit(ok ? 0 : 1);
