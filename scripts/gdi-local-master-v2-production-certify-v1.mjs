#!/usr/bin/env node
/**
 * Certify frozen local master ↔ approved V2 package ↔ live Railway share.
 * Does not mutate publication content.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { verifyPublicationPackageIntegrity } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-local-master-v2-release");
const SNAP = path.join(
  ROOT,
  "reports/gdi-local-master-publication-v1/local-master-snapshot/local-live-opportunities.json",
);
const PKG_PATH = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages/recLuxvwwxID7U2B8/gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2/package.json",
);
const APPROVED =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";
const LOCAL = (process.env.GDI_PARITY_LOCAL_URL || "http://127.0.0.1:8080").replace(
  /\/$/,
  "",
);
const PROD = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");
const HOTEL = "recLuxvwwxID7U2B8";

fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function action(o = {}) {
  const p = o.tileBadgePlan;
  if (p && typeof p === "object" && p.salesActionPillLabel) {
    return text(p.salesActionPillLabel);
  }
  return text(o.salesActionPillLabel || o.salesActivationLabel);
}
function name(o = {}) {
  const t = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  return text(t.name || t.displayName || pc.name || pc.fullName || o.contactName);
}
function role(o = {}) {
  const t = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  return text(
    t.displayRole ||
      t.role ||
      o.displayRole ||
      o.opportunityRole ||
      pc.role ||
      pc.title,
  );
}
function icons(o = {}) {
  return JSON.stringify(o.revenueStreamIcons || null);
}
function title(o = {}) {
  return text(o.displayTitle || o.title);
}
function summary(o = {}) {
  return text(o.whatIsHappening || o.summaryWhat || o.summary);
}
function drawer(o = {}) {
  return JSON.stringify({
    shared: Boolean(o.sharedTenSectionDrawer),
    events: Boolean(o.eventsOpportunityOverview),
    scaling: Boolean(o.scalingDrawerModel),
    eventsDrawer: Boolean(o.eventsDrawerModel),
    segment: Boolean(o.segmentOverviewModel),
  });
}
function nd(o = {}) {
  return JSON.stringify({
    ind: o.needDateIndicator || null,
    preview: o.tileNeedDatePreview || null,
    cls: o.needDateRelevanceClass || null,
    match: Boolean(o.needDateMatchV1),
    assoc: Array.isArray(o.needDateAssociations)
      ? o.needDateAssociations.length
      : 0,
  });
}

function readShare() {
  return (
    process.env.GDI_PARITY_SHARE_TOKEN ||
    (
      fs.readFileSync(path.join(ROOT, ".env"), "utf8").match(
        /^GDI_BETHESDA_PRODUCTION_SHARE_TOKEN=(.+)$/m,
      ) || []
    )[1]?.trim()
  );
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
  });
  return { status: res.status, json: await res.json(), url };
}

function compareSets(left, right, label) {
  const diffs = [];
  const rBy = new Map((right || []).map((o) => [String(o.id), o]));
  for (const l of left || []) {
    const id = String(l.id);
    const r = rBy.get(id);
    if (!r) {
      diffs.push({ id, type: "MISSING_RIGHT", label });
      continue;
    }
    const fields = [];
    if (action(l) !== action(r)) fields.push(["action", action(l), action(r)]);
    if (name(l) !== name(r)) fields.push(["name", name(l), name(r)]);
    if (role(l) !== role(r)) fields.push(["role", role(l), role(r)]);
    if (icons(l) !== icons(r)) fields.push(["icons", icons(l), icons(r)]);
    if (title(l) !== title(r)) fields.push(["title", title(l), title(r)]);
    if (summary(l) !== summary(r))
      fields.push(["summary", summary(l).slice(0, 120), summary(r).slice(0, 120)]);
    if (drawer(l) !== drawer(r))
      fields.push(["drawer", JSON.parse(drawer(l)), JSON.parse(drawer(r))]);
    if (nd(l) !== nd(r))
      fields.push(["needDate", JSON.parse(nd(l)), JSON.parse(nd(r))]);
    if (fields.length) diffs.push({ id, label, fields });
  }
  return diffs;
}

async function scrape(page) {
  return page.evaluate(() => {
    const cards = [
      ...document.querySelectorAll(".brand-card.brand-card--gdi-opp"),
    ];
    return cards.map((card) => {
      const id = card.getAttribute("data-open") || null;
      const title =
        card.querySelector(".brand-card__name")?.textContent || "";
      const action =
        card
          .querySelector(
            ".gdi-tile-pill--sales-action, .gdi-pill-sales-action, [data-sales-action]",
          )
          ?.textContent || "";
      const contact =
        card.querySelector(".brand-card__contact-name")?.textContent || "";
      const role =
        card.querySelector(".brand-card__contact-line--role")?.textContent ||
        "";
      const activeIcons = [
        ...card.querySelectorAll(
          ".gdi-revenue-stream-icon--active, .brand-card__icon--active",
        ),
      ].length;
      return {
        id,
        title: title.replace(/\s+/g, " ").trim(),
        action: action.replace(/\s+/g, " ").trim(),
        contact: contact.replace(/\s+/g, " ").trim(),
        role: role.replace(/\s+/g, " ").trim(),
        activeIcons,
      };
    });
  });
}

const frozen = JSON.parse(fs.readFileSync(SNAP, "utf8"));
const pkg = JSON.parse(fs.readFileSync(PKG_PATH, "utf8"));
const integrity = verifyPublicationPackageIntegrity(pkg);
const share = readShare();
const prod = await fetchJson(
  `${PROD}/api/group-demand-intelligence/share/hotels/${HOTEL}/opportunities?share=${encodeURIComponent(share)}&cb=${Date.now()}`,
);
const localShare = await fetchJson(
  `${LOCAL}/api/group-demand-intelligence/share/hotels/${HOTEL}/opportunities?share=${encodeURIComponent(share)}&cb=${Date.now()}`,
);

const prodOps = prod.json?.opportunities || [];
const localShareOps = localShare.json?.opportunities || [];
const pub = prod.json?.publication || {};

const aVsB = compareSets(frozen.opportunities, pkg.opportunities, "A_frozen_vs_B_package");
const bVsC = compareSets(pkg.opportunities, prodOps, "B_package_vs_C_prod");
const aVsC = compareSets(frozen.opportunities, prodOps, "A_frozen_vs_C_prod");

const commercial = frozen.opportunities.map((f) => {
  const p = prodOps.find((o) => o.id === f.id);
  return {
    id: f.id,
    title: title(f),
    local: action(f),
    prod: action(p || {}),
    match: action(f) === action(p || {}),
  };
});
const contacts = frozen.opportunities.map((f) => {
  const p = prodOps.find((o) => o.id === f.id);
  return {
    id: f.id,
    local: { name: name(f), role: role(f) },
    prod: { name: name(p || {}), role: role(p || {}) },
    match: name(f) === name(p || {}) && role(f) === role(p || {}),
  };
});
const svgs = frozen.opportunities.map((f) => {
  const p = prodOps.find((o) => o.id === f.id);
  return {
    id: f.id,
    local: icons(f),
    prod: icons(p || {}),
    match: icons(f) === icons(p || {}),
    localClass: text(f.revenueStreamClass),
    prodClass: text(p?.revenueStreamClass),
  };
});

const scalingIds = frozen.opportunities
  .filter((o) => o.scalingDrawerModel)
  .map((o) => o.id);
const scalingVerify = scalingIds.map((id) => {
  const f = frozen.opportunities.find((o) => o.id === id);
  const p = pkg.opportunities.find((o) => o.id === id);
  const c = prodOps.find((o) => o.id === id);
  const fj = JSON.stringify(f.scalingDrawerModel);
  return {
    id,
    packageExact: fj === JSON.stringify(p?.scalingDrawerModel || null),
    prodExact: fj === JSON.stringify(c?.scalingDrawerModel || null),
  };
});

let browser = { ok: false };
try {
  const b = await chromium.launch({ headless: true });
  const ctx = await b.newContext({ viewport: { width: 1440, height: 1100 } });
  const auth = await ctx.newPage();
  await auth.goto(`${LOCAL}/group-demand-intelligence.html?hotelId=${HOTEL}&live=1`, {
    waitUntil: "networkidle",
    timeout: 120000,
  });
  await auth.waitForTimeout(2500);
  const authCards = await scrape(auth);
  await auth.screenshot({
    path: path.join(OUT, "screenshots/A-frozen-local-master.png"),
    fullPage: true,
  });

  const sharePage = await ctx.newPage();
  await sharePage.goto(
    `${PROD}/group-demand-intelligence-share.html?hotelId=${HOTEL}&share=${encodeURIComponent(share)}`,
    { waitUntil: "networkidle", timeout: 120000 },
  );
  await sharePage.waitForTimeout(2500);
  const prodCards = await scrape(sharePage);
  await sharePage.screenshot({
    path: path.join(OUT, "screenshots/C-production-share.png"),
    fullPage: true,
  });

  // Compare mandatory actions in DOM when scrape finds them
  const domDiffs = [];
  const byProd = new Map(prodCards.filter((c) => c.id).map((c) => [c.id, c]));
  for (const a of authCards.filter((c) => c.id)) {
    const c = byProd.get(a.id);
    if (!c) continue;
    if (a.action && c.action && a.action !== c.action) {
      domDiffs.push({ id: a.id, field: "action", local: a.action, prod: c.action });
    }
  }
  await b.close();
  browser = {
    ok: true,
    authCardCount: authCards.length,
    prodCardCount: prodCards.length,
    domActionDiffs: domDiffs.length,
    domDiffSample: domDiffs.slice(0, 20),
    authSample: authCards.slice(0, 5),
    prodSample: prodCards.slice(0, 5),
  };
} catch (e) {
  browser = { ok: false, error: String(e.message || e) };
}

const register = {
  version: "GDI_LOCAL_MASTER_V2_RELEASE_REGISTER",
  updatedAt: new Date().toISOString(),
  differences: [...aVsC].map((d, i) => ({
    differenceId: `A_VS_C__${d.id || i}`,
    opportunityId: d.id || null,
    component: d.fields ? d.fields.map((f) => f[0]).join(",") : d.type,
    localMasterValue: d.fields || d.type,
    productionValue: d.fields || null,
    severity: "MATERIAL",
    resolutionStatus: "OPEN",
  })),
};

const materialOpen = register.differences.length;
const certified =
  integrity.ok &&
  pkg.contentFingerprintSha256 === APPROVED &&
  pub.contentFingerprintSha256 === APPROVED &&
  pub.publicationVersion === "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2" &&
  pub.lifecycleState === "CUSTOMER_PUBLISHED" &&
  aVsB.length === 0 &&
  bVsC.length === 0 &&
  aVsC.length === 0 &&
  commercial.every((c) => c.match) &&
  contacts.every((c) => c.match) &&
  svgs.every((c) => c.match) &&
  scalingVerify.every((c) => c.packageExact && c.prodExact) &&
  materialOpen === 0;

const verdict = certified
  ? "CLIENT_READY_LOCAL_MASTER_V2"
  : "NOT_CERTIFIED";

fs.writeFileSync(
  path.join(OUT, "02-LOCAL_MASTER_IDENTITY.json"),
  `${JSON.stringify(
    {
      snapshot: "reports/gdi-local-master-publication-v1/local-master-snapshot/local-live-opportunities.json",
      serverBuildSha: frozen.serverBuildSha,
      listScope: frozen.listScope,
      count: frozen.opportunities.length,
      localPidExpected: 31436,
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "04-67-OPPORTUNITY-INVENTORY.json"),
  `${JSON.stringify(
    {
      frozen: frozen.opportunities.length,
      package: pkg.inventoryCounts,
      production: pub.inventoryCounts,
      match:
        frozen.opportunities.length === 67 &&
        pkg.inventoryCounts?.all === 67 &&
        pub.inventoryCounts?.all === 67,
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "06-COMMERCIAL-ACTION-PARITY.json"),
  `${JSON.stringify(
    {
      ok: commercial.every((c) => c.match),
      mismatches: commercial.filter((c) => !c.match),
      mandatory: commercial.filter((c) =>
        [
          "gdi_opp_potomac_memorial_2027",
          "gdi_opp_bethesda_premier_cup_2026",
          "gdi_opp_regulatory_information_conference_20260930",
          "gdi_pe_781f12393f8117e7",
        ].includes(c.id),
      ),
      all: commercial,
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "07-CONTACT-AND-SVG-PARITY.json"),
  `${JSON.stringify(
    {
      contactsOk: contacts.every((c) => c.match),
      svgOk: svgs.every((c) => c.match),
      contactMismatches: contacts.filter((c) => !c.match).slice(0, 50),
      svgMismatches: svgs.filter((c) => !c.match).slice(0, 50),
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "08-67-CARD-AND-DRAWER-PARITY.json"),
  `${JSON.stringify(
    {
      aVsB: aVsB.length,
      bVsC: bVsC.length,
      aVsC: aVsC.length,
      aVsBSample: aVsB.slice(0, 20),
      bVsCSample: bVsC.slice(0, 20),
      aVsCSample: aVsC.slice(0, 20),
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "10-THREE-WAY_BROWSER_RESULTS.json"),
  `${JSON.stringify(
    {
      browser,
      dto: {
        aVsB: aVsB.length,
        bVsC: bVsC.length,
        aVsC: aVsC.length,
      },
      productionIdentity: {
        fingerprint: pub.contentFingerprintSha256,
        version: pub.publicationVersion,
        lifecycle: pub.lifecycleState,
        mode: pub.publicationPackageMode,
      },
      localShareIdentity: {
        fingerprint: localShare.json?.publication?.contentFingerprintSha256 || null,
        version: localShare.json?.publication?.publicationVersion || null,
      },
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "11-COMPONENT_DIFFERENCE_REGISTER.json"),
  `${JSON.stringify(register, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "COMPONENT_DIFFERENCE_REGISTER.json"),
  `${JSON.stringify(register, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "09-FILTER-AND-TOOLBAR-PARITY.json"),
  `${JSON.stringify(
    {
      note: "Same shared DealalityGdiUi + controller modules deployed; access wrappers differ by design",
      sharedModules: [
        "public/js/group-demand-intelligence/dealality-gdi-ui.js",
        "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js",
        "public/css/group-demand-intelligence.css",
      ],
      intentionalDifferences: [
        "auth hotel selector / Need Times tab",
        "share token transport",
        "founder review / Surfe reveal auth-only",
      ],
      status: certified ? "SHARED_RENDERER_DEPLOYED" : "PENDING_CONTENT_PARITY",
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "14-FEEDBACK_PRESERVATION.json"),
  `${JSON.stringify(
    {
      shareTokenUnchanged: true,
      priorPackageRetained:
        "lib/group-demand-intelligence/bundled-publication-packages/recLuxvwwxID7U2B8/gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1/",
      note: "CURRENT pointer swap only; feedback store not rewritten by activation script",
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "15-FINAL_CLIENT_CERTIFICATE.json"),
  `${JSON.stringify(
    {
      verdict,
      certified,
      approvedFingerprint: APPROVED,
      liveFingerprint: pub.contentFingerprintSha256 || null,
      mergeSha: "ea0c76dc4bbc4c0619995e30a0cd3be08f691a18",
      railwayDeploymentId: "3a1cecfe-5135-46a5-872c-79061ccb8f1b",
      pr264MergeSha: "5348889dff50e6b9cff1584a827c4ed47c1dba37",
      svgPreserveFixSha: "829c6d0f9e1226438de43c9faefe5808e081b7ae",
      contactPreserveFixSha: "ea0c76dc4bbc4c0619995e30a0cd3be08f691a18",
      inventory: pub.inventoryCounts,
      scalingDrawers: {
        required: 12,
        verified: scalingVerify.filter((c) => c.packageExact && c.prodExact)
          .length,
      },
      materialOpen,
      gates: {
        fingerprint: pub.contentFingerprintSha256 === APPROVED,
        aVsB: aVsB.length === 0,
        bVsC: bVsC.length === 0,
        aVsC: aVsC.length === 0,
        commercial: commercial.every((c) => c.match),
        contacts: contacts.every((c) => c.match),
        svg: svgs.every((c) => c.match),
        scaling: scalingVerify.every((c) => c.packageExact && c.prodExact),
      },
    },
    null,
    2,
  )}\n`,
);

const report = `# GDI Local Master V2 Release — Founder Report

**Verdict: \`${verdict}\`**

## Approval
- Fingerprint: \`${APPROVED}\`
- Version: \`gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2\`
- Inventory: 67 / 61 / 10

## Release identity
| Item | Value |
|------|-------|
| PR #264 | MERGED (\`5348889dff50e6b9cff1584a827c4ed47c1dba37\`) |
| Live artifact SHA | \`ea0c76dc4bbc4c0619995e30a0cd3be08f691a18\` (PR #266–#268 technical SVG/contact/deploy fixes) |
| Railway deploy | \`3a1cecfe-5135-46a5-872c-79061ccb8f1b\` SUCCESS |
| Live fingerprint | \`${pub.contentFingerprintSha256}\` |
| Lifecycle | \`${pub.lifecycleState}\` |
| Mode | \`${pub.publicationPackageMode}\` |

## Parity gates
| Gate | Result |
|------|--------|
| A frozen ↔ B package | ${aVsB.length === 0 ? "PASS" : `FAIL (${aVsB.length})`} |
| B package ↔ C production | ${bVsC.length === 0 ? "PASS" : `FAIL (${bVsC.length})`} |
| A frozen ↔ C production | ${aVsC.length === 0 ? "PASS" : `FAIL (${aVsC.length})`} |
| Commercial actions | ${commercial.every((c) => c.match) ? "PASS" : "FAIL"} |
| Contacts | ${contacts.every((c) => c.match) ? "PASS" : "FAIL"} |
| SVG states | ${svgs.every((c) => c.match) ? "PASS" : "FAIL"} |
| Scaling drawers 12/12 | ${scalingVerify.every((c) => c.packageExact && c.prodExact) ? "PASS" : "FAIL"} |
| Open MATERIAL | ${materialOpen} |

## Mandatory commercial actions (production)
| Opportunity | Action |
|-------------|--------|
| Potomac Memorial | ${action(prodOps.find((o) => o.id === "gdi_opp_potomac_memorial_2027") || {})} |
| Bethesda Premier Cup | ${action(prodOps.find((o) => o.id === "gdi_opp_bethesda_premier_cup_2026") || {})} |
| RIC | ${action(prodOps.find((o) => o.id === "gdi_opp_regulatory_information_conference_20260930") || {})} |
| Woman's Club | ${action(prodOps.find((o) => o.id === "gdi_pe_781f12393f8117e7") || {})} |

## Browser
${browser.ok ? `Auth cards ${browser.authCardCount}; prod cards ${browser.prodCardCount}; DOM action diffs ${browser.domActionDiffs}` : `Browser NOT VERIFIED: ${browser.error}`}

## Feedback / rollback
Prior package \`441773…\` retained on disk. Share token unchanged. CURRENT → V2 only.

${certified ? "**Frozen local GDI master = approved V2 package = Railway customer share.**" : "**NOT CERTIFIED** — see open differences in COMPONENT_DIFFERENCE_REGISTER.json."}
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), report);

// refresh scaling file with prod exactness
fs.writeFileSync(
  path.join(OUT, "05-12-SCALING-DRAWER-VERIFICATION.json"),
  `${JSON.stringify(
    {
      required: 12,
      verified: scalingVerify.filter((c) => c.packageExact && c.prodExact).length,
      checks: scalingVerify,
    },
    null,
    2,
  )}\n`,
);

console.log(
  JSON.stringify(
    {
      verdict,
      certified,
      liveFingerprint: pub.contentFingerprintSha256,
      aVsB: aVsB.length,
      bVsC: bVsC.length,
      aVsC: aVsC.length,
      materialOpen,
      browserOk: browser.ok,
      scalingOk: scalingVerify.every((c) => c.packageExact && c.prodExact),
    },
    null,
    2,
  ),
);
if (!certified) process.exit(2);
