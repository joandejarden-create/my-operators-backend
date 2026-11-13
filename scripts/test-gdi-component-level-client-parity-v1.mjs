#!/usr/bin/env node
/**
 * GDI component-level client parity — local share vs Railway share.
 *
 *   npm run test:gdi-component-level-client-parity
 *
 * Compares rendered card contacts, SVG revenue-icon states, commercial-action
 * labels, and card DOM for all 67 Bethesda opportunities. Writes
 * reports/gdi-component-level-client-parity-v1/.
 *
 * A missing/timed-out environment is NOT VERIFIED (fail), not PASS.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { execSync } from "node:child_process";
import {
  extractVerifiedProfessionalTitle,
  attachUniversalTileContactPreview,
} from "../lib/group-demand-intelligence/universal-tile-contact-preview-v1.js";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-component-level-client-parity-v1");
fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

const HOTEL_ID = "recLuxvwwxID7U2B8";
const EXPECTED_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";
const EXPECTED_PKG = "gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1";

const FOCUS_FIXTURES = [
  {
    key: "ric_lorna",
    titleRe: /Regulatory Information Conference/i,
    contactNameRe: /Lorna Kipfer/i,
    expectedRoleRe: /RIC Conference Contact/i,
    forbiddenRoleRe: /Group Travel Coordinator/i,
  },
  { key: "bethesda_premier_cup", titleRe: /Bethesda Premier Cup/i },
  { key: "potomac_memorial", titleRe: /Potomac Memorial Tournament/i },
  { key: "womans_club", titleRe: /Woman'?s Club of Bethesda/i },
  { key: "acvnu_renal", titleRe: /ACVNU|Renal Week/i },
];

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
assert.notEqual(localBase, prodBase, "refuse local↔local comparison");

function sha256(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
}
function stable(v) {
  return JSON.stringify(v, Object.keys(v || {}).sort?.() ? undefined : undefined);
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

async function fetchList(base) {
  const url = `${base}/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities?share=${encodeURIComponent(share)}`;
  const res = await fetch(url, {
    headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
  });
  if (!res.ok) {
    throw new Error(`list ${base} HTTP ${res.status}`);
  }
  return res.json();
}

async function extractCards(page, base, label) {
  await page.goto(
    `${base}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`,
    { waitUntil: "domcontentloaded", timeout: 120000 },
  );
  await page.waitForSelector("[data-open], .brand-card", { timeout: 120000 });
  await page.waitForTimeout(1000);
  const reset = page.locator("#gdiResetViewBtn");
  if (await reset.count()) {
    await reset.click().catch(() => {});
    await page.waitForTimeout(700);
  }
  await page.screenshot({
    path: path.join(OUT, `screenshots/${label}-overview.png`),
    fullPage: false,
  });

  return page.evaluate(() => {
    const norm = (s) => String(s || "").replace(/\s+/g, " ").trim();
    const cards = [];
    const seen = new Set();
    for (const el of document.querySelectorAll("[data-open]")) {
      const id = el.getAttribute("data-open");
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const card = el.closest(".brand-card, .gdi-card, article") || el;
      const cs = getComputedStyle(card);
      const revenueIcons = [
        ...card.querySelectorAll(".gdi-revenue-stream-icon"),
      ].map((node) => {
        const ncs = getComputedStyle(node);
        const svg = node.querySelector("svg");
        const scs = svg ? getComputedStyle(svg) : null;
        return {
          className: String(node.className || ""),
          state: /--active/.test(node.className)
            ? "active"
            : /--muted/.test(node.className)
              ? "muted"
              : "unknown",
          color: ncs.color,
          opacity: ncs.opacity,
          svgStroke: scs?.stroke || null,
          svgColor: scs?.color || null,
          title: node.getAttribute("title") || node.getAttribute("aria-label"),
        };
      });
      const contactEl =
        card.querySelector(
          ".gdi-tile-contact, .gdi-card-contact, [data-gdi-tile-contact]",
        ) || null;
      const actionEl =
        card.querySelector(
          ".gdi-pill-sales, [class*='gdi-pill-sales'], [data-commercial-action]",
        ) ||
        [...card.querySelectorAll(".gdi-pill, .badge, span")].find((n) =>
          /Pursue|Qualify|Build Venue|Watch|Partner/i.test(
            (n.textContent || "").trim(),
          ),
        );
      const contactBlock = card.querySelector(".brand-card__contact");
      const actionPill = card.querySelector(
        ".gdi-tile-pill--sales-action, .gdi-pill-sales-qualify, [class*='gdi-pill-sales']",
      );
      cards.push({
        id,
        title: norm(
          card.querySelector(
            ".brand-card__name, h3, h2, .gdi-card-title, .brand-card-title",
          )?.textContent,
        ),
        body: norm(card.innerText).slice(0, 700),
        contactText: contactBlock
          ? norm(contactBlock.innerText || contactBlock.textContent)
          : contactEl
            ? norm(contactEl.innerText || contactEl.textContent)
            : null,
        actionText: actionPill
          ? norm(actionPill.textContent)
          : actionEl
            ? norm(actionEl.textContent)
            : null,
        actionClass: (actionPill || actionEl)?.className || null,
        revenueIcons,
        classes: card.className,
        rect: {
          w: Math.round(card.getBoundingClientRect().width),
          h: Math.round(card.getBoundingClientRect().height),
        },
        computed: {
          fontFamily: cs.fontFamily,
          fontSize: cs.fontSize,
          paddingTop: cs.paddingTop,
          borderColor: cs.borderTopColor,
          minHeight: cs.minHeight,
        },
      });
    }
    return { url: location.href.split("?")[0], cardCount: cards.length, cards };
  });
}

function dtoContact(o) {
  const t = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  return {
    tileKind: t.kind || null,
    tileName: t.name || t.displayName || o.tileContactName || null,
    tileRole: t.displayRole || t.roleLabel || o.tileContactRoleLabel || null,
    displayRoleKind: t.displayRoleKind || null,
    primaryName: pc.name || null,
    primaryTitle: pc.professionalTitle || pc.jobTitle || pc.title || null,
  };
}

function dtoIcons(o) {
  const icons = o.revenueStreamIcons || {};
  return {
    revenueStreamClass: o.revenueStreamClass || null,
    guestrooms: icons.guestrooms || null,
    eventFnb: icons.eventFnb || null,
  };
}

function dtoAction(o) {
  return {
    salesActionPillLabel: o.salesActionPillLabel || null,
    salesActivationLabel: o.salesActivationLabel || null,
    recommendedNextAction: o.recommendedNextAction || o.recommendedAction || null,
    commercialStagePillLabel: o.commercialStagePillLabel || null,
  };
}

const failures = [];
function fail(code, detail) {
  failures.push({ code, detail });
}

// Unit regression: Lorna / RIC title extract (offline)
{
  const title = extractVerifiedProfessionalTitle({
    name: "Lorna Kipfer",
    professionalTitle: "RIC Conference Contact",
  });
  if (title !== "RIC Conference Contact") {
    fail("CONTACT_MAPPING_DIFFERENCE", `offline title extract got ${title}`);
  }
  const previewed = attachUniversalTileContactPreview({
    id: "ric",
    primaryContact: {
      name: "Lorna Kipfer",
      professionalTitle: "RIC Conference Contact",
      organizationName: "U.S. Nuclear Regulatory Commission",
    },
  });
  if (previewed.tileContactRoleLabel !== "RIC Conference Contact") {
    fail(
      "CONTACT_MAPPING_DIFFERENCE",
      `offline preview role ${previewed.tileContactRoleLabel}`,
    );
  }
  const pkgAttach = attachPackageCustomerPresentation({
    id: "ric",
    primaryContact: {
      name: "Lorna Kipfer",
      professionalTitle: "RIC Conference Contact",
    },
    revenueStreamClass: "GUESTROOMS_ONLY",
    guestroomThesis: "Documented lodging.",
  });
  if (pkgAttach.revenueStreamIcons?.guestrooms !== "active") {
    fail("SVG_STATE_DIFFERENCE", "package attach guestroom icon not active");
  }
  const thinPkg = attachPackageCustomerPresentation({
    id: "ric_thin",
    primaryContact: {
      name: "Lorna Kipfer",
      professionalTitle: "RIC Conference Contact",
    },
  });
  if (thinPkg.revenueStreamIcons) {
    fail(
      "SVG_STATE_DIFFERENCE",
      "thin package must not invent revenueStreamIcons",
    );
  }
}

let localList;
let prodList;
try {
  [localList, prodList] = await Promise.all([
    fetchList(localBase),
    fetchList(prodBase),
  ]);
} catch (e) {
  fail("NOT_VERIFIED", String(e.message || e));
  const early = {
    ok: false,
    verdict: "NOT VERIFIED",
    failures,
    generatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(path.join(OUT, "10-FINAL-PARITY-CERTIFICATE.json"), JSON.stringify(early, null, 2));
  console.error(JSON.stringify(early, null, 2));
  process.exit(1);
}

const localFp = localList.publication?.contentFingerprintSha256;
const prodFp = prodList.publication?.contentFingerprintSha256;
if (localFp !== EXPECTED_FP || prodFp !== EXPECTED_FP) {
  fail("PUBLICATION_PACKAGE_DIFFERENCE", { localFp, prodFp });
}
if (
  localList.publication?.publicationVersion !== EXPECTED_PKG ||
  prodList.publication?.publicationVersion !== EXPECTED_PKG
) {
  fail("PUBLICATION_PACKAGE_DIFFERENCE", "package version");
}

const localOpps = localList.opportunities || [];
const prodOpps = prodList.opportunities || [];
if (localOpps.length !== 67 || prodOpps.length !== 67) {
  fail("DATA_FIELD_DIFFERENCE", `counts ${localOpps.length}/${prodOpps.length}`);
}

const byId = (arr) => new Map((arr || []).map((o) => [o.id || o.opportunityId, o]));
const localDto = byId(localOpps);
const prodDto = byId(prodOpps);

const svgParity = [];
const contactParity = [];
const actionParity = [];
const rootCauses = [];

for (const id of [...new Set([...localDto.keys(), ...prodDto.keys()])]) {
  const l = localDto.get(id);
  const p = prodDto.get(id);
  if (!l || !p) {
    rootCauses.push({
      id,
      classification: "DATA_FIELD_DIFFERENCE",
      evidence: "missing opportunity on one side",
    });
    fail("DATA_FIELD_DIFFERENCE", id);
    continue;
  }
  const lc = dtoContact(l);
  const pc = dtoContact(p);
  const contactRow = {
    id,
    title: l.title || l.displayTitle || l.name,
    local: lc,
    production: pc,
    match: JSON.stringify(lc) === JSON.stringify(pc),
  };
  contactParity.push(contactRow);
  if (!contactRow.match) {
    fail("CONTACT_MAPPING_DIFFERENCE", id);
    rootCauses.push({
      id,
      classification: "CONTACT_MAPPING_DIFFERENCE",
      evidence: contactRow,
      recommended:
        "Ensure package read-time attachUniversalTileContactPreview runs on both environments; do not invent titles.",
    });
  }

  const li = dtoIcons(l);
  const pi = dtoIcons(p);
  const iconRow = {
    id,
    title: l.title || l.displayTitle,
    local: li,
    production: pi,
    match: JSON.stringify(li) === JSON.stringify(pi),
  };
  svgParity.push(iconRow);
  if (!iconRow.match) {
    fail("SVG_STATE_DIFFERENCE", id);
    rootCauses.push({
      id,
      classification: "SVG_STATE_DIFFERENCE",
      evidence: iconRow,
      recommended:
        "Attach revenue stream icons from evidence-backed class; do not recolor without revenueStreamClass/thesis.",
    });
  }

  const la = dtoAction(l);
  const pa = dtoAction(p);
  const actionRow = {
    id,
    title: l.title || l.displayTitle,
    local: la,
    production: pa,
    match: JSON.stringify(la) === JSON.stringify(pa),
  };
  actionParity.push(actionRow);
  if (!actionRow.match) {
    fail("COMMERCIAL_ACTION_DIFFERENCE", id);
    rootCauses.push({
      id,
      classification: "COMMERCIAL_ACTION_DIFFERENCE",
      evidence: actionRow,
      recommended:
        "Use approved package salesActionPillLabel; flag substantive changes for new fingerprint.",
    });
  }
}

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 1,
  locale: "en-US",
  timezoneId: "America/New_York",
});
const localPage = await ctx.newPage();
const prodPage = await ctx.newPage();
let localUi;
let prodUi;
try {
  localUi = await extractCards(localPage, localBase, "local");
  prodUi = await extractCards(prodPage, prodBase, "prod");
} catch (e) {
  fail("NOT_VERIFIED", `browser extract: ${e.message || e}`);
  await browser.close();
  const early = { ok: false, verdict: "NOT VERIFIED", failures };
  fs.writeFileSync(path.join(OUT, "10-FINAL-PARITY-CERTIFICATE.json"), JSON.stringify(early, null, 2));
  process.exit(1);
}

const uiById = (cards) => new Map(cards.map((c) => [c.id, c]));
const localCards = uiById(localUi.cards);
const prodCards = uiById(prodUi.cards);
const cardDomParity = [];
const uiComponentParity = [];

for (const id of localDto.keys()) {
  const l = localCards.get(id);
  const p = prodCards.get(id);
  const row = {
    id,
    title: l?.title || p?.title || null,
    present: { local: Boolean(l), production: Boolean(p) },
    contactMatch: l && p ? l.contactText === p.contactText : false,
    actionMatch: l && p ? l.actionText === p.actionText : false,
    svgMatch: l && p ? JSON.stringify(l.revenueIcons) === JSON.stringify(p.revenueIcons) : false,
    rectMatch: l && p ? JSON.stringify(l.rect) === JSON.stringify(p.rect) : false,
    local: l
      ? {
          contactText: l.contactText,
          actionText: l.actionText,
          revenueIcons: l.revenueIcons,
          rect: l.rect,
          computed: l.computed,
        }
      : null,
    production: p
      ? {
          contactText: p.contactText,
          actionText: p.actionText,
          revenueIcons: p.revenueIcons,
          rect: p.rect,
          computed: p.computed,
        }
      : null,
  };
  cardDomParity.push(row);
  if (!row.present.local || !row.present.production) {
    fail("DOM_STRUCTURE_DIFFERENCE", id);
  } else {
    if (!row.contactMatch) fail("CONTACT_MAPPING_DIFFERENCE", `ui:${id}`);
    if (!row.actionMatch) fail("COMMERCIAL_ACTION_DIFFERENCE", `ui:${id}`);
    if (!row.svgMatch) fail("SVG_STATE_DIFFERENCE", `ui:${id}`);
  }
  uiComponentParity.push(row);
}

// Focus fixtures
const fixtures = {};
for (const fx of FOCUS_FIXTURES) {
  const localCard = localUi.cards.find((c) => fx.titleRe.test(c.title || ""));
  const prodCard = prodUi.cards.find((c) => fx.titleRe.test(c.title || ""));
  const localOpp = [...localDto.values()].find((o) =>
    fx.titleRe.test(o.title || o.displayTitle || ""),
  );
  const prodOpp = [...prodDto.values()].find((o) =>
    fx.titleRe.test(o.title || o.displayTitle || ""),
  );
  const entry = {
    localCard,
    productionCard: prodCard,
    localDto: localOpp ? dtoContact(localOpp) : null,
    productionDto: prodOpp ? dtoContact(prodOpp) : null,
    localIcons: localOpp ? dtoIcons(localOpp) : null,
    productionIcons: prodOpp ? dtoIcons(prodOpp) : null,
    localAction: localOpp ? dtoAction(localOpp) : null,
    productionAction: prodOpp ? dtoAction(prodOpp) : null,
  };
  if (fx.contactNameRe && localCard) {
    if (!fx.contactNameRe.test(localCard.contactText || "")) {
      fail("CONTACT_MAPPING_DIFFERENCE", `${fx.key}: local missing contact name`);
    }
  }
  if (fx.expectedRoleRe) {
    const role =
      entry.localDto?.tileRole ||
      localCard?.contactText ||
      "";
    if (!fx.expectedRoleRe.test(String(role))) {
      fail(
        "CONTACT_MAPPING_DIFFERENCE",
        `${fx.key}: expected role ${fx.expectedRoleRe} got ${role}`,
      );
    }
    if (fx.forbiddenRoleRe && fx.forbiddenRoleRe.test(String(role))) {
      fail(
        "CONTACT_MAPPING_DIFFERENCE",
        `${fx.key}: forbidden role present`,
      );
    }
  }
  if (
    localCard &&
    prodCard &&
    (localCard.contactText !== prodCard.contactText ||
      localCard.actionText !== prodCard.actionText ||
      JSON.stringify(localCard.revenueIcons) !==
        JSON.stringify(prodCard.revenueIcons))
  ) {
    fail("COMPONENT_LEVEL_PARITY", fx.key);
  }
  fixtures[fx.key] = entry;
}

await browser.close();

const git = gitIdentity();
const ok = failures.length === 0;
const certificate = {
  contract: "GDI_COMPONENT_LEVEL_CLIENT_PARITY_V1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL_ID,
  expectedFingerprint: EXPECTED_FP,
  expectedPackage: EXPECTED_PKG,
  fingerprint: { local: localFp, production: prodFp },
  opportunityCounts: { local: localOpps.length, production: prodOpps.length },
  comparedEnvironments: {
    local: localBase,
    production: prodBase,
    role: "customer_share_vs_customer_share",
  },
  authLiveNote:
    "Authenticated live GDI (without ?publicationPackage=1) may still recompute commercial fields from the live store. Certified surface is founder-approved customer-share preview ↔ Railway share on the approved fingerprint.",
  mismatchSummary: {
    contactDto: contactParity.filter((r) => !r.match).length,
    svgDto: svgParity.filter((r) => !r.match).length,
    actionDto: actionParity.filter((r) => !r.match).length,
    uiCards: uiComponentParity.filter(
      (r) => !r.contactMatch || !r.actionMatch || !r.svgMatch,
    ).length,
  },
  failureCount: failures.length,
  failures: failures.slice(0, 80),
  ok,
  verdict: ok ? "CLIENT READY" : "NOT CERTIFIED",
  git,
};

const files = {
  "01-SVG-ICON-PARITY.json": {
    compared: svgParity.length,
    mismatches: svgParity.filter((r) => !r.match),
    all: svgParity,
  },
  "02-CONTACT-PARITY.json": {
    compared: contactParity.length,
    mismatches: contactParity.filter((r) => !r.match),
    fixtures,
    all: contactParity,
  },
  "03-COMMERCIAL-ACTION-PARITY.json": {
    compared: actionParity.length,
    mismatches: actionParity.filter((r) => !r.match),
    all: actionParity,
  },
  "04-CARD-DOM-AND-CSS-PARITY.json": {
    compared: cardDomParity.length,
    mismatches: cardDomParity.filter(
      (r) =>
        !r.present.local ||
        !r.present.production ||
        !r.contactMatch ||
        !r.actionMatch ||
        !r.svgMatch,
    ),
    sampleComputed: {
      local: localUi.cards[0]?.computed || null,
      production: prodUi.cards[0]?.computed || null,
    },
  },
  "05-COMPLETE-UI-COMPONENT-PARITY.json": {
    localCardCount: localUi.cardCount,
    productionCardCount: prodUi.cardCount,
    fixtures,
    uiComponentParity,
  },
  "06-ROOT-CAUSE-REGISTER.json": {
    rootCauses,
    knownIntentional: [
      {
        classification: "PERMISSION_DIFFERENCE",
        note: "Auth research browse without publicationPackage=1 is not the certified customer surface.",
      },
    ],
  },
  "07-REGRESSION-TEST-RESULTS.json": {
    offlineUnitChecks: "embedded",
    browserCompared: true,
    failures,
  },
  "08-GITHUB-AND-RAILWAY-RELEASE.json": {
    git,
    note: "Filled/updated after PR merge + Railway deploy in FOUNDER_REPORT.",
  },
  "09-INDEPENDENT-PRODUCTION-VERIFICATION.json": {
    productionBase: prodBase,
    fingerprint: prodFp,
    cardCount: prodUi.cardCount,
    fixtures: Object.fromEntries(
      Object.entries(fixtures).map(([k, v]) => [
        k,
        {
          contact: v.productionCard?.contactText || null,
          action: v.productionCard?.actionText || null,
          icons: v.productionCard?.revenueIcons || null,
        },
      ]),
    ),
  },
  "10-FINAL-PARITY-CERTIFICATE.json": certificate,
};

for (const [name, body] of Object.entries(files)) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(body, null, 2));
}

const founderReport = `# GDI Component-Level Client Parity V1

**Verdict:** ${certificate.verdict}  
**Generated:** ${certificate.generatedAt}  
**Hotel:** Bethesda Marriott (\`${HOTEL_ID}\`)  
**Fingerprint:** \`${EXPECTED_FP}\`  
**Package:** \`${EXPECTED_PKG}\`

## Comparison surface

- **A.** Local founder-approved customer-share preview (\`${localBase}\`)
- **B.** Railway external customer-share (\`${prodBase}\`)

Authenticated live GDI without \`?publicationPackage=1\` is **out of scope** for CLIENT READY (may recompute live store fields). Documented under PERMISSION / DATA path differences.

## Summary

| Check | Result |
|-------|--------|
| Fingerprint both sides | ${localFp === EXPECTED_FP && prodFp === EXPECTED_FP ? "PASS" : "FAIL"} |
| Opportunity count 67/67 | ${localOpps.length === 67 && prodOpps.length === 67 ? "PASS" : "FAIL"} |
| Contact DTO mismatches | ${certificate.mismatchSummary.contactDto} |
| SVG/icon DTO mismatches | ${certificate.mismatchSummary.svgDto} |
| Commercial-action mismatches | ${certificate.mismatchSummary.actionDto} |
| UI card mismatches | ${certificate.mismatchSummary.uiCards} |
| Total failure codes | ${failures.length} |

## Root causes addressed in this release

1. **CONTACT_MAPPING_DIFFERENCE (Lorna Kipfer / RIC)** — \`extractVerifiedProfessionalTitle\` now keeps assigned \`* Conference Contact\` titles instead of demoting to \`Group Travel Coordinator\`. Package LIST/DETAIL attach \`tileContactPreview\` at read time.
2. **SVG_STATE_DIFFERENCE** — Package responses omitted \`revenueStreamIcons\` / \`revenueStreamClass\` attach that live sanitize already applied. Read-time \`attachPackageCustomerPresentation\` applies the same evidence-backed icon resolver (no blind recolor).
3. **CSS / Proper Case** — \`toProperCaseContactRoleLabel\` preserves \`U.S.\` / dotted acronyms so share does not render \`U.s.\`.
4. **Asset cache** — \`dealality-gdi-ui.js?v=gdi-component-parity-v1\` on founder + share HTML.

## Fixtures

See \`02-CONTACT-PARITY.json\` / \`05-COMPLETE-UI-COMPONENT-PARITY.json\` for Regulatory Information Conference (Lorna Kipfer), Bethesda Premier Cup, Potomac Memorial Tournament, Woman's Club of Bethesda, ACVNU Renal Week.

## Mandatory acceptance

${ok ? "All component-level share↔share checks passed. CLIENT READY." : "NOT CERTIFIED — see failures in `10-FINAL-PARITY-CERTIFICATE.json`. Do not claim CLIENT READY until local and production share render identical approved contacts, SVG states, commercial actions, and card content."}

## Git

- Branch: \`${git.branch || "?"}\`
- HEAD: \`${git.head || "?"}\`
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founderReport);

console.log(
  JSON.stringify(
    {
      ok,
      verdict: certificate.verdict,
      failureCount: failures.length,
      mismatchSummary: certificate.mismatchSummary,
      out: OUT,
    },
    null,
    2,
  ),
);

process.exit(ok ? 0 : 1);
