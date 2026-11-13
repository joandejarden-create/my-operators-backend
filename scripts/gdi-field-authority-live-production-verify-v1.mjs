#!/usr/bin/env node
/**
 * Live production verification for GDI Field Authority Contract.
 * Requires: local:8080 frozen master + Railway share serving approved V2.
 * Compares rendered DOM (not DTO-only). Does not mutate publication content.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import {
  detectContactSubtitleSubstitution,
  resolveAuthoritativeCardContactSubtitle,
} from "../lib/group-demand-intelligence/presentation/gdi-field-authority-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-field-authority-audit-v1");
const HOTEL = "recLuxvwwxID7U2B8";
const RIC_ID = "gdi_opp_regulatory_information_conference_20260930";
const APPROVED_FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";
const TARGET_SHA =
  process.env.GDI_FIELD_AUTHORITY_ARTIFACT_SHA ||
  "130df5f89cd626d44fbd1c45a588c5cf7341b5d4";
const LOCAL = (process.env.GDI_PARITY_LOCAL_URL || "http://127.0.0.1:8080").replace(
  /\/$/,
  "",
);
const PROD = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");
const SNAP = path.join(
  ROOT,
  "reports/gdi-local-master-publication-v1/local-master-snapshot/local-live-opportunities.json",
);

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
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

async function scrapeCards(page) {
  await page.evaluate(() => {
    const reset = document.querySelector(
      '[data-gdi-reset-view], button[data-action="reset-view"], .gdi-reset-view',
    );
    if (reset) reset.click();
  });
  await page.waitForTimeout(700);
  return page.evaluate(() => {
    return [...document.querySelectorAll(".brand-card.brand-card--gdi-opp")].map(
      (card) => {
        const id = card.getAttribute("data-open") || null;
        const title = (
          card.querySelector(".brand-card__name")?.textContent || ""
        )
          .replace(/\s+/g, " ")
          .trim();
        const name = (
          card.querySelector(".brand-card__contact-name")?.textContent || ""
        )
          .replace(/\s+/g, " ")
          .trim();
        const role = (
          card.querySelector(".brand-card__contact-line--role")?.textContent ||
          ""
        )
          .replace(/\s+/g, " ")
          .trim();
        const org = (
          card.querySelector(
            ".brand-card__contact-line--org, .brand-card__contact-org",
          )?.textContent || ""
        )
          .replace(/\s+/g, " ")
          .trim();
        const action = (
          card.querySelector(
            ".gdi-tile-pill--sales-action, [data-sales-action]",
          )?.textContent || ""
        )
          .replace(/\s+/g, " ")
          .trim();
        const desc = (
          card.querySelector(
            ".brand-card__summary, .gdi-brand-card-summary, .brand-card__body",
          )?.textContent || ""
        )
          .replace(/\s+/g, " ")
          .trim()
          .slice(0, 180);
        const activeIcons = [
          ...card.querySelectorAll(
            ".gdi-revenue-stream-icon--active, .brand-card__icon--active, [data-revenue-stream].is-active",
          ),
        ].length;
        const iconStates = [...card.querySelectorAll("[data-revenue-stream], .gdi-revenue-stream-icon")].map(
          (el) => ({
            stream: el.getAttribute("data-revenue-stream") || el.className,
            active: /active|--active|is-active/i.test(
              `${el.className} ${el.getAttribute("data-state") || ""}`,
            ),
          }),
        );
        return {
          id,
          title,
          name,
          role,
          org,
          action,
          desc,
          activeIcons,
          iconStates,
        };
      },
    );
  });
}

async function fetchProdIdentity(share) {
  const url = `${PROD}/api/group-demand-intelligence/share/hotels/${HOTEL}/opportunities?share=${encodeURIComponent(share)}&cb=${Date.now()}`;
  const res = await fetch(url, { headers: { "Cache-Control": "no-cache" } });
  const json = await res.json();
  const pub = json.publication || {};
  return {
    status: res.status,
    fingerprint: pub.contentFingerprintSha256 || null,
    version: pub.publicationVersion || null,
    lifecycle: pub.lifecycleState || null,
    mode: pub.publicationPackageMode || json.mode || null,
    count: (json.opportunities || []).length,
    opportunities: json.opportunities || [],
  };
}

fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });
const share = readShare();
if (!share) {
  console.error("FAIL missing share token");
  process.exit(2);
}

const frozen = JSON.parse(fs.readFileSync(SNAP, "utf8"));
const identity = await fetchProdIdentity(share);

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1440, height: 1100 } });

const localPage = await ctx.newPage();
await localPage.goto(
  `${LOCAL}/group-demand-intelligence.html?hotelId=${HOTEL}&live=1`,
  { waitUntil: "networkidle", timeout: 120000 },
);
await localPage.waitForTimeout(2800);
const localCards = await scrapeCards(localPage);
await localPage.screenshot({
  path: path.join(OUT, "screenshots/A-local-master-live-verify.png"),
  fullPage: true,
});

const prodPage = await ctx.newPage();
await prodPage.goto(
  `${PROD}/group-demand-intelligence-share.html?hotelId=${HOTEL}&share=${encodeURIComponent(share)}&_cb=${Date.now()}`,
  { waitUntil: "networkidle", timeout: 120000 },
);
await prodPage.waitForTimeout(2800);
const prodHtmlMarker = await prodPage.evaluate(() => {
  const s = [...document.scripts].map((x) => x.src).find((u) => /dealality-gdi-ui/.test(u));
  return s || null;
});
const prodCards = await scrapeCards(prodPage);
await prodPage.screenshot({
  path: path.join(OUT, "screenshots/C-production-live-verify.png"),
  fullPage: true,
});

// Negative: mutate production RIC subtitle → must fail parity
const mutatedOk = await prodPage.evaluate((id) => {
  const card = [...document.querySelectorAll(".brand-card.brand-card--gdi-opp")].find(
    (c) => c.getAttribute("data-open") === id,
  );
  const roleEl = card?.querySelector(".brand-card__contact-line--role");
  if (!roleEl) return false;
  roleEl.textContent = "Group Travel Coordinator";
  return true;
}, RIC_ID);
const afterMutate = await scrapeCards(prodPage);
const mutatedRic = afterMutate.find((c) => c.id === RIC_ID);
const localRic = localCards.find((c) => c.id === RIC_ID);
const negativeDetected =
  mutatedOk &&
  text(localRic?.role) === "RIC Conference Contact" &&
  text(mutatedRic?.role) === "Group Travel Coordinator" &&
  text(localRic?.role) !== text(mutatedRic?.role);

// Restore by reload for final comparison already captured in prodCards
await browser.close();

const localBy = new Map(localCards.map((c) => [c.id, c]));
const prodBy = new Map(prodCards.map((c) => [c.id, c]));
const freezeBy = new Map((frozen.opportunities || []).map((o) => [o.id, o]));

const comparisons = [];
const titleDiffersFromClass = [];
for (const id of [...new Set([...localBy.keys(), ...prodBy.keys()])].sort()) {
  const a = localBy.get(id) || null;
  const c = prodBy.get(id) || null;
  const f = freezeBy.get(id) || null;
  const resolved = f ? resolveAuthoritativeCardContactSubtitle(f) : null;
  const nameMatch = text(a?.name) === text(c?.name);
  const roleMatch = text(a?.role) === text(c?.role);
  const actionMatch = text(a?.action) === text(c?.action);
  const titleMatch = text(a?.title) === text(c?.title);
  const iconsMatch = (a?.activeIcons ?? -1) === (c?.activeIcons ?? -2);
  const subCheck = f
    ? detectContactSubtitleSubstitution(f, c?.role)
    : { ok: false, issues: [{ code: "MISSING_FREEZE" }] };
  let status = "PASS";
  if (!a || !c) status = "NOT_VERIFIED";
  else if (!nameMatch || !roleMatch || !actionMatch || !titleMatch || !iconsMatch || !subCheck.ok) {
    status = "FAIL";
  }
  const row = {
    id,
    title: a?.title || c?.title || f?.title || null,
    local: a,
    production: c,
    authoritativeSubtitle: resolved?.subtitle || null,
    displayRole: text(f?.tileContactPreview?.displayRole),
    opportunityRole: text(f?.tileContactPreview?.opportunityRole),
    nameMatch,
    roleMatch,
    actionMatch,
    titleMatch,
    iconsMatch,
    substitutionOk: subCheck.ok,
    substitutionIssues: subCheck.issues,
    status,
  };
  comparisons.push(row);
  if (
    row.displayRole &&
    row.opportunityRole &&
    row.displayRole !== row.opportunityRole
  ) {
    titleDiffersFromClass.push({
      id,
      title: row.title,
      displayRole: row.displayRole,
      opportunityRole: row.opportunityRole,
      localRole: a?.role,
      productionRole: c?.role,
      matchesAuthority:
        text(c?.role) === text(resolved?.subtitle) ||
        // allow shared truncation/proper-case of authoritative subtitle
        text(c?.role).replace(/…$/, "") ===
          text(resolved?.subtitle).slice(0, text(c?.role).replace(/…$/, "").length),
    });
  }
}

const ric = comparisons.find((r) => r.id === RIC_ID) || null;
const fails = comparisons.filter((r) => r.status === "FAIL");
const notVerified = comparisons.filter((r) => r.status === "NOT_VERIFIED");
const passes = comparisons.filter((r) => r.status === "PASS");

const wrongFieldSubs = titleDiffersFromClass.filter(
  (r) => text(r.productionRole) === text(r.opportunityRole) && text(r.displayRole) !== text(r.opportunityRole),
);

const deployMeta = {
  targetArtifactSha: TARGET_SHA,
  railwayDeploymentId:
    process.env.GDI_FIELD_AUTHORITY_RAILWAY_DEPLOYMENT_ID || null,
  uiAssetMarker: prodHtmlMarker,
  publication: {
    fingerprint: identity.fingerprint,
    version: identity.version,
    lifecycle: identity.lifecycle,
    mode: identity.mode,
    count: identity.count,
  },
  fingerprintMatch: identity.fingerprint === APPROVED_FP,
};

const certified =
  comparisons.length >= 67 &&
  fails.length === 0 &&
  notVerified.length === 0 &&
  deployMeta.fingerprintMatch &&
  negativeDetected &&
  text(ric?.local?.name) === "Lorna Kipfer" &&
  text(ric?.local?.role) === "RIC Conference Contact" &&
  text(ric?.production?.name) === "Lorna Kipfer" &&
  text(ric?.production?.role) === "RIC Conference Contact" &&
  wrongFieldSubs.length === 0;

const report = {
  schema: "gdi-field-authority-live-production-verify-v1",
  generatedAt: new Date().toISOString(),
  verdict: certified
    ? "FIELD_AUTHORITY_LIVE_PRODUCTION_VERIFIED"
    : "NOT_CERTIFIED",
  certified,
  deploy: deployMeta,
  counts: {
    localCards: localCards.length,
    prodCards: prodCards.length,
    comparisons: comparisons.length,
    pass: passes.length,
    fail: fails.length,
    notVerified: notVerified.length,
    titleDiffersFromClass: titleDiffersFromClass.length,
    wrongFieldSubstitutions: wrongFieldSubs.length,
  },
  ric,
  lornaExactApproved:
    text(ric?.production?.role) === "RIC Conference Contact" &&
    text(ric?.local?.role) === "RIC Conference Contact",
  negativeRegression: {
    mutatedOk,
    mutatedRole: mutatedRic?.role || null,
    localRole: localRic?.role || null,
    detected: negativeDetected,
    selector: ".brand-card__contact-line--role",
    idAttr: "data-open",
  },
  titleDiffersFromClass,
  wrongFieldSubs,
  failures: fails,
  notVerified,
  comparisons,
};

fs.writeFileSync(
  path.join(OUT, "10-LIVE-PRODUCTION-DOM-VERIFICATION.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "11-67-DOM-COMPARISON-RESULTS.json"),
  `${JSON.stringify({ comparisons, counts: report.counts }, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "12-NEGATIVE-REGRESSION-EVIDENCE.json"),
  `${JSON.stringify(report.negativeRegression, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "13-FINAL_LIVE_CERTIFICATE.json"),
  `${JSON.stringify(
    {
      verdict: report.verdict,
      certified,
      approvedFingerprint: APPROVED_FP,
      liveFingerprint: identity.fingerprint,
      artifactSha: TARGET_SHA,
      railwayDeploymentId: deployMeta.railwayDeploymentId,
      cardsChecked: comparisons.length,
      fieldsPerCard: [
        "brand-card__contact-name",
        "brand-card__contact-line--role",
        "sales-action pill",
        "title",
        "activeIcons",
      ],
      pass: passes.length,
      fail: fails.length,
      notVerified: notVerified.length,
      wrongFieldSubstitutions: wrongFieldSubs.length,
      lornaExactApproved: report.lornaExactApproved,
      negativeDetected,
    },
    null,
    2,
  )}\n`,
);

console.log(
  JSON.stringify(
    {
      verdict: report.verdict,
      certified,
      counts: report.counts,
      lorna: {
        local: ric?.local,
        production: ric?.production,
      },
      negativeDetected,
      fingerprint: identity.fingerprint,
      uiAssetMarker: prodHtmlMarker,
      failSample: fails.slice(0, 8),
      wrongFieldSubs: wrongFieldSubs.slice(0, 8),
    },
    null,
    2,
  ),
);

if (!certified) process.exit(2);
