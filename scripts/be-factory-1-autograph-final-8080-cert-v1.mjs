#!/usr/bin/env node
/**
 * Autograph final 8080 product certification — Proposed Update popup.
 * Compares browser approval hash to frozen approvalContentHash.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const OUT = path.join(ROOT, "reports/brand-explorer/factory-1-0/autograph-final-cert");
const SHOT = path.join(OUT, "screenshots");
fs.mkdirSync(SHOT, { recursive: true });

const frozen = JSON.parse(
  fs.readFileSync(path.join(OUT, "AUTOGRAPH_FROZEN_APPROVAL_PAYLOAD.json"), "utf8")
);
const EXPECTED_HASH = frozen.approvalContentHash;
const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const FAKE = "dealality-local-founder-preview-shell-token";
const ME = {
  ok: true,
  success: true,
  user: { firstName: "Dealality", lastName: "Founder", email: "dealalitydemo@dealality.com" },
  dealality: {
    role: "owner",
    isAdmin: true,
    isOwner: true,
    isDemo: true,
    localDealalityAdminAccess: true,
    founderNavOverridesAvailable: true,
    flags: { isAdmin: true, isDemo: true },
  },
  data: {
    dealality: {
      role: "owner",
      isAdmin: true,
      isOwner: true,
      isDemo: true,
      localDealalityAdminAccess: true,
      founderNavOverridesAvailable: true,
      flags: { isAdmin: true },
    },
  },
};

const TABS = [
  "atelier-overview",
  "atelier-value-owners",
  "atelier-ops",
  "atelier-standards-owner",
  "atelier-commercial",
  "atelier-economics",
  "atelier-loyalty",
  "atelier-footprint",
  "atelier-materials",
  "atelier-insight",
];

const browser = await chromium.launch({
  headless: true,
  executablePath: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
});
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
await page.route("**/api/me**", async (r) =>
  r.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(ME) })
);
await page.addInitScript((jwt) => {
  window.__dealalityMemberstackJwt = jwt;
}, FAKE);

async function auth() {
  await page.evaluate((jwt) => {
    window.__dealalityMemberstackJwt = jwt;
    const g = document.getElementById("dealalityAppShellAuthGate");
    if (g) {
      g.hidden = true;
      g.style.display = "none";
    }
    window.dispatchEvent(
      new CustomEvent("dealality-shell-auth-ready", {
        detail: { ok: true, jwt, authorized: true },
      })
    );
  }, FAKE);
}

await page.goto(
  `${BASE}/app#/brand-explorer-combined?id=${AUTOGRAPH}&beInternalPreview=1&beProfileMode=proposed`,
  { waitUntil: "domcontentloaded", timeout: 90000 }
);
await auth();
await page.waitForTimeout(2000);
await auth();

let frame = null;
for (let i = 0; i < 80; i++) {
  for (const f of page.frames()) {
    if (/brand-explorer-combined\.html/i.test(f.url() || "")) {
      frame = f;
      break;
    }
  }
  if (frame) break;
  await page.waitForTimeout(300);
}
if (!frame) throw new Error("no frame");
await page.waitForTimeout(3000);
await frame.evaluate(async (id) => {
  if (window.openBrandEducationPopupForBrand) {
    await window.openBrandEducationPopupForBrand(id, "Autograph Collection");
  }
}, AUTOGRAPH);
await page.waitForTimeout(6000);

const tabEvidence = [];
for (const tab of TABS) {
  await frame.evaluate((id) => {
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    const btn = root && root.querySelector(`[data-atelier-tab="${id}"]`);
    if (btn) btn.click();
  }, tab);
  await page.waitForTimeout(500);
  const snap = await frame.evaluate((id) => {
    const root = document.getElementById("beCombinedPopupAtelierRoot");
    const panel = root && root.querySelector(".be-atelier-tab-panel.active");
    const text = (panel && panel.innerText) || "";
    const headings = panel
      ? [...panel.querySelectorAll("h2,h3")].map((h) => (h.textContent || "").trim()).filter(Boolean)
      : [];
    const cards = panel ? panel.querySelectorAll(".proof-point-card, .diff-card, .oe-cluster").length : 0;
    const imgs = panel ? [...panel.querySelectorAll("img")].map((img) => img.getAttribute("src") || "") : [];
    const badAnd = headings.filter((h) => /\bAnd\b/.test(h));
    const apacBad = headings.some((h) => /^Apac$/i.test(h) && h !== "APAC");
    const proofPointStub = [...(panel ? panel.querySelectorAll(".proof-point-card__headline") : [])].some(
      (h) => /^Proof Point$/i.test((h.textContent || "").trim())
    );
    const benefitCount =
      id === "atelier-loyalty"
        ? panel
          ? panel.querySelectorAll(".proof-points-grid .proof-point-card").length
          : 0
        : null;
    return {
      tab: id,
      textLen: text.length,
      headingCount: headings.length,
      headings: headings.slice(0, 12),
      cardishCount: cards,
      imageCount: imgs.filter(Boolean).length,
      badAndHeadings: badAnd,
      apacBad,
      proofPointStub,
      benefitCount,
      hasInternalFiller: /RESEARCH_REQUIRED|TODO_RESEARCH|slotKey|Factory 1\.0 Autograph/i.test(text),
      hasNaNumeric: /N\/A — Numeric value unavailable/i.test(text),
    };
  }, tab);
  const shotName = `tab-${tab.replace("atelier-", "")}.png`;
  await page.screenshot({ path: path.join(SHOT, shotName), fullPage: false });
  tabEvidence.push({ ...snap, screenshot: shotName });
}

const meta = await frame.evaluate(() => {
  const brand = window.__BE_ATELIER_ACTIVE_BRAND__;
  return {
    mode: brand && brand.beProfileMode,
    cumulative: !!(brand && brand.beCumulativeProposal),
    hash: brand && brand.beCandidateOverlayMeta && brand.beCandidateOverlayMeta.contentHash,
    name: brand && (brand.name || brand.brandName),
  };
});

const pass = {
  proposedMode: meta.mode === "proposed",
  cumulative: meta.cumulative === true,
  hashMatchesApproval: meta.hash === EXPECTED_HASH,
  tenTabs: tabEvidence.length === 10,
  allTabsHaveContent: tabEvidence.every((t) => t.textLen > 80 && t.headingCount > 0),
  noBadAnd: tabEvidence.every((t) => t.badAndHeadings.length === 0),
  noApacBad: tabEvidence.every((t) => !t.apacBad),
  noProofPointStub: tabEvidence.every((t) => !t.proofPointStub),
  sixLoyaltyBenefits: tabEvidence.some((t) => t.tab === "atelier-loyalty" && t.benefitCount === 6),
  noInternalFiller: tabEvidence.every((t) => !t.hasInternalFiller),
  noNaNumeric: tabEvidence.every((t) => !t.hasNaNumeric),
};

const report = {
  at: new Date().toISOString(),
  brandId: AUTOGRAPH,
  expectedApprovalContentHash: EXPECTED_HASH,
  browserHash: meta.hash,
  meta,
  tabEvidence,
  pass,
  ok: Object.values(pass).every(Boolean),
  writebackStatus: "AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL",
};
fs.writeFileSync(path.join(OUT, "08-8080-FINAL-PRODUCT-CERT.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify({ ok: report.ok, pass, browserHash: meta.hash, expected: EXPECTED_HASH }, null, 2));
await browser.close();
process.exit(report.ok ? 0 : 2);
