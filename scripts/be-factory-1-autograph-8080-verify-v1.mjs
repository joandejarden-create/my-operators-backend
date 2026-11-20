#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const OUT = path.join(ROOT, "reports/brand-explorer/factory-1-0");
const SHOT = path.join(OUT, "screenshots");
fs.mkdirSync(SHOT, { recursive: true });

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
await page.waitForTimeout(5000);

const proof = await frame.evaluate(() => {
  const brand = window.__BE_ATELIER_ACTIVE_BRAND__;
  const root = document.getElementById("beCombinedPopupAtelierRoot");
  const tabs = root
    ? [...root.querySelectorAll("[data-atelier-tab]")].map((el) => el.getAttribute("data-atelier-tab"))
    : [];
  function openTab(id) {
    const btn = root && root.querySelector(`[data-atelier-tab="${id}"]`);
    if (btn) btn.click();
  }
  openTab("atelier-commercial");
  const commercialText = (root && root.innerText) || "";
  openTab("atelier-loyalty");
  const loyaltyText = (root && (root.querySelector(".be-atelier-tab-panel.active") || root).innerText) || "";
  openTab("atelier-economics");
  const econPanel = root && root.querySelector(".be-atelier-tab-panel.active");
  const feeLis = econPanel
    ? [...econPanel.querySelectorAll(".proof-point-card--fee-bucket li")].map((li) =>
        (li.textContent || "").trim()
      )
    : [];
  openTab("atelier-loyalty");
  const loyaltyPanel = root && root.querySelector(".be-atelier-tab-panel.active");
  const benefitHeadlines = loyaltyPanel
    ? [...loyaltyPanel.querySelectorAll(".proof-point-card__headline")].map((h) =>
        (h.textContent || "").trim()
      )
    : [];
  openTab("atelier-overview");
  const overviewPanel = root && root.querySelector(".be-atelier-tab-panel.active");
  const why = overviewPanel
    ? [...overviewPanel.querySelectorAll(".oe-cluster")].find((c) =>
        /Why Value Is Strongest/i.test(c.textContent || "")
      )
    : null;
  const whyLeads = why
    ? [...why.querySelectorAll("li")].map((li) => ({
        t: (li.textContent || "").trim().slice(0, 100),
        strong: !!li.querySelector("strong"),
      }))
    : [];
  openTab("atelier-footprint");
  const footprintPanel = root && root.querySelector(".be-atelier-tab-panel.active");
  const footprintHeadings = footprintPanel
    ? [...footprintPanel.querySelectorAll("h3, .proof-point-card__headline, .oe-cluster h3")].map(
        (h) => (h.textContent || "").trim()
      )
    : [];
  return {
    mode: brand && brand.beProfileMode,
    cumulative: !!(brand && brand.beCumulativeProposal),
    hash: brand && brand.beCandidateOverlayMeta && brand.beCandidateOverlayMeta.contentHash,
    name: brand && (brand.name || brand.brandName),
    tabCount: tabs.length,
    hasCommercialBonvoy: /Bonvoy|Commercial Engine/i.test(commercialText),
    hasLoyaltyBonvoy: /Marriott Bonvoy|Bonvoy/i.test(loyaltyText),
    feeJoinHas100000: feeLis.some((t) => /\$100,000/.test(t)),
    feeOrphans: feeLis.filter((t) => /^(000|800)$/.test(t)),
    whyLeadStrongCount: whyLeads.filter((x) => x.strong).length,
    whyLeads,
    feeLisSample: feeLis.slice(0, 8),
    benefitHeadlines,
    benefitCount: benefitHeadlines.length,
    hasProofPointStub: benefitHeadlines.some((t) => /^Proof Point$/i.test(t)),
    footprintHeadings: footprintHeadings.filter((t) => /apac|emea|cala|mea|americas|europe/i.test(t)),
    apacBadHeading: footprintHeadings.some((t) => /^Apac$/i.test(t) && t !== "APAC"),
    apacOkHeading: footprintHeadings.some((t) => t === "APAC"),
  };
});

await page.screenshot({ path: path.join(SHOT, "01-autograph-proposed-overview.png"), fullPage: false });
await frame.evaluate(() => {
  const root = document.getElementById("beCombinedPopupAtelierRoot");
  const btn = root && root.querySelector('[data-atelier-tab="atelier-economics"]');
  if (btn) btn.click();
});
await page.waitForTimeout(600);
await page.screenshot({ path: path.join(SHOT, "02-autograph-proposed-economics.png"), fullPage: false });

const pass = {
  proposedMode: proof.mode === "proposed",
  tabCount10: proof.tabCount >= 10,
  commercial: proof.hasCommercialBonvoy,
  loyalty: proof.hasLoyaltyBonvoy,
  feeThousandSep: proof.feeJoinHas100000,
  noFeeOrphans: proof.feeOrphans.length === 0,
  whyBold: proof.whyLeadStrongCount >= 3,
  sixBenefits: proof.benefitCount === 6,
  noProofPointStub: !proof.hasProofPointStub,
  apacNotTitleCased: !proof.apacBadHeading,
};
const report = {
  at: new Date().toISOString(),
  brandId: AUTOGRAPH,
  proof,
  pass,
  ok: Object.values(pass).every(Boolean),
  writebackStatus: "AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL",
};
fs.writeFileSync(path.join(OUT, "04-autograph-8080-proposed-verify.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
await browser.close();
process.exit(report.ok ? 0 : 2);
