/**
 * Capture Brand AI Visibility REAL app UI PDF.
 * Strategy: no request interception (avoids hung deferred scripts).
 * Instead, stub DealalityMemberstackAuth.authFetch to return measured fixtures.
 */
import fs from "fs";
import path from "path";
import puppeteer from "puppeteer-core";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  getBrandOverviewPayload,
  getBrandQuestionsPayload,
  getBrandSourcesPayload,
  getBrandCompetitorsPayload,
  getBrandTrendPayload,
  getBrandPortfolioPayload,
} from "../lib/ai-visibility/brand-read-service.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import {
  loadShowcaseCompaniesConfig,
  getShowcaseCompany,
} from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import {
  peerSetBrandNamesById,
  PEER_SET_ID_V2,
} from "../lib/ai-visibility/peer-sets.js";
import { normalizeAiVisibilityViewerContext } from "../lib/ai-visibility/viewer-context.js";

const CHROME =
  process.env.CHROME_PATH ||
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const BASE = process.env.AIV_CAPTURE_BASE || "http://localhost:8080";
const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const GEO = "CALA";
const LANG = "en";
const PROVIDER = "openai";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function buildAuth() {
  const showcase = loadShowcaseCompaniesConfig();
  const marriott = getShowcaseCompany("marriott", showcase);
  const names = { ...peerSetBrandNamesById(PEER_SET_ID_V2) };
  for (const b of marriott.brands || []) names[b.brandId] = b.brandName;
  try {
    const uni = JSON.parse(
      fs.readFileSync("fixtures/ai-visibility/golden-set-v2-entity-universe.json", "utf8")
    );
    for (const e of uni.entities || []) {
      if (e.id && e.name) names[e.id] = e.name;
    }
  } catch {}
  const store = createBrandAiVisibilityReadStore({});
  const dealalityUser = {
    id: "ui-pdf-capture",
    role: "admin",
    isAdmin: true,
    isBrand: true,
    canAccessBrandWorkspace: true,
    activeWorkspace: "Brand",
  };
  const viewerContext = normalizeAiVisibilityViewerContext(dealalityUser);
  viewerContext.roles = ["admin"];
  return {
    marriott,
    auth: {
      dealalityUser,
      viewerContext,
      entitlementGraph: buildFixtureEntitlementGraph({
        entitledBrandIds: marriott.brandIds,
        peerBrandIds: marriott.brandIds,
        source: "demo_showcase_portfolio",
      }),
      brandNamesById: names,
      store,
    },
  };
}

async function buildFixtures() {
  const { auth, marriott } = buildAuth();
  const ok = (p) => {
    p.ok = true;
    p.success = true;
    return p;
  };
  /** @type {Record<string, any>} */
  const out = {};

  out["/api/ai-visibility/brand/executive-summary"] = ok(
    await getBrandExecutiveSummaryPayload({
      ...auth,
      geography: GEO,
      language: LANG,
      provider: PROVIDER,
    })
  );
  out["/api/ai-visibility/brand/executive-summary"].demoBrandPortfolioKey = "marriott";

  out["/api/ai-visibility/brand/portfolio"] = ok(
    await getBrandPortfolioPayload({
      ...auth,
      geography: GEO,
      language: LANG,
      provider: PROVIDER,
    })
  );

  for (const brand of marriott.brands || []) {
    const id = brand.brandId;
    const base = `/api/ai-visibility/brand/${id}`;
    out[`${base}/overview`] = ok(
      await getBrandOverviewPayload({
        ...auth,
        brandId: id,
        geography: GEO,
        language: LANG,
        provider: PROVIDER,
      })
    );
    out[`${base}/trend`] = ok(
      await getBrandTrendPayload({
        ...auth,
        brandId: id,
        geography: GEO,
        language: LANG,
        provider: PROVIDER,
      })
    );
    out[`${base}/competitors`] = ok(
      await getBrandCompetitorsPayload({
        ...auth,
        brandId: id,
        geography: GEO,
        language: LANG,
        provider: PROVIDER,
      })
    );
    out[`${base}/sources`] = ok(
      await getBrandSourcesPayload({
        ...auth,
        brandId: id,
        geography: GEO,
        language: LANG,
        provider: PROVIDER,
      })
    );
    out[`${base}/questions`] = ok(
      await getBrandQuestionsPayload({
        ...auth,
        brandId: id,
        geography: GEO,
        language: LANG,
        provider: PROVIDER,
        limit: 50,
        offset: 0,
      })
    );
    if (id === AUTOGRAPH) {
      out[`${base}/overview|all`] = ok(
        await getBrandOverviewPayload({
          ...auth,
          brandId: id,
          geography: GEO,
          language: LANG,
          provider: "all",
        })
      );
      out[`${base}/questions|all`] = ok(
        await getBrandQuestionsPayload({
          ...auth,
          brandId: id,
          geography: GEO,
          language: LANG,
          provider: "all",
          limit: 50,
          offset: 0,
        })
      );
    }
  }

  out["/api/me"] = {
    ok: true,
    success: true,
    dealalityUser: auth.dealalityUser,
    demoBrandPortfolioKey: "marriott",
    activeWorkspace: "Brand",
    canAccessBrandWorkspace: true,
    isAdmin: true,
  };
  out["/api/auth/me"] = out["/api/me"];
  return out;
}

async function installFixtureAuth(page, fixtures) {
  await page.evaluate((fixtureMap) => {
    function resolve(url) {
      const u = new URL(url, window.location.origin);
      const path = u.pathname;
      const provider = u.searchParams.get("provider") || "";
      if (provider === "all" && fixtureMap[path + "|all"]) return fixtureMap[path + "|all"];
      if (fixtureMap[path]) return fixtureMap[path];
      return {
        ok: true,
        success: true,
        brands: [],
        questions: [],
        sources: [],
        peers: [],
        points: [],
        message: "capture_fixture_miss",
        path,
      };
    }

    function install() {
      const auth = (window.DealalityMemberstackAuth =
        window.DealalityMemberstackAuth || {});
      auth.getMemberstackJwt = async () => "local-ui-capture";
      auth.getMemberstackJwtWhenReady = async () => "local-ui-capture";
      auth.getAuthHeaders = async () => ({
        headers: {
          Authorization: "Bearer local-ui-capture",
          Accept: "application/json",
          "X-Dealality-Active-Workspace": "Brand",
          "X-Dealality-Demo-Brand-Portfolio": "marriott",
        },
      });
      auth.notifyLoginRequired = function () {};
      auth.authFetch = async function (url) {
        const payload = resolve(url);
        return {
          ok: true,
          status: 200,
          async json() {
            return payload;
          },
          async text() {
            return JSON.stringify(payload);
          },
        };
      };
      window.__AIV_CAPTURE_READY = true;
    }

    install();
    // Keep winning over deferred script assignment.
    const t0 = Date.now();
    const timer = setInterval(() => {
      install();
      if (Date.now() - t0 > 5000) clearInterval(timer);
    }, 50);
  }, fixtures);
}

async function printTall(page, outPath) {
  const height = await page.evaluate(() =>
    Math.max(document.body.scrollHeight, document.documentElement.scrollHeight, 1800)
  );
  await page.pdf({
    path: outPath,
    printBackground: true,
    width: "1440px",
    height: `${height}px`,
    margin: { top: "10px", bottom: "10px", left: "10px", right: "10px" },
  });
}

async function main() {
  const res = await fetch(`${BASE}/ai-visibility-brand.html`);
  if (!res.ok) throw new Error(`Server unavailable: ${BASE}`);

  console.log("[capture] Building fixtures…");
  const fixtures = await buildFixtures();
  console.log("[capture] Fixture keys:", Object.keys(fixtures).length);

  const outDir = path.resolve("data/ai-visibility/exports");
  fs.mkdirSync(outDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const pdfExec = path.join(outDir, `brand-ai-visibility-app-ui-executive-${stamp}.pdf`);
  const pdfDetail = path.join(
    outDir,
    `brand-ai-visibility-app-ui-detail-autograph-${stamp}.pdf`
  );
  const pdfCombined = path.join(outDir, `brand-ai-visibility-app-ui-${stamp}.pdf`);
  const stable = path.join(outDir, "brand-ai-visibility-app-ui-latest.pdf");

  console.log("[capture] Launching Chrome…");
  const browser = await puppeteer.launch({
    executablePath: CHROME,
    headless: "new",
    defaultViewport: { width: 1440, height: 1100 },
    args: ["--no-sandbox", "--disable-gpu"],
  });

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(60000);

    await page.evaluateOnNewDocument(() => {
      try {
        localStorage.setItem("dealality_active_workspace", "Brand");
        localStorage.setItem("dealality_demo_brand_portfolio", "marriott");
        sessionStorage.setItem("aiv_brand_tab", "executive");
        // Pre-set language so loadExecutive does not reconcile mid-request and
        // discard the paint via shouldApplyLoadResult filter fingerprint mismatch.
        sessionStorage.setItem("aiv_brand_language", "en");
        sessionStorage.setItem("aiv_brand_geography", "CALA");
        sessionStorage.setItem("aiv_brand_provider", "openai");
      } catch (_) {}
    });

    console.log("[capture] goto…");
    await page.goto(`${BASE}/ai-visibility-brand.html`, {
      waitUntil: "domcontentloaded",
      timeout: 45000,
    });
    console.log("[capture] page loaded, installing fixture auth…");
    await installFixtureAuth(page, fixtures);

    // Wait until brand script exists and auth stub is active
    await page.waitForFunction(() => window.__AIV_CAPTURE_READY === true, {
      timeout: 15000,
    });
    await sleep(400);
    await installFixtureAuth(page, fixtures);

    console.log("[capture] clicking Run Report…");
    await page.evaluate(() => {
      const geo = document.getElementById("aivGeography");
      const prov = document.getElementById("aivProvider");
      const langGroup = document.getElementById("aivLanguageFilterGroup");
      const lang = document.getElementById("aivLanguage");
      if (geo) geo.value = "CALA";
      if (prov) {
        if (![...prov.options].some((o) => o.value === "openai")) {
          const opt = document.createElement("option");
          opt.value = "openai";
          opt.textContent = "OpenAI";
          prov.appendChild(opt);
        }
        prov.value = "openai";
      }
      if (langGroup) langGroup.hidden = false;
      if (lang) {
        if (![...lang.options].some((o) => o.value === "en")) {
          const opt = document.createElement("option");
          opt.value = "en";
          opt.textContent = "English";
          lang.appendChild(opt);
        }
        lang.value = "en";
      }
      try {
        sessionStorage.setItem("aiv_brand_language", "en");
        sessionStorage.setItem("aiv_brand_geography", "CALA");
        sessionStorage.setItem("aiv_brand_provider", "openai");
      } catch (_) {}
      document.getElementById("aivApply")?.click();
    });

    await page.waitForFunction(() => {
      const pos = document.getElementById("aivExecPosition");
      const text = (pos && pos.innerText) || "";
      return text.length > 25 && !/please log in/i.test(document.body.innerText);
    }, { timeout: 45000 });

    await page.evaluate(() => {
      const loading = document.getElementById("aivLoading");
      if (loading) loading.hidden = true;
    });

    console.log("[capture] print executive…");
    await printTall(page, pdfExec);

    console.log("[capture] open detail Autograph…");
    await installFixtureAuth(page, fixtures);
    await page.evaluate((brandId) => {
      sessionStorage.setItem("aiv_brand_tab", "detail");
      document.getElementById("aivTabDetail")?.click();
      const brand = document.getElementById("aivBrand");
      if (brand) {
        brand.value = brandId;
        brand.dispatchEvent(new Event("change", { bubbles: true }));
      }
    }, AUTOGRAPH);
    await sleep(300);
    await installFixtureAuth(page, fixtures);
    await page.evaluate(() => document.getElementById("aivApply")?.click());

    await page.waitForFunction(() => {
      const kpi = document.getElementById("aivKpiRow");
      const detail = document.getElementById("aivDetailView");
      return detail && !detail.hidden && ((kpi && kpi.innerText) || "").length > 20;
    }, { timeout: 45000 });
    await sleep(1200);
    await page.evaluate(() => {
      const loading = document.getElementById("aivLoading");
      if (loading) loading.hidden = true;
    });

    console.log("[capture] print detail…");
    await printTall(page, pdfDetail);

    let merged = false;
    try {
      const { PDFDocument } = await import("pdf-lib");
      const doc = await PDFDocument.create();
      for (const f of [pdfExec, pdfDetail]) {
        const src = await PDFDocument.load(fs.readFileSync(f));
        const pages = await doc.copyPages(src, src.getPageIndices());
        pages.forEach((p) => doc.addPage(p));
      }
      const bytes = await doc.save();
      fs.writeFileSync(pdfCombined, bytes);
      fs.writeFileSync(stable, bytes);
      merged = true;
    } catch (e) {
      console.warn("[capture] merge failed", e.message);
      fs.copyFileSync(pdfExec, pdfCombined);
      fs.copyFileSync(pdfExec, stable);
    }

    console.log(
      JSON.stringify(
        {
          ok: true,
          mode: "REAL_APP_UI_CAPTURE",
          merged,
          executivePdf: pdfExec,
          detailPdf: pdfDetail,
          combinedPdf: pdfCombined,
          stableLatest: stable,
          bytes: fs.statSync(pdfCombined).size,
        },
        null,
        2
      )
    );
  } finally {
    await browser.close();
  }
}

main().catch((err) => {
  console.error("[capture] FAILED", err);
  process.exit(1);
});
