#!/usr/bin/env node
/**
 * Local browser QA — BPP evidence drawers (no deploy).
 * Writes slim fixtures under public/qa-tmp and injects via fetch (avoids CDP arg size limits).
 */
import { chromium } from "playwright";
import { readFileSync, existsSync, mkdirSync, writeFileSync, rmSync } from "fs";
import { join } from "path";
import {
  BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE,
  BPP_CUSTOMER_PUBLISHED_PACK,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";

const ROOT = process.cwd();
const BASE = process.env.ADP_LOCAL_BASE || "http://localhost:8080";
const PACK_PATH = existsSync(join(ROOT, BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE))
  ? join(ROOT, BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE)
  : join(ROOT, BPP_CUSTOMER_PUBLISHED_PACK);
const QA_DIR = join(ROOT, "public", "qa-tmp");

const TARGETS = [
  "adp_casas_del_xvi",
  "adp_bethesda_marriott",
  "adp_jw_marriott_santo_domingo",
  "adp_st_regis_cap_cana",
  "adp_hotel_caribe_faranda_grand",
  "adp_hotel_phillips_kansas_city",
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
];

function slimEvidenceItem(ev) {
  const body = String(ev.aiResponse || ev.exactResponse || ev.rawResponse || "");
  return {
    ...ev,
    aiResponse: body.slice(0, 800),
    exactResponse: body.slice(0, 800),
    rawResponse: body.slice(0, 800),
  };
}

function slimPayload(bpp) {
  const ev = bpp.evidence || {};
  return {
    ...bpp,
    evidence: {
      ...ev,
      positive: (ev.positive || []).slice(0, 5).map(slimEvidenceItem),
      missing: (ev.missing || []).slice(0, 5).map(slimEvidenceItem),
      displacement: (ev.displacement || []).slice(0, 5).map(slimEvidenceItem),
      counts: {
        positive: Math.min(5, (ev.positive || []).length),
        missing: Math.min(5, (ev.missing || []).length),
        displacement: Math.min(5, (ev.displacement || []).length),
      },
    },
  };
}

async function main() {
  const pack = JSON.parse(readFileSync(PACK_PATH, "utf8"));
  mkdirSync(QA_DIR, { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.goto(`${BASE}/owner-ai-demand.html?embed=1&v=adp-v92-bpp-evidence-universal-20260911`, {
    waitUntil: "domcontentloaded",
  });
  await page.waitForFunction(() => typeof window.__adpTestRenderBrandPortfolio === "function", null, {
    timeout: 20000,
  });

  // Wait for the page's initial auto-load to settle so it cannot overwrite fixtures mid-test
  // (first listed property — often Casas — races the first inject otherwise).
  await page.evaluate(
    () =>
      new Promise((resolve) => {
        let settled = false;
        const done = () => {
          if (settled) return;
          settled = true;
          resolve();
        };
        document.addEventListener("adp:report-loaded", done, { once: true });
        setTimeout(done, 8000);
      })
  );

  // Ensure Brand & Portfolio section is visible for clicks
  await page.evaluate(() => {
    const section = document.getElementById("adpBrandPortfolioSection");
    if (section) {
      section.hidden = false;
      section.style.display = "block";
    }
  });

  const results = [];
  for (const propertyId of TARGETS) {
    const bpp = pack.payloads?.[propertyId];
    if (!bpp || bpp.status !== "READY") {
      results.push({ propertyId, pass: false, reason: "not_ready" });
      continue;
    }
    const slim = slimPayload(bpp);
    const fixtureName = `bpp-${propertyId}.json`;
    writeFileSync(join(QA_DIR, fixtureName), JSON.stringify({ brandPortfolioPosition: slim }));

    const injectFixture = async () => {
      await page.evaluate(async (name) => {
        const section = document.getElementById("adpBrandPortfolioSection");
        if (section) {
          section.hidden = false;
          section.style.display = "block";
        }
        const res = await fetch("/qa-tmp/" + name);
        const json = await res.json();
        window.__adpTestRenderBrandPortfolio(json);
      }, fixtureName);
      await page.waitForTimeout(200);
    };

    await injectFixture();
    // Confirm slim counts landed (auto-load must not have re-won the race)
    const expectedPos = (slim.evidence?.positive || []).length;
    const labelOk = await page.evaluate((n) => {
      const btn = document.querySelector('[data-bpp-evidence="positive"]');
      const text = (btn?.textContent || "").trim();
      if (n === 0) return /View Positive Evidence/i.test(text) && !/\(\d+\)/.test(text);
      return text.includes(`(${n})`);
    }, expectedPos);
    if (!labelOk) {
      await injectFixture();
    }

    const diag = await page.evaluate(() => {
      const host = document.getElementById("adpBrandPortfolioEvidenceHost");
      const section = document.getElementById("adpBrandPortfolioSection");
      return {
        sectionHidden: section?.hidden,
        hostHtml: (host?.innerHTML || "").slice(0, 200),
        btnCount: document.querySelectorAll("[data-bpp-evidence]").length,
        readyAttr: section?.getAttribute("data-bpp-ready"),
      };
    });

    const controls = await page.evaluate(() => {
      const btns = [...document.querySelectorAll("[data-bpp-evidence]")];
      return btns.map((b) => ({
        kind: b.getAttribute("data-bpp-evidence"),
        label: b.textContent.trim(),
      }));
    });

    const kinds = ["positive", "missing", "displacement"];
    const drawer = {};
    for (const kind of kinds) {
      const handle = await page.$(`[data-bpp-evidence="${kind}"]`);
      if (!handle) {
        drawer[kind] = { pass: false, reason: "control_missing", diag };
        continue;
      }
      await handle.evaluate((el) => el.click());
      await page.waitForTimeout(120);
      const info = await page.evaluate((k) => {
        const title = document.getElementById("adpEvidenceTitle")?.textContent || "";
        const responses = [...document.querySelectorAll(".aiv-evidence-response")].map((el) =>
          (el.textContent || "").trim()
        );
        const empty = document.querySelector("#adpEvidenceBody .aiv-empty")?.textContent || "";
        const expected =
          k === "positive"
            ? "Brand & Portfolio Evidence · Positive"
            : k === "missing"
              ? "Brand & Portfolio Evidence · Missing"
              : "Brand & Portfolio Evidence · Displacement";
        return {
          title,
          expected,
          responseCount: responses.length,
          nonemptyResponses: responses.filter((t) => t.length > 0).length,
          empty,
          matchTitle: title === expected,
          sampleLen: responses[0] ? responses[0].length : 0,
        };
      }, kind);

      const expectedCount =
        kind === "positive"
          ? (slim.evidence?.positive || []).length
          : kind === "missing"
            ? (slim.evidence?.missing || []).length
            : (slim.evidence?.displacement || []).length;

      const pass =
        info.matchTitle &&
        ((expectedCount === 0 && /No .+ evidence this period/i.test(info.empty || "")) ||
          (expectedCount > 0 &&
            info.responseCount === expectedCount &&
            info.nonemptyResponses === expectedCount &&
            info.sampleLen > 0));

      drawer[kind] = { pass, expectedCount, ...info };

      await page.evaluate(() => {
        const d = document.getElementById("adpEvidenceDrawer");
        if (d?.open && typeof d.close === "function") d.close();
        else if (d) {
          d.hidden = true;
          d.classList.remove("is-open", "open");
        }
      });
    }

    const pass = controls.length === 3 && Object.values(drawer).every((d) => d.pass === true);
    results.push({ propertyId, pass, controls, drawer, diag });
    console.log(
      pass ? "PASS" : "FAIL",
      propertyId,
      JSON.stringify({ nControls: controls.length, diag, drawer })
    );
  }

  await browser.close();
  try {
    rmSync(QA_DIR, { recursive: true, force: true });
  } catch {
    /* ignore */
  }

  const failed = results.filter((r) => !r.pass);
  if (failed.length) {
    console.error("BPP evidence browser QA FAIL", failed.map((f) => f.propertyId));
    process.exit(1);
  }
  console.log("ADP_BPP_EVIDENCE_BROWSER_QA PASS", results.length);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
