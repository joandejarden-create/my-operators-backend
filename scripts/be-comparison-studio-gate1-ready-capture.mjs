#!/usr/bin/env node
/**
 * Comparison Studio — Gate 1 readiness capture on the CANONICAL URL.
 * Default: existing profile vs staged candidate (product oe-section pairs).
 * Also verifies Renderer Parity as selectable diagnostic (not default).
 * Does NOT set FOUNDER_APPROVED. No content regeneration / publish / Gate 2.
 */
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { createHash } from "crypto";
import { join } from "path";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/phase2h-a-renderer-parity/ten-tab-visual-v1");
const shotDir = join(outDir, "screenshots");
mkdirSync(shotDir, { recursive: true });

const PAYLOAD = "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json";

const TABS = [
  { id: "atelier-overview", label: "Overview" },
  { id: "atelier-value-owners", label: "Value" },
  { id: "atelier-ops", label: "Ops" },
  { id: "atelier-standards-owner", label: "Standards" },
  { id: "atelier-commercial", label: "Commercial" },
  { id: "atelier-economics", label: "Economics" },
  { id: "atelier-loyalty", label: "Loyalty" },
  { id: "atelier-footprint", label: "Footprint" },
  { id: "atelier-materials", label: "Materials" },
  { id: "atelier-insight", label: "Insight" },
];

const CANONICAL =
  "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1";
const GATE1_DIAG =
  "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1&brand=receQkxgjlezsc1xg";

const EXPECTED_STANDARDS = [
  "Standard Detail, Where Available",
  "Conversion & Sister-Flag Transitions",
  "Confirm With Brand",
  "Deal Context Inputs",
];

function sha256File(p) {
  return createHash("sha256").update(readFileSync(p)).digest("hex");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function activateTab(page, tabId) {
  return page.evaluate((id) => {
    const btn = document.querySelector(`[data-becs-tab="${id}"]`);
    if (!btn) return false;
    btn.click();
    return true;
  }, tabId);
}

async function measureStudio(page) {
  return page.evaluate(() => {
    const pairs = [
      ...document.querySelectorAll(
        "#becsPairedDoc .becs-pair-gate1, #becsPairedDoc .becs-pair[data-product-section]",
      ),
    ];
    const titles = pairs
      .map(
        (p) =>
          (p.getAttribute("data-gate1-section") ||
            p.getAttribute("data-product-section") ||
            "").trim(),
      )
      .filter(Boolean);
    const leftMounts = [
      ...document.querySelectorAll(
        '#becsPairedDoc .becs-col-left .becs-product-mount, [data-gate1-side="A-compare-left"] .becs-product-mount',
      ),
    ];
    const firstTitle =
      leftMounts[0] && leftMounts[0].querySelector("h2.oe-section-title, .oe-section-title");
    const cs = firstTitle ? getComputedStyle(firstTitle) : null;
    const badge = document.querySelector(".becs-badge");
    const status = document.querySelector("#becsStatus");
    const modeGate1 = document.querySelector('[data-becs-mode="gate1"]');
    return {
      sectionTitles: titles,
      sectionCount: titles.length,
      hasSection1: titles.some((t) => /^Section\s+\d+$/i.test(t)),
      titleStyle: cs
        ? {
            fontFamily: cs.fontFamily,
            fontSize: cs.fontSize,
            fontWeight: cs.fontWeight,
            color: cs.color,
          }
        : null,
      pairRowCount: pairs.length,
      productMountCount: leftMounts.length,
      badge: badge ? badge.textContent.trim() : null,
      status: status ? status.textContent.trim() : null,
      rendererParityActive: !!(modeGate1 && modeGate1.classList.contains("active")),
      gate1Hook: window.__BE_GATE1_COMPARE__ || null,
      compareHook: window.__BE_COMPARE_STUDIO__ || null,
      url: location.href,
    };
  });
}

async function verifyNoStale(page) {
  const findings = [];
  // Tab switch: capture Standards titles, switch to Economics, back — titles must restore
  await activateTab(page, "atelier-standards-owner");
  await sleep(600);
  const s1 = await measureStudio(page);
  await activateTab(page, "atelier-economics");
  await sleep(600);
  const e1 = await measureStudio(page);
  await activateTab(page, "atelier-standards-owner");
  await sleep(600);
  const s2 = await measureStudio(page);
  if (s1.sectionTitles.join("|") !== s2.sectionTitles.join("|")) {
    findings.push({
      issue: "stale_after_tab_switch",
      before: s1.sectionTitles,
      after: s2.sectionTitles,
    });
  }
  if (e1.sectionTitles.join("|") === s1.sectionTitles.join("|") && s1.sectionCount > 0) {
    findings.push({
      issue: "economics_showed_standards_titles",
      economics: e1.sectionTitles,
    });
  }
  // Mode toggle: enter Gate1 diagnostic then leave — must clear Gate1 chrome / restore compare
  const toggled = await page.evaluate(() => {
    const btn = document.querySelector('[data-becs-mode="gate1"]');
    if (!btn) return { ok: false, reason: "no_gate1_button" };
    btn.click();
    return { ok: true };
  });
  await sleep(1200);
  const g1 = await measureStudio(page);
  if (!toggled.ok) findings.push({ issue: "gate1_button_missing" });
  if (!g1.rendererParityActive && !/rendererParity=1/.test(g1.url || "")) {
    findings.push({ issue: "gate1_mode_did_not_activate", url: g1.url });
  }
  await page.evaluate(() => {
    const btn = document.querySelector('[data-becs-mode="gate1"]');
    if (btn) btn.click();
    else {
      const full = document.querySelector('[data-becs-mode="full"]');
      if (full) full.click();
    }
  });
  await sleep(1200);
  const back = await measureStudio(page);
  if (/rendererParity=1/.test(back.url || "")) {
    findings.push({ issue: "stale_renderer_parity_url_after_exit", url: back.url });
  }
  if (back.hasSection1) {
    findings.push({ issue: "section_1_after_mode_toggle", titles: back.sectionTitles });
  }
  return { findings, standardsRoundTrip: s2.sectionTitles, economicsTitles: e1.sectionTitles };
}

async function captureAllTabs(page, prefix) {
  const results = [];
  for (const tab of TABS) {
    const ok = await activateTab(page, tab.id);
    await sleep(800);
    await page.evaluate(() => {
      const sc = document.querySelector("#becsDocScroll");
      if (sc) sc.scrollTop = 0;
      window.scrollTo(0, 0);
    });
    const file = `${prefix}__${tab.id}.png`;
    await page.screenshot({ path: join(shotDir, file), fullPage: false });
    const metrics = await measureStudio(page);
    results.push({
      tabId: tab.id,
      tabName: tab.label,
      clicked: ok,
      screenshot: `screenshots/${file}`,
      metrics,
    });
  }
  return results;
}

async function main() {
  const puppeteer = require("puppeteer");
  const chromePath =
    process.env.PUPPETEER_EXECUTABLE_PATH ||
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
  const payloadHash = sha256File(join(root, PAYLOAD));
  const browser = await puppeteer.launch({
    headless: "new",
    executablePath: chromePath,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const report = {
    version: "BE_COMPARISON_STUDIO_GATE1_READY_V1",
    capturedAt: new Date().toISOString(),
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    contentMutation: false,
    gate2: "BLOCKED",
    engine: "puppeteer+system-chrome",
    canonicalUrl: CANONICAL,
    diagnosticRendererParityUrl: GATE1_DIAG,
    payloadHashSha256: payloadHash,
    checks: {},
    environments: {},
  };

  try {
    // --- Canonical default compare ---
    const page = await browser.newPage();
    await page.setViewport({ width: 1440, height: 1100 });
    await page.goto(CANONICAL, { waitUntil: "networkidle0", timeout: 120000 });
    await sleep(1500);

    const boot = await measureStudio(page);
    report.checks.defaultIsNotRendererParity = {
      pass: !boot.rendererParityActive && !/rendererParity=1/.test(boot.url || ""),
      url: boot.url,
      badge: boot.badge,
    };

    report.environments.canonical_compare = {
      url: CANONICAL,
      tabs: await captureAllTabs(page, "C-canonical"),
    };

    const standards = report.environments.canonical_compare.tabs.find(
      (t) => t.tabId === "atelier-standards-owner",
    );
    const economics = report.environments.canonical_compare.tabs.find(
      (t) => t.tabId === "atelier-economics",
    );

    report.checks.standardsFourSections = {
      pass:
        !!standards &&
        standards.metrics.sectionCount === 4 &&
        EXPECTED_STANDARDS.every((t) => standards.metrics.sectionTitles.includes(t)),
      expected: EXPECTED_STANDARDS,
      observed: standards?.metrics?.sectionTitles || [],
    };
    report.checks.economicsNoSection1 = {
      pass: !!economics && economics.metrics.hasSection1 === false,
      observed: economics?.metrics?.sectionTitles || [],
    };
    report.checks.tenTabsClicked = {
      pass: report.environments.canonical_compare.tabs.every((t) => t.clicked),
      count: report.environments.canonical_compare.tabs.filter((t) => t.clicked).length,
    };
    report.checks.productStylingPresent = {
      pass: report.environments.canonical_compare.tabs.every(
        (t) =>
          t.metrics.productMountCount > 0 &&
          t.metrics.titleStyle &&
          t.metrics.titleStyle.color === "rgb(255, 255, 255)",
      ),
    };

    const stale = await verifyNoStale(page);
    report.checks.noStaleContent = {
      pass: stale.findings.length === 0,
      findings: stale.findings,
      standardsRoundTrip: stale.standardsRoundTrip,
      economicsTitles: stale.economicsTitles,
    };

    // --- Gate 1 diagnostic on same studio ---
    await page.goto(GATE1_DIAG, { waitUntil: "networkidle0", timeout: 120000 });
    await sleep(1200);
    report.environments.gate1_diagnostic = {
      url: GATE1_DIAG,
      tabs: await captureAllTabs(page, "B-compare"),
    };
    const gStd = report.environments.gate1_diagnostic.tabs.find(
      (t) => t.tabId === "atelier-standards-owner",
    );
    const gEcon = report.environments.gate1_diagnostic.tabs.find(
      (t) => t.tabId === "atelier-economics",
    );
    report.checks.gate1StandardsFour = {
      pass: !!gStd && gStd.metrics.sectionCount === 4,
      observed: gStd?.metrics?.sectionTitles || [],
    };
    report.checks.gate1EconomicsNoSection1 = {
      pass: !!gEcon && gEcon.metrics.hasSection1 === false,
      observed: gEcon?.metrics?.sectionTitles || [],
    };

    await page.close();
  } finally {
    await browser.close();
  }

  const allPass = Object.values(report.checks).every((c) => c && c.pass === true);
  report.readyForGate1FounderReview = allPass;
  report.screenshotCount =
    (report.environments.canonical_compare?.tabs?.length || 0) +
    (report.environments.gate1_diagnostic?.tabs?.length || 0);

  writeFileSync(join(outDir, "01-ten-tab-capture-report.json"), JSON.stringify(report, null, 2));
  writeFileSync(
    join(outDir, "00-FOUNDER_GATE1_STATUS.md"),
    `# Gate 1 — Comparison Studio ready for founder visual acceptance

**FOUNDER_APPROVED = false**  
**Gate 2 = BLOCKED** (do not regenerate / publish)

## Canonical URL (permanent)

\`${CANONICAL}\`

- Default: **existing saved profile vs staged reconstructed candidate**
- Actual product subsection components · one page scroll
- Renderer Parity is a **selectable diagnostic** (\`rendererParity=1\`), not the default

## Checks before Gate 1 approval

| Check | Pass |
|---|---|
| Default is not Renderer Parity | ${report.checks.defaultIsNotRendererParity.pass} |
| Standards = 4 product sections | ${report.checks.standardsFourSections.pass} |
| Economics has no "Section 1" | ${report.checks.economicsNoSection1.pass} |
| All 10 tabs clickable | ${report.checks.tenTabsClicked.pass} |
| Product styling present | ${report.checks.productStylingPresent.pass} |
| No stale content on tab/mode change | ${report.checks.noStaleContent.pass} |
| Gate1 diagnostic Standards = 4 | ${report.checks.gate1StandardsFour.pass} |
| Gate1 diagnostic Economics no Section 1 | ${report.checks.gate1EconomicsNoSection1.pass} |

**readyForGate1FounderReview = ${allPass}**

## Screenshots

- Canonical compare: \`C-canonical__atelier-*.png\`
- Gate 1 diagnostic: \`B-compare__atelier-*.png\`
- Report: \`01-ten-tab-capture-report.json\`
- Payload hash: \`${payloadHash}\`

## Standards observed (canonical)

${(report.checks.standardsFourSections.observed || []).map((t) => `- ${t}`).join("\n") || "- (none)"}

## Economics observed (canonical)

${(report.checks.economicsNoSection1.observed || []).map((t) => `- ${t}`).join("\n") || "- (none)"}
`,
  );

  writeFileSync(
    join(outDir, "02-FOUNDER_GATE1_VISUAL_ACCEPTANCE.md"),
    `# Founder Gate 1 — Visual Acceptance

Open the **canonical** Comparison Studio:

**${CANONICAL}**

Confirm:

1. Left = existing Brand Explorer profile (read-only)
2. Right = staged reconstructed candidate (not published)
3. Standards shows **four** product sections
4. Economics has **no** untitled "Section 1" pair
5. All ten tabs render with product styling
6. Switching tabs / brands / modes does not leave stale content
7. "Renderer Parity (Gate 1)" is available as a diagnostic toggle only

Approve Gate 1 only after visual review. Do **not** start Gate 2 until approved.
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: allPass,
        readyForGate1FounderReview: allPass,
        canonicalUrl: CANONICAL,
        checks: Object.fromEntries(
          Object.entries(report.checks).map(([k, v]) => [k, v.pass]),
        ),
        screenshots: report.screenshotCount,
        outDir,
        founderApproved: false,
        gate2: "BLOCKED",
      },
      null,
      2,
    ),
  );

  if (!allPass) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
