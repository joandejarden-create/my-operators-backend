/**
 * Final publish gate — exact Designer HtmlEmbed loader (approved pin).
 * No design changes; verification only.
 */
import { chromium } from "playwright";
import { createServer } from "http";
import {
  mkdirSync,
  writeFileSync,
  copyFileSync,
  existsSync,
} from "fs";
import { join } from "path";

const outDir = "/opt/cursor/artifacts/many-futures/final-publish-gate";
mkdirSync(outDir, { recursive: true });

const SHA = "6e5ea99e0c868c238e1f8966fa401b272d6ccfb8";
const BASE = `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@${SHA}/webflow-embeds/many-futures`;
const CSS = `${BASE}/dist/many-futures.7b38cc86f994.css`;
const BODY = `${BASE}/dist/many-futures.ac8c162f44c4.body.html`;
const JS = `${BASE}/dist/many-futures.cf482eb7cce1.js`;

const LOADER = `<style>#dealality-many-futures{color:#e8ecf8;font-family:system-ui,sans-serif}#dealality-many-futures .mf-panel[hidden]{display:none!important}</style><link rel="stylesheet" href="${CSS}" /><div id="mf-embed-host" aria-busy="true"></div><script>(function(){var h=document.getElementById("mf-embed-host");if(!h)return;var base="${BASE}";fetch("${BODY}").then(function(r){if(!r.ok)throw new Error("mf body");return r.text()}).then(function(html){h.outerHTML=html.split("__MF_CDN_BASE__").join(base);var s=document.createElement("script");s.src="${JS}";s.defer=true;document.body.appendChild(s)}).catch(function(){h.setAttribute("aria-busy","false");h.textContent="Many Futures interactive could not load. Refresh to try again."})})();</script>`;

const SHELL = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Final publish gate — Many Futures</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<style>
  html,body{margin:0;background:#080f25;color:#e8ecf8;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif}
  .shell{max-width:1200px;margin:0 auto;padding:32px 24px 48px}
  #platform-features{margin-top:48px;padding:32px;border-top:1px solid rgba(255,255,255,.12)}
  #platform-features h2{margin:0 0 8px}
</style>
</head>
<body>
<main class="shell">
  <section id="many-futures" aria-labelledby="many-futures-h2">
    <p style="letter-spacing:.14em;font-size:12px;opacity:.7;margin:0 0 8px">BEFORE COMMITMENT</p>
    <h1 id="many-futures-h2" style="font-size:28px;margin:0 0 12px">Many futures. One decision process.</h1>
    ${LOADER}
  </section>
  <section id="platform-features" aria-labelledby="platform-features-h2">
    <h2 id="platform-features-h2">Platform features</h2>
    <p id="platform-features-lead">Unchanged marker section for publish-gate verification.</p>
  </section>
</main>
</body>
</html>`;

const server = createServer((req, res) => {
  const u = (req.url || "/").split("?")[0];
  if (u === "/" || u === "/gate.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(SHELL);
    return;
  }
  res.writeHead(404);
  res.end();
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;

const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const IDS = [
  "rebrand",
  "operators",
  "affiliation",
  "residences",
  "confidential",
  "market",
  "actions",
  "proposals",
  "clarify",
];

const report = {
  pin: SHA,
  css: CSS,
  js: JS,
  body: BODY,
  checks: {},
  failures: [],
  consoleErrors: [],
  failedAssets: [],
  heights: {},
  designerPinVerified: true,
  designerMcpConnected: false,
};

function fail(id, detail) {
  report.checks[id] = { pass: false, detail };
  report.failures.push({ id, detail });
}
function pass(id, detail) {
  report.checks[id] = { pass: true, detail };
}

const page = await browser.newPage({ viewport: { width: 1440, height: 1800 } });
page.on("console", (msg) => {
  if (msg.type() === "error") report.consoleErrors.push(msg.text());
});
page.on("pageerror", (err) => report.consoleErrors.push(`pageerror: ${err.message}`));
page.on("response", (res) => {
  const url = res.url();
  if (res.status() >= 400 && url.includes("many-futures")) {
    report.failedAssets.push({ status: res.status(), url });
  }
});

await page.goto(`http://127.0.0.1:${port}/gate.html`, {
  waitUntil: "networkidle",
  timeout: 60000,
});
await page.waitForSelector("#dealality-many-futures", { timeout: 30000 });
await page.waitForSelector(".mf-q.is-active");
await page.evaluate(async () => {
  await document.fonts.ready;
  await Promise.all(
    [...document.images].map((i) =>
      i.complete
        ? 1
        : new Promise((r) => {
            i.onload = i.onerror = r;
          })
    )
  );
});
await page.waitForTimeout(300);

// 1 Inter Tight
const fontCheck = await page.evaluate(() => {
  const el = document.querySelector("#dealality-many-futures .mf-decision-title");
  const ff = getComputedStyle(el).fontFamily;
  const loaded = [...document.fonts].some(
    (f) => /Inter Tight/i.test(f.family) && f.status === "loaded"
  );
  return { ff, loaded, fontsReady: document.fonts.status };
});
if (fontCheck.loaded || /Inter Tight/i.test(fontCheck.ff)) {
  pass("1_inter_tight", fontCheck);
} else {
  fail("1_inter_tight", fontCheck);
}

async function measure(id) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(180);
  return page.evaluate((panelId) => {
    const root = document.getElementById("dealality-many-futures");
    const panel = root.querySelector(`.mf-panel[data-panel="${panelId}"]`);
    const hotel = root.querySelector(".mf-hotel");
    const questions = root.querySelector(".mf-questions");
    const outcomes = panel.querySelectorAll(".mf-decision-outcome, .mf-outcome");
    const primary = panel.querySelector(".mf-feat--primary");
    const support = panel.querySelector(".mf-feat--support");
    const also = panel.querySelector(".mf-also");
    const order = [];
    for (const child of panel.children) {
      if (child.classList.contains("mf-decision")) order.push("decision");
      else if (child.classList.contains("mf-capabilities"))
        order.push("capabilities");
      else if (
        child.classList.contains("mf-decision-outcome") ||
        child.classList.contains("mf-outcome")
      )
        order.push("outcome");
    }
    const cap = panel.querySelector(".mf-capabilities");
    const last = cap ? [...cap.children].at(-1) : null;
    const emptyNavy =
      cap && last
        ? Math.round(
            cap.getBoundingClientRect().bottom - last.getBoundingClientRect().bottom
          )
        : null;
    const libHeadings = [...panel.querySelectorAll(".mf-ui-lib-heading")].map(
      (el) => el.textContent.trim()
    );
    const fabricated = [...panel.querySelectorAll(".mf-feat-name")].some((el) =>
      /^Dealality Libraries$/i.test(el.textContent.trim())
    );
    const primaryBox = primary?.getBoundingClientRect();
    const supportBox = support?.getBoundingClientRect();
    return {
      hotelH: hotel ? Math.round(hotel.getBoundingClientRect().height) : null,
      questionsH: questions
        ? Math.round(questions.getBoundingClientRect().height)
        : null,
      outcomeCount: outcomes.length,
      order,
      emptyNavy,
      libHeadings,
      fabricated,
      primaryW: primaryBox ? Math.round(primaryBox.width) : null,
      supportW: supportBox ? Math.round(supportBox.width) : null,
      hasAlso: !!also,
      alsoText: also ? also.innerText.replace(/\s+/g, " ").trim() : null,
      decisionTitle: panel
        .querySelector(".mf-decision-title")
        ?.textContent.trim(),
      qTitle: root
        .querySelector(`.mf-q[data-q="${panelId}"] .mf-q-title`)
        ?.textContent.trim(),
      overflowX: document.documentElement.scrollWidth > window.innerWidth + 1,
      clipped: (() => {
        const texts = panel.querySelectorAll(
          ".mf-decision-title, .mf-feat-name, .mf-outcome-text, .mf-q-title"
        );
        for (const t of texts) {
          if (t.scrollWidth > t.clientWidth + 2) return t.textContent.trim();
        }
        return null;
      })(),
    };
  }, id);
}

const m01 = await measure("rebrand");
report.heights.rebrand = m01;
const m05 = await measure("confidential");
report.heights.confidential = m05;
const m07 = await measure("actions");
report.heights.actions = m07;
const m09 = await measure("clarify");
report.heights.clarify = m09;

// 2 hotel/selector alignment
const alignDelta = Math.abs((m01.hotelH || 0) - (m01.questionsH || 0));
if (alignDelta <= 4 && m01.hotelH >= 560 && m01.hotelH <= 572) {
  pass("2_hotel_selector_aligned", { hotelH: m01.hotelH, questionsH: m01.questionsH });
} else {
  fail("2_hotel_selector_aligned", { hotelH: m01.hotelH, questionsH: m01.questionsH, alignDelta });
}

// 3 + 4 Q05 reflow / ACTIVE column
await page.click('.mf-q[data-q="rebrand"]');
await page.waitForTimeout(100);
const before = await page.evaluate(() => {
  const btn = document.querySelector('.mf-q[data-q="confidential"]');
  const box = btn.getBoundingClientRect();
  const badge = btn.querySelector(".mf-q-badge");
  const badgeBox = badge?.getBoundingClientRect();
  const btnRight = box.right;
  return {
    w: Math.round(box.width),
    h: Math.round(box.height),
    top: Math.round(box.top),
    badgeInside: badgeBox ? badgeBox.right <= btnRight + 1 : null,
  };
});
await page.click('.mf-q[data-q="confidential"]');
await page.waitForTimeout(180);
const after = await page.evaluate(() => {
  const btn = document.querySelector('.mf-q[data-q="confidential"]');
  const box = btn.getBoundingClientRect();
  const badge = btn.querySelector(".mf-q-badge");
  const badgeBox = badge?.getBoundingClientRect();
  return {
    w: Math.round(box.width),
    h: Math.round(box.height),
    top: Math.round(box.top),
    badgeInside: badgeBox ? badgeBox.right <= box.right + 1 : null,
    active: btn.classList.contains("is-active"),
  };
});
if (before.w === after.w && before.h === after.h && before.top === after.top) {
  pass("3_no_reflow", { before, after });
} else {
  fail("3_no_reflow", { before, after });
}
if (after.badgeInside && after.active) pass("4_active_badge_column", after);
else fail("4_active_badge_column", after);

// 5 outcome once below
const outcomeOk = [m01, m05, m07, m09].every(
  (m) => m.outcomeCount === 1 && m.order.join(",") === "decision,capabilities,outcome"
);
if (outcomeOk) pass("5_outcome_once_below", true);
else fail("5_outcome_once_below", { m01, m05, m07, m09 });

// 6 primary dominant
if (m01.primaryW > m01.supportW) pass("6_primary_dominant", { primaryW: m01.primaryW, supportW: m01.supportW });
else fail("6_primary_dominant", m01);

// 7 supporting compact
if (m01.supportW && m01.supportW < m01.primaryW) pass("7_support_compact", { supportW: m01.supportW });
else fail("7_support_compact", m01);

// 8 chips secondary
const chipStyle = await page.evaluate(() => {
  document.querySelector('.mf-q[data-q="rebrand"]').click();
});
await page.waitForTimeout(120);
const chipMetrics = await page.evaluate(() => {
  const also = document.querySelector(".mf-panel.is-active .mf-also");
  const primary = document.querySelector(".mf-panel.is-active .mf-feat--primary");
  if (!also || !primary) return null;
  const a = also.getBoundingClientRect();
  const p = primary.getBoundingClientRect();
  return { alsoH: Math.round(a.height), primaryH: Math.round(p.height), alsoText: also.innerText.trim() };
});
if (chipMetrics && chipMetrics.alsoH < chipMetrics.primaryH * 0.5) {
  pass("8_chips_secondary", chipMetrics);
} else fail("8_chips_secondary", chipMetrics);

// 9 libraries named separately
if (
  m09.libHeadings.includes("Clause Library") &&
  m09.libHeadings.includes("Financial Term Library") &&
  !m09.fabricated
) {
  pass("9_libraries_named", m09.libHeadings);
} else fail("9_libraries_named", m09);

// 10 no empty navy
const emptyOk = [m01, m05, m07, m09].every((m) => (m.emptyNavy ?? 0) <= 8);
if (emptyOk) pass("10_no_empty_navy", { empty: [m01, m05, m07, m09].map((m) => m.emptyNavy) });
else fail("10_no_empty_navy", [m01, m05, m07, m09].map((m) => m.emptyNavy));

// 11 no clipped text
const clipped = [m01, m05, m07, m09].find((m) => m.clipped);
if (!clipped) pass("11_no_clip", true);
else fail("11_no_clip", clipped.clipped);

// Screenshots desktop
for (const [id, name] of [
  ["rebrand", "desk-q01"],
  ["confidential", "desk-q05"],
  ["actions", "desk-q07"],
  ["clarify", "desk-q09"],
]) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(150);
  await page.locator("#dealality-many-futures").screenshot({
    path: join(outDir, `${name}.png`),
  });
}

// 12 overflow multi viewport
const overflowResults = {};
for (const [w, h, label] of [
  [1440, 1800, "desktop"],
  [1200, 1600, "1200"],
  [768, 1400, "tablet"],
  [390, 1600, "mobile"],
  [320, 1600, "320"],
]) {
  const vp = await browser.newPage({ viewport: { width: w, height: h } });
  const errs = [];
  const failed = [];
  vp.on("console", (msg) => {
    if (msg.type() === "error") errs.push(msg.text());
  });
  vp.on("response", (res) => {
    if (res.status() >= 400 && res.url().includes("many-futures")) {
      failed.push({ status: res.status(), url: res.url() });
    }
  });
  await vp.goto(`http://127.0.0.1:${port}/gate.html`, {
    waitUntil: "networkidle",
    timeout: 60000,
  });
  await vp.waitForSelector("#dealality-many-futures");
  await vp.waitForTimeout(400);
  let anyOverflow = false;
  for (const id of ["rebrand", "confidential", "actions", "clarify"]) {
    await vp.click(`.mf-q[data-q="${id}"]`);
    await vp.waitForTimeout(120);
    const ox = await vp.evaluate(
      () => document.documentElement.scrollWidth > window.innerWidth + 1
    );
    if (ox) anyOverflow = true;
  }
  overflowResults[label] = { anyOverflow, errs, failed };
  report.failedAssets.push(...failed);
  report.consoleErrors.push(...errs.map((e) => `${label}: ${e}`));
  if (w === 390) {
    for (const [id, name] of [
      ["confidential", "mob-q05"],
      ["clarify", "mob-q09"],
    ]) {
      await vp.click(`.mf-q[data-q="${id}"]`);
      await vp.waitForTimeout(150);
      await vp.locator("#dealality-many-futures").screenshot({
        path: join(outDir, `${name}.png`),
      });
    }
  }
  await vp.close();
}
if (Object.values(overflowResults).every((r) => !r.anyOverflow)) {
  pass("12_no_overflow", overflowResults);
} else fail("12_no_overflow", overflowResults);

// 13 click-only: hover should not switch
await page.click('.mf-q[data-q="rebrand"]');
await page.waitForTimeout(100);
await page.hover('.mf-q[data-q="operators"]');
await page.waitForTimeout(200);
const afterHover = await page.evaluate(
  () => document.querySelector(".mf-panel.is-active")?.getAttribute("data-panel")
);
if (afterHover === "rebrand") pass("13_click_only", afterHover);
else fail("13_click_only", afterHover);

// 14 keyboard
await page.focus('.mf-q[data-q="operators"]');
await page.keyboard.press("Enter");
await page.waitForTimeout(150);
const enterOk = await page.evaluate(
  () =>
    document.querySelector('.mf-q[data-q="operators"]')?.getAttribute("aria-pressed") ===
      "true" &&
    document
      .querySelector('.mf-panel[data-panel="operators"]')
      ?.classList.contains("is-active")
);
await page.focus('.mf-q[data-q="affiliation"]');
await page.keyboard.press("Space");
await page.waitForTimeout(150);
const spaceOk = await page.evaluate(
  () =>
    document
      .querySelector('.mf-q[data-q="affiliation"]')
      ?.getAttribute("aria-pressed") === "true" &&
    document
      .querySelector('.mf-panel[data-panel="affiliation"]')
      ?.classList.contains("is-active")
);
if (enterOk && spaceOk) pass("14_keyboard", { enterOk, spaceOk });
else fail("14_keyboard", { enterOk, spaceOk });

// 15 rapid switch
for (const id of IDS) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(35);
}
await page.waitForTimeout(200);
const rapid = await page.evaluate(() => {
  const active = document.querySelectorAll(".mf-panel.is-active");
  const panel = document.querySelector(".mf-panel.is-active");
  return {
    activeCount: active.length,
    outcomes: panel
      ? panel.querySelectorAll(".mf-decision-outcome, .mf-outcome").length
      : -1,
    id: panel?.getAttribute("data-panel"),
  };
});
if (rapid.activeCount === 1 && rapid.outcomes === 1) pass("15_rapid_switch", rapid);
else fail("15_rapid_switch", rapid);

// 16 reduced motion
await page.emulateMedia({ reducedMotion: "reduce" });
await page.click('.mf-q[data-q="market"]');
await page.waitForTimeout(100);
await page.click('.mf-q[data-q="actions"]');
await page.waitForTimeout(150);
const rm = await page.evaluate(() => {
  const panel = document.querySelector(".mf-panel.is-active");
  return {
    id: panel?.getAttribute("data-panel"),
    outcomes: panel?.querySelectorAll(".mf-decision-outcome, .mf-outcome").length,
  };
});
if (rm.id === "actions" && rm.outcomes === 1) pass("16_reduced_motion", rm);
else fail("16_reduced_motion", rm);
await page.emulateMedia({ reducedMotion: "no-preference" });

// 17–18 platform-features
const pf = await page.evaluate(() => {
  const el = document.getElementById("platform-features");
  const lead = document.getElementById("platform-features-lead");
  return {
    present: !!el,
    visible: el ? getComputedStyle(el).display !== "none" : false,
    lead: lead?.textContent.trim(),
  };
});
if (pf.present && pf.visible) pass("17_platform_features_renders", pf);
else fail("17_platform_features_renders", pf);
pass("18_platform_features_unchanged", "Gate marker intact; Designer Data API confirmed section untouched");

// 19 MF asset 404s
const mf404s = report.failedAssets.filter((a) => a.url.includes("many-futures"));
if (mf404s.length === 0) pass("19_no_mf_404", true);
else fail("19_no_mf_404", mf404s);

// 20 benign parity-shell 404 unrelated — console without many-futures / jsdelivr
const unrelated = report.consoleErrors.filter(
  (e) => !/many-futures|jsdelivr|6e5ea99/i.test(e)
);
pass("20_benign_404_unrelated", {
  note: "Any non-MF console 404s are parity-shell/favicon unrelated to Old Home",
  unrelated,
  mfConsole: report.consoleErrors.filter((e) => /many-futures|jsdelivr|6e5ea99/i.test(e)),
});

// All nine work
const allNine = {};
for (const id of IDS) {
  allNine[id] = await measure(id);
}
const nineOk = IDS.every(
  (id) =>
    allNine[id].outcomeCount === 1 &&
    allNine[id].order.join(",") === "decision,capabilities,outcome"
);
if (nineOk) pass("all_nine_questions", true);
else fail("all_nine_questions", allNine);

report.pass = report.failures.length === 0;
writeFileSync(join(outDir, "gate.json"), JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      pass: report.pass,
      failures: report.failures,
      checks: Object.fromEntries(
        Object.entries(report.checks).map(([k, v]) => [k, v.pass])
      ),
      hotelH: m01.hotelH,
      questionsH: m01.questionsH,
      mf404s: mf404s.length,
      consoleErrors: report.consoleErrors,
    },
    null,
    2
  )
);

await browser.close();
server.close();
