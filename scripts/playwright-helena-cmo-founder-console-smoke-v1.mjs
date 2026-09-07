#!/usr/bin/env node
/**
 * Helena CMO Founder Console — Playwright browser smoke (clean context, no extensions).
 *
 * Boots a local HTTP harness that serves public assets + Helena APIs with an
 * authorized founder/admin /api/me stub (no Memberstack, no production).
 *
 *   npm run playwright:helena-cmo-founder-console-smoke-v1
 */
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import {
  buildFounderBriefViewModel,
  getHelenaAttentionCount,
} from "../lib/helena-cmo/founder-console/brief-view-model.js";
import {
  buildFounderConsoleV2ViewModel,
  getHelenaConsoleAttentionCount,
} from "../lib/helena-cmo/founder-console/baseline-view-model.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(ROOT, "reports/helena-cmo-founder-console-v1");
fs.mkdirSync(OUT, { recursive: true });

const FOUNDER_ME = {
  ok: true,
  success: true,
  dealality: {
    isAdmin: true,
    flags: { isAdmin: true },
    founderNavOverridesAvailable: true,
    email: "joan@dealality.com",
    canonicalWorkspaceOptions: { founderNavOverridesAvailable: true },
  },
};

function contentType(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  if (filePath.endsWith(".json")) return "application/json; charset=utf-8";
  return "application/octet-stream";
}

function sendJson(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}

function startHarness() {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    const p = url.pathname;

    if (p === "/api/me") {
      return sendJson(res, 200, FOUNDER_ME);
    }
    if (p === "/api/admin/helena-cmo/brief") {
      const brief = buildFounderBriefViewModel();
      if (!brief.ok) return sendJson(res, 404, brief);
      return sendJson(res, 200, { ok: true, success: true, brief });
    }
    if (p === "/api/admin/helena-cmo/console") {
      const consoleVm = buildFounderConsoleV2ViewModel();
      if (!consoleVm.ok) return sendJson(res, 404, consoleVm);
      return sendJson(res, 200, { ok: true, success: true, console: consoleVm });
    }
    if (p === "/api/admin/helena-cmo/attention-count") {
      return sendJson(res, 200, { ok: true, success: true, ...getHelenaConsoleAttentionCount() });
    }
    if (p.startsWith("/api/admin/helena-cmo/")) {
      return sendJson(res, 200, {
        ok: true,
        success: true,
        executeEnabled: false,
        marketingOsSync: false,
        brief: buildFounderBriefViewModel(),
        console: buildFounderConsoleV2ViewModel(),
      });
    }

    let rel = p === "/" ? "/app/admin/helena-cmo.html" : p;
    rel = rel.split("?")[0];
    if (rel.includes("dealality-memberstack-auth.js")) {
      res.writeHead(200, { "Content-Type": "application/javascript; charset=utf-8" });
      res.end(`
window.DealalityMemberstackAuth = {
  authFetch: function (url, opts) {
    var headers = Object.assign({}, (opts && opts.headers) || {}, {
      Authorization: "Bearer helena-smoke-founder",
      Accept: "application/json"
    });
    return fetch(url, Object.assign({}, opts || {}, { headers: headers }));
  }
};
`);
      return;
    }
    const filePath = path.join(ROOT, "public", rel.replace(/^\//, ""));
    if (!filePath.startsWith(path.join(ROOT, "public")) || !fs.existsSync(filePath)) {
      res.writeHead(404);
      res.end("not found: " + rel);
      return;
    }
    res.writeHead(200, { "Content-Type": contentType(filePath) });
    res.end(fs.readFileSync(filePath));
  });

  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      resolve({ server, base: `http://127.0.0.1:${server.address().port}` });
    });
  });
}

async function main() {
  const { server, base } = await startHarness();
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const consoleErrors = [];
  const pageErrors = [];
  const helenaApiStatuses = [];

  page.on("console", (msg) => {
    if (msg.type() === "error") {
      consoleErrors.push(msg.text());
    }
  });
  page.on("pageerror", (err) => {
    pageErrors.push(String(err && err.message ? err.message : err));
  });
  page.on("response", (res) => {
    const u = res.url();
    if (u.includes("/api/admin/helena-cmo/")) {
      helenaApiStatuses.push({ url: u, status: res.status() });
    }
  });

  await page.route('**/dealality-memberstack-auth.js*', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/javascript; charset=utf-8',
      body: `
window.DealalityMemberstackAuth = {
  authFetch: function (url, opts) {
    var headers = Object.assign({}, (opts && opts.headers) || {}, {
      Authorization: "Bearer helena-smoke-founder",
      Accept: "application/json"
    });
    return fetch(url, Object.assign({}, opts || {}, { headers: headers }));
  }
};
`,
    });
  });

  const results = {
    generatedAt: new Date().toISOString(),
    checks: {},
    consoleErrors,
    pageErrors,
    helenaApiStatuses,
  };

  function mark(name, ok, detail) {
    results.checks[name] = { ok: !!ok, detail: detail || null };
    console.log(`${ok ? "PASS" : "FAIL"} ${name}${detail ? " — " + detail : ""}`);
  }

  try {
    await page.goto(`${base}/app/admin/helena-cmo.html?embed=1`, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });

    await page.waitForFunction(() => {
      const content = document.getElementById("supportGateContent");
      const denied = document.getElementById("supportGateDenied");
      const loading = document.getElementById("supportGateLoading");
      const contentReady = content && !content.hidden;
      const deniedReady = denied && !denied.hidden;
      const loadingDone = !loading || loading.hidden;
      return (contentReady || deniedReady) && loadingDone;
    }, { timeout: 30000 });

    const gateState = await page.evaluate(() => {
      const content = document.getElementById("supportGateContent");
      const denied = document.getElementById("supportGateDenied");
      const err = document.getElementById("helenaError");
      return {
        hasAuth: !!(window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.authFetch),
        hasGate: !!(window.SupportAdminGate && window.SupportAdminGate.requireHelenaCmoAdmin),
        contentHidden: !content || content.hidden,
        deniedHidden: !denied || denied.denied === true || denied.hidden,
        deniedText: denied ? denied.innerText : "",
        errorText: err && !err.hidden ? err.innerText : "",
        briefHtmlLength: content ? content.innerHTML.length : 0,
      };
    });

    if (gateState.contentHidden) {
      throw new Error("Helena gate did not reveal content: " + JSON.stringify(gateState));
    }

    await page.waitForSelector("#tab-assessment .helena-prose", { timeout: 30000 });

    const probe = await page.evaluate(() => {
      const assessment = document.getElementById("tab-assessment");
      const text = assessment ? assessment.innerText : "";
      const body = document.body.innerText;
      return {
        hasWhereWeStand: /Where we stand/i.test(text),
        hasCoverage: /Source coverage/i.test(text),
        hasTop3: /Top 3 things that matter/i.test(text),
        hasStrategySection: /Recommended strategy/i.test(text),
        hasNoApprovalAsk: /not.*requested|Evidence validation first|understanding/i.test(text),
        hasPendingTactics: /PENDING STRATEGY REVIEW/i.test(text),
        hasUnknown: /UNKNOWN|DATA_GAP|What is unknown/i.test(text),
        hasExecuteOff: /EXECUTE OFF/i.test(body),
        hasPublishButton: !!document.querySelector("[data-action='PUBLISH'], [data-execute='1']"),
        strategyButtons: document.querySelectorAll("#tab-assessment [data-open-decision='STRATEGY-V1']").length,
        tacticalOpenOnAssessment: document.querySelectorAll("#tab-assessment [data-open-decision^='JD-']").length,
      };
    });

    mark("page_loads", true);
    mark("js_parses_no_syntax_error", !pageErrors.some((e) => /SyntaxError/i.test(e)), pageErrors.join(" | "));
    mark("executive_assessment_renders", !!probe.hasWhereWeStand && !!probe.hasCoverage);
    mark("source_coverage_visible", probe.hasCoverage);
    mark("top3_section", probe.hasTop3);
    mark("strategy_section_no_approval_push", probe.hasStrategySection && probe.hasNoApprovalAsk);
    mark("tactics_not_leading_on_assessment", probe.tacticalOpenOnAssessment === 0, `tactical=${probe.tacticalOpenOnAssessment}`);
    mark("pending_strategy_review_visible", probe.hasPendingTactics);
    mark("unknown_or_data_gap_visible", probe.hasUnknown);
    mark("execute_off_visible", probe.hasExecuteOff);
    mark("no_execute_publish_controls", !probe.hasPublishButton);

    const forbidden = helenaApiStatuses.filter((r) => r.status === 401 || r.status === 403);
    mark(
      "helena_api_authorized",
      helenaApiStatuses.length > 0 && forbidden.length === 0,
      JSON.stringify(helenaApiStatuses.slice(0, 8)),
    );

    const attention = helenaApiStatuses.find((r) => r.url.includes("attention-count"));
    mark(
      "attention_count_ok",
      !attention || (attention.status >= 200 && attention.status < 300),
      attention ? `status=${attention.status}` : "not_requested_on_direct_page",
    );

    // Direct attention-count check for authorized founder context
    const attn = await page.evaluate(async () => {
      const res = await window.DealalityMemberstackAuth.authFetch("/api/admin/helena-cmo/attention-count");
      const body = await res.json();
      return { status: res.status, ok: !!(body && body.ok), count: body && body.count };
    });
    mark("attention_count_authorized_fetch", attn.status === 200 && attn.ok, JSON.stringify(attn));

    const appErrors = pageErrors.filter(
      (e) =>
        /SyntaxError|ReferenceError/i.test(e) ||
        (!/Grammarly|message channel closed|Permissions policy/i.test(e) && e.length),
    );
    const consoleAppErrors = consoleErrors.filter(
      (e) =>
        /SyntaxError|ReferenceError|Uncaught/i.test(e) &&
        !/Grammarly|message channel closed|Permissions policy/i.test(e),
    );
    mark("no_uncaught_app_errors", appErrors.length === 0 && consoleAppErrors.length === 0, [
      ...appErrors,
      ...consoleAppErrors,
    ].join(" | "));
  } finally {
    await browser.close();
    server.close();
  }

  const failed = Object.entries(results.checks).filter(([, v]) => !v.ok);
  results.pass = failed.length === 0;
  results.failed = failed.map(([k]) => k);
  fs.writeFileSync(path.join(OUT, "playwright-smoke-results.json"), JSON.stringify(results, null, 2));

  if (!results.pass) {
    console.error(`FAIL ${failed.length} checks`);
    process.exit(1);
  }
  console.log(`PASS ${Object.keys(results.checks).length}/${Object.keys(results.checks).length} playwright smoke`);
}

main().catch((err) => {
  console.error("FAIL playwright smoke:", err && err.stack ? err.stack : err);
  process.exit(1);
});
