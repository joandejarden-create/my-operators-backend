#!/usr/bin/env node
/**
 * Surfe Usage Admin Authentication Fix V1
 *
 * Guards the shell ↔ embed ↔ API authorization contract so an authorized
 * platform administrator cannot see Surfe Usage in Admin Resources while the
 * embedded page incorrectly reports Access restricted.
 *
 * No paid Surfe calls. No Airtable schema changes.
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const htmlPath = join(ROOT, "public/app/admin/surfe-usage.html");
const clientPath = join(ROOT, "public/js/admin-surfe-usage.js");
const gatePath = join(ROOT, "public/js/support-admin-gate.js");
const authPath = join(ROOT, "public/js/dealality-memberstack-auth.js");
const appJsPath = join(ROOT, "public/app.js");
const serverPath = join(ROOT, "server.js");
const missingAuthPath = join(ROOT, "public/js/memberstack-auth.js");

const html = readFileSync(htmlPath, "utf8");
const client = readFileSync(clientPath, "utf8");
const gateSrc = readFileSync(gatePath, "utf8");
const authSrc = readFileSync(authPath, "utf8");
const appJs = readFileSync(appJsPath, "utf8");
const server = readFileSync(serverPath, "utf8");

// --- 1) Embed must load the real auth helper (not the 404 stub name) ---
{
  assert.match(html, /dealality-memberstack-auth\.js/);
  assert.doesNotMatch(html, /\/js\/memberstack-auth\.js/);
  assert.equal(existsSync(missingAuthPath), false, "memberstack-auth.js must not exist");
  assert.equal(existsSync(authPath), true);
  assert.match(html, /support-admin-gate\.js/);
  assert.match(html, /admin-surfe-usage\.js/);
}

// --- 2) Client must fail closed without DealalityMemberstackAuth ---
{
  assert.match(client, /DealalityMemberstackAuth\.authFetch unavailable/);
  assert.doesNotMatch(
    client,
    /return fetch\(url,\s*Object\.assign\(\{\s*credentials:\s*["']same-origin["']/,
  );
  assert.match(client, /gate\.requireAdmin/);
}

// --- 3) Gate admin contract matches backend requireAdminAccess signals ---
{
  assert.match(gateSrc, /localDealalityAdminAccess/);
  assert.match(gateSrc, /flags\.isAdmin/);
  assert.match(gateSrc, /Authenticated but unauthorized/);
  assert.match(gateSrc, /gateFailureMessage|Authentication unavailable/);
}

// --- 4) Localhost same-origin authFetch fallback (shell DEV_AUTH_BYPASS parity) ---
{
  assert.match(authSrc, /isLocalDevHost/);
  assert.match(authSrc, /isSameOriginRequestUrl/);
  assert.match(authSrc, /tryLocalDevAuthFixture|DEV_AUTH_BYPASS|localDevHost/);
  assert.match(authSrc, /delete localHeaders\.Authorization/);
}

// --- 5) Shell route + nav use platformAdmin / admin parity ---
{
  assert.match(appJs, /\/admin\/surfe-usage/);
  assert.match(appJs, /platformAdmin:\s*true/);
  assert.match(
    appJs,
    /normalized === '\/admin\/surfe-usage'[\s\S]*hasAdminNavAccess\(\)\s*\|\|\s*hasAdpMonthlyReviewAdminNavAccess\(\)/,
  );
}

// --- 6) Backend Surfe APIs remain behind adminAuth ---
{
  assert.match(server, /\/api\/admin\/surfe-usage\/summary/);
  assert.match(server, /\/api\/admin\/surfe-usage\/events/);
  assert.match(server, /adminAuth/);
  const requireAdminAccess = readFileSync(
    join(ROOT, "middleware/requireAdminAccess.js"),
    "utf8",
  );
  assert.match(requireAdminAccess, /hasLocalDealalityAdminAccess/);
  assert.match(requireAdminAccess, /forbidden_admin/);
}

// --- 7) In-memory gate decision matrix (authorized vs unauthorized) ---
{
  const sandbox = {
    window: {},
    document: {
      getElementById() {
        return null;
      },
    },
    console,
  };
  sandbox.window = sandbox;
  sandbox.global = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(gateSrc.replace(/\(window\)\s*;?\s*$/, "(window);"), sandbox);
  const gate = sandbox.SupportAdminGate || sandbox.window.SupportAdminGate;
  assert.ok(gate, "SupportAdminGate must export");
  assert.equal(gate.isAdminMe({ dealality: { isAdmin: true } }), true);
  assert.equal(
    gate.isAdminMe({ dealality: { isAdmin: false, flags: { isAdmin: true } } }),
    true,
  );
  assert.equal(
    gate.isAdminMe({
      dealality: { isAdmin: false, localDealalityAdminAccess: true },
    }),
    true,
  );
  assert.equal(
    gate.isAdminMe({ dealality: { isAdmin: false, isDemo: true } }),
    false,
    "Demo Mode alone must not grant Surfe admin",
  );
  assert.equal(gate.isAdminMe({ dealality: { isAdmin: false } }), false);
  assert.equal(gate.isAdminMe(null), false);
}

// --- 8) Live localhost API authorization (when staging :8080 is up) ---
{
  const base = process.env.SURFE_AUTH_TEST_BASE || "http://127.0.0.1:8080";
  async function probe(path, headers = {}) {
    try {
      const res = await fetch(base + path, {
        headers: { Accept: "application/json", ...headers },
        cache: "no-store",
      });
      const body = await res.json().catch(() => ({}));
      return { status: res.status, body, reachable: true };
    } catch (err) {
      return { reachable: false, error: String(err && err.message ? err.message : err) };
    }
  }

  const me = await probe("/api/me");
  if (!me.reachable) {
    console.log(
      "SKIP live API probes — localhost:8080 unreachable (" + me.error + ")",
    );
  } else {
    // Unauthorized: invalid JWT must not elevate
    const badSummary = await probe("/api/admin/surfe-usage/summary", {
      Authorization: "Bearer not.a.jwt",
    });
    assert.equal(badSummary.status, 401, "invalid JWT must be 401");

    // Local no-Bearer path: DEV_AUTH_BYPASS may authorize platform admin
    const localMe = await probe("/api/me");
    assert.equal(localMe.status, 200);
    assert.equal(localMe.body?.dealality?.isAdmin, true);
    const localSummary = await probe("/api/admin/surfe-usage/summary");
    assert.equal(localSummary.status, 200, "local admin must reach Surfe summary");
    assert.equal(localSummary.body?.ok, true);

    const localEvents = await probe("/api/admin/surfe-usage/events");
    assert.equal(localEvents.status, 200, "local admin must reach Surfe events");
  }
}

// --- 9) Regression: shell permits nav while embed denied is a wiring failure ---
{
  // If shell shows Surfe for platformAdmin, HTML must load dealality-memberstack-auth.
  // This is the exact defect that produced Access restricted for authorized admins.
  assert.ok(
    /platformAdmin:\s*true/.test(appJs) &&
      /dealality-memberstack-auth\.js/.test(html) &&
      !/\/js\/memberstack-auth\.js/.test(html),
    "Shell Surfe nav must not ship without embed DealalityMemberstackAuth wiring",
  );
}

console.log("PASS test-gdi-surfe-usage-admin-auth-v1");
