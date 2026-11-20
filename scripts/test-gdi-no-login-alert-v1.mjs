/**
 * GDI must not raise native window.alert("Please log in again…") from optional
 * authenticated fetches (401 retry / Portfolio / Priorities).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const authSrc = fs.readFileSync(
  path.join(root, "public/js/dealality-memberstack-auth.js"),
  "utf8",
);
const appSrc = fs.readFileSync(
  path.join(root, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const pdiSrc = fs.readFileSync(
  path.join(root, "public/js/group-demand-intelligence/portfolio-demand-intelligence-tab.js"),
  "utf8",
);
const prioritiesSrc = fs.readFileSync(
  path.join(root, "public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js"),
  "utf8",
);
const htmlSrc = fs.readFileSync(
  path.join(root, "public/group-demand-intelligence.html"),
  "utf8",
);

assert.match(authSrc, /notifyLoginRequired === false/);
assert.match(authSrc, /opts\.silent === true/);
assert.match(authSrc, /err\.code = 'AUTH_REQUIRED'/);

assert.match(appSrc, /withSilentAuthOptions/);
assert.match(appSrc, /retryWithMemberstackJwt/);
assert.match(appSrc, /notifyLoginRequired:\s*false/);
assert.match(appSrc, /getAuthHeaders/);
assert.doesNotMatch(
  appSrc,
  /if \(r\.status === 401 && memberFetch\) return memberFetch\(url, opts\)/,
);

assert.match(pdiSrc, /getAuthHeaders/);
assert.match(pdiSrc, /waitForLogin:\s*false/);
assert.match(pdiSrc, /AUTH_REQUIRED/);
assert.match(pdiSrc, /OPERATOR_NOT_MAPPED/);
assert.match(pdiSrc, /UNAVAILABLE/);
assert.match(appSrc, /showPortfolio/);
assert.match(appSrc, /DealalityGdiPortfolioTab\.mount/);
assert.match(appSrc, /else if \(\s*window\.DealalityGdiPortfolioTab/);

assert.match(prioritiesSrc, /notifyLoginRequired:\s*false/);
assert.match(prioritiesSrc, /getAuthHeaders/);

assert.match(htmlSrc, /dealality-memberstack-auth\.js\?v=gdi-no-login-alert-v1/);
// Cache marker may advance with later GDI auth UX fixes; app must still load.
assert.match(htmlSrc, /app\.js\?v=gdi-(?:no-login-alert|contact-reveal-auth)-v1/);
assert.match(appSrc, /contactRevealFetch|notifyLoginRequired:\s*false/);

// Runtime: authFetch with notifyLoginRequired:false must not call alert.
const alerts = [];
const sandbox = {
  window: {},
  globalThis: {},
  console,
  setTimeout,
  clearTimeout,
  URL,
  URLSearchParams,
  Promise,
  Object,
  Array,
  String,
  Boolean,
  Error,
  Math,
  Date,
  JSON,
  location: { href: "https://example.test/gdi", search: "", hash: "", origin: "https://example.test" },
  localStorage: { getItem: () => null, setItem: () => {}, removeItem: () => {} },
  fetch: async () => {
    throw new Error("unexpected fetch");
  },
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
sandbox.top = sandbox;
sandbox.parent = sandbox;
sandbox.addEventListener = () => {};
sandbox.removeEventListener = () => {};
sandbox.alert = (msg) => {
  alerts.push(String(msg));
};
sandbox.window.alert = sandbox.alert;
sandbox.window.location = sandbox.location;
sandbox.window.localStorage = sandbox.localStorage;
sandbox.window.addEventListener = sandbox.addEventListener;
sandbox.window.removeEventListener = sandbox.removeEventListener;
vm.createContext(sandbox);
vm.runInContext(authSrc, sandbox);

const auth = sandbox.DealalityMemberstackAuth;
assert.ok(auth && typeof auth.authFetch === "function");
// Sandbox has no Memberstack client / JWT → auth.error path.

let threw = null;
try {
  await auth.authFetch("/api/example", {
    notifyLoginRequired: false,
    waitForLogin: false,
  });
} catch (e) {
  threw = e;
}
assert.ok(threw, "authFetch should throw when JWT missing");
assert.equal(threw.code, "AUTH_REQUIRED");
assert.equal(alerts.length, 0, "silent authFetch must not alert");

// Default path still alerts (legacy My Deals) — intentional.
alerts.length = 0;
threw = null;
try {
  await auth.authFetch("/api/example", { waitForLogin: false });
} catch (e) {
  threw = e;
}
assert.ok(threw);
assert.equal(alerts.length, 1, "default authFetch still alerts for legacy callers");
assert.match(alerts[0], /log in again/i);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "gdi-no-login-alert-v1",
      silentOptionHonored: true,
      legacyAlertPreserved: true,
      gdiFetchNoAlertRetry: true,
      cacheMarker: "gdi-contact-reveal-auth-v1",
    },
    null,
    2,
  ),
);
