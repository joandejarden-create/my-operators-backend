#!/usr/bin/env node
/**
 * GDI contact reveal — auth / provider / credits state classification gates.
 * No live Surfe. No paid spend.
 *   node scripts/test-gdi-contact-reveal-auth-states-v1.mjs
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import vm from "node:vm";

const ROOT = process.cwd();

function loadRevealClient() {
  const src = readFileSync(
    join(
      ROOT,
      "public/js/group-demand-intelligence/gdi-contact-reveal-client-v1.js",
    ),
    "utf8",
  );
  const sandbox = {
    location: { origin: "http://localhost:8080", search: "" },
    document: { body: { contains: () => true } },
    console,
    URL,
    URLSearchParams,
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(src, sandbox, {
    filename: "gdi-contact-reveal-client-v1.js",
  });
  return sandbox.DealalityGdiContactReveal || sandbox.window.DealalityGdiContactReveal;
}

const Reveal = loadRevealClient();
assert.ok(Reveal?.classifyRevealFailure, "classifyRevealFailure exported");

function assertKind(label, input, expectedKind, opts = {}) {
  const got = Reveal.classifyRevealFailure(input);
  assert.equal(
    got.kind,
    expectedKind,
    `${label}: expected kind=${expectedKind} got=${got.kind} msg=${got.message}`,
  );
  if (opts.signIn != null) {
    assert.equal(got.signIn, opts.signIn, `${label}: signIn`);
  }
  if (opts.notMatch) {
    assert.equal(
      opts.notMatch.test(got.message),
      false,
      `${label}: message must not match ${opts.notMatch}`,
    );
  }
  if (opts.match) {
    assert.match(got.message, opts.match, `${label}: message`);
  }
}

// 1) Browser AUTH_REQUIRED / LOGIN_MSG → sign-in (never reuse for other failures)
assertKind(
  "401 SIGN_IN_REQUIRED",
  {
    status: 401,
    data: { error: "SIGN_IN_REQUIRED", message: Reveal.MSG_SIGN_IN },
  },
  "auth",
  { signIn: true, match: /Sign in to Dealality/i },
);
assertKind(
  "legacy Please log in again",
  {
    status: 401,
    data: {
      error: "AUTH_REQUIRED",
      message: "Please log in again to continue.",
    },
  },
  "auth",
  {
    signIn: true,
    notMatch: /Please log in again to continue/i,
    match: /Sign in to Dealality/i,
  },
);

// 2) Anonymous server gate → sign-in
assertKind(
  "403 LIVE_PROVIDER_NOT_AUTHORIZED",
  {
    status: 403,
    data: {
      error: "LIVE_PROVIDER_NOT_AUTHORIZED",
      message: "Contact lookup is not authorized for this session.",
    },
  },
  "auth",
  { signIn: false },
);
assertKind(
  "403 SHARE_VIEWER_BLOCKED",
  {
    status: 403,
    data: {
      error: "SHARE_VIEWER_BLOCKED",
      message: "Contact enrichment is not available on share surfaces.",
    },
  },
  "auth",
  { signIn: true },
);
assertKind(
  "403 HOTEL_ACCESS_DENIED",
  {
    status: 403,
    data: {
      error: "HOTEL_ACCESS_DENIED",
      message:
        "Your account does not have permission to look up contact details for this hotel.",
    },
  },
  "auth",
  { signIn: false, match: /permission/i },
);

// 3) Credits / provider / miss are distinct from login
assertKind(
  "402 credits",
  {
    status: 402,
    data: {
      error: "INSUFFICIENT_CREDITS",
      message: "Insufficient Surfe email credits for this lookup.",
    },
  },
  "credits",
  { notMatch: /log in again/i },
);
assertKind(
  "503 provider",
  {
    status: 503,
    data: {
      error: "PROVIDER_UNAVAILABLE",
      message: Reveal.MSG_PROVIDER,
    },
  },
  "provider",
  { notMatch: /log in again/i, match: /provider is unavailable/i },
);
assertKind(
  "miss",
  { status: 200, data: { miss: true, error: "NO_RESULT", message: Reveal.MSG_MISS } },
  "miss",
  { notMatch: /log in again/i },
);

// Source contracts
const app = readFileSync(
  join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const api = readFileSync(join(ROOT, "api/group-demand-intelligence.js"), "utf8");
const server = readFileSync(join(ROOT, "server.js"), "utf8");
const html = readFileSync(
  join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);

assert.match(app, /contactRevealFetch/);
assert.match(app, /SIGN_IN_REQUIRED/);
assert.match(app, /fetchFn:\s*contactRevealFetch/);
assert.match(api, /SIGN_IN_REQUIRED/);
assert.match(api, /PROVIDER_UNAVAILABLE/);
assert.match(api, /assertGdiContactRevealAuthorization/);
assert.match(api, /resolveContactRevealMockAllowance/);
assert.match(api, /getSurfeApiKeyFromEnv/);
assert.match(
  server,
  /memberstackAuth[\s\S]{0,80}requireDealalityUser[\s\S]{0,80}postGdiContactReveal/,
);
assert.match(html, /gdi-contact-reveal-authz-v2/);

// Client must not surface LOGIN_MSG as catch-all for non-auth failures
const clientSrc = readFileSync(
  join(
    ROOT,
    "public/js/group-demand-intelligence/gdi-contact-reveal-client-v1.js",
  ),
  "utf8",
);
assert.match(clientSrc, /MSG_SIGN_IN/);
assert.match(clientSrc, /MSG_PROVIDER/);
assert.match(clientSrc, /MSG_CREDITS/);
assert.match(clientSrc, /classifyRevealFailure/);
assert.match(clientSrc, /gdi-contact-reveal-signin/);
assert.equal(
  /setRevealStatus\(\s*\(err && err\.message\)/.test(clientSrc) &&
    !/AUTH_REQUIRED/.test(clientSrc),
  false,
  "catch path must special-case AUTH_REQUIRED before raw err.message",
);

console.log(
  JSON.stringify(
    {
      suite: "gdi-contact-reveal-auth-states-v1",
      passed: true,
      rootCause:
        "External Railway GDI page has no Memberstack session; fetchFn→authFetch threw LOGIN_MSG before POST; client catch displayed it for all failures",
      fixed: [
        "contactRevealFetch → SIGN_IN_REQUIRED Response",
        "classifyRevealFailure distinct states",
        "server SIGN_IN_REQUIRED vs PROVIDER_UNAVAILABLE",
        "hotel-scoped Dealality user authorization",
      ],
    },
    null,
    2,
  ),
);
