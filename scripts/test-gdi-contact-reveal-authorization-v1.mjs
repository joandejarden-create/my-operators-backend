#!/usr/bin/env node
/**
 * GDI contact-reveal hotel-scoped authorization + handler regression gates.
 * No Surfe spend.
 *   node scripts/test-gdi-contact-reveal-authorization-v1.mjs
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  assertGdiContactRevealAuthorization,
  resolveContactRevealMockAllowance,
  CONTACT_REVEAL_AUTH_DENY,
} from "../lib/group-demand-intelligence/gdi-contact-reveal-authorization-v1.js";
import { postGdiContactReveal } from "../api/group-demand-intelligence.js";
import {
  revealGdiContactOnDemand,
  createMockGdiContactRevealProvider,
} from "../lib/group-demand-intelligence/gdi-contact-on-demand-reveal-v1.js";

const ROOT = process.cwd();
const HOTEL_ID = "recCAGqkERSXgiku4";
const OTHER_HOTEL = "recnPB3Q02NO8Ma1o";
const OPP_ID = "disc_1490e4966ee8";
const CONTACT_NAME = "David Mendoza Martínez";

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    out,
    status(code) {
      out.statusCode = code;
      return this;
    },
    json(payload) {
      out.body = payload;
      return this;
    },
  };
}

function baseReq(overrides = {}) {
  return {
    params: { hotelId: HOTEL_ID, opportunityId: OPP_ID },
    body: {
      channel: "both",
      contactName: CONTACT_NAME,
      authorizeLiveProvider: true,
    },
    headers: {},
    dealalityUser: null,
    memberstackMemberId: null,
    ...overrides,
  };
}

const authorizedOperator = {
  email: "ops@dealality.local",
  memberstackId: "mem_ops",
  isOperator: true,
  canAccessOperatorWorkspace: true,
  authorizedHotelIds: [HOTEL_ID],
  role: "operator",
};

const wrongHotelUser = {
  email: "other@dealality.local",
  memberstackId: "mem_other",
  isOperator: true,
  canAccessOperatorWorkspace: true,
  authorizedHotelIds: [OTHER_HOTEL],
  role: "operator",
};

const adminUser = {
  email: "admin@dealality.local",
  memberstackId: "mem_admin",
  isAdmin: true,
  role: "admin",
};

// --- Unit: authorization module ---
{
  const anon = assertGdiContactRevealAuthorization({}, { hotelId: HOTEL_ID });
  assert.equal(anon.ok, false);
  assert.equal(anon.code, CONTACT_REVEAL_AUTH_DENY.SIGN_IN_REQUIRED);
  assert.equal(anon.status, 401);
}

{
  const msOnly = assertGdiContactRevealAuthorization(
    { memberstackMemberId: "mem_only" },
    { hotelId: HOTEL_ID },
  );
  assert.equal(msOnly.ok, false);
  assert.equal(
    msOnly.code,
    CONTACT_REVEAL_AUTH_DENY.MEMBERSTACK_ID_INSUFFICIENT,
  );
}

{
  const wrong = assertGdiContactRevealAuthorization(
    { dealalityUser: wrongHotelUser },
    { hotelId: HOTEL_ID },
  );
  assert.equal(wrong.ok, false);
  assert.equal(wrong.code, CONTACT_REVEAL_AUTH_DENY.HOTEL_ACCESS_DENIED);
  assert.equal(wrong.status, 403);
}

{
  const ok = assertGdiContactRevealAuthorization(
    { dealalityUser: authorizedOperator },
    { hotelId: HOTEL_ID },
  );
  assert.equal(ok.ok, true);
  assert.equal(ok.via, "dealalityUser.hotel_scope");
}

{
  const share = assertGdiContactRevealAuthorization(
    { dealalityUser: authorizedOperator, gdiShareCapability: { token: "x" } },
    { hotelId: HOTEL_ID },
  );
  assert.equal(share.ok, false);
  assert.equal(share.code, CONTACT_REVEAL_AUTH_DENY.SHARE_VIEWER_BLOCKED);
}

{
  const prodMock = resolveContactRevealMockAllowance(
    {},
    { useMock: true },
    {
      NODE_ENV: "production",
      RAILWAY_ENVIRONMENT: "production",
      RAILWAY_PROJECT_ID: "proj",
      GDI_CONTACT_REVEAL_MOCK: "1",
    },
  );
  assert.equal(prodMock.allowMock, false);
  assert.equal(prodMock.reason, "production_customer_host");
}

{
  const localMock = resolveContactRevealMockAllowance(
    {},
    { useMock: true },
    {
      NODE_ENV: "development",
      DEALALITY_ALLOW_WORKTREE_SERVER: "1",
      GDI_CONTACT_REVEAL_MOCK: "1",
    },
  );
  assert.equal(localMock.allowMock, true);
}

// --- Handler regressions ---
{
  const res = mockRes();
  await postGdiContactReveal(baseReq(), res);
  assert.equal(res.out.statusCode, 401);
  assert.equal(res.out.body.error, "SIGN_IN_REQUIRED");
  assert.equal(res.out.body.surfePiiPersisted, false);
  assert.equal(res.out.body.contactDetails, null);
}

{
  const res = mockRes();
  await postGdiContactReveal(
    baseReq({
      memberstackMemberId: "mem_only",
      dealalityUser: null,
    }),
    res,
  );
  assert.equal(res.out.statusCode, 403);
  assert.equal(res.out.body.error, "MEMBERSTACK_ID_INSUFFICIENT");
}

{
  const res = mockRes();
  await postGdiContactReveal(
    baseReq({ dealalityUser: wrongHotelUser }),
    res,
  );
  assert.equal(res.out.statusCode, 403, JSON.stringify(res.out.body));
  assert.equal(res.out.body.error, "HOTEL_ACCESS_DENIED");
  assert.equal(res.out.body.contactDetails, null);
}

// Authorized + missing Surfe key → PROVIDER_UNAVAILABLE (no spend)
{
  const prevKey = process.env.SURFE_API_KEY;
  const prevMock = process.env.GDI_CONTACT_REVEAL_MOCK;
  const prevWorktree = process.env.DEALALITY_ALLOW_WORKTREE_SERVER;
  delete process.env.SURFE_API_KEY;
  delete process.env.GDI_CONTACT_REVEAL_MOCK;
  delete process.env.DEALALITY_ALLOW_WORKTREE_SERVER;
  const res = mockRes();
  await postGdiContactReveal(
    baseReq({
      dealalityUser: authorizedOperator,
      body: {
        channel: "both",
        contactName: CONTACT_NAME,
        authorizeLiveProvider: true,
        useMock: false,
      },
    }),
    res,
  );
  assert.equal(res.out.statusCode, 503, JSON.stringify(res.out.body));
  assert.equal(res.out.body.error, "PROVIDER_UNAVAILABLE");
  if (prevKey != null) process.env.SURFE_API_KEY = prevKey;
  if (prevMock != null) process.env.GDI_CONTACT_REVEAL_MOCK = prevMock;
  if (prevWorktree != null) {
    process.env.DEALALITY_ALLOW_WORKTREE_SERVER = prevWorktree;
  }
}

// Authorized + isolated mock → session-only hit (no Surfe)
{
  const prevMock = process.env.GDI_CONTACT_REVEAL_MOCK;
  const prevWorktree = process.env.DEALALITY_ALLOW_WORKTREE_SERVER;
  const prevNode = process.env.NODE_ENV;
  process.env.GDI_CONTACT_REVEAL_MOCK = "1";
  process.env.DEALALITY_ALLOW_WORKTREE_SERVER = "1";
  process.env.NODE_ENV = "development";
  const res = mockRes();
  await postGdiContactReveal(
    baseReq({
      dealalityUser: authorizedOperator,
      body: {
        channel: "both",
        contactName: CONTACT_NAME,
        authorizeLiveProvider: false,
        useMock: true,
        mockScenario: "hit",
      },
    }),
    res,
  );
  assert.equal(res.out.statusCode, 200, JSON.stringify(res.out.body));
  assert.equal(res.out.body.ok, true);
  assert.equal(res.out.body.persisted, false);
  assert.equal(res.out.body.surfePiiPersisted, false);
  assert.ok(
    res.out.body.contactDetails?.email || res.out.body.contactDetails?.phone,
  );
  assert.equal(res.out.body.contactName, CONTACT_NAME);
  assert.equal(res.out.body.opportunityId, OPP_ID);
  if (prevMock == null) delete process.env.GDI_CONTACT_REVEAL_MOCK;
  else process.env.GDI_CONTACT_REVEAL_MOCK = prevMock;
  if (prevWorktree == null) delete process.env.DEALALITY_ALLOW_WORKTREE_SERVER;
  else process.env.DEALALITY_ALLOW_WORKTREE_SERVER = prevWorktree;
  if (prevNode == null) delete process.env.NODE_ENV;
  else process.env.NODE_ENV = prevNode;
}

// Anonymous cannot obtain mock even when env mock is on
{
  const prevMock = process.env.GDI_CONTACT_REVEAL_MOCK;
  const prevWorktree = process.env.DEALALITY_ALLOW_WORKTREE_SERVER;
  process.env.GDI_CONTACT_REVEAL_MOCK = "1";
  process.env.DEALALITY_ALLOW_WORKTREE_SERVER = "1";
  const res = mockRes();
  await postGdiContactReveal(
    baseReq({
      body: {
        channel: "both",
        contactName: CONTACT_NAME,
        authorizeLiveProvider: false,
        useMock: true,
        mockScenario: "hit",
      },
    }),
    res,
  );
  assert.equal(res.out.statusCode, 401);
  assert.equal(res.out.body.error, "SIGN_IN_REQUIRED");
  assert.equal(res.out.body.contactDetails, null);
  if (prevMock == null) delete process.env.GDI_CONTACT_REVEAL_MOCK;
  else process.env.GDI_CONTACT_REVEAL_MOCK = prevMock;
  if (prevWorktree == null) delete process.env.DEALALITY_ALLOW_WORKTREE_SERVER;
  else process.env.DEALALITY_ALLOW_WORKTREE_SERVER = prevWorktree;
}

// Mock miss → no-match (provider layer, no Surfe)
{
  const miss = await revealGdiContactOnDemand({
    channel: "both",
    contact: { name: CONTACT_NAME },
    organization: { name: "Test Org" },
    provider: createMockGdiContactRevealProvider("miss"),
    authorizeLiveProvider: false,
  });
  assert.equal(miss.miss, true);
  assert.equal(miss.contactDetails, null);
  assert.equal(miss.persisted !== true, true);
}

// Credits / provider classification still distinct in client source
{
  const client = readFileSync(
    join(ROOT, "public/js/group-demand-intelligence/gdi-contact-reveal-client-v1.js"),
    "utf8",
  );
  assert.match(client, /MSG_CREDITS/);
  assert.match(client, /MSG_PROVIDER/);
  assert.match(client, /HOTEL_ACCESS_DENIED/);
  assert.equal(/gdiRevealTestToken/.test(client), false);
  assert.equal(/X-Dealality-Gdi-Reveal-Test/.test(client), false);
}

// Route requires memberstackAuth + requireDealalityUser (not pilot read alone)
{
  const server = readFileSync(join(ROOT, "server.js"), "utf8");
  const block = server.match(
    /contacts\/reveal[\s\S]{0,280}postGdiContactReveal/,
  );
  assert.ok(block, "reveal route present");
  assert.match(block[0], /memberstackAuth/);
  assert.match(block[0], /requireDealalityUser/);
  assert.equal(/gdiPilotReadAuth/.test(block[0]), false);
}

// App shell embeds GDI with msToken (real session flow — no invented JWT)
{
  const appShell = readFileSync(join(ROOT, "public/app.js"), "utf8");
  assert.match(appShell, /group-demand-intelligence/);
  assert.match(appShell, /msToken/);
  const memberAuth = readFileSync(
    join(ROOT, "public/js/dealality-memberstack-auth.js"),
    "utf8",
  );
  assert.match(memberAuth, /msToken|memberstackToken/);
  assert.match(memberAuth, /isValidBearerToken/);
}

// Admin can access without explicit hotel list
{
  const ok = assertGdiContactRevealAuthorization(
    { dealalityUser: adminUser },
    { hotelId: HOTEL_ID },
  );
  assert.equal(ok.ok, true);
  assert.equal(ok.via, "dealalityUser.admin");
}

console.log(
  JSON.stringify(
    {
      suite: "gdi-contact-reveal-authorization-v1",
      passed: true,
      gates: [
        "anonymous_401",
        "memberstack_id_insufficient_403",
        "wrong_hotel_403",
        "authorized_mock_200_isolated",
        "authorized_no_surfe_key_503",
        "anonymous_mock_denied",
        "share_blocked",
        "production_mock_disabled",
        "app_shell_msToken_session",
        "route_memberstack_requireDealalityUser",
      ],
    },
    null,
    2,
  ),
);
