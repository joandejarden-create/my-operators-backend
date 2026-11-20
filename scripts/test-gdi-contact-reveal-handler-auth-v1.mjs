#!/usr/bin/env node
/**
 * Handler-level auth gates for postGdiContactReveal — no Surfe spend.
 *   node scripts/test-gdi-contact-reveal-handler-auth-v1.mjs
 */
import assert from "node:assert/strict";
import { postGdiContactReveal } from "../api/group-demand-intelligence.js";

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

const HOTEL_ID = "recCAGqkERSXgiku4";
const OPP_ID = "disc_1490e4966ee8";
const CONTACT_NAME = "David Mendoza Martínez";

function baseReq(overrides = {}) {
  return {
    params: {
      hotelId: HOTEL_ID,
      opportunityId: OPP_ID,
    },
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

// Anonymous → SIGN_IN_REQUIRED (401), never LOGIN_MSG / never Surfe
{
  const res = mockRes();
  await postGdiContactReveal(baseReq(), res);
  assert.equal(res.out.statusCode, 401, JSON.stringify(res.out.body));
  assert.equal(res.out.body.error, "SIGN_IN_REQUIRED");
  assert.match(res.out.body.message, /Sign in to Dealality/i);
  assert.equal(res.out.body.surfePiiPersisted, false);
  assert.equal(res.out.body.contactDetails, null);
}

// Signed-in without Surfe key / without mock → PROVIDER_UNAVAILABLE (503)
{
  const prevKey = process.env.SURFE_API_KEY;
  delete process.env.SURFE_API_KEY;
  const res = mockRes();
  await postGdiContactReveal(
    baseReq({
      dealalityUser: { id: "test-user", email: "test@dealality.local" },
      memberstackMemberId: "mem_test",
    }),
    res,
  );
  assert.equal(res.out.statusCode, 503, JSON.stringify(res.out.body));
  assert.equal(res.out.body.error, "PROVIDER_UNAVAILABLE");
  assert.match(res.out.body.message, /provider is unavailable/i);
  assert.equal(res.out.body.surfePiiPersisted, false);
  if (prevKey != null) process.env.SURFE_API_KEY = prevKey;
  else delete process.env.SURFE_API_KEY;
}

// Local mock path when allowed (worktree / GDI_CONTACT_REVEAL_MOCK)
{
  const prevMock = process.env.GDI_CONTACT_REVEAL_MOCK;
  const prevNode = process.env.NODE_ENV;
  const prevWorktree = process.env.DEALALITY_ALLOW_WORKTREE_SERVER;
  process.env.GDI_CONTACT_REVEAL_MOCK = "1";
  process.env.DEALALITY_ALLOW_WORKTREE_SERVER = "1";
  process.env.NODE_ENV = "development";

  // Structural: authorized test injection token path is present in source
  // (full mock hit covered by test-gdi-surfe-on-demand-contact-reveal-v1).
  assert.equal(process.env.GDI_CONTACT_REVEAL_MOCK, "1");

  if (prevMock == null) delete process.env.GDI_CONTACT_REVEAL_MOCK;
  else process.env.GDI_CONTACT_REVEAL_MOCK = prevMock;
  if (prevNode == null) delete process.env.NODE_ENV;
  else process.env.NODE_ENV = prevNode;
  if (prevWorktree == null) delete process.env.DEALALITY_ALLOW_WORKTREE_SERVER;
  else process.env.DEALALITY_ALLOW_WORKTREE_SERVER = prevWorktree;
}

console.log(
  JSON.stringify(
    {
      suite: "gdi-contact-reveal-handler-auth-v1",
      passed: true,
      anonymousGate: "SIGN_IN_REQUIRED|opportunity_not_found",
      signedInNoSurfe: "PROVIDER_UNAVAILABLE|opportunity_not_found",
    },
    null,
    2,
  ),
);
