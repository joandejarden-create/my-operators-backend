#!/usr/bin/env node
/**
 * Governed platform admin elevation restores founder/demo admin access
 * when Airtable marks the identity as owner/isAdmin=false.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  elevateGovernedPlatformAdmin,
  elevateGovernedPlatformAdminOnRequest,
  isGovernedPlatformAdminEmail,
  loadGovernedPlatformAdminEmails,
  GOVERNED_PLATFORM_ADMIN_EMAILS_SOURCE,
} from "../lib/dealality/governed-platform-admin-emails.js";
import { canAccessAdpMonthlyReviewAdmin } from "../middleware/requireAdpMonthlyReviewAdminAccess.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const emails = loadGovernedPlatformAdminEmails();
assert.equal(emails.has("joandejarden@hotmail.com"), true);
assert.equal(emails.has("dealalitydemo@dealality.com"), true);
assert.equal(isGovernedPlatformAdminEmail("joandejarden@hotmail.com"), true);
assert.equal(GOVERNED_PLATFORM_ADMIN_EMAILS_SOURCE.includes("assignments.v1.json"), true);

const ownerOnly = {
  email: "joandejarden@hotmail.com",
  role: "owner",
  isAdmin: false,
  flags: { isAdmin: false, isOwner: true },
  workspaceAccess: ["Owner"],
};
assert.equal(elevateGovernedPlatformAdmin(ownerOnly, "joandejarden@hotmail.com"), true);
assert.equal(ownerOnly.isAdmin, true);
assert.equal(ownerOnly.flags.isAdmin, true);
assert.equal(ownerOnly.governedPlatformAdminElevated, true);
assert.equal(
  canAccessAdpMonthlyReviewAdmin(ownerOnly, {
    hostname: "localhost",
    headers: { host: "localhost:8080" },
  }),
  true
);

const stranger = {
  email: "customer@example.com",
  role: "owner",
  isAdmin: false,
  flags: { isAdmin: false },
  workspaceAccess: ["Owner"],
};
assert.equal(elevateGovernedPlatformAdmin(stranger), false);
assert.equal(stranger.isAdmin, false);
assert.equal(
  canAccessAdpMonthlyReviewAdmin(stranger, {
    hostname: "localhost",
    headers: { host: "localhost:8080" },
  }),
  false
);

const req = {
  memberstackEmail: "joandejarden@hotmail.com",
  memberstackMemberId: "mem_test_founder",
};
const stub = elevateGovernedPlatformAdminOnRequest(req);
assert.ok(stub);
assert.equal(req.dealalityUser.isAdmin, true);
assert.equal(req.dealalityUser.governedPlatformAdminElevated, true);

const meJs = fs.readFileSync(path.join(root, "api/me.js"), "utf8");
assert.match(meJs, /elevateGovernedPlatformAdmin/);
assert.match(meJs, /elevateGovernedPlatformAdmin\(dealality,\s*emailFromToken\)/);
assert.match(meJs, /users_row_missing_governed_admin_elevated|isGovernedPlatformAdminEmail\(emailFromToken\)/);
assert.match(meJs, /buildLocalDevBypassAdminMeResponse/);

const requireUser = fs.readFileSync(
  path.join(root, "middleware/requireDealalityUser.js"),
  "utf8"
);
assert.match(requireUser, /elevateGovernedPlatformAdminOnRequest/);
assert.match(requireUser, /elevateGovernedPlatformAdmin\(dealalityUser/);

const reviewsJs = fs.readFileSync(
  path.join(root, "public/js/admin-ai-demand-reviews.js"),
  "utf8"
);
assert.match(reviewsJs, /accessDenial|isAccessDenialError/);
assert.match(reviewsJs, /showDenied/);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "adp-governed-platform-admin-elevation-v1",
      founderElevated: true,
      customerDenied: true,
      meBypassParityWired: true,
    },
    null,
    2
  )
);
