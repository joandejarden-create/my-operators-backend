/**
 * Gate: AI Demand Leak Audit admin left-nav visibility for dealalitydemo /
 * governed platform admin emails (assignments.v1.json adminEmails).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  elevateGovernedPlatformAdmin,
  isGovernedPlatformAdminEmail,
  loadGovernedPlatformAdminEmails,
  GOVERNED_PLATFORM_ADMIN_EMAILS_SOURCE,
} from "../lib/dealality/governed-platform-admin-emails.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function read(rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

function main() {
  const emails = loadGovernedPlatformAdminEmails();
  assert(
    emails.has("dealalitydemo@dealality.com"),
    "dealalitydemo@dealality.com must be in governed adminEmails"
  );
  assert(
    isGovernedPlatformAdminEmail("dealalitydemo@dealality.com"),
    "isGovernedPlatformAdminEmail(dealalitydemo)"
  );
  assert(
    GOVERNED_PLATFORM_ADMIN_EMAILS_SOURCE.includes("assignments.v1.json"),
    "source path documented"
  );

  // Simulate Airtable Owner-only profile (the failure mode Joan hit)
  const ownerOnly = {
    email: "dealalitydemo@dealality.com",
    role: "owner",
    isAdmin: false,
    flags: { isAdmin: false, isOwner: true },
    workspaceAccess: ["Owner"],
  };
  assert(
    elevateGovernedPlatformAdmin(ownerOnly, "dealalitydemo@dealality.com") === true,
    "elevation applies for dealalitydemo"
  );
  assert(ownerOnly.isAdmin === true, "isAdmin elevated");
  assert(ownerOnly.flags.isAdmin === true, "flags.isAdmin elevated");
  assert(ownerOnly.governedPlatformAdminElevated === true, "elevation marker set");

  const stranger = {
    email: "random-owner@example.com",
    role: "owner",
    isAdmin: false,
    flags: { isAdmin: false },
  };
  assert(
    elevateGovernedPlatformAdmin(stranger) === false,
    "non-governed email must not elevate"
  );
  assert(stranger.isAdmin === false, "stranger stays non-admin");

  const meJs = read("api/me.js");
  assert(
    meJs.includes("elevateGovernedPlatformAdmin"),
    "/api/me must elevate governed admins for left-nav hasAdminNavAccess"
  );

  const requireUser = read("middleware/requireDealalityUser.js");
  assert(
    requireUser.includes("elevateGovernedPlatformAdminOnRequest"),
    "requireDealalityUser must stub governed admins when Users row is missing"
  );
  assert(
    meJs.includes("users_row_missing_governed_admin_elevated") ||
      meJs.includes("isGovernedPlatformAdminEmail(emailFromToken)"),
    "/api/me must elevate governed admins without Users row"
  );

  const appJs = read("public/app.js");
  assert(appJs.includes("NAV_CONFIG_SOURCE"), "nav config source marker");
  assert(appJs.includes("adpLeakAuditAdmin"), "leak audit nav flag in app.js");
  assert(
    appJs.includes("function hasAdminNavAccess()"),
    "nav visibility gate present"
  );
  assert(
    /elevateGovernedPlatformAdmin\(dealality,\s*emailFromToken\)/.test(meJs),
    "/api/me elevation must also try Memberstack token email"
  );
  // dealalitydemo visibility path: isAdmin from /api/me after elevation
  assert(
    /meDealality\.isAdmin === true/.test(appJs),
    "hasAdminNavAccess trusts dealality.isAdmin"
  );

  const reviewsHtml = read("public/app/admin/ai-demand-reviews.html");
  assert(
    reviewsHtml.includes("Open AI Demand Leak Audits"),
    "fallback CTA on paid ADP admin page"
  );
  assert(
    reviewsHtml.includes("/admin/adp-leak-audits/reports"),
    "fallback CTA href"
  );

  const reportsHtml = read("public/admin/adp-leak-audits-reports.html");
  assert(reportsHtml.includes('id="alaNavDebug"'), "debug panel container");
  assert(reportsHtml.includes("ala-methodology-box"), "ADP-style methodology disclaimer");
  assert(reportsHtml.includes("ala-property-picker"), "ADP-style report dropdown picker");
  assert(reportsHtml.includes('id="alaReportSelect"'), "report select");
  const reportsJs = read("public/js/admin-adp-leak-audits-reports.js");
  assert(reportsJs.includes('get("debug") === "nav"'), "debug=nav gate");
  assert(reportsJs.includes("governedPlatformAdminElevated"), "debug shows elevation");
  assert(reportsJs.includes("navConfigSource"), "debug shows nav source");
  assert(
    reportsJs.includes("filters and preview stay available"),
    "catalog failure must not wipe picker UI"
  );

  const appHtml = read("public/app.html");
  assert(
    appHtml.includes("app.js?v=ma-1.3.6-leak-nav") || appHtml.includes("leak-nav"),
    "app.js cache-bust after nav/admin visibility fix"
  );

  console.log("PASS test-adp-leak-audit-admin-nav-visibility-v1");
}

main();
