/**
 * Governed platform admin elevation (founder / dealalitydemo / assignments list).
 *
 * Airtable may mark these identities as role=owner with isAdmin=false.
 * Elevation restores intended admin access for AI Demand Admin, Leak Audits nav,
 * and related admin APIs — without granting arbitrary emails.
 *
 * Source of truth: data/ai-demand-positioning/owner-property-access/assignments.v1.json
 * adminEmails (+ optional ADP_OWNER_APP_ADMIN_EMAILS env).
 *
 * Production-safe: only elevates emails on the governed allowlist.
 * Does not disable middleware or open admin to all users.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";

export const GOVERNED_PLATFORM_ADMIN_EMAILS_SOURCE =
  "data/ai-demand-positioning/owner-property-access/assignments.v1.json";

function parseEmailList(raw) {
  return String(raw || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function normalizeEmail(email) {
  return String(email || "")
    .trim()
    .toLowerCase();
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 * @param {string} [cwd]
 * @returns {Set<string>}
 */
export function loadGovernedPlatformAdminEmails(env = process.env, cwd = process.cwd()) {
  const fromEnv = parseEmailList(env.ADP_OWNER_APP_ADMIN_EMAILS);
  const path = join(cwd, GOVERNED_PLATFORM_ADMIN_EMAILS_SOURCE);
  let fromDoc = [];
  if (existsSync(path)) {
    try {
      const doc = JSON.parse(readFileSync(path, "utf8"));
      fromDoc = (doc?.adminEmails || [])
        .map((e) => normalizeEmail(e))
        .filter(Boolean);
    } catch {
      fromDoc = [];
    }
  }
  return new Set([...fromEnv, ...fromDoc]);
}

/**
 * @param {string|null|undefined} email
 * @param {NodeJS.ProcessEnv} [env]
 */
export function isGovernedPlatformAdminEmail(email, env = process.env) {
  const normalized = normalizeEmail(email);
  if (!normalized) return false;
  return loadGovernedPlatformAdminEmails(env).has(normalized);
}

/**
 * Mutates dealality-like object in place when email is governed.
 * @returns {boolean} whether elevation applied
 */
export function elevateGovernedPlatformAdmin(dealality, emailHint = null, env = process.env) {
  if (!dealality || typeof dealality !== "object") return false;
  const email = normalizeEmail(
    emailHint || dealality.email || dealality.userEmail || dealality.memberstackEmail
  );
  if (!isGovernedPlatformAdminEmail(email, env)) return false;

  dealality.isAdmin = true;
  if (!dealality.flags || typeof dealality.flags !== "object") {
    dealality.flags = {};
  }
  dealality.flags.isAdmin = true;
  if (!dealality.role || dealality.role === "owner" || dealality.role === "unknown") {
    dealality.role = "admin";
  }
  if (!dealality.primaryRole || dealality.primaryRole === "owner") {
    dealality.primaryRole = "admin";
  }
  const workspaces = Array.isArray(dealality.workspaceAccess)
    ? dealality.workspaceAccess
    : [];
  if (!workspaces.includes("Admin")) {
    dealality.workspaceAccess = [...workspaces, "Admin"];
  }
  dealality.governedPlatformAdminElevated = true;
  return true;
}

/**
 * When Airtable Users row is missing, stub a governed admin on the request
 * so AI Demand Admin /api routes match /api/me elevation.
 *
 * @returns {object|null} dealalityUser stub when elevated
 */
export function elevateGovernedPlatformAdminOnRequest(req = {}, env = process.env) {
  const email = normalizeEmail(
    req.memberstackEmail ||
      req.memberstackAuth?.email ||
      req.dealalityUser?.email
  );
  if (!isGovernedPlatformAdminEmail(email, env)) return null;

  if (!req.dealalityUser || typeof req.dealalityUser !== "object") {
    req.dealalityUser = {
      email,
      memberstackId: req.memberstackMemberId || null,
      role: "admin",
      primaryRole: "admin",
      legacyRole: "admin",
      workspaceAccess: ["Admin"],
      flags: { isAdmin: true, isOwner: true },
      isAdmin: true,
      isOwner: true,
      isBrand: false,
      isOperator: false,
      isDemo: false,
      userRecordId: null,
      airtableUserFound: false,
      userSource: "GOVERNED_PLATFORM_ADMIN_ELEVATION",
      governedPlatformAdminElevated: true,
    };
  } else {
    elevateGovernedPlatformAdmin(req.dealalityUser, email, env);
  }
  return req.dealalityUser;
}
