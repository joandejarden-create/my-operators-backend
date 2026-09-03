/**
 * OWNER_APP_PROPERTY_LEVEL_AUTHORIZATION — property allowlist for authenticated owner app.
 * Share-capability clients use token scope instead (see adpShareCapabilityAuth).
 *
 * Assignments file (optional): data/ai-demand-positioning/owner-property-access/assignments.v1.json
 * Env: ADP_OWNER_APP_ADMIN_EMAILS (comma) — platform admins see all published ADP properties.
 */
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { PROPERTY_IDS } from "../brand-portfolio/bpp-period2-longitudinal-v1.js";
import { listPropertyProfiles } from "../data-model.js";

export const OWNER_APP_PROPERTY_LEVEL_AUTHORIZATION = "OWNER_APP_PROPERTY_LEVEL_AUTHORIZATION";

const ASSIGNMENTS_PATH = join(
  process.cwd(),
  "data/ai-demand-positioning/owner-property-access/assignments.v1.json"
);

function publishedPropertyIds() {
  const fromFixtures = listPropertyProfiles().map((p) => p.propertyId);
  if (fromFixtures.length) return fromFixtures;
  return [...PROPERTY_IDS];
}

function loadAssignmentsDoc() {
  if (!existsSync(ASSIGNMENTS_PATH)) return null;
  try {
    return JSON.parse(readFileSync(ASSIGNMENTS_PATH, "utf8"));
  } catch {
    return null;
  }
}

function parseEmailList(raw) {
  return String(raw || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function isPlatformAdmin(req) {
  const u = req.dealalityUser;
  if (u?.isAdmin === true) return true;
  if (u?.flags?.isAdmin === true) return true;
  const email = String(req.memberstackEmail || u?.email || "").toLowerCase();
  const adminEmails = parseEmailList(process.env.ADP_OWNER_APP_ADMIN_EMAILS);
  if (email && adminEmails.includes(email)) return true;
  const doc = loadAssignmentsDoc();
  const docAdmins = (doc?.adminEmails || []).map((e) => String(e).toLowerCase());
  if (email && docAdmins.includes(email)) return true;
  const msId = req.memberstackMemberId || u?.memberstackId || null;
  if (msId && (doc?.adminMemberstackIds || []).includes(msId)) return true;
  if (
    req.memberstackVerifiedVia === "dev_bypass" &&
    process.env.NODE_ENV !== "production" &&
    process.env.ADP_OWNER_APP_DEV_BYPASS_ALL_PROPERTIES !== "0"
  ) {
    return true;
  }
  return false;
}

function assignmentsForUser(req) {
  const doc = loadAssignmentsDoc();
  if (!doc?.assignments?.length) return [];
  const email = String(req.memberstackEmail || req.dealalityUser?.email || "").toLowerCase();
  const msId = req.memberstackMemberId || req.dealalityUser?.memberstackId || null;
  const out = new Set();
  for (const row of doc.assignments) {
    const matchEmail =
      email && row.email && String(row.email).toLowerCase() === email;
    const matchMs =
      msId && row.memberstackId && String(row.memberstackId) === String(msId);
    if (matchEmail || matchMs) {
      for (const id of row.propertyIds || []) out.add(id);
    }
  }
  return [...out];
}

/**
 * @returns {{ allowedPropertyIds: string[], isAdmin: boolean }}
 */
export function resolveOwnerAppPropertyAccess(req) {
  const published = publishedPropertyIds();
  if (isPlatformAdmin(req)) {
    return { allowedPropertyIds: published, isAdmin: true };
  }
  const assigned = assignmentsForUser(req);
  const allowed = assigned.filter((id) => published.includes(id));
  return { allowedPropertyIds: allowed, isAdmin: false };
}

export function ownerAppCanAccessProperty(req, propertyId) {
  if (!propertyId) return { ok: false, code: "PROPERTY_REQUIRED", status: 400 };
  const { allowedPropertyIds } = resolveOwnerAppPropertyAccess(req);
  if (allowedPropertyIds.includes(propertyId)) {
    return { ok: true, gate: OWNER_APP_PROPERTY_LEVEL_AUTHORIZATION };
  }
  return {
    ok: false,
    code: "OWNER_PROPERTY_ACCESS_DENIED",
    status: 403,
    error: "owner_property_access_denied",
    message: "You do not have access to this property report.",
  };
}

export function filterPropertiesForOwnerApp(req, properties) {
  const { allowedPropertyIds } = resolveOwnerAppPropertyAccess(req);
  const allow = new Set(allowedPropertyIds);
  return properties.filter((p) => allow.has(p.propertyId));
}
