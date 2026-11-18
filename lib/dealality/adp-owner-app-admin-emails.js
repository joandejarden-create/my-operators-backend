/**
 * Shared ADP owner-app admin email allowlist (server-side).
 * Used by optionalDealalityAuth elevation and Radar internal-preview auth.
 * Never trusts client headers alone — callers must already have a verified identity.
 */

import { existsSync, readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const MODULE_DIR = dirname(fileURLToPath(import.meta.url));

function parseEmailList(raw) {
  return String(raw || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function readAdminEmailsFromAssignments(filePath) {
  if (!existsSync(filePath)) return [];
  try {
    const doc = JSON.parse(readFileSync(filePath, "utf8"));
    return (doc?.adminEmails || [])
      .map((e) => String(e).toLowerCase().trim())
      .filter(Boolean);
  } catch {
    return [];
  }
}

/**
 * Load ADP owner-app admin emails from env + assignments.v1.json.
 * Tries process.cwd() first, then repo root relative to this module (cwd-safe).
 */
export function loadAdpOwnerAppAdminEmails(env = process.env) {
  const fromEnv = parseEmailList(env.ADP_OWNER_APP_ADMIN_EMAILS);
  const candidates = [
    join(process.cwd(), "data/ai-demand-positioning/owner-property-access/assignments.v1.json"),
    join(
      MODULE_DIR,
      "../../data/ai-demand-positioning/owner-property-access/assignments.v1.json"
    ),
  ];
  let fromDoc = [];
  for (const path of candidates) {
    fromDoc = readAdminEmailsFromAssignments(path);
    if (fromDoc.length) break;
  }
  return new Set([...fromEnv, ...fromDoc]);
}

export function normalizeIdentityEmail(email) {
  return String(email || "")
    .trim()
    .toLowerCase();
}

/**
 * True when any authenticated identity email is on the ADP owner-app admin list.
 */
export function isAdpOwnerAppAdminEmail(emails, env = process.env) {
  const list = loadAdpOwnerAppAdminEmails(env);
  const values = Array.isArray(emails) ? emails : [emails];
  for (const raw of values) {
    const email = normalizeIdentityEmail(raw);
    if (email && list.has(email)) return true;
  }
  return false;
}
