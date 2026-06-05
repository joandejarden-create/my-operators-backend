/**
 * Memberstack sandbox (Test Mode) vs live (Production) — keys must match.
 *
 * - Secret: sk_sb_* → sandbox Admin API + sandbox members
 * - Secret: sk_* (not sk_sb_) → live/production
 * - App ID: use the app id from the same mode in Memberstack dashboard (toggle Test / Live).
 */

export function memberstackSecretEnvironment(secretKey) {
  const key = typeof secretKey === "string" ? secretKey.trim() : "";
  if (!key) return "unset";
  if (key.startsWith("sk_sb_")) return "sandbox";
  if (key.startsWith("sk_")) return "live";
  return "unknown";
}

export function isSandboxMemberstackId(memberstackId) {
  return String(memberstackId || "").startsWith("mem_sb_");
}

export function isLiveMemberstackId(memberstackId) {
  const id = String(memberstackId || "");
  return id.startsWith("mem_") && !id.startsWith("mem_sb_");
}

/** Pick Admin API secret for member lookups (sandbox mem_sb_ → test key when set). */
export function memberstackAdminSecretForMemberId(memberstackId) {
  const primary = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  const sandbox = (
    process.env.MEMBERSTACK_TEST_SECRET_KEY ||
    process.env.MEMBERSTACK_SANDBOX_SECRET_KEY ||
    ""
  ).trim();
  if (isSandboxMemberstackId(memberstackId) && sandbox) return sandbox;
  return primary;
}

/**
 * When matching /api/me by email, avoid overwriting a live mem_ id with localhost sandbox mem_sb_.
 */
export function shouldSyncMemberstackIdToUsersRow(existingId, incomingId) {
  const existing = String(existingId || "").trim();
  const incoming = String(incomingId || "").trim();
  if (!incoming) return false;
  if (!existing) return true;
  if (existing === incoming) return false;
  if (isSandboxMemberstackId(incoming) && isLiveMemberstackId(existing)) return false;
  return true;
}

export function describeMemberstackEnvForLogs() {
  const key = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  const appId = (process.env.MEMBERSTACK_APP_ID || "").trim();
  const env = memberstackSecretEnvironment(key);
  const parts = [`Memberstack Admin API: ${env}`];
  if (appId) parts.push(`MEMBERSTACK_APP_ID set (${appId.slice(0, 12)}…)`);
  else parts.push("MEMBERSTACK_APP_ID missing — DOM signup on /signup will fail");
  if (env === "live" && process.env.NODE_ENV !== "production") {
    parts.push("WARNING local/dev is using a LIVE secret key — signups go to Production members");
  }
  if (env === "sandbox" && process.env.NODE_ENV === "production") {
    parts.push("WARNING production deploy has SANDBOX secret key");
  }
  return parts.join(" | ");
}
