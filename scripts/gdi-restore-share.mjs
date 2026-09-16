#!/usr/bin/env node
/**
 * Re-register an existing GDI share URL into the local registry without changing the token.
 * Use after Railway redeploys wipe ephemeral registry rows (SHARE_UNKNOWN).
 *
 *   node scripts/gdi-restore-share.mjs --share="https://…/group-demand-intelligence-share.html?share=gdishare.v1.…"
 *   node scripts/gdi-restore-share.mjs --token="gdishare.v1.…"
 *
 * Requires the same GDI_SHARE_CAPABILITY_SECRET that signed the token.
 * Self-heal on verify is also enabled in production by default after deploy.
 */

import {
  restoreGdiShareCapabilityFromToken,
  GDI_SHARE_TOKEN_PREFIX,
} from "../lib/group-demand-intelligence/index.js";

function arg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

function extractToken(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  try {
    if (s.includes("share=")) {
      const u = new URL(s, "https://example.invalid");
      const q = u.searchParams.get("share");
      if (q) return decodeURIComponent(q);
    }
  } catch {
    /* fall through */
  }
  const m = s.match(/(gdishare\.v1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)/);
  if (m) return m[1];
  if (s.startsWith(GDI_SHARE_TOKEN_PREFIX)) return s;
  return s;
}

const raw = arg("share") || arg("token") || process.argv[2];
const token = extractToken(raw);
if (!token) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: "missing_share",
        usage:
          'node scripts/gdi-restore-share.mjs --share="https://…?share=gdishare.v1.…"',
      },
      null,
      2
    )
  );
  process.exit(1);
}

const result = restoreGdiShareCapabilityFromToken(token, {
  reason: "manual_restore_from_share_url",
});

if (!result.ok) {
  console.error(JSON.stringify({ ok: false, ...result }, null, 2));
  process.exit(1);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      tokenId: result.tokenId,
      hotelId: result.hotelId,
      healed: result.healed,
      meta: result.meta,
      note:
        "Same share URL remains valid. Commit config/client-share/gdi-share-registry/active-tokens.json and redeploy, or rely on production signature self-heal.",
    },
    null,
    2
  )
);
