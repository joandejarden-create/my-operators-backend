#!/usr/bin/env node
/**
 * Issue a read-only GDI share token for Bethesda pilot.
 *
 *   GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 node scripts/gdi-issue-share.mjs
 *   node scripts/gdi-issue-share.mjs --expires=2026-12-31
 *   node scripts/gdi-revoke-share.mjs --token-id=gdisht_...
 */

import {
  issueGdiShareCapability,
  PILOT_HOTEL_ID,
} from "../lib/group-demand-intelligence/index.js";

function arg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

const expires = arg("expires");
const label = arg("label") || "Bethesda Marriott GDI Pilot";

process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET =
  process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET || "1";

const issued = issueGdiShareCapability({
  hotelId: PILOT_HOTEL_ID,
  label,
  expiresAt: expires || null,
});

console.log(
  JSON.stringify(
    {
      ok: true,
      tokenId: issued.tokenId,
      hotelId: issued.hotelId,
      sharePath: issued.sharePath,
      localUrlExample: `http://localhost:8080${issued.sharePath}`,
      expiresAt: issued.meta.expiresAt,
      mode: "read_only",
      note: "Recipient cannot run research, edit data, or open Research Audit.",
    },
    null,
    2
  )
);
