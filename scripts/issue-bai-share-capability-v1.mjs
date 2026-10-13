#!/usr/bin/env node
/**
 * Issue a Brand AI (BAI) share capability for local / founder handoff.
 *
 *   BAI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 node scripts/issue-bai-share-capability-v1.mjs --parent=hilton
 *   node scripts/issue-bai-share-capability-v1.mjs --parent=hilton --token-id=sht_baip_5de428d7d284054b8756cf0a
 *   node scripts/issue-bai-share-capability-v1.mjs --parent=hilton --expires=2026-12-31
 *
 * Production note: use the production BAI_SHARE_CAPABILITY_SECRET when minting
 * URLs for Railway. Local DEV secret signatures will not verify in production.
 */

import { issueBaiParentShareCapability } from "../lib/ai-visibility/share/bai-signed-share-capability-v1.js";

function arg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

const parent = arg("parent") || "hilton";
const expires = arg("expires");
const tokenId = arg("token-id");
const label = arg("label") || `hilton-development-share:${parent}`;

process.env.BAI_SHARE_CAPABILITY_ALLOW_DEV_SECRET =
  process.env.BAI_SHARE_CAPABILITY_ALLOW_DEV_SECRET || "1";

const issued = issueBaiParentShareCapability({
  parentCompanyId: parent,
  label,
  expiresAt: expires || null,
  tokenId: tokenId || null,
});

console.log(
  JSON.stringify(
    {
      ok: true,
      tokenId: issued.tokenId,
      kind: issued.kind,
      parentCompanyId: issued.parentCompanyId,
      parentCompanyName: issued.parentCompanyName,
      allowedBrandIds: issued.allowedBrandIds,
      defaultBrandId: issued.defaultBrandId,
      sharePath: issued.sharePath,
      localUrlExample: `http://localhost:8080${issued.sharePath}`,
      expiresAt: issued.meta?.expiresAt || null,
      note: "Read-only Brand AI parent share. Exact prompts remain internal.",
    },
    null,
    2
  )
);
