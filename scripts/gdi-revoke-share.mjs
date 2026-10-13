#!/usr/bin/env node
import { revokeGdiShareCapability } from "../lib/group-demand-intelligence/index.js";

const hit = process.argv.find((a) => a.startsWith("--token-id="));
const tokenId = hit ? hit.slice("--token-id=".length) : null;
if (!tokenId) {
  console.error("Usage: node scripts/gdi-revoke-share.mjs --token-id=gdisht_...");
  process.exit(1);
}
console.log(JSON.stringify(revokeGdiShareCapability(tokenId, "cli_revoke"), null, 2));
