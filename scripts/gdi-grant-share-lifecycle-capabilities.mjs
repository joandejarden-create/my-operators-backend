#!/usr/bin/env node
/**
 * Grant customer lifecycle write capabilities to an existing GDI share tokenId
 * WITHOUT reissuing or changing the signed URL.
 *
 * Default: Bethesda Rad-preserved token gdisht_47c25d74c79216021fb36150
 *
 *   node scripts/gdi-grant-share-lifecycle-capabilities.mjs --dry-run
 *   node scripts/gdi-grant-share-lifecycle-capabilities.mjs --apply
 */

import {
  grantGdiShareCapabilitiesByTokenId,
  GDI_SHARE_CAPABILITY,
  readGdiShareRegistry,
} from "../lib/group-demand-intelligence/index.js";

const BETHESDA_TOKEN = "gdisht_47c25d74c79216021fb36150";

const LIFECYCLE_CAPS = [
  GDI_SHARE_CAPABILITY.CAN_READ_BRIEF,
  GDI_SHARE_CAPABILITY.CAN_READ_OPPORTUNITIES,
  GDI_SHARE_CAPABILITY.CAN_READ_OPPORTUNITY_DETAIL,
  GDI_SHARE_CAPABILITY.CAN_READ_SUMMARY,
  GDI_SHARE_CAPABILITY.CAN_VALIDATE,
  GDI_SHARE_CAPABILITY.CAN_RECORD_ACTION,
  GDI_SHARE_CAPABILITY.CAN_RECORD_OUTCOME,
];

function parseArgs(argv) {
  const apply = argv.includes("--apply");
  const tokenId =
    (argv.find((a) => a.startsWith("--tokenId=")) || "").slice("--tokenId=".length) ||
    BETHESDA_TOKEN;
  return { apply, tokenId, dryRun: !apply };
}

const { apply, tokenId, dryRun } = parseArgs(process.argv.slice(2));
const reg = readGdiShareRegistry();
const before = reg.tokens?.[tokenId] || null;

console.log(
  JSON.stringify(
    {
      tokenId,
      dryRun,
      apply,
      beforeStatus: before?.status || null,
      beforeCapabilities: before?.capabilities || null,
      grant: LIFECYCLE_CAPS,
    },
    null,
    2
  )
);

if (dryRun) {
  console.log("\nDRY RUN — re-run with --apply to write registry capabilities.");
  process.exit(0);
}

const result = grantGdiShareCapabilitiesByTokenId(tokenId, LIFECYCLE_CAPS, {
  merge: true,
  reason: "bethesda_customer_lifecycle_parity_20260917",
});

if (!result.ok) {
  console.error(result);
  process.exit(1);
}

console.log(JSON.stringify({ ok: true, ...result }, null, 2));
