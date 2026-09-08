#!/usr/bin/env node
/**
 * Issue ADP signed share capability tokens (Existing Hotel ADP).
 *
 * Usage:
 *   node scripts/issue-adp-share-capability-v1.mjs --property adp_jw_marriott_monterrey_valle
 *   node scripts/issue-adp-share-capability-v1.mjs --properties adp_jw_marriott_monterrey_valle,adp_westin_monterrey_valle
 *   node scripts/issue-adp-share-capability-v1.mjs --monterrey-valle
 *
 * Requires ADP_SHARE_CAPABILITY_SECRET (min 32 chars), or
 * ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 for local-only tokens.
 */

import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { join } from "path";
import { issueShareCapability } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { MONTERREY_VALLE_PROPERTY_IDS } from "../lib/ai-demand-positioning/execution/monterrey-valle-baseline-period-001-v1.js";
import { CALA_SIX_PROPERTY_IDS } from "../lib/ai-demand-positioning/execution/cala-six-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const publicBase =
  args.find((a) => a.startsWith("--public-base="))?.slice("--public-base=".length) ||
  process.env.ADP_PUBLIC_BASE_URL ||
  "https://my-operators-backend-production.up.railway.app";

let propertyIds = [];
if (args.includes("--monterrey-valle")) {
  propertyIds = [...MONTERREY_VALLE_PROPERTY_IDS];
}
if (args.includes("--cala-six")) {
  propertyIds.push(...CALA_SIX_PROPERTY_IDS);
}
const propIdx = args.indexOf("--property");
if (propIdx >= 0 && args[propIdx + 1]) propertyIds.push(args[propIdx + 1]);
const propEq = args.find((a) => a.startsWith("--property="));
if (propEq) propertyIds.push(propEq.slice("--property=".length));
const propsEq = args.find((a) => a.startsWith("--properties="));
if (propsEq) {
  propertyIds.push(
    ...propsEq
      .slice("--properties=".length)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  );
}

propertyIds = [...new Set(propertyIds)];
if (!propertyIds.length) {
  console.error("Provide --property, --properties, --monterrey-valle, or --cala-six");
  process.exit(1);
}

const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const links = [];

for (const propertyId of propertyIds) {
  const man = loadPublishedManifest(propertyId);
  if (!man?.latestPeriodId) {
    console.error(`No published snapshot for ${propertyId} — refuse share issue`);
    process.exit(1);
  }
  const issued = issueShareCapability({
    propertyId,
    label: `production-distribution:${propertyId}:${stamp}`,
  });
  const sharePath = issued.sharePath;
  const shareUrl = `${String(publicBase).replace(/\/$/, "")}${sharePath}`;
  links.push({
    propertyId,
    tokenId: issued.tokenId,
    latestPeriodId: man.latestPeriodId,
    sharePath,
    shareUrl,
    token: issued.token,
  });
}

const cohortTag = args.includes("--cala-six")
  ? "cala-six"
  : args.includes("--monterrey-valle")
    ? "monterrey-valle"
    : "ad-hoc";

const inventory = {
  warning: "LOCAL ONLY — share tokens grant report access. Do not commit production tokens.",
  issuedAt: new Date().toISOString(),
  publicBase,
  cohortTag,
  links,
};

const localDir = join(process.cwd(), "data/ai-demand-positioning/share-registry");
mkdirSync(localDir, { recursive: true });
const localPath = join(localDir, `issued-share-urls.${cohortTag}.${stamp}.json`);
writeFileSync(localPath, JSON.stringify(inventory, null, 2) + "\n");

const reportDir = join(process.cwd(), "reports/client-share-links");
mkdirSync(reportDir, { recursive: true });
const reportPath = join(
  reportDir,
  `${cohortTag.toUpperCase().replace(/-/g, "_")}_CLIENT_SHARE_LINKS_${stamp}.json`
);
writeFileSync(reportPath, JSON.stringify(inventory, null, 2) + "\n");

// Merge into active local inventory if present (append/replace by propertyId)
const activeLocal = join(localDir, "issued-share-urls.local.json");
if (existsSync(activeLocal)) {
  try {
    const prev = JSON.parse(readFileSync(activeLocal, "utf8"));
    const byId = new Map((prev.links || []).map((l) => [l.propertyId, l]));
    for (const link of links) byId.set(link.propertyId, link);
    const merged = {
      ...prev,
      issuedAt: inventory.issuedAt,
      publicBase,
      links: [...byId.values()],
    };
    writeFileSync(activeLocal, JSON.stringify(merged, null, 2) + "\n");
  } catch (err) {
    console.error("[issue-adp-share] could not merge active local inventory:", err.message);
  }
} else {
  writeFileSync(activeLocal, JSON.stringify(inventory, null, 2) + "\n");
}

console.log(
  JSON.stringify(
    {
      ok: true,
      count: links.length,
      publicBase,
      inventoryPath: localPath,
      reportPath,
      links: links.map((l) => ({
        propertyId: l.propertyId,
        tokenId: l.tokenId,
        shareUrl: l.shareUrl,
      })),
    },
    null,
    2
  )
);
// Print full URLs for founder copy (stdout only — not omitted)
for (const l of links) {
  console.log(`\n${l.propertyId}\n${l.shareUrl}`);
}
