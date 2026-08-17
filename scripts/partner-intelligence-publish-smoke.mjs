/**
 * Smoke: approve one fact + publish + verify overlay keys.
 * Usage: node scripts/partner-intelligence-publish-smoke.mjs --apply [--fact rec…]
 */
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "..", ".env.local") });
dotenv.config({ path: path.join(__dirname, "..", ".env") });

const apply = process.argv.includes("--apply");
const factArg = process.argv.find((a) => a.startsWith("--fact="));
const factId = factArg ? factArg.split("=")[1] : "recyLCMW5xem5TdzK";
const sourceId = "recM4HuV9r5Gz35P7";
const operatorId = "recF5Z87OAqFgndoq";

process.env.PARTNER_INTELLIGENCE_PUBLISH_ENABLED = "1";
process.env.PARTNER_INTELLIGENCE_PUBLISH_OVERLAY = "1";

const { MAP_PARTNER_FACT, MAP_PARTNER_SOURCE } = await import(
  "../api/lib/partner-intelligence-field-map.js"
);
const { patchPartnerFact } = await import("../lib/partner-intelligence/airtable-facts.js");
const { patchPartnerSource, getPartnerSourceById } = await import(
  "../lib/partner-intelligence/airtable-source.js"
);
const { publishApprovedFact, applyPartnerIntelligenceOperatorOverlay } = await import(
  "../lib/partner-intelligence/publish-overlay.js"
);

if (!apply) {
  console.log("Dry run — pass --apply to approve, patch source, publish.");
  process.exit(0);
}

await patchPartnerSource(sourceId, {
  [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "Yes",
  [MAP_PARTNER_SOURCE.sourceQuality]: "Medium",
  [MAP_PARTNER_SOURCE.status]: "Captured",
});

const fact = await patchPartnerFact(factId, {
  [MAP_PARTNER_FACT.humanReviewStatus]: "Approved",
  [MAP_PARTNER_FACT.approvedValue]: "Arbor Lodging",
  [MAP_PARTNER_FACT.publicVisibility]: "Public",
});

console.log("Approved fact:", fact.id, fact.fieldName);

const pub = await publishApprovedFact(factId);
console.log("Published:", pub.published.id, pub.published.fieldName, pub.published.approvedValue);

const prefill = {};
const overlay = await applyPartnerIntelligenceOperatorOverlay(prefill, operatorId);
console.log("Overlay applied:", overlay.applied, overlay.fields);
console.log("Prefill companyName:", prefill.companyName);
