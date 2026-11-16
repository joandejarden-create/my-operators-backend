#!/usr/bin/env node
/**
 * Read-only gate: Radisson staged package is ready; CURRENT and founder approval remain absent.
 * Does not publish, issue tokens, or write Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
} from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_segment_classification_remediation_v1";
const FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const REPORT = path.join(
  ROOT,
  "reports/gdi-radisson-external-share-activation-v1",
);

const fails = [];
const store = createGdiPublicationPackageStore();
const loaded = store.loadPackage(HOTEL_ID, VERSION);
if (!loaded.ok) fails.push("package_load");
const pkg = loaded.package;
const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) fails.push("integrity");
if (String(pkg?.contentFingerprintSha256 || "").toLowerCase() !== FP) {
  fails.push("fingerprint_mismatch");
}
const ops = pkg?.opportunities || [];
const inventory = {
  all: ops.length,
  hotelMarket: ops.filter((o) => o.hotelMarketScopeEligible === true).length,
  cmi: ops.filter((o) => o.competitiveMarketScopeEligible === true).length,
};
if (inventory.all !== 66 || inventory.hotelMarket !== 50 || inventory.cmi !== 16) {
  fails.push("inventory");
}
const current = store.readPointer(HOTEL_ID, "CURRENT");
if (current !== null) fails.push("unexpected_current_pointer");
const ledgerExists = fs.existsSync(
  path.join(
    ROOT,
    "data/group-demand-intelligence/gdi-founder-approval-v1/approval-ledger-v1.json",
  ),
);
if (ledgerExists) fails.push("unexpected_approval_ledger");
const receiptLive = path.join(REPORT, "FOUNDER_APPROVED_FOR_PUBLICATION.json");
if (fs.existsSync(receiptLive)) fails.push("live_approval_receipt_present");
const scriptExists = fs.existsSync(
  path.join(ROOT, "scripts/gdi-activate-radisson-toronto-533f2642-approved.mjs"),
);
if (!scriptExists) fails.push("activation_script_missing");
for (const name of [
  "FOUNDER_REPORT.md",
  "01-ROOT_CAUSE.json",
  "02-FOUNDER_APPROVAL_STATUS.json",
  "03-PRODUCTION_PACKAGE_PROBE.json",
  "04-PREPARED_ACTIVATION_PLAN.json",
  "05-PRODUCTION_BROWSER_VERIFICATION.json",
  "FOUNDER_APPROVED_FOR_PUBLICATION.template.json",
]) {
  if (!fs.existsSync(path.join(REPORT, name))) fails.push(`report_missing:${name}`);
}

const out = {
  ok: fails.length === 0,
  fails,
  fingerprint: FP,
  inventory,
  current,
  founderApproval: "ABSENT",
  activationBlocked: true,
};
fs.mkdirSync(REPORT, { recursive: true });
fs.writeFileSync(
  path.join(REPORT, "08-PREFLIGHT_GATE.json"),
  JSON.stringify(out, null, 2) + "\n",
);
console.log(JSON.stringify(out, null, 2));
process.exit(out.ok ? 0 : 1);
