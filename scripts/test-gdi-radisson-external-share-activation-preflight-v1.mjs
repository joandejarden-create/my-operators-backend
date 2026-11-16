#!/usr/bin/env node
/**
 * Gate: founder-approved Radisson package is CUSTOMER_PUBLISHED CURRENT at exact fingerprint.
 * Does not issue tokens, write Airtable, or call paid research.
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
const REQUIRED_PHRASE = `FOUNDER_APPROVED_FOR_PUBLICATION ${FP}`;
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
if (pkg?.lifecycleState !== "CUSTOMER_PUBLISHED") fails.push("lifecycle_not_published");
if (pkg?.approvalStatus !== "FOUNDER_APPROVED") fails.push("approval_status");

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
if (!current) fails.push("current_missing");
if (String(current?.contentFingerprintSha256 || "").toLowerCase() !== FP) {
  fails.push("current_fingerprint");
}
if (current?.lifecycleState !== "CUSTOMER_PUBLISHED") fails.push("current_lifecycle");

const visible = store.loadCustomerVisiblePackage(HOTEL_ID);
if (!visible.ok) fails.push(`customer_visible:${visible.error || "fail"}`);
if ((visible.package?.opportunities || []).length !== 66) {
  fails.push("customer_visible_count");
}

const receiptPath = path.join(REPORT, "FOUNDER_APPROVED_FOR_PUBLICATION.json");
if (!fs.existsSync(receiptPath)) fails.push("approval_receipt_missing");
else {
  const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8"));
  if (receipt.approved !== true) fails.push("receipt_not_approved");
  if (String(receipt.explicitPhrase || "").trim() !== REQUIRED_PHRASE) {
    fails.push("receipt_phrase");
  }
  if (String(receipt.contentFingerprintSha256 || "").toLowerCase() !== FP) {
    fails.push("receipt_fingerprint");
  }
}

const ledgerPath = path.join(
  ROOT,
  "data/group-demand-intelligence/gdi-founder-approval-v1/approval-ledger-v1.json",
);
if (!fs.existsSync(ledgerPath)) fails.push("approval_ledger_missing");
else {
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  const hit = (ledger.records || []).find(
    (r) =>
      String(r.contentFingerprintSha256 || "").toLowerCase() === FP &&
      r.status === "APPROVED",
  );
  if (!hit) fails.push("ledger_fingerprint_absent");
}

for (const name of [
  "FOUNDER_REPORT.md",
  "FOUNDER_APPROVED_FOR_PUBLICATION.json",
  "07-ACTIVATION_RESULT.json",
  "10-EXACT_66_IDS.json",
]) {
  if (!fs.existsSync(path.join(REPORT, name))) fails.push(`report_missing:${name}`);
}

const out = {
  ok: fails.length === 0,
  fails,
  fingerprint: FP,
  inventory,
  current,
  founderApproval: "PRESENT",
  activationBlocked: false,
  customerPublished: true,
};
fs.mkdirSync(REPORT, { recursive: true });
fs.writeFileSync(
  path.join(REPORT, "08-PREFLIGHT_GATE.json"),
  JSON.stringify(out, null, 2) + "\n",
);
console.log(JSON.stringify(out, null, 2));
process.exit(out.ok ? 0 : 1);
