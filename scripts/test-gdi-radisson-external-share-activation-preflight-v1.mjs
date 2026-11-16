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
import { assertPackageEssentialProseCompleteness } from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_drawer_truncation_remediation_v1";
const FP =
  "f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d";
const PRIOR_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const REQUIRED_PHRASE = `FOUNDER_APPROVED_FOR_PUBLICATION ${FP}`;
const REPORT = path.join(
  ROOT,
  "reports/gdi-radisson-production-drawer-content-remediation-v1",
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

const prose = assertPackageEssentialProseCompleteness(pkg);
if (!prose.ok) fails.push("prose_truncations");

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

const prior = store.loadPackage(
  HOTEL_ID,
  "gdi_pub_recH9h3McgMNFQEla_20261116_segment_classification_remediation_v1",
);
if (!prior.ok) fails.push("prior_package_missing");
if (String(prior.package?.contentFingerprintSha256 || "").toLowerCase() !== PRIOR_FP) {
  fails.push("prior_fingerprint_drift");
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
  if (!hit) fails.push("approval_ledger_missing_f2bfebc9");
}

const result = {
  ok: fails.length === 0,
  fingerprint: FP,
  inventory,
  fails,
};
console.log(JSON.stringify(result, null, 2));
process.exit(result.ok ? 0 : 1);
