/**
 * Offline gates for AVA durable storage + publication / auth posture.
 * Does not hit production Railway or Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  canTransitionAvaLifecycle,
  isCustomerFacingPublicationAllowed,
  isAuditValidated,
  isFounderApprovedForCustomer,
} from "../lib/ai-visibility-audit-action-plan/lifecycle-v1.js";
import {
  ensureAvaDurableStorage,
  resolveAvaStorageMode,
  RECOMMENDED_AVA_VOLUME_MOUNT,
} from "../lib/ai-visibility-audit-action-plan/resolve-ava-storage-root-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const checks = [];

function check(name, ok, detail) {
  checks.push({ name, ok: !!ok, detail: detail || null });
}

const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
check(
  "storage_status_route_uses_admin_auth",
  /ai-visibility-audit\/storage-status[\s\S]{0,120}adpMonthlyReviewAdminAuth/.test(
    server
  ) ||
    /storage-status[\s\S]{0,80}adpMonthlyReviewAdminAuth[\s\S]{0,80}getAdminAvaStorageStatus/.test(
      server
    ),
  "storage-status must sit behind adpMonthlyReviewAdminAuth"
);
check(
  "pdf_route_uses_admin_auth",
  /ai-visibility-audit\/:propertyId\/pdf[\s\S]{0,120}adpMonthlyReviewAdminAuth/.test(
    server
  ) || server.includes('"/api/admin/ai-visibility-audit/:propertyId/pdf"'),
  "PDF remains under admin auth (not public)"
);
check(
  "no_dev_auth_bypass_in_production_path",
  !/DEV_AUTH_BYPASS_EMAIL/.test(
    fs.readFileSync(
      path.join(ROOT, "api/admin-ai-visibility-audit.js"),
      "utf8"
    )
  ),
  "AVA admin API must not implement its own auth bypass"
);

check(
  "audit_validated_not_customer_ready",
  isAuditValidated("AUDIT_VALIDATED") === true &&
    isFounderApprovedForCustomer("AUDIT_VALIDATED") === false,
  "AUDIT_VALIDATED ≠ founder/customer approval"
);
check(
  "publication_requires_explicit_flags",
  isCustomerFacingPublicationAllowed("CUSTOMER_READY", {
    founderPublicationAuthorized: false,
    customerPublicationAuthorized: false,
    founderApprovedForCustomer: true,
  }) === false,
  "CUSTOMER_READY blocked without explicit publication authorizations"
);
check(
  "validated_cannot_skip_to_published",
  canTransitionAvaLifecycle("AUDIT_VALIDATED", "PUBLISHED") === false ||
    isCustomerFacingPublicationAllowed("PUBLISHED", {
      founderPublicationAuthorized: true,
      customerPublicationAuthorized: true,
      founderApprovedForCustomer: false,
    }) === false,
  "Publication still gated"
);

process.env.AVA_STORAGE_MODE = "local";
process.env.AVA_STORAGE_ROOT = path.join(ROOT, "data/ava-data");
const local = ensureAvaDurableStorage();
check("local_durable_writable", local.ok === true && local.writable === true, local.root);

process.env.AVA_STORAGE_MODE = "required";
process.env.AVA_STORAGE_ROOT = path.join(ROOT, "tmp/ava-not-a-dir-file");
fs.mkdirSync(path.dirname(process.env.AVA_STORAGE_ROOT), { recursive: true });
fs.writeFileSync(process.env.AVA_STORAGE_ROOT, "file");
const bad = ensureAvaDurableStorage();
check(
  "required_mode_fails_closed_on_bad_root",
  bad.ok === false && bad.error === "AVA_STORAGE_UNAVAILABLE",
  bad.message
);

check(
  "recommended_mount_documented",
  RECOMMENDED_AVA_VOLUME_MOUNT === "/app/ava-data",
  RECOMMENDED_AVA_VOLUME_MOUNT
);

const failed = checks.filter((c) => !c.ok);
const out = {
  ok: failed.length === 0,
  modeSample: resolveAvaStorageMode({ AVA_STORAGE_MODE: "required" }),
  checks,
  failed: failed.map((c) => c.name),
};
const report = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_DURABLE_STORAGE_GATES.json"
);
fs.mkdirSync(path.dirname(report), { recursive: true });
fs.writeFileSync(report, `${JSON.stringify(out, null, 2)}\n`);
console.log(JSON.stringify({ ok: out.ok, failed: out.failed, report }, null, 2));
process.exit(out.ok ? 0 : 1);
