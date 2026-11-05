/**
 * Staged AI Admin AVA workflow smoke test (no auth HTTP — library-level).
 * Does not publish or deploy.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { loadAdpBaselineForAudit, listAdpPropertyIdsWithStagedReports } from "../lib/ai-visibility-audit-action-plan/adp-baseline-loader-v1.js";
import { listStoredAudits, getAuditMeta, getLatestAuditVersion } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { canTransitionAvaLifecycle, isCustomerFacingPublicationAllowed } from "../lib/ai-visibility-audit-action-plan/lifecycle-v1.js";
import { assertAuditPdfPayload } from "../lib/ai-visibility-audit-action-plan/audit-pdf-contract-v1.js";
import { resolveAvaStorageRoot } from "../lib/ai-visibility-audit-action-plan/resolve-ava-storage-root-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
const AVA_ROOT = resolveAvaStorageRoot();
const REQUIRED = [
  "adp_bethesda_marriott",
  "adp_waterstone_boca_raton",
  "adp_castillo_hotel_son_vida",
  "adp_sheraton_mallorca_arabella_golf",
  "adp_wyndham_san_diego_bayside",
  "adp_spice_island_beach_resort",
];

const results = { ok: true, checks: [] };
function check(name, pass, detail) {
  results.checks.push({ name, pass: !!pass, detail: detail || null });
  if (!pass) results.ok = false;
}

const staged = listAdpPropertyIdsWithStagedReports();
check("bethesda_in_eligible_universe", staged.includes("adp_bethesda_marriott"));
check("all_required_have_staged_adp", REQUIRED.every((id) => staged.includes(id)), REQUIRED.filter((id) => !staged.includes(id)));

for (const id of REQUIRED) {
  const baseline = loadAdpBaselineForAudit(id);
  check(`${id}_baseline`, baseline.ok, baseline.error);
  const meta = getAuditMeta(id);
  check(`${id}_store_meta`, !!meta, "missing meta");
  const latest = getLatestAuditVersion(id);
  check(`${id}_version`, !!latest?.payload, "missing version payload");
  if (latest?.payload) {
    const gate = assertAuditPdfPayload(latest.payload);
    check(`${id}_payload_contract`, gate.ok, gate.errors);
  }
  if (latest?.pdfRelativePath) {
    const candidates = [
      path.join(AVA_ROOT, latest.pdfRelativePath),
      path.join(ROOT, latest.pdfRelativePath),
      path.join(ROOT, "data/ava-data", latest.pdfRelativePath),
    ];
    const hit = candidates.find((p) => fs.existsSync(p));
    check(`${id}_pdf_exists`, Boolean(hit), latest.pdfRelativePath);
  }
  check(
    `${id}_not_customer_ready_auto`,
    (meta?.customerReadinessStatus || "NOT_CUSTOMER_READY") === "NOT_CUSTOMER_READY"
  );
}

check(
  "pdf_success_not_customer_ready",
  !isCustomerFacingPublicationAllowed("CUSTOMER_READY", {
    founderPublicationAuthorized: false,
    customerPublicationAuthorized: false,
  })
);
check(
  "lifecycle_blocks_pdf_to_published",
  !canTransitionAvaLifecycle("PDF_STAGED", "PUBLISHED")
);
check(
  "lifecycle_allows_validation_to_founder_reviewed",
  canTransitionAvaLifecycle("VALIDATION_REQUIRED", "FOUNDER_REVIEWED_STAGED")
);

const out = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/PHASE7_AI_ADMIN_WORKFLOW_TEST.json"
);
fs.writeFileSync(out, JSON.stringify({ ...results, testedAt: new Date().toISOString() }, null, 2));
console.log(JSON.stringify({ ok: results.ok, failed: results.checks.filter((c) => !c.pass), out }, null, 2));
if (!results.ok) process.exit(1);
