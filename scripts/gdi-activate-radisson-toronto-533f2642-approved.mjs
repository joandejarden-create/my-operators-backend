#!/usr/bin/env node
/**
 * Activate founder-approved Radisson Blu Toronto staged package as CUSTOMER_PUBLISHED CURRENT.
 *
 * Exact fingerprint:
 *   533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d
 *
 * Default: dry-run. Apply only with:
 *   --apply
 *   --confirm-fingerprint=533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d
 *   and reports/.../FOUNDER_APPROVED_FOR_PUBLICATION.json (approved=true + exact phrase)
 *
 * Does not issue share tokens, write Airtable, or call paid research.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
  validateFounderApprovalReceipt,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { resolveShareOpportunitiesFromApprovedPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_segment_classification_remediation_v1";
const APPROVED_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const PRIOR_FP =
  "ac55190dd783fe6eb5f3ec60d65fd2ffdd9c7f3e3aba7eb35645197e96979c86";
const REQUIRED_PHRASE = `FOUNDER_APPROVED_FOR_PUBLICATION ${APPROVED_FP}`;
const OUT = path.join(ROOT, "reports/gdi-radisson-external-share-activation-v1");
const RECEIPT_PATH = path.join(OUT, "FOUNDER_APPROVED_FOR_PUBLICATION.json");

function arg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

const apply = process.argv.includes("--apply");
const confirmFp = String(arg("confirm-fingerprint") || "").toLowerCase();

fs.mkdirSync(OUT, { recursive: true });

const store = createGdiPublicationPackageStore();
const loaded = store.loadPackage(HOTEL_ID, VERSION);
if (!loaded.ok || !loaded.package) {
  console.error(JSON.stringify({ ok: false, error: "PACKAGE_LOAD_FAILED", loaded }, null, 2));
  process.exit(1);
}
const pkg = loaded.package;
const integrity = verifyPublicationPackageIntegrity(pkg);
const fpOk =
  integrity.ok &&
  String(pkg.contentFingerprintSha256).toLowerCase() === APPROVED_FP &&
  String(integrity.recomputedFingerprintSha256 || pkg.contentFingerprintSha256).toLowerCase() ===
    APPROVED_FP;

const inventory = {
  all: (pkg.opportunities || []).length,
  hotelMarket: (pkg.opportunities || []).filter((o) => o.hotelMarketScopeEligible === true)
    .length,
  cmi: (pkg.opportunities || []).filter((o) => o.competitiveMarketScopeEligible === true)
    .length,
};

let receiptDoc = null;
let receiptOk = false;
let receiptFails = ["receipt_file_missing"];
if (fs.existsSync(RECEIPT_PATH)) {
  receiptDoc = JSON.parse(fs.readFileSync(RECEIPT_PATH, "utf8"));
  const phraseOk =
    String(receiptDoc.explicitPhrase || "").trim() === REQUIRED_PHRASE &&
    receiptDoc.approved === true &&
    String(receiptDoc.contentFingerprintSha256 || "").toLowerCase() === APPROVED_FP;
  const validated = validateFounderApprovalReceipt(
    {
      approved: receiptDoc.approved === true,
      publicationId: receiptDoc.publicationId || pkg.publicationId,
      publicationVersion: receiptDoc.publicationVersion || VERSION,
      contentFingerprintSha256: receiptDoc.contentFingerprintSha256,
    },
    {
      publicationId: pkg.publicationId,
      publicationVersion: VERSION,
      contentFingerprintSha256: APPROVED_FP,
    },
  );
  receiptOk = phraseOk && validated.ok;
  receiptFails = [
    ...(phraseOk ? [] : ["explicit_phrase_or_approved_flag"]),
    ...(validated.fails || []),
  ];
}

const preflight = {
  ok: fpOk && inventory.all === 66 && inventory.hotelMarket === 50 && inventory.cmi === 16,
  apply,
  confirmFingerprintMatches: confirmFp === APPROVED_FP,
  fingerprint: APPROVED_FP,
  integrityOk: integrity.ok,
  inventory,
  priorFingerprintPreserved: PRIOR_FP,
  currentBefore: store.readPointer(HOTEL_ID, "CURRENT"),
  receiptPath: RECEIPT_PATH,
  receiptOk,
  receiptFails,
  requiredPhrase: REQUIRED_PHRASE,
};

fs.writeFileSync(
  path.join(OUT, "06-ACTIVATION_PREFLIGHT.json"),
  JSON.stringify(preflight, null, 2) + "\n",
);

if (!preflight.ok) {
  console.error(JSON.stringify({ ok: false, stage: "preflight", preflight }, null, 2));
  process.exit(1);
}

if (!apply) {
  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: "DRY_RUN",
        message:
          "Package ready. Founder approval receipt + --apply required to write CURRENT.",
        preflight,
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

if (confirmFp !== APPROVED_FP) {
  console.error(
    JSON.stringify({
      ok: false,
      error: "CONFIRM_FINGERPRINT_REQUIRED",
      expected: APPROVED_FP,
    }),
  );
  process.exit(1);
}

if (!receiptOk) {
  console.error(
    JSON.stringify({
      ok: false,
      error: "FOUNDER_APPROVAL_RECEIPT_REQUIRED",
      requiredPhrase: REQUIRED_PHRASE,
      receiptPath: RECEIPT_PATH,
      receiptFails,
    }),
  );
  process.exit(1);
}

const receipt = {
  approved: true,
  publicationId: pkg.publicationId,
  publicationVersion: VERSION,
  contentFingerprintSha256: APPROVED_FP,
  approvedAt: receiptDoc.approvedAt || new Date().toISOString(),
  approvedBy: receiptDoc.approvedBy || "founder",
  authority: receiptDoc.authority || "FOUNDER_EXPLICIT_APPROVAL_PHRASE",
  priorFingerprint: PRIOR_FP,
  explicitPhrase: REQUIRED_PHRASE,
  note: "Activate Radisson staged remediation package as CUSTOMER_PUBLISHED CURRENT",
};

const toPublish = {
  ...pkg,
  approvalStatus: "FOUNDER_APPROVED",
  approvedAt: receipt.approvedAt,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED,
  founderApprovalReceipt: receipt,
  gates: {
    ...(pkg.gates || {}),
    founder_approval_fingerprint_match: { ok: true, receipt },
    fingerprint_integrity: { ok: true, fingerprint: APPROVED_FP },
    inventory_parity: { ok: true, counts: inventory },
  },
};

const published = store.publishCustomerVisible(toPublish, {
  requireFounderApproval: true,
});
if (!published.ok) {
  console.error(JSON.stringify({ ok: false, published }, null, 2));
  process.exit(1);
}

const current = store.readPointer(HOTEL_ID, "CURRENT");
const share = resolveShareOpportunitiesFromApprovedPackage({ hotelId: HOTEL_ID });
const summary = {
  ok:
    current?.contentFingerprintSha256 === APPROVED_FP &&
    share.ok === true &&
    share.mode === "PACKAGE" &&
    share.meta?.contentFingerprintSha256 === APPROVED_FP &&
    (share.opportunities || []).length === 66,
  current,
  shareMode: share.mode,
  shareFingerprint: share.meta?.contentFingerprintSha256 || null,
  shareCount: share.opportunities?.length ?? null,
  next:
    "Commit CURRENT.json + receipt; merge; Railway deploy; railway run scripts/gdi-issue-share.mjs --hotelId=recH9h3McgMNFQEla",
};

fs.writeFileSync(
  path.join(OUT, "07-ACTIVATION_RESULT.json"),
  JSON.stringify(summary, null, 2) + "\n",
);
console.log(JSON.stringify(summary, null, 2));
process.exit(summary.ok ? 0 : 1);
