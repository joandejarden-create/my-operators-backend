#!/usr/bin/env node
/**
 * Activate founder-approved YOTEL Geneva Lake staged package as CUSTOMER_PUBLISHED CURRENT.
 *
 * Exact fingerprint:
 *   353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3
 *
 * Default: dry-run. Apply only with:
 *   --apply
 *   --confirm-fingerprint=353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3
 *   and reports/.../FOUNDER_APPROVED_FOR_PUBLICATION.json (approved=true + exact phrase)
 *
 * Does not issue share tokens, write Airtable, call paid research, or mutate prior package
 * 41a1b1ca0c34eb5da4b2569db499743ef1721f52376d141c379677e4cd6c054f.
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
import { assertPackageEssentialProseCompleteness } from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  summarizeCertifiedDrawerAudit,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";
import { assertLocalExternalCustomerLinkActivationGate } from "../lib/group-demand-intelligence/share/gdi-local-external-customer-link-activation-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recrPQcZg7SFARRb2";
const VERSION =
  "gdi_pub_recrPQcZg7SFARRb2_20261117_yotel_geneva_lake_exa_30usd_scale_v1";
const APPROVED_FP =
  "353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3";
const PRIOR_PACKAGE_VERSION =
  "gdi_pub_recrPQcZg7SFARRb2_20261116_yotel_geneva_lake_commercial_quality_40plus_readiness_v1";
const PRIOR_PACKAGE_FP =
  "41a1b1ca0c34eb5da4b2569db499743ef1721f52376d141c379677e4cd6c054f";
const REQUIRED_PHRASE = `FOUNDER_APPROVED_FOR_PUBLICATION ${APPROVED_FP}`;
const OUT = path.join(
  ROOT,
  "reports/gdi-yotel-geneva-external-client-publication-parity-v1",
);
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
  console.error(
    JSON.stringify({ ok: false, error: "PACKAGE_LOAD_FAILED", loaded }, null, 2),
  );
  process.exit(1);
}
const pkg = loaded.package;
const integrity = verifyPublicationPackageIntegrity(pkg);
const fpOk =
  integrity.ok &&
  String(pkg.contentFingerprintSha256).toLowerCase() === APPROVED_FP &&
  String(
    integrity.recomputedFingerprintSha256 || pkg.contentFingerprintSha256,
  ).toLowerCase() === APPROVED_FP;

const inventory = {
  all: (pkg.opportunities || []).length,
  hotelMarket: (pkg.opportunities || []).filter(
    (o) => o.hotelMarketScopeEligible === true,
  ).length,
  cmi: (pkg.opportunities || []).filter(
    (o) => o.competitiveMarketScopeEligible === true,
  ).length,
  uniqueIds: new Set(
    (pkg.opportunities || []).map((o) => o.opportunityId || o.id).filter(Boolean),
  ).size,
};

const prose = assertPackageEssentialProseCompleteness(pkg);
const certified = summarizeCertifiedDrawerAudit(
  (pkg.opportunities || []).map((o) =>
    evaluateCertifiedDrawerCommercialContent(o, { hotelId: HOTEL_ID }),
  ),
);

const priorPkg = store.loadPackage(HOTEL_ID, PRIOR_PACKAGE_VERSION);
const parityGate = assertLocalExternalCustomerLinkActivationGate(HOTEL_ID, {
  expectedFingerprint: APPROVED_FP,
  expectedCounts: { all: 42, hotelMarket: 34, cmi: 8 },
});

let receiptDoc = null;
let receiptOk = false;
let receiptFails = ["receipt_file_missing_or_not_approved"];
if (fs.existsSync(RECEIPT_PATH)) {
  receiptDoc = JSON.parse(fs.readFileSync(RECEIPT_PATH, "utf8"));
  const phraseOk =
    String(receiptDoc.explicitPhrase || "").trim() === REQUIRED_PHRASE &&
    receiptDoc.approved === true &&
    String(receiptDoc.contentFingerprintSha256 || "").toLowerCase() ===
      APPROVED_FP;
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
  ok:
    fpOk &&
    inventory.all === 42 &&
    inventory.hotelMarket === 34 &&
    inventory.cmi === 8 &&
    inventory.uniqueIds === 42 &&
    prose.ok === true &&
    certified.fullyPassing === 42 &&
    certified.withTruncatedText === 0 &&
    priorPkg.ok === true &&
    String(priorPkg.package.contentFingerprintSha256).toLowerCase() ===
      PRIOR_PACKAGE_FP &&
    parityGate.ok === true,
  apply,
  confirmFingerprintMatches: confirmFp === APPROVED_FP,
  fingerprint: APPROVED_FP,
  integrityOk: integrity.ok,
  inventory,
  proseOk: prose.ok,
  certified,
  priorPackageFingerprintPreservedImmutable: PRIOR_PACKAGE_FP,
  currentBefore: store.readPointer(HOTEL_ID, "CURRENT"),
  stagedBefore: store.readPointer(HOTEL_ID, "STAGED"),
  parityGate,
  receiptPath: RECEIPT_PATH,
  receiptOk,
  receiptFails,
  requiredPhrase: REQUIRED_PHRASE,
  blocker:
    receiptOk
      ? null
      : "WAITING_FOUNDER_APPROVED_FOR_PUBLICATION — supply exact phrase then set receipt approved=true",
};

fs.writeFileSync(
  path.join(OUT, "12-ACTIVATION_PREFLIGHT.json"),
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
  priorFingerprint: PRIOR_PACKAGE_FP,
  explicitPhrase: REQUIRED_PHRASE,
  note: "Activate YOTEL Geneva Lake $30 Exa scale package as CUSTOMER_PUBLISHED CURRENT",
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
    prose_completeness: { ok: true },
    certified_drawers: { ok: true, fullyPassing: 42 },
    local_external_customer_link_activation: { ok: true, gate: parityGate },
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
const shareProse = assertPackageEssentialProseCompleteness({
  opportunities: share.opportunities || [],
});
const summary = {
  ok:
    current?.contentFingerprintSha256 === APPROVED_FP &&
    current?.lifecycleState === "CUSTOMER_PUBLISHED" &&
    share.ok === true &&
    share.mode === "PACKAGE" &&
    share.meta?.contentFingerprintSha256 === APPROVED_FP &&
    (share.opportunities || []).length === 42 &&
    shareProse.ok === true &&
    priorPkg.ok === true &&
    String(priorPkg.package.contentFingerprintSha256).toLowerCase() ===
      PRIOR_PACKAGE_FP,
  current,
  shareMode: share.mode,
  shareFingerprint: share.meta?.contentFingerprintSha256 || null,
  shareCount: share.opportunities?.length ?? null,
  shareProseOk: shareProse.ok,
  priorPackageBytesPreserved: priorPkg.ok,
  next: "Commit CURRENT.json + receipt; merge; Railway deploy; railway run gdi:issue-share; browser-verify production share",
};

fs.writeFileSync(
  path.join(OUT, "13-ACTIVATION_RESULT.json"),
  JSON.stringify(summary, null, 2) + "\n",
);
console.log(JSON.stringify(summary, null, 2));
process.exit(summary.ok ? 0 : 1);
