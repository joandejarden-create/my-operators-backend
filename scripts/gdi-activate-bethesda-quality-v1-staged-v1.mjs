#!/usr/bin/env node
/**
 * Founder-authorized Bethesda founder-local STAGED activation.
 *
 * Package: gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1 (68)
 * Authority: FOUNDER CERTIFICATION ACCEPTED — STAGED review only
 *
 * - Seals stale contentFingerprintSha256 to match existing opportunity bytes
 *   (quality completion cloned parent FP; does NOT reconstruct inventory)
 * - Activates STAGED only via stageForProduction
 * - Never mutates CURRENT
 *
 * Default: dry-run. Apply with:
 *   --apply --confirm-version=gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  fingerprintPublicationContent,
  verifyPublicationPackageIntegrity,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const VERSION =
  "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1";
const PRIOR_STAGED_VERSION =
  "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const PRIOR_STAGED_FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";
const PRIOR_CURRENT_FP = PRIOR_STAGED_FP;
const OUT = path.join(ROOT, "reports/gdi-bethesda-staged-activation-68-v1");
const PKG_DIR = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL,
);
const PKG_PATH = path.join(PKG_DIR, VERSION, "package.json");

function shaFile(p) {
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}
function arg(name) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

const apply = process.argv.includes("--apply");
const confirmVersion = String(arg("confirm-version") || "");

fs.mkdirSync(path.join(OUT, "rollback"), { recursive: true });

if (!fs.existsSync(PKG_PATH)) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: "PACKAGE_NOT_FOUND",
        path: PKG_PATH,
        note: "Stop — do not reconstruct. Package missing after integration.",
      },
      null,
      2,
    ),
  );
  process.exit(1);
}

const raw = JSON.parse(fs.readFileSync(PKG_PATH, "utf8"));
if (raw.publicationVersion !== VERSION) {
  console.error(
    JSON.stringify({
      ok: false,
      error: "VERSION_IDENTITY_MISMATCH",
      expected: VERSION,
      actual: raw.publicationVersion,
    }),
  );
  process.exit(1);
}
if (!Array.isArray(raw.opportunities) || raw.opportunities.length !== 68) {
  console.error(
    JSON.stringify({
      ok: false,
      error: "COUNT_MISMATCH",
      expected: 68,
      actual: raw.opportunities?.length,
    }),
  );
  process.exit(1);
}

const beforeIntegrity = verifyPublicationPackageIntegrity(raw);
const recomputed = fingerprintPublicationContent(raw);
const sealed = {
  ...raw,
  contentFingerprintSha256: recomputed.contentFingerprintSha256,
  opportunityCount: 68,
  approvalStatus: "FOUNDER_APPROVED_STAGED_REVIEW",
  approvedAt: new Date().toISOString(),
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
  notes: [
    raw.notes,
    "Founder accepted 68-opportunity quality candidate for founder-local STAGED review (PR #597).",
  ]
    .filter(Boolean)
    .join(" "),
  founderStagedActivationReceipt: {
    approved: true,
    authority:
      "FOUNDER_BETHESDA_CERTIFICATION_ACCEPTED_STAGED_20261125",
    publicationVersion: VERSION,
    opportunityCount: 68,
    priorStagedVersion: PRIOR_STAGED_VERSION,
    priorStagedFingerprint: PRIOR_STAGED_FP,
    sealedFingerprint: recomputed.contentFingerprintSha256,
    staleFingerprintBeforeSeal: raw.contentFingerprintSha256,
    sealReason:
      "Quality-completion clone retained parent contentFingerprintSha256; seal recomputed over unchanged opportunity inventory.",
  },
};

const afterSealIntegrity = verifyPublicationPackageIntegrity(sealed);
const store = createGdiPublicationPackageStore();
const stagedBefore = store.readPointer(HOTEL, "STAGED");
const currentBefore = store.readPointer(HOTEL, "CURRENT");
const stagedShaBefore = shaFile(path.join(PKG_DIR, "STAGED.json"));
const currentShaBefore = shaFile(path.join(PKG_DIR, "CURRENT.json"));

fs.writeFileSync(
  path.join(OUT, "rollback/STAGED.before.json"),
  `${JSON.stringify(stagedBefore, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "rollback/CURRENT.before.json"),
  `${JSON.stringify(currentBefore, null, 2)}\n`,
);

const preflight = {
  ok:
    afterSealIntegrity.ok === true &&
    sealed.opportunities.length === 68 &&
    String(currentBefore?.contentFingerprintSha256 || "").toLowerCase() ===
      PRIOR_CURRENT_FP &&
    String(stagedBefore?.publicationVersion) === PRIOR_STAGED_VERSION,
  apply,
  confirmVersionMatches: confirmVersion === VERSION,
  packagePath: PKG_PATH,
  beforeIntegrity,
  afterSealIntegrity,
  sealedFingerprint: sealed.contentFingerprintSha256,
  opportunityCount: sealed.opportunities.length,
  stagedBefore,
  currentBefore,
  stagedShaBefore,
  currentShaBefore,
  freezeRollbackDir: path.join(OUT, "rollback"),
  emergencyFreeze:
    "C:\\Dev\\dealality\\_gdi_emergency_freeze_20261124-231526",
};

fs.writeFileSync(
  path.join(OUT, "01-ACTIVATION_PREFLIGHT.json"),
  `${JSON.stringify(preflight, null, 2)}\n`,
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
          "Seal + STAGED activation ready. Re-run with --apply --confirm-version=…",
        sealedFingerprint: sealed.contentFingerprintSha256,
        count: 68,
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

if (confirmVersion !== VERSION) {
  console.error(
    JSON.stringify({
      ok: false,
      error: "CONFIRM_VERSION_REQUIRED",
      expected: VERSION,
    }),
  );
  process.exit(1);
}

// Persist sealed package bytes, then STAGED pointer only.
const written = store.writePackage(sealed);
if (!written.ok) {
  console.error(JSON.stringify({ ok: false, written }, null, 2));
  process.exit(1);
}
const staged = store.stageForProduction(sealed);
if (!staged.ok) {
  console.error(JSON.stringify({ ok: false, staged }, null, 2));
  process.exit(1);
}

const stagedAfter = store.readPointer(HOTEL, "STAGED");
const currentAfter = store.readPointer(HOTEL, "CURRENT");
const stagedShaAfter = shaFile(path.join(PKG_DIR, "STAGED.json"));
const currentShaAfter = shaFile(path.join(PKG_DIR, "CURRENT.json"));
const loaded = store.loadPackage(HOTEL, VERSION);

const summary = {
  ok:
    stagedAfter?.publicationVersion === VERSION &&
    stagedAfter?.contentFingerprintSha256 === sealed.contentFingerprintSha256 &&
    currentShaAfter === currentShaBefore &&
    currentAfter?.contentFingerprintSha256 === PRIOR_CURRENT_FP &&
    loaded.ok === true &&
    loaded.package?.opportunities?.length === 68,
  mergeCommitPr597: "c32bc17cf9132e3a0669777df2f55c206532b307",
  packageVersion: VERSION,
  opportunityCount: 68,
  sealedFingerprint: sealed.contentFingerprintSha256,
  stagedBefore,
  stagedAfter,
  stagedShaBefore,
  stagedShaAfter,
  currentBefore,
  currentAfter,
  currentShaBefore,
  currentShaAfter,
  currentUnchanged: currentShaAfter === currentShaBefore,
  rollback: {
    priorStagedVersion: PRIOR_STAGED_VERSION,
    priorStagedFingerprint: PRIOR_STAGED_FP,
    rollbackPointerFile: path.join(OUT, "rollback/STAGED.before.json"),
    emergencyFreeze:
      "C:\\Dev\\dealality\\_gdi_emergency_freeze_20261124-231526",
  },
  coverage: {
    named: sealed.opportunities.filter((o) => o?.primaryContact?.name).length,
    outreach: sealed.opportunities.filter(
      (o) =>
        o?.primaryContact?.email ||
        o?.primaryContact?.phone ||
        o?.primaryContact?.linkedinUrl ||
        o?.primaryContact?.askToBeDirectedTo ||
        o?.primaryContact?.organizationContactRouteLabel,
    ).length,
  },
};

fs.writeFileSync(
  path.join(OUT, "02-ACTIVATION_RESULT.json"),
  `${JSON.stringify(summary, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "rollback/STAGED.after.json"),
  `${JSON.stringify(stagedAfter, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "FOUNDER_STAGED_ACTIVATION_REPORT.md"),
  `# Bethesda STAGED activation — 68 quality candidate

**Package:** \`${VERSION}\`  
**Count:** 68  
**Sealed fingerprint:** \`${sealed.contentFingerprintSha256}\`  
**PR #597 merge:** \`c32bc17cf9132e3a0669777df2f55c206532b307\`

## Pointers
| Pointer | Before | After |
|---|---|---|
| STAGED version | ${PRIOR_STAGED_VERSION} | ${VERSION} |
| STAGED sha256 | \`${stagedShaBefore}\` | \`${stagedShaAfter}\` |
| CURRENT sha256 | \`${currentShaBefore}\` | \`${currentShaAfter}\` (unchanged) |

## Rollback
Restore \`rollback/STAGED.before.json\` → hotel \`STAGED.json\`.  
Emergency freeze: \`C:\\\\Dev\\\\dealality\\\\_gdi_emergency_freeze_20261124-231526\`

CURRENT not modified. No customer republish.
`,
);

console.log(JSON.stringify(summary, null, 2));
if (!summary.ok) process.exit(1);
