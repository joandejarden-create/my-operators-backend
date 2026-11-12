#!/usr/bin/env node
/**
 * Bethesda 22 canonical title-only APPLY — requires founder approval token.
 *
 *   node scripts/gdi-bethesda-88-canonical-title-apply-v1.mjs \
 *     --authorize-title-apply FOUNDER_APPROVE_BETHESDA_22_CANONICAL_TITLE_REPAIRS_20261112
 *
 *   node scripts/gdi-bethesda-88-canonical-title-apply-v1.mjs --dry-run \
 *     --authorize-title-apply FOUNDER_APPROVE_BETHESDA_22_CANONICAL_TITLE_REPAIRS_20261112
 */
import {
  applyApprovedCanonicalTitlePatches,
  BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
  BETHESDA_CANONICAL_TITLE_APPLY_APPROVAL_TOKEN,
} from "../lib/group-demand-intelligence/research/bethesda-88-title-quality-repair-v1.js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const authIdx = args.indexOf("--authorize-title-apply");
const token = authIdx >= 0 ? args[authIdx + 1] : null;

if (token !== BETHESDA_CANONICAL_TITLE_APPLY_APPROVAL_TOKEN) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: "APPROVAL_TOKEN_REQUIRED",
        expected: BETHESDA_CANONICAL_TITLE_APPLY_APPROVAL_TOKEN,
      },
      null,
      2
    )
  );
  process.exit(2);
}

const result = applyApprovedCanonicalTitlePatches({
  approvalToken: token,
  dryRun,
});

console.log(
  JSON.stringify(
    {
      version: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
      ...result,
    },
    null,
    2
  )
);

if (!result.ok) process.exitCode = 2;
