#!/usr/bin/env node
/**
 * Bethesda SC merge-add APPLY — requires founder approval token.
 *
 *   node scripts/gdi-bethesda-sc-integrity-restore-apply.mjs \
 *     --authorize-merge-add FOUNDER_APPROVE_BETHESDA_SC_MERGE_ADD_5_IDS_20261112
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  BETHESDA_SC_MERGE_ADD_APPROVAL_TOKEN,
  VERIFIED_59_ROW_SHA256,
  applyAuthorizedMergeAddProtectedBodies,
  sha256File,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-bethesda-sc-integrity-repair-v1");
const SOURCE =
  process.env.BETHESDA_VERIFIED_59_SOURCE ||
  "C:/Dev/dealality/authorized-backups/primary-working-state-e2ae6c7d3-20261111/files/data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json";

const args = process.argv.slice(2);
const authIdx = args.indexOf("--authorize-merge-add");
const token = authIdx >= 0 ? args[authIdx + 1] : null;

if (token !== BETHESDA_SC_MERGE_ADD_APPROVAL_TOKEN) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: "APPROVAL_TOKEN_REQUIRED",
        expected: BETHESDA_SC_MERGE_ADD_APPROVAL_TOKEN,
      },
      null,
      2
    )
  );
  process.exit(2);
}

if (!fs.existsSync(SOURCE)) {
  console.error(JSON.stringify({ ok: false, error: "SOURCE_ABSENT", SOURCE }, null, 2));
  process.exit(2);
}
const sourceSha = sha256File(SOURCE);
if (sourceSha !== VERIFIED_59_ROW_SHA256) {
  console.error(
    JSON.stringify(
      { ok: false, error: "SOURCE_SHA_MISMATCH", sourceSha, expected: VERIFIED_59_ROW_SHA256 },
      null,
      2
    )
  );
  process.exit(2);
}

const sourceDoc = JSON.parse(fs.readFileSync(SOURCE, "utf8"));
const bodies = BETHESDA_PROTECTED_SCALING_IDS.map((id) => {
  const row = (sourceDoc.opportunities || []).find((r) => (r.id || r.opportunityId) === id);
  if (!row) throw new Error(`Missing body ${id}`);
  return row;
});

const result = applyAuthorizedMergeAddProtectedBodies({
  hotelId: BETHESDA_HOTEL_ID,
  recoveredBodies: bodies,
  protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  approvalToken: token,
  expectPriorCount: 54,
  expectPriorAbsentIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  root: ROOT,
});

const shareBefore = {
  active: sha256File(
    path.join(ROOT, "config/client-share/gdi-share-registry/active-tokens.json")
  ),
  revoked: sha256File(
    path.join(ROOT, "config/client-share/gdi-share-registry/revoked-token-ids.json")
  ),
};

let union = null;
let stage = null;
let exa = null;
if (result.writeApplied) {
  const afterDoc = JSON.parse(fs.readFileSync(result.targetPath, "utf8"));
  union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    afterDoc.opportunities || [],
    { root: ROOT }
  );
  const stagePath = path.join(
    ROOT,
    "data/group-demand-intelligence/research-stage",
    BETHESDA_HOTEL_ID,
    "reconstructed-opportunities-v1.json"
  );
  stage = JSON.parse(fs.readFileSync(stagePath, "utf8"));
  const exaIds = [
    "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
    "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
    "gdi_pep_live_c165c897d233",
  ];
  exa = exaIds.map((id) => {
    const row = (stage.opportunities || []).find((r) => r.id === id);
    return {
      id,
      hasExa: Boolean(row?.exaAuthorizedBatch?.structured),
      contact: row?.primaryContact?.name || null,
      maturity: row?.commercialMaturity || null,
    };
  });
}

const shareAfter = {
  active: sha256File(
    path.join(ROOT, "config/client-share/gdi-share-registry/active-tokens.json")
  ),
  revoked: sha256File(
    path.join(ROOT, "config/client-share/gdi-share-registry/revoked-token-ids.json")
  ),
};

const report = {
  version: "gdi-bethesda-sc-integrity-restore-apply-v1",
  authorization: BETHESDA_SC_MERGE_ADD_APPROVAL_TOKEN,
  result,
  union: union?.counts || null,
  stageCount: stage?.opportunities?.length ?? null,
  exa,
  shareRegistry: {
    before: shareBefore,
    after: shareAfter,
    unchanged:
      shareBefore.active === shareAfter.active &&
      shareBefore.revoked === shareAfter.revoked,
  },
  paidCalls: 0,
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "07-merge-add-apply-result.json"),
  JSON.stringify(report, null, 2)
);

console.log(JSON.stringify(report, null, 2));
if (!result.ok) process.exitCode = 2;
