#!/usr/bin/env node
/**
 * Remediate Golden Set v2 candidates: null-subject → entity-specific cases.
 * LIVE_PROVIDER_CALLS: 0. No auto-review. No auto-promotion.
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  auditCandidateSubjects,
  auditNullSubjectCases,
  remediateCandidateEntityNomination,
  validateActiveCandidatesForExport,
  SUPERSEDED_INVALID_SUBJECT,
} from "../lib/ai-visibility/validation/golden-set-candidate-entity-remediation.js";
import { exportAllReviewCandidates, EXPORT_MODE } from "../lib/ai-visibility/validation/golden-set-review-bulk-export-import.js";
import { buildReviewQueue } from "../lib/ai-visibility/validation/golden-set-human-review.js";
import { OUT_CANDIDATES } from "../lib/ai-visibility/validation/golden-set-expansion.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const apply = process.argv.includes("--apply");

console.log("Golden Set Candidate Entity Nomination Remediation\n");
console.log(`Mode: ${apply ? "APPLY (write candidates fixture)" : "DRY-RUN"}`);

const before = auditCandidateSubjects();
console.log("\n## Before");
console.log(
  JSON.stringify(
    {
      TOTAL: before.TOTAL_CANDIDATES,
      ENTITY_SPECIFIC: before.VALID_ENTITY_SPECIFIC_CASES,
      NULL_SUBJECT: before.NULL_SUBJECT_CASES,
      nullByProvider: before.nullByProvider,
    },
    null,
    2
  )
);

const rootAudit = await auditNullSubjectCases({});
console.log("\n## Root Cause");
console.log("ROOT_CAUSE:", rootAudit.ROOT_CAUSE);
console.log("AFFECTED_PROVIDERS:", rootAudit.AFFECTED_PROVIDERS.join(", "));
console.log("AFFECTED_GENERATOR_PATH:", rootAudit.AFFECTED_GENERATOR_PATH);

const out = await remediateCandidateEntityNomination({ apply });
console.log("\n## After");
console.log(JSON.stringify(out.after, null, 2));
console.log("Provider coverage:", JSON.stringify(out.providerCoverage, null, 2));
console.log("Language coverage:", JSON.stringify(out.languageCoverage, null, 2));
console.log("Geography coverage:", JSON.stringify(out.geographyCoverage, null, 2));
console.log(
  "System-suggestion classification:",
  JSON.stringify(out.systemSuggestionClassificationCoverage, null, 2)
);
console.log("Nomination failures:", out.nominationFailures?.length || 0);
if (out.archivedPriorPath) console.log("Archived prior:", out.archivedPriorPath);
if (out.writtenPath) console.log("Written:", out.writtenPath);

const gate = validateActiveCandidatesForExport(out);
console.log("\n## Export Gate");
console.log(JSON.stringify({ ok: gate.ok, ...gate, failures: gate.failures?.slice(0, 10) }, null, 2));

if (apply) {
  const exported = exportAllReviewCandidates({ mode: EXPORT_MODE.ALL, write: true });
  console.log("\n## Re-export");
  console.log(
    JSON.stringify(
      {
        NEW_EXPORT_PATH: exported.writtenFiles?.jsonPath,
        EXPORTED_TOTAL: exported.totalCandidates,
        NULL_SUBJECTS_IN_EXPORT: exported.invalidSubjectCaseCount,
        entitySpecificCaseCount: exported.entitySpecificCaseCount,
      },
      null,
      2
    )
  );
  // Convenience copy for founder
  const convenience = path.join(
    ROOT,
    "data/ai-visibility/validation/human-review/exports/golden-set-review-candidates-all.json"
  );
  fs.mkdirSync(path.dirname(convenience), { recursive: true });
  fs.writeFileSync(convenience, JSON.stringify(exported, null, 2), "utf8");
  console.log("Convenience export:", convenience);
}

const queue = buildReviewQueue({});
const nullInQueue = queue.cases.filter((c) => !c.candidateEntity || !c.canonicalEntityId);
console.log("\n## Queue check");
console.log(
  JSON.stringify(
    {
      queueTotal: queue.cases.length,
      nullInQueue: nullInQueue.length,
      supersededInSource: (out.cases || []).filter(
        (c) => c.reviewStatus === SUPERSEDED_INVALID_SUBJECT
      ).length,
      candidatesPath: OUT_CANDIDATES,
    },
    null,
    2
  )
);

if (!apply) {
  console.log("\nDry-run complete. Re-run with --apply to write remediated candidates + export.");
}
