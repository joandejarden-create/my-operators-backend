#!/usr/bin/env node
/**
 * Wyndham locked-26 STAGED content hydration rematerialize.
 *
 * Usage:
 *   node scripts/gdi-wyndham-content-hydration-rematerialize-v1.mjs --dry-run
 *   node scripts/gdi-wyndham-content-hydration-rematerialize-v1.mjs --apply \
 *     --auth "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTENT_HYDRATION_REMATERIALIZE"
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  rematerializeWyndhamStagedContentHydration,
  WYNDHAM_CONTENT_HYDRATION_AUTH,
  GDI_WYNDHAM_CONTENT_HYDRATION_REMATERIALIZE_V1,
} from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-content-hydration-rematerialize-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-staged-content-quality-hydration-v1",
);

fs.mkdirSync(OUT, { recursive: true });

function writeJson(name, data) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(data, null, 2)}\n`);
}

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run") || !args.includes("--apply");
const authIdx = args.indexOf("--auth");
const auth =
  authIdx >= 0 ? String(args[authIdx + 1] || "").trim() : dryRun ? WYNDHAM_CONTENT_HYDRATION_AUTH : "";

const result = rematerializeWyndhamStagedContentHydration({ dryRun, auth });

writeJson("01-REMATERIALIZE_RESULT.json", {
  contract: GDI_WYNDHAM_CONTENT_HYDRATION_REMATERIALIZE_V1,
  dryRun,
  auth: dryRun ? null : auth,
  result,
});

writeJson("02-TITLE_BEFORE_AFTER.json", result.titleBeforeAfter || []);
writeJson("03-BEFORE_AUDIT.json", result.beforeAudit || {});
writeJson("04-AFTER_AUDIT.json", result.afterAudit || {});

const md = `# GDI Wyndham STAGED Content Hydration

**Contract:** \`${GDI_WYNDHAM_CONTENT_HYDRATION_REMATERIALIZE_V1}\`
**Mode:** ${dryRun ? "DRY_RUN" : "APPLY"}
**Publication version:** ${result.publicationVersion || "(dry-run)"}
**Count:** ${result.opportunityCount ?? "n/a"}

## Gate

| Metric | Before | After |
|---|---:|---:|
| Audited | ${result.beforeAudit?.audited ?? 0} | ${result.afterAudit?.audited ?? 0} |
| Pass | ${result.beforeAudit?.passCount ?? 0} | ${result.afterAudit?.passCount ?? 0} |
| Blocked | ${result.beforeAudit?.blockedCount ?? 0} | ${result.afterAudit?.blockedCount ?? 0} |
| Certification | ${result.beforeAudit?.certification ?? ""} | ${result.afterAudit?.certification ?? ""} |

## Integrity

\`\`\`json
${JSON.stringify(result.integrity || {}, null, 2)}
\`\`\`

## Title changes (sample)

${(result.titleBeforeAfter || [])
  .filter((r) => r.changed)
  .slice(0, 20)
  .map((r) => `- **${r.id}**: ${r.before.slice(0, 60)} → **${r.after}**`)
  .join("\n") || "_none_"}
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_CONTENT_HYDRATION.md"), md);

if (!result.ok) {
  console.error("FAIL", result.error || result);
  process.exit(dryRun && result.afterAudit ? 0 : 10);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      dryRun,
      activated: result.activated === true,
      publicationVersion: result.publicationVersion,
      beforeBlocked: result.beforeAudit?.blockedCount,
      afterBlocked: result.afterAudit?.blockedCount,
      afterCertification: result.afterAudit?.certification,
      outDir: OUT,
    },
    null,
    2,
  ),
);
