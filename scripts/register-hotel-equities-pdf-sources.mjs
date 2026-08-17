#!/usr/bin/env node
/**
 * Register Hotel Equities CALA PDFs in Source Library — dry-run default.
 *
 *   npm run register-hotel-equities-pdf-sources -- --dry-run
 *   npm run register-hotel-equities-pdf-sources -- --apply --approve-hotel-equities-pdf-register
 *
 * Does not approve sources/facts or touch Setup governance.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  planHotelEquitiesPdfRegistration,
  applyHotelEquitiesPdfRegistration,
} from "../lib/partner-intelligence/hotel-equities-pdf-register.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVED =
  process.argv.includes("--approve-hotel-equities-pdf-register") ||
  process.argv.includes("--approve-he-pdf-register");

function mdEscape(s) {
  return String(s ?? "").replace(/\|/g, "\\|");
}

function renderMarkdown(plan, applyResult) {
  const lines = [
    "# Hotel Equities CALA — PDF Source Registration",
    "",
    `Generated: ${plan.generatedAt}`,
    `Mode: **${DRY_RUN ? "dry-run" : "apply"}**`,
    `Operator: \`recWPKu5laVZxsvpn\``,
    "",
    "## Summary",
    "",
    `- Existing linked sources: **${plan.existingSourceCount}**`,
    `- PDF rows in plan: **${plan.summary.total}**`,
    `- Ready to register: **${plan.summary.ready}**`,
    `- Already registered: **${plan.summary.skip}**`,
    `- Blocked: **${plan.summary.blocked}**`,
    "",
    "## File inventory",
    "",
    "| Key | Local File Path | On disk | Root | Bytes | Registered |",
    "|-----|-----------------|---------|------|-------|------------|",
  ];

  for (const row of plan.rows) {
    const res = row.fileCheck.resolution;
    lines.push(
      `| ${mdEscape(row.key)} | \`${mdEscape(row.spec.localFilePath)}\` | ${
        res ? "yes" : "no"
      } | ${res?.resolvedRootKind || "—"} | ${res?.sizeBytes ?? "—"} | ${
        row.alreadyRegistered ? `yes (\`${row.existingSourceId}\`)` : "no"
      } |`
    );
  }

  lines.push("", "## Proposed Source Library rows", "");

  for (const row of plan.rows) {
    lines.push(`### ${row.spec.sourceTitle}`, "");
    lines.push(`- Status: **${row.registrationStatus}**`);
    lines.push(`- Source Type: **${row.spec.sourceType}**`);
    lines.push(`- Source Origin: **${row.spec.sourceOrigin}**`);
    lines.push(`- Region: **${row.spec.region}**`);
    lines.push(`- Source Quality: **${row.spec.sourceQuality}**`);
    lines.push(`- Status after registration: **Captured** (not Approved)`);
    lines.push(`- Approved for Extraction: **No**`);
    lines.push(`- Approved for Explorer Use: **No**`);
    if (row.spec.sourceUrl) lines.push(`- Source URL: ${row.spec.sourceUrl}`);
    if (row.fileCheck.issues.length) {
      lines.push(`- File issues: ${row.fileCheck.issues.join("; ")}`);
    }
    if (!row.validation.ok) {
      lines.push(`- Validation errors: ${row.validation.errors.join("; ")}`);
    }
    lines.push("");
  }

  if (applyResult) {
    lines.push("## Apply result", "");
    lines.push(`- Applied: **${applyResult.applied.length}**`);
    lines.push(`- Skipped: **${applyResult.skipped.length}**`);
    lines.push(`- Errors: **${applyResult.errors.length}**`);
    if (applyResult.applied.length) {
      lines.push("", "| Source ID | Title | Local File Path |", "|-----------|-------|-----------------|");
      for (const a of applyResult.applied) {
        lines.push(`| \`${a.sourceId}\` | ${mdEscape(a.sourceTitle)} | \`${mdEscape(a.localFilePath)}\` |`);
      }
    }
  }

  lines.push("", "## Next steps", "");
  lines.push("1. Founder review this dry-run report.");
  lines.push("2. Apply registration: `npm run register-hotel-equities-pdf-sources -- --apply --approve-hotel-equities-pdf-register`");
  lines.push("3. Steward: approve Explorer use + extraction on selected PDF(s).");
  lines.push("4. Extend narrow extraction allowlist; dry-run extract; do **not** auto-republish governance.");
  lines.push("");

  return lines.join("\n");
}

async function main() {
  const plan = await planHotelEquitiesPdfRegistration();
  let applyResult = null;

  if (APPLY) {
    if (!APPROVED) {
      console.error("Apply requires --approve-hotel-equities-pdf-register");
      process.exit(1);
    }
    applyResult = await applyHotelEquitiesPdfRegistration(plan);
    console.log(
      `[he-pdf-register] apply applied=${applyResult.applied.length} skipped=${applyResult.skipped.length} errors=${applyResult.errors.length}`
    );
  } else {
    console.log(
      `[he-pdf-register] dry-run ready=${plan.summary.ready} skip=${plan.summary.skip} blocked=${plan.summary.blocked}`
    );
  }

  const report = { plan, applyResult, mode: DRY_RUN ? "dry_run" : "apply" };
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  writeFileSync(REPORT_MD, renderMarkdown(plan, applyResult));
  console.log("Wrote", REPORT_MD);
  console.log("Wrote", REPORT_JSON);

  if (applyResult?.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
