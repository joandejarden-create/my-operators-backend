#!/usr/bin/env node
/**
 * Register Radisson Blu by Choice local PDF(s) in Source Library — dry-run default.
 *
 *   npm run register-radisson-blu-pdf-sources -- --dry-run
 *   npm run register-radisson-blu-pdf-sources -- --apply --approve-radisson-blu-pdf-register
 *
 * Does not approve sources/facts, publish governance, or touch Setup fields.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  RB_BRAND_ID,
  planRadissonBluPdfRegistration,
  applyRadissonBluPdfRegistration,
} from "../lib/partner-intelligence/radisson-blu-pdf-register.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVED =
  process.argv.includes("--approve-radisson-blu-pdf-register") ||
  process.argv.includes("--approve-rb-pdf-register");

function mdEscape(s) {
  return String(s ?? "").replace(/\|/g, "\\|");
}

function renderMarkdown(plan, applyResult) {
  const lines = [
    "# Radisson Blu by Choice — PDF Source Registration",
    "",
    `Generated: ${plan.generatedAt}`,
    `Mode: **${DRY_RUN ? "dry-run" : "apply"}**`,
    `Brand: Radisson Blu by Choice — \`${RB_BRAND_ID}\``,
    "",
    "## Summary",
    "",
    `- Existing linked sources: **${plan.existingSourceCount}**`,
    `- v1 PDF rows in plan: **${plan.summary.total}**`,
    `- Ready to register: **${plan.summary.ready}**`,
    `- Already registered: **${plan.summary.skip}**`,
    `- Blocked: **${plan.summary.blocked}**`,
    `- Future candidates (not registered): **${plan.summary.futureCandidates}**`,
    `- Airtable modified: **${applyResult?.applied?.length ? "yes" : "no"}**`,
    "",
    "## File inventory (v1)",
    "",
    "| Key | Local File Path | Found | Bytes | Dual-root readable | Text length | Duplicate | Status |",
    "|-----|-----------------|-------|-------|--------------------|-------------|-----------|--------|",
  ];

  for (const row of plan.rows) {
    const res = row.fileCheck.resolution;
    const tp = row.fileCheck.textPreview;
    lines.push(
      `| ${mdEscape(row.key)} | \`${mdEscape(row.spec.localFilePath)}\` | ${
        res ? "yes" : "no"
      } | ${res?.sizeBytes ?? "—"} | ${tp?.dualRootReadable ? "yes" : "no"} | ${
        tp?.textLength ?? "—"
      } | ${row.alreadyRegistered ? `yes (\`${row.existingSourceId}\`)` : "no"} | **${
        row.registrationStatus
      }** |`
    );
  }

  lines.push("", "## Proposed Source Library values (v1)", "");

  for (const row of plan.rows) {
    lines.push(`### ${row.spec.sourceTitle}`, "");
    lines.push(`- Registration status: **${row.registrationStatus}**`);
    lines.push(`- Profile Type: **Brand**`);
    lines.push(`- Brand link: \`${RB_BRAND_ID}\``);
    lines.push(`- Source Type: **${row.spec.sourceType}**`);
    lines.push(`- Source Origin: **${row.spec.sourceOrigin}**`);
    lines.push(`- Region: **${row.spec.region}** (Americas/CALA company material; CALA is closest schema option)`);
    lines.push(`- Source Quality: **${row.spec.sourceQuality}**`);
    lines.push(`- Status: **Captured**`);
    lines.push(`- Approved for Extraction: **No**`);
    lines.push(`- Approved for Explorer Use: **No**`);
    lines.push(`- Source URL: *(blank)*`);
    lines.push(`- Local File Path: \`${row.spec.localFilePath}\``);
    if (row.duplicate) {
      lines.push(
        `- Duplicate: **${row.duplicate.matchType}** — existing \`${row.duplicate.sourceId}\` (${mdEscape(row.duplicate.sourceTitle)})`
      );
    }
    if (row.fileCheck.textPreview?.preview) {
      lines.push("", "Text preview (first 400 chars):", "", "```", row.fileCheck.textPreview.preview, "```");
    }
    if (row.fileCheck.issues.length) {
      lines.push(`- File issues: ${row.fileCheck.issues.join("; ")}`);
    }
    if (!row.validation.ok) {
      lines.push(`- Validation errors: ${row.validation.errors.join("; ")}`);
    }
    lines.push("");
  }

  if (plan.futureInventory?.length) {
    lines.push("## Future candidates (disabled in v1)", "");
    lines.push("| Key | Local File Path | On disk | Bytes | Text length | Note |");
    lines.push("|-----|-----------------|---------|-------|-------------|------|");
    for (const f of plan.futureInventory) {
      lines.push(
        `| ${mdEscape(f.key)} | \`${mdEscape(f.spec.localFilePath)}\` | ${
          f.fileFound ? "yes" : "no"
        } | ${f.sizeBytes ?? "—"} | ${f.textLength} | ${mdEscape(f.note || "")} |`
      );
    }
    lines.push("");
  }

  if (plan.existingSources?.length) {
    lines.push("## Existing brand-linked sources", "");
    lines.push("| Source ID | Title | Local File Path | Type | Status |");
    lines.push("|-----------|-------|-----------------|------|--------|");
    for (const s of plan.existingSources) {
      lines.push(
        `| \`${s.id}\` | ${mdEscape(s.sourceTitle)} | \`${mdEscape(s.localFilePath || "—")}\` | ${mdEscape(s.sourceType)} | ${mdEscape(s.status)} |`
      );
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

  lines.push("", "## Recommended next command", "");
  if (DRY_RUN && plan.summary.ready > 0) {
    lines.push(
      "```bash",
      "npm run register-radisson-blu-pdf-sources -- --apply --approve-radisson-blu-pdf-register",
      "```"
    );
    lines.push("");
    lines.push("After registration:");
    lines.push("1. Steward: approve Explorer use + extraction on one-pager only.");
    lines.push("2. Extend `radisson-blu-extract` allowlist with new source ID.");
    lines.push("3. `npm run radisson-blu-extract -- --dry-run` — do **not** apply until founder review.");
  } else if (plan.summary.skip > 0 && plan.summary.ready === 0) {
    lines.push("No apply needed — source already registered or blocked. Review duplicate row above.");
  } else if (applyResult?.applied?.length) {
    lines.push("1. Steward new source row.");
    lines.push("2. Update extraction allowlist; dry-run extract.");
  }
  lines.push("");

  return lines.join("\n");
}

async function main() {
  const plan = await planRadissonBluPdfRegistration();
  let applyResult = null;

  if (APPLY) {
    if (!APPROVED) {
      console.error("Apply requires --approve-radisson-blu-pdf-register");
      process.exit(1);
    }
    applyResult = await applyRadissonBluPdfRegistration(plan);
    console.log(
      `[rb-pdf-register] apply applied=${applyResult.applied.length} skipped=${applyResult.skipped.length} errors=${applyResult.errors.length}`
    );
  } else {
    console.log(
      `[rb-pdf-register] dry-run ready=${plan.summary.ready} skip=${plan.summary.skip} blocked=${plan.summary.blocked}`
    );
  }

  const report = {
    plan,
    applyResult,
    mode: DRY_RUN ? "dry_run" : "apply",
    airtableModified: Boolean(applyResult?.applied?.length),
  };
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
