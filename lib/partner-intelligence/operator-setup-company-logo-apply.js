/**
 * Apply square companyLogo attachments to Operator Setup - Profile & Positioning.
 * Default dry-run. Airtable fetches the URL into the attachment field.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { upsertOperatorOneToOneTable } from "../../api/lib/operator-setup-new-base-writer.js";
import {
  listCompanyLogoSlugs,
  resolveCompanyLogoMaster,
} from "./operator-setup-company-logos.js";

export const OPERATOR_SETUP_COMPANY_LOGO_APPLY_VERSION =
  "operator-setup-company-logo-apply-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const PROFILE_TABLE = "Operator Setup - Profile & Positioning";

async function validateLogoUrl(url) {
  try {
    const r = await fetch(url, {
      redirect: "follow",
      headers: { "user-agent": "Mozilla/5.0 DealalityLogoApply/1.0" },
    });
    const ct = (r.headers.get("content-type") || "").split(";")[0].trim();
    const buf = Buffer.from(await r.arrayBuffer());
    const ok =
      r.ok &&
      buf.length > 200 &&
      (/image\//i.test(ct) || /\.(png|jpe?g|webp|svg|ico)(\?|$)/i.test(url));
    return { ok, status: r.status, ct, bytes: buf.length, finalUrl: r.url };
  } catch (err) {
    return { ok: false, error: String(err?.message || err).slice(0, 160) };
  }
}

/**
 * @param {{ operators?: string[], apply?: boolean, approveApply?: boolean }} [opts]
 */
export async function runOperatorSetupCompanyLogoApply(opts = {}) {
  const apply = opts.apply === true;
  if (apply && !opts.approveApply) {
    throw new Error("Apply requires --approve-operator-setup-company-logo-apply");
  }

  const operators = opts.operators?.length
    ? opts.operators
    : listCompanyLogoSlugs();

  const results = [];
  for (const slug of operators) {
    const meta = resolveCompanyLogoMaster(slug);
    if (!meta) {
      results.push({ operatorSlug: slug, error: "unknown_slug_or_missing_logo_spec" });
      continue;
    }

    const probe = await validateLogoUrl(meta.logo.url);
    const attachment = [
      {
        url: meta.logo.url,
        filename: meta.logo.filename,
      },
    ];
    const payload = { companyLogo: attachment };
    const mapping = [
      {
        airtableField: "companyLogo",
        valuePreview: `${meta.logo.filename} <- ${meta.logo.url}`,
      },
    ];

    if (!probe.ok) {
      results.push({
        operatorSlug: slug,
        recordId: meta.recordId,
        companyName: meta.companyName,
        status: "validation_failed",
        probe,
        validation: { pass: false, checksFailed: ["logo_url_unreachable_or_not_image"] },
        exactFieldMapping: mapping,
        note: meta.logo.note,
      });
      continue;
    }

    if (apply) {
      try {
        const res = await upsertOperatorOneToOneTable(
          PROFILE_TABLE,
          meta.recordId,
          payload,
          `company-logo-${slug}`
        );
        results.push({
          operatorSlug: slug,
          recordId: meta.recordId,
          companyName: meta.companyName,
          status: res.created ? "created" : "updated",
          profileRecordId: res.recordId,
          probe,
          exactFieldMapping: mapping,
          sanitizedPayloadPreview: payload,
          note: meta.logo.note,
          validation: { pass: true, checksFailed: [] },
        });
      } catch (err) {
        results.push({
          operatorSlug: slug,
          recordId: meta.recordId,
          companyName: meta.companyName,
          status: "error",
          error: String(err?.message || err).slice(0, 400),
          probe,
          exactFieldMapping: mapping,
          sanitizedPayloadPreview: payload,
          note: meta.logo.note,
          validation: { pass: false, checksFailed: ["airtable_write"] },
        });
      }
    } else {
      results.push({
        operatorSlug: slug,
        recordId: meta.recordId,
        companyName: meta.companyName,
        status: "would_upsert",
        probe,
        exactFieldMapping: mapping,
        sanitizedPayloadPreview: payload,
        note: meta.logo.note,
        validation: { pass: true, checksFailed: [] },
      });
    }
  }

  return {
    version: OPERATOR_SETUP_COMPANY_LOGO_APPLY_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply,
    airtableWrites: apply,
    writeKind: apply ? "operator_setup_company_logo_attachment" : "none",
    tableName: PROFILE_TABLE,
    operators,
    results,
    summary: {
      operators: results.length,
      ready: results.filter((r) =>
        ["would_upsert", "updated", "created"].includes(r.status)
      ).length,
      errors: results.filter((r) =>
        ["error", "validation_failed"].includes(r.status)
      ).length,
    },
    errorHandling: {
      validationError: "Skip operator; replace logo URL with reachable image",
      apiError: "Logged per operator",
      networkError: "Retry once",
      userFacing: "Company logo apply failed for one or more operators.",
    },
  };
}

export function writeOperatorSetupCompanyLogoApplyReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-setup-company-logo-apply.json");
  const mdPath = path.join(reportsDir, "operator-setup-company-logo-apply.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    "# Operator Setup company logo apply",
    "",
    `dryRun: **${report.dryRun}** · ready: **${report.summary.ready}** · errors: **${report.summary.errors}**`,
    "",
  ];
  for (const r of report.results) {
    lines.push(`## ${r.companyName || r.operatorSlug}`);
    lines.push(`- Master: \`${r.recordId}\``);
    lines.push(`- Status: **${r.status}**`);
    if (r.probe) {
      lines.push(
        `- Probe: ${r.probe.ok ? "ok" : "fail"} ${r.probe.bytes || 0}B ${r.probe.ct || r.probe.error || ""}`
      );
    }
    if (r.exactFieldMapping?.[0]) {
      lines.push(`- Mapping: \`${r.exactFieldMapping[0].valuePreview}\``);
    }
    if (r.note) lines.push(`- Note: ${r.note}`);
    if (r.error) lines.push(`- Error: ${r.error}`);
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}
