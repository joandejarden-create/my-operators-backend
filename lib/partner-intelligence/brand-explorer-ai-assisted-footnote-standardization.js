/**
 * Brand Explorer — AI-Assisted Profile footnote standardization runner.
 * Prefer code/rendering (no Airtable writes). Audit + dry-run + apply + validate.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadActiveUniverse, resolveActiveUniverseRecordId } from "./brand-explorer-active-universe.js";
import {
  AI_ASSISTED_FOOTNOTE_VERSION,
  AI_ASSISTED_FOOTNOTE_STANDARD_EFFECTIVE_DATE,
  applyBrandExplorerAiAssistedFootnote,
  auditBrandExplorerFootnoteRow,
  evaluateAiAssistedProfileFootnoteGate,
  factoryPreviewIdentitiesForFootnoteAudit,
  writeFootnoteAuditReports,
} from "./brand-explorer-ai-assisted-footnote.js";
import { normalizeProfileGovernance } from "../profile-governance/normalize-profile-governance.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { getLegacySeedBrand } from "./brand-explorer-legacy-approved-profile-reconciliation.js";
import { resolveSectionPatternBrandIdentity } from "./brand-explorer-section-pattern-parity.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

export const STANDARDIZATION_VERSION = "brand-explorer-ai-assisted-footnote-standardization-v1";

export const APPLY_FLAGS = Object.freeze([
  "--approve-ai-assisted-footnote-standardization",
  "--confirm-global-rendering-requirement",
  "--confirm-every-brand-explorer-profile",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-presentation-content-rewrites",
  "--confirm-no-image-writes",
  "--confirm-no-cala-claims-without-source-support",
  "--confirm-no-brand-verified-wording-unless-company-validated",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isAirtableRecordId(v) {
  return /^rec[a-zA-Z0-9]{10,}$/.test(nz(v));
}

async function fetchBrandApi(slugOrId) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const seed = getLegacySeedBrand(slugOrId);
  const sectionIdentity = resolveSectionPatternBrandIdentity(slugOrId);
  const activeCfg = getActiveProfileBrandConfig(String(slugOrId || "").toLowerCase());
  const discovery = getDiscoveryBrandConfig(slugOrId);
  const sectionRecordId = isAirtableRecordId(sectionIdentity?.recordId)
    ? sectionIdentity.recordId
    : null;
  const lookupId =
    (isAirtableRecordId(slugOrId) && slugOrId) ||
    (isAirtableRecordId(seed?.recordId) && seed.recordId) ||
    sectionRecordId ||
    (isAirtableRecordId(activeCfg?.recordId) && activeCfg.recordId) ||
    (isAirtableRecordId(discovery?.recordId) && discovery.recordId) ||
    resolveActiveUniverseRecordId(slugOrId) ||
    slugOrId;
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: lookupId }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function checkApplyFlags(argv, apply) {
  const missing = APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...APPLY_FLAGS],
  };
}

/**
 * Raw (pre-enrichment) audit uses normalizeProfileGovernance only.
 * Enriched audit uses live API (includes always-on footnote).
 */
export async function runFootnoteAudit({ mode = "raw", includeFactoryPreview = true } = {}) {
  const universe = await loadActiveUniverse({ includeDetails: false });
  const rows = [];

  for (const entry of universe.brands) {
    let brand = null;
    try {
      // Prefer Airtable record id when present; slug path still resolves via
      // resolveActiveUniverseRecordId (Wave 13 Accor anchors + aliases).
      brand = await fetchBrandApi(
        (isAirtableRecordId(entry.recordId) && entry.recordId) || entry.slug || entry.recordId
      );
    } catch (err) {
      rows.push({
        brand: entry.name,
        slug: entry.slug,
        recordId: entry.recordId,
        brandStatus: entry.brandStatus || null,
        shouldRenderFullProfile: false,
        footnoteVisible: false,
        lastReviewedPresent: false,
        sourceBasisPresent: false,
        regionBasisPresent: false,
        failureReason: `brand_api_fetch_failed: ${err?.message || err}`,
        cohort: "active_universe",
        mode,
      });
      continue;
    }

    if (!brand) {
      rows.push({
        brand: entry.name,
        slug: entry.slug,
        recordId: entry.recordId,
        brandStatus: entry.status || entry.brandStatus || null,
        shouldRenderFullProfile: false,
        footnoteVisible: false,
        lastReviewedPresent: false,
        sourceBasisPresent: false,
        regionBasisPresent: false,
        failureReason: "brand_not_found",
        cohort: "active_universe",
        mode,
      });
      continue;
    }

    if (mode === "raw") {
      // Reconstruct native governance chip (pre Brand Explorer always-on enricher).
      const g = brand?.governance || {};
      const native = normalizeProfileGovernance(
        {
          "Validation Status": g.validationStatus,
          "Usage Permission": g.usagePermission,
          "Source Type": g.sourceType,
          "Source Region": g.sourceRegion,
          "Last Reviewed Date": g.lastReviewedDate,
          "Company Validated": g.companyValidated === true,
          "Company Validation Date": g.companyValidationDate,
          "External Display Status": g.externalDisplayStatus,
          "Confidence Level": g.confidenceLevel,
        },
        { entityType: "brand" }
      );
      const row = auditBrandExplorerFootnoteRow({
        brand: { ...brand, governance: native },
        slug: entry.slug || brand?.slug,
        brandStatus: brand?.brandStatus || entry.brandStatus,
        mode: "raw",
      });
      row.cohort = "active_universe";
      rows.push(row);
    } else {
      const row = auditBrandExplorerFootnoteRow({
        brand,
        slug: entry.slug || brand?.slug,
        brandStatus: brand?.brandStatus || entry.brandStatus,
        mode: "enriched",
      });
      const gate = evaluateAiAssistedProfileFootnoteGate(brand, "");
      if (!gate.pass && !row.failureReason) {
        row.failureReason = gate.failures.join(", ");
      }
      if (!gate.pass) {
        row.footnoteVisible = Boolean(brand?.governance?.displayLabel);
        row.lastReviewedPresent = gate.lastReviewedPresent;
        row.sourceBasisPresent = gate.sourceBasisPresent;
        row.regionBasisPresent = gate.regionPresent;
      }
      row.cohort = "active_universe";
      row.gate = gate;
      rows.push(row);
    }
  }

  if (includeFactoryPreview) {
    const preview = factoryPreviewIdentitiesForFootnoteAudit();
    for (const [slug, meta] of Object.entries(preview)) {
      if (rows.some((r) => r.slug === slug || r.recordId === meta.recordId)) {
        // Still mark factory-preview path when already Active.
        const existing = rows.find((r) => r.slug === slug || r.recordId === meta.recordId);
        if (existing) existing.alsoFactoryPreviewIdentity = true;
        continue;
      }
      let brand = null;
      try {
        brand = await fetchBrandApi(slug || meta.recordId);
      } catch (err) {
        rows.push({
          brand: meta.name || slug,
          slug,
          recordId: meta.recordId,
          brandStatus: meta.recommendedStatusWhileInFactory || null,
          shouldRenderFullProfile: false,
          footnoteVisible: false,
          lastReviewedPresent: false,
          sourceBasisPresent: false,
          regionBasisPresent: false,
          failureReason: `factory_preview_fetch_failed: ${err?.message || err}`,
          cohort: "factory_preview",
          mode,
        });
        continue;
      }
      if (mode === "enriched") {
        applyBrandExplorerAiAssistedFootnote(brand, {});
      }
      const row = auditBrandExplorerFootnoteRow({
        brand,
        slug,
        brandStatus: brand?.brandStatus || meta.recommendedStatusWhileInFactory,
        mode: mode === "raw" ? "raw" : "enriched",
      });
      row.cohort = "factory_preview";
      rows.push(row);
    }
  }

  const fail = rows.filter((r) => r.failureReason).length;
  const pass = rows.length - fail;
  const report = {
    version: AI_ASSISTED_FOOTNOTE_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    summary: {
      activeCount: universe.totalCount,
      previewCount: rows.filter((r) => r.cohort === "factory_preview").length,
      totalRows: rows.length,
      pass,
      fail,
      footnoteVisibleCount: rows.filter((r) => r.footnoteVisible).length,
      footnoteMissingCount: rows.filter((r) => !r.footnoteVisible).length,
    },
    rows,
  };

  const names =
    mode === "raw"
      ? {
          jsonName: "brand-explorer-ai-assisted-footnote-audit.json",
          mdName: "brand-explorer-ai-assisted-footnote-audit.md",
        }
      : {
          jsonName: "brand-explorer-ai-assisted-footnote-audit-enriched.json",
          mdName: "brand-explorer-ai-assisted-footnote-audit-enriched.md",
        };
  const paths = writeFootnoteAuditReports(report, names);
  return { report, paths };
}

export async function runStandardization({ argv = [], dryRun = true, apply = false } = {}) {
  const flagCheck = checkApplyFlags(argv, apply);
  if (apply && !flagCheck.ok) {
    return {
      ok: false,
      dryRun: false,
      apply: false,
      error: "missing_apply_flags",
      missingFlags: flagCheck.missing,
      airtableWrites: 0,
    };
  }

  // Phase 1: raw audit (documents current native chip gaps)
  const rawAudit = await runFootnoteAudit({ mode: "raw", includeFactoryPreview: true });

  // Phase 2: enriched audit (code path)
  const enrichedAudit = await runFootnoteAudit({ mode: "enriched", includeFactoryPreview: true });

  const codeOnly =
    "Code/rendering fix only — Brand Explorer API always applies AI-Assisted footnote fallbacks. No Airtable writes.";

  const report = {
    version: STANDARDIZATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: dryRun && !apply,
    apply: apply === true,
    airtableWrites: 0,
    noAirtableWrites: true,
    approach: codeOnly,
    footnoteStandardEffectiveDate: AI_ASSISTED_FOOTNOTE_STANDARD_EFFECTIVE_DATE,
    applyFlags: flagCheck,
    rawAuditSummary: rawAudit.report.summary,
    enrichedAuditSummary: enrichedAudit.report.summary,
    rawFailingSlugs: (rawAudit.report.rows || [])
      .filter((r) => r.failureReason)
      .map((r) => r.slug)
      .filter(Boolean),
    enrichedFailingSlugs: (enrichedAudit.report.rows || [])
      .filter((r) => r.failureReason)
      .map((r) => r.slug)
      .filter(Boolean),
    acceptance: {
      ai_assisted_profile_footnote_standardized_globally:
        enrichedAudit.report.summary.fail === 0,
      everyActiveProfileFootnoteVisible:
        (enrichedAudit.report.rows || [])
          .filter((r) => r.cohort === "active_universe")
          .every((r) => r.footnoteVisible && !r.failureReason),
      noCompanyValidatedChanges: true,
      noSourceLibraryChanges: true,
      noRegistryChanges: true,
      noBrandStatusChanges: true,
      noReleaseFieldWrites: true,
      noPresentationContentRewrites: true,
      noImageWrites: true,
      factoryGate: "ai_assisted_profile_footnote_visible",
      pvqlGate: "ai_assisted_profile_footnote_visible",
    },
    readyState:
      enrichedAudit.report.summary.fail === 0
        ? "ai_assisted_profile_footnote_standardized_globally"
        : "ai_assisted_profile_footnote_standardization_incomplete",
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-ai-assisted-footnote-standardization.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-ai-assisted-footnote-standardization.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const md = [
    `# Brand Explorer — AI-Assisted Profile Footnote Standardization`,
    ``,
    `- Version: \`${STANDARDIZATION_VERSION}\``,
    `- Generated: ${report.generatedAt}`,
    `- Mode: ${report.apply ? "APPLY (code confirmation)" : "DRY-RUN"}`,
    `- Airtable writes: **${report.airtableWrites}**`,
    `- Approach: ${codeOnly}`,
    ``,
    `## Acceptance`,
    ``,
    `- Ready state: \`${report.readyState}\``,
    `- \`ai_assisted_profile_footnote_standardized_globally\`: **${report.acceptance.ai_assisted_profile_footnote_standardized_globally}**`,
    ``,
    `## Raw audit (native governance chip)`,
    ``,
    `- Pass ${report.rawAuditSummary.pass} / Fail ${report.rawAuditSummary.fail}`,
    `- Footnote visible: ${report.rawAuditSummary.footnoteVisibleCount}`,
    `- Missing: ${report.rawAuditSummary.footnoteMissingCount}`,
    ``,
    `## Enriched audit (global Brand Explorer footnote)`,
    ``,
    `- Pass ${report.enrichedAuditSummary.pass} / Fail ${report.enrichedAuditSummary.fail}`,
    `- Failing slugs: ${report.enrichedFailingSlugs.join(", ") || "none"}`,
    ``,
    `## Guardrails`,
    ``,
    `- No Company Validated changes`,
    `- No Source Library / Registry / Brand Status / release writes`,
    `- No presentation content or image writes`,
    `- Factory + PVQL gate: \`ai_assisted_profile_footnote_visible\``,
    ``,
  ].join("\n");
  fs.writeFileSync(mdPath, `${md}\n`, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-ai-assisted-footnote-standardization.md");
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  fs.writeFileSync(
    docPath,
    [
      `# Brand Explorer — AI-Assisted Profile Footnote Standardization`,
      ``,
      `> Binding: every Brand Explorer profile (public-full + factory-preview + future factory waves) must always render the AI-Assisted Profile trust footnote.`,
      ``,
      `## Required visible format`,
      ``,
      "```",
      "AI-Assisted Profile",
      "Last Reviewed: [MMM D, YYYY] · Source Basis: [Source Basis] · Region: [Region Basis]",
      "```",
      ``,
      `## Implementation`,
      ``,
      `- Module: \`lib/partner-intelligence/brand-explorer-ai-assisted-footnote.js\``,
      `- API enricher: \`api/brand-library.js\` → \`applyBrandExplorerAiAssistedFootnote\``,
      `- Hero renderer: \`public/js/brand-explorer-gold-detail.js\` + \`profile-governance-trust-chip.js\``,
      `- Gate id: \`ai_assisted_profile_footnote_visible\` (PVQL + factory rules)`,
      ``,
      `## Rules`,
      ``,
      `- Default label: **AI-Assisted Profile** (even when Source Basis is Company Materials)`,
      `- Use Company-Validated / Brand Verified wording only when Company Validated is truly true`,
      `- Do not invent CALA-specific without source support`,
      `- Prefer computed fallbacks over Airtable Presentation / governance writes`,
      `- Footnote must not depend on a per-brand Presentation row or External Display Status = Show Trust Label`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      "npm run brand-explorer-ai-assisted-footnote-standardization -- --dry-run",
      "npm run brand-explorer-ai-assisted-footnote-standardization -- --audit",
      "npm run brand-explorer-ai-assisted-footnote-standardization -- --apply \\",
      "  --approve-ai-assisted-footnote-standardization \\",
      "  --confirm-global-rendering-requirement \\",
      "  --confirm-every-brand-explorer-profile \\",
      "  --confirm-no-company-validation-changes \\",
      "  --confirm-no-source-library-status-changes \\",
      "  --confirm-no-registry-approval-changes \\",
      "  --confirm-no-brand-status-changes \\",
      "  --confirm-no-release-field-writes \\",
      "  --confirm-no-presentation-content-rewrites \\",
      "  --confirm-no-image-writes \\",
      "  --confirm-no-cala-claims-without-source-support \\",
      "  --confirm-no-brand-verified-wording-unless-company-validated",
      "```",
      ``,
      `## Acceptance token`,
      ``,
      `\`${report.readyState}\``,
      ``,
    ].join("\n"),
    "utf8"
  );

  return {
    ok: report.acceptance.ai_assisted_profile_footnote_standardized_globally,
    report,
    paths: { jsonPath, mdPath, docPath, rawAudit: rawAudit.paths, enrichedAudit: enrichedAudit.paths },
  };
}
