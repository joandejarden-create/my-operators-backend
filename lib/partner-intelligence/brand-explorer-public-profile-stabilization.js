/**
 * Brand Explorer — Public Profile Stabilization
 *
 * Scope: only public-full profiles that currently fail PVQL gates.
 * Presentation field-gate depth + residual owner-copy scrub.
 * Never writes Company Validated, Source Library, Registry, Founder/Active release,
 * or protected passing public brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getLegacySeedBrand } from "./brand-explorer-legacy-approved-profile-reconciliation.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import {
  isOwnerFacingPresentationRow,
  evaluateBrandPublicVisibility,
} from "./brand-explorer-public-visibility-quality-lock.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  buildResidualOwnerCopyPatchPlan,
  scrubResidualOwnerFacingCopy,
} from "./brand-explorer-residual-owner-copy-remediation.js";
import {
  PUBLIC_STABILIZATION_CONTENT_BY_SLUG,
  PUBLIC_STABILIZATION_PROTECTED_PASSING,
  PUBLIC_STABILIZATION_TARGETS,
  PUBLIC_STABILIZATION_PRIMARY_TARGETS,
  PUBLIC_STABILIZATION_LEGACY_TARGETS,
} from "./brand-explorer-public-profile-stabilization-content.js";

export const STABILIZATION_VERSION = "public-profile-stabilization-v1";
export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const REPORT_JSON = "brand-explorer-public-profile-stabilization.json";
export const REPORT_MD = "brand-explorer-public-profile-stabilization.md";
export const BASELINE_JSON = "brand-explorer-public-visibility-baseline.json";
export const BASELINE_MD = "brand-explorer-public-visibility-baseline.md";

export {
  PUBLIC_STABILIZATION_TARGETS,
  PUBLIC_STABILIZATION_PRIMARY_TARGETS,
  PUBLIC_STABILIZATION_LEGACY_TARGETS,
  PUBLIC_STABILIZATION_PROTECTED_PASSING,
};

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-public-profile-stabilization",
  "--confirm-public-full-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-release-field-changes",
  "--confirm-protected-passing-unchanged",
  "--confirm-presentation-only",
]);

const FORBIDDEN_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const EXTERNAL_DISPLAY_STATUS_QUARANTINE = "Do Not Display";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

export function parseStabilizationApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

/** Fallback identity from live stabilization inventory (when configs omit a public-full brand). */
export const STABILIZATION_BRAND_IDENTITY = Object.freeze({
  "everhome-suites": { recordId: "recqkkrsevi4r9ibj", name: "Everhome Suites" },
  kimpton: { recordId: "recCKuXCmGvxHPfb3", name: "Kimpton Hotels" },
  "design-hotels": { recordId: "rec02zPClpWUTCyXM", name: "Design Hotels" },
  ascend: { recordId: "reclkgOzvAcBheUSo", name: "Ascend Hotel Collection" },
  "comfort-inn-suites": { recordId: "recOzH5iAE1xEjyD0", name: "Comfort Inn & Suites" },
  "curio-collection": { recordId: "receQkxgjlezsc1xg", name: "Curio Collection by Hilton" },
  "tribute-portfolio": { recordId: "recCvV0PuZOi8c3hC", name: "Tribute Portfolio" },
});

export function resolveStabilizationBrandMeta(slug) {
  const active = getActiveProfileBrandConfig(slug);
  const legacy = getLegacySeedBrand(slug);
  const fallback = STABILIZATION_BRAND_IDENTITY[slug] || null;
  const recordId = active?.recordId || legacy?.recordId || fallback?.recordId || null;
  if (!recordId) return null;
  return {
    slug,
    recordId,
    name: active?.name || legacy?.name || fallback?.name || slug,
    cohort: PUBLIC_STABILIZATION_PRIMARY_TARGETS.includes(slug)
      ? "primary_release"
      : "restored_legacy_public",
  };
}

async function fetchBrandApi(slug) {
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
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${slug}`);
  return res.payload.brand;
}

function findVisibleSlots(blocks, slotKey) {
  return (blocks || []).filter(
    (b) =>
      nz(b.slotKey) === slotKey &&
      b.active !== false &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  );
}

function scrubBody(text, slotKey, brandSlug) {
  const scrub = scrubResidualOwnerFacingCopy(text, { slotKey, brandSlug });
  return scrub.after || nz(text);
}

function caseFieldsFromItem(item, slotKey, brandSlug) {
  const caseFields = {};
  const map = [
    ["caseSummaryOverview", "Case Summary Overview"],
    ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
    ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
    ["caseSummaryInterpretation", "Case Summary Interpretation"],
    ["caseSummaryTags", "Case Summary Tags"],
  ];
  for (const [api, airtable] of map) {
    if (item[api]) caseFields[airtable] = scrubBody(item[api], slotKey, brandSlug);
  }
  return caseFields;
}

function buildFieldGatePatches({ brandSlug, brandName, recordId, blocks, content }) {
  const patches = [];
  const blockers = [];
  const touchedSlots = [];
  const slotIndex = new Map();

  for (const item of content || []) {
    const slotKey = nz(item.slotKey);
    if (!slotKey) {
      blockers.push({ slotKey: null, reason: "missing_slot_key" });
      continue;
    }

    const idx = slotIndex.get(slotKey) || 0;
    slotIndex.set(slotKey, idx + 1);

    let body = scrubBody(item.body, slotKey, brandSlug);
    let title = item.title ? scrubBody(item.title, slotKey, brandSlug) : "";
    const caseFields = caseFieldsFromItem(item, slotKey, brandSlug);

    const corpus = [title, body, ...Object.values(caseFields)].join("\n");
    const forbidden = scanForbiddenLanguage(corpus);
    if (forbidden.length) {
      blockers.push({
        slotKey,
        index: idx,
        forbidden: forbidden.map((h) => h.id || h.label),
      });
      continue;
    }

    const existing = findVisibleSlots(blocks, slotKey);
    const primary = existing[idx] || null;
    const fields = {
      Body: body,
      ...(title ? { Title: title } : {}),
      ...caseFields,
    };

    const unchanged =
      primary &&
      nz(primary.body) === nz(body) &&
      (!title || nz(primary.title) === nz(title)) &&
      Object.entries(caseFields).every(([k, v]) => {
        const apiKey =
          k === "Case Summary Overview"
            ? "caseSummaryOverview"
            : k === "Case Summary Brand Relevance"
              ? "caseSummaryBrandRelevance"
              : k === "Case Summary Owner Objective"
                ? "caseSummaryOwnerObjective"
                : k === "Case Summary Interpretation"
                  ? "caseSummaryInterpretation"
                  : k === "Case Summary Tags"
                    ? "caseSummaryTags"
                    : null;
        return apiKey ? nz(primary[apiKey]) === nz(v) : true;
      });

    if (!unchanged) {
      if (primary?.recordId) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: primary.recordId,
          brandSlug,
          slotKey,
          reason: "public_profile_stabilization_field_gate",
          fields,
          fieldMapping: Object.fromEntries(
            Object.keys(fields).map((k) => [k, `Brand Explorer Presentation.${k}`])
          ),
          sanitizedPayloadPreview: {
            Title: title ? title.slice(0, 80) : undefined,
            Body: body.slice(0, 120) + (body.length > 120 ? "…" : ""),
            wordCount: wordCount(body),
            caseSummaryFields: Object.keys(caseFields),
          },
        });
      } else {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "POST",
          recordId: null,
          brandSlug,
          slotKey,
          reason: "public_profile_stabilization_create_slot",
          fields: {
            "Slot Key": slotKey,
            "Brand Name": brandName,
            Brand: [recordId],
            Active: true,
            "Sort Order": item.sortOrder ?? 20 + idx,
            Title: title || "",
            Body: body,
            ...caseFields,
          },
          fieldMapping: {
            "Slot Key": "Brand Explorer Presentation.Slot Key",
            Body: "Brand Explorer Presentation.Body",
          },
          sanitizedPayloadPreview: {
            Title: title ? title.slice(0, 80) : undefined,
            Body: body.slice(0, 120) + (body.length > 120 ? "…" : ""),
            wordCount: wordCount(body),
          },
        });
      }
      touchedSlots.push(slotKey);
    }

    if (slotKey === "footprint.portfolio_mix" && existing.length > 1) {
      for (const extra of existing.slice(1)) {
        if (!extra.recordId) continue;
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: extra.recordId,
          brandSlug,
          slotKey,
          reason: "quarantine_extra_portfolio_mix_chip",
          fields: { "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE },
          fieldMapping: {
            "External Display Status": "Brand Explorer Presentation.External Display Status",
          },
          sanitizedPayloadPreview: {
            "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
          },
        });
      }
    }
  }

  return { patches, blockers, touchedSlots };
}

function mergeResidualPatches(brandSlug, blocks, contentPatches) {
  const byId = new Map();
  for (const p of contentPatches) {
    if (p.recordId) byId.set(p.recordId, p);
  }
  const projected = (blocks || []).map((b) => {
    const p = byId.get(b.recordId);
    if (!p?.fields) return b;
    const next = { ...b };
    if (p.fields.Body != null) next.body = p.fields.Body;
    if (p.fields.Title != null) next.title = p.fields.Title;
    if (p.fields["Case Summary Overview"] != null) {
      next.caseSummaryOverview = p.fields["Case Summary Overview"];
    }
    if (p.fields["External Display Status"] === EXTERNAL_DISPLAY_STATUS_QUARANTINE) {
      next.externalDisplayStatus = EXTERNAL_DISPLAY_STATUS_QUARANTINE;
    }
    return next;
  });

  const residual = buildResidualOwnerCopyPatchPlan({
    brandSlug,
    presentationRows: projected.filter(isOwnerFacingPresentationRow),
  });
  const contentIds = new Set(contentPatches.filter((p) => p.recordId).map((p) => p.recordId));
  const residualPatches = [];
  const grouped = new Map();
  for (const p of residual.patches || []) {
    if (!p.recordId || !p.safeForGenericApply) continue;
    if (contentIds.has(p.recordId) && p.field === "Body") continue;
    if (!grouped.has(p.recordId)) grouped.set(p.recordId, {});
    grouped.get(p.recordId)[p.field] = p.after;
  }
  for (const [recordId, fields] of grouped.entries()) {
    const slotKey = projected.find((b) => b.recordId === recordId)?.slotKey || null;
    residualPatches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId,
      brandSlug,
      slotKey,
      reason: "public_profile_stabilization_residual_scrub",
      fields,
      fieldMapping: Object.fromEntries(
        Object.keys(fields).map((k) => [k, `Brand Explorer Presentation.${k}`])
      ),
      sanitizedPayloadPreview: Object.fromEntries(
        Object.entries(fields).map(([k, v]) => [k, String(v).slice(0, 100)])
      ),
    });
  }
  return { residualPatches, residualSummary: residual.summary };
}

export async function planBrandPublicProfileStabilization(brandSlug) {
  if (PUBLIC_STABILIZATION_PROTECTED_PASSING.includes(brandSlug)) {
    throw new Error(`Refuse protected passing public brand: ${brandSlug}`);
  }
  if (!PUBLIC_STABILIZATION_TARGETS.includes(brandSlug)) {
    throw new Error(
      `Stabilization targets only: ${PUBLIC_STABILIZATION_TARGETS.join(", ")} (got ${brandSlug})`
    );
  }

  const meta = resolveStabilizationBrandMeta(brandSlug);
  if (!meta?.recordId) throw new Error(`No brand meta for ${brandSlug}`);

  const content = PUBLIC_STABILIZATION_CONTENT_BY_SLUG[brandSlug];
  if (!content?.length) {
    return {
      brandSlug,
      brandName: meta.name,
      recordId: meta.recordId,
      cohort: meta.cohort,
      blocked: true,
      blockers: [{ reason: "empty_content_pack" }],
      patches: [],
      validation: { pass: false, failedChecks: ["empty_content_pack"] },
    };
  }

  const brand = await fetchBrandApi(brandSlug);
  if (brand.shouldRenderFullProfile !== true) {
    return {
      brandSlug,
      brandName: brand.name || meta.name,
      recordId: brand.id || meta.recordId,
      blocked: true,
      blockers: [{ reason: "not_public_full_profile" }],
      patches: [],
      validation: { pass: false, failedChecks: ["not_public_full_profile"] },
    };
  }

  const brandName = brand.name || meta.name;
  const recordId = brand.id || meta.recordId;
  const blocks = brand.brandExplorer?.blocks || [];
  const ownerFacing = blocks.filter(isOwnerFacingPresentationRow);

  const fieldGate = buildFieldGatePatches({
    brandSlug,
    brandName,
    recordId,
    blocks: ownerFacing,
    content,
  });
  const { residualPatches, residualSummary } = mergeResidualPatches(
    brandSlug,
    ownerFacing,
    fieldGate.patches
  );

  const patches = [...fieldGate.patches, ...residualPatches];
  const blockers = [...fieldGate.blockers];
  const failedChecks = [];
  if (blockers.length) failedChecks.push("content_forbidden_or_invalid");
  if (!content.length) failedChecks.push("empty_content_pack");

  const html = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: false,
  });
  const beforeEval = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerFacing,
    html,
    brandSlug,
  });

  return {
    brandSlug,
    brandName,
    recordId,
    cohort: meta.cohort,
    shouldRenderFullProfile: true,
    before: {
      failFindings: beforeEval.failFindings,
      emptyRenderFailFindings: beforeEval.emptyRenderFailFindings,
      auditPass: beforeEval.auditPass,
      completenessPass: beforeEval.completeness?.auditPass === true,
      provenancePass: beforeEval.provenance?.pass === true,
      uniquenessPass: beforeEval.imageUniqueness?.pass === true,
      rolePass: beforeEval.imageRoleMatch?.pass === true,
    },
    patches,
    blockers,
    blocked: blockers.length > 0,
    touchedSlots: [...new Set(fieldGate.touchedSlots)],
    residualSummary,
    validation: {
      pass: failedChecks.length === 0,
      failedChecks,
      checks: {
        target_in_scope: true,
        public_full_only: true,
        presentation_only: patches.every((p) => p.table === PRESENTATION_TABLE),
        no_forbidden_fields: patches.every((p) =>
          Object.keys(p.fields || {}).every((k) => !FORBIDDEN_FIELDS.has(k))
        ),
        protected_passing_untouched: true,
        content_clean: blockers.length === 0,
      },
    },
    fieldMappingUsed: {
      table: PRESENTATION_TABLE,
      body: "Body",
      title: "Title",
      caseSummary: [
        "Case Summary Overview",
        "Case Summary Brand Relevance",
        "Case Summary Owner Objective",
        "Case Summary Interpretation",
        "Case Summary Tags",
      ],
    },
  };
}

export async function planPublicProfileStabilization({
  brands = PUBLIC_STABILIZATION_TARGETS,
} = {}) {
  const plans = [];
  for (const slug of brands) {
    plans.push(await planBrandPublicProfileStabilization(slug));
  }
  const patchCount = plans.reduce((n, p) => n + (p.patches?.length || 0), 0);
  const blocked = plans.filter((p) => p.blocked);
  return {
    version: STABILIZATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands: plans,
    summary: {
      brandCount: plans.length,
      patchCount,
      blockedCount: blocked.length,
      primaryTargets: PUBLIC_STABILIZATION_PRIMARY_TARGETS,
      legacyTargets: PUBLIC_STABILIZATION_LEGACY_TARGETS,
      protectedPassing: PUBLIC_STABILIZATION_PROTECTED_PASSING,
    },
    validation: {
      pass: blocked.length === 0 && plans.every((p) => p.validation?.pass),
      failedChecks: blocked.flatMap((p) => p.validation?.failedChecks || []),
    },
  };
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json.error?.message || `${method} failed ${recordId || table}: ${res.status}`);
  }
  return json;
}

export async function applyPublicProfileStabilization({
  report,
  apply = false,
  argv = [],
} = {}) {
  const flags = parseStabilizationApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flags };
  if (!flags.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flags.missing, flags };
  }
  if (!report.validation?.pass) {
    return {
      applied: false,
      reason: "validation_failed",
      failedChecks: report.validation?.failedChecks || [],
    };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const results = [];
  for (const brand of report.brands || []) {
    if (PUBLIC_STABILIZATION_PROTECTED_PASSING.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to protected passing brand ${brand.brandSlug}`);
    }
    for (const patch of brand.patches || []) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_FIELDS.has(key)) {
          throw new Error(`Refuse forbidden field write: ${key}`);
        }
      }
      if (patch.table !== PRESENTATION_TABLE) {
        throw new Error(`Refuse unexpected table: ${patch.table}`);
      }
      if (!PUBLIC_STABILIZATION_TARGETS.includes(patch.brandSlug)) {
        throw new Error(`Refuse out-of-scope brand patch: ${patch.brandSlug}`);
      }
      const method = patch.action === "POST" ? "POST" : "PATCH";
      const json = await airtableWrite({
        baseId,
        apiKey,
        table: patch.table,
        recordId: patch.recordId,
        fields: patch.fields,
        method,
      });
      results.push({
        brandSlug: patch.brandSlug,
        recordId: patch.recordId || json.id || null,
        action: method,
        slotKey: patch.slotKey,
        reason: patch.reason,
        fields: Object.keys(patch.fields),
      });
    }
  }

  return {
    applied: true,
    results,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    protectedPassingUntouched: true,
  };
}

export async function verifyPublicProfileStabilization(brands = PUBLIC_STABILIZATION_TARGETS) {
  const rows = [];
  for (const slug of brands) {
    const pvql = await evaluateBrandPublicVisibility(slug);
    rows.push({
      slug,
      lockPass: pvql.lockPass === true,
      failures: pvql.failures || [],
      cohort: pvql.cohort,
      shouldRenderFullProfile: pvql.shouldRenderFullProfile === true,
    });
  }
  return {
    allPass: rows.every((r) => r.lockPass === true),
    brands: rows,
  };
}

export function writeStabilizationReports(report, applyResult = null) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const payload = { ...report, applyResult: applyResult || null, dryRun: !applyResult?.applied };
  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2));

  const lines = [
    `# Public Profile Stabilization`,
    ``,
    `Version: \`${STABILIZATION_VERSION}\``,
    `Generated: ${report.generatedAt}`,
    `Patches: **${report.summary?.patchCount ?? 0}** · Blocked brands: **${report.summary?.blockedCount ?? 0}**`,
    `Applied: **${applyResult?.applied === true}**`,
    ``,
    `## Targets`,
    `- Primary: ${PUBLIC_STABILIZATION_PRIMARY_TARGETS.join(", ")}`,
    `- Legacy: ${PUBLIC_STABILIZATION_LEGACY_TARGETS.join(", ")}`,
    `- Protected (no writes): ${PUBLIC_STABILIZATION_PROTECTED_PASSING.join(", ")}`,
    ``,
    `## Brand plans`,
  ];
  for (const b of report.brands || []) {
    lines.push(`### ${b.brandName || b.brandSlug} (\`${b.brandSlug}\`)`);
    lines.push(
      `- Before failFindings=${b.before?.failFindings ?? "—"} empty=${b.before?.emptyRenderFailFindings ?? "—"} auditPass=${b.before?.auditPass}`
    );
    lines.push(`- Patches: ${(b.patches || []).length} · Blocked: ${b.blocked === true}`);
    if (b.blockers?.length) {
      lines.push(`- Blockers: ${JSON.stringify(b.blockers).slice(0, 300)}`);
    }
    for (const p of (b.patches || []).slice(0, 40)) {
      lines.push(
        `  - ${p.action} \`${p.slotKey}\` ${p.recordId || "(create)"} — ${p.reason}`
      );
    }
    if ((b.patches || []).length > 40) lines.push(`  - … +${b.patches.length - 40} more`);
    lines.push(``);
  }

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}

/**
 * Freeze Public Visibility Baseline once every public-full profile lockPass=true.
 */
export async function freezePublicVisibilityBaseline({
  publicFullSlugs = null,
} = {}) {
  const verifySlugs = publicFullSlugs?.length
    ? publicFullSlugs
    : [
        ...PUBLIC_STABILIZATION_PROTECTED_PASSING,
        ...PUBLIC_STABILIZATION_TARGETS,
      ];
  const verification = await verifyPublicProfileStabilization(verifySlugs);
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });

  const baseline = {
    version: "public-visibility-baseline-v1",
    frozenAt: new Date().toISOString(),
    scope: "public_full_profiles_only",
    freezeReady: verification.allPass === true,
    brands: verification.brands,
    gatesRequired: [
      "rendered_field_completeness",
      "no_empty_rendered_components",
      "tab_factory_audit",
      "source_provenance_by_tab",
      "image_uniqueness",
      "image_role_match",
      "public_visibility_quality_lock",
    ],
    note: verification.allPass
      ? "All public-full profiles pass. Tab Factory is the build process for future brands."
      : "Not frozen — remaining public-full failures listed below.",
  };

  const jsonPath = path.join(reportsDir, BASELINE_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(baseline, null, 2));

  const md = [
    `# Public Visibility Baseline`,
    ``,
    `Frozen: **${baseline.freezeReady}** · ${baseline.frozenAt}`,
    `Scope: ${baseline.scope}`,
    ``,
    `| Brand | lockPass | Failures |`,
    `| --- | --- | --- |`,
    ...baseline.brands.map(
      (b) => `| \`${b.slug}\` | ${b.lockPass} | ${(b.failures || []).join("; ") || "—"} |`
    ),
    ``,
    baseline.note,
  ].join("\n");
  const mdPath = path.join(reportsDir, BASELINE_MD);
  fs.writeFileSync(mdPath, md);
  return { baseline, jsonPath, mdPath };
}
