/**
 * Section Pattern Parity — remediation (Presentation Title/Body/chips only).
 * Forbidden: Company Validated, Source Library status, Registry, release, public restore, images.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import {
  SECTION_PATTERN_PARITY_VERSION,
  SECTION_PATTERN_TRUE_INCOMPLETE,
  resolveSectionPatternBrandIdentity,
  evaluateSectionPatternParity,
} from "./brand-explorer-section-pattern-parity.js";
import {
  getSectionPatternParityContent,
  normalizeMomentumCards,
} from "./brand-explorer-section-pattern-parity-content.js";
import {
  auditSectionPatternParityBrand,
  resolveSectionPatternBrandList,
  ROOT,
} from "./brand-explorer-section-pattern-parity-audit.js";

export const REMEDIATION_VERSION = "section-pattern-parity-remediation-v1";
export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const REPORT_REMEDIATION_MD = "brand-explorer-section-pattern-parity-remediation.md";
export const REPORT_REMEDIATION_JSON = "brand-explorer-section-pattern-parity-remediation.json";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-section-pattern-parity-remediation",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-public-restore-fields",
  "--confirm-section-pattern-only",
  "--confirm-benchmark-pattern-aligned",
]);

const FORBIDDEN_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "External Display Status Restore",
]);

const EXTERNAL_DISPLAY_STATUS_QUARANTINE = "Do Not Display";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

export function parseSectionPatternApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

async function fetchBrandApi(slug) {
  const identity = resolveSectionPatternBrandIdentity(slug);
  const lookupId = identity.recordId || slug;
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
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${slug}`);
  return res.payload.brand;
}

function visibleSlots(blocks, slotKey) {
  return (blocks || []).filter(
    (b) =>
      nz(b.slotKey) === slotKey &&
      b.active !== false &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  );
}

function previewBody(body) {
  const t = nz(body);
  return t.slice(0, 140) + (t.length > 140 ? "…" : "");
}

function validatePatchFields(fields) {
  for (const key of Object.keys(fields || {})) {
    if (FORBIDDEN_FIELDS.has(key)) {
      throw new Error(`Refuse forbidden field: ${key}`);
    }
  }
}

/**
 * Build presentation patches for one brand from content pack + live audit.
 */
export function planSectionPatternParityPatches({ brand, brandSlug, pack, audit }) {
  if (!pack) {
    return {
      brandSlug,
      patches: [],
      skippedReason: "no_content_pack",
      needsRemediation: audit?.pass === false,
    };
  }
  if (SECTION_PATTERN_TRUE_INCOMPLETE.includes(brandSlug)) {
    throw new Error(`Refuse true-incomplete brand: ${brandSlug}`);
  }

  const brandName = brand.name || pack.brandName || brandSlug;
  const recordId = brand.id || brand.recordId;
  const blocks = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const patches = [];
  const sectionsNeeding = (audit?.findings || []).map((f) => f.section);

  const needsMomentum =
    sectionsNeeding.includes("recent_momentum") || pack.replaceMomentum === true;
  const needsGeo =
    sectionsNeeding.includes("geographic_footprint") ||
    Boolean(pack.geoIntro) ||
    (pack.regions || []).length > 0;
  const needsGrowth = sectionsNeeding.includes("growth_priorities");
  const needsContext = sectionsNeeding.includes("portfolio_context");

  if (needsMomentum && pack.replaceMomentum !== false) {
    for (const row of visibleSlots(blocks, "footprint.momentum")) {
      if (!row.recordId) continue;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: row.recordId,
        brandSlug,
        slotKey: "footprint.momentum",
        reason: "quarantine_wrong_pattern_momentum",
        fields: { "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE },
        sanitizedPayloadPreview: {
          "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
          priorTitle: nz(row.title).slice(0, 80),
        },
      });
    }
    for (const row of visibleSlots(blocks, "footprint.momentum_label")) {
      if (!row.recordId) continue;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: row.recordId,
        brandSlug,
        slotKey: "footprint.momentum_label",
        reason: "quarantine_wrong_pattern_momentum_label",
        fields: { "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE },
        sanitizedPayloadPreview: {
          "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
        },
      });
    }

    if (pack.momentumLabel) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        brandSlug,
        slotKey: "footprint.momentum_label",
        reason: "section_pattern_momentum_label",
        fields: {
          "Slot Key": "footprint.momentum_label",
          "Brand Name": brandName,
          Brand: [recordId],
          Active: true,
          "Sort Order": 0,
          Title: "",
          Body: pack.momentumLabel,
        },
        sanitizedPayloadPreview: { Body: previewBody(pack.momentumLabel) },
      });
    }

    for (const card of normalizeMomentumCards(pack)) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        brandSlug,
        slotKey: "footprint.momentum",
        reason: "section_pattern_momentum_card",
        fields: {
          "Slot Key": "footprint.momentum",
          "Brand Name": brandName,
          Brand: [recordId],
          Active: true,
          "Sort Order": card.sort ?? 1,
          Title: card.title,
          Body: card.body,
        },
        sanitizedPayloadPreview: {
          Title: card.title,
          Body: previewBody(card.body),
          wordCount: wordCount(card.body),
        },
      });
    }
  }

  if (needsGeo && pack.geoIntro) {
    const existing = visibleSlots(blocks, "footprint.geo_intro")[0];
    const fields = { Title: "Geographic footprint", Body: pack.geoIntro };
    if (existing?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: existing.recordId,
        brandSlug,
        slotKey: "footprint.geo_intro",
        reason: "section_pattern_geo_intro",
        fields,
        sanitizedPayloadPreview: { Body: previewBody(pack.geoIntro), wordCount: wordCount(pack.geoIntro) },
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        brandSlug,
        slotKey: "footprint.geo_intro",
        reason: "section_pattern_geo_intro_create",
        fields: {
          "Slot Key": "footprint.geo_intro",
          "Brand Name": brandName,
          Brand: [recordId],
          Active: true,
          "Sort Order": 10,
          ...fields,
        },
        sanitizedPayloadPreview: { Body: previewBody(pack.geoIntro) },
      });
    }
  }

  if (needsGeo) {
    for (const region of pack.regions || []) {
      const existing = visibleSlots(blocks, region.slotKey)[0];
      const fields = { Title: region.title || "", Body: region.body || "" };
      if (existing?.recordId) {
        if (nz(existing.body) === nz(region.body) && nz(existing.title) === nz(region.title)) continue;
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: existing.recordId,
          brandSlug,
          slotKey: region.slotKey,
          reason: "section_pattern_region_card",
          fields,
          sanitizedPayloadPreview: {
            Title: region.title,
            Body: previewBody(region.body),
            wordCount: wordCount(region.body),
          },
        });
      } else {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "POST",
          recordId: null,
          brandSlug,
          slotKey: region.slotKey,
          reason: "section_pattern_region_card_create",
          fields: {
            "Slot Key": region.slotKey,
            "Brand Name": brandName,
            Brand: [recordId],
            Active: true,
            "Sort Order": region.sort ?? 11,
            ...fields,
          },
          sanitizedPayloadPreview: { Title: region.title, Body: previewBody(region.body) },
        });
      }
    }
  }

  if (needsGrowth && pack.growthThemes) {
    const existing = visibleSlots(blocks, "footprint.growth_themes")[0];
    const fields = { Body: pack.growthThemes };
    patches.push({
      table: PRESENTATION_TABLE,
      action: existing?.recordId ? "PATCH" : "POST",
      recordId: existing?.recordId || null,
      brandSlug,
      slotKey: "footprint.growth_themes",
      reason: "section_pattern_growth_themes",
      fields: existing?.recordId
        ? fields
        : {
            "Slot Key": "footprint.growth_themes",
            "Brand Name": brandName,
            Brand: [recordId],
            Active: true,
            "Sort Order": 20,
            Title: "Growth themes",
            ...fields,
          },
      sanitizedPayloadPreview: { Body: previewBody(pack.growthThemes) },
    });
  }

  if (needsGrowth && pack.growthEditorial) {
    const existing = visibleSlots(blocks, "footprint.growth_editorial")[0];
    const fields = { Body: pack.growthEditorial };
    patches.push({
      table: PRESENTATION_TABLE,
      action: existing?.recordId ? "PATCH" : "POST",
      recordId: existing?.recordId || null,
      brandSlug,
      slotKey: "footprint.growth_editorial",
      reason: "section_pattern_growth_editorial",
      fields: existing?.recordId
        ? fields
        : {
            "Slot Key": "footprint.growth_editorial",
            "Brand Name": brandName,
            Brand: [recordId],
            Active: true,
            "Sort Order": 21,
            Title: "Growth editorial",
            ...fields,
          },
      sanitizedPayloadPreview: { Body: previewBody(pack.growthEditorial) },
    });
  }

  if (needsContext && pack.portfolioContext?.body) {
    const existing = visibleSlots(blocks, "overview.portfolio_context")[0];
    const fields = {
      Title: pack.portfolioContext.title || "Portfolio context",
      Body: pack.portfolioContext.body,
    };
    patches.push({
      table: PRESENTATION_TABLE,
      action: existing?.recordId ? "PATCH" : "POST",
      recordId: existing?.recordId || null,
      brandSlug,
      slotKey: "overview.portfolio_context",
      reason: "section_pattern_portfolio_context",
      fields: existing?.recordId
        ? fields
        : {
            "Slot Key": "overview.portfolio_context",
            "Brand Name": brandName,
            Brand: [recordId],
            Active: true,
            "Sort Order": 90,
            ...fields,
          },
      sanitizedPayloadPreview: { Body: previewBody(pack.portfolioContext.body) },
    });
  }

  for (const p of patches) validatePatchFields(p.fields);

  return {
    brandSlug,
    brandName,
    recordId,
    auditPassBefore: audit?.pass === true,
    sectionStatusesBefore: audit
      ? Object.fromEntries(Object.entries(audit.sections || {}).map(([k, v]) => [k, v.status]))
      : {},
    patches,
    fieldMapping: {
      PresentationTitle: "Title",
      PresentationBody: "Body",
      ExternalDisplayStatus: "External Display Status (quarantine only)",
      SlotKey: "Slot Key",
    },
    validation: {
      pass: patches.every((p) => p.table === PRESENTATION_TABLE),
      failedChecks: [],
    },
  };
}

export async function planSectionPatternParityRemediation({ brands = null } = {}) {
  const brandList = resolveSectionPatternBrandList(brands);
  const brandPlans = [];
  for (const slug of brandList) {
    const audit = await auditSectionPatternParityBrand(slug);
    if (audit.pass) {
      brandPlans.push({
        brandSlug: slug,
        brandName: audit.brandName,
        skippedReason: "already_passes_section_pattern_parity",
        patches: [],
        auditPassBefore: true,
        sectionStatusesBefore: Object.fromEntries(
          Object.entries(audit.sections).map(([k, v]) => [k, v.status])
        ),
      });
      continue;
    }
    const brand = await fetchBrandApi(slug);
    const pack = getSectionPatternParityContent(slug);
    const plan = planSectionPatternParityPatches({ brand, brandSlug: slug, pack, audit });
    brandPlans.push({ ...plan, tableRows: audit.tableRows, findings: audit.findings });
  }

  const withPatches = brandPlans.filter((b) => (b.patches || []).length > 0);
  const missingPack = brandPlans.filter(
    (b) => b.skippedReason === "no_content_pack" || (b.needsRemediation && !(b.patches || []).length)
  );

  return {
    version: REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    brands: brandPlans,
    summary: {
      brandsPlanned: brandPlans.length,
      brandsWithPatches: withPatches.length,
      patchCount: withPatches.reduce((n, b) => n + b.patches.length, 0),
      alreadyPassing: brandPlans.filter((b) => b.skippedReason === "already_passes_section_pattern_parity")
        .length,
      missingContentPack: missingPack.map((b) => b.brandSlug),
    },
    validation: {
      pass: missingPack.length === 0,
      failedChecks: missingPack.map((b) => `missing_pack_or_patches:${b.brandSlug}`),
    },
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      releaseFieldsUntouched: true,
      publicRestoreUntouched: true,
      sectionPatternOnly: true,
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

export async function applySectionPatternParityRemediation({
  report,
  apply = false,
  argv = [],
} = {}) {
  const flags = parseSectionPatternApplyFlags(argv);
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
    if (SECTION_PATTERN_TRUE_INCOMPLETE.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to true-incomplete ${brand.brandSlug}`);
    }
    for (const patch of brand.patches || []) {
      validatePatchFields(patch.fields);
      if (patch.table !== PRESENTATION_TABLE) {
        throw new Error(`Refuse unexpected table: ${patch.table}`);
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
    releaseFieldsUntouched: true,
    publicRestoreUntouched: true,
    sectionPatternOnly: true,
  };
}

export async function verifySectionPatternParityBrand(brandSlug) {
  const result = await auditSectionPatternParityBrand(brandSlug);
  return {
    brandSlug,
    pass: result.pass === true,
    gates: result.gates,
    sections: Object.fromEntries(
      Object.entries(result.sections).map(([k, v]) => [k, { status: v.status, pass: v.pass }])
    ),
  };
}

export function writeSectionPatternParityRemediationReports(report, applyResult = null) {
  const jsonPath = path.join(ROOT, "reports", REPORT_REMEDIATION_JSON);
  const payload = { ...report, applyResult };
  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2));

  const lines = [
    `# Section Pattern Parity Remediation`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    `Applied: ${applyResult?.applied === true}`,
    ``,
    `## Summary`,
    ``,
    `- Brands planned: ${report.summary.brandsPlanned}`,
    `- Brands with patches: ${report.summary.brandsWithPatches}`,
    `- Patch count: ${report.summary.patchCount}`,
    `- Already passing: ${report.summary.alreadyPassing}`,
    `- Missing pack: ${report.summary.missingContentPack.join(", ") || "(none)"}`,
    ``,
    `| Brand | Section | Current Pattern | Expected Pattern | Status | Failure Reason | Proposed Patch | Benchmark Reference |`,
    `| --- | --- | --- | --- | --- | --- | --- | --- |`,
  ];

  for (const brand of report.brands || []) {
    for (const row of brand.tableRows || []) {
      lines.push(
        `| ${row.brand} | ${row.section} | ${row.currentPattern} | ${row.expectedPattern} | **${row.status}** | ${(row.failureReason || "").replace(/\|/g, "/")} | ${(row.proposedPatch || "").replace(/\|/g, "/")} | ${row.benchmarkReference || ""} |`
      );
    }
    if ((brand.patches || []).length) {
      lines.push(``, `### Patches — ${brand.brandSlug} (${brand.patches.length})`);
      for (const p of brand.patches) {
        lines.push(
          `- \`${p.action}\` ${p.slotKey} ${p.recordId || "(create)"} — ${p.reason}`
        );
      }
    }
  }

  lines.push(
    ``,
    `## Guardrails`,
    ``,
    `- Presentation Title/Body/chips/quarantine only`,
    `- No Company Validated / Source / Registry / release / public restore`,
    ``
  );

  const mdPath = path.join(ROOT, "reports", REPORT_REMEDIATION_MD);
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}

/** Simulate patched payload for local verify (no Airtable). */
export function projectParityPatchesOntoBlocks(blocks, patches) {
  const next = (blocks || []).map((b) => ({ ...b }));
  for (const patch of patches || []) {
    if (patch.action === "PATCH" && patch.recordId) {
      const idx = next.findIndex((b) => b.recordId === patch.recordId);
      if (idx >= 0) {
        const fields = patch.fields || {};
        if (fields.Body != null) next[idx].body = fields.Body;
        if (fields.Title != null) next[idx].title = fields.Title;
        if (fields["External Display Status"] != null) {
          next[idx].externalDisplayStatus = fields["External Display Status"];
        }
      }
    } else if (patch.action === "POST") {
      const fields = patch.fields || {};
      next.push({
        recordId: `projected-${patch.slotKey}-${next.length}`,
        slotKey: fields["Slot Key"] || patch.slotKey,
        title: fields.Title || "",
        body: fields.Body || "",
        sort: fields["Sort Order"] || 0,
        active: true,
        externalDisplayStatus: "",
      });
    }
  }
  return next.filter((b) => !/do not display|internal only/i.test(nz(b.externalDisplayStatus)));
}

export async function evaluateProjectedParity(brandSlug, patches) {
  const brand = await fetchBrandApi(brandSlug);
  const projected = projectParityPatchesOntoBlocks(brand.brandExplorer?.blocks || [], patches);
  const html = renderBrandExplorerHtmlForTest(
    { ...brand, brandExplorer: { ...(brand.brandExplorer || {}), blocks: projected } },
    { allPanels: true, internalPreview: true }
  );
  return evaluateSectionPatternParity({
    brandSlug,
    brandName: brand.name,
    presentationRows: projected.filter(isOwnerFacingPresentationRow),
    html,
  });
}
