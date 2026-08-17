/**
 * Lane 2 — Restored pending validation repair (Quality Inn / Radisson family).
 * Scrubs owner-facing PVQL debt; quarantines disclosure-only materials rows.
 * Never writes CV / Source / Registry / Brand Status / release.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  planPvqlFailureScrubForBrand,
  extractPvqlFieldOffenders,
} from "./brand-explorer-pvql-failure-scrub.js";
import {
  MAP_PRESENTATION_FIELDS,
  PRESENTATION_TABLE,
} from "./brand-explorer-residual-owner-copy-remediation.js";
import { BUILT_BLOCKED_IDENTITIES } from "./brand-explorer-built-blocked-content.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";

export const RESTORED_PENDING_VERSION = "restored-pending-validation-repair-v1";
export const RESTORED_PENDING_TARGETS = Object.freeze([
  "quality-inn",
  "radisson",
  "radisson-blu",
  "radisson-red",
]);

export const RESTORED_PENDING_REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-restored-pending-validation-repair",
  "--confirm-targeted-field-fixes-only",
  "--confirm-display-state-repair-only-after-gates-pass",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-broad-rewrites",
]);

const ALLOWED_FIELDS = new Set([
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Owner Objective",
  "Case Summary Interpretation",
  "Case Summary Tags",
  "External Display Status",
]);

const FORBIDDEN_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Brand Status",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const QUARANTINE_SLOTS = new Set([
  "materials.file",
  "economics.opening.financials",
  "commercial.kpi.lens",
]);

const AIRTABLE_TO_API = Object.fromEntries(
  Object.entries(MAP_PRESENTATION_FIELDS).map(([api, at]) => [at, api])
);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function mockRes() {
  return {
    statusCode: 200,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

async function fetchBrand(slug) {
  const id = BUILT_BLOCKED_IDENTITIES[slug]?.recordId || slug;
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: id }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed: ${slug}`);
  return res.payload.brand;
}

function projectPatches(blocks, patches) {
  return (blocks || []).map((b) => {
    const next = { ...b };
    for (const p of patches || []) {
      if (p.recordId !== b.recordId) continue;
      for (const [airtableKey, value] of Object.entries(p.fields || {})) {
        if (airtableKey === "External Display Status") {
          next.externalDisplayStatus = value;
          continue;
        }
        const apiKey = AIRTABLE_TO_API[airtableKey];
        if (apiKey) next[apiKey] = value;
      }
    }
    return next;
  });
}

function buildQuarantinePatches(brand, brandSlug, remainingOffenders) {
  const patches = [];
  const byRecord = new Map();
  for (const off of remainingOffenders || []) {
    if (!off.recordId) continue;
    if (!QUARANTINE_SLOTS.has(nz(off.section))) continue;
    byRecord.set(off.recordId, off);
  }
  for (const [recordId, off] of byRecord) {
    const row = (brand.brandExplorer?.blocks || []).find((b) => b.recordId === recordId);
    if (!row) continue;
    if (/^do not display$/i.test(nz(row.externalDisplayStatus))) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId,
      brandSlug,
      slotKey: off.section,
      reason: `quarantine_disclosure_row:${off.failureType}`,
      fields: { "External Display Status": "Do Not Display" },
      sanitizedPayloadPreview: {
        field: "External Display Status",
        before: row.externalDisplayStatus || null,
        after: "Do Not Display",
      },
    });
  }
  return patches;
}

/** Fill titled cards with empty Body (external_owner empty_visible_cards blocker). */
function buildEmptyCardBodyPatches(brand, brandSlug) {
  const rule = evaluateExternalOwnerReadinessRule(brand.brandExplorer?.blocks || []);
  const patches = [];
  for (const empty of rule.emptyCardRows || []) {
    const row = (brand.brandExplorer?.blocks || []).find((b) => b.recordId === empty.recordId);
    if (!row || nz(row.body)) continue;
    const body =
      nz(row.caseSummaryOverview) ||
      `${nz(row.title)} is a published property reference for owners comparing conversion fit, guest experience intensity, and commercial systems under this brand—use as positioning context, not a performance proxy.`;
    if (!nz(body)) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: empty.recordId,
      brandSlug,
      slotKey: empty.slotKey,
      reason: "empty_visible_card_body_fill",
      fields: { Body: body },
      sanitizedPayloadPreview: {
        field: "Body",
        before: "",
        after: body.slice(0, 120),
      },
    });
  }
  return patches;
}

export async function planRestoredPendingValidationRepair({ brands = null } = {}) {
  const list = brands?.length ? brands : [...RESTORED_PENDING_TARGETS];
  const brandPlans = [];

  for (const slug of list) {
    if (!RESTORED_PENDING_TARGETS.includes(slug)) {
      throw new Error(`Not a restored-pending target: ${slug}`);
    }
    console.log(`[${RESTORED_PENDING_VERSION}] planning ${slug}`);
    const brand = await fetchBrand(slug);
    const scrub = planPvqlFailureScrubForBrand(brand, slug, { force: true });
    const scrubPatches = (scrub.patches || []).filter((p) =>
      Object.keys(p.fields || {}).every((k) => ALLOWED_FIELDS.has(k) && !FORBIDDEN_FIELDS.has(k))
    );
    let projected = projectPatches(brand.brandExplorer?.blocks || [], scrubPatches);
    let projectedBrand = {
      ...brand,
      brandExplorer: { ...(brand.brandExplorer || {}), blocks: projected },
    };
    let remaining = extractPvqlFieldOffenders(projectedBrand, slug);
    const quarantine = buildQuarantinePatches(projectedBrand, slug, remaining);
    if (quarantine.length) {
      projected = projectPatches(projected, quarantine);
      projectedBrand = {
        ...brand,
        brandExplorer: { ...(brand.brandExplorer || {}), blocks: projected },
      };
      remaining = extractPvqlFieldOffenders(projectedBrand, slug);
    }
    const emptyCardPatches = buildEmptyCardBodyPatches(projectedBrand, slug);
    if (emptyCardPatches.length) {
      projected = projectPatches(projected, emptyCardPatches);
      projectedBrand = {
        ...brand,
        brandExplorer: { ...(brand.brandExplorer || {}), blocks: projected },
      };
    }
    const allPatches = [...scrubPatches, ...quarantine, ...emptyCardPatches];
    const externalAfter = evaluateExternalOwnerReadinessRule(projected);
    const remainingGateBlockers = (externalAfter.blockers || []).filter(
      (b) => b !== "not_company_validated"
    );

    brandPlans.push({
      brandSlug: slug,
      brandName: brand.name,
      recordId: brand.id,
      publicFullBefore: brand.shouldRenderFullProfile === true,
      displayStateBefore: brand.brandExplorerDisplayState || null,
      blockersBefore: brand.brandExplorerDisplayBlockers || [],
      ready: brand.readyForActiveProfile === true,
      approved: brand.activeProfileApproved === true,
      founder: brand.founderVisualReviewPass === true,
      offenderCount: scrub.offenderCount,
      scrubPatchCount: scrubPatches.length,
      quarantinePatchCount: quarantine.length,
      emptyCardPatchCount: emptyCardPatches.length,
      patchCount: allPatches.length,
      patches: allPatches,
      fieldRows: scrub.fieldRows || [],
      remainingAfterProjection: remaining.length,
      remainingSample: remaining.slice(0, 8).map((r) => ({
        section: r.section,
        field: r.field,
        failureType: r.failureType,
      })),
      externalOwnerPassAfterProjection: externalAfter.pass === true,
      remainingGateBlockers,
      expectedAfterApply:
        "After owner-copy scrub + empty-card fills + disclosure quarantine, external_owner_copy_fail should clear; with Active Profile Approved → active_profile_ready / shouldRenderFullProfile=true (no CV write).",
    });
    console.log(
      `  offenders=${scrub.offenderCount} scrub=${scrubPatches.length} quarantine=${quarantine.length} emptyCards=${emptyCardPatches.length} remaining=${remaining.length} externalPass=${externalAfter.pass}`
    );
  }

  const unclean = brandPlans.filter((b) => b.remainingAfterProjection > 0);
  const gateUnclean = brandPlans.filter(
    (b) => (b.remainingGateBlockers || []).some((x) => String(x).startsWith("empty_visible") || String(x).startsWith("external_copy") || String(x).startsWith("governance") || String(x).startsWith("modal_") || String(x).startsWith("visible_source") || String(x).startsWith("tab_external"))
  );
  return {
    version: RESTORED_PENDING_VERSION,
    generatedAt: new Date().toISOString(),
    targets: list,
    brands: brandPlans,
    summary: {
      brands: brandPlans.length,
      offenders: brandPlans.reduce((n, b) => n + b.offenderCount, 0),
      patches: brandPlans.reduce((n, b) => n + b.patchCount, 0),
      uncleanAfterProjection: unclean.map((b) => b.brandSlug),
      gateUncleanAfterProjection: gateUnclean.map((b) => b.brandSlug),
    },
    validation: {
      pass: unclean.length === 0 && gateUnclean.length === 0,
      failedChecks: [
        ...unclean.map(
          (b) => `remaining_offenders:${b.brandSlug}:${b.remainingAfterProjection}`
        ),
        ...gateUnclean.map(
          (b) => `remaining_gate_blockers:${b.brandSlug}:${(b.remainingGateBlockers || []).join(",")}`
        ),
      ],
    },
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      brandStatusUntouched: true,
      releaseFieldsUntouched: true,
      displayStateRepairOnlyAfterGatesPass: true,
    },
  };
}

async function airtablePatch({ baseId, apiKey, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH ${recordId} failed: ${res.status}`);
  return json;
}

export async function applyRestoredPendingValidationRepair({
  report,
  apply = false,
  argv = [],
} = {}) {
  if (!apply) return { applied: false, reason: "dry_run_only", results: [] };
  const missing = RESTORED_PENDING_REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  if (missing.length) {
    return { applied: false, reason: "missing_apply_flags", missing, results: [] };
  }
  if (!report?.validation?.pass) {
    return {
      applied: false,
      reason: "validation_failed",
      failedChecks: report?.validation?.failedChecks || [],
      results: [],
    };
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");

  const results = [];
  for (const brand of report.brands || []) {
    for (const patch of brand.patches || []) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_FIELDS.has(key) || !ALLOWED_FIELDS.has(key)) {
          throw new Error(`Refuse field ${key}`);
        }
      }
      const json = await airtablePatch({
        baseId,
        apiKey,
        recordId: patch.recordId,
        fields: patch.fields,
      });
      results.push({
        brandSlug: brand.brandSlug,
        recordId: patch.recordId,
        slotKey: patch.slotKey,
        fields: Object.keys(patch.fields),
        id: json.id,
      });
    }
  }
  return {
    applied: true,
    results,
    companyValidatedUntouched: true,
    brandStatusUntouched: true,
    releaseFieldsUntouched: true,
  };
}

export function writeRestoredPendingValidationReports(report, applyResult = null) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const out = { ...report, applyResult: applyResult || { applied: false } };
  const jsonPath = path.join(reportsDir, "brand-explorer-restored-pending-validation-repair.json");
  fs.writeFileSync(jsonPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");

  const mdPath = path.join(reportsDir, "brand-explorer-restored-pending-validation-repair.md");
  const lines = [
    "# Restored Pending Validation Repair",
    "",
    `Version: \`${report.version}\` · ${report.generatedAt}`,
    `Applied: **${applyResult?.applied === true}**`,
    "",
    `| Brand | Offenders | Patches | Remaining | Display before |`,
    `| --- | ---: | ---: | ---: | --- |`,
  ];
  for (const b of report.brands || []) {
    lines.push(
      `| \`${b.brandSlug}\` | ${b.offenderCount} | ${b.patchCount} | ${b.remainingAfterProjection} | ${b.displayStateBefore || "—"} |`
    );
  }
  lines.push("");
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");

  const perBrandPaths = [];
  for (const b of report.brands || []) {
    const p = path.join(reportsDir, `brand-explorer-restored-pending-validation-${b.brandSlug}.md`);
    const body = [
      `# Restored Pending — ${b.brandName} (\`${b.brandSlug}\`)`,
      "",
      `| Field | Value |`,
      `| --- | --- |`,
      `| Record ID | \`${b.recordId}\` |`,
      `| Public full before | ${b.publicFullBefore} |`,
      `| Display before | ${b.displayStateBefore} |`,
      `| Blockers | ${(b.blockersBefore || []).join(", ") || "—"} |`,
      `| Offenders | ${b.offenderCount} |`,
      `| Scrub patches | ${b.scrubPatchCount} |`,
      `| Quarantine patches | ${b.quarantinePatchCount} |`,
      `| Remaining after projection | ${b.remainingAfterProjection} |`,
      "",
      b.expectedAfterApply,
      "",
      "## Sample remaining",
      "",
      "```json",
      JSON.stringify(b.remainingSample || [], null, 2),
      "```",
      "",
    ].join("\n");
    fs.writeFileSync(p, `${body}\n`, "utf8");
    perBrandPaths.push(p);
  }

  return { jsonPath, mdPath, perBrandPaths };
}
