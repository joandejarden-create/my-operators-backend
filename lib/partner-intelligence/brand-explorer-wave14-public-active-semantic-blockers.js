/**
 * Wave 14 — Public Active semantic blocker extraction (read-only).
 *
 * Scans live Presentation rows for the eight active Wave 14 brands for
 * PVQL / 24-tab / forbidden-language blockers. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  isOwnerFacingPresentationRow,
  scanOwnerFacingForbiddenLanguage,
} from "./brand-explorer-public-visibility-quality-lock.js";
import {
  WAVE14_PARTIAL_PROMOTION_SLUGS,
  WAVE14_HELD_PROMOTION_SLUG,
} from "./brand-explorer-wave14-factory-plan.js";

export const WAVE14_PUBLIC_ACTIVE_SEMANTIC_BLOCKERS_VERSION =
  "wave14-public-active-semantic-blockers-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

/** Extended semantic forbidden phrases beyond V40B LOI/FDD/ADR set. */
export const WAVE14_SEMANTIC_FORBIDDEN_RES = Object.freeze([
  { id: "source_pack", re: /\bsource pack\b/i, severity: "critical" },
  { id: "current_source_pack", re: /\bcurrent source pack\b/i, severity: "critical" },
  { id: "source_supported", re: /\bsource-supported\b/i, severity: "critical" },
  { id: "steward_matched", re: /\bsteward-matched\b/i, severity: "critical" },
  { id: "steward_confirmed", re: /\bsteward-confirmed\b/i, severity: "critical" },
  { id: "steward", re: /\bsteward\b/i, severity: "critical" },
  { id: "visual_diligence", re: /\bvisual diligence\b/i, severity: "critical" },
  { id: "underwriting_lane", re: /\bunderwriting lane\b/i, severity: "critical" },
  { id: "brand_lane_evidence", re: /\bbrand-lane evidence\b/i, severity: "critical" },
  { id: "own_brand_lane", re: /\bown brand lane\b/i, severity: "critical" },
  { id: "directory_card", re: /\bdirectory card\b/i, severity: "critical" },
  { id: "development_page_confirms", re: /\bdevelopment page confirms\b/i, severity: "high" },
  { id: "brand_site_confirms", re: /\bbrand site confirms\b/i, severity: "high" },
  { id: "use_labeled_example", re: /\bUse this labeled example\b/i, severity: "critical" },
  { id: "confirm_owner", re: /\bconfirm (the )?owner\b/i, severity: "high" },
  { id: "confirm_operator", re: /\bconfirm (the )?operator\b/i, severity: "high" },
  { id: "keep_sibling", re: /\bkeep sibling\b/i, severity: "high" },
  { id: "do_not_reuse", re: /\bdo not reuse\b/i, severity: "high" },
  { id: "avoid_borrowing", re: /\bavoid borrowing\b/i, severity: "high" },
  { id: "stage", re: /\bStage\s*\d/i, severity: "critical" },
  { id: "factory", re: /\bfactory\b/i, severity: "critical" },
  { id: "governance", re: /\bgovernance\b/i, severity: "high" },
  { id: "source_colon", re: /\bSource:\s*/i, severity: "critical" },
  { id: "sources_colon", re: /\bSources:\s*/i, severity: "critical" },
  { id: "qa_gate", re: /\bQA gate\b|\bQA checklist\b|\bfactory QA\b|\bearly QA\b/i, severity: "high" },
]);

const MOMENTUM_BRAND_PAGE_TITLE_RE =
  /guest brand site|brand site supports|brand page frames|development materials confirm/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function rowText(r) {
  return [
    r.title,
    r.body,
    r.caseSummaryOverview,
    r.caseSummaryBrandRelevance,
    r.caseSummaryOwnerObjective,
    r.caseSummaryInterpretation,
    r.caseSummaryTags,
  ]
    .map(nz)
    .filter(Boolean)
    .join("\n");
}

function scanSemanticForbidden(text) {
  const hits = [];
  for (const rule of WAVE14_SEMANTIC_FORBIDDEN_RES) {
    if (rule.re.test(text)) hits.push(rule);
  }
  return hits;
}

/**
 * Extract blockers for one Wave 14 brand from live Presentation rows.
 */
export async function extractWave14PublicActiveSemanticBlockersForBrand(slug) {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!identity?.recordId) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["unknown_identity"],
      failures: [],
    };
  }

  const live = await listPresentationRowsLight(identity.recordId, identity.name);
  const ownerRows = (live.rows || []).filter(isOwnerFacingPresentationRow);
  const failures = [];

  const pvqlHits = scanOwnerFacingForbiddenLanguage(ownerRows);
  for (const h of pvqlHits) {
    // Allow trailing announcement URLs on momentum/openings (PVQL already skips these
    // in scanOwnerFacingForbiddenLanguage — this is belt-and-suspenders logging).
    failures.push({
      brand: identity.name,
      brandSlug: slug,
      section: h.slotKey || "unknown",
      field: "Title/Body/Case Summary",
      currentVisibleCopy: nz(h.snippet || "").slice(0, 220),
      failureType: `pvql_forbidden:${h.id || h.label}`,
      forbiddenPhrase: h.label || h.id || "",
      proposedFix: "Rewrite owner-facing copy without forbidden phrase",
      requiresSourceCheck: false,
      severity: h.id === "raw_url" || h.id === "source_line" || h.id === "source_colon" ? "critical" : "high",
      slotKey: h.slotKey,
      recordId: h.recordId,
      source: "pvql",
    });
  }

  for (const r of ownerRows) {
    const text = rowText(r);
    if (!text) continue;
    const semanticHits = scanSemanticForbidden(text);
    for (const hit of semanticHits) {
      // Skip trailing Source: attribution false-positive duplication if already logged as source_line
      const already = failures.some(
        (f) => f.recordId === r.recordId && (f.forbiddenPhrase === hit.id || f.failureType.includes(hit.id))
      );
      if (already) continue;
      failures.push({
        brand: identity.name,
        brandSlug: slug,
        section: r.slotKey || nz(r.title) || "unknown",
        field: "Title/Body/Case Summary",
        currentVisibleCopy: text.slice(0, 220),
        failureType: `semantic:${hit.id}`,
        forbiddenPhrase: hit.id,
        proposedFix: "Replace internal/source language with owner-facing copy",
        requiresSourceCheck: /steward|source_pack|source_supported/i.test(hit.id),
        severity: hit.severity,
        slotKey: r.slotKey,
        recordId: r.recordId,
        source: "semantic",
      });
    }

    if (r.slotKey === "footprint.momentum" && MOMENTUM_BRAND_PAGE_TITLE_RE.test(nz(r.title))) {
      failures.push({
        brand: identity.name,
        brandSlug: slug,
        section: "Recent Momentum",
        field: "Title",
        currentVisibleCopy: nz(r.title).slice(0, 220),
        failureType: "momentum_brand_or_dev_page_as_event",
        forbiddenPhrase: "brand/development page as momentum",
        proposedFix:
          "Replace with dated opening/announcement or clearly labeled property proof; do not present brand pages as momentum events",
        requiresSourceCheck: true,
        severity: "high",
        slotKey: r.slotKey,
        recordId: r.recordId,
        source: "momentum_semantics",
      });
    }

    if (r.slotKey === "footprint.portfolio_mix") {
      const body = nz(r.body);
      const hasPercent = /%/.test(body);
      const isProse =
        body.length > 200 &&
        !hasPercent &&
        /peer comparison|CALA-supported|evaluate on operating/i.test(body);
      if (isProse) {
        failures.push({
          brand: identity.name,
          brandSlug: slug,
          section: "Portfolio Mix",
          field: "Body",
          currentVisibleCopy: body.slice(0, 220),
          failureType: "portfolio_mix_prose",
          forbiddenPhrase: "prose market note",
          proposedFix: "Convert to structured percentage mix without Source: line",
          requiresSourceCheck: false,
          severity: "high",
          slotKey: r.slotKey,
          recordId: r.recordId,
          source: "portfolio_mix",
        });
      }
    }
  }

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    ownerRowCount: ownerRows.length,
    failureCount: failures.length,
    failures,
    pass: failures.length === 0,
  };
}

export async function extractWave14PublicActiveSemanticBlockers({
  brands = WAVE14_PARTIAL_PROMOTION_SLUGS,
} = {}) {
  const targetSlugs = [...brands].filter((s) => s !== WAVE14_HELD_PROMOTION_SLUG);
  const brandResults = [];

  for (let i = 0; i < targetSlugs.length; i++) {
    const slug = targetSlugs[i];
    process.stdout.write(`[wave14-blockers] ${i + 1}/${targetSlugs.length} ${slug}...\n`);
    brandResults.push(await extractWave14PublicActiveSemanticBlockersForBrand(slug));
    if (i < targetSlugs.length - 1) await sleep(400);
  }

  const allFailures = brandResults.flatMap((b) => b.failures || []);
  const report = {
    version: WAVE14_PUBLIC_ACTIVE_SEMANTIC_BLOCKERS_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    scope: {
      brands: targetSlugs,
      heldUntouched: WAVE14_HELD_PROMOTION_SLUG,
    },
    summary: {
      brandsAudited: brandResults.length,
      brandsWithFailures: brandResults.filter((b) => !b.pass).length,
      brandsClean: brandResults.filter((b) => b.pass).length,
      failureCount: allFailures.length,
    },
    brandResults,
    failures: allFailures,
  };

  return report;
}

export function writeWave14PublicActiveSemanticBlockersReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave14-public-active-semantic-blockers.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-public-active-semantic-blockers.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Wave 14 — Public Active Semantic Blockers",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **read-only**`,
    "",
    `Brands audited: **${report.summary.brandsAudited}**`,
    `With failures: **${report.summary.brandsWithFailures}**`,
    `Clean: **${report.summary.brandsClean}**`,
    `Total failures: **${report.summary.failureCount}**`,
    `Held / untouched: \`${report.scope.heldUntouched}\``,
    "",
    "| Brand | Section | Field | Failure Type | Forbidden Phrase | Requires Source Check? | Current Visible Copy |",
    "|-------|---------|-------|--------------|------------------|------------------------|----------------------|",
  ];

  for (const f of report.failures || []) {
    lines.push(
      `| ${f.brandSlug} | ${String(f.section || "").replace(/\|/g, "/")} | ${f.field} | ${f.failureType} | ${f.forbiddenPhrase} | ${f.requiresSourceCheck} | ${String(f.currentVisibleCopy || "").replace(/\|/g, "/").replace(/\n/g, " ").slice(0, 120)} |`
    );
  }
  lines.push("");

  fs.writeFileSync(mdPath, lines.join("\n"), "utf8");
  return { jsonPath, mdPath };
}
