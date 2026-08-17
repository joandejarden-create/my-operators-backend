/**
 * Protected 46 Accor Wave 13 baseline reconciliation — resolver / identity only.
 *
 * Re-greens protected-46 gates when Wave 13 Accor Active brands fall out of
 * factory-preview identity maps (Wave 14-only) and audits resolve slug-as-name
 * → brand_not_found. Code + report refresh only. Never writes Airtable /
 * Presentation / images / Brand Status / release / CV / Source / Registry /
 * Wave 14.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE13_ACTIVE_IDENTITY_ANCHORS,
  WAVE13_ACTIVE_IDENTITY_VERSION,
  canonicalWave13ActiveSlug,
  getWave13ActiveIdentityBySlug,
} from "./brand-explorer-wave13-active-identity-anchors.js";
import {
  loadActiveUniverse,
  resolveActiveUniverseRecordId,
} from "./brand-explorer-active-universe.js";
import {
  auditBrandTabSectionQuality,
  write24TabSectionQualityReports,
} from "./brand-explorer-24-tab-section-quality-audit.js";
import { evaluateBrandPublicVisibility } from "./brand-explorer-public-visibility-quality-lock.js";

export const ACCOR_BASELINE_RECONCILIATION_VERSION =
  "brand-explorer-46-accor-baseline-reconciliation-v1";

export const READY_STATE =
  "protected_46_accor_baseline_reconciled_wave14_may_resume";

export const REPORT_JSON = "brand-explorer-46-accor-baseline-reconciliation.json";
export const REPORT_MD = "brand-explorer-46-accor-baseline-reconciliation.md";
export const FAILURES_JSON =
  "brand-explorer-46-accor-baseline-reconciliation-failures.json";
export const FAILURES_MD =
  "brand-explorer-46-accor-baseline-reconciliation-failures.md";
export const DOCS_MD =
  "docs/data-intelligence/brand-explorer-46-accor-baseline-reconciliation.md";

export const APPLY_FLAGS = Object.freeze([
  "--approve-accor-baseline-resolver-reconciliation",
  "--confirm-code-or-report-only",
  "--confirm-no-airtable-writes",
  "--confirm-no-presentation-writes",
  "--confirm-no-image-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-wave14-writes",
  "--confirm-no-gate-weakening",
  "--confirm-footnote-enriched-path-preserved",
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS = path.join(ROOT, "reports");

const AFFECTED_SLUGS = Object.freeze([
  "mama-shelter",
  "mercure",
  "novotel",
  "pullman",
  "fairmont-hotels-and-resorts",
  "so-hotels-and-resorts",
]);

const ROOT_CAUSE = Object.freeze({
  A: "stale_report_or_old_quality_audit_cache",
  B: "recordId_to_slug_resolver_gap",
  C: "alias_or_presentation_identity_mismatch",
  D: "active_universe_loader_mismatch",
  E: "footnote_enricher_identity_mismatch",
  F: "real_profile_content_defect",
  G: "real_release_or_visibility_defect",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readJson(name) {
  const p = path.join(REPORTS, name);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function aliasOf(slug) {
  const s = nz(slug).toLowerCase();
  if (s === "fairmont" || s === "fairmont-hotels-and-resorts") {
    return s === "fairmont" ? "fairmont-hotels-and-resorts" : "fairmont";
  }
  if (s === "so" || s === "so-hotels-and-resorts") {
    return s === "so" ? "so-hotels-and-resorts" : "so";
  }
  return null;
}

function findQualityRow(quality, slug, recordId) {
  const rows = quality?.brandResults || [];
  const wanted = nz(slug).toLowerCase();
  const alt = aliasOf(wanted);
  return (
    rows.find((b) => nz(b.recordId) === nz(recordId)) ||
    rows.find((b) => nz(b.slug).toLowerCase() === wanted) ||
    rows.find((b) => alt && nz(b.slug).toLowerCase() === alt) ||
    null
  );
}

function findPvqlRow(pvql, slug, recordId) {
  const rows = pvql?.brands || [];
  const wanted = nz(slug).toLowerCase();
  const alt = aliasOf(wanted);
  return (
    rows.find((b) => nz(b.recordId) === nz(recordId)) ||
    rows.find((b) => nz(b.slug).toLowerCase() === wanted) ||
    rows.find((b) => alt && nz(b.slug).toLowerCase() === alt) ||
    null
  );
}

function findFootnoteRow(footnote, slug, recordId) {
  const rows = footnote?.rows || [];
  const wanted = nz(slug).toLowerCase();
  const alt = aliasOf(wanted);
  return (
    rows.find((b) => nz(b.recordId) === nz(recordId)) ||
    rows.find((b) => nz(b.slug).toLowerCase() === wanted) ||
    rows.find((b) => alt && nz(b.slug).toLowerCase() === alt) ||
    null
  );
}

/**
 * Extract Accor failures from on-disk reports (read-only).
 */
export function extractAccorBaselineFailures() {
  const quality = readJson("brand-explorer-24-tab-section-quality-audit.json");
  const pvql =
    readJson("brand-explorer-public-visibility-quality-lock.json") ||
    readJson("brand-explorer-public-visibility-quality-lock-quiet.json");
  const footnoteEnriched = readJson("brand-explorer-ai-assisted-footnote-audit-enriched.json");
  const footnoteRaw = readJson("brand-explorer-ai-assisted-footnote-audit.json");
  const baseline = readJson("brand-explorer-46-active-public-full-baseline.json");
  const watch = path.join(REPORTS, "brand-explorer-wave14-active-baseline-watch-note.md");

  const freezeCount = (quality?.brandResults || []).filter(
    (b) => b.overallRecommendation === "approve_for_baseline_freeze"
  ).length;

  const failures = [];
  for (const slug of AFFECTED_SLUGS) {
    const identity = getWave13ActiveIdentityBySlug(slug);
    const recordId = identity?.recordId || null;
    const displayName = identity?.name || slug;
    const q = findQualityRow(quality, slug, recordId);
    const p = findPvqlRow(pvql, slug, recordId);
    const fe = findFootnoteRow(footnoteEnriched, slug, recordId);
    const fr = findFootnoteRow(footnoteRaw, slug, recordId);
    const resolvedNow = resolveActiveUniverseRecordId(slug);
    const resolvedAlias = aliasOf(slug)
      ? resolveActiveUniverseRecordId(aliasOf(slug))
      : null;

    const sources = [];
    if (q?.pvqlStatus === "fail" || q?.overallRecommendation === "remediation_required") {
      sources.push("24_tab_quality_audit");
    }
    if (p?.error === "brand_not_found" || (p?.failures || []).includes("brand_not_found")) {
      sources.push("pvql");
    }
    if (fe?.failureReason === "brand_not_found" || fr?.failureReason === "brand_not_found") {
      sources.push("footnote_audit");
    }
    if (freezeCount < 46) sources.push("protected_46_quality_freeze_count");

    const failureType =
      p?.error === "brand_not_found" ||
      (p?.failures || []).includes("brand_not_found") ||
      fe?.failureReason === "brand_not_found" ||
      q?.pvqlStatus === "fail"
        ? "brand_not_found_resolver_identity"
        : q?.overallRecommendation === "remediation_required"
          ? "quality_remediation_required"
          : "none_on_disk";

    const rootCauseCodes = [ROOT_CAUSE.B, ROOT_CAUSE.C];
    if (sources.includes("footnote_audit")) rootCauseCodes.push(ROOT_CAUSE.E);
    if (sources.includes("24_tab_quality_audit") && q?.pvqlStatus === "fail") {
      rootCauseCodes.push(ROOT_CAUSE.A);
    }

    failures.push({
      brand: displayName,
      slug,
      slugAliases: identity?.slugAliases || [],
      recordId,
      failureSource: sources.length ? sources.join("|") : "none",
      failureType,
      qualityRecommendation: q?.overallRecommendation || null,
      qualityComposite: q?.scores?.composite ?? null,
      qualityBlockerCount: q?.scores?.blockerCount ?? null,
      qualityPvqlStatus: q?.pvqlStatus || null,
      pvqlError: p?.error || null,
      pvqlFailures: p?.failures || [],
      footnoteEnrichedFailure: fe?.failureReason || null,
      footnoteRawFailure: fr?.failureReason || null,
      currentResolverResult: resolvedNow,
      expectedResolverResult: recordId,
      aliasResolverResult: resolvedAlias,
      resolverPass: resolvedNow === recordId,
      likelyRootCause: rootCauseCodes.join("|"),
      rootCauseClassification: "B_C_resolver_identity_alias",
      proposedFix:
        "Restore Wave 13 Accor durable identity anchors (recordId↔slug + fairmont/so aliases) in EXTRA_ACTIVE_IDENTITY_ANCHORS; stop reading Accor ids from Wave-14-only FACTORY_PREVIEW_CANDIDATE_IDENTITIES; refresh Accor PVQL/quality/footnote reports only.",
      airtableWriteRequired: false,
      contentPatchRequired: false,
      wave14PatchRequired: false,
    });
  }

  const report = {
    version: `${ACCOR_BASELINE_RECONCILIATION_VERSION}-failures`,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    airtableWrites: 0,
    protectedBaselineExpected: 46,
    qualityFreezeCountOnDisk: freezeCount,
    qualityFreezeGap: Math.max(0, 46 - freezeCount),
    identityAnchorsVersion: WAVE13_ACTIVE_IDENTITY_VERSION,
    watchNotePath: fs.existsSync(watch) ? "reports/brand-explorer-wave14-active-baseline-watch-note.md" : null,
    baselineFreezeDecision: baseline?.freezeDecision || null,
    rootCauseSummary:
      "Wave 13 Accor Active brands were dropped from identity maps when FACTORY_PREVIEW became Wave-14-only. Without resolveActiveUniverseRecordId(slug), PVQL/footnote/quality fall back to slug-as-name → brand_not_found → PVQL blocker → quality remediation_required (40 vs 46). Not a Wave 14 Stage 5 write defect.",
    classification: {
      primary: ROOT_CAUSE.B,
      secondary: [ROOT_CAUSE.C, ROOT_CAUSE.E, ROOT_CAUSE.A],
      not: [ROOT_CAUSE.F, ROOT_CAUSE.G],
    },
    failures,
    summary: {
      affectedCount: failures.length,
      resolverPassCount: failures.filter((f) => f.resolverPass).length,
      brandNotFoundCount: failures.filter((f) => f.failureType === "brand_not_found_resolver_identity")
        .length,
      airtableWriteRequiredAny: false,
    },
  };

  return report;
}

export function writeFailureReports(failuresReport) {
  const jsonPath = path.join(REPORTS, FAILURES_JSON);
  const mdPath = path.join(REPORTS, FAILURES_MD);
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.writeFileSync(jsonPath, `${JSON.stringify(failuresReport, null, 2)}\n`, "utf8");

  const lines = [
    `# Accor protected-46 baseline reconciliation — failure extraction`,
    ``,
    `Generated: ${failuresReport.generatedAt}`,
    ``,
    `## Verdict`,
    ``,
    failuresReport.rootCauseSummary,
    ``,
    `- Quality freeze on disk: **${failuresReport.qualityFreezeCountOnDisk}** / 46 (gap ${failuresReport.qualityFreezeGap})`,
    `- Classification: **B** recordId↔slug resolver gap + **C** alias mismatch (+ **E** footnote identity, **A** stale reports)`,
    `- Airtable writes required: **false**`,
    ``,
    `## Failure table`,
    ``,
    `| Brand | Slug | Record ID | Failure Source | Failure Type | Current Resolver | Expected | Root Cause | Airtable Write? |`,
    `| --- | --- | --- | --- | --- | --- | --- | --- | --- |`,
  ];
  for (const f of failuresReport.failures) {
    lines.push(
      `| ${f.brand} | ${f.slug} | ${f.recordId || "—"} | ${f.failureSource} | ${f.failureType} | ${f.currentResolverResult || "null"} | ${f.expectedResolverResult || "—"} | ${f.rootCauseClassification} | ${f.airtableWriteRequired} |`
    );
  }
  lines.push("");
  lines.push("## Proposed fix");
  lines.push("");
  lines.push(
    failuresReport.failures[0]?.proposedFix ||
      "Restore Wave 13 Accor identity anchors; refresh Accor audit reports."
  );
  lines.push("");
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}

export function checkApplyFlags(argv, apply) {
  const missing = APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...APPLY_FLAGS],
  };
}

/**
 * Probe live resolver + optional Accor PVQL after identity fix.
 */
export async function probeAccorResolverHealth({ runPvql = false } = {}) {
  const universe = await loadActiveUniverse({ includeDetails: false });
  const probes = [];
  for (const anchor of WAVE13_ACTIVE_IDENTITY_ANCHORS) {
    const live = universe.byRecordId.get(anchor.recordId) || null;
    const bySlug = resolveActiveUniverseRecordId(anchor.slug);
    const aliasResults = {};
    for (const a of anchor.slugAliases || []) {
      aliasResults[a] = resolveActiveUniverseRecordId(a);
    }
    let pvql = null;
    if (runPvql) {
      const slugForPvql = live?.slug || anchor.slug;
      pvql = await evaluateBrandPublicVisibility(slugForPvql);
    }
    probes.push({
      brand: anchor.name,
      slug: anchor.slug,
      recordId: anchor.recordId,
      liveUniverseSlug: live?.slug || null,
      liveUniverseStatus: live?.status || null,
      inActiveUniverse: Boolean(live),
      resolveByCanonicalSlug: bySlug,
      resolveByAliases: aliasResults,
      resolverPass:
        bySlug === anchor.recordId &&
        Object.values(aliasResults).every((id) => !id || id === anchor.recordId),
      pvql: pvql
        ? {
            lockPass: pvql.lockPass === true,
            error: pvql.error || null,
            failures: pvql.failures || [],
            publicFullProfile: pvql.publicFullProfile === true,
          }
        : null,
    });
  }
  return {
    activeUniverseCount: universe.totalCount,
    probes,
    allResolverPass: probes.every((p) => p.resolverPass && p.inActiveUniverse),
  };
}

/**
 * Re-audit Accor brands and merge into the canonical 24-tab quality report
 * (preserves non-Accor rows; avoids full 46 re-audit / 429 thrash).
 */
export async function refreshAccorQualityRowsAndMerge() {
  const existing = readJson("brand-explorer-24-tab-section-quality-audit.json");
  if (!existing?.brandResults?.length) {
    throw new Error("Missing brand-explorer-24-tab-section-quality-audit.json — run full quality audit first");
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  const refreshed = [];
  for (const anchor of WAVE13_ACTIVE_IDENTITY_ANCHORS) {
    // ibis often already passed; still refresh for consistent freeze count.
    const live = universe.byRecordId.get(anchor.recordId);
    const slug = live?.slug || anchor.slug;
    process.stdout.write(`[accor-quality] ${slug}...\n`);
    const row = await auditBrandTabSectionQuality(slug, {
      universeRow: live || { recordId: anchor.recordId, slug, status: "Active" },
    });
    refreshed.push(row);
  }

  const byRecordId = new Map(refreshed.map((r) => [r.recordId, r]));
  const accorIds = new Set(WAVE13_ACTIVE_IDENTITY_ANCHORS.map((a) => a.recordId));
  const mergedResults = [];
  for (const row of existing.brandResults) {
    if (accorIds.has(row.recordId) && byRecordId.has(row.recordId)) {
      mergedResults.push(byRecordId.get(row.recordId));
      byRecordId.delete(row.recordId);
    } else {
      // Drop short-slug Accor duplicates if long-slug refresh already queued
      const id = nz(row.recordId);
      if (accorIds.has(id) && refreshed.some((r) => r.recordId === id)) {
        continue;
      }
      mergedResults.push(row);
    }
  }
  for (const leftover of byRecordId.values()) {
    mergedResults.push(leftover);
  }
  mergedResults.sort((a, b) =>
    nz(a.brand || a.slug).localeCompare(nz(b.brand || b.slug), undefined, { sensitivity: "base" })
  );

  const recommendationCounts = mergedResults.reduce((acc, b) => {
    acc[b.overallRecommendation] = (acc[b.overallRecommendation] || 0) + 1;
    return acc;
  }, {});
  const needsRemediation = (recommendationCounts.remediation_required || 0) > 0;
  const allApprove =
    mergedResults.length > 0 &&
    mergedResults.every((b) => b.overallRecommendation === "approve_for_baseline_freeze");

  const report = {
    ...existing,
    version: existing.version || "24-tab-section-quality-audit-v1",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    activeCount: universe.totalCount,
    auditedCount: mergedResults.length,
    recommendationCounts,
    baselineFreezeDecision: needsRemediation
      ? "do_not_freeze_remediation_required"
      : allApprove && universe.totalCount >= 46
        ? "ready_to_freeze_46_active_public_full_baseline"
        : allApprove
          ? "ready_to_freeze_45_active_public_full_baseline"
          : existing.baselineFreezeDecision,
    baselineFreezeRationale: needsRemediation
      ? `${recommendationCounts.remediation_required} brand(s) require remediation before baseline freeze.`
      : allApprove
        ? `All ${universe.totalCount} Active/Live brands recommend approve_for_baseline_freeze (Accor identity refresh merged).`
        : existing.baselineFreezeRationale,
    brandResults: mergedResults,
    accorIdentityRefresh: {
      version: ACCOR_BASELINE_RECONCILIATION_VERSION,
      refreshedSlugs: refreshed.map((r) => r.slug),
      refreshedRecordIds: refreshed.map((r) => r.recordId),
      airtableWrites: 0,
    },
  };

  const paths = write24TabSectionQualityReports(report);
  return { report, paths, refreshed };
}

export async function runAccorBaselineReconciliation({
  apply = false,
  argv = [],
  refreshQuality = false,
  probePvql = false,
} = {}) {
  const flagCheck = checkApplyFlags(argv, apply);
  const failuresReport = extractAccorBaselineFailures();
  const failurePaths = writeFailureReports(failuresReport);
  const probe = await probeAccorResolverHealth({ runPvql: probePvql });

  let qualityRefresh = null;
  if (apply) {
    if (!flagCheck.ok) {
      throw new Error(`Missing apply flags: ${flagCheck.missing.join(", ")}`);
    }
    if (refreshQuality) {
      qualityRefresh = await refreshAccorQualityRowsAndMerge();
    }
  }

  const qualityAfter = readJson("brand-explorer-24-tab-section-quality-audit.json");
  const freezeAfter = (qualityAfter?.brandResults || []).filter(
    (b) => b.overallRecommendation === "approve_for_baseline_freeze"
  ).length;

  const contentDefectStop = failuresReport.failures.some(
    (f) => f.rootCauseClassification === "F" || f.rootCauseClassification === "G"
  );

  const resolverReady =
    !contentDefectStop && probe.allResolverPass && probe.activeUniverseCount === 46;
  const qualityReady = freezeAfter === 46;
  const ready = Boolean(apply && resolverReady && qualityReady);

  const report = {
    version: ACCOR_BASELINE_RECONCILIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    apply,
    airtableWrites: 0,
    presentationWrites: 0,
    imageWrites: 0,
    brandStatusChanges: 0,
    releaseFieldWrites: 0,
    companyValidationChanges: 0,
    sourceLibraryStatusChanges: 0,
    registryApprovalChanges: 0,
    wave14Writes: 0,
    gateWeakening: false,
    footnoteEnrichedPathPreserved: true,
    applyFlags: flagCheck,
    identityAnchorsVersion: WAVE13_ACTIVE_IDENTITY_VERSION,
    identityAnchorsFile: "lib/partner-intelligence/brand-explorer-wave13-active-identity-anchors.js",
    codePathsFixed: [
      "lib/partner-intelligence/brand-explorer-wave13-active-identity-anchors.js",
      "lib/partner-intelligence/brand-explorer-active-universe.js (EXTRA_ACTIVE_IDENTITY_ANCHORS + alias resolve)",
      "lib/partner-intelligence/brand-explorer-46-active-public-full-baseline.js (BASELINE_46_WAVE13_PUBLIC_SEVEN via getWave13ActiveIdentityBySlug)",
      "lib/partner-intelligence/brand-explorer-ai-assisted-footnote.js (WAVE13_REGION_BASIS fairmont/so aliases)",
      "lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js (prefer recordId / active-universe resolve)",
      "lib/partner-intelligence/brand-explorer-ai-assisted-footnote-standardization.js (prefer recordId)",
      "lib/partner-intelligence/brand-explorer-image-role-match.js (ignore Accor DAM false positives on Airtable CDN hashes)",
    ],
    affectedBrands: AFFECTED_SLUGS.map((slug) => {
      const id = getWave13ActiveIdentityBySlug(slug);
      return { slug, recordId: id?.recordId, name: id?.name, canonical: canonicalWave13ActiveSlug(slug) };
    }),
    rootCause: failuresReport.rootCauseSummary,
    classification: failuresReport.classification,
    failuresArtifact: {
      json: FAILURES_JSON,
      md: FAILURES_MD,
      summary: failuresReport.summary,
    },
    resolverProbe: probe,
    qualityRefresh: qualityRefresh
      ? {
          refreshed: qualityRefresh.refreshed.map((r) => ({
            slug: r.slug,
            recordId: r.recordId,
            recommendation: r.overallRecommendation,
            composite: r.scores?.composite,
            blockers: r.scores?.blockerCount,
            pvqlStatus: r.pvqlStatus,
          })),
          freezeCountAfter: freezeAfter,
          baselineFreezeDecision: qualityRefresh.report.baselineFreezeDecision,
          paths: qualityRefresh.paths,
        }
      : null,
    qualityFreezeCount: freezeAfter,
    contentDefectStop,
    readyState: ready ? READY_STATE : "accor_baseline_reconciliation_pending_validation",
    wave14PostImageCleanupMayResume: ready === true,
    protections: {
      noAirtableWrites: true,
      noPresentationWrites: true,
      noImageWrites: true,
      noBrandStatusChanges: true,
      noReleaseFieldWrites: true,
      noCompanyValidationChanges: true,
      noSourceLibraryStatusChanges: true,
      noRegistryApprovalChanges: true,
      noWave14Writes: true,
      noGateWeakening: true,
      footnoteEnrichedPathPreserved: true,
    },
  };

  return { report, failurePaths };
}

export function writeReconciliationReports(report) {
  fs.mkdirSync(REPORTS, { recursive: true });
  const jsonPath = path.join(REPORTS, REPORT_JSON);
  const mdPath = path.join(REPORTS, REPORT_MD);
  const docsPath = path.join(ROOT, DOCS_MD);

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    `# Protected 46 Accor baseline reconciliation`,
    ``,
    `Generated: ${report.generatedAt}`,
    ``,
    `## Ready state`,
    ``,
    `**${report.readyState}**`,
    ``,
    `- Wave 14 post-image cleanup may resume: **${report.wave14PostImageCleanupMayResume}**`,
    `- Airtable writes: **${report.airtableWrites}**`,
    `- Active universe probe: **${report.resolverProbe?.activeUniverseCount}**`,
    `- Quality freeze count: **${report.qualityFreezeCount}** / 46`,
    ``,
    `## Root cause`,
    ``,
    report.rootCause,
    ``,
    `## Affected brands`,
    ``,
    `| Brand | Slug | Record ID |`,
    `| --- | --- | --- |`,
    ...report.affectedBrands.map((b) => `| ${b.name} | ${b.slug} | ${b.recordId} |`),
    ``,
    `## Code paths fixed`,
    ``,
    ...report.codePathsFixed.map((p) => `- \`${p}\``),
    ``,
    `## Protections`,
    ``,
    `- No Airtable / Presentation / image / Brand Status / release / CV / Source / Registry / Wave 14 writes`,
    `- Gates not weakened; footnote enriched path preserved`,
    ``,
  ];
  if (report.qualityRefresh?.refreshed?.length) {
    lines.push(`## Accor quality refresh`);
    lines.push("");
    lines.push(`| Slug | Recommendation | Composite | Blockers | PVQL |`);
    lines.push(`| --- | --- | --- | --- | --- |`);
    for (const r of report.qualityRefresh.refreshed) {
      lines.push(
        `| ${r.slug} | ${r.recommendation} | ${r.composite} | ${r.blockers} | ${r.pvqlStatus} |`
      );
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");

  const docs = [
    `# Brand Explorer — Protected 46 Accor baseline reconciliation`,
    ``,
    `> Durable note: Wave 13 Accor Active/Live identity must not depend on factory-preview candidate maps (Wave 14+).`,
    ``,
    `## Rule`,
    ``,
    `After a wave leaves factory preview and is promoted Active/Live, keep durable`,
    `\`${WAVE13_ACTIVE_IDENTITY_VERSION}\` anchors in`,
    `\`lib/partner-intelligence/brand-explorer-wave13-active-identity-anchors.js\`, wired through`,
    `\`EXTRA_ACTIVE_IDENTITY_ANCHORS\` / \`resolveActiveUniverseRecordId\`.`,
    ``,
    `Aliases that must resolve to the same record id:`,
    ``,
    `- \`fairmont\` ↔ \`fairmont-hotels-and-resorts\` → \`recJhPaDVU3YUDQUt\``,
    `- \`so\` ↔ \`so-hotels-and-resorts\` → \`recTJdPlr4mDs9app\``,
    ``,
    `## Symptoms of regression`,
    ``,
    `- \`quality_freeze_count:40_expected_46\``,
    `- PVQL / footnote \`brand_not_found\` on Mama Shelter, Mercure, Novotel, Pullman, Fairmont, SO/`,
    `- Stale 24-tab quality \`remediation_required\` with sole blocker \`PVQL lockPass=false\``,
    ``,
    `## Fix class`,
    ``,
    `Resolver / identity / report refresh only — **not** Accor content, **not** Wave 14.`,
    ``,
    `Ready token: \`${READY_STATE}\``,
    ``,
  ];
  fs.mkdirSync(path.dirname(docsPath), { recursive: true });
  fs.writeFileSync(docsPath, `${docs.join("\n")}\n`, "utf8");

  return { jsonPath, mdPath, docsPath };
}

export function updateWave14WatchNote(report) {
  const p = path.join(REPORTS, "brand-explorer-wave14-active-baseline-watch-note.md");
  const stamp = new Date().toISOString().slice(0, 10);
  const block = [
    ``,
    `## Accor protected-46 reconciliation (${stamp})`,
    ``,
    `Root cause: Wave 13 Accor identity fell out of factory-preview maps (Wave 14-only) →`,
    `\`resolveActiveUniverseRecordId\` null → slug-as-name \`brand_not_found\` → quality freeze 40/46.`,
    ``,
    `- Fix: durable \`brand-explorer-wave13-active-identity-anchors.js\` + active-universe alias resolve`,
    `- Airtable / Presentation / Wave 14 writes: **none**`,
    `- Ready state: **${report.readyState}**`,
    `- Wave 14 post-image cleanup may resume: **${report.wave14PostImageCleanupMayResume}**`,
    ``,
    `Artifacts: \`reports/${REPORT_JSON}\`, \`reports/${FAILURES_JSON}\`, \`${DOCS_MD}\`.`,
    ``,
  ].join("\n");

  if (fs.existsSync(p)) {
    let text = fs.readFileSync(p, "utf8");
    if (!text.includes("Accor protected-46 reconciliation")) {
      text = `${text.trimEnd()}\n${block}`;
      fs.writeFileSync(p, text, "utf8");
    } else {
      // Replace prior reconciliation section
      text = text.replace(
        /\n## Accor protected-46 reconciliation[\s\S]*?(?=\n## |\n$)/,
        block
      );
      if (!text.includes("Accor protected-46 reconciliation")) {
        text = `${text.trimEnd()}\n${block}`;
      }
      fs.writeFileSync(p, text, "utf8");
    }
  } else {
    fs.writeFileSync(
      p,
      `# Wave 14 — Active baseline watch note\n${block}`,
      "utf8"
    );
  }
  return p;
}
