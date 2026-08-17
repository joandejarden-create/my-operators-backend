/**
 * Operator Explorer OS — gate evaluation + canonical state for baselines / factory candidates.
 * Read-oriented. Does not write Airtable.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OPERATOR_QUALITY_BASELINE_OPERATORS,
  getOperatorQualityBaselineEntry,
  isProtectedOperatorQualityBaseline,
} from "./operator-explorer-quality-baseline.js";
import {
  getOperatorFactoryQueueEntry,
  listOperatorFactoryQueue,
} from "./operator-explorer-factory-queue.js";
import { runOperatorTabFactoryAudit } from "./operator-explorer-tab-factory-audit.js";
import { runOperatorSectionPatternParityAudit } from "./operator-explorer-section-pattern-parity-audit.js";
import { runOperatorSourceProvenanceAudit } from "./operator-explorer-source-provenance-by-tab-audit.js";
import { runOperatorBaselineGapRemediation } from "./operator-explorer-baseline-gap-remediation.js";
import {
  evaluateOperatorSourceProvenanceByTab,
} from "./operator-explorer-source-provenance-by-tab.js";

export const OPERATOR_EXPLORER_OS_VERSION = "operator-explorer-os-v1";

export const OPERATOR_EXPLORER_OS_STATES = Object.freeze([
  "not_started",
  "factory_scaffolded",
  "sources_seeded",
  "draft_in_progress",
  "field_audit_failing",
  "pattern_failing",
  "provenance_failing",
  "founder_review_ready",
  "active_profile_ready",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

/**
 * Resolve canonical OS state from gate bundle.
 */
export function resolveOperatorExplorerOsState(gates = {}) {
  const {
    hasMasterRecord = false,
    hasFixturePack = false,
    provenancePass = false,
    fieldAuditPass = false,
    sectionPatternPass = false,
    tabFactoryPass = false,
    founderReviewPassed = false,
    activeReleaseApproved = false,
  } = gates;

  if (!hasMasterRecord) {
    return { state: "not_started", nextAction: "create_or_confirm_operator_master" };
  }
  if (!hasFixturePack) {
    return {
      state: provenancePass ? "sources_seeded" : "factory_scaffolded",
      nextAction: provenancePass
        ? "run_factory_init_then_tab_by_tab_content"
        : "run_operator_explorer_factory_init",
    };
  }
  if (!provenancePass) {
    return {
      state: "provenance_failing",
      nextAction: "add_operator_official_sources_and_re_audit_provenance",
    };
  }
  if (!fieldAuditPass) {
    return {
      state: "field_audit_failing",
      nextAction: "thicken_fields_or_run_gap_remediation_dry_run",
    };
  }
  if (!sectionPatternPass) {
    return {
      state: "pattern_failing",
      nextAction: "align_section_patterns_to_arbor_he_bar",
    };
  }
  if (!tabFactoryPass) {
    return { state: "draft_in_progress", nextAction: "clear_remaining_tab_factory_gates" };
  }
  if (tabFactoryPass && !founderReviewPassed) {
    return {
      state: "founder_review_ready",
      nextAction: "founder_visual_review_then_active_release",
    };
  }
  if (founderReviewPassed && activeReleaseApproved) {
    return { state: "active_profile_ready", nextAction: "monitor_baseline_regression" };
  }
  if (founderReviewPassed) {
    return { state: "founder_review_ready", nextAction: "explicit_active_release_approval" };
  }
  return { state: "draft_in_progress", nextAction: "continue_tab_factory_sequence" };
}

function resolveOperatorIdentity(id) {
  const baseline = getOperatorQualityBaselineEntry(id);
  if (baseline) {
    return { ...baseline, kind: "quality_baseline", protectedBaseline: true };
  }
  const queued = getOperatorFactoryQueueEntry(id);
  if (queued) {
    return { ...queued, kind: "factory_queue", protectedBaseline: false };
  }
  if (/^rec[a-zA-Z0-9]+$/.test(String(id || ""))) {
    return {
      slug: String(id),
      recordId: String(id),
      companyName: String(id),
      domain: "",
      kind: "ad_hoc",
      protectedBaseline: false,
      explorerUrl: `/operator-explorer-gold-mock.html?id=${id}`,
    };
  }
  return null;
}

function hasLocalFixtures(slug) {
  const dir = path.join(ROOT, "fixtures");
  if (!fs.existsSync(dir)) return false;
  // baseline suffixes + factory slug suffix
  const needles = [slug, slug.replace(/-/g, "")];
  if (slug === "arbor-lodging-cala") needles.push("arbor-cala");
  if (slug === "hotel-equities-cala") needles.push("he-cala");
  if (slug === "ghl-hoteles") needles.push("ghl");
  return fs.readdirSync(dir).some((name) => {
    if (!name.startsWith("operator-") || !name.endsWith(".json")) return false;
    return needles.some((n) => name.includes(n));
  });
}

/**
 * @param {{
 *   operators?: string[],
 *   source?: 'fixtures'|'live'|'merged',
 *   includeRemediationPreview?: boolean,
 *   stage?: string
 * }} [opts]
 */
export async function runOperatorExplorerOs(opts = {}) {
  const source = opts.source || "merged";
  const stage = opts.stage || "release-readiness";
  const includeRemediationPreview = opts.includeRemediationPreview !== false;

  const defaultOps = [
    ...OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug),
    ...listOperatorFactoryQueue()
      .filter((o) => o.status === "in_progress" || o.status === "queued")
      .slice(0, 2)
      .map((o) => o.slug),
  ];
  const operators = opts.operators?.length ? opts.operators : defaultOps;

  const table = [];
  for (const id of operators) {
    const identity = resolveOperatorIdentity(id);
    if (!identity) {
      table.push({
        operatorSlug: id,
        error: "unknown_operator",
        canonicalState: "not_started",
        allowedNextAction: "register_in_factory_queue_or_quality_baseline",
      });
      continue;
    }

    const isBaseline = identity.kind === "quality_baseline";
    const fixturePack = hasLocalFixtures(identity.slug);
    let tabFactory = null;
    let sectionPattern = null;
    let provenance = null;
    let remediation = null;
    const errors = [];

    if (isBaseline) {
      try {
        const tf = await runOperatorTabFactoryAudit({
          operators: [identity.slug],
          source,
        });
        tabFactory = tf.operatorResults[0] || null;
      } catch (err) {
        errors.push(`tab_factory:${err?.message || err}`);
      }

      try {
        const sp = await runOperatorSectionPatternParityAudit({
          operators: [identity.slug],
          source,
        });
        sectionPattern = sp.operatorResults[0] || null;
      } catch (err) {
        errors.push(`section_pattern:${err?.message || err}`);
      }

      try {
        const pv = await runOperatorSourceProvenanceAudit({
          operators: [identity.slug],
          source,
        });
        provenance = pv.operatorResults[0] || null;
      } catch (err) {
        errors.push(`provenance:${err?.message || err}`);
      }

      if (includeRemediationPreview) {
        try {
          remediation = await runOperatorBaselineGapRemediation({
            operators: [identity.slug],
            source,
            apply: false,
          });
        } catch (err) {
          errors.push(`remediation:${err?.message || err}`);
        }
      }
    } else {
      // Factory queue: run tab-factory/pattern when fixtures exist; provenance from domain + fixtures
      if (fixturePack && identity.recordId) {
        try {
          const tf = await runOperatorTabFactoryAudit({
            operators: [identity.slug],
            source: source === "live" ? "fixtures" : source === "merged" ? "fixtures" : source,
          });
          tabFactory = tf.operatorResults[0] || null;
        } catch (err) {
          errors.push(`tab_factory:${err?.message || err}`);
        }
        try {
          const sp = await runOperatorSectionPatternParityAudit({
            operators: [identity.slug],
            source: "fixtures",
          });
          sectionPattern = sp.operatorResults[0] || null;
        } catch (err) {
          errors.push(`section_pattern:${err?.message || err}`);
        }
      } else if (fixturePack) {
        try {
          const tf = await runOperatorTabFactoryAudit({
            operators: [identity.slug],
            source: "fixtures",
          });
          tabFactory = tf.operatorResults[0] || null;
        } catch (err) {
          errors.push(`tab_factory:${err?.message || err}`);
        }
        try {
          const sp = await runOperatorSectionPatternParityAudit({
            operators: [identity.slug],
            source: "fixtures",
          });
          sectionPattern = sp.operatorResults[0] || null;
        } catch (err) {
          errors.push(`section_pattern:${err?.message || err}`);
        }
      }

      const sources = [];
      if (identity.domain) {
        sources.push({
          sourceTitle: `${identity.companyName} official site`,
          sourceUrl: `https://www.${identity.domain}/`,
          origin: "factory_queue_canonical",
        });
      }
      if (identity.slug === "aimbridge-latam") {
        sources.push({
          sourceTitle: "Aimbridge Hospitality — Alex Fiz LATAM appointment",
          sourceUrl:
            "https://www.aimbridgehospitality.com/news/aimbridge-selects-alex-fiz-to-lead-its-latam-and-all-inclusive-divisions-/",
          origin: "parent_enterprise_labeled",
        });
      }
      provenance = evaluateOperatorSourceProvenanceByTab({
        operatorSlug: identity.slug,
        operatorName: identity.companyName,
        recordId: identity.recordId,
        sources,
      });
    }

    const remAfter = remediation?.results?.[0]?.after;
    const fieldPass =
      remAfter?.fieldAuditPass === true || tabFactory?.fieldAuditPass === true;
    const patternPass =
      remAfter?.sectionPatternPass === true ||
      sectionPattern?.pass === true ||
      tabFactory?.sectionPatternParity?.pass === true;
    const provenancePass = provenance?.pass === true;
    const tabFactoryPass =
      remAfter?.auditPass === true ||
      (Boolean(tabFactory?.auditPass) && fieldPass && patternPass && provenancePass);

    const resolved = resolveOperatorExplorerOsState({
      hasMasterRecord: Boolean(identity.recordId),
      hasFixturePack: fixturePack,
      provenancePass,
      fieldAuditPass: fieldPass,
      sectionPatternPass: patternPass,
      tabFactoryPass: isBaseline ? tabFactoryPass : Boolean(tabFactory?.auditPass) && fieldPass && patternPass && provenancePass,
      founderReviewPassed: false,
      activeReleaseApproved: false,
    });

    table.push({
      operatorSlug: identity.slug,
      recordId: identity.recordId,
      companyName: identity.companyName,
      kind: identity.kind,
      protectedBaseline: isProtectedOperatorQualityBaseline(identity.slug),
      explorerUrl: identity.explorerUrl,
      source,
      hasFixturePack: fixturePack,
      gates: {
        field_audit: fieldPass,
        section_pattern_parity: patternPass,
        source_provenance_by_tab: provenancePass,
        tab_factory_audit: tabFactoryPass,
      },
      failFindings: remAfter?.failFindings ?? tabFactory?.failFindings ?? null,
      canonicalState: resolved.state,
      allowedNextAction: resolved.nextAction,
      founderReviewAllowed: resolved.state === "founder_review_ready",
      activeReleaseAllowed: false,
      errors,
      remediationPreview: remAfter
        ? {
            failsBefore: remediation.results[0].before.failFindings,
            failsAfter: remAfter.failFindings,
            auditPassAfter: remAfter.auditPass,
          }
        : null,
    });
  }

  const byState = {};
  for (const row of table) {
    byState[row.canonicalState] = (byState[row.canonicalState] || 0) + 1;
  }

  const baselines = table.filter((r) => r.kind === "quality_baseline");
  const qualityBaselinesReady =
    baselines.length >= 2 &&
    baselines.every((r) => {
      const remOk = r.remediationPreview?.auditPassAfter === true;
      const gatesOk =
        r.gates?.field_audit &&
        r.gates?.section_pattern_parity &&
        r.gates?.source_provenance_by_tab;
      return remOk || gatesOk;
    });

  const nextFactory =
    listOperatorFactoryQueue({ status: "queued" })[0] ||
    listOperatorFactoryQueue({ status: "in_progress" })[0] ||
    listOperatorFactoryQueue({ status: "factory_ready" })[0] ||
    null;

  return {
    version: OPERATOR_EXPLORER_OS_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    stage,
    source,
    operators,
    table,
    summary: {
      byState,
      operatorsEvaluated: table.length,
      qualityBaselinesReady,
      nextFactoryOperator: nextFactory
        ? {
            slug: nextFactory.slug,
            recordId: nextFactory.recordId,
            companyName: nextFactory.companyName,
            explorerUrl: nextFactory.explorerUrl,
            notes: nextFactory.notes,
          }
        : null,
      canStartNextOperatorExplorer: qualityBaselinesReady && Boolean(nextFactory),
    },
  };
}

export function writeOperatorExplorerOsReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-explorer-os.json");
  const mdPath = path.join(reportsDir, "operator-explorer-os.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const next = report.summary.nextFactoryOperator;
  const md = [
    "# Operator Explorer OS",
    "",
    `Version: \`${report.version}\` · stage: **${report.stage}** · source: **${report.source}**`,
    `Generated: ${report.generatedAt}`,
    "",
    "## Ready for next Operator Explorer?",
    "",
    `- Quality baselines ready (Arbor + HE): **${report.summary.qualityBaselinesReady}**`,
    `- Can start next Operator Explorer: **${report.summary.canStartNextOperatorExplorer}**`,
    next
      ? `- **Next operator:** ${next.companyName} (\`${next.slug}\` / \`${next.recordId}\`)`
      : "- Next operator: (queue empty)",
    next ? `- Explorer: ${next.explorerUrl}` : "",
    next?.notes ? `- Notes: ${next.notes}` : "",
    "",
    "## Operators",
    "",
  ];
  for (const row of report.table) {
    md.push(
      `### ${row.companyName || row.operatorSlug}`,
      "",
      `- kind: **${row.kind}** · state: **${row.canonicalState}**`,
      `- next: \`${row.allowedNextAction}\``,
      `- gates: field=${row.gates?.field_audit} pattern=${row.gates?.section_pattern_parity} provenance=${row.gates?.source_provenance_by_tab} tabFactory=${row.gates?.tab_factory_audit}`,
      row.remediationPreview
        ? `- remediation preview: fails ${row.remediationPreview.failsBefore}→${row.remediationPreview.failsAfter} auditPass=${row.remediationPreview.auditPassAfter}`
        : null,
      ""
    );
  }
  fs.writeFileSync(mdPath, md.filter((l) => l != null).join("\n"));
  return { jsonPath, mdPath };
}
