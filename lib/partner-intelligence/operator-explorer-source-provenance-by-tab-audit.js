/**
 * Operator Explorer source provenance by tab — audit runner + reports.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { OPERATOR_QUALITY_BASELINE_OPERATORS } from "./operator-explorer-quality-baseline.js";
import { getOperatorFactoryQueueEntry } from "./operator-explorer-factory-queue.js";
import {
  OPERATOR_SOURCE_PROVENANCE_VERSION,
  collectFixtureProvenanceSources,
  evaluateOperatorSourceProvenanceByTab,
  formatOperatorSourceProvenanceMarkdown,
  loadLiveOperatorPartnerSources,
} from "./operator-explorer-source-provenance-by-tab.js";

export {
  evaluateOperatorSourceProvenanceByTab,
  formatOperatorSourceProvenanceMarkdown,
  OPERATOR_SOURCE_PROVENANCE_VERSION,
} from "./operator-explorer-source-provenance-by-tab.js";

export const REPORT_JSON = "operator-explorer-source-provenance-by-tab.json";
export const REPORT_MD = "operator-explorer-source-provenance-by-tab.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

/**
 * @param {{ operators?: string[], source?: 'fixtures'|'live'|'merged' }} [opts]
 */
export async function runOperatorSourceProvenanceAudit(opts = {}) {
  const source = opts.source || "fixtures";
  const operators =
    opts.operators?.length > 0
      ? opts.operators
      : OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug);

  const operatorResults = [];
  for (const id of operators) {
    const entry =
      OPERATOR_QUALITY_BASELINE_OPERATORS.find((o) => o.slug === id || o.recordId === id) ||
      getOperatorFactoryQueueEntry(id);
    if (!entry) {
      throw new Error(`Unknown operator for provenance audit: ${id}`);
    }

    let sources = collectFixtureProvenanceSources(entry.slug);
    let liveError = null;
    if ((source === "live" || source === "merged") && entry.recordId) {
      const live = await loadLiveOperatorPartnerSources(entry.recordId);
      if (Array.isArray(live)) {
        if (source === "live") sources = live;
        else {
          const seen = new Set(sources.map((s) => s.sourceUrl));
          for (const s of live) {
            if (s.sourceUrl && !seen.has(s.sourceUrl)) {
              sources.push(s);
              seen.add(s.sourceUrl);
            }
          }
        }
      } else if (live?.error) {
        liveError = live.error;
        if (source === "live") {
          throw new Error(`Live PI source load failed for ${entry.slug}: ${live.error}`);
        }
      }
    }

    const evaluated = evaluateOperatorSourceProvenanceByTab({
      operatorSlug: entry.slug,
      operatorName: entry.companyName,
      recordId: entry.recordId,
      sources,
    });
    operatorResults.push({
      ...evaluated,
      sourceMode: source,
      liveError,
      sourceCount: sources.length,
    });
  }

  return {
    version: OPERATOR_SOURCE_PROVENANCE_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    source,
    operators,
    operatorResults,
    summary: {
      operatorsAudited: operatorResults.length,
      pass: operatorResults.filter((o) => o.pass).length,
      fail: operatorResults.filter((o) => !o.pass).length,
      failingSlugs: operatorResults.filter((o) => !o.pass).map((o) => o.operatorSlug),
    },
    auditPass: operatorResults.every((o) => o.pass === true),
  };
}

export function writeOperatorSourceProvenanceReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    "# Operator Explorer Source Provenance by Tab",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Source: **${report.source}** · dryRun: **${report.dryRun}**`,
    "",
    `Pass: **${report.auditPass}** · pass=${report.summary.pass} fail=${report.summary.fail}`,
    `Failing: ${report.summary.failingSlugs.join(", ") || "(none)"}`,
    "",
    ...report.operatorResults.map(formatOperatorSourceProvenanceMarkdown),
  ];
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}
