/**
 * Operator Explorer Section Pattern Parity audit runner + reports.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { OPERATOR_QUALITY_BASELINE_OPERATORS } from "./operator-explorer-quality-baseline.js";
import {
  OPERATOR_SECTION_PATTERN_PARITY_VERSION,
  evaluateOperatorSectionPatternParity,
} from "./operator-explorer-section-pattern-parity.js";
import {
  loadOperatorFixturePayload,
  mergeLiveAndFixturePrefill,
} from "./operator-explorer-fixture-payload.js";
import { loadLiveOperatorPrefill } from "./operator-explorer-tab-factory-audit.js";

export { evaluateOperatorSectionPatternParity } from "./operator-explorer-section-pattern-parity.js";
export { OPERATOR_SECTION_PATTERN_PARITY_VERSION };

export const REPORT_JSON = "operator-explorer-section-pattern-parity-audit.json";
export const REPORT_MD = "operator-explorer-section-pattern-parity-audit.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

async function loadPrefillForOperator(slugOrId, source) {
  const fixture = loadOperatorFixturePayload(slugOrId);
  let prefill = { ...fixture.prefill };
  if (source === "live" || source === "merged") {
    const live = await loadLiveOperatorPrefill(fixture.recordId);
    prefill =
      source === "live"
        ? { ...(live.prefill || {}) }
        : mergeLiveAndFixturePrefill(live.prefill || {}, prefill);
  }
  return {
    operatorSlug: fixture.slug,
    operatorName: fixture.companyName,
    recordId: fixture.recordId,
    prefill,
    fixtureFiles: fixture.fixtureFiles,
  };
}

/**
 * @param {{ operators?: string[], source?: 'fixtures'|'live'|'merged' }} [opts]
 */
export async function runOperatorSectionPatternParityAudit(opts = {}) {
  const source = opts.source || "fixtures";
  const operators =
    opts.operators?.length > 0
      ? opts.operators
      : OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug);

  const operatorResults = [];
  for (const id of operators) {
    const loaded = await loadPrefillForOperator(id, source);
    const evaluated = evaluateOperatorSectionPatternParity({
      ...loaded,
      source,
    });
    operatorResults.push({ ...evaluated, fixtureFiles: loaded.fixtureFiles });
  }

  const pass = operatorResults.filter((o) => o.pass).length;
  const fail = operatorResults.length - pass;

  return {
    version: OPERATOR_SECTION_PATTERN_PARITY_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    source,
    operators,
    operatorResults,
    summary: {
      operatorsAudited: operatorResults.length,
      pass,
      fail,
      failingSlugs: operatorResults.filter((o) => !o.pass).map((o) => o.operatorSlug),
      failingSections: Object.fromEntries(
        operatorResults.map((o) => [o.operatorSlug, o.failingSectionIds])
      ),
    },
    auditPass: fail === 0,
  };
}

export function writeOperatorSectionPatternParityReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    "# Operator Explorer Section Pattern Parity Audit",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Source: **${report.source}** · dryRun: **${report.dryRun}**`,
    "",
    "## Summary",
    "",
    `- Operators audited: **${report.summary.operatorsAudited}**`,
    `- Pass: **${report.summary.pass}** · Fail: **${report.summary.fail}**`,
    `- Failing: ${report.summary.failingSlugs.join(", ") || "(none)"}`,
    "",
  ];

  for (const o of report.operatorResults) {
    md.push(
      `## ${o.operatorName} (\`${o.operatorSlug}\`)`,
      "",
      `pass: **${o.pass}** · sections ${o.passCount}/${o.sectionCount}`,
      ""
    );
    for (const s of o.sections) {
      md.push(`- **${s.sectionId}**: ${s.pass ? "PASS" : "FAIL"} — ${s.status}: ${s.detail}`);
    }
    md.push("");
  }

  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}
