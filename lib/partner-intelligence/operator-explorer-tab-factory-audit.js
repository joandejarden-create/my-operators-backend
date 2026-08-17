/**
 * Operator Explorer Tab Factory audit — tab-by-tab, field-by-field.
 * Default dry-run. auditPass = failFindings === 0 (patch plan is not a pass).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OPERATOR_QUALITY_BASELINE_OPERATORS,
  getOperatorQualityBaselineEntry,
} from "./operator-explorer-quality-baseline.js";
import { getOperatorFactoryQueueEntry } from "./operator-explorer-factory-queue.js";
import {
  OPERATOR_TAB_FACTORY_VERSION,
} from "./operator-explorer-tab-contracts.js";
import {
  loadOperatorFixturePayload,
  mergeLiveAndFixturePrefill,
} from "./operator-explorer-fixture-payload.js";
import { evaluateOperatorTabFactoryFromPayload } from "./operator-explorer-tab-factory-evaluate.js";

export { evaluateOperatorTabFactoryFromPayload } from "./operator-explorer-tab-factory-evaluate.js";

export const AUDIT_VERSION = OPERATOR_TAB_FACTORY_VERSION;
export const REPORT_JSON = "operator-explorer-tab-factory-audit.json";
export const REPORT_MD = "operator-explorer-tab-factory-audit.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

/**
 * Load live Operator Explorer detail prefill via intake detail handler.
 * @param {string} recordId
 */
export async function loadLiveOperatorPrefill(recordId) {
  const { default: getThirdPartyOperatorDetail } = await import(
    "../../api/third-party-operator-detail.js"
  );
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
      return this;
    },
  };
  await getThirdPartyOperatorDetail({ params: { recordId }, query: {}, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.success) {
    const errMsg =
      res.payload?.error ||
      res.payload?.message ||
      `HTTP ${res.statusCode}`;
    throw new Error(`Live operator detail failed for ${recordId}: ${errMsg}`);
  }
  const prefill = res.payload.prefill || res.payload.operator?.prefill;
  if (!prefill || typeof prefill !== "object") {
    throw new Error(`Live operator detail missing prefill for ${recordId}`);
  }
  return {
    prefill,
    operator: res.payload.operator || null,
    meta: res.payload.meta || null,
  };
}

/**
 * @param {string} slugOrRecordId
 * @param {{ source?: 'fixtures'|'live'|'merged' }} [opts]
 */
export async function auditOperatorTabFactory(slugOrRecordId, opts = {}) {
  const source = opts.source || "fixtures";
  const baseline = getOperatorQualityBaselineEntry(slugOrRecordId);
  const queued = getOperatorFactoryQueueEntry(slugOrRecordId);
  const entry = baseline || queued;
  if (!entry && (source === "fixtures" || source === "merged")) {
    throw new Error(
      `Operator "${slugOrRecordId}" is not in the quality baseline or factory queue; fixtures/merged require a registered operator. Use --source=live with a Master rec id for ad-hoc operators.`
    );
  }
  if (!entry?.recordId && (source === "live" || source === "merged")) {
    throw new Error(
      `Operator "${slugOrRecordId}" has no Master recordId yet — create Master before live/merged audit.`
    );
  }

  let fixturePayload = null;
  let live = null;
  let prefill = {};
  let fixtureFiles = [];
  let recordId = entry?.recordId || String(slugOrRecordId);
  let operatorSlug = entry?.slug || String(slugOrRecordId);
  let operatorName = entry?.companyName || String(slugOrRecordId);

  if (source === "fixtures" || source === "merged") {
    fixturePayload = loadOperatorFixturePayload(entry.slug);
    fixtureFiles = fixturePayload.fixtureFiles;
    prefill = { ...fixturePayload.prefill };
    operatorSlug = fixturePayload.slug;
    operatorName = fixturePayload.companyName;
    recordId = fixturePayload.recordId || recordId;
  }

  if (source === "live" || source === "merged") {
    live = await loadLiveOperatorPrefill(recordId);
    if (source === "live") {
      prefill = { ...(live.prefill || {}) };
    } else {
      prefill = mergeLiveAndFixturePrefill(live.prefill || {}, prefill);
    }
    if (live.operator?.fields?.["Company Name"] || live.operator?.fields?.company_name) {
      operatorName = String(
        live.operator.fields["Company Name"] || live.operator.fields.company_name
      );
    }
  }

  return evaluateOperatorTabFactoryFromPayload({
    operatorSlug,
    operatorName,
    recordId,
    prefill,
    source,
    fixtureFiles,
  });
}

/**
 * @param {{
 *   operators?: string[],
 *   source?: 'fixtures'|'live'|'merged'
 * }} [opts]
 */
export async function runOperatorTabFactoryAudit(opts = {}) {
  const source = opts.source || "fixtures";
  const operators =
    opts.operators?.length > 0
      ? opts.operators
      : OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug);

  const operatorResults = [];
  for (const id of operators) {
    operatorResults.push(await auditOperatorTabFactory(id, { source }));
  }

  return {
    version: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    source,
    operators,
    operatorResults,
    summary: {
      operatorsAudited: operatorResults.length,
      totalFailFindings: operatorResults.reduce((n, o) => n + o.failFindings, 0),
      totalEmptyRenderFails: operatorResults.reduce(
        (n, o) => n + o.emptyRenderFailFindings,
        0
      ),
      auditComplete: true,
      patchPlanComplete: operatorResults.every((o) => o.patchPlanComplete === true),
      auditPass: operatorResults.every((o) => o.auditPass === true),
    },
    auditComplete: true,
    patchPlanComplete: operatorResults.every((o) => o.patchPlanComplete === true),
    auditPass: operatorResults.every((o) => o.auditPass === true),
  };
}

function operatorMd(o) {
  const lines = [
    `# Tab Factory — ${o.operatorName}`,
    "",
    `Slug: \`${o.operatorSlug}\` · Record: \`${o.recordId || "—"}\``,
    `Source: **${o.source}** · Protected baseline: **${o.protectedBaseline}**`,
    `auditComplete: **${o.auditComplete}** · patchPlanComplete: **${o.patchPlanComplete}** · auditPass: **${o.auditPass}**`,
    `failFindings: **${o.failFindings}** · emptyRenderFails: **${o.emptyRenderFailFindings}**`,
    `decision: **${o.releaseQualityDecision}**`,
    "",
    "## Gates",
    "",
  ];
  for (const [k, v] of Object.entries(o.gates || {})) {
    lines.push(`- ${k}: **${v === null ? "pending_later_wave" : v}**`);
  }
  lines.push("", "## Tabs", "");
  for (const t of o.tabSummaries || []) {
    lines.push(
      `- **${t.tabName}**: pass=${t.passCount}/${t.fieldCount} · fail=${t.failCount} · auditPass=${t.auditPass}`
    );
  }
  if ((o.failFindingDetails || []).length) {
    lines.push("", "## Fail findings (hard)", "");
    for (const f of o.failFindingDetails.slice(0, 40)) {
      lines.push(
        `- \`${f.tabName}\` / \`${f.fieldKey}\` — ${f.resolution}: ${f.reason}`
      );
      if (f.proposedPatch) lines.push(`  - Patch: ${f.proposedPatch}`);
    }
    if (o.failFindingDetails.length > 40) {
      lines.push(`- … +${o.failFindingDetails.length - 40} more`);
    }
  }
  if ((o.fixtureFiles || []).length) {
    lines.push("", "## Fixture files", "");
    for (const f of o.fixtureFiles) lines.push(`- \`${f}\``);
  }
  lines.push("");
  return lines.join("\n");
}

export function writeOperatorTabFactoryAuditReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    "# Operator Explorer Tab Factory Audit",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Source: **${report.source}** · dryRun: **${report.dryRun}**`,
    "",
    "## Summary",
    "",
    `- Operators audited: **${report.summary.operatorsAudited}**`,
    `- Total failFindings: **${report.summary.totalFailFindings}**`,
    `- Total emptyRenderFails: **${report.summary.totalEmptyRenderFails}**`,
    `- patchPlanComplete: **${report.summary.patchPlanComplete}**`,
    `- auditPass: **${report.summary.auditPass}**`,
    "",
    "## Operators",
    "",
  ];
  for (const o of report.operatorResults) {
    md.push(
      `- **${o.operatorName}** (\`${o.operatorSlug}\`): auditPass=${o.auditPass} fails=${o.failFindings} decision=${o.releaseQualityDecision}`
    );
  }
  md.push("");
  for (const o of report.operatorResults) {
    md.push("---", "", operatorMd(o));
  }
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}
