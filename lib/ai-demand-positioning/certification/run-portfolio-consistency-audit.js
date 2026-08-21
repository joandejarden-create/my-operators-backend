/**
 * Cross-property ADP methodology consistency audit.
 * Compares certification fingerprints across LIVE_EXISTING_HOTEL_PROPERTY_IDS.
 */

import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { LIVE_EXISTING_HOTEL_PROPERTY_IDS, ADP_CERTIFICATION_VERSION } from "./certification-status.js";
import { runPropertyCertification } from "./run-property-certification.js";

function fingerprint(report) {
  const scenario = report.sections?.scenarioUniverse || {};
  const providers = report.sections?.providerCompleteness || {};
  const metrics = report.sections?.metricReconciliation || [];
  return {
    propertyId: report.propertyId,
    status: report.status,
    classification: report.classification?.classification || null,
    inOfficialFour: Boolean(report.classification?.inOfficialFour),
    periodId: report.periodId,
    measurementContractVersion: report.measurementContractVersion,
    measurementContractHash: report.measurementContractHash,
    certificationVersion: report.certificationVersion,
    totalScenarios: scenario.totalScenarios,
    standardScenarios: scenario.standard,
    propertySpecificScenarios: scenario.propertySpecific,
    byIntent: scenario.byIntent || {},
    providerExpected: Object.fromEntries(
      Object.entries(providers.byProvider || {}).map(([p, row]) => [
        p,
        { expected: row.expected, success: row.success, failed: row.failed, missing: row.missing },
      ])
    ),
    considerationPublished: metrics.find((m) => m.metric === "AI Consideration Rate")?.published ?? null,
    scenarioPresencePublished: metrics.find((m) => m.metric === "AI Scenario Presence")?.published ?? null,
    materialFailCount: report.materialFailCount,
    disclosureCount: report.disclosureCount,
    gateStatuses: Object.fromEntries((report.gates || []).map((g) => [g.gateId, g.status])),
  };
}

function classifyDrift(field, values) {
  const uniq = [...new Set(values.map((v) => JSON.stringify(v)))];
  if (uniq.length <= 1) return { field, status: "EXPECTED_AND_GOVERNED", values: values[0] };
  // Phillips standalone may differ on classification / period marker — governed
  if (field === "classification" || field === "inOfficialFour") {
    return { field, status: "EXPECTED_AND_GOVERNED", note: "Phillips CERTIFIED_STANDALONE vs official four", values };
  }
  if (field === "totalScenarios" || field === "byIntent" || field === "propertySpecificScenarios" || field === "standardScenarios") {
    return {
      field,
      status: "EXPECTED_AND_GOVERNED",
      note: "Market-specific scenario packs differ by design under the same contract",
      values,
    };
  }
  if (field === "periodId" || field === "considerationPublished" || field === "scenarioPresencePublished") {
    return { field, status: "EXPECTED_AND_GOVERNED", note: "Property outcomes differ; methodology fingerprint separate", values };
  }
  return { field, status: "INCONSISTENT", values };
}

/**
 * @param {object} options
 * @param {string[]} [options.propertyIds]
 * @param {object[]} [options.reports] precomputed property reports
 */
export async function runPortfolioConsistencyAudit(options = {}) {
  const propertyIds = options.propertyIds || [...LIVE_EXISTING_HOTEL_PROPERTY_IDS];
  const reports = options.reports || [];
  for (const id of propertyIds) {
    if (!reports.find((r) => r.propertyId === id)) {
      reports.push(await runPropertyCertification(id));
    }
  }

  const fps = reports.filter((r) => r.ok !== false).map(fingerprint);
  const fields = [
    "measurementContractVersion",
    "measurementContractHash",
    "certificationVersion",
    "classification",
    "inOfficialFour",
    "totalScenarios",
    "standardScenarios",
    "propertySpecificScenarios",
    "byIntent",
  ];

  const drift = [];
  for (const field of fields) {
    drift.push(classifyDrift(field, fps.map((f) => f[field])));
  }

  // Gate status matrix — soft compare
  const gateIds = [...new Set(fps.flatMap((f) => Object.keys(f.gateStatuses || {})))];
  const gateMatrix = {};
  for (const g of gateIds) {
    gateMatrix[g] = Object.fromEntries(fps.map((f) => [f.propertyId, f.gateStatuses[g] || "—"]));
  }

  const inconsistent = drift.filter((d) => d.status === "INCONSISTENT");
  const generatedAt = new Date().toISOString();

  return {
    ok: true,
    auditVersion: ADP_CERTIFICATION_VERSION,
    generatedAt,
    propertyCount: fps.length,
    fingerprints: fps,
    drift,
    inconsistentCount: inconsistent.length,
    gateMatrix,
    methodologyVerdict:
      inconsistent.length === 0
        ? "SAME_METHODOLOGY_CONTRACT_ACROSS_PORTFOLIO"
        : "METHODOLOGY_DRIFT_DETECTED",
    notes: [
      "Scenario pack size differences by market are EXPECTED_AND_GOVERNED under ADP_MEASUREMENT_CONTRACT_V1.",
      "Hotel Phillips is CERTIFIED_STANDALONE and excluded from the official four baseline set.",
      "Do not auto-change formulas when drift is INCONSISTENT — escalate to founder.",
    ],
  };
}

export function renderPortfolioMarkdown(audit) {
  const lines = [];
  lines.push(`# ADP Portfolio Consistency Audit`);
  lines.push(``);
  lines.push(`Generated: ${audit.generatedAt}`);
  lines.push(`Audit version: ${audit.auditVersion}`);
  lines.push(`Verdict: **${audit.methodologyVerdict}**`);
  lines.push(``);
  lines.push(`## Property fingerprints`);
  lines.push(``);
  lines.push(
    `| Property | Status | Classification | Scenarios | Cons % | Scen % | Contract hash |`
  );
  lines.push(`| --- | --- | --- | --- | --- | --- | --- |`);
  for (const f of audit.fingerprints || []) {
    lines.push(
      `| \`${f.propertyId}\` | ${f.status} | ${f.classification} | ${f.totalScenarios} | ${f.considerationPublished ?? "—"} | ${f.scenarioPresencePublished ?? "—"} | \`${String(f.measurementContractHash || "").slice(0, 12)}\` |`
    );
  }
  lines.push(``);
  lines.push(`## Drift classification`);
  lines.push(``);
  lines.push(`| Field | Status | Note |`);
  lines.push(`| --- | --- | --- |`);
  for (const d of audit.drift || []) {
    lines.push(`| \`${d.field}\` | ${d.status} | ${d.note || "—"} |`);
  }
  lines.push(``);
  lines.push(`## Gate matrix`);
  lines.push(``);
  const props = (audit.fingerprints || []).map((f) => f.propertyId);
  lines.push(`| Gate | ${props.map((p) => p.replace("adp_", "")).join(" | ")} |`);
  lines.push(`| --- | ${props.map(() => "---").join(" | ")} |`);
  for (const [gateId, row] of Object.entries(audit.gateMatrix || {})) {
    lines.push(`| \`${gateId}\` | ${props.map((p) => row[p] || "—").join(" | ")} |`);
  }
  lines.push(``);
  for (const n of audit.notes || []) lines.push(`- ${n}`);
  lines.push(``);
  return lines.join("\n");
}

export function writePortfolioConsistencyAudit(audit, root = process.cwd()) {
  const dir = join(root, "reports/ai-demand-positioning/certification");
  mkdirSync(dir, { recursive: true });
  const md = join(dir, "ADP_PORTFOLIO_CONSISTENCY_AUDIT.md");
  const json = join(dir, "ADP_PORTFOLIO_CONSISTENCY_AUDIT.json");
  writeFileSync(json, JSON.stringify(audit, null, 2), "utf8");
  writeFileSync(md, renderPortfolioMarkdown(audit), "utf8");
  return { dir, md, json };
}
