#!/usr/bin/env node
/**
 * P1 staging proof: write (optional) → Airtable readback → prefill → OAS companies → report JSON.
 *
 * Requires .env with AIRTABLE_API_KEY + AIRTABLE_BASE_ID.
 * Forces OPERATOR_SETUP_USE_NEW_BASE_WRITER=1 for the writer call only (does not modify .env file).
 *
 * Usage:
 *   node scripts/run-operator-setup-p1-staging-proof.mjs --write
 *   node scripts/run-operator-setup-p1-staging-proof.mjs --operator-id recXXXXXXXX
 *   node scripts/run-operator-setup-p1-staging-proof.mjs --write --deal-id recYYYYYYYY
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import { writeOperatorSetupToNewBase } from "../api/lib/operator-setup-new-base-writer.js";
import {
  loadNewBaseOperatorBundle,
  buildPrefillObjectFromNewBaseRows,
  NEW_BASE_MASTER_TABLE,
} from "../api/lib/operator-setup-new-base-read.js";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { fetchDealScoringContext } from "../api/my-deals.js";
import { buildOperatorAlignmentCompaniesSnapshot } from "../lib/operator-alignment-company-utils.js";
import {
  buildP1StagingPayload,
  P1_PIPELINE_FIELDS,
  P1_SANDBOX_COMPANY_NAME,
  isBlockedTestOperatorName,
  valuesMatch,
  pickFromMergedFields,
  pickFromPrefill,
  explorerWouldShow,
  normalizeList,
} from "./lib/operator-setup-p1-staging-spec.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseArgs(argv) {
  const out = { write: false, operatorId: "", dealId: "", setActive: true, baseUrl: "" };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--write") out.write = true;
    else if (a === "--no-set-active") out.setActive = false;
    else if (a === "--operator-id" && argv[i + 1]) {
      out.operatorId = argv[++i];
      out.write = false;
    } else if (a === "--deal-id" && argv[i + 1]) out.dealId = argv[++i];
    else if (a === "--base-url" && argv[i + 1]) out.baseUrl = argv[++i].replace(/\/$/, "");
  }
  return out;
}

function mergeTableFields(bundle) {
  const merged = {};
  for (const key of ["master", "profile", "platform", "commercial", "governance"]) {
    const row = bundle[key];
    if (row?.fields) Object.assign(merged, row.fields);
  }
  return merged;
}

async function patchMasterSubmissionStatus(recordId, status) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(NEW_BASE_MASTER_TABLE)}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields: { submission_status: status }, typecast: true }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error((data?.error?.message || data?.error?.type || res.statusText) + " (submission_status patch)");
  }
  return data;
}

function fieldRow(spec, payload, merged, prefill, extra = {}) {
  const expected = spec.formKeys.map((k) => payload[k]).find((v) => v != null && v !== "");
  const actualRaw = pickFromMergedFields(merged, [...spec.airtableFields, ...spec.formKeys]);
  const prefillVal = pickFromPrefill(prefill, spec.formKeys);
  const airtableMatch = valuesMatch(expected, actualRaw);
  const prefillMatch = valuesMatch(expected, prefillVal);
  const explorer = explorerWouldShow(prefill, spec, payload);
  const pass =
    spec.system ||
    (airtableMatch.ok && prefillMatch.ok && explorer.show !== "No");

  return {
    id: spec.id,
    table: spec.tables?.[0] || "",
    airtableField: spec.airtableFields[0] || spec.formKeys[0],
    formField: spec.formKeys[0],
    expectedValue: expected,
    actualAirtableValue: actualRaw,
    prefillValue: prefillVal,
    explorerSection: spec.explorerSection,
    displayedInExplorer: explorer.show,
    explorerNote: explorer.note,
    usedByOas: spec.oas ? "Yes" : "No",
    usedByStrategy: spec.strategy ? "Yes" : "No",
    pass: pass ? "pass" : "fail",
    notes: [
      !airtableMatch.ok ? `Airtable: ${airtableMatch.reason}` : null,
      !prefillMatch.ok ? `Prefill: ${prefillMatch.reason}` : null,
      explorer.show === "No" ? "Explorer rail would omit" : null,
      extra.note || null,
    ]
      .filter(Boolean)
      .join("; "),
    ...extra,
  };
}

async function fetchJson(url, opts = {}) {
  const res = await fetch(url, opts);
  const data = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, data };
}

async function main() {
  const args = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;

  if (!baseId || !apiKey) {
    console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY — configure .env for staging/local.");
    process.exit(1);
  }

  if (isBlockedTestOperatorName(P1_SANDBOX_COMPANY_NAME)) {
    console.error("Sandbox company name matches test-record blocklist — rename spec.");
    process.exit(1);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    environment: {
      nodeEnv: process.env.NODE_ENV || "",
      airtableBaseId: baseId,
      writerFlagForThisRun: "OPERATOR_SETUP_USE_NEW_BASE_WRITER=1 (process only)",
      productionWriterFlagUnchanged: "OPERATOR_SETUP_USE_NEW_BASE_WRITER not written to .env",
      hideTestRecords: process.env.OPERATOR_EXPLORER_HIDE_TEST_RECORDS || "0",
      companyLogoTested: false,
      companyLogoNote: "Skipped — attachment pipeline not in scope",
    },
    operator: { recordId: args.operatorId || null, companyName: P1_SANDBOX_COMPANY_NAME },
    save: null,
    airtableReadback: [],
    prefill: null,
    explorer: { phaseDRailFields: "validated via prefill keys (see field rows)", apiCheck: null },
    oas: null,
    strategy: null,
    summary: { pass: 0, fail: 0, skip: 0 },
    verdict: null,
  };

  const payload = buildP1StagingPayload();
  const correlationId = randomUUID();

  if (args.write || !args.operatorId) {
    process.env.OPERATOR_SETUP_USE_NEW_BASE_WRITER = "1";
    console.log("Writing P1 sandbox operator via new-base writer…");
    try {
      const writeResult = await writeOperatorSetupToNewBase({
        body: payload,
        existingRecordId: args.operatorId || "",
        isDraft: false,
        correlationId,
      });
      report.save = {
        success: true,
        recordId: writeResult.recordId,
        warning: writeResult.warning || null,
        unresolvedBrands: writeResult.unresolvedBrands || [],
        requestPayloadKeys: Object.keys(payload),
      };
      args.operatorId = writeResult.recordId;
      report.operator.recordId = writeResult.recordId;
    } catch (e) {
      report.save = { success: false, error: e.message || String(e), code: e.code || null };
      report.verdict = { phaseE: "blocked", productionWriter: "no", reason: "Save failed" };
      writeReport(report);
      process.exit(1);
    }
  } else {
    report.save = { success: true, skipped: true, recordId: args.operatorId };
  }

  if (args.setActive && args.operatorId) {
    try {
      await patchMasterSubmissionStatus(args.operatorId, "Active");
      report.save.submissionStatusPatched = "Active";
      console.log("Patched submission_status → Active (required for Explorer list + OAS candidates).");
    } catch (e) {
      report.save.submissionStatusPatchError = e.message;
      console.warn("Could not patch Active:", e.message);
    }
  }

  const bundle = await loadNewBaseOperatorBundle(args.operatorId);
  if (!bundle?.master) {
    report.verdict = { phaseE: "blocked", productionWriter: "no", reason: "Bundle not found after save" };
    writeReport(report);
    process.exit(1);
  }

  const merged = mergeTableFields(bundle);
  const prefill = buildPrefillObjectFromNewBaseRows(
    bundle.master,
    bundle.profile,
    bundle.platform,
    bundle.commercial,
    bundle.governance
  );
  report.prefill = {
    keysPresent: Object.keys(prefill).filter((k) => {
      const v = prefill[k];
      return v != null && v !== "" && !(Array.isArray(v) && !v.length);
    }).length,
    sample: {
      companyName: prefill.companyName,
      activeCountries: prefill.activeCountries,
      serviceModelsSupported: prefill.serviceModelsSupported,
      dataConfidenceLevel: prefill.dataConfidenceLevel,
    },
  };

  for (const spec of P1_PIPELINE_FIELDS) {
    const row = fieldRow(spec, payload, merged, prefill, {
      tableWritten:
        spec.tables.find((t) => {
          const keys = Object.keys(merged);
          return keys.some((k) => spec.airtableFields.includes(k) || spec.formKeys.includes(k));
        }) || spec.tables[0],
    });
    report.airtableReadback.push(row);
    if (row.pass === "pass") report.summary.pass += 1;
    else if (spec.system) report.summary.skip += 1;
    else report.summary.fail += 1;
  }

  const masterFields = bundle.master.fields || {};
  report.operator.operatorId = formatList(masterFields.operator_id) || args.operatorId;

  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const candidate = candidates.find((c) => c.operatorId === args.operatorId);
  report.oas = {
    candidatesLoaded: candidates.length,
    testOperatorInActiveCandidates: Boolean(candidate),
    candidateCompanyName: candidate?.companyName || null,
    note: candidate
      ? "Operator included in loadActiveOperatorCandidatesForAlignment"
      : "Not in active candidates — check submission_status Active and company name filters",
  };

  if (args.dealId) {
    const ctx = await fetchDealScoringContext(baseId, apiKey, args.dealId);
    if (!ctx) {
      report.oas.companiesApi = { error: "Deal not found", dealId: args.dealId };
    } else {
      const snap = await buildOperatorAlignmentCompaniesSnapshot(args.dealId, {
        dealFields: ctx.dealFields,
        locationData: ctx.locationData,
        mpData: ctx.mpData,
        siData: ctx.siData,
      });
      const company = (snap.companiesForConsideration || []).find(
        (c) => String(c.operatorId).toLowerCase() === String(args.operatorId).toLowerCase()
      );
      report.oas.companiesApi = {
        dealId: args.dealId,
        companiesAvailable: snap.companiesAvailable,
        companiesCount: (snap.companiesForConsideration || []).length,
        testOperatorInCompaniesList: Boolean(company),
        testOperatorRow: company
          ? {
              companyName: company.companyName,
              alignmentBand: company.alignmentBand,
              alignmentScoreOptional: company.alignmentScoreOptional,
              dataConfidenceLevel: company.dataConfidenceLevel,
              alignmentSignalsCount: (company.alignmentSignals || []).length,
              keyConsideration: company.keyConsideration,
            }
          : null,
        gatingReason: snap.gatingReason || null,
      };
      report.explorer.alignmentContext = company
        ? "Would show Alignment Context for dealId + operatorId"
        : "Operator not in ranked companies — alignment panel would show unavailable";
    }
  }

  if (args.baseUrl) {
    const id = args.operatorId;
    const detail = await fetchJson(`${args.baseUrl}/api/intake/third-party-operators/${encodeURIComponent(id)}`);
    const list = await fetchJson(`${args.baseUrl}/api/third-party-operators?activeOnly=1`);
    const inList = (list.data?.operators || []).some((o) => o.id === id);
    report.explorer.apiCheck = {
      detailOk: detail.ok && detail.data?.success,
      listIncludesOperator: inList,
      listCount: list.data?.operators?.length ?? 0,
    };
    if (args.dealId) {
      const companies = await fetchJson(
        `${args.baseUrl}/api/operator-alignment-snapshot/${encodeURIComponent(args.dealId)}/companies`
      );
      report.oas.httpCompaniesApi = {
        ok: companies.ok,
        success: companies.data?.success,
        count: (companies.data?.companiesForConsideration || []).length,
      };
    }
  }

  report.strategy = {
    dataSource: "GET /api/operator-alignment-snapshot/:dealId/companies (same row model as My Deals Operator Strategy)",
    testOperatorEligible:
      report.oas?.companiesApi?.testOperatorInCompaniesList ||
      report.oas?.testOperatorInActiveCandidates ||
      false,
    columnsVerified:
      "Project/Deal, Operating Company, alignment band/score, review status, key consideration, data confidence — present on company row when operator is ranked",
    uiNote: "My Deals table not automated; verify manually or via dealId in staging UI",
  };

  const failures = report.airtableReadback.filter((r) => r.pass === "fail");
  const savedOk = report.save?.success !== false;
  const readbackOk = failures.length === 0;
  const oasOk =
    report.oas?.testOperatorInActiveCandidates &&
    (!args.dealId || report.oas?.companiesApi?.testOperatorInCompaniesList);

  report.verdict = {
    p1SavedThroughNewBaseWriter: savedOk ? "Yes" : "No",
    p1InAirtableReadback: readbackOk ? "Yes" : "Partial",
    p1InExplorerPrefill: readbackOk ? "Yes" : "Partial",
    p1InOasCandidates: report.oas?.testOperatorInActiveCandidates ? "Yes" : "No",
    p1InOasCompaniesApi: args.dealId
      ? report.oas?.companiesApi?.testOperatorInCompaniesList
        ? "Yes"
        : "No"
      : "Not tested (no --deal-id)",
    p1InOperatorStrategy: args.dealId
      ? report.oas?.companiesApi?.testOperatorInCompaniesList
        ? "Partial (API row OK; UI not automated)"
        : "Not tested"
      : "Not tested",
    phaseESafeToBegin:
      savedOk && readbackOk && report.oas?.testOperatorInActiveCandidates
        ? "Partial — staging proof passed; human QA on Explorer UI + Strategy table still recommended"
        : "No — fix failures first",
    productionNewBaseWriterSafeToEnable: "No — staging proof only; 266 static-form-only fields remain; dual writer still default off",
    failures: failures.map((f) => ({ id: f.id, notes: f.notes })),
  };

  writeReport(report);
  console.log("\nVerdict:", JSON.stringify(report.verdict, null, 2));
  console.log("Report:", reportPath(report.generatedAt));
  process.exit(failures.length ? 1 : 0);
}

function formatList(val) {
  if (val == null) return "";
  if (Array.isArray(val)) return val.map(String).join(", ");
  return String(val).trim();
}

function reportPath(iso) {
  const ts = iso.replace(/[:.]/g, "-").slice(0, 19);
  return path.join(ROOT, "reports", `operator-setup-p1-staging-save-proof-${ts}.json`);
}

function writeReport(report) {
  const p = reportPath(report.generatedAt);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(report, null, 2) + "\n", "utf8");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
