#!/usr/bin/env node
/**
 * Validate P1 staging proof for an existing operator record.
 *
 *   node scripts/validate-operator-setup-p1-staging-proof.mjs --operator-id recXXXXXXXX
 *   node scripts/validate-operator-setup-p1-staging-proof.mjs --operator-id recXXX --deal-id recYYY
 *   node scripts/validate-operator-setup-p1-staging-proof.mjs --report reports/operator-setup-p1-staging-save-proof-....json
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadNewBaseOperatorBundle,
  buildPrefillObjectFromNewBaseRows,
} from "../api/lib/operator-setup-new-base-read.js";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { fetchDealScoringContext } from "../api/my-deals.js";
import { buildOperatorAlignmentCompaniesSnapshot } from "../lib/operator-alignment-company-utils.js";
import {
  buildP1StagingPayload,
  P1_PIPELINE_FIELDS,
  valuesMatch,
  pickFromMergedFields,
  pickFromPrefill,
  explorerWouldShow,
} from "./lib/operator-setup-p1-staging-spec.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

function parseArgs(argv) {
  const out = { operatorId: "", dealId: "", reportPath: "" };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--operator-id" && argv[i + 1]) out.operatorId = argv[++i];
    else if (a === "--deal-id" && argv[i + 1]) out.dealId = argv[++i];
    else if (a === "--report" && argv[i + 1]) out.reportPath = argv[++i];
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

async function main() {
  const args = parseArgs(process.argv);

  if (args.reportPath) {
    const p = path.isAbsolute(args.reportPath) ? args.reportPath : path.join(ROOT, args.reportPath);
    if (!fs.existsSync(p)) fail("Report not found: " + p);
    else {
      const j = JSON.parse(fs.readFileSync(p, "utf8"));
      pass("Report loaded: " + path.basename(p));
      if (j.verdict?.failures?.length) {
        for (const f of j.verdict.failures) fail(`Report failure ${f.id}: ${f.notes}`);
      } else pass("Report has no field failures");
      if (j.verdict?.phaseESafeToBegin?.startsWith("Partial") || j.verdict?.p1SavedThroughNewBaseWriter === "Yes") {
        pass("Report verdict: " + (j.verdict.phaseESafeToBegin || j.verdict.p1SavedThroughNewBaseWriter));
      }
      if (!args.operatorId && j.operator?.recordId) args.operatorId = j.operator.recordId;
    }
  }

  if (!args.operatorId) {
    fail("--operator-id required (or --report with recordId)");
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    fail("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");
    process.exit(1);
  }

  if (process.env.OPERATOR_SETUP_USE_NEW_BASE_WRITER !== "1") {
    console.log(
      "NOTE: OPERATOR_SETUP_USE_NEW_BASE_WRITER is not 1 in .env — validating read path only (expected for local default)."
    );
  }

  const bundle = await loadNewBaseOperatorBundle(args.operatorId);
  if (!bundle?.master) fail("Operator bundle not found: " + args.operatorId);
  else pass("loadNewBaseOperatorBundle OK");

  const merged = mergeTableFields(bundle);
  const prefill = buildPrefillObjectFromNewBaseRows(
    bundle.master,
    bundle.profile,
    bundle.platform,
    bundle.commercial,
    bundle.governance
  );
  pass("buildPrefillObjectFromNewBaseRows OK");

  const payload = buildP1StagingPayload();
  let failCount = 0;

  for (const spec of P1_PIPELINE_FIELDS) {
    if (spec.system) {
      const opId = pickFromMergedFields(merged, ["operator_id", "operatorId"]);
      if (!opId && !args.operatorId.startsWith("rec")) fail("operator_id missing on master");
      else pass("system field operator_id present or record id used");
      continue;
    }

    const expected = spec.formKeys.map((k) => payload[k]).find((v) => v != null && v !== "");
    const actual = pickFromMergedFields(merged, [...spec.airtableFields, ...spec.formKeys]);
    const pf = pickFromPrefill(prefill, spec.formKeys);
    const am = valuesMatch(expected, actual);
    const pm = valuesMatch(expected, pf);
    const ex = explorerWouldShow(prefill, spec, payload);

    if (!am.ok) {
      fail(`${spec.id}: Airtable readback — ${am.reason}`);
      failCount += 1;
    } else pass(`${spec.id}: Airtable readback`);

    if (!pm.ok) {
      fail(`${spec.id}: prefill — ${pm.reason}`);
      failCount += 1;
    } else pass(`${spec.id}: prefill`);

    if (ex.show === "No") {
      fail(`${spec.id}: Explorer would not display (${ex.note})`);
      failCount += 1;
    } else pass(`${spec.id}: Explorer ${ex.show}`);
  }

  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const cand = candidates.find((c) => c.operatorId === args.operatorId);
  if (!cand) {
    fail("Operator not in active OAS candidates (submission_status Active? test name filter?)");
    failCount += 1;
  } else pass("Operator in loadActiveOperatorCandidatesForAlignment");

  if (args.dealId) {
    const ctx = await fetchDealScoringContext(
      process.env.AIRTABLE_BASE_ID,
      process.env.AIRTABLE_API_KEY,
      args.dealId
    );
    if (!ctx) fail("Deal not found: " + args.dealId);
    else {
      const snap = await buildOperatorAlignmentCompaniesSnapshot(args.dealId, {
        dealFields: ctx.dealFields,
        locationData: ctx.locationData,
        mpData: ctx.mpData,
        siData: ctx.siData,
      });
      const row = (snap.companiesForConsideration || []).find((c) => c.operatorId === args.operatorId);
      if (!row) {
        fail("Operator not in companiesForConsideration for deal " + args.dealId);
        failCount += 1;
      } else {
        pass("Operator in OAS companies snapshot");
        if (!row.alignmentBand || !row.alignmentScoreOptional) {
          fail("company row missing alignment band or score");
          failCount += 1;
        } else {
          pass("alignmentBand + score present on company row");
        }
        if (!row.dataConfidenceLevel) fail("Strategy/OAS row missing dataConfidenceLevel");
        else pass("dataConfidenceLevel on company row");
        if (!row.companyName) fail("company row missing companyName");
        else pass("companyName on company row");
      }
    }
  }

  const profileJs = fs.readFileSync(
    path.join(ROOT, "public/js/operator-explorer-new-base-profile.js"),
    "utf8"
  );
  if (!profileJs.includes("operator-alignment-snapshot")) fail("Explorer profile missing OAS API");
  else pass("Explorer alignment context uses OAS companies API");

  if (failCount) {
    console.error(`\n${failCount} P1 pipeline check(s) failed.`);
    process.exit(1);
  }
  console.log("\nAll P1 staging proof validation checks passed.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
