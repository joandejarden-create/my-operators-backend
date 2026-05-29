#!/usr/bin/env node
/**
 * P1 UI QA helper — API + data-layer checks (no browser auth required).
 * Browser My Deals / OAS HTTP require Memberstack Bearer; use logged-in manual pass for full UI.
 *
 *   node scripts/qa-operator-setup-p1-ui-before-phase-e.mjs
 *   node scripts/qa-operator-setup-p1-ui-before-phase-e.mjs --base-url http://localhost:8080
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchDealScoringContext } from "../api/my-deals.js";
import { buildOperatorAlignmentCompaniesSnapshot } from "../lib/operator-alignment-company-utils.js";
import {
  loadNewBaseOperatorBundle,
  buildPrefillObjectFromNewBaseRows,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const OP_ID = "recBVEgtm8cS96mu7";
const DEAL_ID = "recIeGRZP21udmTnt";
const COMPANY = "P1 Staging Proof Sandbox (Do Not Use Production)";

const EXPLORER_SECTIONS = [
  "Profile Snapshot",
  "Market Presence",
  "Operating Profile",
  "Services & Platform",
  "Opening / Transition Support",
  "Owner Reporting & Governance",
  "Brand / Portfolio Experience",
];

const PREFILL_KEYS_BY_SECTION = {
  "Profile Snapshot": ["companyName", "companyDescription", "website", "headquarters", "dataConfidenceLevel", "lastUpdatedDate"],
  "Market Presence": ["activeCountries", "activeMarkets", "marketPresenceType"],
  "Operating Profile": ["serviceModelsSupported", "chainScalesSupported", "managementStructuresSupported"],
  "Services & Platform": ["offeredServices", "revenueManagementCapability", "salesPlatform", "fbCapabilityLevel"],
  "Opening / Transition Support": ["newBuildOpeningExperience", "preOpeningSupportCapability"],
  "Owner Reporting & Governance": ["ownerReportingLevel", "governanceCadence", "sourceType"],
  "Brand / Portfolio Experience": ["brandFamiliesOperated", "softBrandLifestyleExperience", "brandsPortfolioDetail"],
};

function hasVal(v) {
  if (v == null || v === "") return false;
  if (Array.isArray(v)) return v.length > 0;
  return true;
}

function sectionPass(prefill, section) {
  const keys = PREFILL_KEYS_BY_SECTION[section] || [];
  const populated = keys.filter((k) => hasVal(prefill[k]));
  return { populated, total: keys.length, ok: populated.length > 0 };
}

async function main() {
  const baseUrl = (process.argv.find((a, i) => process.argv[i - 1] === "--base-url") || "").replace(/\/$/, "");
  const out = {
    generatedAt: new Date().toISOString(),
    operatorId: OP_ID,
    dealId: DEAL_ID,
    companyName: COMPANY,
    explorer: { sections: {}, intakeDetailPublic: null },
    oas: {},
    strategy: {},
    http: {},
    overall: "pending",
  };

  const bundle = await loadNewBaseOperatorBundle(OP_ID);
  if (!bundle?.master) {
    console.error("FAIL: operator bundle not found");
    process.exit(1);
  }
  const prefill = buildPrefillObjectFromNewBaseRows(
    bundle.master,
    bundle.profile,
    bundle.platform,
    bundle.commercial,
    bundle.governance
  );

  out.explorer.companyName = prefill.companyName;
  out.explorer.notMock = !String(prefill.companyName || "").includes("Sample Operator Platform");
  for (const sec of EXPLORER_SECTIONS) {
    out.explorer.sections[sec] = sectionPass(prefill, sec);
  }

  const ctx = await fetchDealScoringContext(process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_API_KEY, DEAL_ID);
  const snap = await buildOperatorAlignmentCompaniesSnapshot(DEAL_ID, {
    dealFields: ctx.dealFields,
    locationData: ctx.locationData,
    mpData: ctx.mpData,
    siData: ctx.siData,
  });
  const row = (snap.companiesForConsideration || []).find((c) => c.operatorId === OP_ID);
  out.oas = {
    companiesAvailable: snap.companiesAvailable,
    companiesCount: (snap.companiesForConsideration || []).length,
    operatorFound: Boolean(row),
    alignmentBand: row?.alignmentBand,
    score: row?.alignmentScoreOptional,
    dataConfidence: row?.dataConfidenceLevel,
    reviewStatus: row?.reviewStatusLabel,
    keyConsideration: row?.keyConsideration,
    alignmentSignalsSample: (row?.alignmentSignals || []).slice(0, 3),
    whatNeedsValidationSample: (row?.whatNeedsValidation || row?.reviewConsiderations || []).slice(0, 3),
  };

  out.strategy = {
    note: "Table uses API row model; Alignment Signal is a filter dropdown, not a column",
    expectedColumns: [
      "Project / Deal",
      "Operating Company",
      "Project Location",
      "Score",
      "Review Status",
      "Key Consideration",
      "Data Confidence",
      "Call to Action",
    ],
    rowWouldRender: Boolean(row),
    simulatedRow: row
      ? {
          projectName: snap.dealContext?.dealName,
          companyName: row.companyName,
          score: row.alignmentScoreOptional,
          reviewStatus: row.reviewStatusLabel,
          keyConsideration: row.keyConsideration,
          dataConfidence: row.dataConfidenceLevel,
          operatorId: row.operatorId,
          dealId: DEAL_ID,
        }
      : null,
    ctaExpectations: {
      viewOas: `dealId=${DEAL_ID}`,
      viewOcs: `dealId=${DEAL_ID}`,
      openProfile: `operator-explorer-gold-mock.html?id=${OP_ID}&dealId=${DEAL_ID}`,
      addToReviewDisabled: true,
      prepareOutreachDisabled: true,
    },
  };

  if (baseUrl) {
    const detail = await fetch(`${baseUrl}/api/intake/third-party-operators/${OP_ID}`);
    out.http.intakeDetail = { status: detail.status, success: (await detail.json()).success };
    const companies = await fetch(`${baseUrl}/api/operator-alignment-snapshot/${DEAL_ID}/companies`);
    const cj = await companies.json().catch(() => ({}));
    out.http.oasCompanies = {
      status: companies.status,
      authRequired: companies.status === 401 || cj.error === "authentication_required",
    };
    const list = await fetch(`${baseUrl}/api/third-party-operators?activeOnly=1`);
    const lj = await list.json().catch(() => ({}));
    const inList = (lj.operators || []).some((o) => o.id === OP_ID);
    out.http.explorerList = { status: list.status, includesSandbox: inList };
  }

  const sectionFails = Object.entries(out.explorer.sections).filter(([, v]) => !v.ok);
  const pass =
    out.explorer.notMock &&
    sectionFails.length <= 1 &&
    out.oas.operatorFound &&
    out.strategy.rowWouldRender;

  out.overall = pass ? "pass" : "partial";

  const reportPath = path.join(ROOT, "reports", `operator-setup-p1-ui-qa-${new Date().toISOString().slice(0, 10)}.json`);
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });
  fs.writeFileSync(reportPath, JSON.stringify(out, null, 2) + "\n", "utf8");

  console.log(JSON.stringify(out, null, 2));
  console.log("\nWrote", reportPath);
  process.exit(pass ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
