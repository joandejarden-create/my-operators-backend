#!/usr/bin/env node
/**
 * ADP Admin local regression recovery assertions.
 * No paid research / Airtable / republish / deploy.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import {
  buildAdpAdminRuntimeIdentityV1,
  assertAdpAdminRuntimeCompatibleV1,
  ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY,
} from "../lib/ai-demand-positioning/monthly-review/admin/adp-admin-runtime-identity-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../lib/ai-demand-positioning/governance/adp-operational-registry-hygiene-v1.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../lib/ai-demand-positioning/certification/adp-unprompted-independent-discovery-standard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/dealality-adp-admin-local-regression-recovery-v1"
);
const DURABLE = path.join(
  ROOT,
  "reports/dealality-adp-single-version-publication-contract-v1/admin-published-vs-measured-dates/PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json"
);

fs.mkdirSync(OUT, { recursive: true });
const results = [];
function check(id, pass, detail = null) {
  results.push({ id, pass: Boolean(pass), detail });
  console.log(`${pass ? "PASS" : "FAIL"} ${id}${detail ? ` — ${detail}` : ""}`);
}

const coverage = buildPublishedAdpReviewCoverageV1({});
const props = (coverage.properties || []).filter(
  (p) => !isAdpSyntheticOrTestPropertyId(p.propertyId)
);
const synth = (coverage.properties || []).filter((p) =>
  isAdpSyntheticOrTestPropertyId(p.propertyId)
);
const identity = buildAdpAdminRuntimeIdentityV1(process.env, coverage);
const compat = assertAdpAdminRuntimeCompatibleV1(identity);
const durable = JSON.parse(fs.readFileSync(DURABLE, "utf8"));

check("INVENTORY_38", props.length === ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY, String(props.length));
check("NO_SYNTHETIC", synth.length === 0, JSON.stringify(synth.map((p) => p.propertyId)));
check(
  "ALL_READY",
  props.every((p) => p.coverageStatus === "READY"),
  props.filter((p) => p.coverageStatus !== "READY").slice(0, 3).map((p) => p.propertyId).join(",")
);
check(
  "ALL_PDF_READY",
  props.every((p) => p.overallPdfStatus?.status === "READY"),
  String(props.filter((p) => p.overallPdfStatus?.status !== "READY").length)
);
check(
  "ALL_VERIFIED_LEDGER",
  props.every((p) => p.customerAccess?.status === "EXTERNAL_ACCESS_VERIFIED"),
  String(props.filter((p) => p.customerAccess?.status !== "EXTERNAL_ACCESS_VERIFIED").length)
);
check(
  "PUBLISHED_VS_MEASURED_DISTINCT",
  props.some((p) => p.measuredMonthYear && p.publishedMonthYear),
  "has measured+published labels"
);
check(
  "CONSIDERATION_MATCHES_DURABLE",
  durable.rows.every((d) => {
    const row = props.find((p) => p.propertyId === d.propertyId);
    return (
      row &&
      Math.abs(Number(row.publishedConsideration) - Number(d.considerationRate)) <= 0.05
    );
  }),
  "38 consideration parity"
);
check(
  "MAYFLOWER_NOT_NEEDS_REBUILD",
  props.find((p) => p.propertyId === "adp_mayflower_hotel_washington_dc")
    ?.coverageStatus === "READY"
);
check(
  "DURABLE_MODE_PREFER",
  identity.environment.durableMode === "prefer" ||
    identity.environment.durableMode === "required",
  identity.environment.durableMode
);
check("RUNTIME_IDENTITY_OK", compat.ok, JSON.stringify(compat.defects?.slice(0, 3)));
check(
  "METHODOLOGY_V1_1_1",
  identity.methodology.id === CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID
);
check(
  "HTML_ENTRYPOINT_EXISTS",
  fs.existsSync(path.join(ROOT, "public/app/admin/ai-demand-admin.html"))
);

const doc = {
  generatedAt: new Date().toISOString(),
  results,
  passed: results.filter((r) => r.pass).length,
  failed: results.filter((r) => !r.pass).length,
  identity,
  coverageSummary: {
    propertyCount: props.length,
    ready: props.filter((p) => p.coverageStatus === "READY").length,
    pdfReady: props.filter((p) => p.overallPdfStatus?.status === "READY").length,
    verified: props.filter(
      (p) => p.customerAccess?.status === "EXTERNAL_ACCESS_VERIFIED"
    ).length,
  },
  stopBeforeProductionDeploy: true,
};
fs.writeFileSync(path.join(OUT, "LOCAL_REGRESSION_TEST_RESULTS.json"), `${JSON.stringify(doc, null, 2)}\n`);
console.log(JSON.stringify({ passed: doc.passed, failed: doc.failed }, null, 2));
process.exit(doc.failed ? 1 : 0);
