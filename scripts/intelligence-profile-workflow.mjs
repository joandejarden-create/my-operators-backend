#!/usr/bin/env node
/**
 * Dealality Intelligence Production Workflow v1 — plan + read-only dry-run orchestration.
 *
 * Usage:
 *   npm run intelligence-profile-workflow -- --entity-type operator --target-rec-id rec... --plan
 *   npm run intelligence-profile-workflow -- --entity-type operator --target-rec-id rec... --verify
 *   npm run intelligence-profile-workflow -- --entity-type operator --target-rec-id rec... --all-dry-run
 *
 * v1 does NOT apply writes. Use printed commands for steward/extract/publish apply paths.
 */
import "../load-env.js";
import { spawnSync } from "child_process";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { PARTNER_INTELLIGENCE_LINKS } from "../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources } from "../lib/partner-intelligence/airtable-source.js";
import { listPartnerFacts } from "../lib/partner-intelligence/airtable-facts.js";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildIntelligenceProfileWorkflowPlan,
  buildWorkflowMarkdown,
  isSupportedEntityType,
} from "../lib/partner-intelligence/intelligence-profile-workflow.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const READINESS_JSON = join(ROOT, "reports", "partner-intelligence-publish-readiness.json");

const PLAN = process.argv.includes("--plan") || (!hasActionFlag() && !process.argv.includes("--help"));
const VERIFY = process.argv.includes("--verify");
const ALL_DRY_RUN = process.argv.includes("--all-dry-run");
const PUBLISH_DRY_RUN = process.argv.includes("--publish-dry-run");
const EXTRACT = process.argv.includes("--extract");
const STEWARD_SOURCES = process.argv.includes("--steward-sources");
const STEWARD_FACTS = process.argv.includes("--steward-facts");
const CAPTURE = process.argv.includes("--capture");

function hasActionFlag() {
  return (
    VERIFY ||
    ALL_DRY_RUN ||
    PUBLISH_DRY_RUN ||
    EXTRACT ||
    STEWARD_SOURCES ||
    STEWARD_FACTS ||
    CAPTURE ||
    process.argv.includes("--publish-apply")
  );
}

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const ENTITY_TYPE = argValue("--entity-type");
const TARGET_REC_ID = argValue("--target-rec-id");

function rejectApplyFlags() {
  if (process.argv.includes("--publish-apply") || process.argv.includes("--apply")) {
    console.error(
      "[intelligence-profile-workflow] Apply orchestration is disabled in v1. Use explicit steward/extract/publish scripts from the plan output."
    );
    process.exit(1);
  }
}

function runNpm(scriptArgs, label) {
  console.log(`[intelligence-profile-workflow] → ${label}`);
  const res = spawnSync("npm", ["run", ...scriptArgs], {
    cwd: ROOT,
    stdio: "inherit",
    shell: true,
    env: process.env,
  });
  if (res.status !== 0) {
    console.error(`[intelligence-profile-workflow] command failed: ${label}`);
    process.exit(res.status || 1);
  }
}

async function fetchAllSources(filter) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ ...filter, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchAllFacts(filter) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ ...filter, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchTargetProfile(entityType, targetRecId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const table =
    entityType === "brand"
      ? PARTNER_INTELLIGENCE_LINKS.brandBasics
      : process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || PARTNER_INTELLIGENCE_LINKS.operatorMaster;
  const base = new Airtable({ apiKey }).base(baseId);
  try {
    const rec = await base(table).find(targetRecId);
    const fields = rec.fields || {};
    const name =
      entityType === "brand"
        ? String(fields["Brand Name"] || fields.brand_name || "").trim()
        : String(
            fields.company_name || fields["Company Name"] || fields["Operator Name"] || ""
          ).trim();
    return { id: rec.id, entityType, name: name || null, fields };
  } catch {
    return null;
  }
}

function loadReadinessReport() {
  try {
    return JSON.parse(readFileSync(READINESS_JSON, "utf8"));
  } catch {
    return null;
  }
}

async function buildPlan() {
  if (!ENTITY_TYPE || !TARGET_REC_ID) {
    console.error(
      "Usage: npm run intelligence-profile-workflow -- --entity-type brand|operator --target-rec-id rec... [--plan|--verify|--all-dry-run]"
    );
    process.exit(1);
  }
  if (!isSupportedEntityType(ENTITY_TYPE)) {
    console.error(`Unsupported --entity-type "${ENTITY_TYPE}" for v1.`);
    process.exit(1);
  }
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const filter =
    ENTITY_TYPE === "brand"
      ? { brandId: TARGET_REC_ID }
      : { operatorId: TARGET_REC_ID };

  const [sources, facts, targetProfile] = await Promise.all([
    fetchAllSources(filter),
    fetchAllFacts(filter),
    fetchTargetProfile(ENTITY_TYPE, TARGET_REC_ID),
  ]);

  if (!targetProfile) {
    console.error(`Target record not found: ${TARGET_REC_ID}`);
    process.exit(1);
  }

  return buildIntelligenceProfileWorkflowPlan({
    entityType: ENTITY_TYPE,
    targetRecId: TARGET_REC_ID,
    targetProfile,
    sources,
    facts,
    published: [],
    readinessReport: loadReadinessReport(),
  });
}

async function main() {
  rejectApplyFlags();

  const plan = await buildPlan();
  plan.mode = PLAN
    ? "plan"
    : VERIFY
      ? "verify"
      : ALL_DRY_RUN
        ? "all-dry-run"
        : "plan";

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(plan, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildWorkflowMarkdown(plan), "utf8");

  console.log(
    `[intelligence-profile-workflow] stage=${plan.currentStage.stageId} (${plan.currentStage.stageKey}) entity=${plan.entityName}`
  );
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);

  const et = ENTITY_TYPE;
  const id = TARGET_REC_ID;

  if (VERIFY) {
    runNpm(["audit-partner-intelligence-publish-readiness"], "publish readiness audit");
    runNpm(
      [
        "publish-partner-intelligence-profile-governance",
        "--",
        "--entity-type",
        et,
        "--target-rec-id",
        id,
        "--dry-run",
      ],
      "governance publish dry-run"
    );
  }

  if (ALL_DRY_RUN) {
    runNpm(["audit-partner-intelligence-publish-readiness"], "publish readiness audit");
    runNpm(
      [
        "steward-partner-intelligence",
        "--",
        "--entity-type",
        et,
        "--target-rec-id",
        id,
        "--dry-run",
        "--recompute",
      ],
      "stewardship dry-run"
    );
    if (plan.narrowExtract?.dryRun) {
      const parts = plan.narrowExtract.dryRun.replace(/^npm run /, "").split(" -- ");
      const script = parts[0];
      const args = parts[1] ? ["--", ...parts[1].split(" ")] : [];
      runNpm([script, ...args], "narrow extract dry-run");
    }
    runNpm(
      [
        "publish-partner-intelligence-profile-governance",
        "--",
        "--entity-type",
        et,
        "--target-rec-id",
        id,
        "--dry-run",
      ],
      "governance publish dry-run"
    );
  }

  if (PUBLISH_DRY_RUN) {
    runNpm(
      [
        "publish-partner-intelligence-profile-governance",
        "--",
        "--entity-type",
        et,
        "--target-rec-id",
        id,
        "--dry-run",
      ],
      "governance publish dry-run"
    );
  }

  if (EXTRACT && plan.narrowExtract?.dryRun) {
    const parts = plan.narrowExtract.dryRun.replace(/^npm run /, "").split(" -- ");
    runNpm([parts[0], ...(parts[1] ? ["--", ...parts[1].split(" ")] : [])], "narrow extract dry-run");
  } else if (EXTRACT) {
    console.log("[intelligence-profile-workflow] No narrow extract registered for this entity — see plan nextCommands.");
  }

  if (STEWARD_SOURCES || STEWARD_FACTS) {
    runNpm(
      [
        "steward-partner-intelligence",
        "--",
        "--entity-type",
        et,
        "--target-rec-id",
        id,
        "--dry-run",
        "--recompute",
      ],
      "stewardship dry-run"
    );
  }

  if (CAPTURE) {
    console.log(
      "[intelligence-profile-workflow] Capture is not orchestrated in v1. Use partner-reference:init-folder / partner-reference:download commands from the plan report."
    );
  }

  if (PLAN || (!VERIFY && !ALL_DRY_RUN && !PUBLISH_DRY_RUN && !EXTRACT && !STEWARD_SOURCES && !STEWARD_FACTS && !CAPTURE)) {
    console.log("[intelligence-profile-workflow] Next commands:");
    for (const cmd of plan.nextCommands) console.log(`  ${cmd}`);
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
